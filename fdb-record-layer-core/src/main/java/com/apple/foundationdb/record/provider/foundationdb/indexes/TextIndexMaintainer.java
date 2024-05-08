/*
 * TextIndexMaintainer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.map.BunchedMap;
import com.apple.foundationdb.map.BunchedMapMultiIterator;
import com.apple.foundationdb.record.ByteScanLimiter;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.common.text.TextTokenizer;
import com.apple.foundationdb.record.provider.common.text.TextTokenizerRegistry;
import com.apple.foundationdb.record.provider.common.text.TextTokenizerRegistryImpl;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * The index maintainer class for full-text indexes. This takes an expression whose first
 * column (not counting grouping columns) is of type {@link com.google.protobuf.Descriptors.FieldDescriptor.Type#STRING string}.
 * It will split the text found at that column using a {@link TextTokenizer} and then write separate index keys
 * for each token found in the text. This then supports queries on the tokenized text, such as:
 *
 * <ul>
 *     <li>All records containing <em>all</em> elements from a set of tokens: {@code Query.field(fieldName).text().containsAll(tokens)}</li>
 *     <li>All records containing <em>any</em> elements from a set of tokens: {@code Query.field(fieldName).text().containsAny(tokens)}</li>
 *     <li>All records containing all elements from a set of tokens within some maximum span: {@code Query.field(fieldName).text().containsAll(tokens, span)}</li>
 *     <li>All records containing an exact phrase (modulo normalization and stop-word removal done by the tokenizer): {@code Query.field(fieldName).text().containsPhrase(phrase)}</li>
 *     <li>All records containing at least one token that begins with a given prefix: {@code Query.field(fieldName).text().containsPrefix(prefix)}</li>
 *     <li>All records containing at least one token that begins with <em>any</em> of a set of prefixes: {@code Query.field(fieldName).text().containsAnyPrefix(prefixes)}</li>
 *     <li>All records containing at least one token that begins with <em>each</em> of a set of prefixes: {@code Query.field(fieldName).text().containsAllPrefixes(prefixes)}</li>
 * </ul>
 *
 * <p>
 * One can specify a tokenizer to use by setting the {@value IndexOptions#TEXT_TOKENIZER_NAME_OPTION} and
 * {@value IndexOptions#TEXT_TOKENIZER_VERSION_OPTION} options on the index. If no tokenizer is given,
 * it will use a {@link com.apple.foundationdb.record.provider.common.text.DefaultTextTokenizer DefaultTextTokenizer},
 * and if no version is specified, it will assume version {@value TextTokenizer#GLOBAL_MIN_VERSION}.
 * There should be one {@link TextTokenizer} implementation that uses that name and one
 * {@link com.apple.foundationdb.record.provider.common.text.TextTokenizerFactory TextTokenizerFactory} implementation
 * that will supply instances of the tokenizer of that name. The version of the tokenizer used to serialize
 * each record is stored by this index maintainer, so if an index's tokenizer version changes, then this
 * index maintainer will continue to use the older tokenizer version to tokenize the fields of any records
 * present in the index prior to the version change. This guarantees that for every record, the same tokenizer
 * version is used when inserting it and when deleting it. If one wants to re-tokenize a record following
 * a tokenizer version change, then if one takes an existing record (tokenized with an older version) and saves
 * the record again, then that record will be re-indexed using the newer version.
 * </p>
 *
 * <p>
 * Because each update will add a conflict range for each token included in each indexed text field per record,
 * index updates can be particularly taxing on the resolver process within the FoundationDB cluster. Some use cases
 * can therefore benefit from having fewer, larger conflict ranges per transaction to lessen the work done. The
 * trade-off is that there is now potentially less parallelism in that there is a larger change of conflicts
 * between records that arrive simultaneously, though it should be noted that the underlying data structure of the
 * text index means that it is already likely that two records that happen to share common tokens that are updated
 * simultaneously will conflict, so it might not actually produce more conflicts in practice. To enable adding
 * conflict ranges over larger areas, set the {@value IndexOptions#TEXT_ADD_AGGRESSIVE_CONFLICT_RANGES_OPTION} option
 * to {@code true}. <b>Warning:</b> This feature is currently experimental, and may change at any moment without prior notice.
 * <!-- TODO: Remove the above disclaimer if/when we are happy with this feature staying in.-->
 * </p>
 *
 * <p>
 * <b>Note:</b> At the moment, this index is under active development and should be considered
 * experimental. At the current time, this index will be correctly updated on insert and removal
 * and can be manually scanned, but it will only be selected by the query planner in limited circumstances
 * to satisfy full text queries. For example, the query planner will not select this index if there
 * are sorts involved in the query or if the filter involves using the position list to determine the
 * relative positions of tokens within a document.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public class TextIndexMaintainer extends StandardIndexMaintainer {
    private static final Logger LOGGER = LoggerFactory.getLogger(TextIndexMaintainer.class);
    private static final TextTokenizerRegistry registry = TextTokenizerRegistryImpl.instance();
    private static final int BUNCH_SIZE = 20;
    private static final BunchedMap<Tuple, List<Integer>> BUNCHED_MAP = new BunchedMap<>(TextIndexBunchedSerializer.instance(), Comparator.naturalOrder(), BUNCH_SIZE);

    // Subspaces used within the index secondary subspace for additional meta-data.
    // (Currently, there is only one, but this allows for expansion if we ever decide
    // to use a more compact format or add an indirection layer for keys to reduce the key-size, etc.)
    @VisibleForTesting
    @Nonnull
    static final Tuple TOKENIZER_VERSION_SUBSPACE_TUPLE = Tuple.from(0L);

    @Nonnull
    private final TextTokenizer tokenizer;
    private final int tokenizerVersion;
    private final boolean addAggressiveConflictRanges;
    private final boolean omitPositionLists;

    /**
     * Get the text tokenizer associated with this index. This uses the
     * value of the "{@value IndexOptions#TEXT_TOKENIZER_NAME_OPTION}" option to
     * determine the name of the tokenizer and then looks up the tokenizer
     * in the tokenizer registry.
     *
     * @param index the index to get the tokenizer of
     * @return the tokenizer associated with this index
     */
    @Nonnull
    public static TextTokenizer getTokenizer(@Nonnull Index index) {
        String tokenizerName = index.getOption(IndexOptions.TEXT_TOKENIZER_NAME_OPTION);
        return registry.getTokenizer(tokenizerName);
    }

    /**
     * Get the tokenizer version associated with this index. This will parse the
     * "{@value IndexOptions#TEXT_TOKENIZER_VERSION_OPTION}" option and produce an integer value
     * from it. If none is specified, this returns the global miminum tokenizer
     * version.
     *
     * @param index the index to get the tokenizer version of
     * @return the tokenizer version associated with the given index
     */
    @SuppressWarnings("PMD.PreserveStackTrace")
    public static int getIndexTokenizerVersion(@Nonnull Index index) {
        String versionStr = index.getOption(IndexOptions.TEXT_TOKENIZER_VERSION_OPTION);
        if (versionStr != null) {
            try {
                return Integer.parseInt(versionStr);
            } catch (NumberFormatException e) {
                throw new MetaDataException("tokenizer version could not be parsed as int")
                        .addLogInfo("index", index.getName())
                        .addLogInfo(IndexOptions.TEXT_TOKENIZER_VERSION_OPTION, versionStr);
            }
        } else {
            return TextTokenizer.GLOBAL_MIN_VERSION;
        }
    }

    static boolean getIfAddAggressiveConflictRanges(@Nonnull Index index) {
        return index.getBooleanOption(IndexOptions.TEXT_ADD_AGGRESSIVE_CONFLICT_RANGES_OPTION, false);
    }

    static boolean getIfOmitPositions(@Nonnull Index index) {
        return index.getBooleanOption(IndexOptions.TEXT_OMIT_POSITIONS_OPTION, false);
    }

    // Gets the position of the text field this index is tokenizing from within the
    // index's expression. This is the first column of the index expression after
    // all grouping columns (or the first column if there are no grouping columns).
    static int textFieldPosition(@Nonnull KeyExpression expression) {
        if (expression instanceof GroupingKeyExpression) {
            return ((GroupingKeyExpression) expression).getGroupingCount();
        } else {
            return 0;
        }
    }

    @Nonnull
    static BunchedMap<Tuple, List<Integer>> getBunchedMap(@Nonnull FDBRecordContext context) {
        if (context.getTimer() != null) {
            return new InstrumentedBunchedMap<>(BUNCHED_MAP, context.getTimer(), context.getExecutor());
        } else {
            return BUNCHED_MAP;
        }
    }

    protected TextIndexMaintainer(@Nonnull IndexMaintainerState state) {
        super(state);
        this.tokenizer = getTokenizer(state.index);
        this.tokenizerVersion = getIndexTokenizerVersion(state.index);
        this.addAggressiveConflictRanges = getIfAddAggressiveConflictRanges(state.index);
        this.omitPositionLists = getIfOmitPositions(state.index);
    }

    private static int varIntSize(int val) {
        if (val == 0) {
            return 1;
        } else {
            return (Integer.SIZE - Integer.numberOfLeadingZeros(val) + 6) / 7;
        }
    }

    @Nonnull
    private byte[] getRecordTokenizerKey(@Nonnull Tuple primaryKey) {
        return getSecondarySubspace().subspace(TOKENIZER_VERSION_SUBSPACE_TUPLE).subspace(primaryKey).pack();
    }

    @Nonnull
    private CompletableFuture<Integer> getRecordTokenizerVersion(@Nonnull Tuple primaryKey) {
        byte[] key = getRecordTokenizerKey(primaryKey);
        return state.transaction.get(key).thenApply(rawVersion -> {
            if (rawVersion == null) {
                return TextTokenizer.GLOBAL_MIN_VERSION;
            } else {
                return (int)(Tuple.fromBytes(rawVersion).getLong(0));
            }
        });
    }

    private void writeRecordTokenizerVersion(@Nonnull Tuple primaryKey) {
        state.transaction.set(getRecordTokenizerKey(primaryKey), Tuple.from(tokenizerVersion).pack());
    }

    private void clearRecordTokenizerVersion(@Nonnull Tuple primaryKey) {
        state.transaction.clear(getRecordTokenizerKey(primaryKey));
    }

    @Nonnull
    private NonnullPair<Integer, Integer> estimateSize(@Nullable Tuple groupingKey, @Nonnull Map<String, List<Integer>> positionMap, @Nonnull Tuple groupedKey) {
        final int idSize = groupedKey.pack().length;
        final int subspaceSize = getIndexSubspace().getKey().length + (groupingKey != null ? groupingKey.pack().length : 0);
        int keySize = 0;
        int valueSize = 0;
        for (Map.Entry<String, List<Integer>> posting : positionMap.entrySet()) {
            keySize += subspaceSize + 2 + posting.getKey().length() + idSize;
            if (omitPositionLists) {
                valueSize += 1;
            } else {
                int listSize = posting.getValue().stream().mapToInt(TextIndexMaintainer::varIntSize).sum();
                valueSize += varIntSize(idSize) + idSize + varIntSize(listSize) + listSize;
            }
        }
        return NonnullPair.of(keySize, valueSize);
    }

    @Nonnull
    private <M extends Message> CompletableFuture<Void> updateOneKeyAsync(@Nonnull FDBIndexableRecord<M> savedRecord,
                                                                          final boolean remove,
                                                                          @Nonnull IndexEntry entry,
                                                                          int textPosition,
                                                                          int recordTokenizerVersion) {
        long startTime = System.nanoTime();
        final Tuple indexEntryKey = indexEntryKey(entry.getKey(), savedRecord.getPrimaryKey());
        final String text = indexEntryKey.getString(textPosition);
        if (text == null || text.isEmpty()) {
            // If the text is "null" or the string is empty, it means that the text was either
            // empty or not set. Either way, there is nothing to tokenize, so just exit now.
            return AsyncUtil.DONE;
        }
        final Tuple groupingKey = (textPosition == 0) ? null : TupleHelpers.subTuple(indexEntryKey, 0, textPosition);
        final Tuple groupedKey = TupleHelpers.subTuple(indexEntryKey, textPosition + 1, indexEntryKey.size());
        final Map<String, List<Integer>> positionMap = tokenizer.tokenizeToMap(text, recordTokenizerVersion, TextTokenizer.TokenizerMode.INDEX);
        final StoreTimer.Event indexUpdateEvent = remove ? FDBStoreTimer.Events.DELETE_INDEX_ENTRY : FDBStoreTimer.Events.SAVE_INDEX_ENTRY;
        if (LOGGER.isDebugEnabled()) {
            final NonnullPair<Integer, Integer> estimatedSize = estimateSize(groupingKey, positionMap, groupedKey);
            KeyValueLogMessage msg = KeyValueLogMessage.build("performed text tokenization",
                                        LogMessageKeys.REMOVE, remove,
                                        LogMessageKeys.TEXT_SIZE, text.length(),
                                        LogMessageKeys.UNIQUE_TOKENS, positionMap.size(),
                                        LogMessageKeys.AVG_TOKEN_SIZE, positionMap.keySet().stream().mapToInt(String::length).sum() * 1.0 / positionMap.size(),
                                        LogMessageKeys.MAX_TOKEN_SIZE, positionMap.keySet().stream().mapToInt(String::length).max().orElse(0),
                                        LogMessageKeys.AVG_POSITIONS, positionMap.values().stream().mapToInt(List::size).sum() * 1.0 / positionMap.size(),
                                        LogMessageKeys.MAX_POSITIONS, positionMap.values().stream().mapToInt(List::size).max().orElse(0),
                                        LogMessageKeys.TEXT_KEY_SIZE, estimatedSize.getKey(),
                                        LogMessageKeys.TEXT_VALUE_SIZE, estimatedSize.getValue(),
                                        LogMessageKeys.TEXT_INDEX_SIZE_AMORTIZED, estimatedSize.getKey() / 10 + estimatedSize.getValue(),
                                        IndexOptions.TEXT_TOKENIZER_NAME_OPTION, tokenizer.getName(),
                                        IndexOptions.TEXT_TOKENIZER_VERSION_OPTION, recordTokenizerVersion,
                                        IndexOptions.TEXT_ADD_AGGRESSIVE_CONFLICT_RANGES_OPTION, addAggressiveConflictRanges,
                                        LogMessageKeys.PRIMARY_KEY, savedRecord.getPrimaryKey(),
                                        LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(state.store.getSubspace().getKey()),
                                        LogMessageKeys.INDEX_SUBSPACE, ByteArrayUtil2.loggable(state.indexSubspace.getKey()),
                                        LogMessageKeys.WROTE_INDEX, true);
            LOGGER.debug(msg.toString());
        }
        if (positionMap.isEmpty()) {
            if (state.store.getTimer() != null) {
                state.store.getTimer().recordSinceNanoTime(indexUpdateEvent, startTime);
            }
            return AsyncUtil.DONE;
        }
        if (addAggressiveConflictRanges) {
            // Add a read and write conflict range over the whole index to decrease the number of mutations
            // sent to the resolver. In theory, this will increase the number of conflicts in that if two
            // records with the same grouping key come in at the same time, then they will now definitely
            // conflict. However, this isn't too bad because there is already a high chance of conflict
            // in the text index because each token insert has to do a read on its own.
            final Range indexRange = groupingKey == null ? state.indexSubspace.range() : state.indexSubspace.range(groupingKey);
            state.context.ensureActive().addReadConflictRange(indexRange.begin, indexRange.end);
            state.context.ensureActive().addWriteConflictRange(indexRange.begin, indexRange.end);
        }
        final BunchedMap<Tuple, List<Integer>> bunchedMap = getBunchedMap(state.context);
        CompletableFuture<Void> tokenInsertFuture = RecordCursor.fromIterator(state.context.getExecutor(), positionMap.entrySet().iterator())
                .forEachAsync((Map.Entry<String, List<Integer>> tokenEntry) -> {
                    Tuple subspaceTuple;
                    if (groupingKey == null) {
                        subspaceTuple = Tuple.from(tokenEntry.getKey());
                    } else {
                        subspaceTuple = groupingKey.add(tokenEntry.getKey());
                    }
                    Subspace mapSubspace = state.indexSubspace.subspace(subspaceTuple);
                    if (remove) {
                        return bunchedMap.remove(state.transaction, mapSubspace, groupedKey).thenAccept(ignore -> { });
                    } else {
                        final List<Integer> value = omitPositionLists ? Collections.emptyList() : tokenEntry.getValue();
                        return bunchedMap.put(state.transaction, mapSubspace, groupedKey, value).thenAccept(ignore -> { });
                    }
                }, state.store.getPipelineSize(PipelineOperation.TEXT_INDEX_UPDATE));
        if (state.store.getTimer() != null) {
            return state.store.getTimer().instrument(indexUpdateEvent, tokenInsertFuture, state.context.getExecutor(), startTime);
        } else {
            return tokenInsertFuture;
        }
    }

    @Nonnull
    private <M extends Message> CompletableFuture<Void> updateIndexKeys(@Nonnull final FDBIndexableRecord<M> savedRecord,
                                                                        final boolean remove,
                                                                        @Nonnull final List<IndexEntry> indexEntries,
                                                                        final int recordTokenizerVersion) {
        if (indexEntries.isEmpty()) {
            return AsyncUtil.DONE;
        }
        int textPosition = textFieldPosition(state.index.getRootExpression());
        if (indexEntries.size() == 1) {
            // Most text indexes are probably on a single field and return a single entry, so this
            // should generally be the branch that the index maintainer chooses. In those cases,
            // we don't want all of the cruft necessary to handle multiple entries.
            return updateOneKeyAsync(savedRecord, remove, indexEntries.get(0), textPosition, recordTokenizerVersion);
        } else {
            // TODO: If there are multiple index entries, it is possible that they all share the same text object.
            //  We don't want to tokenize more than once if that is the case because it is expensive.
            //  Also, this has to be done in serial (rather than in parallel) in the case where there are
            //  multiple keys for the given text, we can't add the two documents concurrently
            //  due to limitations in the thread safety of the underlying data structure.
            //  (In theory, we could do this if each key has it's own grouping key, but that's an
            //  optimization for another day.)
            AtomicInteger pos = new AtomicInteger(0);
            return AsyncUtil.whileTrue(() ->
                            updateOneKeyAsync(savedRecord, remove, indexEntries.get(pos.getAndIncrement()), textPosition, recordTokenizerVersion)
                                    .thenApply(ignore -> pos.get() < indexEntries.size()),
                    state.store.getExecutor());
        }
    }

    /**
     * Update index according to record keys. This will tokenize the text associated with this record and
     * write out one index key for each token containing the position list as its value. Because writing
     * to the full-text data structures requires reading from the database, so this future should be
     * assumed to take a while to complete.
     *
     * @param savedRecord the record being indexed
     * @param remove <code>true</code> if removing from index.
     * @param indexEntries the result of {@link #evaluateIndex(com.apple.foundationdb.record.provider.foundationdb.FDBRecord)}
     * @return a future completed when update is done
     */
    @Nonnull
    @Override
    protected <M extends Message> CompletableFuture<Void> updateIndexKeys(@Nonnull final FDBIndexableRecord<M> savedRecord,
                                                                          final boolean remove,
                                                                          @Nonnull final List<IndexEntry> indexEntries) {
        if (indexEntries.isEmpty()) {
            return AsyncUtil.DONE;
        }
        if (remove) {
            // Get the tokenizer version of the record to make sure that the correct tokens are removed.
            return getRecordTokenizerVersion(savedRecord.getPrimaryKey()).thenCompose(recordTokenizerVersion ->
                    updateIndexKeys(savedRecord, true, indexEntries, recordTokenizerVersion)
            );
        } else {
            // Not a removal. Write using the index's configured tokenizer version.
            return updateIndexKeys(savedRecord, false, indexEntries, tokenizerVersion);
        }
    }

    /**
     * Updates an associated text index with the data associated with a new record.
     * Unlike most standard indexes, the text-index can behave somewhat differently
     * if a record was previously written with this index but with an older tokenizer
     * version, then it will always re-index the record and will write index entries
     * to the database even if they are un-changed. The record will then be registered
     * as having been written at the new tokenizer version (so subsequent updates will
     * not have to do any additional updates for unchanged fields).
     *
     * @param oldRecord the previous stored record or <code>null</code> if a new record is being created
     * @param newRecord the new record or <code>null</code> if an old record is being deleted
     * @return a future that is complete when the record update is done
     * @see com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer#update(FDBIndexableRecord, FDBIndexableRecord)
     */
    @Nonnull
    @Override
    @SuppressWarnings("squid:S1604") // need annotation so no lambda
    public <M extends Message> CompletableFuture<Void> update(@Nullable FDBIndexableRecord<M> oldRecord, @Nullable FDBIndexableRecord<M> newRecord) {
        if (oldRecord == null && newRecord != null) {
            // Inserting a new record.
            // Write the tokenizer version now, then insert the record.
            writeRecordTokenizerVersion(newRecord.getPrimaryKey());
            return super.update(null, newRecord);
        } else if (oldRecord != null && newRecord == null) {
            // Deleting an old record. Clear out the entries, then
            // clear out the tokenizer version.
            return super.update(oldRecord, null).thenRun(new Runnable() {
                @Override
                @SpotBugsSuppressWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", justification = "https://github.com/spotbugs/spotbugs/issues/552")
                public void run() {
                    TextIndexMaintainer.this.clearRecordTokenizerVersion(oldRecord.getPrimaryKey());
                }
            }
            );
        } else if (oldRecord != null) {
            // Updating an existing record.
            return getRecordTokenizerVersion(oldRecord.getPrimaryKey()).thenCompose(recordTokenizerVersion -> {
                if (recordTokenizerVersion == tokenizerVersion) {
                    // In this case, we don't need to do any book-keeping of the tokenizer version, and
                    // updating the entries works exactly the same for this record as all others.
                    return super.update(oldRecord, newRecord);
                } else {
                    // Because the tokenizer version changed, we will re-index the record.
                    // This is necessary if some of the entries have changed but not others in
                    // order to make sure all entries are tokenized with the same version.
                    // (The alternative is keeping a version per index entry, which sounds painful.)
                    // TODO: It is entirely possible that most of these entries are the same even after tokenizing, so we should not update them needlessly
                    // TODO: Be more selective about which values are re-written during re-tokenization (https://github.com/FoundationDB/fdb-record-layer/issues/8)
                    return super.update(oldRecord, null).thenCompose(new Function<Void, CompletionStage<Void>>() {
                        @Override
                        @SpotBugsSuppressWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", justification = "https://github.com/spotbugs/spotbugs/issues/552")
                        public CompletionStage<Void> apply(Void vignore) {
                            TextIndexMaintainer.this.writeRecordTokenizerVersion(newRecord.getPrimaryKey());
                            return TextIndexMaintainer.super.update(null, newRecord);
                        }
                    });
                }
            });
        } else {
            // Both records are null. Nothing to do.
            // In practice, we should never reach this case, but this is the thing
            // that would be done should we ever reach here.
            return AsyncUtil.DONE;
        }
    }

    /**
     * Indicates whether the expression allows for this index to perform a {@link FDBRecordStore#deleteRecordsWhere(QueryComponent)}
     * operation. A text index can only delete records that are aligned with its grouping key, as
     * once text from the index has been tokenized, there is not a way to efficiently remove all of
     * documents within the grouped part of the index.
     *
     * @param matcher object to match the grouping key to a query component
     * @param evaluated an evaluated key that might align with this index's grouping key
     * @return whether the index maintainer can remove all records matching <code>matcher</code>
     */
    @Override
    public boolean canDeleteWhere(@Nonnull QueryToKeyMatcher matcher, @Nonnull Key.Evaluated evaluated) {
        return canDeleteGroup(matcher, evaluated);
    }

    /**
     * Scan this index between a range of tokens. This index type requires that it be scanned only
     * by text token. The range to scan can otherwise be between any two entries in the list, and
     * scans over a prefix are supported by passing a value of <code>range</code> that uses
     * {@link com.apple.foundationdb.record.EndpointType#PREFIX_STRING PREFIX_STRING} as both endpoint types.
     * The keys returned in the index entry will include the token that was found in the index
     * when scanning in the column that is used for the text field of the index's root expression.
     * The value portion of each index entry will be a tuple whose first element is the position
     * list for that entry within its associated record's field.
     *
     * @param scanType the {@link IndexScanType type} of scan to perform
     * @param range the range to scan
     * @param continuation any continuation from a previous scan invocation
     * @param scanProperties skip, limit and other properties of the scan
     * @return a cursor over all index entries in <code>range</code>
     * @throws RecordCoreException if <code>scanType</code> is not {@link IndexScanType#BY_TEXT_TOKEN}
     * @see TextCursor
     */
    @Nonnull
    @Override
    @SuppressWarnings({"squid:S2095", "PMD.CloseResource"}) // not closing the returned cursor
    public RecordCursor<IndexEntry> scan(@Nonnull IndexScanType scanType,
                                         @Nonnull TupleRange range,
                                         @Nullable byte[] continuation,
                                         @Nonnull ScanProperties scanProperties) {
        if (!scanType.equals(IndexScanType.BY_TEXT_TOKEN)) {
            throw new RecordCoreException("Can only scan text index by text token.");
        }
        int textPosition = textFieldPosition(state.index.getRootExpression());
        TextSubspaceSplitter subspaceSplitter = new TextSubspaceSplitter(state.indexSubspace, textPosition + 1);
        Range byteRange = range.toRange();
        ScanProperties withAdjustedLimit = scanProperties.with(ExecuteProperties::clearSkipAndAdjustLimit);
        ExecuteProperties adjustedExecuteProperties = withAdjustedLimit.getExecuteProperties();

        // Callback for updating the byte scan limit
        final ByteScanLimiter byteScanLimiter = adjustedExecuteProperties.getState().getByteScanLimiter();
        final Consumer<KeyValue> callback = keyValue -> byteScanLimiter.registerScannedBytes(keyValue.getKey().length + keyValue.getValue().length);

        BunchedMapMultiIterator<Tuple, List<Integer>, Tuple> iterator = getBunchedMap(state.context).scanMulti(
                state.context.readTransaction(adjustedExecuteProperties.getIsolationLevel().isSnapshot()),
                state.indexSubspace,
                subspaceSplitter,
                byteRange.begin,
                byteRange.end,
                continuation,
                adjustedExecuteProperties.getReturnedRowLimit(),
                callback,
                scanProperties.isReverse()
        );
        RecordCursor<IndexEntry> cursor = new TextCursor(iterator, state.store.getExecutor(), state.context, withAdjustedLimit, state.index);
        if (scanProperties.getExecuteProperties().getSkip() != 0) {
            cursor = cursor.skip(scanProperties.getExecuteProperties().getSkip());
        }
        return cursor;
    }

    private static class InstrumentedBunchedMap<K, V> extends BunchedMap<K, V> {
        @Nonnull
        private final FDBStoreTimer timer;
        @Nonnull
        private final Executor executor;

        public InstrumentedBunchedMap(@Nonnull BunchedMap<K, V> model, @Nonnull FDBStoreTimer timer, @Nonnull Executor executor) {
            super(model);
            this.timer = timer;
            this.executor = executor;
        }

        @Override
        protected void instrumentDelete(@Nonnull byte[] key, @Nullable byte[] oldValue) {
            timer.increment(FDBStoreTimer.Counts.DELETE_INDEX_KEY);
            timer.increment(FDBStoreTimer.Counts.DELETE_INDEX_KEY_BYTES, key.length);
            if (oldValue != null) {
                timer.increment(FDBStoreTimer.Counts.DELETE_INDEX_VALUE_BYTES, oldValue.length);
            }
        }

        @Override
        protected void instrumentWrite(@Nonnull byte[] key, @Nonnull byte[] value, @Nullable byte[] oldValue) {
            timer.increment(FDBStoreTimer.Counts.SAVE_INDEX_KEY);
            timer.increment(FDBStoreTimer.Counts.SAVE_INDEX_KEY_BYTES, key.length);
            timer.increment(FDBStoreTimer.Counts.SAVE_INDEX_VALUE_BYTES, value.length);
            if (oldValue != null) {
                // Or should this ignore the value altogether?
                timer.increment(FDBStoreTimer.Counts.DELETE_INDEX_VALUE_BYTES, oldValue.length);
            }
        }

        @Override
        @Nonnull
        protected CompletableFuture<List<KeyValue>> instrumentRangeRead(@Nonnull CompletableFuture<List<KeyValue>> readFuture) {
            return timer.instrument(FDBStoreTimer.Events.SCAN_INDEX_KEYS, readFuture, executor).whenComplete((list, err) -> {
                if (list != null && !list.isEmpty()) {
                    int keyBytes = 0;
                    int valueBytes = 0;
                    for (KeyValue kv : list) {
                        keyBytes += kv.getKey().length;
                        valueBytes += kv.getValue().length;
                    }
                    timer.increment(FDBStoreTimer.Counts.LOAD_INDEX_KEY, list.size());
                    timer.increment(FDBStoreTimer.Counts.LOAD_INDEX_KEY_BYTES, keyBytes);
                    timer.increment(FDBStoreTimer.Counts.LOAD_INDEX_VALUE_BYTES, valueBytes);
                }
            });
        }
    }
}
