/*
 * LuceneIndexAutoCompleteQueryPlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.apple.foundationdb.record.query.plan.PlanOrderingKey;
import com.apple.foundationdb.record.query.plan.plans.QueryPlanUtils;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.Iterables;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Lucene query plan that auto-completes partial phrases.
 */
public class LuceneIndexAutoCompleteQueryPlan extends LuceneIndexQueryPlan {
    protected LuceneIndexAutoCompleteQueryPlan(@Nonnull String indexName, @Nonnull LuceneScanParameters scanParameters,
                                               @Nonnull FetchIndexRecords fetchIndexRecords, boolean reverse,
                                               @Nullable PlanOrderingKey planOrderingKey, @Nullable List<KeyExpression> storedFields) {
        super(indexName, scanParameters, fetchIndexRecords, reverse, planOrderingKey, storedFields);
    }

    @SuppressWarnings("resource")
    @Nonnull
    @Override
    public <M extends Message> RecordCursor<FDBQueriedRecord<M>> fetchIndexRecords(@Nonnull final FDBRecordStoreBase<M> store,
                                                                                   @Nonnull final EvaluationContext evaluationContext,
                                                                                   @Nonnull final Function<byte[], RecordCursor<IndexEntry>> entryCursorFunction,
                                                                                   @Nullable final byte[] continuation,
                                                                                   @Nonnull final ExecuteProperties executeProperties) {
        final RecordMetaData metaData = store.getRecordMetaData();
        final Index index = metaData.getIndex(indexName);
        final Collection<RecordType> recordTypes = metaData.recordTypesForIndex(index);
        final IndexScanType scanType = getScanType();

        final int limit = Math.min(executeProperties.getReturnedRowLimitOrMax(),
                Verify.verifyNotNull(store.getContext().getPropertyStorage().getPropertyValue(LuceneRecordContextProperties.LUCENE_AUTO_COMPLETE_SEARCH_LIMITATION)));

        final var indexEntriesFromRecordCursor =
                findIndexEntriesInRecord(store,
                        index,
                        nestedContinuation -> super.fetchIndexRecords(store, evaluationContext, entryCursorFunction, nestedContinuation, executeProperties.clearSkipAndLimit()),
                        (LuceneScanAutoCompleteParameters)scanParameters,
                        evaluationContext,
                        continuation);
        final RecordType recordType = Iterables.getOnlyElement(recordTypes);
        return indexEntriesFromRecordCursor
                .skipThenLimit(executeProperties.getSkip(), limit)
                .map(QueryPlanUtils.getCoveringIndexEntryToPartialRecordFunction(store, recordType.getName(), indexName,
                        LuceneIndexKeyValueToPartialRecordUtils.getToPartialRecord(index, recordType, scanType), true));
    }

    @Nonnull
    @SuppressWarnings({"unchecked", "resource"})
    public static <M extends Message> RecordCursor<IndexEntry> findIndexEntriesInRecord(@Nonnull final FDBRecordStoreBase<M> store,
                                                                                        @Nonnull final Index index,
                                                                                        @Nonnull final Function<byte[], RecordCursor<FDBQueriedRecord<M>>> recordCursorFunction,
                                                                                        @Nonnull final LuceneScanAutoCompleteParameters autoCompleteScanParameters,
                                                                                        @Nonnull final EvaluationContext evaluationContext,
                                                                                        @Nullable byte[] continuation) {
        final var recordContext = store.getContext();
        final int maxTextLength = Objects.requireNonNull(recordContext.getPropertyStorage()
                .getPropertyValue(LuceneRecordContextProperties.LUCENE_AUTO_COMPLETE_TEXT_SIZE_UPPER_LIMIT));
        final var excludedFieldNames = index.getOption(LuceneIndexOptions.AUTO_COMPLETE_EXCLUDED_FIELDS);
        final var indexMaintainer = (LuceneIndexMaintainer)store.getIndexMaintainer(index);
        final var autoCompleteScanBounds = autoCompleteScanParameters.bind(store, index, evaluationContext);
        final var queryAnalyzer = indexMaintainer.getAutoCompleteAnalyzerSelector().provideQueryAnalyzer(autoCompleteScanBounds.getKeyToComplete()).getAnalyzer();

        return RecordCursor.flatMapPipelined(recordCursorFunction,
                (fetchedRecord, innerContinuation) -> {
                    final var indexEntry = Verify.verifyNotNull(fetchedRecord.getIndexEntry());
                    final var groupingKey = indexEntry.getKey();
                    final var valueTuple = indexEntry.getValue();
                    final var score = valueTuple.getFloat(0);
                    final var autoCompleteTokens = (LuceneAutoCompleteResultCursor.AutoCompleteTokens)valueTuple.get(1);

                    // Extract the indexed fields from the document again
                    final List<LuceneDocumentFromRecord.DocumentField> documentFields = LuceneDocumentFromRecord.getRecordFields(index.getRootExpression(), fetchedRecord)
                            .get(groupingKey)
                            .stream()
                            .filter(f -> excludedFieldNames == null || !excludedFieldNames.contains(f.getFieldName()))
                            .collect(Collectors.toList());
                    return RecordCursor.fromList(store.getExecutor(), documentFields, innerContinuation).map(documentField -> {
                        // Search each field to find the first match.
                        Object fieldValue = documentField.getValue();
                        if (!(fieldValue instanceof String)) {
                            // Only can search through string fields
                            return null;
                        }
                        String text = (String)fieldValue;
                        if (text.length() > maxTextLength) {
                            // Apply the text length filter before searching through the text for the
                            // matched terms
                            return null;
                        }
                        String match;
                        if (autoCompleteScanBounds.isHighlight()) {
                            match = LuceneHighlighting.searchAllAndHighlight(documentField.getFieldName(), queryAnalyzer, text,
                                    autoCompleteTokens.getQueryTokensAsSet(), autoCompleteTokens.getPrefixTokens(), true,
                                    new LuceneScanQueryParameters.LuceneQueryHighlightParameters(-1), null);
                        } else {
                            match = LuceneAutoCompleteResultCursor.findMatch(documentField.getFieldName(), queryAnalyzer, text, autoCompleteTokens);
                        }
                        if (match == null) {
                            // Text not found in this field
                            return null;
                        }

                        // Found a match with this field!
                        Tuple key = Tuple.from(documentField.getFieldName(), match);
                        key = groupingKey.addAll(key);
                        final List<Object> primaryKey = fetchedRecord.getPrimaryKey().getItems();
                        index.trimPrimaryKey(primaryKey);
                        key = key.addAll(primaryKey);

                        IndexEntry newIndexEntry = new IndexEntry(index, key, Tuple.from(score), fetchedRecord.getPrimaryKey());
                        if (LOGGER.isTraceEnabled()) {
                            final var logMessage =
                                    KeyValueLogMessage.build("Suggestion read as an index entry")
                                            .addKeyAndValue(LogMessageKeys.INDEX_NAME, index.getName())
                                            .addKeyAndValue(LogMessageKeys.INDEX_KEY, key)
                                            .addKeyAndValue(LogMessageKeys.INDEX_VALUE, indexEntry.getValue());
                            final var subspaceProvider = store.getSubspaceProvider();
                            if (subspaceProvider != null) {
                                logMessage.addKeyAndValue(subspaceProvider.logKey(), subspaceProvider.toString(store.getContext()));
                            }
                            LOGGER.trace(logMessage.toString());
                        }
                        return newIndexEntry;
                    }).filter(Objects::nonNull); // Note: may not return any results if all matches exceed the maxTextLength
                },
                fetchedRecord -> fetchedRecord.getPrimaryKey().pack(),
                continuation,
                store.getPipelineSize(PipelineOperation.INDEX_TO_RECORD));
    }

    /**
     * Auto-Complete and Spell-Check scan has their own implementation for {@link IndexKeyValueToPartialRecord} to build partial records,
     * so they are not appropriate for the optimization by {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan}.
     */
    @Override
    public boolean allowedForCoveringIndexPlan() {
        return false;
    }

    @Nonnull
    @Override
    protected RecordQueryIndexPlan withIndexScanParameters(@Nonnull final IndexScanParameters newIndexScanParameters) {
        Verify.verify(newIndexScanParameters instanceof LuceneScanParameters);
        Verify.verify(newIndexScanParameters.getScanType().equals(LuceneScanTypes.BY_LUCENE_AUTO_COMPLETE));
        return new LuceneIndexAutoCompleteQueryPlan(getIndexName(), (LuceneScanParameters)newIndexScanParameters, getFetchIndexRecords(), reverse, getPlanOrderingKey(), getStoredFields());
    }
}
