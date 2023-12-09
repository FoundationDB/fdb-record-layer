/*
 * FDBDirectoryManager.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene.directory;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.KeyRange;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.cursors.ChainedCursor;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.lucene.LuceneAnalyzerWrapper;
import com.apple.foundationdb.record.lucene.LuceneIndexTypes;
import com.apple.foundationdb.record.lucene.LuceneLogMessageKeys;
import com.apple.foundationdb.record.lucene.LucenePartitioner;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.KeyValueCursor;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.MergeTrigger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A transaction-scoped manager of {@link FDBDirectory} objects. For a single transaction, all {@link FDBDirectory}
 * objects should be created through this manager. This allows for cached data (like the block cache or file
 * list cache) for a single directory to persist across different operations (e.g., different queries) conducted
 * in the same transaction.
 */
@API(API.Status.INTERNAL)
public class FDBDirectoryManager implements AutoCloseable {
    @Nonnull
    private final IndexMaintainerState state;
    @Nonnull
    private final Map<Tuple, FDBDirectoryWrapper> createdDirectories;
    private final int mergeDirectoryCount;

    private FDBDirectoryManager(@Nonnull IndexMaintainerState state) {
        this.state = state;
        this.createdDirectories = new ConcurrentHashMap<>();

        this.mergeDirectoryCount = getMergeDirectoryCount(state);
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    public synchronized void close() throws IOException {
        for (FDBDirectoryWrapper directory : createdDirectories.values()) {
            directory.close();
        }
        createdDirectories.clear();
    }

    @SuppressWarnings("PMD.CloseResource")
    public CompletableFuture<Void> mergeIndex(LucenePartitioner partitioner, LuceneAnalyzerWrapper analyzerWrapper) {
        // This function will iterate the grouping keys and explicitly merge each

        final ScanProperties scanProperties = ScanProperties.FORWARD_SCAN.with(
                props -> props.clearState().setReturnedRowLimit(1));

        final Range range = state.indexSubspace.range();
        final KeyRange keyRange = new KeyRange(range.begin, range.end);
        final Subspace subspace = state.indexSubspace;
        final KeyExpression rootExpression = state.index.getRootExpression();

        if (! (rootExpression instanceof GroupingKeyExpression)) {
            mergeIndex(analyzerWrapper, TupleHelpers.EMPTY, partitioner.isPartitioningEnabled() ? 0 : null /* TODO */);
            return AsyncUtil.DONE;
        }
        GroupingKeyExpression expression = (GroupingKeyExpression) rootExpression;
        final int groupingCount = expression.getGroupingCount();

        final RecordCursor<Tuple> cursor = new ChainedCursor<>(
                state.context,
                lastKey -> nextTuple(state.context, subspace, keyRange, lastKey, scanProperties, groupingCount),
                Tuple::pack,
                Tuple::fromBytes,
                null,
                ScanProperties.FORWARD_SCAN);

        return cursor
                .map(tuple ->
                        Tuple.fromItems(tuple.getItems().subList(0, groupingCount)))
                .forEach(groupingKey ->
                        mergeIndex(analyzerWrapper, groupingKey, partitioner.isPartitioningEnabled() ? 0 : null /* TODO */));
    }

    private void mergeIndex(LuceneAnalyzerWrapper analyzerWrapper, Tuple groupingKey, @Nullable Integer partitionId) {
        try {
            getDirectoryWrapper(groupingKey, partitionId, true).mergeIndex(analyzerWrapper);
        } catch (IOException e) {
            throw new RecordCoreStorageException("Lucene mergeIndex failed", e);
        }
    }

    @SuppressWarnings("PMD.CloseResource")
    private static CompletableFuture<Optional<Tuple>> nextTuple(@Nonnull FDBRecordContext context,
                                                                @Nonnull Subspace subspace,
                                                                @Nonnull KeyRange range,
                                                                @Nonnull Optional<Tuple> lastTuple,
                                                                @Nonnull ScanProperties scanProperties,
                                                                int groupingCount) {
        KeyValueCursor.Builder cursorBuilder =
                KeyValueCursor.Builder.withSubspace(subspace)
                        .setContext(context)
                        .setContinuation(null)
                        .setScanProperties(scanProperties);

        if (lastTuple.isPresent()) {
            final byte[] lowKey = subspace.pack(Tuple.fromItems(lastTuple.get().getItems().subList(0, groupingCount)));
            cursorBuilder
                    .setLow(lowKey, EndpointType.RANGE_EXCLUSIVE)
                    .setHigh(range.getHighKey(), range.getHighEndpoint());
        } else {
            cursorBuilder.setContext(context)
                    .setRange(range);
        }

        return cursorBuilder.build().onNext().thenApply(next -> {
            if (next.hasNext()) {
                KeyValue kv = next.get();
                if (kv != null) {
                    return Optional.of(subspace.unpack(kv.getKey()));
                }
            }
            return Optional.empty();
        });
    }

    /**
     * Invalidate directories from the cache if their grouping key begins with a specified prefix.
     * @param prefix the prefix of grouping keys to remove from the cache
     */
    public void invalidatePrefix(@Nonnull Tuple prefix) {
        final Iterator<Map.Entry<Tuple, FDBDirectoryWrapper>> iterator = createdDirectories.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Tuple, FDBDirectoryWrapper> item = iterator.next();
            if (TupleHelpers.isPrefix(prefix, item.getKey())) {
                try {
                    // Close the directory and associated readers/writers
                    item.getValue().close();
                } catch (IOException e) {
                    throw new RecordCoreStorageException("unable to close index writer", e);
                }
                iterator.remove();
            }
        }
    }

    private FDBDirectoryWrapper getDirectoryWrapper(@Nullable Tuple groupingKey, @Nullable Integer partitionId) {
        return getDirectoryWrapper(groupingKey, partitionId, false);
    }

    private FDBDirectoryWrapper getDirectoryWrapper(@Nullable Tuple groupingKey, @Nullable Integer partitionId, boolean useAgilityContext) {
        Tuple mapKey = groupingKey == null ? TupleHelpers.EMPTY : groupingKey;
        if (partitionId != null) {
            mapKey = mapKey.add(LucenePartitioner.PARTITION_DATA_SUBSPACE).add(partitionId);
        }
        return createdDirectories.computeIfAbsent(mapKey, key -> new FDBDirectoryWrapper(state, key, mergeDirectoryCount, useAgilityContext));
    }

    @Nonnull
    public FDBDirectory getDirectory(@Nullable Tuple groupingKey, @Nullable Integer partitionId) {
        return getDirectoryWrapper(groupingKey, partitionId).getDirectory();
    }

    public IndexReader getIndexReader(@Nullable Tuple groupingKey, @Nullable Integer partitionId) throws IOException {
        return getDirectoryWrapper(groupingKey, partitionId).getReader();
    }

    @Nonnull
    public IndexWriter getIndexWriter(@Nullable Tuple groupingKey, @Nullable Integer partitionId, @Nonnull LuceneAnalyzerWrapper analyzerWrapper) throws IOException {
        return getDirectoryWrapper(groupingKey, partitionId).getWriter(analyzerWrapper);
    }

    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    public static FDBDirectoryManager getManager(@Nonnull IndexMaintainerState state) {
        synchronized (state.context) {
            FDBRecordContext context = state.context;
            FDBDirectoryManager existing = context.getInSession(state.indexSubspace, FDBDirectoryManager.class);
            if (existing != null) {
                return existing;
            }
            FDBDirectoryManager newManager = new FDBDirectoryManager(state);
            context.putInSessionIfAbsent(state.indexSubspace, newManager);
            context.addCommitCheck(() -> {
                try {
                    newManager.close();
                } catch (IOException e) {
                    throw new RecordCoreStorageException("unable to close directories", e);
                }
                return AsyncUtil.DONE;
            });
            return newManager;
        }
    }

    private int getMergeDirectoryCount(@Nonnull IndexMaintainerState state) {
        return Math.toIntExact(state.store
                .getRecordMetaData()
                .getAllIndexes()
                .stream()
                .filter(i -> LuceneIndexTypes.LUCENE.equals(i.getType()))
                .count());
    }

    public static String getMergeLogMessage(@Nonnull MergeScheduler.MergeSource mergeSource, @Nonnull MergeTrigger trigger,
                                            @Nonnull IndexMaintainerState state, @Nonnull String logMessage) {
        return KeyValueLogMessage.of(logMessage,
                LuceneLogMessageKeys.MERGE_SOURCE, mergeSource,
                LuceneLogMessageKeys.MERGE_TRIGGER, trigger,
                LogMessageKeys.INDEX_NAME, state.index.getName(),
                LogMessageKeys.INDEX_SUBSPACE, state.indexSubspace);
    }
}
