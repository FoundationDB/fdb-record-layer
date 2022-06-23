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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.lucene.LuceneAnalyzerWrapper;
import com.apple.foundationdb.record.lucene.LuceneIndexOptions;
import com.apple.foundationdb.record.lucene.LuceneIndexTypes;
import com.apple.foundationdb.record.lucene.LuceneLogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.common.annotations.VisibleForTesting;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.MergeTrigger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A transaction-scoped manager of {@link FDBDirectory} objects. For a single transaction, all {@link FDBDirectory}
 * objects should be created through this manager. This allows for cached data (like the block cache or file
 * list cache) for a single directory to persist across different operations (e.g., different queries) conducted
 * in the same transaction.
 */
@API(API.Status.INTERNAL)
public class FDBDirectoryManager implements AutoCloseable {
    @VisibleForTesting
    @Nonnull
    public static final Tuple AUTO_COMPLETE_SUFFIX = Tuple.from("s");

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

    private FDBDirectoryWrapper getDirectoryWrapper(@Nullable Tuple groupingKey) {
        final Tuple mapKey = groupingKey == null ? TupleHelpers.EMPTY : groupingKey;
        return createdDirectories.computeIfAbsent(mapKey, key -> new FDBDirectoryWrapper(state, key, mergeDirectoryCount));
    }

    @Nonnull
    public FDBDirectory getDirectory(@Nullable Tuple groupingKey) {
        return getDirectoryWrapper(groupingKey).getDirectory();
    }

    public IndexReader getIndexReader(@Nullable Tuple groupingKey) throws IOException {
        return getDirectoryWrapper(groupingKey).getReader();
    }

    @Nonnull
    public IndexWriter getIndexWriter(@Nullable Tuple groupingKey, @Nonnull LuceneAnalyzerWrapper analyzerWrapper) throws IOException {
        return getDirectoryWrapper(groupingKey).getWriter(analyzerWrapper);
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
        final AtomicInteger luceneMergeCount = new AtomicInteger();
        state.store.getRecordMetaData().getAllIndexes().stream().filter(i -> LuceneIndexTypes.LUCENE.equals(i.getType())).forEach(i -> {
            if (i.getBooleanOption(LuceneIndexOptions.AUTO_COMPLETE_ENABLED, false)) {
                // Auto-complete has its separate directory to merge
                luceneMergeCount.getAndAdd(2);
            } else {
                luceneMergeCount.incrementAndGet();
            }
        });
        return luceneMergeCount.get();
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
