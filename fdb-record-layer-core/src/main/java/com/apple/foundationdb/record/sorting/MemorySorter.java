/*
 * TreeMapSorter.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.sorting;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.provider.common.StoreTimer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

/**
 * Collect keyed values into a {@link TreeMap} so that they end up sorted.
 * @param <K> type of key
 * @param <V> type of value
 */
@API(API.Status.EXPERIMENTAL)
public class MemorySorter<K, V> {
    @Nonnull
    private final NavigableMap<K, V> map;
    @Nonnull
    private final MemorySortAdapter<K, V> adapter;
    @Nullable
    private final StoreTimer timer;

    private LoadResult loadResult;

    public MemorySorter(@Nonnull MemorySortAdapter<K, V> adapter, @Nullable StoreTimer timer) {
        this.adapter = adapter;
        this.map = new TreeMap<>(adapter);
        this.timer = timer;
    }

    @Nonnull
    public NavigableMap<K, V> getMap() {
        return map;
    }

    @Nonnull
    public MemorySortAdapter<K, V> getAdapter() {
        return adapter;
    }

    public void addKeyValue(K key, V value) {
        map.put(key, value);
    }

    public void addValue(V value) {
        addKeyValue(adapter.generateKey(value), value);
    }

    /** How the {@code sizeLimit} is interpreted for {@link #load}. */
    public enum SizeLimitMode { 
        /** Map retains top {@code sizeLimit} values and discards additional values. */
        DISCARD,
        /** Map retains {@code sizeLimit} values and then stops. */
        STOP
    }

    /** The result of {@link #load}. */
    public static class LoadResult {
        private final boolean full;
        @Nonnull
        private final RecordCursorContinuation sourceContinuation;
        @Nullable
        private final RecordCursor.NoNextReason sourceNoNextReason;

        public LoadResult(final boolean full, @Nonnull RecordCursorContinuation sourceContinuation, @Nullable RecordCursor.NoNextReason sourceNoNextReason) {
            this.full = full;
            this.sourceContinuation = sourceContinuation;
            this.sourceNoNextReason = sourceNoNextReason;
        }

        public boolean isFull() {
            return full;
        }

        @Nonnull
        public RecordCursorContinuation getSourceContinuation() {
            return sourceContinuation;
        }

        @Nullable
        public RecordCursor.NoNextReason getSourceNoNextReason() {
            return sourceNoNextReason;
        }
    }

    /**
     * Load and sort records from a cursor.
     * @param source source cursor
     * @param minimumKey ignore cursor entries that are not greater than this key
     * @return the reason the load stopped
     */
    public CompletableFuture<LoadResult> load(@Nonnull RecordCursor<V> source, @Nullable K minimumKey) {
        loadResult = null;
        return AsyncUtil.whileTrue(() -> source.onNext().thenApply(sourceResult -> {
            if (!sourceResult.hasNext()) {
                loadResult = new LoadResult(false, sourceResult.getContinuation(), sourceResult.getNoNextReason());
                return false;
            }
            final long startTime = System.nanoTime();
            try {
                if (minimumKey == null) {
                    addValue(sourceResult.get());
                } else {
                    V value = sourceResult.get();
                    K key = adapter.generateKey(value);
                    if (adapter.compare(key, minimumKey) > 0) {
                        addKeyValue(key, value);
                    } else {
                        return true;
                    }
                }
                if (map.size() <= adapter.getMaxMapSize()) {
                    return true;
                }
                switch (adapter.getSizeLimitMode()) {
                    case DISCARD:
                        map.pollLastEntry();
                        return true;
                    case STOP:
                        // TODO: Need some more NoNextReason's
                        loadResult = new LoadResult(true, sourceResult.getContinuation(), RecordCursor.NoNextReason.SCAN_LIMIT_REACHED);
                        return false;
                    default:
                        throw new RecordCoreArgumentException("Unknown size limit mode: " + adapter.getSizeLimitMode());
                }
            } finally {
                if (timer != null) {
                    timer.recordSinceNanoTime(SortEvents.Events.MEMORY_SORT_STORE_RECORD, startTime);
                }
            }
        })).thenApply(vignore -> loadResult);
    }
}
