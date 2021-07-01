/*
 * MemorySortCursor.java
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
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.provider.common.StoreTimer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * Sort records in memory up to a specified limit, then return them in order.
 * @param <K> type of key
 * @param <V> type of value
 */
@API(API.Status.EXPERIMENTAL)
public class MemorySortCursor<K, V> implements RecordCursor<V> {
    @Nonnull
    private final RecordCursor<V> inputCursor;
    @Nonnull
    private final MemorySorter<K, V> sorter;
    @Nonnull
    private final MemorySortAdapter<K, V> adapter;
    @Nullable
    private final StoreTimer timer;
    @Nullable
    private K minimumKey;

    private RecordCursorContinuation inputContinuation;
    private Iterator<Map.Entry<K, V>> iterator;
    
    private MemorySortCursor(@Nonnull final MemorySortAdapter<K, V> adapter, @Nonnull MemorySorter<K, V> sorter,
                             @Nonnull RecordCursor<V> inputCursor, @Nullable StoreTimer timer, @Nullable K minimumKey) {
        this.inputCursor = inputCursor;
        this.sorter = sorter;
        this.adapter = adapter;
        this.timer = timer;
        this.minimumKey = minimumKey;
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<V>> onNext() {
        if (iterator != null) {
            return CompletableFuture.completedFuture(nextFromIterator());
        }
        return sorter.load(inputCursor, minimumKey).thenApply(loadResult -> {
            inputContinuation = loadResult.getSourceContinuation();
            if (loadResult.getSourceNoNextReason().isOutOfBand()) {
                // The input cursor did not complete; we must save the sorter state in the continuation so can pick up after.
                MemorySortCursorContinuation<K, V> continuation = new MemorySortCursorContinuation<>(adapter, false, sorter.getMap().values(), minimumKey, inputContinuation);
                return RecordCursorResult.withoutNextValue(continuation, loadResult.getSourceNoNextReason());
            }
            // Loaded all the records into the sorter, start returning them.
            iterator = sorter.getMap().entrySet().iterator();
            return nextFromIterator();
        });
    }

    @Nonnull
    private RecordCursorResult<V> nextFromIterator() {
        final long startTime = System.nanoTime();
        if (iterator.hasNext()) {
            // Return a sorted record.
            Map.Entry<K, V> next = iterator.next();
            minimumKey = next.getKey();
            Collection<V> remainingRecords = sorter.getMap().tailMap(minimumKey, false).values();
            MemorySortCursorContinuation<K, V> continuation = new MemorySortCursorContinuation<>(adapter, false, remainingRecords, minimumKey, inputContinuation);
            RecordCursorResult<V> result = RecordCursorResult.withNextValue(next.getValue(), continuation);
            if (timer != null) {
                timer.recordSinceNanoTime(SortEvents.Events.MEMORY_SORT_LOAD_RECORD, startTime);
            }
            return result;
        }
        // If filling the sorter didn't reach the limit, none were discarded and all the records in it must be all the records period.
        boolean exhausted = sorter.getMap().size() < adapter.getMaxMapSize();
        MemorySortCursorContinuation<K, V> continuation = new MemorySortCursorContinuation<>(adapter, exhausted, Collections.emptyList(), minimumKey, inputContinuation);
        return RecordCursorResult.withoutNextValue(continuation, exhausted ? NoNextReason.SOURCE_EXHAUSTED : NoNextReason.RETURN_LIMIT_REACHED);
    }

    @Override
    public void close() {
        inputCursor.close();
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return inputCursor.getExecutor();
    }

    @Override
    public boolean accept(@Nonnull final RecordCursorVisitor visitor) {
        if (visitor.visitEnter(this)) {
            inputCursor.accept(visitor);
        }
        return visitor.visitLeave(this);
    }

    public static <K, V> MemorySortCursor<K, V> create(@Nonnull MemorySortAdapter<K, V> adapter,
                                                       @Nonnull Function<byte[], RecordCursor<V>> inputCursorFunction,
                                                       @Nullable StoreTimer timer,
                                                       @Nullable byte[] continuation) {
        final MemorySortCursorContinuation<K, V> parsedContinuation = MemorySortCursorContinuation.from(continuation, adapter);
        final RecordCursor<V> inputCursor = inputCursorFunction.apply(parsedContinuation.getChild().toBytes());
        final MemorySorter<K, V> sorter = new MemorySorter<>(adapter, timer);
        for (V record : parsedContinuation.getRecords()) {
            sorter.addValue(record);
        }
        final K minimumKey = parsedContinuation.getMinimumKey();
        return new MemorySortCursor<>(adapter, sorter, inputCursor, timer, minimumKey);
    }
}
