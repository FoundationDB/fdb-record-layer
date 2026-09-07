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

import org.jspecify.annotations.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Sort records in memory up to a specified limit, then return them in order.
 * @param <K> type of key
 * @param <V> type of value
 */
@API(API.Status.EXPERIMENTAL)
public class MemorySortCursor<K, V> implements RecordCursor<V> {
    private final RecordCursor<V> inputCursor;
    private final MemoryScratchpad<K, V, ? extends Map<K, V>> scratchpad;
    private final MemorySortAdapter<K, V> adapter;
    @Nullable
    private final StoreTimer timer;
    @Nullable
    private K minimumKey;

    @Nullable
    private RecordCursorContinuation inputContinuation;
    @Nullable
    private Iterator<Map.Entry<K, V>> iterator;
    
    private MemorySortCursor(final MemorySortAdapter<K, V> adapter,
                             MemoryScratchpad<K, V, ? extends Map<K, V>> scratchpad,
                             RecordCursor<V> inputCursor, @Nullable StoreTimer timer, @Nullable K minimumKey) {
        this.inputCursor = inputCursor;
        this.scratchpad = scratchpad;
        this.adapter = adapter;
        this.timer = timer;
        this.minimumKey = minimumKey;
    }

    @Override
    public CompletableFuture<RecordCursorResult<V>> onNext() {
        if (iterator != null) {
            return CompletableFuture.completedFuture(nextFromIterator());
        }
        return scratchpad.load(inputCursor, minimumKey).thenApply(loadResult -> {
            inputContinuation = loadResult.getSourceContinuation();
            if (loadResult.getSourceNoNextReason().isOutOfBand()) {
                // The input cursor did not complete; we must save the sorter state in the continuation so can pick up after.
                final MemorySortCursorContinuation<K, V> continuation =
                        new MemorySortCursorContinuation<>(adapter, false, scratchpad.getMap().values(),
                                loadResult.getNextMinimumKey(),
                                inputContinuation);

                return RecordCursorResult.withoutNextValue(continuation, loadResult.getSourceNoNextReason());
            }
            // Loaded all the records into the sorter, start returning them.
            iterator = scratchpad.getMap().entrySet().iterator();
            return nextFromIterator();
        });
    }

    private RecordCursorResult<V> nextFromIterator() {
        final long startTime = System.nanoTime();
        final RecordCursorContinuation currentInputContinuation =
                Objects.requireNonNull(inputContinuation, "inputContinuation must be set before nextFromIterator is called");
        final Iterator<Map.Entry<K, V>> currentIterator =
                Objects.requireNonNull(iterator, "iterator must be set before nextFromIterator is called");
        if (currentIterator.hasNext()) {
            // Return a sorted record.
            Map.Entry<K, V> next = currentIterator.next();
            minimumKey = next.getKey();
            Collection<V> remainingRecords = scratchpad.tailValues(minimumKey);
            MemorySortCursorContinuation<K, V> continuation = new MemorySortCursorContinuation<>(adapter, false, remainingRecords, minimumKey, currentInputContinuation);
            RecordCursorResult<V> result = RecordCursorResult.withNextValue(next.getValue(), continuation);
            if (timer != null) {
                timer.recordSinceNanoTime(SortEvents.Events.MEMORY_SORT_LOAD_RECORD, startTime);
            }
            return result;
        }
        // If filling the sorter didn't reach the limit, none were discarded and all the records in it must be all the records period.
        boolean exhausted = scratchpad.getMap().size() < adapter.getMaxRecordCountInMemory();
        MemorySortCursorContinuation<K, V> continuation = new MemorySortCursorContinuation<>(adapter, exhausted, Collections.emptyList(), minimumKey, currentInputContinuation);
        return RecordCursorResult.withoutNextValue(continuation, exhausted ? NoNextReason.SOURCE_EXHAUSTED : NoNextReason.RETURN_LIMIT_REACHED);
    }

    @Override
    public void close() {
        inputCursor.close();
    }

    @Override
    public boolean isClosed() {
        return inputCursor.isClosed();
    }

    @Override
    public Executor getExecutor() {
        return inputCursor.getExecutor();
    }

    @Override
    public boolean accept(final RecordCursorVisitor visitor) {
        if (visitor.visitEnter(this)) {
            inputCursor.accept(visitor);
        }
        return visitor.visitLeave(this);
    }

    @SuppressWarnings({"PMD.CloseResource", "NullAway"}) // RecordCursorContinuation#toBytes() is legitimately
    // @Nullable (a null byte[] commonly means "start from the beginning"), but NullAway/JSpecify does not
    // reliably track @Nullable on array (byte[]) type parameters of a generic Function, so a null continuation
    // here is flagged as mismatched even though inputCursorFunction implementations (e.g.
    // RecordQueryPlan#executePlan) accept it.
    public static <K, V, M extends Map<K, V>> MemorySortCursor<K, V> create(MemorySortAdapter<K, V> adapter,
                                                                            Function<byte[], RecordCursor<V>> inputCursorFunction,
                                                                            @Nullable StoreTimer timer,
                                                                            BiFunction<MemorySortAdapter<K, V>, StoreTimer, MemoryScratchpad<K, V, M>> scratchPadCreator,
                                                                            @Nullable byte[] continuation) {
        final MemorySortCursorContinuation<K, V> parsedContinuation = MemorySortCursorContinuation.from(continuation, adapter);
        final RecordCursor<V> inputCursor = inputCursorFunction.apply(parsedContinuation.getChild().toBytes());
        final MemoryScratchpad<K, V, M> scratchpad = scratchPadCreator.apply(adapter, timer);
        for (V record : parsedContinuation.getRecords()) {
            scratchpad.addValue(record);
        }
        final K minimumKey = parsedContinuation.getMinimumKey();
        return new MemorySortCursor<>(adapter, scratchpad, inputCursor, timer, minimumKey);
    }

    @SuppressWarnings("PMD.CloseResource")
    public static <K, V> MemorySortCursor<K, V> createSort(MemorySortAdapter<K, V> adapter,
                                                           Function<byte[], RecordCursor<V>> inputCursorFunction,
                                                           @Nullable StoreTimer timer,
                                                           @Nullable byte[] continuation) {
        return create(adapter, inputCursorFunction, timer, MemorySorter::new, continuation);
    }

    @SuppressWarnings("PMD.CloseResource")
    public static <K, V> MemorySortCursor<K, V> createDam(MemorySortAdapter<K, V> adapter,
                                                          Function<byte[], RecordCursor<V>> inputCursorFunction,
                                                          @Nullable StoreTimer timer,
                                                          @Nullable byte[] continuation) {
        return create(adapter, inputCursorFunction, timer, MemoryDam::new, continuation);
    }
}
