/*
 * FileSortCursor.java
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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.provider.common.StoreTimer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * Sort records in memory and / or in files, then return them in order.
 * @param <K> type of key
 * @param <V> type of value
 */
@API(API.Status.EXPERIMENTAL)
public class FileSortCursor<K, V> implements RecordCursor<V> {
    @Nonnull
    private final RecordCursor<V> inputCursor;
    @Nonnull
    private final FileSorter<K, V> sorter;
    @Nonnull
    private final FileSortAdapter<K, V> adapter;
    @Nullable
    private final StoreTimer timer;
    private final int skip;
    private final int limit;

    private RecordCursorContinuation inputContinuation;
    private Iterator<Map.Entry<K, V>> inMemoryIterator;
    private int inMemoryPosition;
    @Nullable
    private K minimumKey;
    private SortedFileReader<V> fileReader;

    private FileSortCursor(@Nonnull FileSortAdapter<K, V> adapter, @Nonnull FileSorter<K, V> sorter,
                           @Nonnull RecordCursor<V> inputCursor, @Nullable StoreTimer timer,
                           int skip, int limit) {
        this.inputCursor = inputCursor;
        this.sorter = sorter;
        this.adapter = adapter;
        this.timer = timer;
        this.skip = skip;
        this.limit = limit;
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<V>> onNext() {
        if (inMemoryIterator != null) {
            return CompletableFuture.completedFuture(nextFromIterator());
        }
        if (fileReader != null) {
            return CompletableFuture.completedFuture(nextFromReader());
        }
        return sorter.load(inputCursor).thenApply(loadResult -> {
            inputContinuation = loadResult.getSourceContinuation();
            if (loadResult.getSourceNoNextReason().isOutOfBand()) {
                // The input cursor did not complete; we must save the sorter state in the continuation so can pick up after.
                Collection<V> inMemoryRecords = sorter.getMapSorter().getMap().values();
                FileSortCursorContinuation<K, V> continuation = new FileSortCursorContinuation<>(adapter, false, true, inMemoryRecords, sorter.getFiles(), inputContinuation, 0, 0);
                return RecordCursorResult.withoutNextValue(continuation, loadResult.getSourceNoNextReason());
            }
            // Loaded all the records into the sorter, start returning them.
            if (loadResult.isInMemory()) {
                inMemoryIterator = sorter.getMapSorter().getMap().entrySet().iterator();
                for (int i = 0; i < skip; i++) {
                    if (!inMemoryIterator.hasNext()) {
                        break;
                    }
                    inMemoryIterator.next();
                }
                return nextFromIterator();
            }
            try {
                fileReader = new SortedFileReader<>(sorter.getFiles().get(0), adapter, timer, skip, limit);
            } catch (IOException | GeneralSecurityException ex) {
                throw new RecordCoreException(ex);
            }
            return nextFromReader();
        });
    }

    @Nonnull
    private RecordCursorResult<V> nextFromIterator() {
        if (inMemoryPosition >= limit) {
            FileSortCursorContinuation<K, V> continuation = new FileSortCursorContinuation<>(adapter, true, false, Collections.emptyList(), Collections.emptyList(), inputContinuation, inMemoryPosition, 0);
            return RecordCursorResult.withoutNextValue(continuation, NoNextReason.RETURN_LIMIT_REACHED);
        }
        if (inMemoryIterator.hasNext()) {
            // Return a sorted record.
            Map.Entry<K, V> next = inMemoryIterator.next();
            minimumKey = next.getKey();
            inMemoryPosition++;
            Collection<V> remainingRecords = sorter.getMapSorter().getMap().tailMap(minimumKey, false).values();
            FileSortCursorContinuation<K, V> continuation = new FileSortCursorContinuation<>(adapter, false, false, remainingRecords, sorter.getFiles(), inputContinuation, inMemoryPosition, 0);
            return RecordCursorResult.withNextValue(next.getValue(), continuation);
        }
        FileSortCursorContinuation<K, V> continuation = new FileSortCursorContinuation<>(adapter, true, false, Collections.emptyList(), Collections.emptyList(), inputContinuation, inMemoryPosition, 0);
        return RecordCursorResult.withoutNextValue(continuation, NoNextReason.SOURCE_EXHAUSTED);
    }

    @Nonnull
    private RecordCursorResult<V> nextFromReader() {
        @Nullable V record;
        try {
            record = fileReader.read();
        } catch (IOException | GeneralSecurityException ex) {
            throw new RecordCoreException(ex);
        }
        FileSortCursorContinuation<K, V> continuation = new FileSortCursorContinuation<>(adapter, record == null, false, Collections.emptyList(), sorter.getFiles(), inputContinuation, fileReader.getRecordPosition(), fileReader.getFilePosition());
        if (record != null) {
            return RecordCursorResult.withNextValue(record, continuation);
        } else {
            return RecordCursorResult.withoutNextValue(continuation, NoNextReason.SOURCE_EXHAUSTED);
        }
    }

    @Override
    public void close() {
        inputCursor.close();
        try {
            sorter.deleteFiles();
        } catch (IOException ex) {
            throw new RecordCoreException(ex);
        }
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

    public static <K, V> FileSortCursor<K, V> create(@Nonnull FileSortAdapter<K, V> adapter,
                                                     @Nonnull Function<byte[], RecordCursor<V>> inputCursorFunction,
                                                     @Nullable StoreTimer timer,
                                                     @Nullable byte[] continuation, int skip, int limit) {
        final FileSortCursorContinuation<K, V> parsedContinuation = FileSortCursorContinuation.from(continuation, adapter);
        final RecordCursor<V> inputCursor = parsedContinuation.isLoading() ? inputCursorFunction.apply(parsedContinuation.getChild().toBytes()) : RecordCursor.empty();
        final FileSorter<K, V> sorter = new FileSorter<>(adapter, timer, inputCursor.getExecutor());
        for (V record : parsedContinuation.getInMemoryRecords()) {
            sorter.getMapSorter().addValue(record);
        }
        for (File file : parsedContinuation.getFiles()) {
            sorter.getFiles().add(file);
        }
        return new FileSortCursor<>(adapter, sorter, inputCursor, timer, skip, limit);
    }
}
