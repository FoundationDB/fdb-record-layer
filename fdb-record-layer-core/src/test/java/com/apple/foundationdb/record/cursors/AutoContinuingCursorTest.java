/*
 * AutoContinuingCursorTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.cursors;

import com.apple.foundationdb.FDBError;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseRunner;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBTestBase;
import com.apple.test.Tags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test for {@link AutoContinuingCursor}.
 */
@Tag(Tags.RequiresFDB)
public class AutoContinuingCursorTest extends FDBTestBase {
    private FDBDatabase database;

    @BeforeEach
    public void getDatabase() {
        database = FDBDatabaseFactory.instance().getDatabase();
    }

    private static final List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

    private void testAutoContinuingCursorGivenCursorGenerator(
            BiFunction<FDBRecordContext, byte[], RecordCursor<Integer>> nextCursorGenerator,
            List<Integer> expectedList) {
        testAutoContinuingCursorGivenCursorGenerator(nextCursorGenerator, 0, expectedList);
    }

    private void testAutoContinuingCursorGivenCursorGenerator(
            BiFunction<FDBRecordContext, byte[], RecordCursor<Integer>> nextCursorGenerator,
            int retryAttempts,
            List<Integer> expectedList) {
        try (FDBDatabaseRunner runner = database.newRunner()) {
            RecordCursor<Integer> cursor = new AutoContinuingCursor<>(runner, nextCursorGenerator, retryAttempts);

            List<Integer> returnedList = cursor.asList().join();
            assertEquals(expectedList, returnedList);
        }
    }

    @Test
    void testAutoContinuingCursorSimple() {
        testAutoContinuingCursorGivenCursorGenerator((context, continuation) ->
                        new ListCursor<>(list, continuation).limitRowsTo(3),
                list);
    }

    @Test
    void testAutoContinuingCursorWhenSomeGeneratedCursorsNeverHaveNext() {
        // This underlying cursor may not produce any item in one transaction. AutoContinuingCursor is expected to make
        // progress until it is truly exhausted.
        testAutoContinuingCursorGivenCursorGenerator((context, continuation) ->
                        new ListCursor<>(list, continuation).limitRowsTo(2).filter(item -> item % 3 == 0),
                Arrays.asList(3, 6, 9)
        );
    }

    @Test
    void testContinuesOnRetryableException() {
        final AtomicInteger iteration = new AtomicInteger(0);

        testAutoContinuingCursorGivenCursorGenerator((context, continuation) ->
                new TestingListCursor<>(list, continuation, () -> {
                    if (iteration.incrementAndGet() % 3 == 0) {
                        CompletableFuture<Void> failedFuture = new CompletableFuture<>();
                        failedFuture.completeExceptionally(new FDBException("transaction_too_old", FDBError.TRANSACTION_TOO_OLD.code()));
                        return failedFuture;
                    }
                    return AsyncUtil.DONE;
                }), 1, list);
    }

    @Test
    void testRetryableMaxAttempts() {
        final AtomicInteger exceptionCount = new AtomicInteger(0);

        CompletionException e = assertThrows(CompletionException.class, () ->
            testAutoContinuingCursorGivenCursorGenerator((context, continuation) ->
                    new TestingListCursor<>(list, continuation, () -> {
                        exceptionCount.incrementAndGet();
                        CompletableFuture<Void> failedFuture = new CompletableFuture<>();
                        failedFuture.completeExceptionally(new FDBException("transaction_too_old", FDBError.TRANSACTION_TOO_OLD.code()));
                        return failedFuture;
                    }), 3, list));
        assertEquals(((FDBException) e.getCause()).getCode(), FDBError.TRANSACTION_TOO_OLD.code());
        assertEquals(exceptionCount.get(), 4);
    }

    private static class TestingListCursor<T> extends ListCursor<T> {
        private final Supplier<CompletableFuture<Void>> pollOnNext;

        public TestingListCursor(@Nonnull final List<T> list, final byte[] continuation,
                                 Supplier<CompletableFuture<Void>> pollOnNext) {
            super(list, continuation);
            this.pollOnNext = pollOnNext;
        }

        @Nonnull
        @Override
        public CompletableFuture<RecordCursorResult<T>> onNext() {
            return pollOnNext.get().thenCompose(vignore -> super.onNext());
        }
    }
}
