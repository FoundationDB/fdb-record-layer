/*
 * FallbackCursorTest.java
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

package com.apple.foundationdb.record.cursors;

import com.apple.foundationdb.record.ByteArrayContinuation;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.test.TestExecutors;
import com.apple.foundationdb.util.LoggableException;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link FallbackCursor}.
 */
public class FallbackCursorTest {

    @Test
    public void testFallBackCursorNoFailure() throws Exception {
        List<Integer> integers = List.of(1, 2, 3);
        ListCursor<Integer> inner = new ListCursor<>(integers, null);
        FallbackCursor<Integer> classUnderTest = new FallbackCursor<>(inner, (lastResult) -> {
            throw new RuntimeException("This should not be thrown");
        });

        List<Integer> result = classUnderTest.asList().get();
        assertEquals(3, result.size());
        assertEquals(integers, result);
    }

    @Test
    public void testPrimaryCursorImmediateFailure() throws Exception {
        RecordCursor<Integer> inner = new FailingCursor(0);
        List<Integer> integers = List.of(1, 2, 3);
        RecordCursor<Integer> fallbackCursor = new ListCursor<>(integers, null);
        FallbackCursor<Integer> classUnderTest = new FallbackCursor<>(inner, (lastResult) -> {
            assertNull(lastResult);
            return fallbackCursor;
        });

        List<Integer> result = classUnderTest.asList().get();
        assertEquals(3, result.size());
        assertEquals(integers, result);
    }

    @Test
    public void testPrimaryCursorFailureAfterFewResultsNotSupported() throws Exception {
        RecordCursor<Integer> inner = new FailingCursor(2);
        List<Integer> integers = List.of(1, 2, 3);
        RecordCursor<Integer> fallbackCursor = new ListCursor<>(integers, null);
        FallbackCursor<Integer> classUnderTest = new FallbackCursor<>(inner, (lastResult) -> {
            // simulate a cursor that cannot continue after a result has been returned
            if (lastResult != null) {
                throw new FallbackCursor.FallbackExecutionFailedException("Cannot fallback to alternate cursor since inner already produced a record", null);
            }
            return fallbackCursor;
        });

        Exception ex = assertThrows(ExecutionException.class, () -> classUnderTest.asList().get());
        assertTrue(ex.getCause() instanceof RecordCoreException);
        // in this case, the cursor returns the fallback cursor's onNext() directly, which, if calling get() on,
        // will fail immediately, not going through the wrapping mechanism of the cursor.
        assertEquals("Cannot fallback to alternate cursor since inner already produced a record", ex.getCause().getMessage());
    }

    @Test
    public void testPrimaryCursorFailureAfterFewResultsIsSupported() throws Exception {
        RecordCursor<Integer> inner = new FailingCursor(2);
        List<Integer> integers = List.of(1, 2, 3);
        RecordCursor<Integer> fallbackCursor = new ListCursor<>(integers, null);
        FallbackCursor<Integer> classUnderTest = new FallbackCursor<>(inner, (lastResult) -> fallbackCursor);

        List<Integer> result = classUnderTest.asList().get();
        assertEquals(5, result.size());
        assertEquals(List.of(0, 1, 1, 2, 3), result);
    }

    @Test
    public void testFallBackCursorFailureFailsImmediately() throws Exception {
        RecordCursor<Integer> inner = new FailingCursor(0);
        RecordCursor<Integer> fallbackCursor = new FailingCursor(0);
        FallbackCursor<Integer> classUnderTest = new FallbackCursor<>(inner, (lastResult) -> fallbackCursor);

        Exception ex = assertThrows(ExecutionException.class, () -> classUnderTest.asList().get());
        // in this case, the cursor returns the fallback cursor's onNext() directly, which, if calling get() on,
        // will fail immediately, not going through the wrapping mechanism of the cursor.
        assertTrue(ex.getCause() instanceof RecordCoreException);
    }

    @Test
    public void testFallBackCursorFailureFailsAfterFewResults() throws Exception {
        RecordCursor<Integer> inner = new FailingCursor(0);
        RecordCursor<Integer> fallbackCursor = new FailingCursor(3);
        FallbackCursor<Integer> classUnderTest = new FallbackCursor<>(inner, (lastResult) -> fallbackCursor);

        Exception ex = assertThrows(ExecutionException.class, () -> classUnderTest.asList().get());
        assertTrue(ex.getCause() instanceof RecordCoreException);
        assertEquals("Fallback cursor failed, cannot fallback again",
                ((LoggableException)(ex.getCause())).getLogInfo().get("fallback_failed"));
    }

    /**
     * A cursor that returns a number of Integer values and then fails.
     */
    private static class FailingCursor implements RecordCursor<Integer> {
        @Nonnull
        private final Executor executor;
        @Nonnull
        private final List<Integer> list;
        private int nextPosition = 0; // position of the next value to return
        private boolean closed = false;

        public FailingCursor(int numOfElementsBeforeFailure) {
            executor = TestExecutors.defaultThreadPool();
            list = listOfLength(numOfElementsBeforeFailure);
        }

        @Nonnull
        @Override
        public CompletableFuture<RecordCursorResult<Integer>> onNext() {
            try {
                return CompletableFuture.completedFuture(getNext());
            } catch (Exception ex) {
                return CompletableFuture.failedFuture(ex);
            }
        }

        @Nonnull
        @Override
        public RecordCursorResult<Integer> getNext() {
            RecordCursorResult<Integer> nextResult;
            if (nextPosition < list.size()) {
                nextResult = RecordCursorResult.withNextValue(list.get(nextPosition), ByteArrayContinuation.fromInt(nextPosition + 1));
                nextPosition++;
            } else {
                throw new RecordCoreException("Failing");
            }
            return nextResult;
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public boolean isClosed() {
            return closed;
        }

        @Override
        public boolean accept(@Nonnull RecordCursorVisitor visitor) {
            visitor.visitEnter(this);
            return visitor.visitLeave(this);
        }

        @Override
        @Nonnull
        public Executor getExecutor() {
            return executor;
        }

        @Nonnull
        private static List<Integer> listOfLength(final int length) {
            return IntStream.range(0, length).boxed().collect(Collectors.toList());
        }
    }
}
