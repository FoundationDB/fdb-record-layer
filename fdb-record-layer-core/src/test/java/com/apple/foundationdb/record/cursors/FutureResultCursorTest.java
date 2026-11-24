/*
 * FutureResultCursorTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link FutureResultCursor}.
 */
class FutureResultCursorTest {
    private static final RecordCursorContinuation CONTINUATION = ByteArrayContinuation.fromNullable(new byte[] {1});

    @Nonnull
    private static FutureResultCursor<String> cursorOf(@Nonnull final RecordCursorResult<String> result) {
        return new FutureResultCursor<>(ForkJoinPool.commonPool(), CompletableFuture.completedFuture(result));
    }

    /**
     * Tests that a cursor whose result has a value returns that value, and is exhausted afterwards.
     */
    @Test
    void testFutureResultCursor1() {
        try (FutureResultCursor<String> cursor = cursorOf(RecordCursorResult.withNextValue("a", CONTINUATION))) {
            final RecordCursorResult<String> first = cursor.getNext();
            assertThat(first.hasNext()).isTrue();
            assertThat(first.get()).isEqualTo("a");
            assertThat(first.getContinuation()).isSameAs(CONTINUATION);

            final RecordCursorResult<String> second = cursor.getNext();
            assertThat(second.hasNext()).isFalse();
            assertThat(second.getNoNextReason()).isEqualTo(RecordCursor.NoNextReason.SOURCE_EXHAUSTED);
            assertThat(second.getContinuation().isEnd()).isTrue();
        }
    }

    /**
     * Tests that a cursor whose result has no value keeps returning that result, with its reason and continuation.
     */
    @Test
    void testFutureResultCursor2() {
        try (FutureResultCursor<String> cursor =
                     cursorOf(RecordCursorResult.withoutNextValue(CONTINUATION, RecordCursor.NoNextReason.SCAN_LIMIT_REACHED))) {
            for (int i = 0; i < 2; i++) {
                final RecordCursorResult<String> result = cursor.getNext();
                assertThat(result.hasNext()).isFalse();
                assertThat(result.getNoNextReason()).isEqualTo(RecordCursor.NoNextReason.SCAN_LIMIT_REACHED);
                assertThat(result.getContinuation()).isSameAs(CONTINUATION);
            }
        }
    }

    /**
     * Tests that closing the cursor is reflected by {@code isClosed()}, and that it has the given executor.
     */
    @Test
    void testFutureResultCursor3() {
        final FutureResultCursor<String> cursor = cursorOf(RecordCursorResult.exhausted());
        assertThat(cursor.isClosed()).isFalse();
        assertThat(cursor.getExecutor()).isSameAs(ForkJoinPool.commonPool());
        cursor.close();
        assertThat(cursor.isClosed()).isTrue();
    }
}
