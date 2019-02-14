/*
 * TimeLimitedCursorTest.java
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

import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests of the {@link TimeLimitedCursor}.
 */
public class TimeLimitedCursorTest {

    @Test
    public void exhaustedWhenTimeLimited() throws ExecutionException, InterruptedException {
        TimeLimitedCursor<Object> cursor = new TimeLimitedCursor<>(RecordCursor.empty(), System.currentTimeMillis() - 200, 100);
        RecordCursorResult<Object> cursorResult = cursor.onNext().get();
        assertThat(cursorResult.hasNext(), is(false));
        assertThat(cursorResult.getContinuation().isEnd(), is(true));
        assertNull(cursorResult.getContinuation().toBytes());
        assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, cursorResult.getNoNextReason());
    }

    @Test
    public void singleResult() throws ExecutionException, InterruptedException {
        TimeLimitedCursor<Integer> cursor = new TimeLimitedCursor<>(RecordCursor.fromList(Arrays.asList(1, 2)), System.currentTimeMillis() - 200, 100);
        RecordCursorResult<Integer> cursorResult = cursor.onNext().get();
        assertThat(cursorResult.hasNext(), is(true));
        assertEquals(1, (int)cursorResult.get());
        final RecordCursorContinuation continuation = cursorResult.getContinuation();
        cursorResult = cursor.onNext().get();
        assertThat(cursorResult.hasNext(), is(false));
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, cursorResult.getNoNextReason());
        assertThat(cursorResult.getContinuation().isEnd(), is(false));
        assertNotNull(cursorResult.getContinuation().toBytes());
        assertArrayEquals(continuation.toBytes(), cursorResult.getContinuation().toBytes());

        RecordCursor<Integer> resumedCursor = RecordCursor.fromList(Arrays.asList(1, 2), cursor.getContinuation());
        assertEquals(2, (int)resumedCursor.onNext().get().get());
        assertThat(resumedCursor.onNext().get().hasNext(), is(false));
    }
}
