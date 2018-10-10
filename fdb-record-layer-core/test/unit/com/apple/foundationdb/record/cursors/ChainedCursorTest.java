/*
 * ChainedCursorTest.java
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

import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.tuple.Tuple;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link ChainedCursor}.
 */
public class ChainedCursorTest {

    @Test
    public void testChainedCursor() {
        RecordCursor<Long> cursor = newCursor(null);

        long i = 0L;
        while (cursor.hasNext()) {
            assertEquals((Long) i, cursor.next());
            ++i;
        }
        assertEquals(25, i);
    }

    @Test
    public void testChainedCursorContinuation() {
        RecordCursor<Long> cursor = newCursor(null);

        long i = 0L;
        while (cursor.hasNext()) {
            assertEquals((Long) i, cursor.next());
            ++i;
            if ((i % 2) == 0) {
                final byte[] continuation = cursor.getContinuation();
                cursor = newCursor(continuation);
            }
        }
        assertEquals(25, i);
    }

    private RecordCursor<Long> newCursor(byte[] continuation) {
        return new ChainedCursor<>(
                (lastKey) -> nextKey(lastKey),
                (key) -> Tuple.from(key).pack(),
                (prevContinuation) -> Tuple.fromBytes(prevContinuation).getLong(0),
                continuation,
                null
        );
    }

    private static CompletableFuture<Optional<Long>> nextKey(Optional<Long> currentKey) {
        final Optional<Long> ret;
        if (currentKey.isPresent()) {
            if (currentKey.get() >= 24) {
                ret = Optional.empty();
            } else {
                ret = Optional.of(currentKey.get() + 1L);
            }
        } else {
            ret = Optional.of(0L);
        }

        return CompletableFuture.completedFuture(ret);
    }
}
