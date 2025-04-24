/*
 * DedupCursorTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.tuple.Tuple;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DedupCursorTest {
    @Test
    void uniqueItemsTest() throws ExecutionException, InterruptedException {
        List<Long> items = List.of(1L, 2L, 3L, 4L, 5L, 6L);
        Function<byte[], RecordCursor<Long>> innerFunction = cont -> new ListCursor<>(items, cont);

        DedupCursor<Long> classUnderTest = new DedupCursor<>(innerFunction, this::fromBytes, this::toBytes, null);
        assertEquals(items, classUnderTest.asList().get());
    }

    @Test
    void duplicateItemsTest() throws ExecutionException, InterruptedException {
        List<Long> items = List.of(1L, 1L, 2L, 3L, 4L, 5L, 5L, 5L, 5L, 6L, 8L, 8L, 10L, 10L);
        Function<byte[], RecordCursor<Long>> innerFunction = cont -> new ListCursor<>(items, cont);

        DedupCursor<Long> classUnderTest = new DedupCursor<>(innerFunction, this::fromBytes, this::toBytes, null);
        assertEquals(items.stream().distinct().collect(Collectors.toList()), classUnderTest.asList().get());
    }

    @Test
    void uniqueContinuationTest() throws ExecutionException, InterruptedException {
        List<Long> items = List.of(1L, 2L, 3L, 4L, 5L, 6L);
        Function<byte[], RecordCursor<Long>> innerFunction = cont -> new ListCursor<>(items, cont).limitRowsTo(4);

        DedupCursor<Long> classUnderTest = new DedupCursor<>(innerFunction, this::fromBytes, this::toBytes, null);
        List<Long> actual = new ArrayList<>();
        RecordCursorResult<Long> result = readThroughContinuation(classUnderTest, actual);
        // First iteration of 4
        assertEquals(4, actual.size());
        assertFalse(result.getContinuation().isEnd());
        assertTrue(result.hasStoppedBeforeEnd());
        assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, result.getNoNextReason());

        // Second iteration of 2
        classUnderTest = new DedupCursor<>(innerFunction, this::fromBytes, this::toBytes, result.getContinuation().toBytes());
        result = readThroughContinuation(classUnderTest, actual);
        assertEquals(items, actual);
        assertTrue(result.getContinuation().isEnd());
        assertFalse(result.hasStoppedBeforeEnd());
        assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, result.getNoNextReason());
    }

    @Test
    void dupliucateContinuationTest() throws ExecutionException, InterruptedException {
        List<Long> items = List.of(1L, 1L, 2L, 3L, 4L, 5L, 5L, 5L, 5L, 6L, 8L, 8L, 10L, 10L);
        Function<byte[], RecordCursor<Long>> innerFunction = cont -> new ListCursor<>(items, cont).limitRowsTo(4);

        DedupCursor<Long> classUnderTest = new DedupCursor<>(innerFunction, this::fromBytes, this::toBytes, null);
        List<Long> actual = new ArrayList<>();
        RecordCursorResult<Long> result = readThroughContinuation(classUnderTest, actual);
        // first iteration of 3
        assertEquals(3, actual.size());
        assertFalse(result.getContinuation().isEnd());
        assertTrue(result.hasStoppedBeforeEnd());
        assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, result.getNoNextReason());

        // second iteration of 2
        classUnderTest = new DedupCursor<>(innerFunction, this::fromBytes, this::toBytes, result.getContinuation().toBytes());
        result = readThroughContinuation(classUnderTest, actual);
        assertEquals(5, actual.size());
        assertFalse(result.getContinuation().isEnd());
        assertTrue(result.hasStoppedBeforeEnd());
        assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, result.getNoNextReason());

        // third iteration of 2
        classUnderTest = new DedupCursor<>(innerFunction, this::fromBytes, this::toBytes, result.getContinuation().toBytes());
        result = readThroughContinuation(classUnderTest, actual);
        assertEquals(7, actual.size());
        assertFalse(result.getContinuation().isEnd());
        assertTrue(result.hasStoppedBeforeEnd());
        assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, result.getNoNextReason());

        // final iteration of 1
        classUnderTest = new DedupCursor<>(innerFunction, this::fromBytes, this::toBytes, result.getContinuation().toBytes());
        result = readThroughContinuation(classUnderTest, actual);
        assertEquals(items.stream().distinct().collect(Collectors.toList()), actual);
        assertTrue(result.getContinuation().isEnd());
        assertFalse(result.hasStoppedBeforeEnd());
        assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, result.getNoNextReason());
    }

    private byte[] toBytes(long value) {
        return Tuple.from(value).pack();
    }

    private Long fromBytes(byte[] bytes) {
        return Tuple.fromBytes(bytes).getLong(0);
    }

    private RecordCursorResult<Long> readThroughContinuation(RecordCursor<Long> cursor, List<Long> actual) {
        RecordCursorResult<Long> result;
        for (result = cursor.getNext(); result.hasNext(); result = cursor.getNext()) {
            actual.add(result.get());
        }
        return result;
    }
}
