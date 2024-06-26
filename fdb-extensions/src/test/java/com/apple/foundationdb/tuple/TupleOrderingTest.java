/*
 * TupleOrderingTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.tuple;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.TreeMap;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests of {@link TupleOrdering}.
 */
class TupleOrderingTest {

    private static final byte[][] INVERT_BYTES = {
        new byte[] { }, new byte[] { (byte)0x80 },
        new byte[] { 0x00 }, new byte[] { 0x7F, 0x7F, (byte)0xE0 },
        new byte[] { 0x01 }, new byte[] { 0x7F, 0x3F, (byte)0xE0 },
        new byte[] { 0x01, 0x02 }, new byte[] { 0x7F, 0x3F, 0x3F, (byte)0xD0 },
        new byte[] { 0x01, 0x02, 0x00 }, new byte[] { 0x7F, 0x3F, 0x3F, 0x7F, (byte)0xC0 },
        new byte[] { 0x01, 0x02, 0x03 }, new byte[] { 0x7F, 0x3F, 0x3F, 0x4F, (byte)0xC0 },
        new byte[] { 0x01, 0x02, (byte)0xFF }, new byte[] { 0x7F, 0x3F, 0x20, 0x0F, (byte)0xC0 },
        new byte[] { 0x01, 0x02, (byte)0xFF, 0x00 }, new byte[] { 0x7F, 0x3F, 0x20, 0x0F, 0x7F, (byte)0xB0 },
        new byte[] { 0x01, 0x02, (byte)0xFF, 0x01 }, new byte[] { 0x7F, 0x3F, 0x20, 0x0F, 0x77, (byte)0xB0 },
        new byte[] { (byte)0xFF }, new byte[] { 0x00, 0x3F, (byte)0xE0 },
    };

    @Test
    void testInvert() {
        for (int i = 0; i < INVERT_BYTES.length; i += 2) {
            final byte[] original = INVERT_BYTES[i];
            final byte[] inverted = TupleOrdering.invert(original);
            final byte[] expected = INVERT_BYTES[i + 1];
            assertArrayEquals(expected, inverted,
                    () -> ByteArrayUtil2.toHexString(inverted) + " != " + ByteArrayUtil2.toHexString(expected) + " for " + ByteArrayUtil2.toHexString(original));
        }
    }

    @Test
    void testUninvert() {
        for (int i = 0; i < INVERT_BYTES.length; i += 2) {
            final byte[] inverted = INVERT_BYTES[i + 1];
            final byte[] uninverted = TupleOrdering.uninvert(inverted);
            final byte[] expected = INVERT_BYTES[i];
            assertArrayEquals(expected, uninverted,
                    () -> ByteArrayUtil2.toHexString(uninverted) + " != " + ByteArrayUtil2.toHexString(expected) + " for " + ByteArrayUtil2.toHexString(inverted));
        }
    }

    private static final Object[] INVERT_OBJS = {
        null,
        new byte[] { }, new byte[] { 0x01 }, new byte[] { 0x01, 0x02 }, new byte[] { (byte)0xFF },
        "", "a", "ab", "abc", "abcd", "abcde", "abcdef", "abcdefg", "ac",
        Long.MIN_VALUE, -2, -1, 0, 1, 2, Long.MAX_VALUE,
        Double.longBitsToDouble(0xFFF8000000000001L), Double.longBitsToDouble(0xFFF8000000000000L), Double.NEGATIVE_INFINITY,
        -1e10, -2.0, -1.0, -0.0, 0.0, 2.0, 1e10,
        Double.POSITIVE_INFINITY, Double.NaN, Double.longBitsToDouble(0x7FF8000000000001L),
        false, true,
        UUID.fromString("123e4567-e89b-12d3-a456-426614174000"), UUID.fromString("123e4567-e89b-12d3-a456-426614174001"),
    };

    @ParameterizedTest
    @EnumSource(TupleOrdering.Direction.class)
    void testOrdering(TupleOrdering.Direction direction) {
        final TreeMap<byte[], Object> ordered = new TreeMap<>(ByteArrayUtil.comparator());
        for (Object obj : INVERT_OBJS) {
            ordered.put(TupleOrdering.pack(Tuple.from(obj), direction), obj);
        }
        List<Object> expected = Lists.newArrayList(INVERT_OBJS);
        if (direction.isCounterflowNulls()) {
            expected.remove(0);
            expected.add(null);
        }
        if (direction.isInverted()) {
            expected = Lists.reverse(expected);
        }
        assertEquals(expected, Lists.newArrayList(ordered.values()));
    }
}
