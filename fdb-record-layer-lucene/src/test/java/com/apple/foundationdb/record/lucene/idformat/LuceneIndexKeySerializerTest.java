/*
 * LuceneIndexKeySerializerTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene.idformat;

import com.apple.foundationdb.tuple.Tuple;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Test for the serializer.
 */
public class LuceneIndexKeySerializerTest {
    @Test
    void singleElement() {
        Tuple key = Tuple.from(12345L);
        LuceneIndexKeySerializer classUnderTest = LuceneIndexKeySerializer.fromStringFormat("[INT64]", key);

        assertResult(new byte[] {22, 48, 57}, classUnderTest.asPackedByteArray());
        assertResult(new byte[][] {{22, 48, 57, 0, 0, 0, 0, 0, 0}}, classUnderTest.asPackedBinaryPoint());
        assertResult(new byte[][] {{22, 48, 57, 0, 0, 0, 0, 0, 0}}, classUnderTest.asFormattedBinaryPoint());
    }

    @Test
    void testMultipleElements() {
        Tuple key = Tuple.from(1234567890L, 56789);
        LuceneIndexKeySerializer classUnderTest = LuceneIndexKeySerializer.fromStringFormat("[INT64, INT32]", key);

        assertResult(new byte[] {24, 73, -106, 2, -46, 22, -35, -43}, classUnderTest.asPackedByteArray());
        assertResult(new byte[][] {{24, 73, -106, 2, -46, 22, -35, -43, 0}}, classUnderTest.asPackedBinaryPoint());
        assertResult(new byte[][] {{24, 73, -106, 2, -46, 0, 0, 0, 0},
                                   {22, -35, -43, 0, 0, 0, 0, 0, 0}}, classUnderTest.asFormattedBinaryPoint());
    }

    @Test
    void testMultipleElementsOverflowAnotherDimension() {
        Tuple key = Tuple.from(1234567890L, 56789, 9876543210L);
        LuceneIndexKeySerializer classUnderTest = LuceneIndexKeySerializer.fromStringFormat("[INT64, INT32, INT64]", key);

        assertResult(new byte[] {24, 73, -106, 2, -46, 22, -35, -43, 25, 2, 76, -80, 22, -22}, classUnderTest.asPackedByteArray());
        assertResult(new byte[][] {{24, 73, -106, 2, -46, 22, -35, -43, 25},
                                   {2, 76, -80, 22, -22, 0, 0, 0, 0}}, classUnderTest.asPackedBinaryPoint());
        assertResult(new byte[][] {{24, 73, -106, 2, -46, 0, 0, 0, 0},
                                   {22, -35, -43, 0, 0, 0, 0, 0, 0},
                                   {25, 2, 76, -80, 22, -22, 0, 0, 0}}, classUnderTest.asFormattedBinaryPoint());
    }

    @Test
    void testShortString() {
        Tuple key = Tuple.from("abc");
        LuceneIndexKeySerializer classUnderTest = LuceneIndexKeySerializer.fromStringFormat("[STRING_16]", key);

        assertResult(new byte[] {2, 97, 98, 99, 0}, classUnderTest.asPackedByteArray());
        assertResult(new byte[][] {{2, 97, 98, 99, 0, 0, 0, 0, 0}}, classUnderTest.asPackedBinaryPoint());
        assertResult(new byte[][] {{97, 98, 99, 0, 0, 0, 0, 0, 0},
                                   {0, 0, 0, 0, 0, 0, 0, 0, 0},
                                   {0, 0, 0, 0, 0, 0, 0, 0, 0}}, classUnderTest.asFormattedBinaryPoint());
    }

    @Test
    void testLongString() {
        Tuple key = Tuple.from("abcdefghabcdefgh");
        LuceneIndexKeySerializer classUnderTest = LuceneIndexKeySerializer.fromStringFormat("[STRING_16]", key);

        assertResult(new byte[] {2, 97, 98, 99, 100, 101, 102, 103, 104, 97, 98, 99, 100, 101, 102, 103, 104, 0}, classUnderTest.asPackedByteArray());
        assertResult(new byte[][] {{2, 97, 98, 99, 100, 101, 102, 103, 104},
                                   {97, 98, 99, 100, 101, 102, 103, 104, 0}}, classUnderTest.asPackedBinaryPoint());
        assertResult(new byte[][] {{97, 98, 99, 100, 101, 102, 103, 104, 97},
                                   {98, 99, 100, 101, 102, 103, 104, 0, 0},
                                   {0, 0, 0, 0, 0, 0, 0, 0, 0}}, classUnderTest.asFormattedBinaryPoint());
    }

    @Test
    void testUuid() {
        Tuple key = Tuple.from("98e6c88d-d757-4d0f-a5f3-d3055e1163b0");
        LuceneIndexKeySerializer classUnderTest = LuceneIndexKeySerializer.fromStringFormat("[UUID_AS_STRING]", key);

        assertResult(new byte[] {2, 57, 56, 101, 54, 99, 56, 56, 100, 45, 100, 55, 53, 55, 45, 52, 100, 48, 102, 45, 97, 53, 102, 51, 45, 100, 51, 48, 53, 53, 101, 49, 49, 54, 51, 98, 48, 0}, classUnderTest.asPackedByteArray());
        assertResult(new byte[][] {{2, 57, 56, 101, 54, 99, 56, 56, 100},
                                   {45, 100, 55, 53, 55, 45, 52, 100, 48},
                                   {102, 45, 97, 53, 102, 51, 45, 100, 51},
                                   {48, 53, 53, 101, 49, 49, 54, 51, 98},
                                   {48, 0, 0, 0, 0, 0, 0, 0, 0}}, classUnderTest.asPackedBinaryPoint());
        assertResult(new byte[][] {{48, -104, -26, -56, -115, -41, 87, 77, 15},
                                   {-91, -13, -45, 5, 94, 17, 99, -80, 0}}, classUnderTest.asFormattedBinaryPoint());
    }

    @Test
    void testNested() {
        Tuple key = Tuple.from(123L, Tuple.from(456L, "abcdef"));
        LuceneIndexKeySerializer classUnderTest = LuceneIndexKeySerializer.fromStringFormat("[INT64, [INT64, STRING_16]]", key);

        assertResult(new byte[] {21, 123, 5, 22, 1, -56, 2, 97, 98, 99, 100, 101, 102, 0, 0}, classUnderTest.asPackedByteArray());
        assertResult(new byte[][] {{21, 123, 5, 22, 1, -56, 2, 97, 98},
                                   {99, 100, 101, 102, 0, 0, 0, 0, 0}}, classUnderTest.asPackedBinaryPoint());
        assertResult(new byte[][] {{21, 123, 0, 0, 0, 0, 0, 0, 0},
                                   {22, 1, -56, 0, 0, 0, 0, 0, 0},
                                   {97, 98, 99, 100, 101, 102, 0, 0, 0},
                                   {0, 0, 0, 0, 0, 0, 0, 0, 0},
                                   {0, 0, 0, 0, 0, 0, 0, 0, 0}}, classUnderTest.asFormattedBinaryPoint());
    }

    @Test
    void testNull() {
        Tuple key = Tuple.from(12L, 45L, 78L);
        LuceneIndexKeySerializer classUnderTest = LuceneIndexKeySerializer.fromStringFormat("[NULL, INT64, NULL]", key);

        assertResult(new byte[] {21, 12, 21, 45, 21, 78}, classUnderTest.asPackedByteArray());
        assertResult(new byte[][] {{21, 12, 21, 45, 21, 78, 0, 0, 0}}, classUnderTest.asPackedBinaryPoint());
        assertResult(new byte[][] {{0, 0, 0, 0, 0, 0, 0, 0, 0},
                                   {21, 45, 0, 0, 0, 0, 0, 0, 0},
                                   {0, 0, 0, 0, 0, 0, 0, 0, 0}}, classUnderTest.asFormattedBinaryPoint());
    }

    @Test
    void testNullableInteger() {
        Tuple key = Tuple.from(null, 45L, null);
        LuceneIndexKeySerializer classUnderTest = LuceneIndexKeySerializer.fromStringFormat("[INT64_OR_NULL, INT64, INT32_OR_NULL]", key);

        assertResult(new byte[] {0, 21, 45, 0}, classUnderTest.asPackedByteArray());
        assertResult(new byte[][] {{0, 21, 45, 0, 0, 0, 0, 0, 0}}, classUnderTest.asPackedBinaryPoint());
        assertResult(new byte[][] {{0, 0, 0, 0, 0, 0, 0, 0, 0},
                                   {21, 45, 0, 0, 0, 0, 0, 0, 0},
                                   {0, 0, 0, 0, 0, 0, 0, 0, 0}}, classUnderTest.asFormattedBinaryPoint());
    }

    @Test
    void testNone() {
        Tuple key = Tuple.from(12L, 45L, 78L);
        LuceneIndexKeySerializer classUnderTest = LuceneIndexKeySerializer.fromStringFormat("[NONE, INT64, NONE]", key);

        assertResult(new byte[] {21, 12, 21, 45, 21, 78}, classUnderTest.asPackedByteArray());
        assertResult(new byte[][] {{21, 12, 21, 45, 21, 78, 0, 0, 0}}, classUnderTest.asPackedBinaryPoint());
        assertResult(new byte[][] {{21, 45, 0, 0, 0, 0, 0, 0, 0}}, classUnderTest.asFormattedBinaryPoint());
    }

    @Test
    void testStringTooLong() {
        Tuple key = Tuple.from("abcdefghij abcdefghij");
        LuceneIndexKeySerializer classUnderTest = LuceneIndexKeySerializer.fromStringFormat("[STRING_16]", key);

        Assertions.assertThrows(RecordCoreFormatException.class, classUnderTest::asFormattedBinaryPoint);
    }

    @Test
    void testStringEncodedTooLong() {
        Tuple key = Tuple.from("æčęÿæčęÿæčęÿæčęÿ");
        LuceneIndexKeySerializer classUnderTest = LuceneIndexKeySerializer.fromStringFormat("[STRING_16]", key);

        Assertions.assertThrows(RecordCoreSizeException.class, classUnderTest::asFormattedBinaryPoint);
    }

    @Test
    void testInvalidUuid() {
        Tuple key = Tuple.from("abcdefghij");
        LuceneIndexKeySerializer classUnderTest = LuceneIndexKeySerializer.fromStringFormat("[UUID_AS_STRING]", key);

        Assertions.assertThrows(RecordCoreFormatException.class, classUnderTest::asFormattedBinaryPoint);
    }

    @Test
    void testStringNonAscii() {
        Tuple key = Tuple.from("æčęÿ");
        LuceneIndexKeySerializer classUnderTest = LuceneIndexKeySerializer.fromStringFormat("[STRING_16]", key);

        assertResult(new byte[] {2, -61, -90, -60, -115, -60, -103, -61, -65, 0}, classUnderTest.asPackedByteArray());
        assertResult(new byte[][] {{2, -61, -90, -60, -115, -60, -103, -61, -65},
                                   {0, 0, 0, 0, 0, 0, 0, 0, 0}}, classUnderTest.asPackedBinaryPoint());
        assertResult(new byte[][] {{-61, -90, -60, -115, -60, -103, -61, -65, 0},
                                   {0, 0, 0, 0, 0, 0, 0, 0, 0},
                                   {0, 0, 0, 0, 0, 0, 0, 0, 0}}, classUnderTest.asFormattedBinaryPoint());
    }

    @Test
    void testStringCjk() {
        Tuple key = Tuple.from("苹果园区");
        LuceneIndexKeySerializer classUnderTest = LuceneIndexKeySerializer.fromStringFormat("[STRING_16]", key);

        assertResult(new byte[] {2, -24, -117, -71, -26, -98, -100, -27, -101, -83, -27, -116, -70, 0}, classUnderTest.asPackedByteArray());
        assertResult(new byte[][] {{2, -24, -117, -71, -26, -98, -100, -27, -101},
                                   {-83, -27, -116, -70, 0, 0, 0, 0, 0}}, classUnderTest.asPackedBinaryPoint());
        assertResult(new byte[][] {{-24, -117, -71, -26, -98, -100, -27, -101, -83},
                                   {-27, -116, -70, 0, 0, 0, 0, 0, 0},
                                   {0, 0, 0, 0, 0, 0, 0, 0, 0}}, classUnderTest.asFormattedBinaryPoint());
    }

    private void assertResult(byte[] expected, byte[] actual) {
        Assertions.assertArrayEquals(expected, actual);
    }

    private void assertResult(byte[][] expected, byte[][] actual) {
        Assertions.assertArrayEquals(expected, actual);
    }
}
