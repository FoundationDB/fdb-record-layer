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
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static com.apple.foundationdb.record.lucene.idformat.RecordIdFormat.FormatElementType.INT32;
import static com.apple.foundationdb.record.lucene.idformat.RecordIdFormat.FormatElementType.INT64;
import static com.apple.foundationdb.record.lucene.idformat.RecordIdFormat.FormatElementType.STRING_16;
import static com.apple.foundationdb.record.lucene.idformat.RecordIdFormat.FormatElementType.UUID_AS_STRING;

public class LuceneIndexKeySerializerTest {
    @Test
    void testSingleElement() {
        final RecordIdFormat format = RecordIdFormat.of(INT64);
        Tuple key = Tuple.from(12345L);
        LuceneIndexKeySerializer classUnderTest = new LuceneIndexKeySerializer(format, key);

        byte[] packed = classUnderTest.asPackedByteArray();
        byte[][] packedBinaryPoint = classUnderTest.asPackedBinaryPoint();
        byte[][] formattedBinaryPoint = classUnderTest.asFormattedBinaryPoint();
    }

    @Test
    void testMultipleElements() {
        final RecordIdFormat format = RecordIdFormat.of(INT64, INT32);
        Tuple key = Tuple.from(1234567890L, 56789);
        LuceneIndexKeySerializer classUnderTest = new LuceneIndexKeySerializer(format, key);

        byte[] packed = classUnderTest.asPackedByteArray();
        byte[][] packedBinaryPoint = classUnderTest.asPackedBinaryPoint();
        byte[][] formattedBinaryPoint = classUnderTest.asFormattedBinaryPoint();
    }

    @Test
    void testMultipleElementsOverflowAnotherDimension() {
        final RecordIdFormat format = RecordIdFormat.of(INT64, INT32, INT64);
        Tuple key = Tuple.from(1234567890L, 56789, 9876543210L);
        LuceneIndexKeySerializer classUnderTest = new LuceneIndexKeySerializer(format, key);

        byte[] packed = classUnderTest.asPackedByteArray();
        byte[][] packedBinaryPoint = classUnderTest.asPackedBinaryPoint();
        byte[][] formattedBinaryPoint = classUnderTest.asFormattedBinaryPoint();
    }

    @Test
    void testShortString() {
        final RecordIdFormat format = RecordIdFormat.of(STRING_16);
        Tuple key = Tuple.from("abc");
        LuceneIndexKeySerializer classUnderTest = new LuceneIndexKeySerializer(format, key);

        byte[] packed = classUnderTest.asPackedByteArray();
        byte[][] packedBinaryPoint = classUnderTest.asPackedBinaryPoint();
        byte[][] formattedBinaryPoint = classUnderTest.asFormattedBinaryPoint();
    }

    @Test
    void testLongString() {
        final RecordIdFormat format = RecordIdFormat.of(STRING_16);
        Tuple key = Tuple.from("abcdefghabcdefgh");
        LuceneIndexKeySerializer classUnderTest = new LuceneIndexKeySerializer(format, key);

        byte[] packed = classUnderTest.asPackedByteArray();
        byte[][] packedBinaryPoint = classUnderTest.asPackedBinaryPoint();
        byte[][] formattedBinaryPoint = classUnderTest.asFormattedBinaryPoint();
    }

    @Test
    void testUuid() {
        final RecordIdFormat format = RecordIdFormat.of(UUID_AS_STRING);
        Tuple key = Tuple.from(UUID.randomUUID().toString());
        LuceneIndexKeySerializer classUnderTest = new LuceneIndexKeySerializer(format, key);

        byte[] packed = classUnderTest.asPackedByteArray();
        byte[][] packedBinaryPoint = classUnderTest.asPackedBinaryPoint();
        byte[][] formattedBinaryPoint = classUnderTest.asFormattedBinaryPoint();
    }

    @Test
    void testNestedNumbers() {
        final RecordIdFormat format = RecordIdFormat.of(INT64, RecordIdFormat.TupleElement.of(INT64, STRING_16));
        Tuple key = Tuple.from(123L, Tuple.from(456L, "abcdef"));
        LuceneIndexKeySerializer classUnderTest = new LuceneIndexKeySerializer(format, key);

        byte[] packed = classUnderTest.asPackedByteArray();
        byte[][] packedBinaryPoint = classUnderTest.asPackedBinaryPoint();
        byte[][] formattedBinaryPoint = classUnderTest.asFormattedBinaryPoint();
    }

    // More tests: null, NONE, nested deep, String too long, String with non-ascii characters

}
