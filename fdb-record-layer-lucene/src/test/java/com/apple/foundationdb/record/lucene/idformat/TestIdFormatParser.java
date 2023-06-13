/*
 * TestIdFormatParser.java
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

import com.apple.foundationdb.record.lucene.idformat.RecordIdFormat.TupleElement;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static com.apple.foundationdb.record.lucene.idformat.RecordIdFormat.FormatElementType.INT32;
import static com.apple.foundationdb.record.lucene.idformat.RecordIdFormat.FormatElementType.INT64;
import static com.apple.foundationdb.record.lucene.idformat.RecordIdFormat.FormatElementType.STRING_16;


public class TestIdFormatParser {
    @Test
    void testParseSimple() {
        RecordIdFormat format = RecordIdFormatParser.parse("[INT32]");
        Assertions.assertEquals(RecordIdFormat.of(INT32), format);
    }

    @Test
    void testParseMulti() {
        RecordIdFormat format = RecordIdFormatParser.parse("[INT32, INT64, STRING_16]");
        Assertions.assertEquals(RecordIdFormat.of(INT32, INT64, STRING_16), format);
    }

    @Test
    void testParseNested() {
        RecordIdFormat format = RecordIdFormatParser.parse("[INT32, INT32, [INT64, STRING_16], INT64]");
        Assertions.assertEquals(RecordIdFormat.of(INT32, INT32, TupleElement.of(INT64, STRING_16), INT64), format);
    }

}
