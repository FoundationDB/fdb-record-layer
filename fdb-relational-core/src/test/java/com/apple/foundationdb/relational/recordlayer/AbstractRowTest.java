/*
 * AbstractRowTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.relational.api.Row;

import org.junit.jupiter.api.Test;

import java.util.List;

import static com.apple.foundationdb.relational.utils.RelationalAssertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AbstractRowTest {

    @Test
    void startsWith() {
        assertTrue(new ValueTuple(4L).startsWith(new ValueTuple(4L)));
        assertTrue(new ValueTuple(4L).startsWith(EmptyTuple.INSTANCE));
        assertTrue(EmptyTuple.INSTANCE.startsWith(EmptyTuple.INSTANCE));
        assertTrue(new ImmutableKeyValue(new ValueTuple(4), new ValueTuple(5)).startsWith(new ValueTuple(4)));
        assertTrue(new ImmutableKeyValue(new ValueTuple(4L), new FDBTuple(new Tuple().add(5).add(6).add(7)))
                .startsWith(new FDBTuple(new Tuple().add(4L).add(5L))));

        assertFalse(new ValueTuple(5L).startsWith(new ValueTuple(4L)));
        assertFalse(EmptyTuple.INSTANCE.startsWith(new ValueTuple(4L)));
        assertFalse(new ValueTuple(4).startsWith(new ImmutableKeyValue(new ValueTuple(4), new ValueTuple(5))));
        assertFalse(new ImmutableKeyValue(new ValueTuple(4L), new FDBTuple(new Tuple().add(5).add(6).add(7)))
                .startsWith(new FDBTuple(new Tuple().add(4L).add(6L))));
    }

    @Test
    void testToStringShowsByteStrings() {
        Row row = new ValueTuple(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11});
        assertThat(row.toString()).isEqualTo("(ByteArray[0102030405060708090A0B])");
    }

    @Test
    void testToStringShowsCollections() {
        Row row = new ArrayRow(new Object[]{
                List.of("this", "is", "a", "test")
        });
        assertThat(row.toString()).isEqualTo("(Array[this, is, a, test])");
    }

    @Test
    void testToStringShowsArrays() {
        Row row = new ArrayRow(new Object[]{
                new String[]{"this", "is", "a", "test"}
        });
        assertThat(row.toString()).isEqualTo("(Array[this, is, a, test])");
    }
}
