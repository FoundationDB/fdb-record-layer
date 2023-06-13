/*
 * SplitTest.java
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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

/**
 * Test for the array splitter utility.
 */
public class SplitTest {
    @Test
    void emptyArray() {
        List<byte[]> actual = LuceneIndexKeySerializer.split(new byte[] {}, 9);
        assertEquals(Collections.emptyList(), actual);
    }

    @Test
    void shortArray() {
        List<byte[]> actual = LuceneIndexKeySerializer.split(new byte[] {1, 2, 3, 4, 5}, 9);
        assertEquals(Collections.singletonList(new byte[] {1, 2, 3, 4, 5, 0, 0, 0, 0}), actual);
    }

    @Test
    void exactLengthArray() {
        List<byte[]> actual = LuceneIndexKeySerializer.split(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9}, 9);
        assertEquals(Collections.singletonList(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9}), actual);
    }

    @Test
    void longArray() {
        List<byte[]> actual = LuceneIndexKeySerializer.split(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}, 9);
        assertEquals(List.of(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9}, new byte[] {10, 11, 12, 0, 0, 0, 0, 0, 0}), actual);
    }

    @Test
    void twiceExactArray() {
        List<byte[]> actual = LuceneIndexKeySerializer.split(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}, 9);
        assertEquals(List.of(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9}, new byte[] {10, 11, 12, 13, 14, 15, 16, 17, 18}), actual);
    }

    @Test
    void veryLongArray() {
        List<byte[]> actual = LuceneIndexKeySerializer.split(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, 9);
        assertEquals(List.of(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9}, new byte[] {10, 11, 12, 13, 14, 15, 16, 17, 18}, new byte[] {19, 20, 0, 0, 0, 0, 0, 0, 0}), actual);
    }

    private void assertEquals(final List<byte[]> expected, final List<byte[]> actual) {
        Assertions.assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            Assertions.assertArrayEquals(expected.get(i), actual.get(i));
        }
    }
}
