/*
 * DataInKeySpacePathTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.keyspace;

import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory.KeyType;
import com.apple.foundationdb.tuple.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link DataInKeySpacePath}.
 */
class DataInKeySpacePathTest {

    @Test
    void basicAccessors() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("test", KeyType.STRING, UUID.randomUUID().toString()));

        KeySpacePath testPath = root.path("test");
        byte[] valueBytes = Tuple.from("test_value").pack();

        DataInKeySpacePath dataInPath = new DataInKeySpacePath(testPath, null, valueBytes);

        // Verify accessors
        assertSame(testPath, dataInPath.getPath());
        assertNull(dataInPath.getRemainder());
        assertArrayEquals(valueBytes, dataInPath.getValue());
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 5, 10})
    void accessorsWithRemainder(int remainderSize) {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("test", KeyType.STRING, UUID.randomUUID().toString()));

        KeySpacePath testPath = root.path("test");

        // Create a remainder tuple with the specified number of elements
        Tuple remainderTuple;
        if (remainderSize > 0) {
            remainderTuple = Tuple.from();
            for (int i = 0; i < remainderSize; i++) {
                remainderTuple.add("element_" + i);
            }
        } else {
            remainderTuple = null;
        }

        byte[] valueBytes = Tuple.from("test_value").pack();

        DataInKeySpacePath dataInPath = new DataInKeySpacePath(testPath, remainderTuple, valueBytes);

        // Verify accessors
        assertSame(testPath, dataInPath.getPath());
        assertEquals(remainderTuple, dataInPath.getRemainder());
        assertArrayEquals(valueBytes, dataInPath.getValue());
    }

    @Test
    void withBinaryValue() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("binary_store", KeyType.STRING, UUID.randomUUID().toString()));

        KeySpacePath storePath = root.path("binary_store");
        Tuple remainder = Tuple.from("key1", "key2");
        byte[] binaryValue = {0x01, 0x02, 0x03, (byte) 0xFF, (byte) 0xFE};

        DataInKeySpacePath dataInPath = new DataInKeySpacePath(storePath, remainder, binaryValue);

        // Verify accessors
        assertSame(storePath, dataInPath.getPath());
        assertEquals(remainder, dataInPath.getRemainder());
        assertArrayEquals(binaryValue, dataInPath.getValue());
    }

    /**
     * Test if there is a null value. FDB shouldn't ever return this, and if you got it somehow, you wouldn't be
     * able to insert it back into a database because {@link com.apple.foundationdb.FDBTransaction#set(byte[], byte[])}
     * does not support a {@code null} key or value.
     */
    @Test
    void nullValue() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("test", KeyType.STRING, UUID.randomUUID().toString()));

        KeySpacePath testPath = root.path("test");
        byte[] valueBytes = null;

        assertThrows(RecordCoreArgumentException.class,
                () -> new DataInKeySpacePath(testPath, null, valueBytes));
    }
}
