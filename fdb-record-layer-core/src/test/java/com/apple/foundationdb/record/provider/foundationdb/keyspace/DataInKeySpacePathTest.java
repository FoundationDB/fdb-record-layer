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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    void testEquals() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("test", KeyType.STRING, UUID.randomUUID().toString()));

        KeySpacePath testPath = root.path("test");
        Tuple remainder1 = Tuple.from("key1", "key2");
        byte[] value1 = Tuple.from("value1").pack();

        DataInKeySpacePath data1 = new DataInKeySpacePath(testPath, remainder1, value1);
        DataInKeySpacePath data2 = new DataInKeySpacePath(testPath, remainder1, value1);

        // Reflexive: object equals itself
        assertEquals(data1, data1);

        // Symmetric: a.equals(b) implies b.equals(a)
        assertEquals(data1, data2);
        assertEquals(data2, data1);

        // Test with different remainder
        Tuple remainder2 = Tuple.from("different", "key");
        DataInKeySpacePath data3 = new DataInKeySpacePath(testPath, remainder2, value1);
        assertNotEquals(data1, data3);

        // Test with different value
        byte[] value2 = Tuple.from("value2").pack();
        DataInKeySpacePath data4 = new DataInKeySpacePath(testPath, remainder1, value2);
        assertNotEquals(data1, data4);

        // Test with different path
        KeySpace root2 = new KeySpace(
                new KeySpaceDirectory("test", KeyType.STRING, UUID.randomUUID().toString()));
        KeySpacePath testPath2 = root2.path("test");
        DataInKeySpacePath data5 = new DataInKeySpacePath(testPath2, remainder1, value1);
        assertNotEquals(data1, data5);

        // Test with null remainder
        DataInKeySpacePath data6 = new DataInKeySpacePath(testPath, null, value1);
        DataInKeySpacePath data7 = new DataInKeySpacePath(testPath, null, value1);
        assertEquals(data6, data7);
        assertNotEquals(data1, data6);

        // Test with null object
        assertNotEquals(data1, null);

        // Test with different class
        assertNotEquals(data1, "not a DataInKeySpacePath");
    }

    @Test
    void testHashCode() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("test", KeyType.STRING, UUID.randomUUID().toString()));

        KeySpacePath testPath = root.path("test");
        Tuple remainder = Tuple.from("key1", "key2");
        byte[] value = Tuple.from("value1").pack();

        DataInKeySpacePath data1 = new DataInKeySpacePath(testPath, remainder, value);
        DataInKeySpacePath data2 = new DataInKeySpacePath(testPath, remainder, value);

        // Equal objects must have equal hash codes
        assertEquals(data1.hashCode(), data2.hashCode());

        // Test with null remainder
        DataInKeySpacePath data3 = new DataInKeySpacePath(testPath, null, value);
        DataInKeySpacePath data4 = new DataInKeySpacePath(testPath, null, value);
        assertEquals(data3.hashCode(), data4.hashCode());

        // Different objects should generally have different hash codes (not required, but good practice)
        Tuple remainder2 = Tuple.from("different", "key");
        DataInKeySpacePath data5 = new DataInKeySpacePath(testPath, remainder2, value);
        assertNotEquals(data1.hashCode(), data5.hashCode());
    }

    @Test
    void testToString() {
        final String rootUuid = UUID.randomUUID().toString();
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("test", KeyType.STRING, rootUuid));

        KeySpacePath testPath = root.path("test");
        Tuple remainder = Tuple.from("key1", "key2");
        byte[] value = Tuple.from("value1").pack();

        DataInKeySpacePath data = new DataInKeySpacePath(testPath, remainder, value);

        String result = data.toString();

        // Verify the string contains expected components
        assertTrue(result.contains(rootUuid));
        assertTrue(result.contains("test"));
        assertTrue(result.contains("key1"), "toString should contain remainder elements");
        assertTrue(result.contains("key2"), "toString should contain remainder elements");

        // Test with null remainder
        DataInKeySpacePath dataWithNullRemainder = new DataInKeySpacePath(testPath, null, value);
        String resultWithNull = dataWithNullRemainder.toString();
        assertTrue(resultWithNull.contains("null"), "toString should contain 'null' for null remainder");
    }
}
