/*
 * KeySpacePathSerializerTest.java
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
import com.apple.test.ParameterizedTestUtils;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link KeySpacePathSerializer}.
 */
class KeySpacePathSerializerTest {

    private static final UUID TEST_UUID = UUID.randomUUID();
    private static final Map<KeyType, Object> KEY_TYPE_TEST_VALUES;

    static {
        // Using HashMap to allow null values (Map.of() doesn't allow nulls)
        Map<KeyType, Object> testValues = new HashMap<>();
        testValues.put(KeyType.NULL, null);
        testValues.put(KeyType.STRING, "test_string");
        testValues.put(KeyType.LONG, 12345L);
        testValues.put(KeyType.FLOAT, 3.14f);
        testValues.put(KeyType.DOUBLE, 2.71828);
        testValues.put(KeyType.BOOLEAN, true);
        testValues.put(KeyType.BYTES, new byte[]{1, 2, 3, (byte) 0xFF});
        testValues.put(KeyType.UUID, TEST_UUID);
        KEY_TYPE_TEST_VALUES = Collections.unmodifiableMap(testValues);
    }

    @Test
    void testKeyTypeTestValuesIncludesAllKeyTypes() {
        // Verify that KEY_TYPE_TEST_VALUES contains all KeyType enum values
        var allKeyTypes = Arrays.stream(KeyType.values()).collect(Collectors.toSet());
        var coveredKeyTypes = KEY_TYPE_TEST_VALUES.keySet();
        assertEquals(allKeyTypes, coveredKeyTypes);
    }

    @Test
    void testSerializeAndDeserializeSimplePath() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("app", KeyType.STRING, "myapp")
                        .addSubdirectory(new KeySpaceDirectory("tenant", KeyType.STRING)));

        KeySpacePath rootPath = root.path("app");
        KeySpacePath fullPath = rootPath.add("tenant", "tenant1");
        byte[] value = new byte[]{1, 2, 3, 4};

        DataInKeySpacePath data = new DataInKeySpacePath(fullPath, null, value);
        DataInKeySpacePath deserialized = serializeAndDeserialize(rootPath, data);

        assertEquals(fullPath.getDirectoryName(), deserialized.getPath().getDirectoryName());
        assertEquals("tenant1", deserialized.getPath().getValue());
        assertNull(deserialized.getRemainder());
        assertArrayEquals(value, deserialized.getValue());
    }

    @Test
    void testSerializeAndDeserializeWithRemainder() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("app", KeyType.STRING, "myapp")
                        .addSubdirectory(new KeySpaceDirectory("records", KeyType.STRING)));

        KeySpacePath rootPath = root.path("app");
        KeySpacePath fullPath = rootPath.add("records", "store1");
        Tuple remainder = Tuple.from("key1", "key2");
        byte[] value = new byte[]{10, 20, 30};

        DataInKeySpacePath data = new DataInKeySpacePath(fullPath, remainder, value);
        DataInKeySpacePath deserialized = serializeAndDeserialize(rootPath, data);

        assertEquals("store1", deserialized.getPath().getValue());
        assertNotNull(deserialized.getRemainder());
        assertEquals(remainder, deserialized.getRemainder());
        assertArrayEquals(value, deserialized.getValue());
    }

    @Test
    void testSerializeAndDeserializeMultiLevelPath() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.STRING, "r")
                        .addSubdirectory(new KeySpaceDirectory("level1", KeyType.STRING)
                                .addSubdirectory(new KeySpaceDirectory("level2", KeyType.LONG)
                                        .addSubdirectory(new KeySpaceDirectory("level3", KeyType.STRING)))));

        KeySpacePath rootPath = root.path("root");
        KeySpacePath fullPath = rootPath
                .add("level1", "l1value")
                .add("level2", 42L)
                .add("level3", "l3value");
        byte[] value = new byte[]{100};

        DataInKeySpacePath data = new DataInKeySpacePath(fullPath, null, value);
        DataInKeySpacePath deserialized = serializeAndDeserialize(rootPath, data);

        assertEquals("l3value", deserialized.getPath().getValue());
        assertArrayEquals(value, deserialized.getValue());
    }

    static Stream<Arguments> testSerializeDeserializeAllKeyTypes() {
        return ParameterizedTestUtils.cartesianProduct(
                KEY_TYPE_TEST_VALUES.entrySet().stream(),
                ParameterizedTestUtils.booleans("isConstant"));
    }

    @ParameterizedTest
    @MethodSource
    void testSerializeDeserializeAllKeyTypes(Map.Entry<KeyType, Object> typeAndValue, boolean isConstant) {
        final KeyType keyType = typeAndValue.getKey();
        final Object value = typeAndValue.getValue();
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.STRING, "root")
                        .addSubdirectory(isConstant ?
                                         new KeySpaceDirectory("typed", keyType, value) :
                                         new KeySpaceDirectory("typed", keyType)));

        KeySpacePath rootPath = root.path("root");
        KeySpacePath fullPath = rootPath.add("typed", value);
        byte[] dataValue = new byte[]{1};

        DataInKeySpacePath data = new DataInKeySpacePath(fullPath, null, dataValue);
        final DataInKeySpacePath deserialized = serializeAndDeserialize(rootPath, data);

        if (value instanceof byte[]) {
            assertArrayEquals((byte[]) value, (byte[]) deserialized.getPath().getValue());
        } else {
            assertEquals(value, deserialized.getPath().getValue());
        }
        assertArrayEquals(dataValue, deserialized.getValue());
    }

    @Test
    void testSerializeWithIntegerForLongKeyType() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.STRING, "root")
                        .addSubdirectory(new KeySpaceDirectory("long_dir", KeyType.LONG)));

        KeySpacePath rootPath = root.path("root");
        // Pass an Integer for a LONG key type
        KeySpacePath fullPath = rootPath.add("long_dir", 42);
        byte[] value = new byte[]{1};

        DataInKeySpacePath data = new DataInKeySpacePath(fullPath, null, value);
        final DataInKeySpacePath deserialized = serializeAndDeserialize(rootPath, data);

        // Should deserialize as Long
        assertEquals(42L, deserialized.getPath().getValue());
    }

    @Test
    void testSerializeRootOnly() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.STRING, "rootval"));

        KeySpacePath rootPath = root.path("root");
        byte[] value = new byte[]{5, 6, 7};

        DataInKeySpacePath data = new DataInKeySpacePath(rootPath, null, value);
        final DataInKeySpacePath deserialized = serializeAndDeserialize(rootPath, data);

        assertEquals("rootval", deserialized.getPath().getValue());
        assertArrayEquals(value, deserialized.getValue());
    }

    @Test
    void testSerializePathNotContainedInRoot() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("app1", KeyType.STRING, "app1")
                        .addSubdirectory(new KeySpaceDirectory("tenant", KeyType.STRING)));

        KeySpace otherRoot = new KeySpace(
                new KeySpaceDirectory("app2", KeyType.STRING, "app2")
                        .addSubdirectory(new KeySpaceDirectory("tenant", KeyType.STRING)));

        KeySpacePath rootPath = root.path("app1");
        KeySpacePath otherPath = otherRoot.path("app2").add("tenant", "t1");
        byte[] value = new byte[]{1};

        DataInKeySpacePath data = new DataInKeySpacePath(otherPath, null, value);
        KeySpacePathSerializer serializer = new KeySpacePathSerializer(rootPath);
        assertThrows(RecordCoreArgumentException.class, () -> serializer.serialize(data));
    }

    @Test
    void testSerializeWithEmptyValue() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.STRING, "root"));

        KeySpacePath rootPath = root.path("root");
        byte[] value = new byte[]{};

        DataInKeySpacePath data = new DataInKeySpacePath(rootPath, null, value);
        final DataInKeySpacePath deserialized = serializeAndDeserialize(rootPath, data);

        assertArrayEquals(value, deserialized.getValue());
        assertEquals(0, deserialized.getValue().length);
    }

    @Test
    void testDeserializeInvalidProto() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.STRING, "root"));

        KeySpacePath rootPath = root.path("root");
        KeySpacePathSerializer serializer = new KeySpacePathSerializer(rootPath);

        // Create invalid proto with no value field set
        KeySpaceProto.DataInKeySpacePath invalidProto = KeySpaceProto.DataInKeySpacePath.newBuilder().build();

        assertThrows(RecordCoreArgumentException.class, () -> serializer.deserialize(invalidProto));
    }

    @Test
    void testRoundTripWithComplexRemainder() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("db", KeyType.STRING, "database"));

        KeySpacePath rootPath = root.path("db");
        Tuple remainder = Tuple.from("string", 123L, 3.14, true, new byte[]{1, 2});
        byte[] value = new byte[]{9, 8, 7};

        DataInKeySpacePath data = new DataInKeySpacePath(rootPath, remainder, value);
        final DataInKeySpacePath deserialized = serializeAndDeserialize(rootPath, data);

        assertEquals(remainder, deserialized.getRemainder());
        assertArrayEquals(value, deserialized.getValue());
    }

    @Test
    void testSerializeNullKeyType() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.STRING, "root")
                        .addSubdirectory(new KeySpaceDirectory("null_dir", KeyType.NULL)));

        KeySpacePath rootPath = root.path("root");
        KeySpacePath fullPath = rootPath.add("null_dir");
        byte[] value = new byte[]{1};

        DataInKeySpacePath data = new DataInKeySpacePath(fullPath, null, value);
        final DataInKeySpacePath deserialized = serializeAndDeserialize(rootPath, data);

        assertNull(deserialized.getPath().getValue());
    }

    static Stream<Arguments> testSerializeDeserializeDifferentRoot() {
        return Stream.of(
                Arguments.of(Named.of("Same", new KeySpaceDirectory("tenant", KeyType.STRING)
                        .addSubdirectory(new KeySpaceDirectory("record", KeyType.LONG))),
                        Named.of("successful", null)),
                Arguments.of(Named.of("Extra subdirectories in destination",
                                new KeySpaceDirectory("tenant", KeyType.STRING)
                                        .addSubdirectory(new KeySpaceDirectory("users", KeyType.STRING))
                                        .addSubdirectory(new KeySpaceDirectory("groups", KeyType.BYTES))
                                        .addSubdirectory(new KeySpaceDirectory("record", KeyType.LONG))
                                        .addSubdirectory(new KeySpaceDirectory("settings", KeyType.BOOLEAN))),
                        Named.of("successful", null)),
                Arguments.of(Named.of("Different constant",
                        new KeySpaceDirectory("tenant", KeyType.STRING)
                                .addSubdirectory(new KeySpaceDirectory("record", KeyType.LONG, 104L))),
                        RecordCoreArgumentException.class),
                Arguments.of(Named.of("Different type",
                                new KeySpaceDirectory("tenant", KeyType.STRING)
                                        .addSubdirectory(new KeySpaceDirectory("record", KeyType.STRING))),
                        RecordCoreArgumentException.class),
                Arguments.of(Named.of("Null type",
                                new KeySpaceDirectory("tenant", KeyType.STRING)
                                        .addSubdirectory(new KeySpaceDirectory("record", KeyType.NULL))),
                        RecordCoreArgumentException.class),
                Arguments.of(Named.of("Different name",
                                new KeySpaceDirectory("tenant", KeyType.STRING)
                                        .addSubdirectory(new KeySpaceDirectory("compact disc", KeyType.STRING))),
                        NoSuchDirectoryException.class),
                Arguments.of(Named.of("Missing subdirectory",
                        new KeySpaceDirectory("tenant", KeyType.STRING)),
                        NoSuchDirectoryException.class));
    }

    @ParameterizedTest
    @MethodSource
    void testSerializeDeserializeDifferentRoot(KeySpaceDirectory destDirectory,
                                               @Nullable Class<? extends Exception> errorType) {
        KeySpace keySpace = new KeySpace(
                new KeySpaceDirectory("source_app", KeyType.STRING, "app1")
                        .addSubdirectory(new KeySpaceDirectory("tenant", KeyType.STRING)
                                .addSubdirectory(new KeySpaceDirectory("record", KeyType.LONG))),
                new KeySpaceDirectory("dest_app", KeyType.STRING, "app2")
                        .addSubdirectory(destDirectory));

        // Create data in source hierarchy
        KeySpacePath sourcePath = keySpace.path("source_app")
                .add("tenant", "tenant1")
                .add("record", 42L);
        byte[] value = new byte[]{10, 20, 30};
        DataInKeySpacePath sourceData = new DataInKeySpacePath(sourcePath, null, value);

        // Serialize from source
        KeySpacePathSerializer sourceSerializer = new KeySpacePathSerializer(keySpace.path("source_app"));
        KeySpaceProto.DataInKeySpacePath serialized = sourceSerializer.serialize(sourceData);

        // Deserialize to destination
        KeySpacePathSerializer destSerializer = new KeySpacePathSerializer(keySpace.path("dest_app"));

        if (errorType == null) {
            DataInKeySpacePath deserializedData = destSerializer.deserialize(serialized);

            assertEquals("dest_app", deserializedData.getPath().getParent().getParent().getDirectoryName());
            assertEquals("tenant1", deserializedData.getPath().getParent().getValue());
            assertEquals(42L, deserializedData.getPath().getValue());
            assertArrayEquals(new byte[] {10, 20, 30}, deserializedData.getValue());
        } else {
            assertThrows(errorType, () -> destSerializer.deserialize(serialized));
        }
    }

    @Test
    void testDeserializeSameStructureDifferentRootValue() {
        // Create a KeySpace with a single root having multiple children with identical structures
        KeySpace keySpace = new KeySpace(
                new KeySpaceDirectory("environments", KeyType.NULL)
                        .addSubdirectory(new KeySpaceDirectory("production", KeyType.STRING, "prod")
                                .addSubdirectory(new KeySpaceDirectory("database", KeyType.STRING)
                                        .addSubdirectory(new KeySpaceDirectory("table", KeyType.STRING))))
                        .addSubdirectory(new KeySpaceDirectory("staging", KeyType.STRING, "stage")
                                .addSubdirectory(new KeySpaceDirectory("database", KeyType.STRING)
                                        .addSubdirectory(new KeySpaceDirectory("table", KeyType.STRING)))));

        // Create data in production
        KeySpacePath sourcePath = keySpace.path("environments").add("production")
                .add("database", "db1")
                .add("table", "users");
        Tuple remainder = Tuple.from("primary_key", 12345L);
        byte[] value = new byte[]{(byte) 0xFF, 0x00, 0x11};
        DataInKeySpacePath sourceData = new DataInKeySpacePath(sourcePath, remainder, value);

        // Serialize from production
        KeySpacePathSerializer sourceSerializer = new KeySpacePathSerializer(keySpace.path("environments").add("production"));
        KeySpaceProto.DataInKeySpacePath serialized = sourceSerializer.serialize(sourceData);

        // Deserialize to staging
        KeySpacePathSerializer destSerializer = new KeySpacePathSerializer(keySpace.path("environments").add("staging"));
        DataInKeySpacePath deserializedData = destSerializer.deserialize(serialized);

        // Verify the root value changed but path structure and data preserved
        assertEquals("staging", deserializedData.getPath().getParent().getParent().getDirectoryName());
        assertEquals("stage", deserializedData.getPath().getParent().getParent().getValue());

        // The logical path values should be preserved
        assertEquals("db1", deserializedData.getPath().getParent().getValue());
        assertEquals("users", deserializedData.getPath().getValue());
        assertEquals(remainder, deserializedData.getRemainder());
        assertArrayEquals(value, deserializedData.getValue());
    }

    @Test
    void testSerializeDirectoryLayerDirectoryWithStringValue() {
        // DirectoryLayerDirectory has KeyType.LONG but typically accepts String values
        // The serializer uses KeyType.typeOf to determine the value is a String and serialize it as STRING
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("app", KeyType.STRING, "myapp")
                        .addSubdirectory(new DirectoryLayerDirectory("tenant")
                                .addSubdirectory(new KeySpaceDirectory("data", KeyType.STRING))));

        KeySpacePath rootPath = root.path("app");
        // Use String value (the typical usage with DirectoryLayerDirectory)
        KeySpacePath fullPath = rootPath.add("tenant", "my_tenant").add("data", "mydata");
        byte[] value = new byte[]{1, 2, 3};

        DataInKeySpacePath data = new DataInKeySpacePath(fullPath, null, value);
        DataInKeySpacePath deserialized = serializeAndDeserialize(rootPath, data);

        // Verify the String value was preserved
        assertEquals("my_tenant", deserialized.getPath().getParent().getValue());
        assertEquals("mydata", deserialized.getPath().getValue());
        assertArrayEquals(value, deserialized.getValue());
    }

    @Test
    void testSerializeDirectoryLayerDirectoryWithConstant() {
        // Test DirectoryLayerDirectory with a constant String value
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("app", KeyType.STRING, "myapp")
                        .addSubdirectory(new DirectoryLayerDirectory("tenant", "my_tenant")
                                .addSubdirectory(new KeySpaceDirectory("data", KeyType.STRING))));

        KeySpacePath rootPath = root.path("app");
        KeySpacePath fullPath = rootPath.add("tenant", "my_tenant").add("data", "mydata");
        byte[] value = new byte[]{1, 2, 3};

        DataInKeySpacePath data = new DataInKeySpacePath(fullPath, null, value);
        DataInKeySpacePath deserialized = serializeAndDeserialize(rootPath, data);

        // Verify the constant String value was preserved
        assertEquals("my_tenant", deserialized.getPath().getParent().getValue());
        assertEquals("mydata", deserialized.getPath().getValue());
        assertArrayEquals(value, deserialized.getValue());
    }

    @Test
    void testSerializeDirectoryLayerDirectoryMultiLevel() {
        // Test multiple DirectoryLayerDirectory nodes in a path with String values
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.STRING, "r")
                        .addSubdirectory(new DirectoryLayerDirectory("app")
                                .addSubdirectory(new DirectoryLayerDirectory("tenant")
                                        .addSubdirectory(new KeySpaceDirectory("record", KeyType.LONG)))));

        KeySpacePath rootPath = root.path("root");
        // Use String values for DirectoryLayerDirectory nodes
        KeySpacePath fullPath = rootPath
                .add("app", "my_app")
                .add("tenant", "my_tenant")
                .add("record", 12345L);
        byte[] value = new byte[]{10, 20};

        DataInKeySpacePath data = new DataInKeySpacePath(fullPath, null, value);
        DataInKeySpacePath deserialized = serializeAndDeserialize(rootPath, data);

        assertEquals("my_app", deserialized.getPath().getParent().getParent().getValue());
        assertEquals("my_tenant", deserialized.getPath().getParent().getValue());
        assertEquals(12345L, deserialized.getPath().getValue());
        assertArrayEquals(value, deserialized.getValue());
    }

    @Test
    void testDeserializeDirectoryLayerDirectoryToDifferentRoot() {
        // Test that DirectoryLayerDirectory paths can be serialized from one root and deserialized to another
        KeySpace keySpace = new KeySpace(
                new KeySpaceDirectory("source_app", KeyType.STRING, "app1")
                        .addSubdirectory(new DirectoryLayerDirectory("tenant")
                                .addSubdirectory(new KeySpaceDirectory("record", KeyType.LONG))),
                new KeySpaceDirectory("dest_app", KeyType.STRING, "app2")
                        .addSubdirectory(new DirectoryLayerDirectory("tenant")
                                .addSubdirectory(new KeySpaceDirectory("record", KeyType.LONG))));

        // Create data in source hierarchy with String value for DirectoryLayerDirectory
        KeySpacePath sourcePath = keySpace.path("source_app")
                .add("tenant", "tenant1")
                .add("record", 123L);
        byte[] value = new byte[]{5, 6, 7};
        DataInKeySpacePath sourceData = new DataInKeySpacePath(sourcePath, null, value);

        // Serialize from source
        KeySpacePathSerializer sourceSerializer = new KeySpacePathSerializer(keySpace.path("source_app"));
        KeySpaceProto.DataInKeySpacePath serialized = sourceSerializer.serialize(sourceData);

        // Deserialize to destination
        KeySpacePathSerializer destSerializer = new KeySpacePathSerializer(keySpace.path("dest_app"));
        DataInKeySpacePath deserializedData = destSerializer.deserialize(serialized);

        // Verify structure is preserved
        assertEquals("dest_app", deserializedData.getPath().getParent().getParent().getDirectoryName());
        assertEquals("tenant1", deserializedData.getPath().getParent().getValue());
        assertEquals(123L, deserializedData.getPath().getValue());
        assertArrayEquals(value, deserializedData.getValue());
    }

    @Nonnull
    private static DataInKeySpacePath serializeAndDeserialize(final KeySpacePath rootPath, final DataInKeySpacePath data) {
        final KeySpacePathSerializer serializer = new KeySpacePathSerializer(rootPath);
        return serializer.deserialize(serializer.serialize(data));
    }
}
