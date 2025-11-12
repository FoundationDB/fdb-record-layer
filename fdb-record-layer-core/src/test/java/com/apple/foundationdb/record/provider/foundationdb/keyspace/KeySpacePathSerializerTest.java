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
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;
import java.util.UUID;
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

    @Test
    void testSerializeAndDeserializeSimplePath() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("app", KeyType.STRING, "myapp")
                        .addSubdirectory(new KeySpaceDirectory("tenant", KeyType.STRING)));

        KeySpacePath rootPath = root.path("app");
        KeySpacePath fullPath = rootPath.add("tenant", "tenant1");
        byte[] value = new byte[]{1, 2, 3, 4};

        DataInKeySpacePath data = new DataInKeySpacePath(fullPath, null, value);

        KeySpacePathSerializer serializer = new KeySpacePathSerializer(rootPath);
        ByteString serialized = serializer.serialize(data);

        DataInKeySpacePath deserialized = serializer.deserialize(serialized);

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

        KeySpacePathSerializer serializer = new KeySpacePathSerializer(rootPath);
        ByteString serialized = serializer.serialize(data);

        DataInKeySpacePath deserialized = serializer.deserialize(serialized);

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

        KeySpacePathSerializer serializer = new KeySpacePathSerializer(rootPath);
        ByteString serialized = serializer.serialize(data);

        DataInKeySpacePath deserialized = serializer.deserialize(serialized);

        assertEquals("l3value", deserialized.getPath().getValue());
        assertArrayEquals(value, deserialized.getValue());
    }

    @ParameterizedTest
    @MethodSource("provideKeyTypes")
    void testSerializeDeserializeAllKeyTypes(KeyType keyType, Object value) {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.STRING, "root")
                        .addSubdirectory(new KeySpaceDirectory("typed", keyType)));

        KeySpacePath rootPath = root.path("root");
        KeySpacePath fullPath = rootPath.add("typed", value);
        byte[] dataValue = new byte[]{1};

        DataInKeySpacePath data = new DataInKeySpacePath(fullPath, null, dataValue);

        KeySpacePathSerializer serializer = new KeySpacePathSerializer(rootPath);
        ByteString serialized = serializer.serialize(data);

        DataInKeySpacePath deserialized = serializer.deserialize(serialized);

        if (value instanceof byte[]) {
            assertArrayEquals((byte[]) value, (byte[]) deserialized.getPath().getValue());
        } else {
            assertEquals(value, deserialized.getPath().getValue());
        }
        assertArrayEquals(dataValue, deserialized.getValue());
    }

    private static Stream<Arguments> provideKeyTypes() {
        UUID testUuid = UUID.randomUUID();
        return Stream.of(
                Arguments.of(KeyType.NULL, null),
                Arguments.of(KeyType.STRING, "test_string"),
                Arguments.of(KeyType.LONG, 12345L),
                Arguments.of(KeyType.FLOAT, 3.14f),
                Arguments.of(KeyType.DOUBLE, 2.71828),
                Arguments.of(KeyType.BOOLEAN, true),
                Arguments.of(KeyType.BOOLEAN, false),
                Arguments.of(KeyType.BYTES, new byte[]{1, 2, 3, (byte) 0xFF}),
                Arguments.of(KeyType.UUID, testUuid)
        );
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

        KeySpacePathSerializer serializer = new KeySpacePathSerializer(rootPath);
        ByteString serialized = serializer.serialize(data);

        DataInKeySpacePath deserialized = serializer.deserialize(serialized);

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

        KeySpacePathSerializer serializer = new KeySpacePathSerializer(rootPath);
        ByteString serialized = serializer.serialize(data);

        DataInKeySpacePath deserialized = serializer.deserialize(serialized);

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

        KeySpacePathSerializer serializer = new KeySpacePathSerializer(rootPath);
        ByteString serialized = serializer.serialize(data);

        DataInKeySpacePath deserialized = serializer.deserialize(serialized);

        assertArrayEquals(value, deserialized.getValue());
        assertEquals(0, deserialized.getValue().length);
    }

    @Test
    void testDeserializeInvalidProto() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.STRING, "root"));

        KeySpacePath rootPath = root.path("root");
        KeySpacePathSerializer serializer = new KeySpacePathSerializer(rootPath);

        // Create invalid ByteString
        ByteString invalid = ByteString.copyFrom(new byte[]{1, 2, 3, 4, 5});

        assertThrows(RecordCoreArgumentException.class, () -> serializer.deserialize(invalid));
    }

    @Test
    void testRoundTripWithComplexRemainder() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("db", KeyType.STRING, "database"));

        KeySpacePath rootPath = root.path("db");
        Tuple remainder = Tuple.from("string", 123L, 3.14, true, new byte[]{1, 2});
        byte[] value = new byte[]{9, 8, 7};

        DataInKeySpacePath data = new DataInKeySpacePath(rootPath, remainder, value);

        KeySpacePathSerializer serializer = new KeySpacePathSerializer(rootPath);
        ByteString serialized = serializer.serialize(data);

        DataInKeySpacePath deserialized = serializer.deserialize(serialized);

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

        KeySpacePathSerializer serializer = new KeySpacePathSerializer(rootPath);
        ByteString serialized = serializer.serialize(data);

        DataInKeySpacePath deserialized = serializer.deserialize(serialized);

        assertNull(deserialized.getPath().getValue());
    }

    static Stream<Arguments> testSerializeDeserializeDifferentRoot() {
        return Stream.of(
                Arguments.of(Named.of("Same", new KeySpaceDirectory("tenant", KeyType.STRING)
                        .addSubdirectory(new KeySpaceDirectory("record", KeyType.LONG))),
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
        ByteString serialized1 = sourceSerializer.serialize(sourceData);

        // Deserialize to destination
        KeySpacePathSerializer destSerializer1 = new KeySpacePathSerializer(keySpace.path("dest_app"));

        if (errorType == null) {
            DataInKeySpacePath deserializedData = destSerializer1.deserialize(serialized1);

            assertEquals("dest_app", deserializedData.getPath().getParent().getParent().getDirectoryName());
            assertEquals("tenant1", deserializedData.getPath().getParent().getValue());
            assertEquals(42L, deserializedData.getPath().getValue());
            assertArrayEquals(new byte[] {10, 20, 30}, deserializedData.getValue());
        } else {
            assertThrows(errorType, () -> destSerializer1.deserialize(serialized1));
        }
    }

    @Test
    void testDeserializeIncompatiblePath() {
        // Create a KeySpace with two roots having incompatible structures
        KeySpace keySpace = new KeySpace(
                new KeySpaceDirectory("source", KeyType.STRING, "src")
                        .addSubdirectory(new KeySpaceDirectory("level1", KeyType.STRING)
                                .addSubdirectory(new KeySpaceDirectory("level2", KeyType.LONG))),
                new KeySpaceDirectory("dest", KeyType.STRING, "dst")
                        .addSubdirectory(new KeySpaceDirectory("level1", KeyType.STRING)));

        // Create data in source
        KeySpacePath sourcePath = keySpace.path("source")
                .add("level1", "value1")
                .add("level2", 100L);
        byte[] value = new byte[]{1, 2};
        DataInKeySpacePath sourceData = new DataInKeySpacePath(sourcePath, null, value);

        // Serialize from source
        KeySpacePathSerializer sourceSerializer = new KeySpacePathSerializer(keySpace.path("source"));
        ByteString serialized = sourceSerializer.serialize(sourceData);

        // Try to deserialize to incompatible destination - should fail
        KeySpacePathSerializer destSerializer = new KeySpacePathSerializer(keySpace.path("dest"));
        assertThrows(NoSuchDirectoryException.class, () -> destSerializer.deserialize(serialized));
    }

    @Test
    void testDeserializeWithExtraSubdirectoriesInDestination() {
        // Create a KeySpace with two roots where destination has extra subdirectories
        KeySpace keySpace = new KeySpace(
                new KeySpaceDirectory("source", KeyType.STRING, "src")
                        .addSubdirectory(new KeySpaceDirectory("data", KeyType.STRING)
                                .addSubdirectory(new KeySpaceDirectory("users", KeyType.STRING))),
                new KeySpaceDirectory("dest", KeyType.STRING, "dst")
                        .addSubdirectory(new KeySpaceDirectory("data", KeyType.STRING)
                                .addSubdirectory(new KeySpaceDirectory("users", KeyType.STRING))
                                .addSubdirectory(new KeySpaceDirectory("groups", KeyType.LONG))
                                .addSubdirectory(new KeySpaceDirectory("settings", KeyType.BOOLEAN))));

        // Create data using only the "data/users" path
        KeySpacePath sourcePath = keySpace.path("source").add("data", "records").add("users", "user123");
        byte[] value = new byte[]{5, 6, 7, 8};
        DataInKeySpacePath sourceData = new DataInKeySpacePath(sourcePath, null, value);

        // Serialize from source
        KeySpacePathSerializer sourceSerializer = new KeySpacePathSerializer(keySpace.path("source"));
        ByteString serialized = sourceSerializer.serialize(sourceData);

        // Deserialize to destination with extra directories - should succeed
        // The extra directories (groups, settings) are not part of the data path, so deserialization should work
        KeySpacePathSerializer destSerializer = new KeySpacePathSerializer(keySpace.path("dest"));
        DataInKeySpacePath deserializedData = destSerializer.deserialize(serialized);

        // Verify data is correctly deserialized
        assertEquals("user123", deserializedData.getPath().getValue());
        assertEquals("users", deserializedData.getPath().getDirectoryName());
        assertEquals("records", deserializedData.getPath().getParent().getValue());
        assertArrayEquals(value, deserializedData.getValue());
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
        ByteString serialized = sourceSerializer.serialize(sourceData);

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
}
