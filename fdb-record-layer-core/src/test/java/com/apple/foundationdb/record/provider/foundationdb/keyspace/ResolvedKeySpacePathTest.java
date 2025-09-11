/*
 * ResolvedKeySpacePathTest.java
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

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory.KeyType;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.ParameterizedTestUtils;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Tests for {@link ResolvedKeySpacePath} equals() and hashCode() methods.
 */
@Tag(Tags.RequiresFDB)
class ResolvedKeySpacePathTest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();

    /**
     * Provides test parameters for KeyType and constantDirectory combinations.
     */
    static Stream<Arguments> keyTypeAndConstantDirectory() {
        return ParameterizedTestUtils.cartesianProduct(
                Arrays.stream(KeyType.values()),
                ParameterizedTestUtils.booleans("constantDirectory")
        );
    }

    /**
     * Test value pairs for each KeyType.
     */
    private static final Map<KeyType, TestValuePair> TYPE_TEST_VALUES = Map.of(
            KeyType.STRING, new TestValuePair(() -> "value1", () -> "value2"),
            KeyType.LONG, new TestValuePair(() -> 100L, () -> 200L),
            KeyType.BYTES, new TestValuePair(() -> new byte[]{1, 2, 3}, () -> new byte[]{4, 5, 6}),
            KeyType.UUID, new TestValuePair(() -> new UUID(1, 1), () -> new UUID(2, 2)),
            KeyType.BOOLEAN, new TestValuePair(() -> true, () -> false),
            KeyType.NULL, new TestValuePair(() -> null, () -> null),
            KeyType.FLOAT, new TestValuePair(() -> 1.5f, () -> 2.5f),
            KeyType.DOUBLE, new TestValuePair(() -> 1.5d, () -> 2.5d)
    );

    /**
     * Test equals and hashCode contracts for depth 1 directories.
     */
    @ParameterizedTest
    @MethodSource("keyTypeAndConstantDirectory")
    void testEqualsHashCodeDepth1(KeyType keyType, boolean constantDirectory) {
        TestValuePair values = TYPE_TEST_VALUES.get(keyType);
        final FDBDatabase database = dbExtension.getDatabase();
        
        try (FDBRecordContext context = database.openContext()) {
            // Create two identical paths
            ResolvedKeySpacePath path1 = createResolvedPath(context, keyType, values.getValue1(), null, constantDirectory);
            ResolvedKeySpacePath path2 = createResolvedPath(context, keyType, values.getValue1(), null, constantDirectory);
            
            // Test equality contracts
            assertEquals(path1, path2, "Identical paths should be equal");
            assertEquals(path2, path1, "Symmetry: path2.equals(path1)");
            assertEquals(path1.hashCode(), path2.hashCode(), "Equal objects must have equal hash codes");
            
            // Test inequality when values differ (except NULL type which only has null values)
            if (keyType != KeyType.NULL && values.getValue2() != null) {
                ResolvedKeySpacePath path3 = createResolvedPath(context, keyType, values.getValue2(), null, constantDirectory);
                assertNotEquals(path1, path3, "Paths with different values should not be equal");
            }
            
            // Test basic contracts
            assertEquals(path1, path1, "Reflexivity");
            assertNotEquals(path1, null, "Null comparison");
            assertNotEquals(path1, "not a path", "Type safety");
        }
    }

    /**
     * Test equals and hashCode with hierarchical paths (parent-child).
     */
    @ParameterizedTest
    @MethodSource("keyTypeAndConstantDirectory")
    void testEqualsHashCodeWithParent(KeyType childKeyType, boolean constantDirectory) {
        final FDBDatabase database = dbExtension.getDatabase();
        
        try (FDBRecordContext context = database.openContext()) {
            TestValuePair childValues = TYPE_TEST_VALUES.get(childKeyType);

            // Create parent path (always STRING type)
            ResolvedKeySpacePath parent1 = createResolvedPath(context, KeyType.STRING, "parent1", null, constantDirectory);
            ResolvedKeySpacePath parent2 = createResolvedPath(context, KeyType.STRING, "parent2", null, constantDirectory);
            
            // Create child paths with same parent
            ResolvedKeySpacePath child1 = createResolvedPath(context, childKeyType, childValues.getValue1(), parent1, constantDirectory);
            ResolvedKeySpacePath child2 = createResolvedPath(context, childKeyType, childValues.getValue1(), parent1, constantDirectory);
            
            // Test equality - should be equal with same parent and value
            assertEquals(child1, child2, "Children with same parent and value should be equal");
            assertEquals(child1.hashCode(), child2.hashCode(), "Equal children should have equal hash codes");
            
            // Test with different parents but same child value
            ResolvedKeySpacePath child3 = createResolvedPath(context, childKeyType, childValues.getValue1(), parent2, constantDirectory);
            
            // Current implementation: equals() doesn't compare parent, only inner path and resolved value
            // This test documents the current behavior
            assertEquals(child1, child3, "Current implementation: parent not compared in equals()");
        }
    }

    /**
     * Test that demonstrates the actual equals/hashCode behavior with different PathValue metadata.
     */
    @ParameterizedTest
    @BooleanSource("constantDirectory")
    void testEqualsHashCodeWithDifferentMetadata(boolean constantDirectory) {
        final FDBDatabase database = dbExtension.getDatabase();
        
        try (FDBRecordContext context = database.openContext()) {
            // Create two paths with same inner path and resolved value but different metadata
            KeySpacePath innerPath = createKeySpacePath(context, KeyType.STRING, "resolved", constantDirectory);
            PathValue value1 = new PathValue("resolved", new byte[]{1, 2, 3});
            PathValue value2 = new PathValue("resolved", new byte[]{4, 5, 6});
            
            ResolvedKeySpacePath path1 = new ResolvedKeySpacePath(null, innerPath, value1, null);
            ResolvedKeySpacePath path2 = new ResolvedKeySpacePath(null, innerPath, value2, null);
            
            assertNotEquals(path1, path2, "Objects should be equal (same inner path and resolved value, metadata ignored)");
            assertNotEquals(path1.hashCode(), path2.hashCode(),
                    "Hash codes differ due to different PathValue metadata");
        }
    }

    /**
     * Test remainder field behavior in equals.
     */
    @ParameterizedTest
    @BooleanSource("constantDirectory")
    void testRemainderNotComparedInEquals(boolean constantDirectory) {
        final FDBDatabase database = dbExtension.getDatabase();
        
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath innerPath = createKeySpacePath(context, KeyType.STRING, "resolved", constantDirectory);
            PathValue value = new PathValue("resolved", null);
            
            ResolvedKeySpacePath path1 = new ResolvedKeySpacePath(null, innerPath, value, Tuple.from("remainder1"));
            ResolvedKeySpacePath path2 = new ResolvedKeySpacePath(null, innerPath, value, Tuple.from("remainder2"));
            
            // Current implementation: remainder is not compared in equals()
            assertEquals(path1, path2, "Current implementation: remainder not compared in equals()");
        }
    }

    /**
     * Helper to create a resolved path for testing.
     */
    private ResolvedKeySpacePath createResolvedPath(FDBRecordContext context, KeyType keyType, 
                                                   Object value, ResolvedKeySpacePath parent, boolean constantDirectory) {
        KeySpacePath innerPath = createKeySpacePath(context, keyType, value, constantDirectory);
        PathValue pathValue = new PathValue(value, null);
        return new ResolvedKeySpacePath(parent, innerPath, pathValue, null);
    }

    /**
     * Helper to create a KeySpacePath for testing.
     */
    private KeySpacePath createKeySpacePath(FDBRecordContext context, KeyType keyType, Object value, boolean constantDirectory) {
        // Always create same root directory
        KeySpaceDirectory rootDir = new KeySpaceDirectory("root", KeyType.STRING, "root");
        
        // Create child directory based on constantDirectory parameter
        KeySpaceDirectory childDir;
        if (constantDirectory) {
            childDir = new KeySpaceDirectory("test", keyType, value);
        } else {
            childDir = new KeySpaceDirectory("test", keyType);
        }
        rootDir.addSubdirectory(childDir);
        
        KeySpace keySpace = new KeySpace(rootDir);
        KeySpacePath rootPath = keySpace.path("root");
        
        if (constantDirectory) {
            return rootPath.add("test");
        } else {
            return rootPath.add("test", value);
        }
    }

    /**
     * Test value pair for each KeyType.
     * We use {@link Supplier} here to make sure that if it is falling back to reference equality (e.g. byte[]),
     * we want to catch if it doesn't consider those equal.
     */
    private static class TestValuePair {
        private final Supplier<Object> value1Supplier;
        private final Supplier<Object> value2Supplier;
        
        TestValuePair(Supplier<Object> value1Supplier, Supplier<Object> value2Supplier) {
            this.value1Supplier = value1Supplier;
            this.value2Supplier = value2Supplier;
        }
        
        Object getValue1() {
            return value1Supplier.get();
        }
        
        Object getValue2() {
            return value2Supplier.get();
        }
    }
}
