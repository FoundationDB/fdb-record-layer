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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
    @Nonnull
    static Stream<Arguments> testEqualsHashCode() {
        return ParameterizedTestUtils.cartesianProduct(
                Arrays.stream(KeyType.values()),
                ParameterizedTestUtils.booleans("constantDirectory"),
                ParameterizedTestUtils.booleans("differenceInParent")
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
    @MethodSource("testEqualsHashCode")
    void testEqualsHashCode(@Nonnull KeyType keyType, boolean constantDirectory, boolean differenceInParent) {
        @Nonnull TestValuePair values = TYPE_TEST_VALUES.get(keyType);
        // Test case 1: Same logical and resolved values (existing test)
        ResolvedKeySpacePath path1 = createResolvedPath(keyType, values.getValue1(), values.getValue1(),
                createRootParent(), constantDirectory, differenceInParent);
        ResolvedKeySpacePath path2 = createResolvedPath(keyType, values.getValue1(), values.getValue1(),
                createRootParent(), constantDirectory, differenceInParent);

        // Test equality contracts
        assertEquals(path1, path2, "Identical paths should be equal");
        assertEquals(path2, path1, "Symmetry: path2.equals(path1)");
        assertEquals(path1.hashCode(), path2.hashCode(), "Equal objects must have equal hash codes");
            
        // Test inequality when values differ (except NULL type which only has null values)
        if (keyType != KeyType.NULL && values.getValue2() != null) {
            ResolvedKeySpacePath path3 = createResolvedPath(keyType, values.getValue2(), createRootParent(), constantDirectory);
            assertNotEquals(path1, path3, "Paths with different values should not be equal");

            assertNotEquals(createResolvedPath(keyType, values.getValue1(), values.getValue2(), createRootParent(), constantDirectory, differenceInParent),
                    path1, "Paths with different resolved values should not be equal");
            assertNotEquals(createResolvedPath(keyType, values.getValue2(), values.getValue1(), createRootParent(), constantDirectory, differenceInParent),
                    path1, "Paths with different logical values should not be equal");
        }
            
        // Test basic contracts
        assertEquals(path1, path1, "Reflexivity");
        assertNotEquals(path1, null, "Null comparison");
        assertNotEquals(path1, "not a path", "Type safety");
    }

    /**
     * Test that demonstrates the actual equals/hashCode behavior with different PathValue metadata.
     */
    @ParameterizedTest
    @BooleanSource("constantDirectory")
    void testEqualsHashCodeWithDifferentMetadata(boolean constantDirectory) {
        // Create two paths with same inner path and resolved value but different metadata
        KeySpacePath innerPath = createKeySpacePath(createRootParent(), KeyType.STRING, "resolved", constantDirectory);
        PathValue value1 = new PathValue("resolved", new byte[]{1, 2, 3});
        PathValue value2 = new PathValue("resolved", new byte[]{4, 5, 6});
            
        ResolvedKeySpacePath path1 = new ResolvedKeySpacePath(null, innerPath, value1, null);
        ResolvedKeySpacePath path2 = new ResolvedKeySpacePath(null, innerPath, value2, null);
            
        assertNotEquals(path1, path2, "Objects should be equal (same inner path and resolved value, metadata ignored)");
        assertNotEquals(path1.hashCode(), path2.hashCode(),
                "Hash codes differ due to different PathValue metadata");
    }

    /**
     * Test remainder field behavior in equals.
     */
    @ParameterizedTest
    @BooleanSource("constantDirectory")
    void testRemainderNotComparedInEquals(boolean constantDirectory) {
        KeySpacePath innerPath = createKeySpacePath(createRootParent(), KeyType.STRING, "resolved", constantDirectory);
        PathValue value = new PathValue("resolved", null);
            
        ResolvedKeySpacePath path1 = new ResolvedKeySpacePath(null, innerPath, value, Tuple.from("remainder1"));
        ResolvedKeySpacePath path2 = new ResolvedKeySpacePath(null, innerPath, value, Tuple.from("remainder2"));
            
        // Current implementation: remainder is not compared in equals()
        assertEquals(path1, path2, "Current implementation: remainder not compared in equals()");
    }

    @Nonnull
    private ResolvedKeySpacePath createResolvedPath(@Nonnull KeyType keyType, @Nullable Object value,
                                                    @Nonnull ResolvedKeySpacePath parent, boolean constantDirectory) {
        return createResolvedPath(keyType, value, value, parent, constantDirectory, false);
    }

    @Nonnull
    private ResolvedKeySpacePath createResolvedPath(@Nonnull KeyType keyType,
                                                    @Nullable Object logicalValue, @Nullable Object resolvedValue,
                                                    @Nonnull ResolvedKeySpacePath parent,
                                                    boolean constantDirectory, final boolean addConstantChild) {
        KeySpacePath innerPath = createKeySpacePath(parent, keyType, logicalValue, constantDirectory);
        PathValue pathValue = new PathValue(resolvedValue, null);
        final ResolvedKeySpacePath resolvedKeySpacePath = new ResolvedKeySpacePath(parent, innerPath, pathValue, null);
        if (addConstantChild) {
            final ResolvedKeySpacePath resolvedPath = createResolvedPath(KeyType.STRING, "Constant", "Constant", resolvedKeySpacePath, true, false);
            System.out.println(resolvedPath);
            return resolvedPath;
        } else {
            System.out.println(resolvedKeySpacePath);
            return resolvedKeySpacePath;
        }
    }

    /**
     * Helper to create a KeySpacePath for testing.
     */
    @Nonnull
    private KeySpacePath createKeySpacePath(@Nonnull ResolvedKeySpacePath parent, @Nonnull KeyType keyType, @Nullable Object value,
                                            boolean constantDirectory) {
        // Create child directory based on constantDirectory parameter
        KeySpaceDirectory childDir;
        if (constantDirectory) {
            childDir = new KeySpaceDirectory("test", keyType, value);
        } else {
            childDir = new KeySpaceDirectory("test", keyType);
        }
        parent.getDirectory().addSubdirectory(childDir);
        
        if (constantDirectory) {
            return parent.toPath().add("test");
        } else {
            return parent.toPath().add("test", value);
        }
    }

    @Nonnull
    private static ResolvedKeySpacePath createRootParent() {
        KeySpacePath parent;
        final KeySpaceDirectory parentDir = new KeySpaceDirectory("root", KeyType.STRING, "root");
        KeySpace keySpace = new KeySpace(parentDir);
        parent = keySpace.path("root");
        return new ResolvedKeySpacePath(null, parent, new PathValue("root", null), null);
    }

    /**
     * Test value pair for each KeyType.
     * We use {@link Supplier} here to make sure that if it is falling back to reference equality (e.g. byte[]),
     * we want to catch if it doesn't consider those equal.
     */
    private static class TestValuePair {
        @Nonnull
        private final Supplier<Object> value1Supplier;
        @Nonnull
        private final Supplier<Object> value2Supplier;
        
        TestValuePair(@Nonnull Supplier<Object> value1Supplier, @Nonnull Supplier<Object> value2Supplier) {
            this.value1Supplier = value1Supplier;
            this.value2Supplier = value2Supplier;
        }
        
        @Nullable
        Object getValue1() {
            return value1Supplier.get();
        }
        
        @Nullable
        Object getValue2() {
            return value2Supplier.get();
        }
    }
}
