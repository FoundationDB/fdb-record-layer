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
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests for {@link ResolvedKeySpacePath}.
 */
@Tag(Tags.RequiresFDB)
class ResolvedKeySpacePathTest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();

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

    @Nonnull
    static Stream<Arguments> testEqualsHashCode() {
        return ParameterizedTestUtils.cartesianProduct(
                Arrays.stream(KeyType.values()),
                ParameterizedTestUtils.booleans("constantDirectory"),
                ParameterizedTestUtils.booleans("differenceInParent")
        );
    }

    /**
     * Test equals and hashCode contracts for depth 1 directories.
     */
    @ParameterizedTest
    @MethodSource("testEqualsHashCode")
    void testEqualsHashCode(@Nonnull KeyType keyType, boolean constantDirectory, boolean differenceInParent) {
        @Nonnull TestValuePair values = TYPE_TEST_VALUES.get(keyType);

        // Create a single KeySpace with the appropriate directory structure
        KeySpaceDirectory rootDir = new KeySpaceDirectory("root", KeyType.STRING, "root");
        KeySpaceDirectory childDir = constantDirectory
                ? new KeySpaceDirectory("test", keyType, values.getValue1())
                : new KeySpaceDirectory("test", keyType);
        rootDir.addSubdirectory(childDir);

        // Optionally add a constant child for the differenceInParent test
        if (differenceInParent) {
            KeySpaceDirectory constantChild = new KeySpaceDirectory("constant", KeyType.STRING, "Constant");
            childDir.addSubdirectory(constantChild);
        }

        KeySpace keySpace = new KeySpace(rootDir);

        // Create paths from the same KeySpace
        KeySpacePath rootPath1 = keySpace.path("root");
        KeySpacePath rootPath2 = keySpace.path("root");

        KeySpacePath childPath1;
        KeySpacePath childPath2;
        if (constantDirectory) {
            childPath1 = rootPath1.add("test");
            childPath2 = rootPath2.add("test");
        } else {
            childPath1 = rootPath1.add("test", values.getValue1());
            childPath2 = rootPath2.add("test", values.getValue1());
        }

        // Create ResolvedKeySpacePath instances
        ResolvedKeySpacePath resolvedRoot1 = new ResolvedKeySpacePath(null, rootPath1, new PathValue("root", null), null);
        ResolvedKeySpacePath resolvedRoot2 = new ResolvedKeySpacePath(null, rootPath2, new PathValue("root", null), null);

        ResolvedKeySpacePath path1 = new ResolvedKeySpacePath(resolvedRoot1, childPath1, new PathValue(values.getValue1(), null), null);
        ResolvedKeySpacePath path2 = new ResolvedKeySpacePath(resolvedRoot2, childPath2, new PathValue(values.getValue1(), null), null);

        if (differenceInParent) {
            KeySpacePath constantChildPath1 = childPath1.add("constant");
            KeySpacePath constantChildPath2 = childPath2.add("constant");
            path1 = new ResolvedKeySpacePath(path1, constantChildPath1, new PathValue("Constant", null), null);
            path2 = new ResolvedKeySpacePath(path2, constantChildPath2, new PathValue("Constant", null), null);
        }

        // Test equality contracts
        assertEquals(path1, path2, "Identical paths should be equal");
        assertEquals(path2, path1, "Symmetry: path2.equals(path1)");
        assertEquals(path1.hashCode(), path2.hashCode(), "Equal objects must have equal hash codes");

        // Test inequality when values differ (except NULL type which only has null values)
        if (keyType != KeyType.NULL) {
            if (constantDirectory) {
                // For constant directories, we need a different directory (and thus different KeySpace) to test different values
                // this doesn't really need to be parameterized by value type, since they will always be non-equal due
                // to the directories being different
                KeySpaceDirectory rootDir3 = new KeySpaceDirectory("root", KeyType.STRING, "root");
                KeySpaceDirectory childDir3 = new KeySpaceDirectory("test", keyType, values.getValue2());
                rootDir3.addSubdirectory(childDir3);
                KeySpace keySpace3 = new KeySpace(rootDir3);

                KeySpacePath rootPath3 = keySpace3.path("root");
                KeySpacePath childPath3 = rootPath3.add("test");

                ResolvedKeySpacePath resolvedRoot3 = new ResolvedKeySpacePath(null, rootPath3, new PathValue("root", null), null);
                ResolvedKeySpacePath path3 = new ResolvedKeySpacePath(resolvedRoot3, childPath3, new PathValue(values.getValue2(), null), null);

                assertNotEquals(path1, path3, "Paths with different constant values should not be equal");
            } else {
                // For non-constant directories, we can use the same directory with different values
                KeySpacePath childPath3 = rootPath1.add("test", values.getValue2());
                ResolvedKeySpacePath path3 = new ResolvedKeySpacePath(resolvedRoot1, childPath3, new PathValue(values.getValue2(), null), null);
                assertNotEquals(path1, path3, "Paths with different values should not be equal");
            }

            // Test different resolved value (same logical, different resolved)
            KeySpacePath childPath4 = constantDirectory
                    ? rootPath1.add("test")
                    : rootPath1.add("test", values.getValue1());
            ResolvedKeySpacePath path4 = new ResolvedKeySpacePath(resolvedRoot1, childPath4, new PathValue(values.getValue2(), null), null);
            assertNotEquals(path4, path1, "Paths with different resolved values should not be equal");

            // Test different logical value (different logical, same resolved)
            if (!constantDirectory) {
                KeySpacePath childPath5 = rootPath1.add("test", values.getValue2());
                ResolvedKeySpacePath path5 = new ResolvedKeySpacePath(resolvedRoot1, childPath5, new PathValue(values.getValue1(), null), null);
                assertNotEquals(path5, path1, "Paths with different logical values should not be equal");
            }
        } else {
            assertNull(values.getValue2());
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
    void testRemainderComparedInEquals(boolean constantDirectory) {
        KeySpacePath innerPath = createKeySpacePath(createRootParent(), KeyType.STRING, "resolved", constantDirectory);
        PathValue value = new PathValue("resolved", null);
            
        ResolvedKeySpacePath path1 = new ResolvedKeySpacePath(null, innerPath, value, Tuple.from("remainder1"));
        ResolvedKeySpacePath path2 = new ResolvedKeySpacePath(null, innerPath, value, Tuple.from("remainder2"));
        ResolvedKeySpacePath path3 = new ResolvedKeySpacePath(null, innerPath, value, Tuple.from("remainder1"));

        assertNotEquals(path1, path2, "Paths with different remainders should not be equal");
        assertEquals(path1, path3, "Paths with the same remainder should be equal");
        assertEquals(path1.hashCode(), path3.hashCode(), "Paths with the same remainder should have same hashCode");
        ResolvedKeySpacePath nullRemainder = new ResolvedKeySpacePath(null, innerPath, value, null);
        assertNotEquals(path1, nullRemainder, "Path without a remainder should not be the equal to one with a remainder");
        assertNotEquals(nullRemainder, null, "Make sure null is properly handled in equals");
        IntStream.range(0, 10_000).mapToObj(i -> new ResolvedKeySpacePath(null, innerPath, value, Tuple.from(i)))
                .filter(path -> path.hashCode() != path1.hashCode())
                .findAny()
                .orElseThrow(() -> new AssertionError("Paths with different remainders should sometimes have different hash codes"));
    }

    /** Test withRemainder returns a new path with the updated remainder. */
    @ParameterizedTest
    @BooleanSource("constantDirectory")
    void testWithRemainder(boolean constantDirectory) {
        KeySpacePath innerPath = createKeySpacePath(createRootParent(), KeyType.STRING, "resolved", constantDirectory);
        PathValue value = new PathValue("resolved", null);
        Tuple originalRemainder = Tuple.from("remainder1");

        ResolvedKeySpacePath original = new ResolvedKeySpacePath(null, innerPath, value, originalRemainder);

        // Test changing remainder
        Tuple newRemainder = Tuple.from("remainder2");
        ResolvedKeySpacePath modified = original.withRemainder(newRemainder);

        assertEquals(newRemainder, modified.getRemainder());
        assertEquals(originalRemainder, original.getRemainder(), "Original should be unchanged");
        assertNotEquals(original, modified, "Paths with different remainders should not be equal");

        // Verify other fields are preserved
        assertEquals(original.toPath(), modified.toPath());
        assertEquals(original.getResolvedValue(), modified.getResolvedValue());
        assertEquals(original.getResolvedPathValue(), modified.getResolvedPathValue());

        // Verify modified is equal to a freshly created path with the new remainder
        ResolvedKeySpacePath freshlyCreated = new ResolvedKeySpacePath(null, innerPath, value, newRemainder);
        assertEquals(freshlyCreated, modified);
        assertEquals(modified, freshlyCreated);
        assertEquals(freshlyCreated.hashCode(), modified.hashCode());
    }

    /** Test withRemainder can set remainder to null. */
    @ParameterizedTest
    @BooleanSource("constantDirectory")
    void testWithRemainderSetToNull(boolean constantDirectory) {
        KeySpacePath innerPath = createKeySpacePath(createRootParent(), KeyType.STRING, "resolved", constantDirectory);
        PathValue value = new PathValue("resolved", null);
        Tuple originalRemainder = Tuple.from("remainder1");

        ResolvedKeySpacePath original = new ResolvedKeySpacePath(null, innerPath, value, originalRemainder);
        ResolvedKeySpacePath modified = original.withRemainder(null);

        assertNull(modified.getRemainder());
        assertEquals(originalRemainder, original.getRemainder(), "Original should be unchanged");
        assertNotEquals(original, modified);

        // Verify modified is equal to a freshly created path with null remainder
        ResolvedKeySpacePath freshlyCreated = new ResolvedKeySpacePath(null, innerPath, value, null);
        assertEquals(freshlyCreated, modified);
        assertEquals(modified, freshlyCreated);
        assertEquals(freshlyCreated.hashCode(), modified.hashCode());
    }

    /** Test withRemainder on a path that already has null remainder. */
    @ParameterizedTest
    @BooleanSource("constantDirectory")
    void testWithRemainderFromNull(boolean constantDirectory) {
        KeySpacePath innerPath = createKeySpacePath(createRootParent(), KeyType.STRING, "resolved", constantDirectory);
        PathValue value = new PathValue("resolved", null);

        ResolvedKeySpacePath original = new ResolvedKeySpacePath(null, innerPath, value, null);
        Tuple newRemainder = Tuple.from("newRemainder");
        ResolvedKeySpacePath modified = original.withRemainder(newRemainder);

        assertEquals(newRemainder, modified.getRemainder());
        assertNull(original.getRemainder(), "Original should be unchanged");
        assertNotEquals(original, modified);

        // Verify modified is equal to a freshly created path with the new remainder
        ResolvedKeySpacePath freshlyCreated = new ResolvedKeySpacePath(null, innerPath, value, newRemainder);
        assertEquals(freshlyCreated, modified);
        assertEquals(modified, freshlyCreated);
        assertEquals(freshlyCreated.hashCode(), modified.hashCode());
    }

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
        final KeySpaceDirectory parentDir = new KeySpaceDirectory("root", KeyType.STRING, "root");
        KeySpacePath parent = new KeySpace(parentDir).path("root");
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
