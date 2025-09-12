/*
 * PathValueTest.java
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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * Tests for {@link PathValue} equals() and hashCode() methods.
 */
class PathValueTest {

    /**
     * Test data for PathValue equality tests.
     */
    static Stream<Arguments> equalPathValuePairs() {
        return Stream.of(
                Arguments.of("null values", null, null, null, null),
                Arguments.of("same string values", "test", null, "test", null),
                Arguments.of("same long values", 42L, null, 42L, null),
                Arguments.of("same boolean values", true, null, true, null),
                Arguments.of("same byte[] values", new byte[] {1, 2, 3}, null, new byte[] {1, 2, 3}, null),
                Arguments.of("same metadata", "test", new byte[]{1, 2, 3}, "test", new byte[]{1, 2, 3})
        );
    }

    /**
     * Test data for PathValue inequality tests.
     */
    static Stream<Arguments> unequalPathValuePairs() {
        return Stream.of(
                Arguments.of("different string values", "test1", null, "test2", null),
                Arguments.of("different long values", 42L, null, 100L, null),
                Arguments.of("different boolean values", true, null, false, null),
                Arguments.of("different types", "string", null, 42L, null),
                Arguments.of("different metadata", "test", new byte[]{1, 2, 3}, "test", new byte[]{4, 5, 6}),
                Arguments.of("one null metadata", "test", new byte[]{1, 2, 3}, "test", null),
                Arguments.of("different resolved values", "test1", new byte[]{1, 2, 3}, "test2", new byte[]{1, 2, 3})
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("equalPathValuePairs")
    void testEqualsAndHashCodeForEqualValues(String description, Object resolvedValue1, byte[] metadata1, 
                                           Object resolvedValue2, byte[] metadata2) {
        PathValue value1 = new PathValue(resolvedValue1, metadata1);
        PathValue value2 = new PathValue(resolvedValue2, metadata2);
        
        assertEquals(value1, value2, "PathValues should be equal: " + description);
        assertEquals(value1.hashCode(), value2.hashCode(), "Equal PathValues should have equal hash codes: " + description);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("unequalPathValuePairs")
    void testNotEqualsForUnequalValues(String description, Object resolvedValue1, byte[] metadata1, 
                                     Object resolvedValue2, byte[] metadata2) {
        PathValue value1 = new PathValue(resolvedValue1, metadata1);
        PathValue value2 = new PathValue(resolvedValue2, metadata2);
        
        assertNotEquals(value1, value2, "PathValues should not be equal: " + description);
    }
}
