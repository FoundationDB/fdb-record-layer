/*
 * CardinalityValueTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2023-2030 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class CardinalityValueTest {

    private static final Type.Array INT_ARRAY_TYPE = new Type.Array(Type.primitiveType(Type.TypeCode.INT));
    private static final Type.Array STRING_ARRAY_TYPE = new Type.Array(Type.primitiveType(Type.TypeCode.STRING));

    private static final LiteralValue<List<Integer>> INT_ARRAY_1 =
            new LiteralValue<>(INT_ARRAY_TYPE, List.of(1, 2, 3));
    private static final LiteralValue<List<Integer>> INT_ARRAY_2 =
            new LiteralValue<>(INT_ARRAY_TYPE, List.of(4, 5));
    private static final LiteralValue<List<String>> STRING_ARRAY =
            new LiteralValue<>(STRING_ARRAY_TYPE, List.of("a", "b"));

    @Test
    void testEqualsAndHashCode() {
        final var c1 = new CardinalityValue(INT_ARRAY_1);
        final var c2 = new CardinalityValue(INT_ARRAY_1);
        final var cDifferentChild = new CardinalityValue(INT_ARRAY_2);
        final var cDifferentElementType = new CardinalityValue(STRING_ARRAY);

        // Reflexive
        Assertions.assertEquals(c1, c1);

        // Two instances with the same child are equal and have the same hash code
        Assertions.assertEquals(c1, c2);
        Assertions.assertEquals(c1.hashCode(), c2.hashCode());

        // Instances with different children are not equal
        Assertions.assertNotEquals(cDifferentChild, c1);
        Assertions.assertNotEquals(cDifferentElementType, c1);

        // Not equal to null or an unrelated type
        Assertions.assertNotEquals(null, c1);
        Assertions.assertNotEquals("not a Value", c1);
    }
}
