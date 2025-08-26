/*
 * ConstantArrayDistinctValueTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;

class ConstantArrayDistinctValueTest {

    @Test
    void rejectsNonConstantValues() {
        Assertions.assertThrowsExactly(VerifyException.class, () -> {
            new ConstantArrayDistinctValue(
                    QuantifiedObjectValue.of(CorrelationIdentifier.uniqueID(), new Type.Array())
            );
        });
    }

    @Test
    void rejectsNonArrayValues() {
        Assertions.assertThrowsExactly(VerifyException.class, () -> {
            new ConstantArrayDistinctValue(LiteralValue.ofScalar(42));
        });
    }

    static Stream<Arguments> arraysSource() {
        return Stream.of(
                arguments(ImmutableList.of(1, 2, 1, 2, 1, 2, 3), ImmutableList.of(1, 2, 3)),
                arguments(ImmutableList.of(1, 2, 3, 4, 5), ImmutableList.of(1, 2, 3, 4, 5)),
                arguments(
                        ImmutableList.of("val2", "val1", "val3", "val1", "val2"),
                        ImmutableList.of("val2", "val1", "val3")
                )
        );
    }

    @ParameterizedTest(name = "returnsArrayWithoutDuplicates[input={0}, expected={1}])")
    @MethodSource("arraysSource")
    void returnsArrayWithoutDuplicates(List<?> inputArray, List<?> expectedArray) {
        final var literalValue = LiteralValue.ofList(inputArray);

        final var constantArrayDistinctValue = new ConstantArrayDistinctValue(literalValue);
        final var actualArray = constantArrayDistinctValue.evalWithoutStore(EvaluationContext.EMPTY);

        Assertions.assertEquals(expectedArray, actualArray);
    }

    @ParameterizedTest(name = "builtInFunctionReturnsArrayWithoutDuplicates[input={0}, expected={1}])")
    @MethodSource("arraysSource")
    void builtInFunctionReturnsArrayWithoutDuplicates(List<?> inputArray, List<?> expectedArray) {
        final var fn = new ConstantArrayDistinctValue.ConstantArrayDistinctFn();

        final var retValue = fn.encapsulate(List.of(LiteralValue.ofList(inputArray)));

        Assertions.assertInstanceOf(ConstantArrayDistinctValue.class, retValue);
        Assertions.assertEquals(expectedArray, ((Value)retValue).evalWithoutStore(EvaluationContext.EMPTY));
    }

    @Test
    void withNewChildReplacesUnderlyingArray() {
        final var expectedArray = ImmutableList.of(1, 2, 3);
        final ConstantArrayDistinctValue value = new ConstantArrayDistinctValue(LiteralValue.ofList(ImmutableList.of(4, 5, 6)));

        final var newValue = value.withNewChild(LiteralValue.ofList(expectedArray));

        Assertions.assertEquals(expectedArray, newValue.evalWithoutStore(EvaluationContext.EMPTY));
    }

    @Test
    void equalsComparesUnderlyingValues() {
        final var val1 = new ConstantArrayDistinctValue(LiteralValue.ofList(ImmutableList.of(5, 6, 7)));
        final var val2 = new ConstantArrayDistinctValue(LiteralValue.ofList(ImmutableList.of(5, 6, 7)));
        final var val3 = new ConstantArrayDistinctValue(
                ConstantObjectValue.of(Quantifier.constant(), "c0", new Type.Array())
        );

        Assertions.assertEquals(val1, val2);
        Assertions.assertNotEquals(val1, val3);
        Assertions.assertNotEquals(val2, val3);
    }
}
