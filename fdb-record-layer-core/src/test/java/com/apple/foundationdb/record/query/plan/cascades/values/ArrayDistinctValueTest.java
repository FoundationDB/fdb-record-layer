/*
 * ArrayDistinctValueTest.java
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

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecords4Proto;
import com.apple.foundationdb.record.TestRecords6Proto;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.explain.DefaultExplainFormatter;
import com.apple.foundationdb.record.query.plan.explain.DefaultExplainSymbolMap;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;

class ArrayDistinctValueTest {

    private static TestRecords1Proto.MySimpleRecord simpleRecord(int recNo) {
        return TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(recNo).build();
    }

    private static TestRecords1Proto.MySimpleRecord simpleRecord(List<Integer> repeater) {
        return TestRecords1Proto.MySimpleRecord.newBuilder().addAllRepeater(repeater).build();
    }

    private static TestRecords6Proto.MyRepeatedRecord repeatedRecord(List<String> s1) {
        return TestRecords6Proto.MyRepeatedRecord.newBuilder().setRecNo(1L).addAllS1(s1).build();
    }

    private static TestRecords4Proto.RestaurantReview restaurantReview(int reviewer, int rating) {
        return TestRecords4Proto.RestaurantReview.newBuilder().setReviewer(reviewer).setRating(rating).build();
    }

    private static TestRecords4Proto.RestaurantRecord restaurantRecord(List<TestRecords4Proto.RestaurantReview> reviews) {
        return TestRecords4Proto.RestaurantRecord.newBuilder().setRestNo(1).addAllReviews(reviews).build();
    }

    private static Stream<Arguments> literalArraySources() {
        return Stream.of(
                arguments(
                        LiteralValue.ofList(ImmutableList.of(1, 2, 1, 2, 1, 2, 3)),
                        ImmutableList.of(1, 2, 3),
                        EvaluationContext.EMPTY
                ),
                arguments(
                        LiteralValue.ofList(ImmutableList.of(1, 2, 3, 4, 5)),
                        ImmutableList.of(1, 2, 3, 4, 5),
                        EvaluationContext.EMPTY
                ),
                arguments(
                        LiteralValue.ofList(ImmutableList.of("val2", "val1", "val3", "val1", "val2")),
                        ImmutableList.of("val2", "val1", "val3"),
                        EvaluationContext.EMPTY
                ),
                arguments(
                        LiteralValue.ofList(ImmutableList.of(
                                simpleRecord(1), simpleRecord(2), simpleRecord(2), simpleRecord(4),
                                simpleRecord(3), simpleRecord(2), simpleRecord(3),
                                repeatedRecord(ImmutableList.of("1"))
                        )),
                        ImmutableList.of(
                                simpleRecord(1), simpleRecord(2), simpleRecord(4), simpleRecord(3),
                                repeatedRecord(ImmutableList.of("1"))
                        ),
                        EvaluationContext.EMPTY
                )
        );
    }

    private static Stream<Arguments> boundArraySources() {
        return Stream.of(
                arguments(
                        ConstantObjectValue.of(
                                Quantifier.constant(),
                                "c0",
                                new Type.Array(Type.primitiveType(Type.TypeCode.INT))
                        ),
                        List.of(1, 2, 3),
                        EvaluationContext.newBuilder()
                                .setConstant(
                                        Quantifier.constant(),
                                        ImmutableMap.of("c0", ImmutableList.of(1, 2, 2, 3, 3, 3))
                                )
                                .build(TypeRepository.empty())
                ),
                arguments(
                        FieldValue.ofFieldName(
                                QuantifiedObjectValue.of(
                                        CorrelationIdentifier.of("id1"),
                                        Type.Record.fromDescriptor(TestRecords1Proto.MySimpleRecord.getDescriptor())
                                ),
                                "repeater"
                        ),
                        ImmutableList.of(1, 2, 5, 4, 3),
                        EvaluationContext.forBinding(
                                Bindings.Internal.CORRELATION.bindingName("id1"),
                                QueryResult.ofComputed(simpleRecord(List.of(1, 2, 2, 5, 4, 3, 3)))
                        )
                ),
                arguments(
                        FieldValue.ofFieldName(
                                QuantifiedObjectValue.of(
                                        CorrelationIdentifier.of("id2"),
                                        Type.Record.fromDescriptor(TestRecords6Proto.MyRepeatedRecord.getDescriptor())
                                ),
                                "s1"
                        ),
                        ImmutableList.of("val2", "val1", "val3"),
                        EvaluationContext.forBinding(
                                Bindings.Internal.CORRELATION.bindingName("id2"),
                                QueryResult.ofComputed(repeatedRecord(List.of("val2", "val1", "val3", "val1", "val2")))
                        )
                ),
                arguments(
                        FieldValue.ofFieldName(
                                QuantifiedObjectValue.of(
                                        CorrelationIdentifier.of("id3"),
                                        Type.Record.fromDescriptor(TestRecords4Proto.RestaurantRecord.getDescriptor())
                                ),
                                "reviews"
                        ),
                        ImmutableList.of(
                                restaurantReview(1, 5), restaurantReview(3, 5),
                                restaurantReview(2, 5), restaurantReview(2, 4)
                        ),
                        EvaluationContext.forBinding(
                                Bindings.Internal.CORRELATION.bindingName("id3"),
                                QueryResult.ofComputed(restaurantRecord(
                                        ImmutableList.of(
                                                restaurantReview(1, 5), restaurantReview(3, 5),
                                                restaurantReview(3, 5), restaurantReview(1, 5),
                                                restaurantReview(2, 5), restaurantReview(2, 4)
                                        )
                                ))
                        )
                )
        );
    }

    @Test
    void rejectsNonArrayValues() {
        Assertions.assertThrowsExactly(VerifyException.class, () -> {
            new ArrayDistinctValue(LiteralValue.ofScalar(42));
        });
    }

    @ParameterizedTest(name = "returnsArrayWithoutDuplicates[input={0}, expected={1}])")
    @MethodSource({"literalArraySources", "boundArraySources"})
    void returnsArrayWithoutDuplicates(Value inputArray, List<?> expectedArray, EvaluationContext evaluationContext) {
        final var constantArrayDistinctValue = new ArrayDistinctValue(inputArray);
        final var actualArray = constantArrayDistinctValue.evalWithoutStore(evaluationContext);

        Assertions.assertEquals(expectedArray, actualArray);
    }

    @Test
    void withNewChildReplacesUnderlyingArray() {
        final var expectedArray = ImmutableList.of(1, 2, 3);
        final ArrayDistinctValue value = new ArrayDistinctValue(LiteralValue.ofList(ImmutableList.of(4, 5, 6)));

        final var newValue = value.withNewChild(LiteralValue.ofList(expectedArray));

        Assertions.assertEquals(expectedArray, newValue.evalWithoutStore(EvaluationContext.EMPTY));
    }

    @Test
    void equalsComparesUnderlyingValues() {
        final var val1 = new ArrayDistinctValue(LiteralValue.ofList(ImmutableList.of(5, 6, 7)));
        final var val2 = new ArrayDistinctValue(LiteralValue.ofList(ImmutableList.of(5, 6, 7)));
        final var val3 = new ArrayDistinctValue(
                ConstantObjectValue.of(Quantifier.constant(), "c0", new Type.Array())
        );

        Assertions.assertEquals(val1, val2);
        Assertions.assertNotEquals(val1, val3);
        Assertions.assertNotEquals(val2, val3);
    }

    @ParameterizedTest(name = "testSerialization[childValue={0}])")
    @MethodSource("boundArraySources")  // Don't include literalArraySources as these can't be serialized
    void testSerialization(Value childValue) {
        final var val1 = new ArrayDistinctValue(childValue);
        final var context = PlanSerializationContext.newForCurrentMode();

        final var serializedValue = val1.toValueProto(context);
        final var deserializedValue = Value.fromValueProto(context, serializedValue);

        Assertions.assertInstanceOf(ArrayDistinctValue.class, deserializedValue);
        Assertions.assertEquals(deserializedValue, val1);
        Assertions.assertEquals(((ArrayDistinctValue)deserializedValue).getChild(), childValue);
    }

    @Test
    void testExplain() {
        final var inputArray = ImmutableList.of(4, 5, 5, 6, 4);
        final ArrayDistinctValue value = new ArrayDistinctValue(LiteralValue.ofList(inputArray));

        Assertions.assertEquals(
                "arrayDistinct(" + inputArray + ")",
                value.explain().getExplainTokens().render(DefaultExplainFormatter.create(DefaultExplainSymbolMap::new)).toString());
    }

    @Test
    void testPlanHash() {
        final ArrayDistinctValue val1 = new ArrayDistinctValue(LiteralValue.ofList(ImmutableList.of(4, 5, 6)));
        final ArrayDistinctValue val2 = new ArrayDistinctValue(LiteralValue.ofList(ImmutableList.of(1, 2, 3)));
        final ArrayDistinctValue val3 = new ArrayDistinctValue(LiteralValue.ofList(ImmutableList.of(4, 5, 6)));

        Assertions.assertEquals(1978183775, val1.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        Assertions.assertEquals(1978180796, val2.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        Assertions.assertEquals(1978183775, val3.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
    }
}
