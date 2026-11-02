/*
 * InExplodeSourceTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.ArraySortValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ExplodeValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ParameterObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.serialization.DefaultPlanSerializationRegistry;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Stream;

/**
 * Tests for {@link InExplodeSource} and the values it is built from, in particular that they survive serialization,
 * so that a plan using them can be executed by a server that did not plan it.
 */
class InExplodeSourceTest {

    @Nonnull
    private static PlanSerializationContext newSerializationContext() {
        return new PlanSerializationContext(new DefaultPlanSerializationRegistry(),
                PlanHashable.CURRENT_FOR_CONTINUATION);
    }

    @Nonnull
    private static Value elements() {
        return LiteralValue.ofList(List.of(30L, 10L, 20L));
    }

    @Nonnull
    private static List<Object> elementsOf(@Nonnull final InExplodeSource source) {
        final var context = EvaluationContext.forTypeRepository(
                TypeRepository.newBuilder().addAllTypes(source.getDynamicTypes()).build());
        return source.getValues(context).stream()
                .map(struct -> {
                    final var message = (Message)struct;
                    return message.getField(message.getDescriptorForType().getFields().get(0));
                })
                .collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    static Stream<Arguments> sources() {
        return Stream.of(
                Arguments.of(new InExplodeSource("in", new ExplodeValue(elements())), List.of(30L, 10L, 20L)),
                Arguments.of(new InExplodeSource("in", new ExplodeValue(new ArraySortValue(elements(), false))), List.of(10L, 20L, 30L)),
                Arguments.of(new InExplodeSource("in", new ExplodeValue(new ArraySortValue(elements(), true))), List.of(30L, 20L, 10L)));
    }

    @ParameterizedTest
    @MethodSource("sources")
    void producesTheElementsOfTheExplode(@Nonnull final InExplodeSource source, @Nonnull final List<Object> expected) {
        Assertions.assertEquals(expected, elementsOf(source));
    }

    @ParameterizedTest
    @MethodSource("sources")
    void sizeCountsTheElementsWithoutTheirType(@Nonnull final InExplodeSource source, @Nonnull final List<Object> expected) {
        // the empty context has no type repository, so creating any struct would fail
        Assertions.assertEquals(expected.size(), source.size(EvaluationContext.EMPTY));
    }

    @Test
    void sizeOfANullCollectionIsZero() {
        final var source = new InExplodeSource("in",
                new ExplodeValue(ParameterObjectValue.of("p", new Type.Array(Type.primitiveType(Type.TypeCode.LONG, false)))));
        Assertions.assertEquals(0, source.size(EvaluationContext.EMPTY.withBinding("p", null)));
        Assertions.assertEquals(2, source.size(EvaluationContext.EMPTY.withBinding("p", List.of(1L, 2L))));
    }

    @Test
    void valuesNeedAnEvaluationContext() {
        final var source = new InExplodeSource("in", new ExplodeValue(elements()));
        Assertions.assertThrows(RecordCoreException.class, source::getValues);
    }

    @ParameterizedTest
    @MethodSource("sources")
    void protoRoundTrip(@Nonnull final InExplodeSource source, @Nonnull final List<Object> expected) {
        final var inSourceProto = source.toInSourceProto(newSerializationContext());
        Assertions.assertTrue(inSourceProto.hasInExplodeSource());

        final var deserialized = InSource.fromInSourceProto(newSerializationContext(), inSourceProto);
        Assertions.assertInstanceOf(InExplodeSource.class, deserialized);
        Assertions.assertEquals(source, deserialized);
        Assertions.assertEquals(source.planHash(PlanHashable.CURRENT_FOR_CONTINUATION),
                deserialized.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
        Assertions.assertEquals(source.isSorted(), deserialized.isSorted());
        Assertions.assertEquals(source.isReverse(), deserialized.isReverse());
        Assertions.assertEquals(expected, elementsOf((InExplodeSource)deserialized));
    }

    @ParameterizedTest
    @MethodSource("sources")
    void arraySortValueProtoRoundTrip(@Nonnull final InExplodeSource source, @Nonnull final List<Object> expected) {
        final var collectionValue = new ExplodeValue(new ArraySortValue(elements(), source.isReverse())).getCollectionValue();
        final var valueProto = collectionValue.toValueProto(newSerializationContext());
        Assertions.assertTrue(valueProto.hasArraySortValue());

        final var deserialized = Value.fromValueProto(newSerializationContext(), valueProto);
        Assertions.assertInstanceOf(ArraySortValue.class, deserialized);
        Assertions.assertEquals(collectionValue, deserialized);
        Assertions.assertEquals(source.isReverse(), ((ArraySortValue)deserialized).isReverse());
    }

    @Test
    void listLiteralProtoRoundTrip() {
        final var literal = elements();
        final var valueProto = literal.toValueProto(newSerializationContext());
        Assertions.assertTrue(valueProto.getLiteralValue().getIsList());

        final var deserialized = Value.fromValueProto(newSerializationContext(), valueProto);
        Assertions.assertEquals(literal, deserialized);
        Assertions.assertEquals(literal.getResultType(), deserialized.getResultType());
        Assertions.assertEquals(List.of(30L, 10L, 20L), ((LiteralValue<?>)deserialized).getLiteralValue());
    }
}
