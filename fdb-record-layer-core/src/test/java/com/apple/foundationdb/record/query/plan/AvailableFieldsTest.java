/*
 * AvailableFieldsTest.java
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

package com.apple.foundationdb.record.query.plan;

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PIndexKeyValueToPartialRecord.PCopyIfPredicate;
import com.apple.foundationdb.record.query.plan.serialization.DefaultPlanSerializationRegistry;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.primitives.ImmutableIntArray;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link AvailableFields}.
 */
class AvailableFieldsTest {
    private static PlanSerializationContext serializationContext() {
        return new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE, PlanHashable.CURRENT_FOR_CONTINUATION);
    }

    /**
     * Ordinal paths to serialize. Apart from the empty path, none is of the form {@code [0, 1, ..., n-1]}, so
     * that a deserializer that rebuilds the path from the element index rather than the element is caught.
     */
    static Stream<List<Integer>> ordinalPaths() {
        return Stream.of(List.of(), List.of(5), List.of(2, 0), List.of(3, 1, 4));
    }

    private static AvailableFields.ConditionalUponPathPredicate predicate(final List<Integer> ordinalPath) {
        return new AvailableFields.ConditionalUponPathPredicate(ImmutableIntArray.copyOf(ordinalPath));
    }

    @ParameterizedTest
    @MethodSource("ordinalPaths")
    void conditionalUponPathPredicateToProto(final List<Integer> ordinalPath) {
        assertThat(predicate(ordinalPath).toProto(serializationContext()).getOrdinalPathList())
                .containsExactlyElementsOf(ordinalPath);
    }

    @ParameterizedTest
    @MethodSource("ordinalPaths")
    void conditionalUponPathPredicateToCopyIfPredicateProto(final List<Integer> ordinalPath) {
        final PCopyIfPredicate copyIfPredicate = predicate(ordinalPath).toCopyIfPredicateProto(serializationContext());

        assertThat(copyIfPredicate.hasConditionalUponPathPredicate()).isTrue();
        assertThat(copyIfPredicate.getConditionalUponPathPredicate().getOrdinalPathList())
                .containsExactlyElementsOf(ordinalPath);
    }

    @ParameterizedTest
    @MethodSource("ordinalPaths")
    void conditionalUponPathPredicateRoundTrip(final List<Integer> ordinalPath) {
        final PlanSerializationContext context = serializationContext();
        final AvailableFields.ConditionalUponPathPredicate original = predicate(ordinalPath);

        final AvailableFields.ConditionalUponPathPredicate deserialized =
                AvailableFields.ConditionalUponPathPredicate.fromProto(context, original.toProto(context));

        assertThat(deserialized.toProto(context).getOrdinalPathList()).containsExactlyElementsOf(ordinalPath);
        assertThat(deserialized.planHash(PlanHashable.CURRENT_FOR_CONTINUATION))
                .isEqualTo(original.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
    }

    @Test
    void conditionalUponPathPredicateRoundTripPreservesBehavior() {
        final PlanSerializationContext context = serializationContext();
        final AvailableFields.ConditionalUponPathPredicate deserialized =
                AvailableFields.ConditionalUponPathPredicate.fromProto(context,
                        predicate(List.of(1, 0)).toProto(context));

        assertThat(deserialized.test(Tuple.from("a", Tuple.from(Tuple.from("x"))))).isTrue();
        assertThat(deserialized.test(Tuple.from("a", Tuple.from("x")))).isFalse();
    }
}
