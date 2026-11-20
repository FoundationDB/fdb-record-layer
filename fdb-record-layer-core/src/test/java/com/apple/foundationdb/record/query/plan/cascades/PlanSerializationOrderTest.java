/*
 * PlanSerializationOrderTest.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PRangeConstraints;
import com.apple.foundationdb.record.provider.foundationdb.MultidimensionalIndexScanComparisons;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.predicates.RangeConstraints;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.MessageHelpers;
import com.apple.foundationdb.record.query.plan.cascades.values.PromoteValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.serialization.DefaultPlanSerializationRegistry;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that objects are deserialized in the order in which they are serialized. A record type is serialized in full
 * (only) the first time that a context visits it and just as a reference id after that. If an object that carries a
 * reference were (due to a bug in the deserialization logic) read before the object that carries the definition,
 * the deserialization would fail with an internal error.
 */
class PlanSerializationOrderTest {
    @Nonnull
    private static Type.Record sharedType() {
        return Type.Record.fromFieldsWithName("Shared", false, List.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("a"), Optional.of(2)),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("b"), Optional.of(5))));
    }

    @Nonnull
    private static PlanSerializationContext newContext() {
        return new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE, PlanHashable.CURRENT_FOR_CONTINUATION);
    }

    @Nonnull
    private static ScanComparisons scanComparisonsOn(@Nonnull final String alias) {
        return new ScanComparisons(
                List.of(new Comparisons.ValueComparison(Comparisons.Type.EQUALS,
                        QuantifiedObjectValue.of(CorrelationIdentifier.of(alias), sharedType()))),
                Set.of());
    }

    /**
     * Tests that a {@link MultidimensionalIndexScanComparisons} survives a round trip when a dimension refers to a
     * record type defined by the prefix.
     */
    @Test
    void testMultidimensionalIndexScanComparisons1() {
        final MultidimensionalIndexScanComparisons comparisons = new MultidimensionalIndexScanComparisons(
                scanComparisonsOn("p"), List.of(scanComparisonsOn("d")), scanComparisonsOn("s"));

        final var proto = comparisons.toProto(newContext());
        final var deserialized = MultidimensionalIndexScanComparisons.fromProto(newContext(), proto);

        assertThat(deserialized.toProto(newContext())).isEqualTo(proto);
    }

    /**
     * Tests that a {@link UserDefinedMacroFunction} survives a round trip when a default value defines a record type
     * that a later parameter refers to.
     */
    @Test
    void testUserDefinedMacroFunction1() {
        final QuantifiedObjectValue first = QuantifiedObjectValue.of(CorrelationIdentifier.of("first"), Type.primitiveType(Type.TypeCode.LONG));
        final QuantifiedObjectValue second = QuantifiedObjectValue.of(CorrelationIdentifier.of("second"), sharedType());
        final UserDefinedMacroFunction function = new UserDefinedMacroFunction("f",
                List.of(first, second),
                List.of("first", "second"),
                List.of(Optional.of(QuantifiedObjectValue.of(CorrelationIdentifier.of("d"), sharedType())), Optional.empty()),
                first);

        final var proto = function.toProto();
        final var deserialized = UserDefinedMacroFunction.fromProto(proto.getUserDefinedMacroFunction());

        assertThat(deserialized.toProto()).isEqualTo(proto);
    }

    /**
     * Tests that a {@link RangeConstraints} survives a round trip when a deferred range refers to a record type defined
     * by the evaluable range.
     */
    @Test
    void testRangeConstraints1() {
        final var evaluableRange = new RangeConstraints.CompilableRange(Set.of(
                new Comparisons.ValueComparison(Comparisons.Type.EQUALS,
                        QuantifiedObjectValue.of(CorrelationIdentifier.of("e"), sharedType()))));
        final var deferredRange = new Comparisons.ValueComparison(Comparisons.Type.EQUALS,
                QuantifiedObjectValue.of(CorrelationIdentifier.of("d"), sharedType()));
        final PlanSerializationContext context = newContext();
        final PRangeConstraints proto = PRangeConstraints.newBuilder()
                .setEvaluableRange(evaluableRange.toProto(context))
                .addDeferredRanges(deferredRange.toComparisonProto(context))
                .build();

        final RangeConstraints deserialized = RangeConstraints.fromProto(newContext(), proto);

        assertThat(deserialized.toProto(newContext())).isEqualTo(proto);
    }

    /**
     * Tests that a {@link PromoteValue.ArrayCoercionBiFunction} survives a round trip when its elements trie refers to
     * a record type defined by its array types.
     */
    @Test
    void testArrayCoercionBiFunction1() {
        final MessageHelpers.CoercionBiFunction nested = new PromoteValue.ArrayCoercionBiFunction(
                new Type.Array(false, sharedType()), new Type.Array(true, sharedType()), null);
        final PromoteValue.ArrayCoercionBiFunction function = new PromoteValue.ArrayCoercionBiFunction(
                new Type.Array(false, sharedType()), new Type.Array(true, sharedType()),
                new MessageHelpers.CoercionTrieNode(nested, null));

        final var proto = function.toProto(newContext());
        final var deserialized = PromoteValue.ArrayCoercionBiFunction.fromProto(newContext(), proto);

        assertThat(deserialized.toProto(newContext())).isEqualTo(proto);
    }
}
