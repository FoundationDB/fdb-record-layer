/*
 * PickValueTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.serialization.DefaultPlanSerializationRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link PickValue}.
 */
class PickValueTest {
    /**
     * Tests that a {@link PickValue} survives a round trip through its proto representation when its selector and its
     * alternatives share a record type, which is serialized as a back-reference.
     */
    @Test
    void testSerializationRoundTripWithSharedRecordType1() {
        final Type.Record sharedType = Type.Record.fromFieldsWithName("Shared", false, List.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.LONG), Optional.of("a"), Optional.of(2)),
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("b"), Optional.of(5))));
        final Type.Record selectorType = Type.Record.fromFields(false, List.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("i"), Optional.of(1)),
                Type.Record.Field.of(sharedType, Optional.of("s"), Optional.of(2))));
        final Value selector = FieldValue.ofFieldName(
                QuantifiedObjectValue.of(CorrelationIdentifier.of("sel"), selectorType), "i");
        final PickValue pickValue = new PickValue(selector, List.of(
                QuantifiedObjectValue.of(CorrelationIdentifier.of("x"), sharedType),
                QuantifiedObjectValue.of(CorrelationIdentifier.of("y"), sharedType)));

        final PlanSerializationContext serializeContext =
                new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE, PlanHashable.CURRENT_FOR_CONTINUATION);
        final var proto = pickValue.toValueProto(serializeContext);
        final PlanSerializationContext deserializeContext =
                new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE, PlanHashable.CURRENT_FOR_CONTINUATION);
        final Value deserialized = Value.fromValueProto(deserializeContext, proto);

        assertThat(deserialized).isEqualTo(pickValue);
        assertThat(deserialized.getResultType()).isEqualTo(pickValue.getResultType());
    }
}
