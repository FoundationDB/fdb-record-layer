/*
 * ExistsValueTest.java
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

import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ExistsValue} and {@link ExistsValue.ExistsFn}.
 */
class ExistsValueTest {

    @Test
    void encapsulateTest() {
        final var childValue = QuantifiedObjectValue.of(
                CorrelationIdentifier.of("q1"), Type.primitiveType(Type.TypeCode.BOOLEAN, true));
        final var result = new ExistsValue.ExistsFn().encapsulate(ImmutableList.of(childValue));
        assertThat(result).isInstanceOf(ExistsValue.class);
        assertThat(((ExistsValue) result).getChild()).isEqualTo(childValue);
    }

    @Test
    void serializationRoundTripTest() throws InvalidProtocolBufferException {
        final var childValue = QuantifiedObjectValue.of(
                CorrelationIdentifier.of("q1"), Type.primitiveType(Type.TypeCode.BOOLEAN, true));
        final var existsValue = new ExistsValue(childValue);

        // Serialize
        final var serializationContext = PlanSerializationContext.newForCurrentMode();
        final PValue valueProto = existsValue.toValueProto(serializationContext);

        // Round-trip through bytes
        final PValue parsedValueProto = PValue.parseFrom(valueProto.toByteArray());

        // Deserialize
        final var deserialized = Value.fromValueProto(PlanSerializationContext.newForCurrentMode(), parsedValueProto);
        assertThat(deserialized).isInstanceOf(ExistsValue.class);
        assertThat(deserialized).isEqualTo(existsValue);
    }
}
