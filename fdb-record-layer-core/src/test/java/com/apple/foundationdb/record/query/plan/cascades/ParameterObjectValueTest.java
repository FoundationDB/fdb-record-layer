/*
 * ConstantObjectValueTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ParameterObjectValue;
import com.apple.foundationdb.record.query.plan.serialization.DefaultPlanSerializationRegistry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ParameterObjectValueTest}.
 */
public class ParameterObjectValueTest {
    @Test
    void testEval() {
        final var ctx = EvaluationContext.forBindings(Bindings.newBuilder().set("parameter", 42).build());
        final var pov = ParameterObjectValue.of("parameter", Type.primitiveType(Type.TypeCode.INT));
        final var actualValue =  pov.eval(null, ctx);
        Assertions.assertEquals(42, actualValue);
    }

    @Test
    void testRebaseLeaf() {
        final var pov = ParameterObjectValue.of("parameter", Type.primitiveType(Type.TypeCode.INT));
        Assertions.assertEquals(pov, pov.rebaseLeaf(CorrelationIdentifier.uniqueId()));
    }

    @Test
    void testProto() {
        final var pov = ParameterObjectValue.of("parameter", Type.primitiveType(Type.TypeCode.INT));

        final var serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE, PlanHashable.CURRENT_FOR_CONTINUATION);
        final var proto = pov.toProto(serializationContext);
        final var deserializedPov = ParameterObjectValue.fromProto(serializationContext, proto);
        Assertions.assertEquals(pov, deserializedPov);
        Assertions.assertEquals(pov.planHash(PlanHashable.CURRENT_FOR_CONTINUATION), deserializedPov.planHash(PlanHashable.CURRENT_FOR_CONTINUATION));
    }
}
