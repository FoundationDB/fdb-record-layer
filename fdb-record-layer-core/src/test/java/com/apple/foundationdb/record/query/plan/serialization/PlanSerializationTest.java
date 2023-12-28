/*
 * PlanSerializationTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.serialization;

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordQueryPlanProto;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.Optional;

/**
 * Tests around serialization/deserialization of {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan}s.
 */
public class PlanSerializationTest {
    @Test
    void simpleFieldValueTest() throws Exception {
        final FieldValue fieldValue = FieldValue.ofFieldNames(
                QuantifiedObjectValue.of(Quantifier.current(), Type.Record.fromFields(true,
                        ImmutableList.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT, false), Optional.of("aField"))))),
                ImmutableList.of("aField"));
        final RecordQueryPlanProto.PValue valueProto = fieldValue.toValueProto(PlanHashable.CURRENT_FOR_CONTINUATION);
        final byte[] valueBytes = valueProto.toByteArray();
        final RecordQueryPlanProto.PValue parsedValueProto = RecordQueryPlanProto.PValue.parseFrom(valueBytes);
        final Value parsedValue = Value.fromValueProto(PlanHashable.CURRENT_FOR_CONTINUATION, parsedValueProto);
        Verify.verify(parsedValue instanceof FieldValue);
        System.out.println(parsedValue);
    }
}
