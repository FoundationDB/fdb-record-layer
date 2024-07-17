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

import com.apple.foundationdb.record.IndexFetchMethod;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.planprotos.PPlanReference;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
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
        final PValue valueProto = fieldValue.toValueProto(PlanSerializationContext.newForCurrentMode());
        final byte[] valueBytes = valueProto.toByteArray();
        final PValue parsedValueProto = PValue.parseFrom(valueBytes);
        final Value parsedValue = Value.fromValueProto(PlanSerializationContext.newForCurrentMode(), parsedValueProto);
        Verify.verify(parsedValue instanceof FieldValue);
        Assertions.assertEquals(fieldValue, parsedValue);
    }

    @Test
    void simpleIndexScanTest() throws Exception {
        final RecordQueryIndexPlan plan = new RecordQueryIndexPlan("an_index",
                null,
                IndexScanComparisons.byValue(),
                IndexFetchMethod.SCAN_AND_FETCH,
                RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                true,
                false,
                Optional.empty(),
                Type.Record.fromFields(false,
                        ImmutableList.of(Type.Record.Field.of(Type.primitiveType(Type.TypeCode.INT), Optional.of("field1")),
                                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("field2")))),
                QueryPlanConstraint.tautology());
        PlanSerializationContext planSerializationContext = PlanSerializationContext.newForCurrentMode();
        final PPlanReference proto = planSerializationContext.toPlanReferenceProto(plan);
        final byte[] bytes = proto.toByteArray();
        final PPlanReference parsedProto = PPlanReference.parseFrom(bytes);
        planSerializationContext = PlanSerializationContext.newForCurrentMode();
        final RecordQueryPlan parsedPlan = planSerializationContext.fromPlanReferenceProto(parsedProto);
        Verify.verify(parsedPlan instanceof RecordQueryIndexPlan);
        Assertions.assertTrue(plan.semanticEquals(parsedPlan));
    }
}
