/*
 * QueryPlanTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.PlanHashable.PlanHashMode;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Unit tests for QueryPlan classes.
 */
class QueryPlanTest {

    /**
     * Test that ContinuedPhysicalQueryPlan.withExecutionContext() returns the same instance.
     *
     * <p>This test exists primarily to satisfy code coverage requirements. In production,
     * ContinuedPhysicalQueryPlan.withExecutionContext() is never called because continuation
     * plans bypass the plan cache - each EXECUTE CONTINUATION statement deserializes the plan
     * fresh from the continuation blob.
     *
     * <p>TODO: This test can be removed if/when the class hierarchy is refactored to eliminate
     * the need for this dead code method (e.g., by collapsing ContinuedPhysicalQueryPlan into
     * PhysicalQueryPlan with a flag, or making Plan.withExecutionContext() optional).
     */
    @Test
    void continuedPhysicalQueryPlanWithExecutionContextReturnsSameInstance() {
        // Create minimal mocks to construct a ContinuedPhysicalQueryPlan
        final RecordQueryPlan mockRecordQueryPlan = Mockito.mock(RecordQueryPlan.class);
        final Type.Relation mockRelation = Mockito.mock(Type.Relation.class);
        final Type.Record mockRecord = Type.Record.fromFields(ImmutableList.of(
                Type.Record.Field.of(Type.primitiveType(Type.TypeCode.STRING), Optional.of("test_field"))));

        Mockito.when(mockRecordQueryPlan.getResultType()).thenReturn(mockRelation);
        Mockito.when(mockRelation.getInnerType()).thenReturn(mockRecord);

        final TypeRepository typeRepository = TypeRepository.newBuilder().build();
        final QueryPlanConstraint constraint = QueryPlanConstraint.noConstraint();
        final QueryExecutionContext executionContext = new MutablePlanGenerationContext(
                PreparedParams.empty(),
                PlanHashMode.VC0,
                "SELECT 1",
                "SELECT 1",
                0);

        final ImmutableList<DataType> semanticFieldTypes = ImmutableList.of(
                DataType.Primitives.STRING.type());

        // Wrap semantic types in a StructType
        final DataType.StructType semanticStructType = DataType.StructType.from("QUERY_RESULT",
                ImmutableList.of(
                        DataType.StructType.Field.from("field_0", DataType.Primitives.INTEGER.type(), 0),
                        DataType.StructType.Field.from("field_1", DataType.Primitives.STRING.type(), 1)),
                true);

        // Create a ContinuedPhysicalQueryPlan instance
        final QueryPlan.ContinuedPhysicalQueryPlan continuedPlan = new QueryPlan.ContinuedPhysicalQueryPlan(
                mockRecordQueryPlan,
                typeRepository,
                constraint,
                executionContext,
                "EXECUTE CONTINUATION test",
                PlanHashMode.VC0,
                PlanHashMode.VL0,
                semanticStructType);

        // Create a different execution context
        final QueryExecutionContext differentContext = new MutablePlanGenerationContext(
                PreparedParams.empty(),
                PlanHashMode.VC0,
                "SELECT 2",
                "SELECT 2",
                1);

        // Call withExecutionContext and verify it returns the same instance
        final QueryPlan.PhysicalQueryPlan result = continuedPlan.withExecutionContext(differentContext);

        assertSame(continuedPlan, result,
                "ContinuedPhysicalQueryPlan.withExecutionContext() should return the same instance");
    }
}
