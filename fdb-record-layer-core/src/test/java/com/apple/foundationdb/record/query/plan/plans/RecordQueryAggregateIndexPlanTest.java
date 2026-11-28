/*
 * RecordQueryAggregateIndexPlanTest.java
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

import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.ValueTestHelpers;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link RecordQueryAggregateIndexPlan}.
 */
class RecordQueryAggregateIndexPlanTest {
    @Nonnull
    private static RecordQueryAggregateIndexPlan planOf(@Nonnull final Value resultValue,
                                                        @Nonnull final Value groupByResultValue,
                                                        @Nonnull final QueryPlanConstraint constraint) {
        return new RecordQueryAggregateIndexPlan(
                new RecordQueryIndexPlan("agg_index", new IndexScanComparisons(IndexScanType.BY_GROUP, ScanComparisons.EMPTY), false),
                "MySimpleRecord",
                IndexKeyValueToPartialRecord.newBuilder(TestRecords1Proto.MySimpleRecord.getDescriptor()).build(),
                null, resultValue, groupByResultValue, constraint);
    }

    private static void assertRoundTrips(@Nonnull final RecordQueryAggregateIndexPlan plan) {
        final var proto = plan.toProto(PlanSerializationContext.newForCurrentMode());
        final RecordQueryAggregateIndexPlan deserialized = RecordQueryAggregateIndexPlan.fromProto(
                PlanSerializationContext.newForCurrentMode(), proto);

        assertThat(deserialized.toProto(PlanSerializationContext.newForCurrentMode())).isEqualTo(proto);
    }

    /**
     * Tests that a {@link RecordQueryAggregateIndexPlan} survives a round trip through its proto representation when
     * the constraint refers to a record type that is defined by the group-by result value.
     */
    @Test
    void testSerialization1() {
        final Value groupByResultValue = QuantifiedObjectValue.of(CorrelationIdentifier.of("g"), ValueTestHelpers.namedRecordType());
        final QueryPlanConstraint constraint = QueryPlanConstraint.ofPredicate(new ValuePredicate(
                FieldValue.ofFieldName(QuantifiedObjectValue.of(CorrelationIdentifier.of("c"), ValueTestHelpers.namedRecordType()), "a"),
                new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, 1L)));

        assertRoundTrips(planOf(LiteralValue.ofScalar(1L), groupByResultValue, constraint));
    }
}
