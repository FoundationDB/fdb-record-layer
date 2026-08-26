/*
 * RecordQueryStreamingAggregationPlanTest.java
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

import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.CountValue;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

class RecordQueryStreamingAggregationPlanTest {

    private static RecordQueryScanPlan scanPlanOver(final Type flowedType, final String recordType) {
        return new RecordQueryScanPlan(ImmutableSet.of(recordType), flowedType, null, ScanComparisons.EMPTY, false, false);
    }

    private static Type recordTypeWith(final Type.TypeCode fieldType) {
        return Type.Record.fromFields(false, List.of(
                Type.Record.Field.of(Type.primitiveType(fieldType), Optional.of("a"))));
    }

    @Test
    void withChildReplacesInnerPlanButPreservesGroupingAndAggregateState() {
        final var recordType = recordTypeWith(Type.TypeCode.LONG);
        final var alias = CorrelationIdentifier.of("q");
        final var originalQuantifier = Quantifier.physical(Reference.plannedOf(scanPlanOver(recordType, "R")), alias);
        final var aggregateValue = new CountValue(originalQuantifier.getFlowedObjectValue());
        final var originalPlan = RecordQueryStreamingAggregationPlan.ofFlattened(originalQuantifier, null, aggregateValue);

        final var newChildPlan = scanPlanOver(recordType, "S");
        final var withNewChild = originalPlan.withChild(Reference.plannedOf(newChildPlan));

        assertSame(newChildPlan, withNewChild.getInner().getRangesOverPlan());
        assertEquals(alias, withNewChild.getInner().getAlias());
        assertNotSame(originalPlan.getInner(), withNewChild.getInner());

        assertSame(originalPlan.getGroupingValue(), withNewChild.getGroupingValue());
        assertSame(originalPlan.getAggregateValue(), withNewChild.getAggregateValue());
        assertSame(originalPlan.getGroupingKeyAlias(), withNewChild.getGroupingKeyAlias());
        assertSame(originalPlan.getAggregateAlias(), withNewChild.getAggregateAlias());
        assertSame(originalPlan.getCompleteResultValue(), withNewChild.getCompleteResultValue());
    }
}
