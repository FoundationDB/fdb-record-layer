/*
 * RecordQueryDeletePlanTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecordQueryDeletePlanTest {

    private static RecordQueryDeletePlan deletePlanOver(final Type flowedType,
                                                        final CorrelationIdentifier alias) {
        final var scan = new RecordQueryScanPlan(ImmutableSet.of("R"), flowedType, null,
                ScanComparisons.EMPTY, false, false);
        return RecordQueryDeletePlan.deletePlan(Quantifier.physical(Reference.plannedOf(scan), alias));
    }

    private static Type recordTypeWith(final Type.TypeCode fieldType) {
        return Type.Record.fromFields(false, List.of(
                Type.Record.Field.of(Type.primitiveType(fieldType), Optional.of("a"))));
    }

    @Test
    void equalsWithoutChildrenReturnsFalseWhenResultValuesDiffer() {
        final var typeLong = recordTypeWith(Type.TypeCode.LONG);
        final var deletePlan1 = deletePlanOver(typeLong, CorrelationIdentifier.of("q1"));
        final var deletePlan2 = deletePlanOver(typeLong, CorrelationIdentifier.of("q2"));

        assertFalse(deletePlan1.equalsWithoutChildren(deletePlan2, AliasMap.emptyMap()));
        assertFalse(deletePlan2.equalsWithoutChildren(deletePlan1, AliasMap.emptyMap()));
    }

    @Test
    void equalsWithoutChildrenReturnsTrueWhenResultValuesMatch() {
        final var typeLong = recordTypeWith(Type.TypeCode.LONG);
        final var alias = CorrelationIdentifier.of("q");
        final var deletePlan1 = deletePlanOver(typeLong, alias);
        final var deletePlan2 = deletePlanOver(typeLong, alias);

        assertTrue(deletePlan1.equalsWithoutChildren(deletePlan2, AliasMap.emptyMap()));
        assertTrue(deletePlan1.equalsWithoutChildren(deletePlan1, AliasMap.emptyMap()));
    }
}
