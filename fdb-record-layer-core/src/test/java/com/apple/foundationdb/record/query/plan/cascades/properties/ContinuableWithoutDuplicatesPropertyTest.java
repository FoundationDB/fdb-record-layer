/*
 * ContinuableWithoutDuplicatesPropertyTest.java
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

package com.apple.foundationdb.record.query.plan.cascades.properties;

import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryMapPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedDistinctPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import static com.apple.foundationdb.record.query.plan.cascades.properties.ContinuableWithoutDuplicatesProperty.continuableWithoutDuplicates;
import static org.assertj.core.api.Assertions.assertThat;


class ContinuableWithoutDuplicatesPropertyTest {
    @Test
    void indexScanWithMapAndFilterIsContinuableWithoutDuplicates() {
        final var indexScan = new RecordQueryIndexPlan(
                "DummyIndex", IndexScanComparisons.byValue(), false);
        final var filterPlan = new RecordQueryFilterPlan(
                indexScan, Query.field("a_field").lessThan(50));
        final var mapPlan = new RecordQueryMapPlan(
                Quantifier.physical(Reference.plannedOf(filterPlan)),
                RecordConstructorValue.ofUnnamed(ImmutableList.of()));

        final var continuableWithoutDuplicates = continuableWithoutDuplicates().evaluate(mapPlan);

        assertThat(continuableWithoutDuplicates).isTrue();
    }

    @Test
    void primaryKeyScanWithMapAndFilterIsContinuableWithoutDuplicates() {
        final var primaryKeyScan = new RecordQueryScanPlan(ScanComparisons.EMPTY, false);
        final var filterPlan = new RecordQueryFilterPlan(
                primaryKeyScan, Query.field("a_field").lessThan(50));
        final var mapPlan = new RecordQueryMapPlan(
                Quantifier.physical(Reference.plannedOf(filterPlan)),
                RecordConstructorValue.ofUnnamed(ImmutableList.of()));

        final var continuableWithoutDuplicates = continuableWithoutDuplicates().evaluate(mapPlan);

        assertThat(continuableWithoutDuplicates).isTrue();
    }

    @Test
    void unorderedDistinctByPrimaryKeyWithMapAndFilterIsNotContinuableWithoutDuplicates() {
        final var indexScanA = new RecordQueryIndexPlan(
                "DummyIndexA", IndexScanComparisons.byValue(), false);
        final var indexScanB = new RecordQueryIndexPlan(
                "DummyIndexB", IndexScanComparisons.byValue(), false);
        final var unionPlan = RecordQueryUnorderedUnionPlan.from(
                ImmutableList.of(indexScanA, indexScanB));
        final var unorderedDistinctByPrimaryKey = new RecordQueryUnorderedPrimaryKeyDistinctPlan(unionPlan);
        final var filterPlan = new RecordQueryFilterPlan(
                unorderedDistinctByPrimaryKey, Query.field("a_field").lessThan(50));
        final var mapPlan = new RecordQueryMapPlan(
                Quantifier.physical(Reference.plannedOf(filterPlan)),
                RecordConstructorValue.ofUnnamed(ImmutableList.of()));

        final var continuableWithoutDuplicates = continuableWithoutDuplicates().evaluate(mapPlan);

        assertThat(continuableWithoutDuplicates).isFalse();
    }

    @Test
    void unorderedDistinctWithMapAndFilterIsNotContinuableWithoutDuplicates() {
        final var indexScanA = new RecordQueryIndexPlan(
                "DummyIndexA", IndexScanComparisons.byValue(), false);
        final var indexScanB = new RecordQueryIndexPlan(
                "DummyIndexB", IndexScanComparisons.byValue(), false);
        final var unionPlan = RecordQueryUnorderedUnionPlan.from(
                ImmutableList.of(indexScanA, indexScanB));
        final var unorderedDistinct = new RecordQueryUnorderedDistinctPlan(unionPlan, Key.Expressions.field("a_field"));
        final var filterPlan = new RecordQueryFilterPlan(
                unorderedDistinct, Query.field("a_field").lessThan(50));
        final var mapPlan = new RecordQueryMapPlan(
                Quantifier.physical(Reference.plannedOf(filterPlan)),
                RecordConstructorValue.ofUnnamed(ImmutableList.of()));

        final var continuableWithoutDuplicates = continuableWithoutDuplicates().evaluate(mapPlan);

        assertThat(continuableWithoutDuplicates).isFalse();
    }

    @Test
    void toStringReturnsSimpleClassName() {
        assertThat(continuableWithoutDuplicates()).hasToString("ContinuableWithoutDuplicatesProperty");
    }
}
