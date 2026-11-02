/*
 * DistinctRecordsPropertyTest.java
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
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryComparatorPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQuerySelectorPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;

import static com.apple.foundationdb.record.query.plan.cascades.properties.DistinctRecordsProperty.distinctRecords;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link DistinctRecordsProperty}.
 */
class DistinctRecordsPropertyTest {
    @Nonnull
    private static RecordQueryPlan distinctPlan() {
        return new RecordQueryScanPlan(ScanComparisons.EMPTY, false);
    }

    @Nonnull
    private static RecordQueryPlan nonDistinctPlan() {
        // The branches of an unordered union may produce the same record.
        return RecordQueryUnorderedUnionPlan.from(List.of(distinctPlan(), distinctPlan()));
    }

    /**
     * Tests that a comparator plan over children that all produce distinct records produces distinct records.
     */
    @Test
    void testComparatorPlan1() {
        final RecordQueryPlan plan = RecordQueryComparatorPlan.from(
                List.of(distinctPlan(), distinctPlan()), Key.Expressions.field("a"), 0);

        assertThat(distinctRecords().evaluate(plan)).isTrue();
    }

    /**
     * Tests that a comparator plan with a child that may produce duplicates does not produce distinct records.
     */
    @Test
    void testComparatorPlan2() {
        final RecordQueryPlan plan = RecordQueryComparatorPlan.from(
                List.of(distinctPlan(), nonDistinctPlan()), Key.Expressions.field("a"), 0);

        assertThat(distinctRecords().evaluate(plan)).isFalse();
    }

    /**
     * Tests that a selector plan over children that all produce distinct records produces distinct records.
     */
    @Test
    void testSelectorPlan1() {
        final RecordQueryPlan plan = RecordQuerySelectorPlan.from(List.of(distinctPlan(), distinctPlan()), List.of(50, 50));

        assertThat(distinctRecords().evaluate(plan)).isTrue();
    }

    /**
     * Tests that a selector plan with a child that may produce duplicates does not produce distinct records.
     */
    @Test
    void testSelectorPlan2() {
        final RecordQueryPlan plan = RecordQuerySelectorPlan.from(List.of(distinctPlan(), nonDistinctPlan()), List.of(50, 50));

        assertThat(distinctRecords().evaluate(plan)).isFalse();
    }
}
