/*
 * CostModelTests.java
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

package com.apple.foundationdb.record.query.plan.cascades.costing;

import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests of the {@link StableSelectorCostModel} implementation. This class is used to distinguish between entries
 * in the plan cache, and so the main invariant it has to uphold is just that it makes a consistent choice
 * for a given set of plans.
 */
class StableSelectorCostModelTests {
    private static final RecordQueryPlan forwardScan = new RecordQueryScanPlan(ScanComparisons.EMPTY, false);
    private static final RecordQueryPlan backwardScan = new RecordQueryScanPlan(ScanComparisons.EMPTY, true);

    @Nonnull
    private final StableSelectorCostModel costModel = new StableSelectorCostModel();

    @Test
    void emptySet() {
        assertCompareAndCostingAreEquivalent(ImmutableSet.of());
    }

    @Test
    void singleElementSet() {
        assertCompareAndCostingAreEquivalent(ImmutableSet.of(forwardScan));
    }

    @Test
    void costPlansEquivalentToCompare() {
        final ImmutableSet<RecordQueryPlan> plans = ImmutableSet.of(
                forwardScan, backwardScan);
        assertThat(plans)
                .hasSize(2);
        assertCompareAndCostingAreEquivalent(plans);
    }

    @Test
    void pickLowerPlanHash() {
        final RecordQueryPlan planA = new RecordQueryPlanWithFixedPlanHash("foo", 9000);
        final RecordQueryPlan planB = new RecordQueryPlanWithFixedPlanHash("bar", 9001);

        assertThat(assertCompareAndCostingAreEquivalent(ImmutableSet.of(planA, planB)))
                .isSameAs(planA);
    }

    @Test
    void pickLeftIfTwoShareTheSamePlanHash() {
        final RecordQueryPlan planA = new RecordQueryPlanWithFixedPlanHash("foo", 9001);
        final RecordQueryPlan planB = new RecordQueryPlanWithFixedPlanHash("bar", 9001);

        assertThat(assertCompareAndCostingAreEquivalent(ImmutableSet.of(planA, planB)))
                .isSameAs(planA);
    }

    @Test
    void pickLowestPlanHash() {
        final RecordQueryPlan planA = new RecordQueryPlanWithFixedPlanHash("a", 30);
        final RecordQueryPlan planB = new RecordQueryPlanWithFixedPlanHash("b", 14);
        final RecordQueryPlan planC = new RecordQueryPlanWithFixedPlanHash("c", 18);
        final RecordQueryPlan planD = new RecordQueryPlanWithFixedPlanHash("d", 20);
        final RecordQueryPlan planE = new RecordQueryPlanWithFixedPlanHash("e", 15);

        assertThat(assertCompareAndCostingAreEquivalent(ImmutableSet.of(planA, planB, planC, planD, planE)))
                .isSameAs(planB);
    }

    @Test
    void pickLeftMostIfMultipleShareTheSamePlanHash() {
        final RecordQueryPlan planA = new RecordQueryPlanWithFixedPlanHash("a", 30);
        final RecordQueryPlan planB = new RecordQueryPlanWithFixedPlanHash("b", 14);
        final RecordQueryPlan planC = new RecordQueryPlanWithFixedPlanHash("c", 14);
        final RecordQueryPlan planD = new RecordQueryPlanWithFixedPlanHash("d", 20);
        final RecordQueryPlan planE = new RecordQueryPlanWithFixedPlanHash("e", 15);
        final RecordQueryPlan planF = new RecordQueryPlanWithFixedPlanHash("f", 14);

        assertThat(assertCompareAndCostingAreEquivalent(ImmutableSet.of(planA, planB, planC, planD, planE, planF)))
                .isSameAs(planB);
    }

    @Nullable
    private RecordQueryPlan assertCompareAndCostingAreEquivalent(@Nonnull Set<RecordQueryPlan> plans) {
        final RecordQueryPlan fromComparison = bestViaComparison(plans);
        final LinkedIdentitySet<RecordQueryPlan> removedSet = new LinkedIdentitySet<>();
        final RecordQueryPlan fromCosting = bestViaCosting(plans, removedSet);
        if (plans.isEmpty()) {
            assertThat(fromComparison)
                    .isNull();
        } else {
            assertThat(fromComparison)
                    .isNotNull();
        }
        assertThat(fromCosting)
                .isSameAs(fromComparison);
        assertThat(removedSet)
                .hasSize(Math.max(0, plans.size() - 1));
        assertThat(plans)
                .allSatisfy(plan -> {
                    if (plan != fromComparison) {
                        assertThat(removedSet)
                                .contains(plan);
                    }
                });
        return fromComparison;
    }

    @Nullable
    private RecordQueryPlan bestViaComparison(@Nonnull Iterable<RecordQueryPlan> plans) {
        RecordQueryPlan bestSoFar = null;
        for (RecordQueryPlan plan : plans) {
            // This reflects the logic in the PlanGenerator
            if (bestSoFar == null || costModel.compare(plan, bestSoFar) < 0) {
                bestSoFar = plan;
            }
        }
        return bestSoFar;
    }

    @Nullable
    private RecordQueryPlan bestViaCosting(@Nonnull Set<RecordQueryPlan> plans, @Nonnull Set<RecordQueryPlan> removedSet) {
        return costModel.getBestExpression(plans, removed -> assertThat(removedSet.add(removed)).isTrue())
                .orElse(null);
    }
}
