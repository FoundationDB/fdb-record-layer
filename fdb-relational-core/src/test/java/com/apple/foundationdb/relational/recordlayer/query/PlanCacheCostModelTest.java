/*
 * PlanCacheCostModelTest.java
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.PlannerStage;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.costing.PlanningCostModel;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicatesFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests of {@link PlanCacheCostModel}, which picks between the plans of one plan cache entry.
 *
 * @see <a href="https://github.com/FoundationDB/fdb-record-layer/issues/4642">Issue #4642</a>
 */
class PlanCacheCostModelTest {
    @Nonnull
    private static final Comparisons.Comparison COMPARISON =
            new Comparisons.SimpleComparison(Comparisons.Type.GREATER_THAN, 200);

    @Nonnull
    private final PlanCacheCostModel costModel = new PlanCacheCostModel();

    @Nonnull
    private static RecordQueryPlan indexPlan(@Nonnull final String indexName) {
        return new RecordQueryIndexPlan(indexName,
                new IndexScanComparisons(IndexScanType.BY_VALUE, Objects.requireNonNull(ScanComparisons.from(COMPARISON))),
                false);
    }

    /** The shape of the unconstrained plan that poisons an entry: a full scan with a residual filter. */
    @Nonnull
    private static RecordQueryPlan scanWithResidualFilter() {
        final var qun = Quantifier.physical(Reference.ofFinalExpression(PlannerStage.PLANNED,
                new RecordQueryScanPlan(ScanComparisons.EMPTY, false)));
        return new RecordQueryPredicatesFilterPlan(qun,
                ImmutableList.of(qun.getFlowedObjectValue().withComparison(COMPARISON)));
    }

    @Test
    void prefersIndexScanOverScanWithResidualFilter() {
        final RecordQueryPlan indexScan = indexPlan("someIndex");
        final RecordQueryPlan poison = scanWithResidualFilter();

        assertThat(costModel.compare(indexScan, poison)).isNegative();
        assertThat(costModel.compare(poison, indexScan)).isPositive();
        assertThat(bestOf(indexScan, poison)).isSameAs(indexScan);
        assertThat(bestOf(poison, indexScan)).isSameAs(indexScan);
    }

    /** Cached plans carry no match candidate, so {@link PlanningCostModel} cannot be used here. */
    @Test
    void doesNotRequireMatchCandidate() {
        final Set<RecordQueryPlan> plans = ImmutableSet.of(indexPlan("someIndex"), indexPlan("someOtherIndex"));

        assertThatThrownBy(() -> new PlanningCostModel(costModel.getConfiguration()).getBestExpression(plans))
                .isInstanceOf(RecordCoreException.class)
                .hasMessageContaining("expected match candidate to be present");

        assertThat(costModel.getBestExpression(plans)).isPresent();
    }

    @Test
    void compareAndCostingAgree() {
        final Set<RecordQueryPlan> plans =
                ImmutableSet.of(indexPlan("someIndex"), indexPlan("someOtherIndex"), scanWithResidualFilter());

        assertThat(costModel.getBestExpression(plans).orElse(null))
                .isSameAs(bestOf(plans.toArray(new RecordQueryPlan[0])));
    }

    @Test
    void emptySet() {
        assertThat(costModel.getBestExpression(ImmutableSet.of())).isEmpty();
    }

    @Test
    void singleElementSet() {
        final RecordQueryPlan plan = indexPlan("someIndex");
        assertThat(costModel.getBestExpression(ImmutableSet.of(plan))).contains(plan);
    }

    /** Mirrors the reduction the plan cache performs over the plans of one entry. */
    @Nullable
    private RecordQueryPlan bestOf(@Nonnull final RecordQueryPlan... plans) {
        RecordQueryPlan bestSoFar = null;
        for (final RecordQueryPlan plan : plans) {
            if (bestSoFar == null || costModel.compare(plan, bestSoFar) < 0) {
                bestSoFar = plan;
            }
        }
        return bestSoFar;
    }
}
