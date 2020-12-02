/*
 * CascadesCostModel.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.QueryPlanner.IndexScanPreference;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.temp.properties.ExpressionCountProperty;
import com.apple.foundationdb.record.query.plan.temp.properties.PredicateCountProperty;
import com.apple.foundationdb.record.query.plan.temp.properties.RelationalExpressionDepthProperty;
import com.apple.foundationdb.record.query.plan.temp.properties.TypeFilterCountProperty;
import com.apple.foundationdb.record.query.plan.temp.properties.UnmatchedFieldsProperty;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Comparator;
import java.util.Map;
import java.util.OptionalInt;
import java.util.function.Supplier;

/**
 * A comparator implementing the current heuristic cost model for the {@link CascadesPlanner}.
 */
@API(API.Status.EXPERIMENTAL)
public class CascadesCostModel implements Comparator<RelationalExpression> {
    @Nonnull
    private final RecordQueryPlannerConfiguration configuration;
    @Nonnull
    private final PlanContext planContext;

    public CascadesCostModel(@Nonnull RecordQueryPlannerConfiguration configuration,
                             @Nonnull PlanContext planContext) {
        this.configuration = configuration;
        this.planContext = planContext;
    }

    @Override
    public int compare(@Nonnull RelationalExpression a, @Nonnull RelationalExpression b) {
        if (a instanceof RecordQueryPlan && !(b instanceof RecordQueryPlan)) {
            return -1;
        }
        if (!(a instanceof RecordQueryPlan) && b instanceof RecordQueryPlan) {
            return 1;
        }

        int unsatisfiedFilterCompare = Integer.compare(PredicateCountProperty.evaluate(a),
                PredicateCountProperty.evaluate(b));
        if (unsatisfiedFilterCompare != 0) {
            return unsatisfiedFilterCompare;
        }

        final Map<Class<? extends RelationalExpression>, Integer> countDataAccessMapA =
                ExpressionCountProperty.evaluate(ImmutableSet.of(RecordQueryScanPlan.class, RecordQueryPlanWithIndex.class), a);

        final Map<Class<? extends RelationalExpression>, Integer> countDataAccessMapB =
                ExpressionCountProperty.evaluate(ImmutableSet.of(RecordQueryScanPlan.class, RecordQueryPlanWithIndex.class), b);

        final int numDataAccessA = countDataAccessMapA.getOrDefault(RecordQueryScanPlan.class, 0) +
                                   countDataAccessMapA.getOrDefault(RecordQueryPlanWithIndex.class, 0);

        final int numDataAccessB = countDataAccessMapB.getOrDefault(RecordQueryScanPlan.class, 0) +
                                   countDataAccessMapB.getOrDefault(RecordQueryPlanWithIndex.class, 0);

        int countDataAccessesCompare =
                Integer.compare(numDataAccessA, numDataAccessB);
        if (countDataAccessesCompare != 0) {
            return countDataAccessesCompare;
        }

        final int typeFilterCountA = TypeFilterCountProperty.evaluate(a);
        final int typeFilterCountB = TypeFilterCountProperty.evaluate(b);

        // special case
        // if one plan is a primary scan with a type filter and the other one is an index scan with the same number of
        // unsatisfied filters (i.e. both plans use the same number of filters as search arguments), we break the tie
        // by using a planning flag
        final OptionalInt primaryScanVsIndexScanCompareOptional =
                flipFlop(() -> comparePrimaryScanToIndexScan(countDataAccessMapA, countDataAccessMapB, typeFilterCountA),
                        () -> comparePrimaryScanToIndexScan(countDataAccessMapB, countDataAccessMapA, typeFilterCountB));
        if (primaryScanVsIndexScanCompareOptional.isPresent() && primaryScanVsIndexScanCompareOptional.getAsInt() != 0) {
            return primaryScanVsIndexScanCompareOptional.getAsInt();
        }

        int typeFilterCountCompare = Integer.compare(typeFilterCountA,
                typeFilterCountB);
        if (typeFilterCountCompare != 0) {
            return typeFilterCountCompare;
        }

        int typeFilterPositionCompare = Integer.compare(RelationalExpressionDepthProperty.TYPE_FILTER_DEPTH.evaluate(b),
                RelationalExpressionDepthProperty.TYPE_FILTER_DEPTH.evaluate(a)); // prefer the one with a deeper type filter
        if (typeFilterPositionCompare != 0) {
            return typeFilterPositionCompare;
        }

        int distinctFilterPositionCompare = Integer.compare(RelationalExpressionDepthProperty.DISTINCT_FILTER_DEPTH.evaluate(b),
                RelationalExpressionDepthProperty.DISTINCT_FILTER_DEPTH.evaluate(a));
        if (distinctFilterPositionCompare != 0) {
            return distinctFilterPositionCompare;
        }
        return Integer.compare(UnmatchedFieldsProperty.evaluate(planContext, a),
                UnmatchedFieldsProperty.evaluate(planContext, b));
    }

    private OptionalInt comparePrimaryScanToIndexScan(@Nonnull Map<Class<? extends RelationalExpression>, Integer> countDataAccessMapPrimaryScan,
                                                      @Nonnull Map<Class<? extends RelationalExpression>, Integer> countDataAccessMapIndexScan,
                                                      final int typeFilterCountPrimaryScan) {
        if (countDataAccessMapPrimaryScan.getOrDefault(RecordQueryScanPlan.class, 0) == 1 &&
                countDataAccessMapPrimaryScan.getOrDefault(RecordQueryPlanWithIndex.class, 0) == 0 &&
                countDataAccessMapIndexScan.getOrDefault(RecordQueryScanPlan.class, 0) == 0 &&
                countDataAccessMapIndexScan.getOrDefault(RecordQueryPlanWithIndex.class, 0) == 1) {
            if (typeFilterCountPrimaryScan > 0) {
                if (configuration.getIndexScanPreference() == IndexScanPreference.PREFER_SCAN) {
                    return OptionalInt.of(-1);
                } else {
                    return OptionalInt.of(1);
                }
            }

            return OptionalInt.of(1);
        }
        return OptionalInt.empty();
    }

    private static OptionalInt flipFlop(final Supplier<OptionalInt> variantA,
                                        final Supplier<OptionalInt> variantB) {
        final OptionalInt resultA = variantA.get();
        if (resultA.isPresent()) {
            return resultA;
        } else {
            final OptionalInt resultB = variantB.get();
            if (resultB.isPresent()) {
                return OptionalInt.of(-1 * resultB.getAsInt());
            }
        }

        return OptionalInt.empty();
    }
}
