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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.QueryPlanner.IndexScanPreference;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.cascades.properties.ExpressionCountProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.PredicateCountProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.RelationalExpressionDepthProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.ScanComparisonsProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.TypeFilterCountProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.UnmatchedFieldsCountProperty;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Comparator;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Supplier;

import static com.apple.foundationdb.record.Bindings.Internal.CORRELATION;

/**
 * A comparator implementing the current heuristic cost model for the {@link CascadesPlanner}.
 */
@API(API.Status.EXPERIMENTAL)
public class CascadesCostModel implements Comparator<RelationalExpression> {
    private static Set<Class<? extends RelationalExpression>> interestingPlanClasses =
            ImmutableSet.of(
                    RecordQueryScanPlan.class,
                    RecordQueryPlanWithIndex.class,
                    RecordQueryCoveringIndexPlan.class,
                    RecordQueryFetchFromPartialRecordPlan.class,
                    RecordQueryInJoinPlan.class);
    
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

        final Map<Class<? extends RelationalExpression>, Integer> planCountMapA =
                ExpressionCountProperty.evaluate(interestingPlanClasses, a);

        final Map<Class<? extends RelationalExpression>, Integer> planCountMapB =
                ExpressionCountProperty.evaluate(interestingPlanClasses, b);

        final int numDataAccessA = planCountMapA.getOrDefault(RecordQueryScanPlan.class, 0) +
                                   planCountMapA.getOrDefault(RecordQueryPlanWithIndex.class, 0) +
                                   planCountMapA.getOrDefault(RecordQueryCoveringIndexPlan.class, 0);

        final int numDataAccessB = planCountMapB.getOrDefault(RecordQueryScanPlan.class, 0) +
                                   planCountMapB.getOrDefault(RecordQueryPlanWithIndex.class, 0) +
                                   planCountMapB.getOrDefault(RecordQueryCoveringIndexPlan.class, 0);

        int countDataAccessesCompare =
                Integer.compare(numDataAccessA, numDataAccessB);
        if (countDataAccessesCompare != 0) {
            return countDataAccessesCompare;
        }

        // special case
        // if one plan is a inUnion plan
        final OptionalInt inPlanVsOtherOptional =
                flipFlop(() -> compareInOperator(a, b), () -> compareInOperator(b, a));
        if (inPlanVsOtherOptional.isPresent() && inPlanVsOtherOptional.getAsInt() != 0) {
            return inPlanVsOtherOptional.getAsInt();
        }

        final int typeFilterCountA = TypeFilterCountProperty.evaluate(a);
        final int typeFilterCountB = TypeFilterCountProperty.evaluate(b);

        // special case
        // if one plan is a primary scan with a type filter and the other one is an index scan with the same number of
        // unsatisfied filters (i.e. both plans use the same number of filters as search arguments), we break the tie
        // by using a planning flag
        final OptionalInt primaryScanVsIndexScanCompareOptional =
                flipFlop(() -> comparePrimaryScanToIndexScan(planCountMapA, planCountMapB, typeFilterCountA),
                        () -> comparePrimaryScanToIndexScan(planCountMapB, planCountMapA, typeFilterCountB));
        if (primaryScanVsIndexScanCompareOptional.isPresent() && primaryScanVsIndexScanCompareOptional.getAsInt() != 0) {
            return primaryScanVsIndexScanCompareOptional.getAsInt();
        }

        int typeFilterCountCompare = Integer.compare(typeFilterCountA, typeFilterCountB);
        if (typeFilterCountCompare != 0) {
            return typeFilterCountCompare;
        }

        int typeFilterPositionCompare = Integer.compare(RelationalExpressionDepthProperty.TYPE_FILTER_DEPTH.evaluate(b),
                RelationalExpressionDepthProperty.TYPE_FILTER_DEPTH.evaluate(a)); // prefer the one with a deeper type filter
        if (typeFilterPositionCompare != 0) {
            return typeFilterPositionCompare;
        }

        if (planCountMapA.getOrDefault(RecordQueryPlanWithIndex.class, 0) + planCountMapA.getOrDefault(RecordQueryCoveringIndexPlan.class, 0) > 0 &&
                planCountMapB.getOrDefault(RecordQueryPlanWithIndex.class, 0) + planCountMapB.getOrDefault(RecordQueryCoveringIndexPlan.class, 0) > 0) {
            // both plans are index scans

            // how many fetches are there, regular index scans fetch when they scan
            int numFetchesA = planCountMapA.getOrDefault(RecordQueryPlanWithIndex.class, 0) + planCountMapA.getOrDefault(RecordQueryFetchFromPartialRecordPlan.class, 0);
            int numFetchesB = planCountMapB.getOrDefault(RecordQueryPlanWithIndex.class, 0) + planCountMapB.getOrDefault(RecordQueryFetchFromPartialRecordPlan.class, 0);

            final int numFetchesCompare = Integer.compare(numFetchesA, numFetchesB);
            if (numFetchesCompare != 0) {
                return numFetchesCompare;
            }

            final int fetchDepthB = RelationalExpressionDepthProperty.FETCH_DEPTH.evaluate(b);
            final int fetchDepthA = RelationalExpressionDepthProperty.FETCH_DEPTH.evaluate(a);
            int fetchPositionCompare = Integer.compare(fetchDepthA, fetchDepthB);
            if (fetchPositionCompare != 0) {
                return fetchPositionCompare;
            }

            // All things being equal for index vs covering index -- there are plans competing of the following shape
            // FETCH(COVERING(INDEX_SCAN())) vs INDEX_SCAN() that count identically up to here. Let the plan win that
            // has fewer actual FETCH() operators.
            int numFetchOperatorsCompare =
                    Integer.compare(planCountMapA.getOrDefault(RecordQueryFetchFromPartialRecordPlan.class, 0),
                            planCountMapB.getOrDefault(RecordQueryFetchFromPartialRecordPlan.class, 0));
            if (numFetchOperatorsCompare != 0) {
                return numFetchOperatorsCompare;
            }
        }

        int distinctFilterPositionCompare = Integer.compare(RelationalExpressionDepthProperty.DISTINCT_FILTER_DEPTH.evaluate(b),
                RelationalExpressionDepthProperty.DISTINCT_FILTER_DEPTH.evaluate(a));
        if (distinctFilterPositionCompare != 0) {
            return distinctFilterPositionCompare;
        }

        int ufpA = UnmatchedFieldsCountProperty.evaluate(planContext, a);
        int ufpB = UnmatchedFieldsCountProperty.evaluate(planContext, b);
        if (ufpA != ufpB) {
            return Integer.compare(ufpA, ufpB);
        }

        //
        //  If a plan has more in-join sources it is preferable.
        //
        final int numSourcesInJoinA = planCountMapA.getOrDefault(RecordQueryInJoinPlan.class, 0);
        final int numSourcesInJoinB = planCountMapB.getOrDefault(RecordQueryInJoinPlan.class, 0);

        int countSourcesInJoinCompare =
                Integer.compare(numSourcesInJoinB, numSourcesInJoinA);
        if (countSourcesInJoinCompare != 0) {
            // bigger one wins
            return countSourcesInJoinCompare;
        }
        
        //
        // If plans are indistinguishable from a cost perspective, select one by planHash. This would make the cost model stable
        // (select the same plan on subsequent plannings).
        //
        if ((a instanceof PlanHashable) && (b instanceof PlanHashable)) {
            int hA = ((PlanHashable)a).planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS);
            int hB = ((PlanHashable)b).planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS);
            return Integer.compare(hA, hB);
        }

        return 0;
    }

    /**
     * Method to break a tie between a plan using singular index scan and one using a singular primary scan.
     *
     * The problematic case this method tries to resolve is that:
     *
     * <ul>
     *     <li>we have a scan plan that is not constraining the types of records (bad) but naturally does not need a fetch (good)</li>
     *     <li>we have an index scan that is constraining the records to one type (good) but needs a fetch (bad)</li>
     * </ul>
     *
     * The method is written in a way that it attempts to establish that the first parameter is assumed to be the primary
     * scan plan and the second parameter is assumed to be the index scan. We verify the assumption and return
     * {@code OptionalInt.empty()} if it does not hold true. This method is meant to be called using
     * {@link #flipFlop(Supplier, Supplier)} meaning that we will discover if the opposite holds true.
     *
     * @param planCountMapPrimaryScan map to hold counts for the primary scan plan
     * @param planCountMapIndexScan map to hold counts for the index scan plan
     * @param typeFilterCountPrimaryScan number of type filters on the primary scan plan
     * @return an {@link OptionalInt} that is the result of the comparison between a primary scan plan and an index
     *         scan plan, or {@code OptionalInt.empty()}.
     */
    private OptionalInt comparePrimaryScanToIndexScan(@Nonnull Map<Class<? extends RelationalExpression>, Integer> planCountMapPrimaryScan,
                                                      @Nonnull Map<Class<? extends RelationalExpression>, Integer> planCountMapIndexScan,
                                                      final int typeFilterCountPrimaryScan) {
        if (planCountMapPrimaryScan.getOrDefault(RecordQueryScanPlan.class, 0) == 1 &&
                planCountMapPrimaryScan.getOrDefault(RecordQueryPlanWithIndex.class, 0) == 0 &&
                planCountMapIndexScan.getOrDefault(RecordQueryScanPlan.class, 0) == 0 &&
                isSingularIndexScanWithFetch(planCountMapIndexScan)) {
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

    /**
     * This comparator compares the left expression which must be of type {@link RecordQueryInUnionPlan} or
     * {@link RecordQueryInJoinPlan} and only returns an indication that the other plan is considered preferable
     * or that this plan and the other plan are comparable. It never returns that the in-plan should be preferable.
     * The reasoning behind this is to avoid plans that were generated out of an IN-transformation that wasn't able
     * to translate the rewritten equality into an index search argument (SARG).
     * @param leftExpression this expression
     * @param rightExpression other expression
     * @return {@code OptionalInt.empty()} if the comparator is unable to compare the two expressions handed in. That
     *         happens if the left expression is not an in-plan (see {@link #isInPlan(RelationalExpression)}). If the
     *         left expression is an in-plan it returns {@code OptionalInt.of(1)} (pick other) if none of the
     *         in-arguments are sargs underneath the in-plan and {@code OptionalInt.of(1)} if at least one of the
     *         in-arguments have turned into sargables. That in turn causes the remainder of the tie-breaking code
     *         to be used.
     */
    @SuppressWarnings("java:S1172")
    private OptionalInt compareInOperator(@Nonnull final RelationalExpression leftExpression,
                                          @SuppressWarnings("unused") @Nonnull final RelationalExpression rightExpression) {
        if (!isInPlan(leftExpression)) {
            return OptionalInt.empty();
        }
        
        // If no scan comparison on the in union side uses a comparison to the in-values, then the in union
        // plan is not useful.
        final Set<ScanComparisons> scanComparisonsSet = ScanComparisonsProperty.evaluate(leftExpression);

        final ImmutableSet<CorrelationIdentifier> scanComparisonsCorrelatedTo =
                scanComparisonsSet
                        .stream()
                        .flatMap(scanComparisons ->
                                scanComparisons.getEqualityComparisons()
                                        .stream()
                                        .filter(comparison -> comparison instanceof Comparisons.ValueComparison)
                                        .map(comparison -> (Comparisons.ValueComparison)comparison))
                        .flatMap(comparison -> comparison.getCorrelatedTo().stream())
                        .collect(ImmutableSet.toImmutableSet());

        if (leftExpression instanceof RecordQueryInJoinPlan) {
            final var inJoinPlan = (RecordQueryInJoinPlan)leftExpression;
            final var inSource = inJoinPlan.getInSource();
            if (!scanComparisonsCorrelatedTo.contains(CorrelationIdentifier.of(CORRELATION.identifier(inSource.getBindingName())))) {
                return OptionalInt.of(1);
            }
        } else if (leftExpression instanceof RecordQueryInUnionPlan) {
            final var inUnionPlan = (RecordQueryInUnionPlan)leftExpression;
            if (inUnionPlan.getInSources()
                    .stream()
                    .noneMatch(inValuesSource -> scanComparisonsCorrelatedTo.contains(CorrelationIdentifier.of(CORRELATION.identifier(inValuesSource.getBindingName()))))) {
                return OptionalInt.of(1);
            }
        }

        return OptionalInt.of(0);
    }

    private static boolean isInPlan(@Nonnull final RelationalExpression expression) {
        return expression instanceof RecordQueryInJoinPlan || expression instanceof RecordQueryInUnionPlan;
    }

    private static boolean isSingularIndexScanWithFetch(@Nonnull Map<Class<? extends RelationalExpression>, Integer> countDataAccessMapIndexScan) {
        return countDataAccessMapIndexScan.getOrDefault(RecordQueryPlanWithIndex.class, 0) == 1 ||
               (countDataAccessMapIndexScan.getOrDefault(RecordQueryCoveringIndexPlan.class, 0) == 1 &&
                countDataAccessMapIndexScan.getOrDefault(RecordQueryFetchFromPartialRecordPlan.class, 0) == 1);
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
