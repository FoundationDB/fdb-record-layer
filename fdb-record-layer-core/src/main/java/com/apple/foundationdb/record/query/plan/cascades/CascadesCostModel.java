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
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.properties.CardinalitiesProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.CardinalitiesProperty.Cardinalities;
import com.apple.foundationdb.record.query.plan.cascades.properties.CardinalitiesProperty.Cardinality;
import com.apple.foundationdb.record.query.plan.cascades.properties.ComparisonsProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.FindExpressionProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.NormalizedResidualPredicateProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.RelationalExpressionDepthProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.TypeFilterCountProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.UnmatchedFieldsCountProperty;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryMapPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicatesFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

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
    @Nonnull
    private static final Set<Class<? extends RelationalExpression>> interestingPlanClasses =
            ImmutableSet.of(
                    RecordQueryScanPlan.class,
                    RecordQueryPlanWithIndex.class,
                    RecordQueryCoveringIndexPlan.class,
                    RecordQueryFetchFromPartialRecordPlan.class,
                    RecordQueryInJoinPlan.class,
                    RecordQueryMapPlan.class,
                    RecordQueryPredicatesFilterPlan.class);

    @Nonnull
    private final RecordQueryPlannerConfiguration configuration;

    public CascadesCostModel(@Nonnull RecordQueryPlannerConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public int compare(@Nonnull RelationalExpression a, @Nonnull RelationalExpression b) {
        if (a instanceof RecordQueryPlan && !(b instanceof RecordQueryPlan)) {
            return -1;
        }
        if (!(a instanceof RecordQueryPlan) && b instanceof RecordQueryPlan) {
            return 1;
        }

        final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> planOpsMapA =
                FindExpressionProperty.evaluate(interestingPlanClasses, a);

        final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> planOpsMapB =
                FindExpressionProperty.evaluate(interestingPlanClasses, b);

        final Cardinalities cardinalitiesA = CardinalitiesProperty.evaluate(a);
        final Cardinalities cardinalitiesB = CardinalitiesProperty.evaluate(b);

        //
        // Technically, both cardinalities at runtime must be the same. The question is if we can actually
        // statically prove that there is a max cardinality other than the unknown cardinality.
        //
        if (!cardinalitiesA.getMaxCardinality().isUnknown() || !cardinalitiesB.getMaxCardinality().isUnknown()) {
            final Cardinality maxOfMaxCardinalityOfAllDataAccessesA = maxOfMaxCardinalitiesOfAllDataAccesses(planOpsMapA);
            final Cardinality maxOfMaxCardinalityOfAllDataAccessesB = maxOfMaxCardinalitiesOfAllDataAccesses(planOpsMapB);

            if (!maxOfMaxCardinalityOfAllDataAccessesA.isUnknown() || !maxOfMaxCardinalityOfAllDataAccessesB.isUnknown()) {
                // at least one of them is not just unknown
                if (maxOfMaxCardinalityOfAllDataAccessesA.isUnknown()) {
                    return 1;
                }
                if (maxOfMaxCardinalityOfAllDataAccessesB.isUnknown()) {
                    return -1;
                }
                int maxOfMaxCardinalityCompare =
                        Long.compare(maxOfMaxCardinalityOfAllDataAccessesA.getCardinality(),
                                maxOfMaxCardinalityOfAllDataAccessesB.getCardinality());
                if (maxOfMaxCardinalityCompare != 0) {
                    return maxOfMaxCardinalityCompare;
                }
            }
        }

        int unsatisfiedFilterCompare = Long.compare(NormalizedResidualPredicateProperty.countNormalizedConjuncts(a),
                NormalizedResidualPredicateProperty.countNormalizedConjuncts(b));
        if (unsatisfiedFilterCompare != 0) {
            return unsatisfiedFilterCompare;
        }

        final int numDataAccessA =
                count(planOpsMapA,
                        RecordQueryScanPlan.class,
                        RecordQueryPlanWithIndex.class,
                        RecordQueryCoveringIndexPlan.class);


        final int numDataAccessB =
                count(planOpsMapB,
                        RecordQueryScanPlan.class,
                        RecordQueryPlanWithIndex.class,
                        RecordQueryCoveringIndexPlan.class);

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
                flipFlop(() -> comparePrimaryScanToIndexScan(a, b, planOpsMapA, planOpsMapB, typeFilterCountA, typeFilterCountB),
                        () -> comparePrimaryScanToIndexScan(b, a, planOpsMapB, planOpsMapA, typeFilterCountB, typeFilterCountA));
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

        if (count(planOpsMapA, RecordQueryPlanWithIndex.class, RecordQueryCoveringIndexPlan.class) > 0 &&
                count(planOpsMapB, RecordQueryPlanWithIndex.class, RecordQueryCoveringIndexPlan.class) > 0) {
            // both plans are index scans

            // how many fetches are there, regular index scans fetch when they scan
            int numFetchesA = count(planOpsMapA, RecordQueryPlanWithIndex.class, RecordQueryFetchFromPartialRecordPlan.class);
            int numFetchesB = count(planOpsMapB, RecordQueryPlanWithIndex.class, RecordQueryFetchFromPartialRecordPlan.class);

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
                    Integer.compare(count(planOpsMapA, RecordQueryFetchFromPartialRecordPlan.class),
                            count(planOpsMapB, RecordQueryFetchFromPartialRecordPlan.class));
            if (numFetchOperatorsCompare != 0) {
                return numFetchOperatorsCompare;
            }
        }

        int distinctFilterPositionCompare = Integer.compare(RelationalExpressionDepthProperty.DISTINCT_FILTER_DEPTH.evaluate(b),
                RelationalExpressionDepthProperty.DISTINCT_FILTER_DEPTH.evaluate(a));
        if (distinctFilterPositionCompare != 0) {
            return distinctFilterPositionCompare;
        }

        int ufpA = UnmatchedFieldsCountProperty.evaluate(a);
        int ufpB = UnmatchedFieldsCountProperty.evaluate(b);
        if (ufpA != ufpB) {
            return Integer.compare(ufpA, ufpB);
        }

        //
        //  If a plan has more in-join sources, it is preferable.
        //
        final int numSourcesInJoinA = count(planOpsMapA, RecordQueryInJoinPlan.class);
        final int numSourcesInJoinB = count(planOpsMapB, RecordQueryInJoinPlan.class);

        int numSourcesInJoinCompare =
                Integer.compare(numSourcesInJoinB, numSourcesInJoinA);
        if (numSourcesInJoinCompare != 0) {
            // bigger one wins
            return numSourcesInJoinCompare;
        }

        //
        //  If a plan has fewer MAP/FILTERS operations, it is preferable.
        //
        final int numSimpleOperationsA = count(planOpsMapA, RecordQueryMapPlan.class) +
                count(planOpsMapA, RecordQueryPredicatesFilterPlan.class);
        final int numSimpleOperationsB = count(planOpsMapB, RecordQueryMapPlan.class) +
                count(planOpsMapB, RecordQueryPredicatesFilterPlan.class);

        int numSimpleOperationsCompare =
                Integer.compare(numSimpleOperationsA, numSimpleOperationsB);
        if (numSimpleOperationsCompare != 0) {
            // smaller one wins
            return numSimpleOperationsCompare;
        }
        
        //
        // If plans are indistinguishable from a cost perspective, select one by planHash. This would make the cost model stable
        // (select the same plan on subsequent plannings).
        //
        if ((a instanceof PlanHashable) && (b instanceof PlanHashable)) {
            int hA = ((PlanHashable)a).planHash(PlanHashable.CURRENT_FOR_CONTINUATION);
            int hB = ((PlanHashable)b).planHash(PlanHashable.CURRENT_FOR_CONTINUATION);
            return Integer.compare(hA, hB);
        }

        return 0;
    }

    @Nonnull
    private Cardinality maxOfMaxCardinalitiesOfAllDataAccesses(@Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> planOpsMap) {
        return FindExpressionProperty.slice(planOpsMap, RecordQueryScanPlan.class, RecordQueryPlanWithIndex.class, RecordQueryCoveringIndexPlan.class)
                .stream()
                .map(plan -> CardinalitiesProperty.evaluate(plan).getMaxCardinality())
                .reduce(Cardinality.ofCardinality(0),
                        (l, r) -> {
                            if (l.isUnknown()) {
                                return l;
                            }
                            if (r.isUnknown()) {
                                return r;
                            }
                            return l.getCardinality() > r.getCardinality() ? l : r;
                        });
    }

    /**
     * Method to break a tie between a plan using singular index scan and one using a singular primary scan.
     * <br>
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
     * @param planOpsMapPrimaryScan map to hold counts for the primary scan plan
     * @param planOpsMapIndexScan map to hold counts for the index scan plan
     * @param typeFilterCountPrimaryScan number of type filters on the primary scan plan
     * @return an {@link OptionalInt} that is the result of the comparison between a primary scan plan and an index
     *         scan plan, or {@code OptionalInt.empty()}.
     */
    private OptionalInt comparePrimaryScanToIndexScan(@Nonnull RelationalExpression primaryScan,
                                                      @Nonnull RelationalExpression indexScan,
                                                      @Nonnull Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> planOpsMapPrimaryScan,
                                                      @Nonnull Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> planOpsMapIndexScan,
                                                      final int typeFilterCountPrimaryScan,
                                                      final int typeFilterCountIndexScan) {
        if (count(planOpsMapPrimaryScan, RecordQueryScanPlan.class) == 1 &&
                count(planOpsMapPrimaryScan, RecordQueryPlanWithIndex.class) == 0 &&
                count(planOpsMapIndexScan, RecordQueryScanPlan.class) == 0 &&
                isSingularIndexScanWithFetch(planOpsMapIndexScan)) {

            if (typeFilterCountPrimaryScan > 0 && typeFilterCountIndexScan == 0) {
                final var primaryScanComparisons = ComparisonsProperty.evaluate(primaryScan);
                final var indexScanComparisons = ComparisonsProperty.evaluate(indexScan);

                //
                // The primary scan side has a type filter in it, the index scan side does not. The primary side
                // does not need a fetch, though. We need to weigh the additional type filter on the primary side
                // and a potentially high discard rate against the cost of an additional fetch. If the index scan
                // has any additional comparisons that the primary scan does not have, we'll side in favor of the
                // index scan.
                //
                final var primaryMinusIndex = Sets.difference(primaryScanComparisons, indexScanComparisons);
                if (primaryMinusIndex.isEmpty()) {
                    final var indexMinusPrimary =
                            Sets.difference(indexScanComparisons, primaryScanComparisons);
                    //
                    // Note that we don't need to worry about the index scan using a comparison on the record type key.
                    // If that is the case, the primary scan must also use that same comparison (or we wouldn't be in
                    // this if branch). If the primary uses this comparison, then there is no need for that side
                    // to also use a type filter.
                    //
                    if (!indexMinusPrimary.isEmpty()) {
                        return OptionalInt.of(1);
                    }
                }
            }

            if (configuration.getIndexScanPreference() == IndexScanPreference.PREFER_SCAN) {
                return OptionalInt.of(-1);
            } else {
                return OptionalInt.of(1);
            }
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
        final Set<Comparisons.Comparison> scanComparisonsSet = ComparisonsProperty.evaluate(leftExpression);

        final ImmutableSet<CorrelationIdentifier> scanComparisonsCorrelatedTo =
                scanComparisonsSet
                        .stream()
                        .filter(comparison -> comparison instanceof Comparisons.ValueComparison)
                        .map(comparison -> (Comparisons.ValueComparison)comparison)
                        .filter(comparison -> comparison.getType() == Comparisons.Type.EQUALS)
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

    private static boolean isSingularIndexScanWithFetch(@Nonnull Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> planOpsMapIndexScan) {
        return count(planOpsMapIndexScan, RecordQueryPlanWithIndex.class) == 1 ||
               (count(planOpsMapIndexScan, RecordQueryCoveringIndexPlan.class) == 1 &&
                count(planOpsMapIndexScan, RecordQueryFetchFromPartialRecordPlan.class) == 1);
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

    @SafeVarargs
    private static int count(@Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> expressionsMap, @Nonnull final Class<? extends RelationalExpression>... interestingClasses) {
        return FindExpressionProperty.slice(expressionsMap, interestingClasses).size();
    }
}
