/*
 * PlanningCostModel.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.QueryPlanner.IndexScanPreference;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.FindExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.PlannerPhase;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.properties.CardinalitiesProperty.Cardinalities;
import com.apple.foundationdb.record.query.plan.cascades.properties.CardinalitiesProperty.Cardinality;
import com.apple.foundationdb.record.query.plan.cascades.properties.ExpressionDepthProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.NormalizedResidualPredicateProperty;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFlatMapPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryMapPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicatesFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.google.common.base.Verify;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.apple.foundationdb.record.Bindings.Internal.CORRELATION;
import static com.apple.foundationdb.record.query.plan.cascades.properties.CardinalitiesProperty.cardinalities;
import static com.apple.foundationdb.record.query.plan.cascades.properties.ComparisonsProperty.comparisons;
import static com.apple.foundationdb.record.query.plan.cascades.properties.ExpressionDepthProperty.fetchDepth;
import static com.apple.foundationdb.record.query.plan.cascades.properties.ExpressionDepthProperty.typeFilterDepth;
import static com.apple.foundationdb.record.query.plan.cascades.properties.UnmatchedFieldsCountProperty.unmatchedFieldsCount;

/**
 * A comparator implementing the current heuristic cost model for the {@link CascadesPlanner} during the
 * {@link PlannerPhase#PLANNING} phase.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
@SpotBugsSuppressWarnings("SE_COMPARATOR_SHOULD_BE_SERIALIZABLE")
public class PlanningCostModel implements CascadesCostModel<RecordQueryPlan> {
    @Nonnull
    private static final Set<Class<? extends RelationalExpression>> interestingPlanClasses =
            ImmutableSet.of(
                    RecordQueryScanPlan.class,
                    RecordQueryPlanWithIndex.class,
                    RecordQueryCoveringIndexPlan.class,
                    RecordQueryFetchFromPartialRecordPlan.class,
                    RecordQueryInJoinPlan.class,
                    RecordQueryMapPlan.class,
                    RecordQueryTypeFilterPlan.class,
                    RecordQueryPredicatesFilterPlan.class);

    @Nonnull
    private static final Tiebreaker<RecordQueryPlan> tiebreaker =
            Tiebreaker.combineTiebreakers(ImmutableList.of(
                    smallestCardinalityOfDataAccessesTiebreaker(),
                    lowestNumUnsatisfiedFiltersTiebreaker(),
                    lowestNumDataAccessesTiebreaker(),
                    inOperatorTiebreaker(),
                    primaryScanVsIndexScanTiebreaker(),
                    lowestNumTypeFilterTiebreaker(),
                    deepestTypeFilterPositionTiebreaker(),
                    betterIndexScanTiebreaker(),
                    deepestDistinctTiebreaker(),
                    lowestNumUnmatchedFieldsTiebreaker(),
                    highestNumInJoinTiebreaker(),
                    lowestNumSimpleOperationsTiebreaker(),
                    lowestOuterCardinalityTiebreaker(),
                    planHashTiebreaker(),
                    PickRightTiebreaker.pickRightTiebreaker()));

    @Nonnull
    private final RecordQueryPlannerConfiguration configuration;

    public PlanningCostModel(@Nonnull final RecordQueryPlannerConfiguration configuration) {
        this.configuration = configuration;
    }

    @Nonnull
    @Override
    public RecordQueryPlannerConfiguration getConfiguration() {
        return configuration;
    }

    @Nonnull
    @Override
    public Optional<RecordQueryPlan> getBestExpression(@Nonnull final Set<? extends RelationalExpression> expressions,
                                                       @Nonnull final Consumer<RecordQueryPlan> onRemoveConsumer) {
        return costPlans(expressions, onRemoveConsumer).getOnlyExpressionMaybe();
    }

    @Nonnull
    private TiebreakerResult<RecordQueryPlan> costPlans(@Nonnull final Set<? extends RelationalExpression> expressions,
                                                        @Nonnull final Consumer<RecordQueryPlan> onRemoveConsumer) {
        final LoadingCache<RelationalExpression, Map<Class<? extends RelationalExpression>, Set<RelationalExpression>>> opsCache =
                createOpsCache();

        return Tiebreaker.ofContext(getConfiguration(), opsCache, expressions, RecordQueryPlan.class, onRemoveConsumer)
                .thenApply(tiebreaker);
    }

    @Nullable
    @Override
    public Integer compare(@Nonnull final RelationalExpression a,
                           @Nonnull final RelationalExpression b) {
        if (!(a instanceof RecordQueryPlan) && !(b instanceof RecordQueryPlan)) {
            return null;
        }

        if (a instanceof RecordQueryPlan && !(b instanceof RecordQueryPlan)) {
            return -1;
        }

        if (!(a instanceof RecordQueryPlan) /*&& b instanceof RecordQueryPlan*/) {
            return 1;
        }

        final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapA =
                FindExpressionVisitor.evaluate(interestingPlanClasses, a);
        final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapB =
                FindExpressionVisitor.evaluate(interestingPlanClasses, b);

        return tiebreaker.compare(getConfiguration(), opsMapA, opsMapB, (RecordQueryPlan)a, (RecordQueryPlan)b);
    }

    @Nonnull
    private static LoadingCache<RelationalExpression, Map<Class<? extends RelationalExpression>, Set<RelationalExpression>>>
            createOpsCache() {
        return CacheBuilder.newBuilder()
                .build(new CacheLoader<>() {
                    @Override
                    @Nonnull
                    public Map<Class<? extends RelationalExpression>, Set<RelationalExpression>>
                             load(@Nonnull final RelationalExpression key) {
                        return FindExpressionVisitor.evaluate(interestingPlanClasses, key);
                    }
                });
    }

    @Nonnull
    private static Cardinality maxOfMaxCardinalitiesOfAllDataAccesses(@Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> planOpsMap) {
        return FindExpressionVisitor.slice(planOpsMap, RecordQueryScanPlan.class, RecordQueryPlanWithIndex.class, RecordQueryCoveringIndexPlan.class)
                .stream()
                .map(plan -> cardinalities().evaluate(plan).getMaxCardinality())
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
     * @param plannerConfiguration the current planner configuration
     * @param primaryScan the {@link RecordQueryPlan} that is the primary scan
     * @param indexScan the {@link RecordQueryPlan} that is the index scan
     * @param planOpsMapPrimaryScan map to hold counts for the primary scan plan
     * @param planOpsMapIndexScan map to hold counts for the index scan plan
     * @return an {@link OptionalInt} that is the result of the comparison between a primary scan plan and an index
     *         scan plan, or {@code OptionalInt.empty()}.
     */
    private static OptionalInt comparePrimaryScanToIndexScan(@Nonnull final RecordQueryPlannerConfiguration plannerConfiguration,
                                                             @Nonnull RelationalExpression primaryScan,
                                                             @Nonnull RelationalExpression indexScan,
                                                             @Nonnull Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> planOpsMapPrimaryScan,
                                                             @Nonnull Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> planOpsMapIndexScan) {
        if (count(planOpsMapPrimaryScan, RecordQueryScanPlan.class) == 1 &&
                count(planOpsMapPrimaryScan, RecordQueryPlanWithIndex.class) == 0 &&
                count(planOpsMapIndexScan, RecordQueryScanPlan.class) == 0 &&
                isSingularIndexScanWithFetch(planOpsMapIndexScan)) {

            final int typeFilterCountPrimaryScan = count(planOpsMapPrimaryScan, RecordQueryTypeFilterPlan.class);
            final int typeFilterCountIndexScan = count(planOpsMapIndexScan, RecordQueryTypeFilterPlan.class);

            if (typeFilterCountPrimaryScan > 0 && typeFilterCountIndexScan == 0) {
                final var primaryScanComparisons = comparisons().evaluate(primaryScan);
                final var indexScanComparisons = comparisons().evaluate(indexScan);

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

            if (plannerConfiguration.getIndexScanPreference() == IndexScanPreference.PREFER_SCAN) {
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
    private static OptionalInt compareInOperator(@Nonnull final RelationalExpression leftExpression,
                                                 @SuppressWarnings("unused") @Nonnull final RelationalExpression rightExpression) {
        if (!isInPlan(leftExpression)) {
            return OptionalInt.empty();
        }

        // If no scan comparison on the in union side uses a comparison to the in-values, then the in union
        // plan is not useful.
        final Set<Comparisons.Comparison> scanComparisonsSet = comparisons().evaluate(leftExpression);

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
        return FindExpressionVisitor.slice(expressionsMap, interestingClasses).size();
    }

    @Nonnull
    static SmallestCardinalityOfDataAccessesTiebreaker smallestCardinalityOfDataAccessesTiebreaker() {
        return SmallestCardinalityOfDataAccessesTiebreaker.INSTANCE;
    }

    @Nonnull
    static LowestNumUnsatisfiedFiltersTiebreaker lowestNumUnsatisfiedFiltersTiebreaker() {
        return LowestNumUnsatisfiedFiltersTiebreaker.INSTANCE;
    }

    @Nonnull
    static LowestNumDataAccessesTiebreaker lowestNumDataAccessesTiebreaker() {
        return LowestNumDataAccessesTiebreaker.INSTANCE;
    }

    @Nonnull
    static InOperatorTiebreaker inOperatorTiebreaker() {
        return InOperatorTiebreaker.INSTANCE;
    }

    @Nonnull
    static PrimaryScanVsIndexScanTiebreaker primaryScanVsIndexScanTiebreaker() {
        return PrimaryScanVsIndexScanTiebreaker.INSTANCE;
    }

    @Nonnull
    static LowestNumTypeFilterTiebreaker lowestNumTypeFilterTiebreaker() {
        return LowestNumTypeFilterTiebreaker.INSTANCE;
    }

    @Nonnull
    static DeepestTypeFilterPositionTiebreaker deepestTypeFilterPositionTiebreaker() {
        return DeepestTypeFilterPositionTiebreaker.INSTANCE;
    }

    @Nonnull
    static BetterIndexScanTiebreaker betterIndexScanTiebreaker() {
        return BetterIndexScanTiebreaker.INSTANCE;
    }

    @Nonnull
    static DeepestDistinctTiebreaker deepestDistinctTiebreaker() {
        return DeepestDistinctTiebreaker.INSTANCE;
    }

    @Nonnull
    static LowestNumUnmatchedFieldsTiebreaker lowestNumUnmatchedFieldsTiebreaker() {
        return LowestNumUnmatchedFieldsTiebreaker.INSTANCE;
    }

    @Nonnull
    static HighestNumInJoinTiebreaker highestNumInJoinTiebreaker() {
        return HighestNumInJoinTiebreaker.INSTANCE;
    }

    @Nonnull
    static LowestNumSimpleOperationsTiebreaker lowestNumSimpleOperationsTiebreaker() {
        return LowestNumSimpleOperationsTiebreaker.INSTANCE;
    }

    @Nonnull
    static LowestOuterCardinalityTiebreaker lowestOuterCardinalityTiebreaker() {
        return LowestOuterCardinalityTiebreaker.INSTANCE;
    }

    @Nonnull
    static PlanHashTiebreaker planHashTiebreaker() {
        return PlanHashTiebreaker.INSTANCE;
    }

    static class SmallestCardinalityOfDataAccessesTiebreaker implements Tiebreaker<RecordQueryPlan> {
        private static final SmallestCardinalityOfDataAccessesTiebreaker INSTANCE = new SmallestCardinalityOfDataAccessesTiebreaker();

        @Override
        public int compare(@Nonnull final RecordQueryPlannerConfiguration configuration,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapA,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapB,
                           @Nonnull final RecordQueryPlan a, @Nonnull final RecordQueryPlan b) {
            final Cardinalities cardinalitiesA = cardinalities().evaluate(a);
            final Cardinalities cardinalitiesB = cardinalities().evaluate(b);

            //
            // Technically, both cardinalities at runtime must be the same. The question is if we can actually
            // statically prove that there is a max cardinality other than the unknown cardinality.
            //
            if (!cardinalitiesA.getMaxCardinality().isUnknown() || !cardinalitiesB.getMaxCardinality().isUnknown()) {
                final Cardinality maxOfMaxCardinalityOfAllDataAccessesA = maxOfMaxCardinalitiesOfAllDataAccesses(opsMapA);
                final Cardinality maxOfMaxCardinalityOfAllDataAccessesB = maxOfMaxCardinalitiesOfAllDataAccesses(opsMapB);

                if (!maxOfMaxCardinalityOfAllDataAccessesA.isUnknown() || !maxOfMaxCardinalityOfAllDataAccessesB.isUnknown()) {
                    // at least one of them is not just unknown
                    if (maxOfMaxCardinalityOfAllDataAccessesA.isUnknown()) {
                        return 1;
                    }
                    if (maxOfMaxCardinalityOfAllDataAccessesB.isUnknown()) {
                        return -1;
                    }
                    return Long.compare(maxOfMaxCardinalityOfAllDataAccessesA.getCardinality(),
                            maxOfMaxCardinalityOfAllDataAccessesB.getCardinality());
                }
            }

            return 0;
        }
    }

    static class LowestNumUnsatisfiedFiltersTiebreaker implements Tiebreaker<RecordQueryPlan> {
        private static final LowestNumUnsatisfiedFiltersTiebreaker INSTANCE = new LowestNumUnsatisfiedFiltersTiebreaker();

        @Override
        public int compare(@Nonnull final RecordQueryPlannerConfiguration configuration,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapA,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapB,
                           @Nonnull final RecordQueryPlan a, @Nonnull final RecordQueryPlan b) {
            return Long.compare(NormalizedResidualPredicateProperty.countNormalizedConjuncts(a),
                    NormalizedResidualPredicateProperty.countNormalizedConjuncts(b));
        }
    }

    static class LowestNumDataAccessesTiebreaker implements Tiebreaker<RecordQueryPlan> {
        private static final LowestNumDataAccessesTiebreaker INSTANCE = new LowestNumDataAccessesTiebreaker();

        @Override
        public int compare(@Nonnull final RecordQueryPlannerConfiguration configuration,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapA,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapB,
                           @Nonnull final RecordQueryPlan a, @Nonnull final RecordQueryPlan b) {
            final int numDataAccessA =
                    count(opsMapA,
                            RecordQueryScanPlan.class,
                            RecordQueryPlanWithIndex.class,
                            RecordQueryCoveringIndexPlan.class);


            final int numDataAccessB =
                    count(opsMapB,
                            RecordQueryScanPlan.class,
                            RecordQueryPlanWithIndex.class,
                            RecordQueryCoveringIndexPlan.class);

            return Integer.compare(numDataAccessA, numDataAccessB);
        }
    }

    static class InOperatorTiebreaker implements Tiebreaker<RecordQueryPlan> {
        private static final InOperatorTiebreaker INSTANCE = new InOperatorTiebreaker();

        @Override
        public int compare(@Nonnull final RecordQueryPlannerConfiguration configuration,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapA,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapB,
                           @Nonnull final RecordQueryPlan a, @Nonnull final RecordQueryPlan b) {
            // special case
            // if one plan is a inUnion plan
            final OptionalInt inPlanVsOtherOptional =
                    flipFlop(() -> compareInOperator(a, b), () -> compareInOperator(b, a));
            if (inPlanVsOtherOptional.isPresent() && inPlanVsOtherOptional.getAsInt() != 0) {
                return inPlanVsOtherOptional.getAsInt();
            }

            return 0;
        }
    }

    static class PrimaryScanVsIndexScanTiebreaker implements Tiebreaker<RecordQueryPlan> {
        private static final PrimaryScanVsIndexScanTiebreaker INSTANCE = new PrimaryScanVsIndexScanTiebreaker();

        @Override
        public int compare(@Nonnull final RecordQueryPlannerConfiguration configuration,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapA,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapB,
                           @Nonnull final RecordQueryPlan a, @Nonnull final RecordQueryPlan b) {
            // special case
            // if one plan is a primary scan with a type filter and the other one is an index scan with the same number of
            // unsatisfied filters (i.e. both plans use the same number of filters as search arguments), we break the tie
            // by using a planning flag
            final OptionalInt primaryScanVsIndexScanCompareOptional =
                    flipFlop(() -> comparePrimaryScanToIndexScan(configuration, a, b, opsMapA, opsMapB),
                            () -> comparePrimaryScanToIndexScan(configuration, b, a, opsMapB, opsMapA));
            if (primaryScanVsIndexScanCompareOptional.isPresent() && primaryScanVsIndexScanCompareOptional.getAsInt() != 0) {
                return primaryScanVsIndexScanCompareOptional.getAsInt();
            }
            return 0;
        }
    }

    static class LowestNumTypeFilterTiebreaker implements Tiebreaker<RecordQueryPlan> {
        private static final LowestNumTypeFilterTiebreaker INSTANCE = new LowestNumTypeFilterTiebreaker();

        @Override
        public int compare(@Nonnull final RecordQueryPlannerConfiguration configuration,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapA,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapB,
                           @Nonnull final RecordQueryPlan a, @Nonnull final RecordQueryPlan b) {
            return Integer.compare(count(opsMapA, RecordQueryTypeFilterPlan.class),
                    count(opsMapB, RecordQueryTypeFilterPlan.class));
        }
    }

    static class DeepestTypeFilterPositionTiebreaker implements Tiebreaker<RecordQueryPlan> {
        private static final DeepestTypeFilterPositionTiebreaker INSTANCE = new DeepestTypeFilterPositionTiebreaker();

        @Override
        public int compare(@Nonnull final RecordQueryPlannerConfiguration configuration,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapA,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapB,
                           @Nonnull final RecordQueryPlan a, @Nonnull final RecordQueryPlan b) {
            // prefer the one with a deeper type filter
            return Integer.compare(typeFilterDepth().evaluate(b), typeFilterDepth().evaluate(a));
        }
    }

    static class BetterIndexScanTiebreaker implements Tiebreaker<RecordQueryPlan> {
        private static final BetterIndexScanTiebreaker INSTANCE = new BetterIndexScanTiebreaker();

        @Override
        public int compare(@Nonnull final RecordQueryPlannerConfiguration configuration,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapA,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapB,
                           @Nonnull final RecordQueryPlan a, @Nonnull final RecordQueryPlan b) {
            if (count(opsMapA, RecordQueryPlanWithIndex.class, RecordQueryCoveringIndexPlan.class) > 0 &&
                    count(opsMapB, RecordQueryPlanWithIndex.class, RecordQueryCoveringIndexPlan.class) > 0) {
                // both plans are index scans

                // how many fetches are there, regular index scans fetch when they scan
                int numFetchesA = count(opsMapA, RecordQueryPlanWithIndex.class, RecordQueryFetchFromPartialRecordPlan.class);
                int numFetchesB = count(opsMapB, RecordQueryPlanWithIndex.class, RecordQueryFetchFromPartialRecordPlan.class);

                final int numFetchesCompare = Integer.compare(numFetchesA, numFetchesB);
                if (numFetchesCompare != 0) {
                    return numFetchesCompare;
                }

                final int fetchDepthA = fetchDepth().evaluate(a);
                final int fetchDepthB = fetchDepth().evaluate(b);
                int fetchPositionCompare = Integer.compare(fetchDepthA, fetchDepthB);
                if (fetchPositionCompare != 0) {
                    return fetchPositionCompare;
                }

                // All things being equal for index vs covering index -- there are plans competing of the following shape
                // FETCH(COVERING(INDEX_SCAN())) vs INDEX_SCAN() that count identically up to here. Let the plan win that
                // has fewer actual FETCH() operators.
                return Integer.compare(count(opsMapA, RecordQueryFetchFromPartialRecordPlan.class),
                        count(opsMapB, RecordQueryFetchFromPartialRecordPlan.class));
            }
            return 0;
        }
    }

    static class DeepestDistinctTiebreaker implements Tiebreaker<RecordQueryPlan> {
        private static final DeepestDistinctTiebreaker INSTANCE = new DeepestDistinctTiebreaker();

        @Override
        public int compare(@Nonnull final RecordQueryPlannerConfiguration configuration,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapA,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapB,
                           @Nonnull final RecordQueryPlan a, @Nonnull final RecordQueryPlan b) {
            return Integer.compare(ExpressionDepthProperty.distinctDepth().evaluate(b),
                    ExpressionDepthProperty.distinctDepth().evaluate(a));
        }
    }

    static class LowestNumUnmatchedFieldsTiebreaker implements Tiebreaker<RecordQueryPlan> {
        private static final LowestNumUnmatchedFieldsTiebreaker INSTANCE = new LowestNumUnmatchedFieldsTiebreaker();

        @Override
        public int compare(@Nonnull final RecordQueryPlannerConfiguration configuration,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapA,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapB,
                           @Nonnull final RecordQueryPlan a, @Nonnull final RecordQueryPlan b) {
            return Integer.compare(unmatchedFieldsCount().evaluate(a), unmatchedFieldsCount().evaluate(b));
        }
    }

    static class HighestNumInJoinTiebreaker implements Tiebreaker<RecordQueryPlan> {
        private static final HighestNumInJoinTiebreaker INSTANCE = new HighestNumInJoinTiebreaker();

        @Override
        public int compare(@Nonnull final RecordQueryPlannerConfiguration configuration,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapA,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapB,
                           @Nonnull final RecordQueryPlan a, @Nonnull final RecordQueryPlan b) {
            //
            //  If a plan has more in-join sources, it is preferable.
            //
            return Integer.compare(count(opsMapB, RecordQueryInJoinPlan.class),
                    count(opsMapA, RecordQueryInJoinPlan.class));
        }
    }

    static class LowestNumSimpleOperationsTiebreaker implements Tiebreaker<RecordQueryPlan> {
        private static final LowestNumSimpleOperationsTiebreaker INSTANCE = new LowestNumSimpleOperationsTiebreaker();

        @Override
        public int compare(@Nonnull final RecordQueryPlannerConfiguration configuration,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapA,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapB,
                           @Nonnull final RecordQueryPlan a, @Nonnull final RecordQueryPlan b) {
            //
            //  If a plan has fewer MAP/FILTERS operations, it is preferable.
            //
            final int numSimpleOperationsA = count(opsMapA, RecordQueryMapPlan.class) +
                    count(opsMapA, RecordQueryPredicatesFilterPlan.class);
            final int numSimpleOperationsB = count(opsMapB, RecordQueryMapPlan.class) +
                    count(opsMapB, RecordQueryPredicatesFilterPlan.class);

            // smaller one wins
            return Integer.compare(numSimpleOperationsA, numSimpleOperationsB);
        }
    }

    static class LowestOuterCardinalityTiebreaker implements Tiebreaker<RecordQueryPlan> {
        private static final LowestOuterCardinalityTiebreaker INSTANCE = new LowestOuterCardinalityTiebreaker();

        @Override
        public int compare(@Nonnull final RecordQueryPlannerConfiguration configuration,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapA,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapB,
                           @Nonnull final RecordQueryPlan a,
                           @Nonnull final RecordQueryPlan b) {
            //
            // If both plans are nested loop joins. Attempt to pick a plan with a more preferable join ordering
            //
            if (a instanceof RecordQueryFlatMapPlan && b instanceof RecordQueryFlatMapPlan) {
                final List<RecordQueryPlan> aChildren = a.getChildren();
                Verify.verify(aChildren.size() == 2);
                final RecordQueryPlan aOuter = aChildren.get(0);

                final List<RecordQueryPlan> bChildren = b.getChildren();
                Verify.verify(bChildren.size() == 2);
                final RecordQueryPlan bOuter = bChildren.get(0);

                //
                // Return the one with lower cardinality on the outer plan
                //
                // This is an imperfect heuristic, but the idea is that if we have something that
                // only returns a small number (especially 1) number of results, we want that to
                // be on the outside so that we execute the inner (with more results) fewer times.
                // If there's just one result, that's probably safe, though we may have to adjust
                // this, as the actual more important thing is going to be the discard rate--the
                // optimal plan should have fewer discarded records, which may involve placing the
                // lower cardinality plan in the inner
                //
                final Cardinalities aOuterCardinalities = cardinalities().evaluate(aOuter);
                final Cardinalities bOuterCardinalities = cardinalities().evaluate(bOuter);
                if (!aOuterCardinalities.getMaxCardinality().isUnknown() || !bOuterCardinalities.getMaxCardinality().isUnknown()) {
                    long aEffectiveMaxCardinality = aOuterCardinalities.getMaxCardinality().isUnknown() ? Long.MAX_VALUE : aOuterCardinalities.getMaxCardinality().getCardinality();
                    long bEffectiveMaxCardinality = bOuterCardinalities.getMaxCardinality().isUnknown() ? Long.MAX_VALUE : bOuterCardinalities.getMaxCardinality().getCardinality();
                    return Long.compare(aEffectiveMaxCardinality, bEffectiveMaxCardinality);
                }
            }
            return 0;
        }
    }

    static class PlanHashTiebreaker implements Tiebreaker<RecordQueryPlan> {
        private static final PlanHashTiebreaker INSTANCE = new PlanHashTiebreaker();

        @Override
        public int compare(@Nonnull final RecordQueryPlannerConfiguration configuration,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapA,
                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> opsMapB,
                           @Nonnull final RecordQueryPlan a, @Nonnull final RecordQueryPlan b) {
            //
            // If expressions are indistinguishable from a cost perspective, select one by its semanticHash.
            //
            final int aPlanHash = a.planHash(PlanHashable.CURRENT_FOR_CONTINUATION);
            final int bPlanHash = b.planHash(PlanHashable.CURRENT_FOR_CONTINUATION);
            return Integer.compare(aPlanHash, bPlanHash);
        }
    }

}
