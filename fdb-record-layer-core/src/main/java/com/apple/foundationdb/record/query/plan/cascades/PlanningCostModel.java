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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.indexes.VectorIndexEngine;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.QueryPlanner.IndexScanPreference;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.VectorIndexEnginePreference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.properties.CardinalitiesProperty.Cardinalities;
import com.apple.foundationdb.record.query.plan.cascades.properties.CardinalitiesProperty.Cardinality;
import com.apple.foundationdb.record.query.plan.cascades.properties.ExpressionDepthProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.NormalizedResidualPredicateProperty;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryDefaultOnEmptyPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFlatMapPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryMapPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithMatchCandidate;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryRecursiveDfsJoinPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryRecursiveLevelUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPredicatesFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Supplier;

import static com.apple.foundationdb.record.Bindings.Internal.CORRELATION;
import static com.apple.foundationdb.record.query.plan.cascades.properties.CardinalitiesProperty.cardinalities;
import static com.apple.foundationdb.record.query.plan.cascades.properties.ComparisonsProperty.comparisons;
import static com.apple.foundationdb.record.query.plan.cascades.properties.ExpressionDepthProperty.fetchDepth;
import static com.apple.foundationdb.record.query.plan.cascades.properties.ExpressionDepthProperty.typeFilterDepth;
import static com.apple.foundationdb.record.query.plan.cascades.properties.TypeFilterCountProperty.typeFilterCount;
import static com.apple.foundationdb.record.query.plan.cascades.properties.UnmatchedFieldsCountProperty.unmatchedFieldsCount;

/**
 * A comparator implementing the current heuristic cost model for the {@link CascadesPlanner} during the
 * {@link PlannerPhase#PLANNING} phase.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
@SpotBugsSuppressWarnings("SE_COMPARATOR_SHOULD_BE_SERIALIZABLE")
public class PlanningCostModel implements CascadesCostModel {
    @Nonnull
    private static final ImmutableSet<Class<? extends RelationalExpression>> interestingPlanClasses =
            ImmutableSet.of(
                    RecordQueryCoveringIndexPlan.class,
                    RecordQueryDefaultOnEmptyPlan.class,
                    RecordQueryFetchFromPartialRecordPlan.class,
                    RecordQueryInJoinPlan.class,
                    RecordQueryMapPlan.class,
                    RecordQueryPlanWithIndex.class,
                    RecordQueryPlanWithMatchCandidate.class,
                    RecordQueryPredicatesFilterPlan.class,
                    RecordQueryScanPlan.class);

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

    @Override
    public int compare(@Nonnull final RelationalExpression a, @Nonnull final RelationalExpression b) {
        if (a instanceof RecordQueryPlan && !(b instanceof RecordQueryPlan)) {
            return -1;
        }
        if (!(a instanceof RecordQueryPlan) && b instanceof RecordQueryPlan) {
            return 1;
        }

        final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> planOpsMapA =
                FindExpressionVisitor.evaluate(interestingPlanClasses, a);

        final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> planOpsMapB =
                FindExpressionVisitor.evaluate(interestingPlanClasses, b);

        //
        // If a vector index engine is preferred, that preference outranks every cost criterion below. It is a deliberate
        // choice by whoever set it, not an estimate the planner is entitled to overrule -- the motivating case is
        // steering queries away from an engine that is misbehaving, where being outvoted by a cardinality estimate is
        // precisely the failure being guarded against. See compareVectorIndexEnginePreference() for the one thing it
        // still refuses to decide.
        //
        int vectorIndexEnginePreferenceCompare = compareVectorIndexEnginePreference(planOpsMapA, planOpsMapB);
        if (vectorIndexEnginePreferenceCompare != 0) {
            return vectorIndexEnginePreferenceCompare;
        }

        final Cardinalities cardinalitiesA = cardinalities().evaluate(a);
        final Cardinalities cardinalitiesB = cardinalities().evaluate(b);

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
        // rCTE tie-breaker, if both plans are rCTE plans; one is DFS and the other is Level-based, always prefer DFS.
        final OptionalInt dfsVsLevelOptional =
                flipFlop(() -> compareRecursiveCteOperator(a, b), () -> compareRecursiveCteOperator(b, a));
        if (dfsVsLevelOptional.isPresent() && dfsVsLevelOptional.getAsInt() != 0) {
            return dfsVsLevelOptional.getAsInt();
        }

        // special case
        // if one plan is a inUnion plan
        final OptionalInt inPlanVsOtherOptional =
                flipFlop(() -> compareInOperator(a, b), () -> compareInOperator(b, a));
        if (inPlanVsOtherOptional.isPresent() && inPlanVsOtherOptional.getAsInt() != 0) {
            return inPlanVsOtherOptional.getAsInt();
        }

        final int typeFilterCountA = typeFilterCount().evaluate(a);
        final int typeFilterCountB = typeFilterCount().evaluate(b);

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

        // prefer the one with a deeper type filter
        int typeFilterPositionCompare = Integer.compare(typeFilterDepth().evaluate(b), typeFilterDepth().evaluate(a));
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

            final int fetchDepthB = fetchDepth().evaluate(b);
            final int fetchDepthA = fetchDepth().evaluate(a);
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

        int distinctFilterPositionCompare = Integer.compare(ExpressionDepthProperty.distinctDepth().evaluate(b),
                ExpressionDepthProperty.distinctDepth().evaluate(a));
        if (distinctFilterPositionCompare != 0) {
            return distinctFilterPositionCompare;
        }

        int ufpA = unmatchedFieldsCount().evaluate(a);
        int ufpB = unmatchedFieldsCount().evaluate(b);
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
        //  If a plan has fewer “simple” operations, it is preferable.
        //
        final int numSimpleOpsA = countSimpleOps(planOpsMapA);
        final int numSimpleOpsB = countSimpleOps(planOpsMapB);
        int numSimpleOpsCompare = Integer.compare(numSimpleOpsA, numSimpleOpsB);
        if (numSimpleOpsCompare != 0) {
            return numSimpleOpsCompare;
        }

        //
        // Both plans are nested loop joins. Attempt to pick a plan with a more preferable join ordering
        //
        if (a instanceof RecordQueryFlatMapPlan && b instanceof RecordQueryFlatMapPlan) {
            final List<RecordQueryPlan> aChildren = ((RecordQueryFlatMapPlan)a).getChildren();
            Verify.verify(aChildren.size() == 2);
            final RecordQueryPlan aOuter = aChildren.get(0);

            final List<RecordQueryPlan> bChildren = ((RecordQueryFlatMapPlan)b).getChildren();
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
                int compareOuterMaxCardinality = Long.compare(aEffectiveMaxCardinality, bEffectiveMaxCardinality);
                if (compareOuterMaxCardinality != 0) {
                    return compareOuterMaxCardinality;
                }
            }
        }
        
        //
        // If a plan has fewer ON EMPTY NULL operations, it is preferable.
        //
        final int numDefaultOnEmptyA = count(planOpsMapA, RecordQueryDefaultOnEmptyPlan.class);
        final int numDefaultOnEmptyB = count(planOpsMapB, RecordQueryDefaultOnEmptyPlan.class);
        int numDefaultOnEmptyCompare = Integer.compare(numDefaultOnEmptyA, numDefaultOnEmptyB);
        if (numDefaultOnEmptyCompare != 0) {
            return numDefaultOnEmptyCompare;
        }

        //
        // If plans are indistinguishable from a cost perspective, select one by planHash. This makes the cost model
        // stable (select the same plan on subsequent plannings).
        //
        if ((a instanceof PlanHashable) && (b instanceof PlanHashable)) {
            int hA = ((PlanHashable)a).planHash(PlanHashable.CURRENT_FOR_CONTINUATION);
            int hB = ((PlanHashable)b).planHash(PlanHashable.CURRENT_FOR_CONTINUATION);
            return Integer.compare(hA, hB);
        }

        return 0;
    }

    /**
     * Compares two plans by which vector index engine backs the vector indexes they scan, favoring the
     * {@link VectorIndexEnginePreference preferred} engine; fewer accesses over an index of the other engine is better.
     * Returns {@code 0} when no engine is preferred, which is the default, so that this criterion does not participate
     * in the comparison at all unless it was asked for.
     * <p>
     * This criterion is evaluated <em>before</em> every cost criterion, and deliberately so. It is not an estimate of
     * what is cheaper, it is a stated choice by whoever configured it, and the motivating case — moving queries off an
     * engine that is misbehaving — is one where losing to a cardinality or data-access estimate would defeat the point.
     * A consequence worth being aware of: a plan on the preferred engine wins even if the alternative on the other engine
     * looks considerably cheaper, including when it makes more vector accesses than the alternative does.
     * <p>
     * The one thing the preference does not decide is whether to make a vector access at all. It is about which engine
     * backs a vector access, so a plan making no vector access is not comparable on this criterion and the comparison is
     * abandoned, leaving it to the cost criteria below. Without that, preferring an engine could push a query off ANN
     * entirely — turning a which-engine knob into a whether-to-use-ANN knob, and (since it now outranks cost) doing so in
     * favor of an arbitrarily expensive plan. Making an engine's indexes unavailable to the planner altogether is a
     * different job, for the allowed-index mechanism rather than for a cost model preference.
     * <p>
     * Counting the non-preferred accesses rather than the preferred ones keeps the criterion from also rewarding a plan
     * for merely making more (approximate) vector accesses than the alternative.
     *
     * @param planOpsMapA the interesting operators of the first plan
     * @param planOpsMapB the interesting operators of the second plan
     * @return a negative integer, zero, or a positive integer as the first plan scans fewer, as many, or more
     *         non-preferred vector indexes than the second; {@code 0} if the two are not comparable on this criterion
     */
    @VisibleForTesting
    int compareVectorIndexEnginePreference(@Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> planOpsMapA,
                                           @Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> planOpsMapB) {
        final VectorIndexEngine.Kind preferredKind;
        switch (configuration.getVectorIndexEnginePreference()) {
            case PREFER_HNSW:
                preferredKind = VectorIndexEngine.Kind.HNSW;
                break;
            case PREFER_GUARDIANN:
                preferredKind = VectorIndexEngine.Kind.GUARDIANN;
                break;
            case NO_PREFERENCE:
            default:
                return 0;
        }

        final List<VectorIndexEngine.Kind> vectorIndexEngineKindsA = vectorIndexEngineKinds(planOpsMapA);
        final List<VectorIndexEngine.Kind> vectorIndexEngineKindsB = vectorIndexEngineKinds(planOpsMapB);
        if (vectorIndexEngineKindsA.isEmpty() || vectorIndexEngineKindsB.isEmpty()) {
            // One of them makes no vector access at all; the engine preference has no opinion on that comparison.
            return 0;
        }
        return Integer.compare(numNotOfKind(vectorIndexEngineKindsA, preferredKind),
                numNotOfKind(vectorIndexEngineKindsB, preferredKind));
    }

    /**
     * The engines backing the vector indexes a plan scans, one element per vector index access. Accesses that are not
     * over a vector index do not contribute.
     *
     * @param planOpsMap the interesting operators of a plan
     * @return the engine kinds, in no particular order
     */
    @Nonnull
    private static List<VectorIndexEngine.Kind> vectorIndexEngineKinds(@Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> planOpsMap) {
        return FindExpressionVisitor.slice(planOpsMap, RecordQueryPlanWithMatchCandidate.class)
                .stream()
                .map(PlanningCostModel::vectorIndexEngineKindMaybe)
                .flatMap(Optional::stream)
                .collect(ImmutableList.toImmutableList());
    }

    private static int numNotOfKind(@Nonnull final List<VectorIndexEngine.Kind> vectorIndexEngineKinds,
                                    @Nonnull final VectorIndexEngine.Kind kind) {
        return (int)vectorIndexEngineKinds.stream()
                .filter(vectorIndexEngineKind -> vectorIndexEngineKind != kind)
                .count();
    }

    /**
     * The vector engine backing the index an expression scans, if the expression is an index scan over a vector index
     * at all. The engine is taken from the {@link VectorIndexScanMatchCandidate} the plan was created from, which is the
     * only handle on the index the plan retains — a plan itself only carries the index <em>name</em>.
     *
     * @param expression the expression to inspect
     * @return the engine kind, or {@code Optional.empty()} if this is not a scan over a vector index
     */
    @Nonnull
    private static Optional<VectorIndexEngine.Kind> vectorIndexEngineKindMaybe(@Nonnull final RelationalExpression expression) {
        if (!(expression instanceof RecordQueryPlanWithMatchCandidate)) {
            return Optional.empty();
        }
        return ((RecordQueryPlanWithMatchCandidate)expression).getMatchCandidateMaybe()
                .filter(VectorIndexScanMatchCandidate.class::isInstance)
                .map(matchCandidate -> ((VectorIndexScanMatchCandidate)matchCandidate).getIndexEngineKind());
    }

    @Nonnull
    private Cardinality maxOfMaxCardinalitiesOfAllDataAccesses(@Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> planOpsMap) {
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

    /**
     * Compares two recursive CTE plans, preferring DFS traversal over level-based traversal. The left expression
     * is assumed to be a DFS plan and the right expression is assumed to be a level-based plan. Returns
     * {@link OptionalInt#empty()} if the assumption does not hold. This method is meant to be called using
     * {@link #flipFlop(Supplier, Supplier)} so the reversed case is also checked.
     *
     * @param leftExpression this expression (expected to be a DFS plan)
     * @param rightExpression other expression (expected to be a level-based plan)
     * @return {@code OptionalInt.of(-1)} to prefer the DFS plan (left), or {@code OptionalInt.empty()} if the
     *         expressions are not a DFS vs level-based pair
     */
    @SuppressWarnings("java:S1172")
    private static OptionalInt compareRecursiveCteOperator(@Nonnull final RelationalExpression leftExpression,
                                                           @Nonnull final RelationalExpression rightExpression) {
        if (leftExpression instanceof RecordQueryRecursiveDfsJoinPlan &&
                rightExpression instanceof RecordQueryRecursiveLevelUnionPlan) {
            return OptionalInt.of(-1);
        }
        return OptionalInt.empty();
    }

    /** First evaluates {@code variantA} which compares
     * {@code (a, b)} in some specific way. If that yields a result, it is returned directly. Otherwise, evaluates
     * {@code variantB} which compares {@code (b, a)} in the same way; if that yields a result, its sign is negated
     * before returning (since the argument order was swapped). Returns {@link OptionalInt#empty()} if neither
     * variant produces a result.
     *
     * @param variantA supplier for the {@code (a, b)} comparison, returning a positive value if {@code a} is preferred
     * @param variantB supplier for the {@code (b, a)} comparison, returning a positive value if {@code b} is preferred
     * @return the comparison result with consistent sign convention, or empty if neither variant matched
     */
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

    /**
     * Counts the number of “simple” per-tuple operations in {@code planOpsMap}. Operations considered simple are the
     * per-tuple operations {@code MAP} and {@code FILTER}.
     */
    private static int countSimpleOps(@Nonnull final Map<Class<? extends RelationalExpression>, Set<RelationalExpression>> planOpsMap) {
        return count(planOpsMap,
                RecordQueryMapPlan.class,
                RecordQueryPredicatesFilterPlan.class);
    }
}
