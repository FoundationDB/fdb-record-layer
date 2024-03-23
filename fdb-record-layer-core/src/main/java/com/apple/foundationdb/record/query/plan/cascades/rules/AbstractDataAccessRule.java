/*
 * AbstractDataAccessRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.combinatorics.ChooseK;
import com.apple.foundationdb.record.query.combinatorics.PartiallyOrderedSet;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRule;
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.Compensation;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.cascades.MatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.MatchInfo;
import com.apple.foundationdb.record.query.plan.cascades.MatchPartition;
import com.apple.foundationdb.record.query.plan.cascades.MatchedOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.Memoizer;
import com.apple.foundationdb.record.query.plan.cascades.Ordering;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.PlanContext;
import com.apple.foundationdb.record.query.plan.cascades.PrimaryScanMatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.ReferencedFieldsConstraint;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrderingConstraint;
import com.apple.foundationdb.record.query.plan.cascades.ValueIndexScanMatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.WithPrimaryKeyMatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalDistinctExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalIntersectionExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.PrimaryScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

/**
 * A rule that utilizes index matching information compiled by {@link CascadesPlanner} to create one or more
 * expressions for data access. While this rule delegates specifics to the {@link MatchCandidate}s, the following are
 * possible outcomes of the application of this transformation rule. Based on the match info, we may create for a single match:
 *
 * <ul>
 *     <li>a {@link PrimaryScanExpression} for a single {@link PrimaryScanMatchCandidate},</li>
 *     <li>an index scan/index scan + fetch for a single {@link ValueIndexScanMatchCandidate}</li>
 *     <li>an intersection ({@link LogicalIntersectionExpression}) of data accesses </li>
 * </ul>
 *
 * The logic that this rules delegates to and actually creates the expressions can be found in
 * {@link MatchCandidate#toEquivalentPlan(PartialMatch, PlanContext, Memoizer, boolean)}.
 * @param <R> subtype of {@link RelationalExpression}
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings({"java:S3776", "java:S4738"})
public abstract class AbstractDataAccessRule<R extends RelationalExpression> extends CascadesRule<MatchPartition> {
    private final BindingMatcher<PartialMatch> completeMatchMatcher;
    private final BindingMatcher<R> expressionMatcher;

    protected AbstractDataAccessRule(@Nonnull final BindingMatcher<MatchPartition> rootMatcher,
                                     @Nonnull final BindingMatcher<PartialMatch> completeMatchMatcher,
                                     @Nonnull final BindingMatcher<R> expressionMatcher) {
        super(rootMatcher, ImmutableSet.of(ReferencedFieldsConstraint.REFERENCED_FIELDS, RequestedOrderingConstraint.REQUESTED_ORDERING));
        this.completeMatchMatcher = completeMatchMatcher;
        this.expressionMatcher = expressionMatcher;
    }

    @Nonnull
    protected BindingMatcher<PartialMatch> getCompleteMatchMatcher() {
        return completeMatchMatcher;
    }

    @Nonnull
    protected BindingMatcher<R> getExpressionMatcher() {
        return expressionMatcher;
    }

    /**
     * Method that does the leg work to create the appropriate expression dag for data access using value indexes or
     * value index-like scans (primary scans).
     * <br>
     * Conceptually we do the following work:
     *
     * <ul>
     * <li> This method yields a scan plan for each matching primary candidate ({@link PrimaryScanMatchCandidate}).
     *      There is only ever going to be exactly one {@link PrimaryScanMatchCandidate} for a primary key. Due to the
     *      candidate being solely based upon a primary key, the match structure is somewhat limited. In essence, there
     *      is an implicit guarantee that we can always create a primary scan for a data source.
     * </li>
     * <li> This method yields an index scan plan for each matching value index candidate
     *      ({@link ValueIndexScanMatchCandidate}).
     * </li>
     * <li> This method yields the combinatorial expansion of intersections of distinct-ed index scan plans.
     * </li>
     * </ul>
     *
     * The work described above is semantically correct in a sense that it creates a search space that can be explored
     * and pruned in suitable ways that will eventually converge into an optimal data access plan.
     * <br>
     * We can choose to create an index scan for every index that is available regardless what the coverage
     * of an index is. The coverage of an index is a measurement that tells us how well an index can answer what a
     * filter (or by extension a query) asks for. For instance, a high number of search arguments used in the index scan
     * can be associated with high coverage (as in the index scan covers more of the query) and vice versa.
     * <br>
     * Similarly, we can choose to create the intersection of all possible combinations of suitable scans over indexes
     * (that we have matches for). Since we create a logical intersection of these access plans we can leave it up to
     * the respective implementation rules (e.g., {@link ImplementIntersectionRule}) to do the right thing and implement
     * the physical plan for the intersection if possible (e.g. ensuring compatibly ordered legs, etc.).
     * <br>
     * In fact, the two before-mentioned approaches are completely valid with respect to correctness of the plan and
     * the guaranteed creation of the optimal plan. However, in reality using this approach, although valid and probably
     * the conceptually better and more orthogonal approach, will result in a ballooning of the search space very quickly.
     * While that may be acceptable for group-by engines and only few index access paths, in an OLTP world where there
     * are potentially dozens of indexes, memory footprint and the sheer number of tasks that would be created for
     * subsequent exploration and implementation of all these alternatives make the purist approach to planning these
     * indexes infeasible.
     * <br>
     * Thus, we would like to eliminate unnecessary exploration by avoiding variations we know can never be successful
     * either in creating a successful executable plan (e.g. logical expression may not ever be able to produce a
     * compatible ordering) or cannot ever create an optimal plan. In a nutshell, we try to utilize additional
     * information that is available in addition to the matching partition in order to make decisions about which
     * expression variation to create and which to avoid:
     *
     * <ul>
     * <li> For a matching primary scan candidate ({@link PrimaryScanMatchCandidate})
     *      we will not create a primary scan if the scan is incompatible with an order constraint that has been
     *      communicated downwards in the graph.
     * </li>
     * <li> For a matching index scan candidate ({@link ValueIndexScanMatchCandidate})
     *      we will not create an index scan if the scan is incompatible with an order constraint that has been
     *      communicated downwards in the graph.
     * </li>
     * <li> We will only create a scan if there is no other index scan with a greater coverage (think of coverage
     *      as the assumed amount of filtering or currently the number of bound predicates) for the search arguments
     *      which are bound by the query.
     *      For instance, an index scan {@code INDEX SCAN(i1, a = [5, 5], b = [10, 10])} is still planned along
     *      {@code INDEX SCAN(i2, x = ["hello", "hello"], y = ["world", "world"], z = [10, inf])} even though
     *      the latter utilizes three search arguments while the former one only uses two. However, an index scan
     *      {@code INDEX SCAN(i1, a = [5, 5], b = [10, 10])} is not created (and yielded) if there we also
     *      have a choice to plan {@code INDEX SCAN(i2, b = [10, 10], a = [5, 5], c = ["good", "morning"])} as that
     *      index {@code i2} has a higher coverage compared to {@code i1} <em>and</em> all bound arguments in the scan
     *      over {@code i2} are also bound in the scan over {@code i1}.
     * <li>
     *      We will only create intersections of scans if we can already establish that the logical intersection
     *      can be implemented by a {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan}.
     *      That requires that the legs of the intersection are compatibly ordered <em>and</em> that that ordering follows
     *      a potentially required ordering.
     * </li>
     * </ul>
     *
     * @param call the {@link CascadesRuleCall} that this invocation is a part of
     * @param requestedOrderings a set of requested orderings
     * @param matchPartition a match partition of compatibly-matching {@link PartialMatch}es
     * @return an expression reference that contains all compensated data access plans for the given match partition.
     *         Note that the reference can also include index-ANDed plans for match intersections and that other matches
     *         contained in the match partition passed in may not be planned at all.
     */
    protected Set<? extends RelationalExpression> dataAccessForMatchPartition(@Nonnull CascadesRuleCall call,
                                                                              @Nonnull Set<RequestedOrdering> requestedOrderings,
                                                                              @Nonnull Collection<? extends PartialMatch> matchPartition) {
        //
        // return if there are no complete matches
        //
        Verify.verify(!matchPartition.isEmpty());

        final var bestMaximumCoverageMatches = maximumCoverageMatches(matchPartition, requestedOrderings);
        if (bestMaximumCoverageMatches.isEmpty()) {
            return LinkedIdentitySet.of();
        }

        // create scans for all best matches
        final var bestMatchToPlanMap =
                createScansForMatches(call.getContext(), call, bestMaximumCoverageMatches);

        final var resultSet = new LinkedIdentitySet<RelationalExpression>();

        // create single scan accesses
        for (final var bestMatch : bestMaximumCoverageMatches) {
            applyCompensationForSingleDataAccess(call, bestMatch, bestMatchToPlanMap.get(bestMatch.getPartialMatch()))
                    .ifPresent(resultSet::add);
        }

        final var bestMatchToDistinctPlanMap =
                distinctMatchToScanMap(call, bestMatchToPlanMap);

        final var commonPrimaryKeyValuesOptional =
                WithPrimaryKeyMatchCandidate.commonPrimaryKeyValuesMaybe(
                        bestMaximumCoverageMatches.stream()
                                .map(PartialMatchWithCompensation::getPartialMatch)
                                .map(PartialMatch::getMatchCandidate)
                                .collect(ImmutableList.toImmutableList()));
        commonPrimaryKeyValuesOptional.ifPresent(commonPrimaryKeyValues -> {
            final var boundPartitions = Lists.<List<PartialMatchWithCompensation>>newArrayList();
            // create intersections for all n choose k partitions from k = 2 .. n
            IntStream.range(2, bestMaximumCoverageMatches.size() + 1)
                    .mapToObj(k -> ChooseK.chooseK(bestMaximumCoverageMatches, k))
                    .flatMap(iterable -> StreamSupport.stream(iterable.spliterator(), false))
                    .forEach(boundPartitions::add);

            boundPartitions
                    .stream()
                    .flatMap(partition ->
                            createIntersectionAndCompensation(
                                    call,
                                    commonPrimaryKeyValues,
                                    bestMatchToDistinctPlanMap,
                                    partition,
                                    requestedOrderings).stream())
                    .forEach(resultSet::add);
        });
        return resultSet;
    }

    /**
     * Private helper method to eliminate {@link PartialMatch}es whose coverage is entirely contained in other matches
     * (among the matches given).
     * @param matches candidate matches
     * @param requestedOrderings a set of interesting orderings
     * @return a collection of {@link PartialMatch}es that are the maximum coverage matches among the matches handed in
     */
    @Nonnull
    @SuppressWarnings({"java:S1905", "java:S135"})
    private static List<PartialMatchWithCompensation> maximumCoverageMatches(@Nonnull final Collection<? extends PartialMatch> matches,
                                                                             @Nonnull final Set<RequestedOrdering> requestedOrderings) {
        final List<PartialMatchWithCompensation> partialMatchesWithCompensation = new ArrayList<>();
        for (final var partialMatch: matches) {
            final var ordering = satisfiedOrderings(partialMatch, requestedOrderings);
            if (ordering.isEmpty()) {
                continue;
            }
            partialMatchesWithCompensation.add(new PartialMatchWithCompensation(
                    partialMatch, partialMatch.compensate(), ordering.get()));
        }
        partialMatchesWithCompensation.sort(
                Comparator.comparing((Function<PartialMatchWithCompensation, Integer>)
                        p -> p.getPartialMatch().getBindingPredicates().size()).reversed());

        final var maximumCoverageMatchesBuilder = ImmutableList.<PartialMatchWithCompensation>builder();
        for (var i = 0; i < partialMatchesWithCompensation.size(); i++) {
            final var outerPartialMatchWithCompensation =
                    partialMatchesWithCompensation.get(i);
            final var outerMatch = outerPartialMatchWithCompensation.getPartialMatch();
            final var outerBindingPredicates = outerMatch.getBindingPredicates();

            var foundContainingInner = false;
            for (var j = 0; j < partialMatchesWithCompensation.size(); j++) {
                final var innerPartialMatchWithCompensation =
                        partialMatchesWithCompensation.get(j);
                final var innerBindingPredicates = innerPartialMatchWithCompensation.getPartialMatch().getBindingPredicates();
                // check if outer is completely contained in inner
                if (outerBindingPredicates.size() >= innerBindingPredicates.size()) {
                    break;
                }

                if (i != j && innerBindingPredicates.containsAll(outerBindingPredicates)) {
                    foundContainingInner = true;
                    break;
                }
            }

            if (!foundContainingInner) {
                //
                // no other partial match completely contained this one
                //
                maximumCoverageMatchesBuilder.add(outerPartialMatchWithCompensation);
            }
        }

        return maximumCoverageMatchesBuilder.build();
    }

    /**
     * Private helper method to compute the subset of orderings passed in that would be satisfied by a scan
     * if the given {@link PartialMatch} were to be planned.
     * @param partialMatch a partial match
     * @param requestedOrderings a set of {@link Ordering}s
     * @return an optional boolean that is empty if no orderings was satisfied, true if there was a descending match, and false if there was an ascending match
     */
    @Nonnull
    @SuppressWarnings("java:S135")
    private static Optional<Boolean> satisfiedOrderings(@Nonnull final PartialMatch partialMatch, @Nonnull final Set<RequestedOrdering> requestedOrderings) {
        Set<RequestedOrdering> satisfiedOrderings = requestedOrderings
                .stream()
                .filter(requestedOrdering -> {
                    if (requestedOrdering.isPreserve()) {
                        return true;
                    }

                    final var matchInfo = partialMatch.getMatchInfo();
                    final var orderingParts = matchInfo.getMatchedOrderingParts();
                    final var equalityBoundKeys =
                            orderingParts
                                    .stream()
                                    .filter(orderingPart -> orderingPart.getComparisonRangeType() == ComparisonRange.Type.EQUALITY)
                                    .map(MatchedOrderingPart::getValue)
                                    .collect(ImmutableSet.toImmutableSet());

                    final var orderingPartIterator = orderingParts.iterator();
                    for (final var requestedOrderingPart : requestedOrdering.getOrderingParts()) {
                        final var requestedOrderingValue = requestedOrderingPart.getValue();

                        if (equalityBoundKeys.contains(requestedOrderingValue)) {
                            continue;
                        }

                        // if we are here, we must now find a non-equality-bound expression
                        var found = false;
                        while (orderingPartIterator.hasNext()) {
                            final var orderingPart = orderingPartIterator.next();
                            if (orderingPart.getComparisonRangeType() == ComparisonRange.Type.EQUALITY) {
                                continue;
                            }

                            final var orderingValue = orderingPart.getValue();
                            if (requestedOrderingValue.equals(orderingValue)) {
                                found = true;
                                break;
                            } else {
                                return false;
                            }
                        }
                        if (!found) {
                            return false;
                        }
                    }
                    return true;
                })
                .collect(ImmutableSet.toImmutableSet());
        if (satisfiedOrderings.isEmpty()) {
            return Optional.empty();
        }
        for (final var ordering: satisfiedOrderings) {
            // Ignore orderings without parts (only containing preserve_distinctness for example)
            if (!ordering.getOrderingParts().isEmpty()) {
                // If one is reverse, they are all reverse
                return Optional.of(ordering.getOrderingParts().get(0).isReverse());
            }
        }
        return Optional.of(false);
    }

    /**
     * Private helper method to compute a map of matches to scans (no compensation applied yet).
     * @param planContext plan context
     * @param memoizer the memoizer for {@link Reference}s
     * @param matches a collection of matches
     * @return a map of the matches where a match is associated with a scan expression created based on that match
     */
    @Nonnull
    private static Map<PartialMatch, RecordQueryPlan> createScansForMatches(@Nonnull final PlanContext planContext,
                                                                            @Nonnull final Memoizer memoizer,
                                                                            @Nonnull final Collection<PartialMatchWithCompensation> matches) {
        return matches
                .stream()
                .collect(ImmutableMap.toImmutableMap(
                        PartialMatchWithCompensation::getPartialMatch,
                        partialMatchWithCompensation -> {
                            final var partialMatch = partialMatchWithCompensation.getPartialMatch();
                            return partialMatch.getMatchCandidate()
                                    .toEquivalentPlan(partialMatch, planContext, memoizer, partialMatchWithCompensation.isReverseScanOrder());
                        }));
    }

    /**
     * Private helper method to compute a new match to scan map by applying a {@link LogicalDistinctExpression} on each
     * scan.
     * @param memoizer the memoizer for {@link Reference}s
     * @param matchToExpressionMap a map of matches to {@link RelationalExpression}s
     * @return a map of the matches where a match is associated with a {@link RecordQueryUnorderedPrimaryKeyDistinctPlan}
     *         ranging over a {@link RecordQueryPlan} that was created based on that match
     */
    @Nonnull
    private static Map<PartialMatch, RecordQueryPlan> distinctMatchToScanMap(@Nonnull final Memoizer memoizer,
                                                                             @Nonnull final Map<PartialMatch, RecordQueryPlan> matchToExpressionMap) {
        return matchToExpressionMap
                .entrySet()
                .stream()
                .collect(
                        ImmutableMap.toImmutableMap(Map.Entry::getKey,
                                entry -> {
                                    final var partialMatch = entry.getKey();
                                    final var matchCandidate = partialMatch.getMatchCandidate();
                                    final var dataAccessPlan = entry.getValue();
                                    if (matchCandidate.createsDuplicates()) {
                                        return new RecordQueryUnorderedPrimaryKeyDistinctPlan(
                                                Quantifier.physical(memoizer.memoizePlans(dataAccessPlan)));
                                    }
                                    return dataAccessPlan;
                                }));
    }

    /**
     * Private helper method to apply compensation for an already existing data access (a scan over materialized data).
     * Planning the data access and its compensation for a given match is a two-step approach as we compute
     * the compensation for intersections by intersecting the {@link Compensation} for the single data accesses first
     * before using the resulting {@link Compensation} to compute the compensating expression for the entire
     * intersection. For single data scans that will not be used in an intersection we still follow the same
     * two-step approach of separately planning the scan and then computing the compensation and the compensating
     * expression.
     * @param memoizer the memoizer of this call
     * @param partialMatchWithCompensation the match the caller wants to apply compensation for
     * @param plan the plan the caller would like to create compensation for.
     * @return a new {@link RelationalExpression} that represents the data access and its compensation
     */
    @Nonnull
    private static Optional<RelationalExpression> applyCompensationForSingleDataAccess(@Nonnull final Memoizer memoizer,
                                                                                       @Nonnull final PartialMatchWithCompensation partialMatchWithCompensation,
                                                                                       @Nonnull final RecordQueryPlan plan) {
        final var compensation = partialMatchWithCompensation.getCompensation();
        return compensation.isImpossible()
               ? Optional.empty()
               : Optional.of(compensation.isNeeded()
                             ? compensation.apply(memoizer, plan)
                             : plan);
    }
    
    /**
     * Private helper method to plan an intersection and subsequently compensate it using the partial match structures
     * kept for all participating data accesses.
     * Planning the data access and its compensation for a given match is a two-step approach as we compute
     * the compensation for intersections by intersecting the {@link Compensation} for the single data accesses first
     * before using the resulting {@link Compensation} to compute the compensating expression for the entire
     * intersection.
     * @param memoizer the memoizer
     * @param commonPrimaryKeyValues normalized common primary key
     * @param matchToPlanMap a map from match to single data access expression
     * @param partition a partition (i.e. a list of {@link PartialMatch}es that the caller would like to compute
     *        and intersected data access for
     * @param requestedOrderings a set of ordering that have been requested by consuming expressions/plan operators
     * @return an optional containing a new {@link RelationalExpression} that represents the data access and its
     *         compensation, {@code Optional.empty()} if this method was unable to compute the intersection expression
     */
    @Nonnull
    private static List<RelationalExpression> createIntersectionAndCompensation(@Nonnull final Memoizer memoizer,
                                                                                @Nonnull final List<Value> commonPrimaryKeyValues,
                                                                                @Nonnull final Map<PartialMatch, RecordQueryPlan> matchToPlanMap,
                                                                                @Nonnull final List<PartialMatchWithCompensation> partition,
                                                                                @Nonnull final Set<RequestedOrdering> requestedOrderings) {
        final var expressionsBuilder = ImmutableList.<RelationalExpression>builder();

        final var partitionOrderings = partition
                .stream()
                .map(PartialMatchWithCompensation::getPartialMatch)
                .map(PartialMatch::getMatchInfo)
                .map(MatchInfo::getMatchedOrderingParts)
                .collect(ImmutableList.toImmutableList());

        final var equalityBoundKeyValues =
                partitionOrderings
                        .stream()
                        .flatMap(orderingParts ->
                                orderingParts.stream()
                                        .filter(boundOrderingKey -> boundOrderingKey.getComparisonRangeType() == ComparisonRange.Type.EQUALITY)
                                        .map(MatchedOrderingPart::getValue))
                        .collect(ImmutableSet.toImmutableSet());

        final var orderingPartialOrder = intersectionOrderingPartialOrder(partitionOrderings);

        for (final var requestedOrdering : requestedOrderings) {
            final var comparisonKeyValuesIterable =
                    Ordering.enumerateSatisfyingOrderingComparisonKeyValues(orderingPartialOrder,
                            equalityBoundKeyValues,
                            requestedOrdering.getOrderingParts());
            for (final var comparisonKeyValues : comparisonKeyValuesIterable) {
                if (!isCompatibleComparisonKey(comparisonKeyValues,
                        commonPrimaryKeyValues,
                        equalityBoundKeyValues)) {
                    continue;
                }

                final var compensation =
                        partition
                                .stream()
                                .map(PartialMatchWithCompensation::getCompensation)
                                .reduce(Compensation.impossibleCompensation(), Compensation::intersect);

                if (!compensation.isImpossible()) {
                    final var newQuantifiers =
                            partition
                                    .stream()
                                    .map(partialMatch -> Objects.requireNonNull(matchToPlanMap.get(partialMatch.getPartialMatch())))
                                    .map(memoizer::memoizePlans)
                                    .map(Quantifier::physical)
                                    .collect(ImmutableList.toImmutableList());

                    final var intersectionPlan = RecordQueryIntersectionPlan.fromQuantifiers(newQuantifiers, ImmutableList.copyOf(comparisonKeyValues));
                    final var compensatedIntersection =
                            compensation.isNeeded()
                            ? compensation.apply(memoizer, intersectionPlan)
                            : intersectionPlan;
                    expressionsBuilder.add(compensatedIntersection);
                }
            }
        }

        return expressionsBuilder.build();
    }

    /**
     * Private helper method that computes the ordering of the intersection using matches and the common primary key
     * of the data source.
     * @param partitionOrderings partition we would like to intersect
     * @return a {@link PartiallyOrderedSet}  of {@link OrderingPart}s representing a common intersection ordering.
     */
    @SuppressWarnings("java:S1066")
    @Nonnull
    private static PartiallyOrderedSet<OrderingPart> intersectionOrderingPartialOrder(@Nonnull final List<List<MatchedOrderingPart>> partitionOrderings) {

        final var orderingPartialOrders =
                partitionOrderings.stream()
                        .map(matchedOrderingParts -> {
                            final var orderingPartsPartitions =
                                    matchedOrderingParts.stream()
                                            .collect(Collectors.partitioningBy(orderingPart -> orderingPart.getComparisonRangeType() == ComparisonRange.Type.EQUALITY));

                            final var equalityBoundOrderingParts = orderingPartsPartitions.get(true).stream()
                                    .map(MatchedOrderingPart::getOrderingPart)
                                    .collect(ImmutableSet.toImmutableSet());
                            final var orderingParts =
                                    orderingPartsPartitions.get(false)
                                            .stream()
                                            .map(MatchedOrderingPart::getOrderingPart)
                                            .collect(ImmutableList.toImmutableList());

                            return PartiallyOrderedSet.<OrderingPart>builder()
                                    .addListWithDependencies(orderingParts)
                                    .addAll(equalityBoundOrderingParts)
                                    .build();
                        })
                        .collect(ImmutableList.toImmutableList());

        return Ordering.mergePartialOrderOfOrderings(orderingPartialOrders);
    }

    /**
     * Private helper method to verify that a list of {@link Value}s can be used as a comparison key.
     * ordering information coming from a match in form of a list of {@link Value}s.
     * @param comparisonKeyValues a list of {@link Value}s
     * @param commonPrimaryKeyValues common primary key
     * @param equalityBoundKeyValues  a set of equality-bound key parts
     * @return a boolean that indicates if the list of values passed in can be used as comparison key
     */
    private static boolean isCompatibleComparisonKey(@Nonnull Set<Value> comparisonKeyValues,
                                                     @Nonnull List<Value> commonPrimaryKeyValues,
                                                     @Nonnull ImmutableSet<Value> equalityBoundKeyValues) {
        if (comparisonKeyValues.isEmpty()) {
            // everything is in one row
            return true;
        }

        return commonPrimaryKeyValues
                .stream()
                .filter(commonPrimaryKeyValue -> !equalityBoundKeyValues.contains(commonPrimaryKeyValue))
                .allMatch(comparisonKeyValues::contains);
    }

    private static class PartialMatchWithCompensation {
        @Nonnull
        private final PartialMatch partialMatch;
        @Nonnull
        private final Compensation compensation;
        private final boolean reverseScanOrder;

        public PartialMatchWithCompensation(@Nonnull final PartialMatch partialMatch,
                                            @Nonnull final Compensation compensation,
                                            final boolean reverseScanOrder) {
            this.partialMatch = partialMatch;
            this.compensation = compensation;
            this.reverseScanOrder = reverseScanOrder;
        }

        @Nonnull
        public PartialMatch getPartialMatch() {
            return partialMatch;
        }

        @Nonnull
        public Compensation getCompensation() {
            return compensation;
        }

        public boolean isReverseScanOrder() {
            return reverseScanOrder;
        }
    }
}
