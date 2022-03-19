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

package com.apple.foundationdb.record.query.plan.temp.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.combinatorics.ChooseK;
import com.apple.foundationdb.record.query.combinatorics.PartialOrder;
import com.apple.foundationdb.record.query.plan.temp.BoundKeyPart;
import com.apple.foundationdb.record.query.plan.temp.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.temp.ComparisonRange;
import com.apple.foundationdb.record.query.plan.temp.Compensation;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.temp.MatchCandidate;
import com.apple.foundationdb.record.query.plan.temp.MatchInfo;
import com.apple.foundationdb.record.query.plan.temp.MatchPartition;
import com.apple.foundationdb.record.query.plan.temp.Ordering;
import com.apple.foundationdb.record.query.plan.temp.OrderingAttribute;
import com.apple.foundationdb.record.query.plan.temp.PartialMatch;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.PrimaryScanMatchCandidate;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.ReferencedFieldsAttribute;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.temp.ValueIndexScanMatchCandidate;
import com.apple.foundationdb.record.query.plan.temp.expressions.IndexScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalDistinctExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalIntersectionExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.PrimaryScanExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.BindingMatcher;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.predicates.ValueComparisonRangePredicate.Placeholder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

/**
 * A rule that utilizes index matching information compiled by {@link CascadesPlanner} to create one or more
 * expressions for data access. While this rule delegates specifics to the {@link MatchCandidate}s, the following are
 * possible outcomes of the application of this transformation rule. Based on the match info, we may create for a single match:
 *
 * <ul>
 *     <li>a {@link PrimaryScanExpression} for a single {@link PrimaryScanMatchCandidate},</li>
 *     <li>an {@link IndexScanExpression} for a single {@link ValueIndexScanMatchCandidate}</li>
 *     <li>an intersection ({@link LogicalIntersectionExpression}) of data accesses </li>
 * </ul>
 *
 * The logic that this rules delegates to to actually create the expressions can be found in
 * {@link MatchCandidate#toEquivalentExpression(PartialMatch)}.
 * @param <R> sub type of {@link RelationalExpression}
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings({"java:S3776", "java:S4738"})
public abstract class AbstractDataAccessRule<R extends RelationalExpression> extends PlannerRule<MatchPartition> {
    private final BindingMatcher<PartialMatch> completeMatchMatcher;
    private final BindingMatcher<R> expressionMatcher;

    protected AbstractDataAccessRule(@Nonnull final BindingMatcher<MatchPartition> rootMatcher,
                                     @Nonnull final BindingMatcher<PartialMatch> completeMatchMatcher,
                                     @Nonnull final BindingMatcher<R> expressionMatcher) {
        super(rootMatcher, ImmutableSet.of(ReferencedFieldsAttribute.REFERENCED_FIELDS, OrderingAttribute.ORDERING));
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
     *
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
     *
     * We can choose to create an index scan for every index that is available regardless what the coverage
     * of an index is. The coverage of an index is a measurement that tells us how well an index can answer what a
     * filter (or by extension a query) asks for. For instance, a high number of search arguments used in the index scan
     * can be associated with high coverage (as in the index scan covers more of the query) and vice versa.
     *
     * Similarly, we can choose to create the intersection of all possible combinations of suitable scans over indexes
     * (that we have matches for). Since we create a logical intersection of these access plans we can leave it up to
     * the respective implementation rules (e.g., {@link ImplementIntersectionRule}) to do the right thing and implement
     * the physical plan for the intersection if possible (e.g. ensuring compatibly ordered legs, etc.).
     *
     * In fact, the two before-mentioned approaches are completely valid with respect to correctness of the plan and
     * the guaranteed creation of the optimal plan. However, in reality using this approach, although valid and probably
     * the conceptually better and more orthogonal approach, will result in a ballooning of the search space very quickly.
     * While that may be acceptable for group-by engines and only few index access paths, in an OLTP world where there
     * are potentially dozens of indexes, memory footprint and the sheer number of tasks that would be created for
     * subsequent exploration and implementation of all these alternatives make the purist approach to planning these
     * indexes infeasible.
     *
     * Thus we would like to eliminate unnecessary exploration by avoiding variations we know can never be successful
     * either in creating a successful executable plan (e.g. logical expression may not ever be able to produce a
     * compatible ordering) or cannot ever create an optimal plan. In a nutshell, we try to utilize additional
     * information that is available in addition to the matching partition in order to make decisions about which
     * expression variation to create and which to avoid:
     *
     * <ul>
     * <li> For a matching primary scan candidate ({@link PrimaryScanMatchCandidate})
     *      we will not create a primary scan if the scan is incompatible with an interesting order that has been
     *      communicated downwards in the graph.
     * </li>
     * <li> For a matching index scan candidate ({@link ValueIndexScanMatchCandidate})
     *      we will not create an index scan if the scan is incompatible with an interesting order that has been
     *      communicated downwards in the graph.
     * </li>
     * <li> We will only create a scan if there is no other index scan with a greater coverage (think of coverage
     *      as the assumed amount of filtering or currently the number of bound predicates) for the search arguments
     *      which are bound by the query.
     *      For instance, an index scan {@code INDEX SCAN(i1, a = [5, 5], b = [10, 10])} is still planned along
     *      {@code INDEX SCAN(i2, x = ["hello", "hello"], y = ["world", "world"], z = [10, inf])} even though
     *      the latter utilizes three search arguments while the former one only uses two. However, an index scan
     *      {@code INDEX SCAN(i1, a = [5, 5], b = [10, 10])} is not created (and yielded) if there we also
     *      have a choice to plan {@code INDEX SCAN(i2, b = [10, 10], a = [5, 5], c = ["Guten", "Morgen"])} as that
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
     * @param call the call associated with this planner rule execution
     */
    @Override
    @SuppressWarnings("java:S135")
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final var planContext = call.getContext();
        final var bindings = call.getBindings();
        final var completeMatches = bindings.getAll(getCompleteMatchMatcher());
        final var expression = bindings.get(getExpressionMatcher());

        //
        // return if there are no complete matches
        //
        if (completeMatches.isEmpty()) {
            return;
        }

        //
        // return if there is no pre-determined interesting ordering
        //
        final var requestedOrderingsOptional = call.getInterestingProperty(OrderingAttribute.ORDERING);
        if (requestedOrderingsOptional.isEmpty()) {
            return;
        }

        final var requestedOrderings = requestedOrderingsOptional.get();

        final var bestMaximumCoverageMatches = maximumCoverageMatches(completeMatches, requestedOrderings);
        if (bestMaximumCoverageMatches.isEmpty()) {
            return;
        }

        // create scans for all best matches
        final var bestMatchToExpressionMap =
                createScansForMatches(planContext.getMetaData(), bestMaximumCoverageMatches);

        final var toBeInjectedReference = GroupExpressionRef.empty();

        // create single scan accesses
        for (final var bestMatch : bestMaximumCoverageMatches) {
            final var dataAccessAndCompensationExpression =
                    compensateSingleDataAccess(bestMatch, bestMatchToExpressionMap.get(bestMatch));
            toBeInjectedReference.insert(dataAccessAndCompensationExpression);
        }

        final Map<PartialMatch, RelationalExpression> bestMatchToDistinctExpressionMap =
                distinctMatchToScanMap(bestMatchToExpressionMap);

        @Nullable final var commonPrimaryKey = call.getContext().getCommonPrimaryKey();
        if (commonPrimaryKey != null) {
            final var commonPrimaryKeyParts = commonPrimaryKey.normalizeKeyForPositions();

            final var boundPartitions = Lists.<List<PartialMatch>>newArrayList();
            // create intersections for all n choose k partitions from k = 2 .. n
            IntStream.range(2, bestMaximumCoverageMatches.size() + 1)
                    .mapToObj(k -> ChooseK.chooseK(bestMaximumCoverageMatches, k))
                    .flatMap(iterable -> StreamSupport.stream(iterable.spliterator(), false))
                    .forEach(boundPartitions::add);

            boundPartitions
                    .stream()
                    .flatMap(partition ->
                            createIntersectionAndCompensation(
                                    commonPrimaryKeyParts,
                                    bestMatchToDistinctExpressionMap,
                                    partition,
                                    requestedOrderings).stream())
                    .forEach(toBeInjectedReference::insert);
        }
        call.yield(inject(expression, completeMatches, toBeInjectedReference));
    }

    @Nonnull
    protected abstract ExpressionRef<? extends RelationalExpression> inject(@Nonnull R expression,
                                                                            @Nonnull List<? extends PartialMatch> completeMatches,
                                                                            @Nonnull final ExpressionRef<? extends RelationalExpression> compensatedScanGraph);

    /**
     * Private helper method to eliminate {@link PartialMatch}es whose coverage is entirely contained in other matches
     * (among the matches given).
     * @param matches candidate matches
     * @param interestedOrderings a set of interesting orderings
     * @return a list of {@link PartialMatch}es that are the maximum coverage matches among the matches handed in
     */
    @Nonnull
    @SuppressWarnings({"java:S1905", "java:S135"})
    private static List<PartialMatch> maximumCoverageMatches(@Nonnull final List<? extends PartialMatch> matches, @Nonnull final Set<RequestedOrdering> interestedOrderings) {
        final var bindingQueryPredicatesForMatches =
                matches
                        .stream()
                        .filter(partialMatch -> !satisfiedOrderings(partialMatch, interestedOrderings).isEmpty())
                        .map(partialMatch -> Pair.of(partialMatch, computeBindingQueryPredicates(partialMatch)))
                        .sorted(Comparator.comparing((Function<Pair<? extends PartialMatch, Set<QueryPredicate>>, Integer>)p -> p.getValue().size()).reversed())
                        .collect(ImmutableList.toImmutableList());

        final var maximumCoverageMatchesBuilder = ImmutableList.<PartialMatch>builder();
        for (var i = 0; i < bindingQueryPredicatesForMatches.size(); i++) {
            final var outerMatch = bindingQueryPredicatesForMatches.get(i).getKey();
            final var outer = bindingQueryPredicatesForMatches.get(i).getValue();

            var foundContainingInner = false;
            for (var j = 0; j < bindingQueryPredicatesForMatches.size(); j++) {
                final var inner = bindingQueryPredicatesForMatches.get(j).getValue();
                // check if outer is completely contained in inner
                if (outer.size() >= inner.size()) {
                    break;
                }

                if (i != j && inner.containsAll(outer)) {
                    foundContainingInner = true;
                    break;
                }
            }

            if (!foundContainingInner) {
                //
                // no other partial match completely contained this one
                //
                maximumCoverageMatchesBuilder.add(outerMatch);
            }
        }

        return maximumCoverageMatchesBuilder.build();
    }

    /**
     * Private helper method to compute the subset of orderings passed in that would be satisfied by a scan
     * if the given {@link PartialMatch} were to be planned.
     * @param partialMatch a partial match
     * @param requestedOrderings a set of {@link Ordering}s
     * @return a subset of {@code requestedOrderings} where each contained {@link Ordering} would be satisfied by the
     *         given partial match
     */
    @Nonnull
    @SuppressWarnings("java:S135")
    private static Set<RequestedOrdering> satisfiedOrderings(@Nonnull final PartialMatch partialMatch, @Nonnull final Set<RequestedOrdering> requestedOrderings) {
        return requestedOrderings
                .stream()
                .filter(requestedOrdering -> {
                    if (requestedOrdering.isPreserve()) {
                        return true;
                    }

                    final var matchInfo = partialMatch.getMatchInfo();
                    final var orderingKeyParts = matchInfo.getOrderingKeyParts();
                    final var equalityBoundKeys =
                            orderingKeyParts
                                    .stream()
                                    .filter(boundKeyPart -> boundKeyPart.getComparisonRangeType() == ComparisonRange.Type.EQUALITY)
                                    .map(BoundKeyPart::getNormalizedKeyExpression)
                                    .collect(ImmutableSet.toImmutableSet());

                    final var boundKeyPartIterator = orderingKeyParts.iterator();
                    for (final var requestedOrderingKeyPart : requestedOrdering.getOrderingKeyParts()) {
                        final var requestedOrderingKey = requestedOrderingKeyPart.getNormalizedKeyExpression();

                        if (equalityBoundKeys.contains(requestedOrderingKey)) {
                            continue;
                        }

                        // if we are here, we must now find a non-equality-bound expression
                        var found = false;
                        while (boundKeyPartIterator.hasNext()) {
                            final var boundKeyPart = boundKeyPartIterator.next();
                            if (boundKeyPart.getComparisonRangeType() == ComparisonRange.Type.EQUALITY) {
                                continue;
                            }

                            final var boundKey = boundKeyPart.getNormalizedKeyExpression();
                            if (requestedOrderingKey.equals(boundKey)) {
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
    }

    /**
     * Private helper method to compute a map of matches to scans (no compensation applied yet).
     * @param recordMetaData the mata data used by the plann context
     * @param matches a collection of matches
     * @return a map of the matches where a match is associated with a scan expression created based on that match
     */
    @Nonnull
    private static Map<PartialMatch, RelationalExpression> createScansForMatches(@Nonnull RecordMetaData recordMetaData,
                                                                                 @Nonnull final Collection<PartialMatch> matches) {
        return matches
                .stream()
                .collect(ImmutableMap.toImmutableMap(
                        Function.identity(),
                        partialMatch -> partialMatch.getMatchCandidate()
                                .toEquivalentExpression(recordMetaData, partialMatch)));
    }

    /**
     * Private helper method to compute a new match to scan map by applying a {@link LogicalDistinctExpression} on each
     * scan.
     * @param matchToExpressionMap a map of matches to {@link RelationalExpression}s
     * @return a map of the matches where a match is associated with a {@link LogicalDistinctExpression} ranging over a
     *         scan expression that was created based on that match
     */
    @Nonnull
    private static Map<PartialMatch, RelationalExpression> distinctMatchToScanMap(@Nonnull Map<PartialMatch, RelationalExpression> matchToExpressionMap) {
        return matchToExpressionMap
                .entrySet()
                .stream()
                .collect(
                        ImmutableMap.toImmutableMap(Map.Entry::getKey,
                                entry -> {
                                    final RelationalExpression dataAccessExpression = entry.getValue();
                                    return new LogicalDistinctExpression(dataAccessExpression);
                                }));
    }

    /**
     * Private helper method to compensate an already existing data access (a scan over materialized data).
     * Planning the data access and its compensation for a given match is a two-step approach as we compute
     * the compensation for intersections by intersecting the {@link Compensation} for the single data accesses first
     * before using the resulting {@link Compensation} to compute the compensating expression for the entire
     * intersection. For single data scans that will not be used in an intersection we still follow the same
     * two-step approach of seprately planning the scan and then computing the compensation and the compensating
     * expression.
     * @param partialMatch the match the caller wants to compensate
     * @param scanExpression the scan expression the caller would like to create compensation for.
     * @return a new {@link RelationalExpression} that represents the data access and its compensation
     */
    @Nonnull
    private static RelationalExpression compensateSingleDataAccess(@Nonnull final PartialMatch partialMatch,
                                                                   @Nonnull final RelationalExpression scanExpression) {
        final var compensation = partialMatch.compensate(partialMatch.getBoundParameterPrefixMap());
        return compensation.isNeeded()
               ? compensation.apply(GroupExpressionRef.of(scanExpression))
               : scanExpression;
    }

    @Nonnull
    private static Set<QueryPredicate> computeBindingQueryPredicates(@Nonnull final PartialMatch partialMatch) {
        final var matchInfo = partialMatch.getMatchInfo();
        final var boundParameterPrefixMap = partialMatch.getBoundParameterPrefixMap();
        final var bindingQueryPredicates = Sets.<QueryPredicate>newIdentityHashSet();

        //
        // Go through all accumulated parameter bindings -- find the query predicates binding the parameters. Those
        // query predicates are binding the parameters by means of a placeholder on the candidate side (they themselves
        // should be of class Placeholder). Note that there could be more than one query predicate mapping to a candidate
        // predicate.
        //
        for (final var entry : matchInfo.getAccumulatedPredicateMap().entries()) {
            final var predicateMapping = entry.getValue();
            final var candidatePredicate = predicateMapping.getCandidatePredicate();
            if (!(candidatePredicate instanceof Placeholder)) {
                continue;
            }

            final var placeholder = (Placeholder)candidatePredicate;
            if (boundParameterPrefixMap.containsKey(placeholder.getAlias())) {
                bindingQueryPredicates.add(predicateMapping.getQueryPredicate());
            }
        }

        return bindingQueryPredicates;
    }

    /**
     * Private helper method to plan an intersection and subsequently compensate it using the partial match structures
     * kept for all participating data accesses.
     * Planning the data access and its compensation for a given match is a two-step approach as we compute
     * the compensation for intersections by intersecting the {@link Compensation} for the single data accesses first
     * before using the resulting {@link Compensation} to compute the compensating expression for the entire
     * intersection.
     * @param commonPrimaryKeyParts normalized common primary key
     * @param matchToExpressionMap a map from match to single data access expression
     * @param partition a partition (i.e. a list of {@link PartialMatch}es that the caller would like to compute
     *        and intersected data access for
     * @param requestedOrderings a set of ordering that have been requested by consuming expressions/plan operators
     * @return a optional containing a new {@link RelationalExpression} that represents the data access and its
     *         compensation, {@code Optional.empty()} if this method was unable to compute the intersection expression
     */
    @Nonnull
    private static List<RelationalExpression> createIntersectionAndCompensation(@Nonnull final List<KeyExpression> commonPrimaryKeyParts,
                                                                                @Nonnull final Map<PartialMatch, RelationalExpression> matchToExpressionMap,
                                                                                @Nonnull final List<PartialMatch> partition,
                                                                                @Nonnull final Set<RequestedOrdering> requestedOrderings) {
        final var expressionsBuilder = ImmutableList.<RelationalExpression>builder();

        final var orderingPartialOrder = intersectionOrdering(partition);

        final ImmutableSet<BoundKeyPart> equalityBoundKeyParts = partition
                .stream()
                .map(partialMatch -> partialMatch.getMatchInfo().getOrderingKeyParts())
                .flatMap(orderingKeyParts ->
                        orderingKeyParts.stream()
                                .filter(boundOrderingKey -> boundOrderingKey.getComparisonRangeType() == ComparisonRange.Type.EQUALITY))
                .collect(ImmutableSet.toImmutableSet());

        for (final var requestedOrdering : requestedOrderings) {
            final var satisfyingOrderingPartsOptional =
                    Ordering.satisfiesKeyPartsOrdering(orderingPartialOrder,
                            requestedOrdering.getOrderingKeyParts(),
                            BoundKeyPart::getKeyPart);
            final var comparisonKeyOptional =
                    satisfyingOrderingPartsOptional
                            .map(parts -> parts.stream().filter(part -> !equalityBoundKeyParts.contains(part)).collect(ImmutableList.toImmutableList()))
                            .flatMap(parts -> comparisonKey(commonPrimaryKeyParts, equalityBoundKeyParts, parts));

            if (comparisonKeyOptional.isEmpty()) {
                continue;
            }
            final KeyExpression comparisonKey = comparisonKeyOptional.get();

            final var compensation =
                    partition
                            .stream()
                            .map(partialMatch -> partialMatch.compensate(partialMatch.getBoundParameterPrefixMap()))
                            .reduce(Compensation.impossibleCompensation(), Compensation::intersect);

            final var scans =
                    partition
                            .stream()
                            .map(partialMatch -> Objects.requireNonNull(matchToExpressionMap.get(partialMatch)))
                            .collect(ImmutableList.toImmutableList());

            final var logicalIntersectionExpression = LogicalIntersectionExpression.from(scans, comparisonKey);
            final var compensatedIntersection =
                    compensation.isNeeded()
                    ? compensation.apply(GroupExpressionRef.of(logicalIntersectionExpression))
                    : logicalIntersectionExpression;
            expressionsBuilder.add(compensatedIntersection);
        }

        return expressionsBuilder.build();
    }

    /**
     * Private helper method that computes the ordering of the intersection using matches and the common primary key
     * of the data source.
     * @param partition partition we would like to intersect
     * @return a {@link PartialOrder}  of {@link BoundKeyPart} representing a common intersection ordering.
     */
    @SuppressWarnings("java:S1066")
    @Nonnull
    private static PartialOrder<BoundKeyPart> intersectionOrdering(@Nonnull final List<PartialMatch> partition) {

        final var orderingPartialOrders = partition.stream()
                .map(PartialMatch::getMatchInfo)
                .map(MatchInfo::getOrderingKeyParts)
                .map(boundKeyParts -> {
                    final var orderingKeyParts =
                            boundKeyParts.stream()
                                    .filter(boundOrderingKey -> boundOrderingKey.getComparisonRangeType() != ComparisonRange.Type.EQUALITY)
                                    .collect(ImmutableList.toImmutableList());

                    return PartialOrder.<BoundKeyPart>builder()
                            .addListWithDependencies(orderingKeyParts)
                            .addAll(boundKeyParts)
                            .build();
                })
                .collect(ImmutableList.toImmutableList());

        return Ordering.mergePartialOrderOfOrderings(orderingPartialOrders);
    }

    /**
     * Private helper method to compute a {@link KeyExpression} based upon a primary key and ordering information
     * coming from a match in form of a list of {@link BoundKeyPart}.
     * @param commonPrimaryKeyParts common primary key
     * @param equalityBoundKeyParts  a set of equality-bound key parts
     * @param indexOrderingParts alist of {@link BoundKeyPart}s
     * @return a newly constructed {@link KeyExpression} that is used for the comparison key of the intersection
     *         expression
     */
    @Nonnull
    private static Optional<KeyExpression> comparisonKey(@Nonnull List<KeyExpression> commonPrimaryKeyParts,
                                                         @Nonnull ImmutableSet<BoundKeyPart> equalityBoundKeyParts,
                                                         @Nonnull List<BoundKeyPart> indexOrderingParts) {
        final var equalityBoundPartsSet = equalityBoundKeyParts.stream()
                .map(BoundKeyPart::getNormalizedKeyExpression)
                .collect(ImmutableSet.toImmutableSet());

        final var indexOrderingPartsSet = indexOrderingParts.stream()
                .map(BoundKeyPart::getNormalizedKeyExpression)
                .collect(ImmutableSet.toImmutableSet());

        final var allCommonPrimaryKeyPartsInIndexParts =
                commonPrimaryKeyParts
                        .stream()
                        .filter(commonPrimaryKeyPart -> !equalityBoundPartsSet.contains(commonPrimaryKeyPart))
                        .allMatch(indexOrderingPartsSet::contains);

        if (!allCommonPrimaryKeyPartsInIndexParts) {
            return Optional.empty();
        }

        if (indexOrderingParts.isEmpty()) {
            return Optional.of(Key.Expressions.value(true));
        }

        if (indexOrderingParts.size() == 1) {
            return Optional.of(Iterables.getOnlyElement(indexOrderingParts).getNormalizedKeyExpression());
        }

        return Optional.of(Key.Expressions.concat(
                indexOrderingParts.stream()
                        .map(BoundKeyPart::getNormalizedKeyExpression)
                        .collect(ImmutableList.toImmutableList())));
    }

    @Nonnull
    protected static Set<Quantifier.ForEach> computeIntersectedUnmatchedForEachQuantifiers(@Nonnull RelationalExpression expression,
                                                                                           @Nonnull List<? extends PartialMatch> completeMatches) {
        final var completeMatchesIterator = completeMatches.iterator();
        if (!completeMatchesIterator.hasNext()) {
            return ImmutableSet.of();
        }

        final var intersectedQuantifiers = new LinkedIdentitySet<Quantifier.ForEach>();
        intersectedQuantifiers.addAll(expression.computeUnmatchedForEachQuantifiers(completeMatchesIterator.next()));

        while (completeMatchesIterator.hasNext()) {
            final var unmatched = expression.computeUnmatchedForEachQuantifiers(completeMatchesIterator.next());
            intersectedQuantifiers.retainAll(unmatched);
        }

        return intersectedQuantifiers;
    }
}
