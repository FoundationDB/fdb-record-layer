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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.temp.BoundKeyPart;
import com.apple.foundationdb.record.query.plan.temp.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.temp.ChooseK;
import com.apple.foundationdb.record.query.plan.temp.ComparisonRange;
import com.apple.foundationdb.record.query.plan.temp.Compensation;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.KeyPart;
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
import com.apple.foundationdb.record.query.plan.temp.ValueIndexScanMatchCandidate;
import com.apple.foundationdb.record.query.plan.temp.expressions.IndexScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalDistinctExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalIntersectionExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.PrimaryScanExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.BindingMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
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
public abstract class AbstractDataAccessRule<R extends RelationalExpression> extends PlannerRule<MatchPartition> {
    private final BindingMatcher<PartialMatch> completeMatchMatcher;
    private final BindingMatcher<R> expressionMatcher;

    public AbstractDataAccessRule(@Nonnull final BindingMatcher<MatchPartition> rootMatcher,
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
        final PlannerBindings bindings = call.getBindings();
        final List<? extends PartialMatch> completeMatches = bindings.getAll(getCompleteMatchMatcher());
        final R expression = bindings.get(getExpressionMatcher());

        //
        // return if there are no complete matches
        //
        if (completeMatches.isEmpty()) {
            return;
        }

        //
        // return if there is no pre-determined interesting ordering
        //
        final Optional<Set<Ordering>> interestingOrderingsOptional =
                call.getInterestingProperty(OrderingAttribute.ORDERING);
        if (!interestingOrderingsOptional.isPresent()) {
            return;
        }

        final Set<Ordering> interestingOrderings = interestingOrderingsOptional.get();

        //
        // group matches by candidates
        //
        final LinkedHashMap<MatchCandidate, ? extends ImmutableList<? extends PartialMatch>> completeMatchMap =
                completeMatches
                        .stream()
                        .collect(Collectors.groupingBy(PartialMatch::getMatchCandidate, LinkedHashMap::new, ImmutableList.toImmutableList()));

        // find the best match for a candidate as there may be more than one due to partial matching
        final ImmutableSet<PartialMatch> maximumCoverageMatchPerCandidate =
                completeMatchMap.entrySet()
                        .stream()
                        .flatMap(entry -> {
                            final List<? extends PartialMatch> completeMatchesForCandidate = entry.getValue();
                            final Optional<? extends PartialMatch> bestMatchForCandidateOptional =
                                    completeMatchesForCandidate
                                            .stream()
                                            .max(Comparator.comparing(PartialMatch::getNumBoundParameterPrefix));
                            return bestMatchForCandidateOptional.map(Stream::of).orElse(Stream.empty());
                        })
                        .collect(ImmutableSet.toImmutableSet());

        final List<PartialMatch> bestMaximumCoverageMatches = maximumCoverageMatches(maximumCoverageMatchPerCandidate, interestingOrderings);
        if (bestMaximumCoverageMatches.isEmpty()) {
            return;
        }

        // create scans for all best matches
        final Map<PartialMatch, RelationalExpression> bestMatchToExpressionMap =
                createScansForMatches(bestMaximumCoverageMatches);

        final ExpressionRef<RelationalExpression> toBeInjectedReference = GroupExpressionRef.empty();

        // create single scan accesses
        for (final PartialMatch bestMatch : bestMaximumCoverageMatches) {
            final ImmutableList<PartialMatch> singleMatchPartition = ImmutableList.of(bestMatch);
            if (trimAndCombineBoundKeyParts(singleMatchPartition).isPresent()) {
                final RelationalExpression dataAccessAndCompensationExpression =
                        compensateSingleDataAccess(bestMatch, bestMatchToExpressionMap.get(bestMatch));
                toBeInjectedReference.insert(dataAccessAndCompensationExpression);
            }
        }

        final Map<PartialMatch, RelationalExpression> bestMatchToDistinctExpressionMap =
                distinctMatchToScanMap(bestMatchToExpressionMap);

        @Nullable final KeyExpression commonPrimaryKey = call.getContext().getCommonPrimaryKey();
        if (commonPrimaryKey != null) {
            final List<KeyExpression> commonPrimaryKeyParts = commonPrimaryKey.normalizeKeyForPositions();

            final List<BoundPartition> boundPartitions = Lists.newArrayList();
            // create intersections for all n choose k partitions from k = 2 .. n
            IntStream.range(2, bestMaximumCoverageMatches.size() + 1)
                    .mapToObj(k -> ChooseK.chooseK(bestMaximumCoverageMatches, k))
                    .flatMap(iterable -> StreamSupport.stream(iterable.spliterator(), false))
                    .forEach(partition -> {
                        final Optional<BoundPartition> newBoundPartitionOptional = trimAndCombineBoundKeyParts(partition);
                        newBoundPartitionOptional.ifPresent(boundPartitions::add);
                    });

            boundPartitions
                    .stream()
                    .map(BoundPartition::getPartition)
                    .map(partition ->
                            createIntersectionAndCompensation(
                                    commonPrimaryKeyParts,
                                    bestMatchToDistinctExpressionMap,
                                    partition))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
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
    private static List<PartialMatch> maximumCoverageMatches(@Nonnull final Collection<PartialMatch> matches, @Nonnull final Set<Ordering> interestedOrderings) {
        final ImmutableList<Pair<PartialMatch, Map<QueryPredicate, BoundKeyPart>>> boundKeyPartMapsForMatches =
                matches
                        .stream()
                        .filter(partialMatch -> !satisfiedOrderings(partialMatch, interestedOrderings).isEmpty())
                        .map(partialMatch -> Pair.of(partialMatch, computeBoundKeyPartMap(partialMatch)))
                        .sorted(Comparator.comparing((Function<Pair<PartialMatch, Map<QueryPredicate, BoundKeyPart>>, Integer>)p -> p.getValue().size()).reversed())
                        .collect(ImmutableList.toImmutableList());

        final ImmutableList.Builder<PartialMatch> maximumCoverageMatchesBuilder = ImmutableList.builder();
        for (int i = 0; i < boundKeyPartMapsForMatches.size(); i++) {
            final PartialMatch outerMatch = boundKeyPartMapsForMatches.get(i).getKey();
            final Map<QueryPredicate, BoundKeyPart> outer = boundKeyPartMapsForMatches.get(i).getValue();

            boolean foundContainingInner = false;
            for (int j = 0; j < boundKeyPartMapsForMatches.size(); j++) {
                final Map<QueryPredicate, BoundKeyPart> inner = boundKeyPartMapsForMatches.get(j).getValue();
                // check if outer is completely contained in inner
                if (outer.size() >= inner.size()) {
                    break;
                }

                if (i != j) {
                    final boolean allContained =
                            outer.entrySet()
                                    .stream()
                                    .allMatch(outerEntry -> inner.containsKey(outerEntry.getKey()));
                    if (allContained) {
                        foundContainingInner = true;
                        break;
                    }
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
     * Private helper method to compute the subset of orderings of orderings passed in that would be satisfied by a scan
     * if the given {@link PartialMatch} were to be planned.
     * @param partialMatch a partial match
     * @param requestedOrderings a set of {@link Ordering}s
     * @return a subset of {@code requestedOrderings} where each contained {@link Ordering} would be satisfied by the
     *         given partial match
     */
    @Nonnull
    @SuppressWarnings("java:S135")
    private static Set<Ordering> satisfiedOrderings(@Nonnull final PartialMatch partialMatch, @Nonnull final Set<Ordering> requestedOrderings) {
        return requestedOrderings
                .stream()
                .filter(requestedOrdering -> {
                    if (requestedOrdering.isPreserve()) {
                        return true;
                    }

                    final MatchInfo matchInfo = partialMatch.getMatchInfo();
                    final List<BoundKeyPart> boundKeyParts = matchInfo.getBoundKeyParts();
                    final ImmutableSet<KeyExpression> equalityBoundKeys =
                            boundKeyParts
                                    .stream()
                                    .filter(boundKeyPart -> boundKeyPart.getComparisonRangeType() == ComparisonRange.Type.EQUALITY)
                                    .map(KeyPart::getNormalizedKeyExpression)
                                    .collect(ImmutableSet.toImmutableSet());

                    final Iterator<BoundKeyPart> boundKeyPartIterator = boundKeyParts.iterator();
                    for (final KeyPart requestedOrderingKeyPart : requestedOrdering.getOrderingKeyParts()) {
                        final KeyExpression requestedOrderingKey = requestedOrderingKeyPart.getNormalizedKeyExpression();

                        if (equalityBoundKeys.contains(requestedOrderingKey)) {
                            continue;
                        }

                        // if we are here, we must now find an non-equality-bound expression
                        boolean found = false;
                        while (boundKeyPartIterator.hasNext()) {
                            final BoundKeyPart boundKeyPart = boundKeyPartIterator.next();
                            if (boundKeyPart.getComparisonRangeType() == ComparisonRange.Type.EQUALITY) {
                                continue;
                            }

                            final KeyExpression boundKey = boundKeyPart.getNormalizedKeyExpression();
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
     * @param matches a collection of matches
     * @return a map of the matches where a match is associated with a scan expression created based on that match
     */
    @Nonnull
    private static Map<PartialMatch, RelationalExpression> createScansForMatches(@Nonnull final Collection<PartialMatch> matches) {
        return matches
                .stream()
                .collect(ImmutableMap.toImmutableMap(
                        Function.identity(),
                        partialMatch -> {
                            final MatchCandidate matchCandidate = partialMatch.getMatchCandidate();
                            return matchCandidate.toEquivalentExpression(partialMatch);
                        }));
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
        final Compensation compensation = partialMatch.compensate(partialMatch.getBoundParameterPrefixMap());
        return compensation.isNeeded()
               ? compensation.apply(GroupExpressionRef.of(scanExpression))
               : scanExpression;
    }

    @Nonnull
    @SuppressWarnings("java:S1905")
    private Optional<BoundPartition> trimAndCombineBoundKeyParts(@Nonnull final List<PartialMatch> partition) {
        final ImmutableList<Map<QueryPredicate, BoundKeyPart>> boundKeyPartMapsForMatches = partition
                .stream()
                .map(AbstractDataAccessRule::computeBoundKeyPartMap)
                .sorted(Comparator.comparing((Function<Map<QueryPredicate, BoundKeyPart>, Integer>)Map::size).reversed())
                .collect(ImmutableList.toImmutableList());

        //
        // TODO This is redundant logic (see #maximumCoverageMatches()). I will leave this in here for now as
        //      this loop should never have any effect on the outcome of this rule.
        //
        for (int i = 0; i < boundKeyPartMapsForMatches.size(); i++) {
            final Map<QueryPredicate, BoundKeyPart> outer = boundKeyPartMapsForMatches.get(i);

            for (int j = 0; j < boundKeyPartMapsForMatches.size(); j++) {
                final Map<QueryPredicate, BoundKeyPart> inner = boundKeyPartMapsForMatches.get(j);
                // check if outer is completely contained in inner
                if (outer.size() >= inner.size()) {
                    break;
                }

                if (i != j) {
                    final boolean allContained =
                            outer.entrySet()
                                    .stream()
                                    .allMatch(outerEntry -> inner.containsKey(outerEntry.getKey()));
                    if (allContained) {
                        return Optional.empty();
                    }
                }
            }
        }

        final Map<QueryPredicate, BoundKeyPart> predicateToBoundKeyPartMap = boundKeyPartMapsForMatches
                .stream()
                .flatMap(map -> map.values().stream())
                .collect(Collectors.toMap(BoundKeyPart::getQueryPredicate,
                        Function.identity(),
                        (oldValue, newValue) -> {
                            switch (oldValue.getComparisonRangeType()) {
                            case EMPTY:
                                return newValue;
                            case EQUALITY:
                                Verify.verify(oldValue.getNormalizedKeyExpression().equals(newValue.getNormalizedKeyExpression()));
                                return oldValue;
                            case INEQUALITY:
                                if (newValue.getComparisonRangeType() == ComparisonRange.Type.EMPTY) {
                                    return oldValue;
                                }
                                return newValue;
                            default:
                                throw new RecordCoreException("unknown range comparison");
                            }
                        },
                        Maps::newIdentityHashMap));
        return Optional.of(new BoundPartition(predicateToBoundKeyPartMap, partition));
    }

    @Nonnull
    private static Map<QueryPredicate, BoundKeyPart> computeBoundKeyPartMap(final PartialMatch partialMatch) {
        final MatchInfo matchInfo = partialMatch.getMatchInfo();
        final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap = partialMatch.getBoundParameterPrefixMap();
        return
                matchInfo.getBoundKeyParts()
                        .stream()
                        .filter(boundKeyPart -> boundKeyPart.getParameterAlias().isPresent()) // matching bound it
                        .filter(boundKeyPart -> boundParameterPrefixMap.containsKey(boundKeyPart.getParameterAlias().get())) // can be used by a scan
                        .peek(boundKeyPart -> Objects.requireNonNull(boundKeyPart.getQueryPredicate())) // make sure we got a predicate mapping
                        .collect(Collectors.toMap(BoundKeyPart::getQueryPredicate,
                                Function.identity(),
                                (a, b) -> {
                                    if (a.getCandidatePredicate() == b.getCandidatePredicate() &&
                                            a.getComparisonRangeType() == b.getComparisonRangeType()) {
                                        return a;
                                    }
                                    throw new RecordCoreException("merge conflict");
                                },
                                Maps::newIdentityHashMap));
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
     * @return a optional containing a new {@link RelationalExpression} that represents the data access and its
     *         compensation, {@code Optional.empty()} if this method was unable to compute the intersection expression
     *
     */
    @Nonnull
    private static Optional<RelationalExpression> createIntersectionAndCompensation(@Nonnull final List<KeyExpression> commonPrimaryKeyParts,
                                                                                    @Nonnull final Map<PartialMatch, RelationalExpression> matchToExpressionMap,
                                                                                    @Nonnull final List<PartialMatch> partition) {

        final Optional<KeyExpression> comparisonKeyOptional = intersectionOrdering(commonPrimaryKeyParts, partition);
        if (!comparisonKeyOptional.isPresent()) {
            return Optional.empty();
        }
        final KeyExpression comparisonKey = comparisonKeyOptional.get();

        final Compensation compensation =
                partition
                        .stream()
                        .map(partialMatch -> partialMatch.compensate(partialMatch.getBoundParameterPrefixMap()))
                        .reduce(Compensation.impossibleCompensation(), Compensation::intersect);

        final ImmutableList<RelationalExpression> scans =
                partition
                        .stream()
                        .map(partialMatch -> Objects.requireNonNull(matchToExpressionMap.get(partialMatch)))
                        .collect(ImmutableList.toImmutableList());

        final LogicalIntersectionExpression logicalIntersectionExpression = LogicalIntersectionExpression.from(scans, comparisonKey);
        return Optional.of(compensation.isNeeded()
                           ? compensation.apply(GroupExpressionRef.of(logicalIntersectionExpression))
                           : logicalIntersectionExpression);
    }

    /**
     * Private helper method that computes the ordering of the intersection using matches and the common primary key
     * of the data source.
     * TODO This logic turned out to be very similar to {@link Ordering#commonOrderingKeys(List, Ordering)}. This is
     *      a case of converging evolution but should be addressed. We should call that code path to establish that
     *      we are creating a valid intersection. In fact, we shouldn't need a comparison key in the
     *      {@link LogicalIntersectionExpression} as that will be computable through the plan children of the
     *      implementing intersection plan.
     * @param commonPrimaryKeyParts common primary key of the data source (e.g., record types)
     * @param partition partition we would like to intersect
     * @return an optional {@link KeyExpression} if there is a common intersection ordering, {@code Optional.empty()} if
     *         such a common intersection ordering could not be established
     */
    @SuppressWarnings({"ConstantConditions", "java:S1066"})
    @Nonnull
    private static Optional<KeyExpression> intersectionOrdering(@Nonnull final List<KeyExpression> commonPrimaryKeyParts,
                                                                @Nonnull final List<PartialMatch> partition) {
        final Optional<ImmutableList<BoundKeyPart>> compatibleOrderingKeyPartsOptional =
                partition
                        .stream()
                        .map(partialMatch -> partialMatch.getMatchInfo().getBoundKeyParts())
                        .map(boundOrderingKeyParts -> Optional.of(
                                boundOrderingKeyParts.stream()
                                        .filter(boundOrderingKey -> boundOrderingKey.getComparisonRangeType() != ComparisonRange.Type.EQUALITY)
                                        .collect(ImmutableList.toImmutableList())))
                        .reduce((leftBoundOrderingKeysOptional, rightBoundOrderingKeysOptional) -> {
                            if (!leftBoundOrderingKeysOptional.isPresent()) {
                                return Optional.empty();
                            }

                            if (!rightBoundOrderingKeysOptional.isPresent()) {
                                return Optional.empty();
                            }

                            final ImmutableList<BoundKeyPart> leftBoundKeyParts =
                                    leftBoundOrderingKeysOptional.get();
                            final ImmutableList<BoundKeyPart> rightBoundKeyParts =
                                    rightBoundOrderingKeysOptional.get();

                            if (leftBoundKeyParts.size() != rightBoundKeyParts.size()) {
                                return Optional.empty();
                            }

                            for (int i = 0; i < leftBoundKeyParts.size(); i++) {
                                final BoundKeyPart leftKeyPart = leftBoundKeyParts.get(i);
                                final BoundKeyPart rightKeyPart = rightBoundKeyParts.get(i);

                                if (leftKeyPart.getComparisonRangeType() != ComparisonRange.Type.EMPTY &&
                                        rightKeyPart.getComparisonRangeType() != ComparisonRange.Type.EMPTY) {
                                    if (leftKeyPart.getQueryPredicate() != rightKeyPart.getQueryPredicate()) {
                                        return Optional.empty();
                                    }
                                }

                                //
                                // Example of where this is a problem
                                // (a is a repeated field)
                                //
                                // Index 1: concat(a.b, a.c))
                                // Index 2: a.(concat(b, c))
                                //
                                // normalized expressions for both:
                                // concat(a.b, a.c)
                                //
                                // Hence we cannot do this comparison in the presence of repeated field unless we
                                // distinct the records first and then feed it into the intersection. Since we can
                                // rely on the planner to apply optimizations orthogonally we can just inject the
                                // distinct expression here assuming the distinct expression can be optimized
                                // away later if possible.
                                //
                                final KeyExpression leftKeyExpression = leftKeyPart.getNormalizedKeyExpression();
                                final KeyExpression rightKeyExpression = rightKeyPart.getNormalizedKeyExpression();

                                //
                                // Without a distinct expression underneath the intersection we would have to reject
                                // the partition is either left or right key expression creates duplicates.
                                //
                                if (!leftKeyExpression.equals(rightKeyExpression)) {
                                    return Optional.empty();
                                }
                            }

                            return Optional.of(leftBoundKeyParts);
                        })
                        .orElseThrow(() -> new RecordCoreException("there should be at least one ordering"));

        final ImmutableSet<BoundKeyPart> equalityBoundKeyParts = partition
                .stream()
                .map(partialMatch -> partialMatch.getMatchInfo().getBoundKeyParts())
                .flatMap(boundOrderingKeyParts ->
                        boundOrderingKeyParts.stream()
                                .filter(boundOrderingKey -> boundOrderingKey.getComparisonRangeType() == ComparisonRange.Type.EQUALITY))
                .collect(ImmutableSet.toImmutableSet());

        return compatibleOrderingKeyPartsOptional
                .flatMap(parts -> comparisonKey(commonPrimaryKeyParts, equalityBoundKeyParts, parts));
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
        final ImmutableSet<KeyExpression> equalityBoundPartsSet = equalityBoundKeyParts.stream()
                .map(BoundKeyPart::getNormalizedKeyExpression)
                .collect(ImmutableSet.toImmutableSet());

        final ImmutableSet<KeyExpression> indexOrderingPartsSet = indexOrderingParts.stream()
                .map(BoundKeyPart::getNormalizedKeyExpression)
                .collect(ImmutableSet.toImmutableSet());

        final boolean allComparisonPartsInIndexParts =
                commonPrimaryKeyParts
                        .stream()
                        .filter(commonPrimaryKeyPart -> !equalityBoundPartsSet.contains(commonPrimaryKeyPart))
                        .allMatch(indexOrderingPartsSet::contains);

        if (!allComparisonPartsInIndexParts) {
            return Optional.empty();
        }

        if (indexOrderingParts.isEmpty()) {
            Key.Expressions.value(true);
        }

        if (indexOrderingParts.size() == 1) {
            return Optional.of(Iterables.getOnlyElement(indexOrderingParts).getNormalizedKeyExpression());
        }

        return Optional.of(Key.Expressions.concat(
                indexOrderingParts.stream()
                        .map(KeyPart::getNormalizedKeyExpression)
                        .collect(ImmutableList.toImmutableList())));
    }

    @Nonnull
    protected static Set<Quantifier.ForEach> computeIntersectedUnmatchedForEachQuantifiers(@Nonnull RelationalExpression expression,
                                                                                           @Nonnull List<? extends PartialMatch> completeMatches) {
        final Iterator<? extends PartialMatch> completeMatchesIterator = completeMatches.iterator();
        if (!completeMatchesIterator.hasNext()) {
            return ImmutableSet.of();
        }

        final LinkedIdentitySet<Quantifier.ForEach> intersectedQuantifiers = new LinkedIdentitySet<>();
        intersectedQuantifiers.addAll(expression.computeUnmatchedForEachQuantifiers(completeMatchesIterator.next()));

        while (completeMatchesIterator.hasNext()) {
            final Set<Quantifier.ForEach> unmatched = expression.computeUnmatchedForEachQuantifiers(completeMatchesIterator.next());
            intersectedQuantifiers.retainAll(unmatched);
        }

        return intersectedQuantifiers;
    }

    @SuppressWarnings("unused")
    private static class BoundPartition {
        @Nonnull
        private final Map<QueryPredicate, BoundKeyPart> predicateToBoundKeyPartMap;
        @Nonnull
        private final List<PartialMatch> partition;

        private BoundPartition(@Nonnull final Map<QueryPredicate, BoundKeyPart> predicateToBoundKeyPartMap, @Nonnull final List<PartialMatch> partition) {
            this.predicateToBoundKeyPartMap = predicateToBoundKeyPartMap;
            this.partition = partition;
        }

        @Nonnull
        public Map<QueryPredicate, BoundKeyPart> getPredicateToBoundKeyPartMap() {
            return predicateToBoundKeyPartMap;
        }

        @Nonnull
        public List<PartialMatch> getPartition() {
            return partition;
        }
    }
}
