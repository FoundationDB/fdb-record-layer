/*
 * DataAccessMatchRule.java
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

package com.apple.foundationdb.record.query.plan.temp.rules;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.temp.BoundKeyPart;
import com.apple.foundationdb.record.query.plan.temp.ChooseK;
import com.apple.foundationdb.record.query.plan.temp.ComparisonRange;
import com.apple.foundationdb.record.query.plan.temp.Compensation;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.MatchCandidate;
import com.apple.foundationdb.record.query.plan.temp.MatchWithCompensation;
import com.apple.foundationdb.record.query.plan.temp.PartialMatch;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalIntersectionExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.AnyChildrenMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.ReferenceMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.TypeMatcher;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A rule that converts a logical index scan expression to a {@link RecordQueryIndexPlan}. This rule simply converts
 * the logical index scan's {@link com.apple.foundationdb.record.query.plan.temp.view.ViewExpressionComparisons} to a
 * {@link com.apple.foundationdb.record.query.plan.ScanComparisons} to be used during query execution.
 */
@API(API.Status.EXPERIMENTAL)
public class DataAccessMatchRule extends PlannerRule<ExpressionRef<RelationalExpression>> {
    private static final ExpressionMatcher<RelationalExpression> expressionMatcher = TypeMatcher.of(RelationalExpression.class, AnyChildrenMatcher.ANY);
    private static final ReferenceMatcher<RelationalExpression> rootMatcher = ReferenceMatcher.of(expressionMatcher);
    
    public DataAccessMatchRule() {
        super(rootMatcher);
    }

    @Override
    @SuppressWarnings("java:S135")
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final ExpressionRef<RelationalExpression> ref = call.get(rootMatcher);
        final RelationalExpression expression = call.get(expressionMatcher);

        final ImmutableSet<PartialMatch> bestMatches = ref.getMatchCandidates()
                .stream()
                .flatMap(matchCandidate -> {
                    final Optional<PartialMatch> bestMatchForCandidateOptional =
                            ref.getPartialMatchesForCandidate(matchCandidate)
                                    .stream()
                                    .filter(partialMatch -> partialMatch.getQueryExpression() == expression)
                                    .filter(partialMatch -> partialMatch.getCandidateRef() == matchCandidate.getTraversal().getRootReference())
                                    .max(Comparator.comparing(PartialMatch::getNumBoundParameterPrefix));
                    return bestMatchForCandidateOptional.map(Stream::of).orElse(Stream.empty());
                })
                .collect(ImmutableSet.toImmutableSet());

        if (bestMatches.isEmpty()) {
            return;
        }

        // create scans for all best matches
        final ImmutableMap<PartialMatch, RelationalExpression> bestMatchToExpressionMap =
                bestMatches
                        .stream()
                        .collect(ImmutableMap.toImmutableMap(Function.identity(),
                                partialMatch -> {
                                    final MatchCandidate matchCandidate = partialMatch.getMatchCandidate();
                                    return matchCandidate.toScanExpression(partialMatch.getMatchWithCompensation());
                                }));

        final Collection<Pair<Map<QueryPredicate, BoundKeyPart>, List<PartialMatch>>> coverage = Lists.newArrayList();

        // create single scan accesses
        bestMatches
                .forEach(partialMatch -> updateBoundKeysForPartition(coverage, ImmutableList.of(partialMatch)));
        coverage
                .stream()
                .map(Pair::getRight)
                .map(partition -> createSimpleDataAccessAndCompensation(bestMatchToExpressionMap, Iterables.getOnlyElement(partition)))
                .forEach(newExpression -> call.yield(call.ref(newExpression)));
        coverage.clear();

        @Nullable final KeyExpression commonPrimaryKey = call.getContext().getCommonPrimaryKey();
        if (commonPrimaryKey != null) {
            final List<KeyExpression> commonPrimaryKeyParts = commonPrimaryKey.normalizeKeyForPositions();

            // create intersections for all n choose k partitions from k = 2 .. n
            IntStream.range(2, bestMatches.size())
                    .mapToObj(k -> ChooseK.chooseK(bestMatches, k))
                    .flatMap(iterable -> StreamSupport.stream(iterable.spliterator(), false))
                    .forEach(partition -> updateBoundKeysForPartition(coverage, partition));

            coverage
                    .stream()
                    .map(Pair::getRight)
                    .map(partition ->
                            createIntersectionAndCompensation(
                                    commonPrimaryKeyParts,
                                    bestMatchToExpressionMap,
                                    partition))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(newExpression -> call.yield(call.ref(newExpression)));
        }
    }

    @Nonnull
    private RelationalExpression createSimpleDataAccessAndCompensation(@Nonnull final Map<PartialMatch, RelationalExpression> bestMatchToExpressionMap,
                                                                       @Nonnull final PartialMatch partialMatch) {
        final Compensation compensation = partialMatch.compensate(partialMatch.getBoundParameterPrefixMap());
        final RelationalExpression scanExpression = bestMatchToExpressionMap.get(partialMatch);

        return compensation.isNeeded()
               ? compensation.apply(GroupExpressionRef.of(scanExpression))
               : scanExpression;
    }

    @CanIgnoreReturnValue
    private boolean updateBoundKeysForPartition(@Nonnull final Collection<Pair<Map<QueryPredicate, BoundKeyPart>, List<PartialMatch>>> coverage,
                                                @Nonnull final List<PartialMatch> partition) {
        final Map<QueryPredicate, BoundKeyPart> newBoundKeyParts = combineBoundKeyParts(partition);

        for (final Iterator<Pair<Map<QueryPredicate, BoundKeyPart>, List<PartialMatch>>> iterator = coverage.iterator(); iterator.hasNext(); ) {
            final Pair<Map<QueryPredicate, BoundKeyPart>, List<PartialMatch>> pair = iterator.next();
            final Map<QueryPredicate, BoundKeyPart> currentBoundKeyParts = pair.getLeft();

            final boolean newIncludesCurrent =
                    currentBoundKeyParts
                            .values()
                            .stream()
                            .allMatch(currentBoundKeyPart -> newBoundKeyParts.containsKey(currentBoundKeyPart.getQueryPredicate()));

            if (newIncludesCurrent && newBoundKeyParts.size() > currentBoundKeyParts.size()) {
                iterator.remove();
            } else {
                final boolean currentIncludesNew =
                        newBoundKeyParts
                                .values()
                                .stream()
                                .allMatch(currentBoundKeyPart -> currentBoundKeyParts.containsKey(currentBoundKeyPart.getQueryPredicate()));

                if (currentIncludesNew && newBoundKeyParts.size() < currentBoundKeyParts.size()) {
                    return false;
                }
            }
        }

        coverage.add(Pair.of(newBoundKeyParts, partition));
        return true;
    }

    @Nonnull
    private Map<QueryPredicate, BoundKeyPart> combineBoundKeyParts(@Nonnull final List<PartialMatch> partition) {
        return partition
                .stream()
                .flatMap(partialMatch -> {
                    final MatchWithCompensation matchWithCompensation = partialMatch.getMatchWithCompensation();
                    final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap = partialMatch.getBoundParameterPrefixMap();
                    return
                            matchWithCompensation.getBoundOrderingKeyParts()
                                    .stream()
                                    .filter(boundKeyPart -> boundKeyPart.getParameterAlias().isPresent()) // matching bound it
                                    .filter(boundKeyPart -> boundParameterPrefixMap.containsKey(boundKeyPart.getParameterAlias().get())) // can be used by a scan
                                    .peek(boundKeyPart -> Objects.requireNonNull(boundKeyPart.getQueryPredicate())); // make sure we got a predicate mapping
                })
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
                                throw new RecordCoreException("unknown range binding");
                            }
                        },
                        Maps::newIdentityHashMap));
    }

    @Nonnull
    private Optional<RelationalExpression> createIntersectionAndCompensation(@Nonnull final List<KeyExpression> commonPrimaryKeyParts,
                                                                             @Nonnull final ImmutableMap<PartialMatch, RelationalExpression> bestMatchToExpressionMap,
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
                        .collect(Compensation.toCompensation());

        final ImmutableList<RelationalExpression> scans =
                partition
                        .stream()
                        .map(partialMatch -> Objects.requireNonNull(bestMatchToExpressionMap.get(partialMatch)))
                        .collect(ImmutableList.toImmutableList());

        final LogicalIntersectionExpression logicalIntersectionExpression = LogicalIntersectionExpression.from(scans, comparisonKey);
        return Optional.of(compensation.isNeeded()
                           ? compensation.apply(GroupExpressionRef.of(logicalIntersectionExpression))
                           : logicalIntersectionExpression);
    }

    @SuppressWarnings("ConstantConditions")
    @Nonnull
    private Optional<KeyExpression> intersectionOrdering(@Nonnull final List<KeyExpression> commonPrimaryKeyParts,
                                                         @Nonnull final List<PartialMatch> partition) {
        final Optional<ImmutableList<BoundKeyPart>> compatibleOrderingKeyParts =
                partition
                        .stream()
                        .map(partialMatch -> partialMatch.getMatchWithCompensation().getBoundOrderingKeyParts())
                        .map(boundOrderingKeys -> Optional.of(boundOrderingKeys.stream().filter(boundOrderingKey -> boundOrderingKey.getComparisonRangeType() != ComparisonRange.Type.EQUALITY).collect(ImmutableList.toImmutableList())))
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
                                    leftBoundOrderingKeysOptional.get();

                            if (leftBoundKeyParts.size() != rightBoundKeyParts.size()) {
                                return Optional.empty();
                            }

                            for (int i = 0; i < leftBoundKeyParts.size(); i++) {
                                final BoundKeyPart leftKeyPart = leftBoundKeyParts.get(i);
                                final BoundKeyPart rightKeyPart = rightBoundKeyParts.get(i);

                                if (!leftKeyPart.getNormalizedKeyExpression().equals(rightKeyPart.getNormalizedKeyExpression())) {
                                    return Optional.empty();
                                }
                            }

                            return Optional.of(leftBoundKeyParts);
                        })
                        .orElseThrow(() -> new RecordCoreException("there should be at least one ordering"));

        return compatibleOrderingKeyParts
                .map(parts -> comparisonKey(commonPrimaryKeyParts, parts));
    }

    @Nonnull
    private KeyExpression comparisonKey(@Nonnull List<KeyExpression> commonPrimaryKeyParts,
                                        @Nonnull List<BoundKeyPart> indexOrderingParts) {
        final ImmutableSet<KeyExpression> indexOrderingPartsSet = indexOrderingParts.stream()
                .map(BoundKeyPart::getNormalizedKeyExpression)
                .collect(ImmutableSet.toImmutableSet());

        final ImmutableList<KeyExpression> comparisonKeyParts = commonPrimaryKeyParts
                .stream()
                .filter(indexOrderingPartsSet::contains)
                .collect(ImmutableList.toImmutableList());

        if (comparisonKeyParts.isEmpty()) {
            return Key.Expressions.value(true);
        }

        if (comparisonKeyParts.size() == 1) {
            return Iterables.getOnlyElement(commonPrimaryKeyParts);
        }

        return Key.Expressions.concat(comparisonKeyParts);
    }
}
