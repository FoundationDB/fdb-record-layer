/*
 * DataAccessRule.java
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
import com.apple.foundationdb.record.query.plan.temp.BoundKeyPart;
import com.apple.foundationdb.record.query.plan.temp.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.temp.ChooseK;
import com.apple.foundationdb.record.query.plan.temp.ComparisonRange;
import com.apple.foundationdb.record.query.plan.temp.Compensation;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.IndexScanMatchCandidate;
import com.apple.foundationdb.record.query.plan.temp.MatchCandidate;
import com.apple.foundationdb.record.query.plan.temp.MatchInfo;
import com.apple.foundationdb.record.query.plan.temp.MatchPartition;
import com.apple.foundationdb.record.query.plan.temp.PartialMatch;
import com.apple.foundationdb.record.query.plan.temp.PlannerRule;
import com.apple.foundationdb.record.query.plan.temp.PlannerRuleCall;
import com.apple.foundationdb.record.query.plan.temp.PrimaryScanMatchCandidate;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.IndexScanExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalDistinctExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.LogicalIntersectionExpression;
import com.apple.foundationdb.record.query.plan.temp.expressions.PrimaryScanExpression;
import com.apple.foundationdb.record.query.plan.temp.matchers.ExpressionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.MatchPartitionMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PartialMatchMatcher;
import com.apple.foundationdb.record.query.plan.temp.matchers.PlannerBindings;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Comparator;
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
 * A rule that utilizes index matching information compiled by {@link CascadesPlanner} to create a logical expression
 * for data access. While this rule delegates specifics to the {@link MatchCandidate}s, the following are possible
 * outcomes of the application of this transformation rule. Based on the match info, we may create for a single match:
 *
 * <ul>
 *     <li>a {@link PrimaryScanExpression} for a single {@link PrimaryScanMatchCandidate},</li>
 *     <li>an {@link IndexScanExpression} for a single {@link IndexScanMatchCandidate}</li>
 * </ul>
 *
 * The logic that this rules delegates to to actually create the expressions can be found in
 * {@link MatchCandidate#toScanExpression(MatchInfo)}.
 */
@API(API.Status.EXPERIMENTAL)
public class DataAccessRule extends PlannerRule<MatchPartition> {
    private static final ExpressionMatcher<PartialMatch> completeMatchMatcher = PartialMatchMatcher.completeMatch();
    private static final ExpressionMatcher<MatchPartition> rootMatcher = MatchPartitionMatcher.some(completeMatchMatcher);
    
    public DataAccessRule() {
        super(rootMatcher);
    }

    @Override
    @SuppressWarnings("java:S135")
    public void onMatch(@Nonnull PlannerRuleCall call) {
        final PlannerBindings bindings = call.getBindings();
        final List<PartialMatch> completeMatches = bindings.getAll(completeMatchMatcher);

        if (completeMatches.isEmpty()) {
            return;
        }

        final Map<MatchCandidate, List<PartialMatch>> completeMatchMap =
                completeMatches
                        .stream()
                        .collect(Collectors.groupingBy(PartialMatch::getMatchCandidate));

        // find the best match for a candidate as there may be more than one due to partial matching
        final ImmutableSet<PartialMatch> bestMatches =
                completeMatchMap.entrySet()
                .stream()
                        .flatMap(entry -> {
                            final List<PartialMatch> completeMatchesForCandidate = entry.getValue();
                            final Optional<PartialMatch> bestMatchForCandidateOptional =
                                    completeMatchesForCandidate
                                            .stream()
                                            .max(Comparator.comparing(PartialMatch::getNumBoundParameterPrefix));
                            return bestMatchForCandidateOptional.map(Stream::of).orElse(Stream.empty());
                        })
                        .collect(ImmutableSet.toImmutableSet());

        if (bestMatches.isEmpty()) {
            return;
        }

        // create scans for all best matches
        final ImmutableMap<PartialMatch, RelationalExpression> bestMatchToExpressionMap =
                createScansForBestMatches(bestMatches);


        // create single scan accesses
        for (final PartialMatch bestMatch : bestMatches) {
            final ImmutableList<PartialMatch> singleMatchPartition = ImmutableList.of(bestMatch);
            if (trimAndCombineBoundKeyParts(singleMatchPartition).isPresent()) {
                final RelationalExpression dataAccessAndCompensationExpression =
                        createSingleDataAccessAndCompensation(bestMatchToExpressionMap, bestMatch);
                call.yield(call.ref(dataAccessAndCompensationExpression));
            }
        }

        final ImmutableMap<PartialMatch, RelationalExpression> bestMatchToDistinctExpressionMap =
                distinctBestMatchMap(bestMatchToExpressionMap);

        @Nullable final KeyExpression commonPrimaryKey = call.getContext().getCommonPrimaryKey();
        if (commonPrimaryKey != null) {
            final List<KeyExpression> commonPrimaryKeyParts = commonPrimaryKey.normalizeKeyForPositions();


            final List<BoundPartition> boundPartitions = Lists.newArrayList();
            // create intersections for all n choose k partitions from k = 2 .. n
            IntStream.range(2, bestMatches.size())
                    .mapToObj(k -> ChooseK.chooseK(bestMatches, k))
                    .flatMap(iterable -> StreamSupport.stream(iterable.spliterator(), false))
                    .forEach(partition -> {
                        final Optional<BoundPartition> newBoundPartitionOptional = trimAndCombineBoundKeyParts(partition);
                        newBoundPartitionOptional
                                .filter(boundPartition -> isBoundPartitionInteresting(boundPartitions, boundPartition))
                                .ifPresent(boundPartitions::add);
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
                    .forEach(newExpression -> call.yield(call.ref(newExpression)));
        }
    }

    @Nonnull
    private ImmutableMap<PartialMatch, RelationalExpression> createScansForBestMatches(@Nonnull final ImmutableSet<PartialMatch> bestMatches) {
        return bestMatches
                .stream()
                .collect(ImmutableMap.toImmutableMap(
                        Function.identity(),
                        partialMatch -> {
                            final MatchCandidate matchCandidate = partialMatch.getMatchCandidate();
                            return matchCandidate.toScanExpression(partialMatch.getMatchInfo());
                        }));
    }

    @Nonnull
    private ImmutableMap<PartialMatch, RelationalExpression> distinctBestMatchMap(@Nonnull ImmutableMap<PartialMatch, RelationalExpression> bestMatchToExpressionMap) {
        return bestMatchToExpressionMap
                .entrySet()
                .stream()
                .collect(
                        ImmutableMap.toImmutableMap(Map.Entry::getKey,
                                entry -> {
                                    final RelationalExpression dataAccessExpression = entry.getValue();
                                    return new LogicalDistinctExpression(dataAccessExpression);
                                }));
    }

    @Nonnull
    private RelationalExpression createSingleDataAccessAndCompensation(@Nonnull final Map<PartialMatch, RelationalExpression> bestMatchToExpressionMap,
                                                                       @Nonnull final PartialMatch partialMatch) {
        final Compensation compensation = partialMatch.compensate(partialMatch.getBoundParameterPrefixMap());
        final RelationalExpression scanExpression = bestMatchToExpressionMap.get(partialMatch);

        return compensation.isNeeded()
               ? compensation.apply(GroupExpressionRef.of(scanExpression))
               : scanExpression;
    }

    @SuppressWarnings("unused")
    private boolean isBoundPartitionInteresting(@Nonnull final List<BoundPartition> boundPartitions,
                                                @Nonnull final BoundPartition partition) {
        return true;
    }
    
    @Nonnull
    @SuppressWarnings("java:S1905")
    private Optional<BoundPartition> trimAndCombineBoundKeyParts(@Nonnull final List<PartialMatch> partition) {
        final ImmutableList<Map<QueryPredicate, BoundKeyPart>> boundKeyPartMapsForMatches = partition
                .stream()
                .map(partialMatch -> {
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
                })
                .sorted(Comparator.comparing((Function<Map<QueryPredicate, BoundKeyPart>, Integer>)Map::size).reversed())
                .collect(ImmutableList.toImmutableList());

        for (int i = 0; i < boundKeyPartMapsForMatches.size(); i++) {
            final Map<QueryPredicate, BoundKeyPart> outer = boundKeyPartMapsForMatches.get(i);

            for (int j = 0; j < boundKeyPartMapsForMatches.size(); j++) {
                final Map<QueryPredicate, BoundKeyPart> inner = boundKeyPartMapsForMatches.get(j);
                // check if outer is completely contained in inner
                if (outer.size() > inner.size()) {
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
                        .reduce(Compensation.impossibleCompensation(), Compensation::intersect);

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

    @SuppressWarnings({"ConstantConditions", "java:S1066"})
    @Nonnull
    private Optional<KeyExpression> intersectionOrdering(@Nonnull final List<KeyExpression> commonPrimaryKeyParts,
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

        return compatibleOrderingKeyPartsOptional
                .map(parts -> comparisonKey(commonPrimaryKeyParts, parts));
    }

    @Nonnull
    private KeyExpression comparisonKey(@Nonnull List<KeyExpression> commonPrimaryKeyParts,
                                        @Nonnull List<BoundKeyPart> indexOrderingParts) {
        final ImmutableSet<KeyExpression> indexOrderingPartsSet = indexOrderingParts.stream()
                .map(BoundKeyPart::getNormalizedKeyExpression)
                .collect(ImmutableSet.toImmutableSet());

        final ImmutableList<KeyExpression> comparisonKeyParts =
                commonPrimaryKeyParts
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
