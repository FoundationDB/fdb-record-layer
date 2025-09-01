/*
 * WithPrimaryKeyDataAccessRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.Compensation;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.MatchPartition;
import com.apple.foundationdb.record.query.plan.cascades.Memoizer;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.WithPrimaryKeyMatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.RegularTranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQuerySetPlan;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MatchPartitionMatchers.ofExpressionAndMatches;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.some;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PartialMatchMatchers.completeMatch;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PartialMatchMatchers.matchingWithPrimaryKeyMatchCandidate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.anyExpression;

/**
 * A rule that utilizes index matching information compiled by {@link CascadesPlanner} to create one or more
 * expressions for data access specifically for a {@link SelectExpression}. A {@link SelectExpression} behaves
 * different compared to essentially all other expressions in a way that we can conceptually deconstruct such an expression
 * on the fly and only replace the matched part of the original expression with the scan over the materialized view.
 * That allows us to relax restrictions (.e.g. to match all quantifiers the select expression owns) while matching
 * select expressions.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class WithPrimaryKeyDataAccessRule extends AbstractDataAccessRule {
    private static final BindingMatcher<PartialMatch> completeMatchMatcher =
            completeMatch().and(matchingWithPrimaryKeyMatchCandidate());
    private static final BindingMatcher<RelationalExpression> expressionMatcher = anyExpression();

    private static final BindingMatcher<MatchPartition> rootMatcher =
            ofExpressionAndMatches(expressionMatcher, some(completeMatchMatcher));

    public WithPrimaryKeyDataAccessRule() {
        super(rootMatcher, completeMatchMatcher, expressionMatcher);
    }

    @Nonnull
    @Override
    protected IntersectionResult createIntersectionAndCompensation(@Nonnull final Memoizer memoizer,
                                                                   @Nonnull final Map<BitSet, IntersectionInfo> intersectionInfoMap,
                                                                   @Nonnull final Map<PartialMatch, RecordQueryPlan> matchToPlanMap,
                                                                   @Nonnull final List<Vectored<SingleMatchedAccess>> partition,
                                                                   @Nonnull final Set<RequestedOrdering> requestedOrderings) {
        Verify.verify(partition.size() > 1);

        final var partitionAccesses =
                partition.stream()
                        .map(Vectored::getElement)
                        .collect(ImmutableList.toImmutableList());

        final var commonPrimaryKeyValuesOptional =
                commonPrimaryKeyValuesMaybe(
                        partitionAccesses.stream()
                                .map(SingleMatchedAccess::getPartialMatch)
                                .collect(ImmutableList.toImmutableList()));
        if (commonPrimaryKeyValuesOptional.isEmpty()) {
            return IntersectionResult.noViableIntersection();
        }

        final var commonPrimaryKeyValues = commonPrimaryKeyValuesOptional.get();

        final var partitionOrderings =
                partitionAccesses.stream()
                        .map(AbstractDataAccessRule::adjustMatchedOrderingParts)
                        .collect(ImmutableList.toImmutableList());
        final var intersectionOrdering = intersectOrderings(partitionOrderings);

        final var equalityBoundKeyValues =
                partitionOrderings
                        .stream()
                        .flatMap(orderingPartsPair ->
                                orderingPartsPair.getKey()
                                        .stream()
                                        .filter(boundOrderingKey -> boundOrderingKey.getComparisonRangeType() ==
                                                ComparisonRange.Type.EQUALITY)
                                        .map(OrderingPart.MatchedOrderingPart::getValue))
                        .collect(ImmutableSet.toImmutableSet());

        final var isPartitionRedundant =
                isPartitionRedundant(intersectionInfoMap, partition, equalityBoundKeyValues);
        if (isPartitionRedundant) {
            return IntersectionResult.noViableIntersection();
        }

        final var compensation =
                partitionAccesses
                        .stream()
                        .map(SingleMatchedAccess::getCompensation)
                        .reduce(Compensation.impossibleCompensation(), Compensation::intersect);
        final Function<CorrelationIdentifier, TranslationMap> matchedToRealizedTranslationMapFunction =
                realizedAlias -> matchedToRealizedTranslationMap(partition, realizedAlias);

        //
        // Grab the first matched access from the partition in order to translate to the required ordering
        // to the candidate's top.
        //
        final var firstMatchedAccess = partition.get(0).getElement();
        final var topToTopTranslationMap = firstMatchedAccess.getTopToTopTranslationMap();

        boolean hasCommonOrdering = false;
        final var expressionsBuilder = ImmutableList.<RelationalExpression>builder();
        for (final var requestedOrdering : requestedOrderings) {
            final var translatedRequestedOrdering =
                    requestedOrdering.translateCorrelations(topToTopTranslationMap, true);
            final var comparisonKeyValuesIterable =
                    intersectionOrdering.enumerateSatisfyingComparisonKeyValues(translatedRequestedOrdering);
            for (final var comparisonKeyValues : comparisonKeyValuesIterable) {
                if (!isCompatibleComparisonKey(comparisonKeyValues,
                        commonPrimaryKeyValues,
                        equalityBoundKeyValues)) {
                    continue;
                }

                hasCommonOrdering = true;
                if (!compensation.isImpossible()) {
                    var comparisonOrderingParts =
                            intersectionOrdering.directionalOrderingParts(comparisonKeyValues, translatedRequestedOrdering,
                                    OrderingPart.ProvidedSortOrder.FIXED);
                    final var comparisonIsReverse =
                            RecordQuerySetPlan.resolveComparisonDirection(comparisonOrderingParts);
                    comparisonOrderingParts = RecordQuerySetPlan.adjustFixedBindings(comparisonOrderingParts, comparisonIsReverse);

                    final var newQuantifiers =
                            partition
                                    .stream()
                                    .map(pair ->
                                            Objects.requireNonNull(matchToPlanMap.get(pair.getElement().getPartialMatch())))
                                    .map(memoizer::memoizePlan)
                                    .map(Quantifier::physical)
                                    .collect(ImmutableList.toImmutableList());

                    final var intersectionPlan =
                            RecordQueryIntersectionPlan.fromQuantifiers(newQuantifiers,
                                    comparisonOrderingParts, comparisonIsReverse);
                    final var compensatedIntersection =
                            compensation.applyAllNeededCompensations(memoizer, intersectionPlan,
                                    matchedToRealizedTranslationMapFunction);
                    expressionsBuilder.add(compensatedIntersection);
                }
            }
        }

        return IntersectionResult.of(hasCommonOrdering ? intersectionOrdering : null, compensation,
                expressionsBuilder.build());
    }

    @Nonnull
    protected static Optional<List<Value>> commonPrimaryKeyValuesMaybe(@Nonnull Iterable<? extends PartialMatch> partialMatches) {
        Verify.verify(!Iterables.isEmpty(partialMatches));
        List<Value> common = null;
        var first = true;
        for (final var partialMatch : partialMatches) {
            final var matchCandidate = partialMatch.getMatchCandidate();

            final List<Value> key;
            if (matchCandidate instanceof WithPrimaryKeyMatchCandidate) {
                final var withPrimaryKeyMatchCandidate = (WithPrimaryKeyMatchCandidate)matchCandidate;
                final var keyMaybe = withPrimaryKeyMatchCandidate.getPrimaryKeyValuesMaybe();
                if (keyMaybe.isEmpty()) {
                    return Optional.empty();
                }
                key = keyMaybe.get();
            }  else {
                return Optional.empty();
            }
            if (first) {
                common = key;
                first = false;
            } else if (!common.equals(key)) {
                return Optional.empty();
            }
        }
        return Optional.of(Objects.requireNonNull(common));
    }

    @Nonnull
    private static TranslationMap matchedToRealizedTranslationMap(@Nonnull final List<Vectored<SingleMatchedAccess>> partition,
                                                                  @Nonnull final CorrelationIdentifier realizedAlias) {
        final var translationMapBuilder = RegularTranslationMap.builder();
        for (final var singleMatchedAccessWithIndex : partition) {
            translationMapBuilder.when(singleMatchedAccessWithIndex.getElement().getCandidateTopAlias())
                    .then((sourceAlias, leafValue) -> leafValue.rebaseLeaf(
                            realizedAlias));
        }
        return translationMapBuilder.build();
    }
}
