/*
 * AggregateDataAccessRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.AggregateIndexMatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.Compensation;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.MatchPartition;
import com.apple.foundationdb.record.query.plan.cascades.Memoizer;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.ValueEquivalence;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.Values;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.RegularTranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryMultiIntersectionOnValuesPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQuerySetPlan;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MatchPartitionMatchers.ofExpressionAndMatches;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.some;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PartialMatchMatchers.completeMatch;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PartialMatchMatchers.matchingAggregateIndexMatchCandidate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.RelationalExpressionMatchers.anyExpression;

/**
 * A rule that utilizes index matching information compiled by {@link CascadesPlanner} to create a multitude of
 * expressions for data access involving only {@link AggregateIndexMatchCandidate}.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class AggregateDataAccessRule extends AbstractDataAccessRule {
    private static final BindingMatcher<PartialMatch> completeMatchMatcher =
            completeMatch().and(matchingAggregateIndexMatchCandidate());
    private static final BindingMatcher<RelationalExpression> expressionMatcher = anyExpression();

    private static final BindingMatcher<MatchPartition> rootMatcher =
            ofExpressionAndMatches(expressionMatcher, some(completeMatchMatcher));

    public AggregateDataAccessRule() {
        super(rootMatcher, completeMatchMatcher, expressionMatcher);
    }

    /**
     * Method to compute the intersection of multiple compatibly-ordered index scans of aggregate indexes. This method
     * utilizes {@link RecordQueryMultiIntersectionOnValuesPlan} which intersects {@code n} compatibly-ordered data
     * streams of records comprised of the groupings and the aggregates of the underlying indexes. In contrast to the
     * simpler {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionOnValuesPlan} which is
     * used to intersect value indexes, a multi intersection allows us to associate the current records of all data
     * streams by the use of a common comparison key (that identifies the group) and then freely to pick any datum
     * from any participating stream. In this way, we can access the different aggregates flowed in the individuals
     * stream.
     * <br>
     * For instance, if the partition we are trying to intersect, flows the result of an index scan over a
     * {@code SUM} index ({@code q1 := (a, b, SUM(c)}) and another flows the result of a {@code COUNT} index using an
     * identical grouping ({@code q2: = (a, b, COUNT_STAR}), we can form a multi intersection using a comparison key
     * {@code _.1, _2}. We can then also access the aggregates in individually, e.g.
     * ({@code (q1._0, q1._1, q1._2, q2._2}) would return the groupings comprised of {@code a} and {@code b} as well
     * as the sum and the count.
     * @param memoizer the memoizer
     * @param intersectionInfoMap a map that allows us to access information about other intersections of degree
     *        {@code n-1}.
     * @param matchToPlanMap a map from match to single data access expression
     * @param partition a partition (i.e. a list of {@link SingleMatchedAccess}es that the caller would like to compute
     *        and intersected data access for
     * @param requestedOrderings a set of ordering that have been requested by consuming expressions/plan operators
     * @return a new {@link IntersectionResult}
     */
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
        final var commonGroupingKeyValuesOptional =
                commonGroupingKeyValuesMaybe(
                        partitionAccesses.stream()
                                .map(SingleMatchedAccess::getPartialMatch)
                                .collect(ImmutableList.toImmutableList()));
        if (commonGroupingKeyValuesOptional.isEmpty()) {
            return IntersectionResult.noViableIntersection();
        }
        final var commonGroupingKeyValues = commonGroupingKeyValuesOptional.get();

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

        //
        // Grab the first matched access from the partition in order to translate to the required ordering
        // to the candidate's top. This is only correct as long as the derivations are consistent among the indexes.
        // We check that a bit further down.
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
                //
                // We need to ensure that the common record key, that is the key that is used to identify the
                // entire record is part of the comparison key (unless it's equality-bound).
                //
                if (!isCompatibleComparisonKey(comparisonKeyValues, commonGroupingKeyValues, equalityBoundKeyValues)) {
                    continue;
                }

                //
                // We need to ensure that the comparison key has consistent translations to all participating
                // candidates. For grouping values (and we only allow grouping values) that is rather straightforward.
                // We map the grouping values from each match candidate one-by-one to the query side grouping values
                // and see if they match across candidates.
                //
                if (!isConsistentComparisonKeyDerivations(partition, comparisonKeyValues)) {
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

                    final var newQuantifiersBuilder = ImmutableList.<Quantifier.Physical>builder();
                    final var candidateTopAliasesBuilder = ImmutableList.<CorrelationIdentifier>builder();
                    for (final var singleMatchedAccessWithIndex : partition) {
                        final var singleMatchedAccess = singleMatchedAccessWithIndex.getElement();
                        final var plan = Objects.requireNonNull(matchToPlanMap.get(
                                singleMatchedAccess.getPartialMatch()));
                        final var reference = memoizer.memoizePlan(plan);
                        newQuantifiersBuilder.add(Quantifier.physical(reference));
                        candidateTopAliasesBuilder.add(singleMatchedAccess.getCandidateTopAlias());
                    }

                    final var newQuantifiers = newQuantifiersBuilder.build();
                    final var commonAndPickUpValues =
                            computeCommonAndPickUpValues(partition, commonGroupingKeyValues.size());
                    final var intersectionResultValue =
                            computeIntersectionResultValue(newQuantifiers, commonAndPickUpValues.getLeft(), commonAndPickUpValues.getRight());
                    final var intersectionPlan =
                            RecordQueryMultiIntersectionOnValuesPlan.intersection(newQuantifiers,
                                    comparisonOrderingParts, intersectionResultValue, comparisonIsReverse);
                    final var compensatedIntersection =
                            compensation.applyAllNeededCompensations(memoizer, intersectionPlan,
                                    baseAlias -> computeTranslationMap(baseAlias,
                                            newQuantifiers, candidateTopAliasesBuilder.build(),
                                            (Type.Record)intersectionResultValue.getResultType(),
                                            commonGroupingKeyValues.size()));
                    expressionsBuilder.add(compensatedIntersection);
                }
            }
        }

        return IntersectionResult.of(hasCommonOrdering ? intersectionOrdering : null, compensation,
                expressionsBuilder.build());
    }

    @Nonnull
    protected static Optional<List<Value>> commonGroupingKeyValuesMaybe(@Nonnull Iterable<? extends PartialMatch> partialMatches) {
        List<Value> common = null;
        var first = true;
        for (final var partialMatch : partialMatches) {
            final var matchCandidate = partialMatch.getMatchCandidate();
            final var regularMatchInfo = partialMatch.getRegularMatchInfo();

            final List<Value> key;
            if (matchCandidate instanceof AggregateIndexMatchCandidate) {
                final var aggregateIndexMatchCandidate = (AggregateIndexMatchCandidate)matchCandidate;
                final var rollUpToGroupingValues = regularMatchInfo.getRollUpToGroupingValues();
                if (rollUpToGroupingValues == null) {
                    key = aggregateIndexMatchCandidate.getGroupingAndAggregateAccessors(Quantifier.current()).getLeft();
                } else {
                    key = aggregateIndexMatchCandidate.getGroupingAndAggregateAccessors(rollUpToGroupingValues.size(),
                            Quantifier.current()).getLeft();
                }
            } else {
                return Optional.empty();
            }
            if (first) {
                common = key;
                first = false;
            } else if (!common.equals(key)) {
                return Optional.empty();
            }
        }
        return Optional.ofNullable(common); // common can only be null if we didn't have any match candidates to start with
    }

    private static boolean isConsistentComparisonKeyDerivations(@Nonnull final List<Vectored<SingleMatchedAccess>> partition,
                                                                @Nonnull final List<Value> comparisonKeyValues) {
        for (final var comparisonKeyValue : comparisonKeyValues) {
            if (consistentQueryValueForGroupingValueMaybe(partition, comparisonKeyValue).isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Nonnull
    private static Optional<Value> consistentQueryValueForGroupingValueMaybe(@Nonnull final List<Vectored<SingleMatchedAccess>> partition,
                                                                             @Nonnull final Value comparisonKeyValue) {
        Value queryComparisonKeyValue = null;
        for (final var singleMatchedAccessWithIndex : partition) {
            final var singledMatchedAccess = singleMatchedAccessWithIndex.getElement();
            final var groupByMappings = singledMatchedAccess.getPulledUpGroupByMappingsForOrdering();
            final var inverseMatchedGroupingsMap =
                    groupByMappings.getMatchedGroupingsMap().inverse();
            final var currentQueryComparisonKeyValue = inverseMatchedGroupingsMap.get(comparisonKeyValue);
            if (currentQueryComparisonKeyValue == null) {
                return Optional.empty();
            }
            if (queryComparisonKeyValue == null) {
                queryComparisonKeyValue = currentQueryComparisonKeyValue;
            } else {
                final var semanticEquals =
                        queryComparisonKeyValue.semanticEquals(currentQueryComparisonKeyValue,
                                ValueEquivalence.empty());
                if (semanticEquals.isFalse()) {
                    return Optional.empty();
                }
                if (!semanticEquals.getConstraint().isTautology()) {
                    return Optional.empty();
                }
            }
        }

        return Optional.of(Objects.requireNonNull(queryComparisonKeyValue));
    }

    @Nonnull
    private static NonnullPair<List<Value>, List<Value>> computeCommonAndPickUpValues(@Nonnull final List<Vectored<SingleMatchedAccess>> partition,
                                                                                      final int numGroupings) {
        final var commonValuesAndPickUpValueByAccess =
                partition
                        .stream()
                        .map(singleMatchedAccessWithIndex ->
                                singleMatchedAccessWithIndex.getElement()
                                        .getPartialMatch()
                                        .getMatchCandidate())
                        .map(matchCandidate -> (AggregateIndexMatchCandidate)matchCandidate)
                        .map(aggregateIndexMatchCandidate ->
                                aggregateIndexMatchCandidate.getGroupingAndAggregateAccessors(numGroupings, Quantifier.current()))
                        .collect(ImmutableList.toImmutableList());

        final var pickUpValuesBuilder = ImmutableList.<Value>builder();
        for (int i = 0; i < commonValuesAndPickUpValueByAccess.size(); i++) {
            final var commonAndPickUpValuePair = commonValuesAndPickUpValueByAccess.get(i);
            final var pickUpValue = commonAndPickUpValuePair.getRight();
            pickUpValuesBuilder.add(pickUpValue);
        }
        return NonnullPair.of(commonValuesAndPickUpValueByAccess.get(0).getLeft(), pickUpValuesBuilder.build());
    }

    @Nonnull
    private static Value computeIntersectionResultValue(@Nonnull final List<? extends Quantifier> quantifiers,
                                                        @Nonnull final List<Value> commonValues,
                                                        @Nonnull final List<Value> pickUpValues) {
        final var columnBuilder = ImmutableList.<Column<? extends Value>>builder();

        // grab the common values from the first quantifier
        final var commonTranslationMap =
                TranslationMap.ofAliases(Quantifier.current(), quantifiers.get(0).getAlias());
        for (final var commonValue : commonValues) {
            columnBuilder.add(Column.unnamedOf(commonValue.translateCorrelations(commonTranslationMap)));
        }

        for (int i = 0; i < quantifiers.size(); i++) {
            final var quantifier = quantifiers.get(i);
            final var pickUpTranslationMap =
                    TranslationMap.ofAliases(Quantifier.current(), quantifier.getAlias());
            columnBuilder.add(Column.unnamedOf(pickUpValues.get(i).translateCorrelations(pickUpTranslationMap)));
        }

        return RecordConstructorValue.ofColumns(columnBuilder.build());
    }

    @Nonnull
    private static TranslationMap computeTranslationMap(@Nonnull final CorrelationIdentifier intersectionAlias,
                                                        @Nonnull final List<? extends Quantifier> quantifiers,
                                                        @Nonnull final List<CorrelationIdentifier> candidateTopAliases,
                                                        @Nonnull final Type.Record intersectionResultType,
                                                        final int numGrouped) {
        final var builder = RegularTranslationMap.builder();
        final var deconstructedIntersectionValues =
                Values.deconstructRecord(QuantifiedObjectValue.of(intersectionAlias, intersectionResultType));
        for (int quantifierIndex = 0; quantifierIndex < quantifiers.size(); quantifierIndex++) {
            final var quantifier = quantifiers.get(quantifierIndex);
            final var quantifierFlowedObjectType = (Type.Record)quantifier.getFlowedObjectType();
            final var quantifierFields = quantifierFlowedObjectType.getFields();
            Verify.verify(quantifierFields.size() == numGrouped + 1);
            final var columnBuilder =
                    ImmutableList.<Column<? extends Value>>builder();
            for (int columnIndex = 0; columnIndex < numGrouped; columnIndex++) {
                columnBuilder.add(Column.of(quantifierFields.get(columnIndex), deconstructedIntersectionValues.get(columnIndex)));
            }
            columnBuilder.add(Column.of(quantifierFields.get(numGrouped),
                    deconstructedIntersectionValues.get(numGrouped + quantifierIndex)));
            builder.when(candidateTopAliases.get(quantifierIndex)).then((alias, leaf) ->
                    RecordConstructorValue.ofColumns(columnBuilder.build()));
        }

        return builder.build();
    }
}
