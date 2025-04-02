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
import com.apple.foundationdb.record.query.plan.cascades.CascadesRuleCall;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.Compensation;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentitySet;
import com.apple.foundationdb.record.query.plan.cascades.MatchPartition;
import com.apple.foundationdb.record.query.plan.cascades.Memoizer;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrderingConstraint;
import com.apple.foundationdb.record.query.plan.cascades.ValueEquivalence;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.Values;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryMultiIntersectionOnValuesPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQuerySetPlan;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.record.util.pair.Pair;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MatchPartitionMatchers.ofExpressionAndMatches;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.some;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PartialMatchMatchers.completeMatch;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PartialMatchMatchers.matchingAggregateIndexMatchCandidate;
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
public class AggregateDataAccessRule extends AbstractDataAccessRule<RelationalExpression> {
    private static final BindingMatcher<PartialMatch> completeMatchMatcher =
            completeMatch().and(matchingAggregateIndexMatchCandidate());
    private static final BindingMatcher<RelationalExpression> expressionMatcher = anyExpression();

    private static final BindingMatcher<MatchPartition> rootMatcher =
            ofExpressionAndMatches(expressionMatcher, some(completeMatchMatcher));

    public AggregateDataAccessRule() {
        super(rootMatcher, completeMatchMatcher, expressionMatcher);
    }

    @Override
    public void onMatch(@Nonnull final CascadesRuleCall call) {
        final var bindings = call.getBindings();
        final var completeMatches = bindings.getAll(getCompleteMatchMatcher());
        if (completeMatches.isEmpty()) {
            return;
        }

        final var expression = bindings.get(getExpressionMatcher());

        //
        // return if there is no pre-determined interesting ordering
        //
        final var requestedOrderingsOptional = call.getPlannerConstraintMaybe(RequestedOrderingConstraint.REQUESTED_ORDERING);
        if (requestedOrderingsOptional.isEmpty()) {
            return;
        }

        final var requestedOrderings = requestedOrderingsOptional.get();
        final var aliasToQuantifierMap = Quantifiers.aliasToQuantifierMap(expression.getQuantifiers());
        final var aliases = aliasToQuantifierMap.keySet();

        // group all successful matches by their sets of compensated aliases
        final var matchPartitionByMatchAliasMap =
                completeMatches
                        .stream()
                        .flatMap(match -> {
                            final var compensatedAliases = match.getCompensatedAliases();
                            if (!compensatedAliases.containsAll(aliases)) {
                                return Stream.empty();
                            }
                            final Set<CorrelationIdentifier> matchedForEachAliases =
                                    compensatedAliases.stream()
                                            .filter(matchedAlias -> Objects.requireNonNull(aliasToQuantifierMap.get(matchedAlias)) instanceof Quantifier.ForEach)
                                            .collect(ImmutableSet.toImmutableSet());
                            if (matchedForEachAliases.size() == 1) {
                                return Stream.of(NonnullPair.of(Iterables.getOnlyElement(matchedForEachAliases), match));
                            }
                            return Stream.empty();
                        })
                        .collect(Collectors.groupingBy(
                                Pair::getLeft,
                                LinkedHashMap::new,
                                Collectors.mapping(Pair::getRight, ImmutableList.toImmutableList())));

        // loop through all compensated alias sets and their associated match partitions
        for (final var matchPartitionByMatchAliasEntry : matchPartitionByMatchAliasMap.entrySet()) {
            final var matchPartitionForMatchedAlias =
                    matchPartitionByMatchAliasEntry.getValue();

            //
            // We do know that local predicates (which includes predicates only using the matchedAlias quantifier)
            // are definitely handled by the logic expressed by the partial matches of the current match partition.
            // Join predicates are different in a sense that there will be matches that handle those predicates and
            // there will be matches where these predicates will not be handled. We further need to sub-partition the
            // current match partition, by the predicates that are being handled by the matches.
            //
            // TODO this should just be exactly one key
            final var matchPartitionsForAliasesByPredicates =
                    matchPartitionForMatchedAlias
                            .stream()
                            .collect(Collectors.groupingBy(match ->
                                            new LinkedIdentitySet<>(match.getRegularMatchInfo().getPredicateMap().keySet()),
                                    HashMap::new,
                                    ImmutableList.toImmutableList()));

            //
            // Note that this works because there is only one for-each and potentially 0 - n existential quantifiers
            // that are covered by the match partition. Even though that logically forms a join, the existential
            // quantifiers do not mutate the result of the join, they only cause filtering, that is, the resulting
            // record is exactly what the for each quantifier produced filtered by the predicates expressed on the
            // existential quantifiers.
            //
            for (final var matchPartitionEntry : matchPartitionsForAliasesByPredicates.entrySet()) {
                final var matchPartition = matchPartitionEntry.getValue();

                //
                // The current match partition covers all matches that match the aliases in matchedAliases
                // as well as all predicates in matchedPredicates. In other words we now have to compensate
                // for all the remaining quantifiers and all remaining predicates.
                //
                final var dataAccessExpressions =
                        dataAccessForMatchPartition(call,
                                requestedOrderings,
                                matchPartition);
                call.yieldMixedUnknownExpressions(dataAccessExpressions);
            }
        }
    }

    @Nonnull
    @Override
    protected IntersectionResult createIntersectionAndCompensation(@Nonnull final Memoizer memoizer,
                                                                   @Nonnull final Map<BitSet, IntersectionInfo> intersectionInfoMap,
                                                                   @Nonnull final Map<PartialMatch, RecordQueryPlan> matchToPlanMap,
                                                                   @Nonnull final List<Vectored<SingleMatchedAccess>> partition,
                                                                   @Nonnull final Set<RequestedOrdering> requestedOrderings) {
        Verify.verify(partition.size() > 1);
        final var commonRecordKeyValuesOptional =
                commonRecordKeyValuesMaybe(
                        partition.stream()
                                .map(singleMatchedAccessVectored ->
                                        singleMatchedAccessVectored.getElement().getPartialMatch())
                                .collect(ImmutableList.toImmutableList()));
        if (commonRecordKeyValuesOptional.isEmpty()) {
            return IntersectionResult.noCommonOrdering();
        }
        final var commonRecordKeyValues = commonRecordKeyValuesOptional.get();

        final var partitionOrderings =
                partition.stream()
                        .map(Vectored::getElement)
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
            return IntersectionResult.noCommonOrdering();
        }

        final var compensation =
                partition
                        .stream()
                        .map(pair -> pair.getElement().getCompensation())
                        .reduce(Compensation.impossibleCompensation(), Compensation::intersect);

        boolean hasCommonOrdering = false;
        final var expressionsBuilder = ImmutableList.<RelationalExpression>builder();
        for (final var requestedOrdering : requestedOrderings) {
            final var comparisonKeyValuesIterable =
                    intersectionOrdering.enumerateSatisfyingComparisonKeyValues(requestedOrdering);
            for (final var comparisonKeyValues : comparisonKeyValuesIterable) {
                if (!isCompatibleComparisonKey(comparisonKeyValues,
                        commonRecordKeyValues,
                        equalityBoundKeyValues)) {
                    continue;
                }

                if (!isCompatibleDerivationAcrossMatches(partition, comparisonKeyValues)) {
                    continue;
                }

                hasCommonOrdering = true;
                if (!compensation.isImpossible()) {
                    var comparisonOrderingParts =
                            intersectionOrdering.directionalOrderingParts(comparisonKeyValues, requestedOrdering,
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
                        final var reference = memoizer.memoizePlans(plan);
                        newQuantifiersBuilder.add(Quantifier.physical(reference));
                        candidateTopAliasesBuilder.add(singleMatchedAccess.getCandidateTopAlias());
                    }

                    final var newQuantifiers = newQuantifiersBuilder.build();
                    final var commonAndPickUpValues =
                            computeCommonAndPickUpValues(partition, commonRecordKeyValues.size());
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
                                            commonRecordKeyValues.size()));
                    expressionsBuilder.add(compensatedIntersection);
                }
            }
        }

        return IntersectionResult.of(hasCommonOrdering ? intersectionOrdering : null, compensation,
                expressionsBuilder.build());
    }

    private static boolean isCompatibleDerivationAcrossMatches(@Nonnull final List<Vectored<SingleMatchedAccess>> partition,
                                                               @Nonnull final List<Value> comparisonKeyValues) {
        for (final var comparisonKeyValue : comparisonKeyValues) {
            Value queryComparisonKeyValue = null;
            for (final var singleMatchedAccessWithIndex : partition) {
                final var singledMatchedAccess = singleMatchedAccessWithIndex.getElement();
                final var groupByMappings = singledMatchedAccess.getPulledUpGroupByMappingsForOrdering();
                final var inverseMatchedGroupingsMap =
                        groupByMappings.getMatchedGroupingsMap().inverse();
                final var currentQueryComparisonKeyValue = inverseMatchedGroupingsMap.get(comparisonKeyValue);
                if (currentQueryComparisonKeyValue == null) {
                    return false;
                }
                if (queryComparisonKeyValue == null) {
                    queryComparisonKeyValue = currentQueryComparisonKeyValue;
                } else {
                    final var semanticEquals =
                            queryComparisonKeyValue.semanticEquals(currentQueryComparisonKeyValue,
                                    ValueEquivalence.empty());
                    if (semanticEquals.isFalse()) {
                        return false;
                    }
                    if (!semanticEquals.getConstraint().isTautology()) {
                        return false;
                    }
                }
            }
        }
        return true;
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

    private static TranslationMap computeTranslationMap(@Nonnull final CorrelationIdentifier intersectionAlias,
                                                        @Nonnull final List<? extends Quantifier> quantifiers,
                                                        @Nonnull final List<CorrelationIdentifier> candidateTopAliases,
                                                        @Nonnull final Type.Record intersectionResultType,
                                                        final int numGrouped) {
        final var builder = TranslationMap.builder();
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
