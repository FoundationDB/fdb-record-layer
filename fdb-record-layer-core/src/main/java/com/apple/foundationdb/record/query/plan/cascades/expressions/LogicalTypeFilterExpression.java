/*
 * LogicalTypeFilterExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.Compensation;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.GroupByMappings;
import com.apple.foundationdb.record.query.plan.cascades.IdentityBiMap;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentityMap;
import com.apple.foundationdb.record.query.plan.cascades.MatchInfo;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.ValueEquivalence;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.predicates.Placeholder;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValueAndRanges;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.RangeConstraints;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordTypeValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.expressions.RecordTypeKeyComparison;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.MaxMatchMap;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.PullUp;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A relational planner expression that represents an unimplemented type filter on the records produced by its inner
 * relational planner expression.
 * @see com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan for the fallback implementation
 */
@API(API.Status.EXPERIMENTAL)
public class LogicalTypeFilterExpression extends AbstractRelationalExpressionWithChildren implements TypeFilterExpression, PlannerGraphRewritable {
    @Nonnull
    private final QueryPredicate recordTypePredicate;
    @Nonnull
    private final Set<String> recordTypes;
    @Nonnull
    private final Quantifier innerQuantifier;
    @Nonnull
    private final Type resultType;

    public LogicalTypeFilterExpression(@Nonnull QueryPredicate recordTypePredicate, @Nonnull Set<String> recordTypes,
                                       @Nonnull Quantifier innerQuantifier, @Nonnull Type resultType) {
        this.recordTypePredicate = recordTypePredicate;
        this.recordTypes = ImmutableSet.copyOf(recordTypes);
        this.innerQuantifier = innerQuantifier;
        this.resultType = resultType;
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return QuantifiedObjectValue.of(innerQuantifier.getAlias(), resultType);
    }

    @Override
    @Nonnull
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(getInnerQuantifier());
    }

    @Override
    @Nonnull
    public Set<String> getRecordTypes() {
        return recordTypes;
    }

    @Nonnull
    public QueryPredicate getRecordTypePredicate() {
        return recordTypePredicate;
    }

    @Override
    public int getRelationalChildCount() {
        return 1;
    }

    @Nonnull
    public Quantifier getInnerQuantifier() {
        return innerQuantifier;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public LogicalTypeFilterExpression translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                             final boolean shouldSimplifyValues,
                                                             @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        final var translatedPredicates = recordTypePredicate.translateCorrelations(translationMap, shouldSimplifyValues);
        return new LogicalTypeFilterExpression(translatedPredicates, getRecordTypes(), Iterables.getOnlyElement(translatedQuantifiers),
                resultType);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public int computeHashCodeWithoutChildren() {
        return TypeFilterExpression.super.computeHashCodeWithoutChildren();
    }

    @Nonnull
    @Override
    public Iterable<MatchInfo> subsumedBy(@Nonnull final RelationalExpression candidateExpression,
                                          @Nonnull final AliasMap bindingAliasMap,
                                          @Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap,
                                          @Nonnull final EvaluationContext evaluationContext) {
        // the candidate must be a type filter expression.
        if (candidateExpression.getClass() != this.getClass()) {
            return ImmutableList.of();
        }

        if (!isCompatiblyAndCompletelyBound(bindingAliasMap, candidateExpression.getQuantifiers())) {
            return ImmutableList.of();
        }

        final var candidateTypeFilterExpression = (LogicalTypeFilterExpression)candidateExpression;
        final var candidateInnerQuantifier = candidateTypeFilterExpression.getInnerQuantifier();

        if (!(innerQuantifier instanceof Quantifier.ForEach)) {
            return ImmutableList.of();
        }

        final var candidateAlias = bindingAliasMap.getTarget(innerQuantifier.getAlias());
        if (candidateAlias == null) {
            return ImmutableList.of();
        }
        Verify.verify(candidateAlias.equals(candidateInnerQuantifier.getAlias()));
        if (!(candidateInnerQuantifier instanceof Quantifier.ForEach)) {
            return ImmutableList.of();
        }
        if (((Quantifier.ForEach)innerQuantifier).isNullOnEmpty() !=
                ((Quantifier.ForEach)candidateInnerQuantifier).isNullOnEmpty()) {
            return ImmutableList.of();
        }

        final var translationMapOptional =
                RelationalExpression.pullUpAndComposeTranslationMapsMaybe(candidateExpression, bindingAliasMap, partialMatchMap);
        if (translationMapOptional.isEmpty()) {
            return ImmutableList.of();
        }

        final var translationMap = translationMapOptional.get();
        final var translatedPredicate = recordTypePredicate.translateCorrelations(translationMap, true);

        final var bindingValueEquivalence =
                ValueEquivalence.fromAliasMap(bindingAliasMap)
                        .then(ValueEquivalence.constantEquivalenceWithEvaluationContext(evaluationContext));

        PredicateMultiMap.PredicateMapping impliedMappingForPredicate =
                Iterables.getOnlyElement(translatedPredicate.findImpliedMappings(bindingValueEquivalence, recordTypePredicate,
                        ImmutableList.of(candidateTypeFilterExpression.getRecordTypePredicate()), evaluationContext));

        if (impliedMappingForPredicate.getCandidatePredicate().isTautology()) {
            return ImmutableList.of();
        }

        final var predicateMapBuilder = PredicateMultiMap.builder();
        final var originalQueryPredicate = impliedMappingForPredicate.getOriginalQueryPredicate();
        predicateMapBuilder.put(originalQueryPredicate, impliedMappingForPredicate);

        final var parameterAliasOptional = impliedMappingForPredicate.getParameterAliasOptional();
        final var comparisonRangeOptional = impliedMappingForPredicate.getComparisonRangeOptional();
        final var parameterBindingMap = Maps.<CorrelationIdentifier, ComparisonRange>newHashMap();
        if (parameterAliasOptional.isPresent() &&
                comparisonRangeOptional.isPresent()) {
            parameterBindingMap.put(parameterAliasOptional.get(), comparisonRangeOptional.get());
        }

        final var predicateMapOptional = predicateMapBuilder.buildMaybe();
        if (predicateMapOptional.isEmpty()) {
            return ImmutableList.of();
        }

        final var matchInfos = PartialMatch.matchInfosFromMap(partialMatchMap);

        // merge parameter maps -- early out if a binding clashes
        final var parameterBindingMaps =
                matchInfos
                        .stream()
                        .map(MatchInfo::getRegularMatchInfo)
                        .map(MatchInfo.RegularMatchInfo::getParameterBindingMap)
                        .collect(ImmutableList.toImmutableList());
        final var mergedParameterBindingMapOptional =
                MatchInfo.RegularMatchInfo.tryMergeParameterBindings(parameterBindingMaps);
        if (mergedParameterBindingMapOptional.isEmpty()) {
            return ImmutableList.of();
        }
        final var mergedParameterBindingMap = mergedParameterBindingMapOptional.get();


        final Optional<Map<CorrelationIdentifier, ComparisonRange>> allParameterBindingMapOptional =
                MatchInfo.RegularMatchInfo.tryMergeParameterBindings(ImmutableList.of(mergedParameterBindingMap, parameterBindingMap));
        final var translatedResultValue =
                getResultValue().translateCorrelations(translationMap, true);
        return allParameterBindingMapOptional
                .flatMap(allParameterBindingMap -> {
                    final var maxMatchMap =
                            MaxMatchMap.compute(translatedResultValue,
                                    candidateExpression.getResultValue(),
                                    Quantifiers.aliases(candidateExpression.getQuantifiers()),
                                    bindingValueEquivalence);
                    return MatchInfo.RegularMatchInfo.tryMerge(bindingAliasMap, partialMatchMap,
                            allParameterBindingMap, predicateMapOptional.get(),
                            maxMatchMap, GroupByMappings.empty(), null,
                            maxMatchMap.getQueryPlanConstraint());
                })
                .map(ImmutableList::of)
                .orElse(ImmutableList.of());
    }

    @Nonnull
    @Override
    public Compensation compensate(@Nonnull final PartialMatch partialMatch,
                                   @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap,
                                   @Nullable final PullUp pullUp,
                                   @Nonnull final CorrelationIdentifier candidateAlias) {
        final var predicateCompensationMap = new LinkedIdentityMap<QueryPredicate, PredicateMultiMap.PredicateCompensationFunction>();
        final var regularMatchInfo = partialMatch.getRegularMatchInfo();
        final var quantifiers = getQuantifiers();

        final var nestedPullUpPair =
                partialMatch.nestPullUp(pullUp, candidateAlias);
        final var rootOfMatchPullUp = nestedPullUpPair.getKey();
        final var adjustedPullUp = Objects.requireNonNull(nestedPullUpPair.getRight());

        //
        // The partial match we are called with here has child matches that have compensations on their own.
        // Given a pair of these matches that we reach along two for-each quantifiers (forming a join) we have to
        // apply both compensations. The compensation class has a union method to combine two compensations in an
        // optimal way. We need to fold over all those compensations to form one child compensation. The tree that
        // is formed by partial matches therefore collapses into a chain of compensations.
        //
        final Compensation childCompensation = quantifiers
                .stream()
                .filter(quantifier -> quantifier instanceof Quantifier.ForEach)
                .flatMap(quantifier ->
                        regularMatchInfo.getChildPartialMatchMaybe(quantifier)
                                .map(childPartialMatch -> {
                                    final var bindingAliasMap = regularMatchInfo.getBindingAliasMap();
                                    return childPartialMatch.compensate(boundParameterPrefixMap, adjustedPullUp,
                                            Objects.requireNonNull(bindingAliasMap.getTarget(quantifier.getAlias())));
                                }).stream())
                .reduce(Compensation.noCompensation(), Compensation::union);

        if (childCompensation.isImpossible() ||
                !childCompensation.canBeDeferred()) {
            return Compensation.impossibleCompensation();
        }

        final var predicateMap = regularMatchInfo.getPredicateMap();
        final var unmatchedQuantifiers = partialMatch.getUnmatchedQuantifiers();
        final var unmatchedQuantifierAliases =
                unmatchedQuantifiers.stream().map(Quantifier::getAlias).collect(ImmutableList.toImmutableList());
        boolean isAnyCompensationFunctionImpossible = false;
        boolean isAnyCompensationFunctionNeeded = false;

        //
        // Go through all predicates and invoke the reapplication logic for each associated mapping. Remember, each
        // predicate MUST have a mapping to the other side (which may just be a tautology). If something needs to be
        // reapplied that logic creates the correct predicates. The reapplication logic is also passed enough context
        // to skip reapplication in which case we won't do anything when compensation needs to be applied.
        //
        final var predicate = recordTypePredicate;
        final var predicateMappings = predicateMap.get(predicate);
        if (!predicateMappings.isEmpty()) {
            if (predicate.getCorrelatedTo().stream().anyMatch(unmatchedQuantifierAliases::contains)) {
                isAnyCompensationFunctionImpossible = true;
            }

            //
            // This logic follows the same approach used in Compensation::intersect where duplicate mappings can
            // arise from different partial matches when their compensations are intersected.
            // We use the first compensation we encounter (with the same reasoning as given in
            // Compensation::intersect). If we find a mapping that does not need a compensation to be applied
            // (in all reality those mappings are mappings to tautological placeholders), we do not need
            // compensation for this query predicate at all.
            //
            PredicateMultiMap.PredicateCompensationFunction compensationFunction = null;
            boolean isCompensationFunctionNeeded = true;
            boolean isCompensationFunctionImpossible = true;
            for (final var predicateMapping : predicateMappings) {
                final var compensationFunctionForCandidatePredicate =
                        predicateMapping.getPredicateCompensation()
                                .computeCompensationFunction(partialMatch, boundParameterPrefixMap, adjustedPullUp);
                if (!compensationFunctionForCandidatePredicate.isNeeded()) {
                    isCompensationFunctionNeeded = false;
                    break;
                }

                if (compensationFunction == null) {
                    compensationFunction = compensationFunctionForCandidatePredicate;
                }

                if (!compensationFunctionForCandidatePredicate.isImpossible()) {
                    isCompensationFunctionImpossible = false;
                }
            }

            if (isCompensationFunctionNeeded) {
                isAnyCompensationFunctionNeeded = true;
                if (isCompensationFunctionImpossible) {
                    isAnyCompensationFunctionImpossible = true;
                }
                predicateCompensationMap.put(predicate, Objects.requireNonNull(compensationFunction));
            }
        }

        final var compensatedResultOptional =
                Compensation.computeResultCompensation(partialMatch, rootOfMatchPullUp);
        if (compensatedResultOptional.isEmpty()) {
            return Compensation.impossibleCompensation();
        }
        final var compensatedResult = compensatedResultOptional.get();
        isAnyCompensationFunctionImpossible |= compensatedResult.isCompensationImpossible();

        final var isCompensationNeeded =
                childCompensation.isNeeded() ||
                        !unmatchedQuantifiers.isEmpty() ||
                        isAnyCompensationFunctionNeeded ||
                        compensatedResult.getResultCompensationFunction().isNeeded();

        if (!isCompensationNeeded) {
            return Compensation.noCompensation();
        }

        //
        // We now know we need compensation, and if we have more than one quantifier, we would have to translate
        // the references of the values from the query graph to values operating on the MQT in order to do that
        // compensation. We cannot do that (yet). If we, however, do not have to worry about compensation we just do
        // this select entirely with the scan.
        //
        final var partialMatchMap = regularMatchInfo.getPartialMatchMap();
        if (quantifiers.stream()
                .filter(quantifier -> quantifier instanceof Quantifier.ForEach &&
                        partialMatchMap.containsKeyUnwrapped(quantifier))
                .count() > 1) {
            return Compensation.impossibleCompensation();
        }

        return childCompensation.derived(isAnyCompensationFunctionImpossible,
                predicateCompensationMap,
                getMatchedQuantifiers(partialMatch),
                partialMatch.getUnmatchedQuantifiers(),
                partialMatch.getCompensatedAliases(),
                compensatedResult.getResultCompensationFunction(),
                compensatedResult.getGroupByMappings());
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.LogicalOperatorNodeWithInfo(this,
                        NodeInfo.TYPE_FILTER_OPERATOR,
                        ImmutableList.of("WHERE record IS {{types}}"),
                        ImmutableMap.of("types", Attribute.gml(getRecordTypes().stream().map(Attribute::gml).collect(ImmutableList.toImmutableList())))),
                childGraphs);
    }

    @Nonnull
    public static LogicalTypeFilterExpression newInstanceForMatchCandidate(@Nonnull final Set<String> recordTypes,
                                                                           @Nonnull final Quantifier innerQuantifier,
                                                                           @Nonnull final Type resultType,
                                                                           @Nullable final CorrelationIdentifier recordTypeKeyParameterAlias) {
        final var value = new RecordTypeValue(QuantifiedObjectValue.of(innerQuantifier));
        final var rangeConstraints = recordTypeNamesToRangeConstraints(recordTypes);

        //
        // if the record type parameter alias is provided, create a placeholder with that same parameter alias,
        // otherwise, create a predicate resembling an index filter.
        //
        final QueryPredicate predicate;
        if (recordTypeKeyParameterAlias != null) {
            predicate = Placeholder.newInstance(value, rangeConstraints, recordTypeKeyParameterAlias);
        } else {
            predicate = PredicateWithValueAndRanges.ofRanges(value, rangeConstraints);
        }

        return new LogicalTypeFilterExpression(predicate, recordTypes, innerQuantifier, resultType);
    }

    @Nonnull
    public static LogicalTypeFilterExpression newInstance(@Nonnull final Set<String> recordTypes,
                                                          @Nonnull final Quantifier innerQuantifier,
                                                          @Nonnull final Type resultType) {
        final var value = new RecordTypeValue(QuantifiedObjectValue.of(innerQuantifier));
        final var rangeConstraints = recordTypeNamesToRangeConstraints(recordTypes);

        final var recordTypePredicate = PredicateWithValueAndRanges.ofRanges(value, rangeConstraints);
        return new LogicalTypeFilterExpression(recordTypePredicate, recordTypes, innerQuantifier, resultType);
    }

    @Nonnull
    private static Set<RangeConstraints> recordTypeNamesToRangeConstraints(@Nonnull final Set<String> recordTypeNames) {
        return recordTypeNames.stream().flatMap(recordTypeName -> {
            final var rangeConstraintBuilder = RangeConstraints.newBuilder();
            rangeConstraintBuilder.addComparisonMaybe(new RecordTypeKeyComparison(recordTypeName).getComparison());
            return rangeConstraintBuilder.build().stream();
        }).collect(Collectors.toCollection(LinkedHashSet::new));
    }
}
