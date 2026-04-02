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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.RecordTypeKeyComparison;
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
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedRecordValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordTypeValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.MaxMatchMap;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.PullUp;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.google.common.base.Predicate;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
    private final Set<String> recordTypes;
    @Nonnull
    private final Quantifier innerQuantifier;
    @Nonnull
    private final Type resultType;
    @Nonnull
    private final List<? extends QueryPredicate> predicates;

    private LogicalTypeFilterExpression(@Nonnull final Set<String> recordTypes, @Nonnull final Quantifier innerQuantifier,
                                        @Nonnull final Type resultType, @Nonnull final List<? extends QueryPredicate> predicates) {
        this.recordTypes = recordTypes;
        this.innerQuantifier = innerQuantifier;
        this.resultType = resultType;
        this.predicates = ImmutableList.copyOf(predicates);
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
        final var translatedPredicates =
                predicates.stream()
                        .map(p -> p.translateCorrelations(translationMap, shouldSimplifyValues))
                        .collect(Collectors.toList());
        return new LogicalTypeFilterExpression(getRecordTypes(), Iterables.getOnlyElement(translatedQuantifiers),
                resultType, translatedPredicates);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other);
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression otherExpression, @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }

        if (getClass() != otherExpression.getClass()) {
            return false;
        }

        final var otherLogicalTypeFilterExpression = (LogicalTypeFilterExpression)otherExpression;
        if (!getRecordTypes().equals(otherLogicalTypeFilterExpression.getRecordTypes())) {
            return false;
        }

        final var otherPredicates = otherLogicalTypeFilterExpression.getPredicates();
        return TypeFilterExpression.super.equalsWithoutChildren(otherExpression, equivalencesMap) &&
                predicates.size() == otherPredicates.size() &&
                Streams.zip(predicates.stream(),
                                otherPredicates.stream(),
                                (queryPredicate, otherQueryPredicate) -> queryPredicate.semanticEquals(otherQueryPredicate, equivalencesMap))
                        .allMatch(isSame -> isSame);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public int computeHashCodeWithoutChildren() {
        if (predicates.isEmpty()) {
            return TypeFilterExpression.super.computeHashCodeWithoutChildren();
        } else {
            return Objects.hash(getRecordTypes(), predicates);
        }
    }

    @Nonnull
    @Override
    @SuppressWarnings("OptionalIsPresent")
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

        final var translatedResultValue =
                getResultValue().translateCorrelations(translationMapOptional.get(), true);

        if (candidateTypeFilterExpression.getPredicates().isEmpty()) {
            return exactlySubsumedBy(candidateExpression, bindingAliasMap, partialMatchMap, translationMapOptional.get());
        } else {
            if (recordTypes.size() > 1 || candidateTypeFilterExpression.getPredicates().size() != 1) {
                // we can't plan queries across multiple types now.
                return ImmutableList.of();
            }
            final String recordTypeName = Iterables.getOnlyElement(recordTypes);
            final QueryPredicate candidatePredicate = Iterables.getOnlyElement(candidateTypeFilterExpression.getPredicates());
            final Comparisons.Comparison comparison = new RecordTypeKeyComparison.RecordTypeComparison(recordTypeName);
            final var translationMap = translationMapOptional.get();
            final QueryPredicate queryPredicate = new ValuePredicate(new RecordTypeValue(QuantifiedRecordValue.of(innerQuantifier)), comparison);
            final QueryPredicate translatedPredicate = queryPredicate.translateCorrelations(translationMap, true);
            final var placeholderAlias = ((Placeholder)candidatePredicate).getParameterAlias();

            final var mapping = PredicateMultiMap.PredicateMapping.regularMappingBuilder(queryPredicate, translatedPredicate, candidatePredicate)
                    .setParameterAlias(placeholderAlias).build();

            final var predicateMappingBuilder = PredicateMultiMap.builder();
            predicateMappingBuilder.put(queryPredicate, mapping);
            final var predicateMappingMaybe = predicateMappingBuilder.buildMaybe();

            if (predicateMappingMaybe.isEmpty()) {
                return ImmutableList.of();
            }

            ComparisonRange comparisonRange = ComparisonRange.from(comparison);

            final var predicateMapping = predicateMappingMaybe.get();
            final var maxMatchMap =
                    MaxMatchMap.compute(translatedResultValue, candidateExpression.getResultValue(),
                            Quantifiers.aliases(candidateExpression.getQuantifiers()), ValueEquivalence.fromAliasMap(bindingAliasMap));
            final var constraints = maxMatchMap.getQueryPlanConstraint();
            return MatchInfo.RegularMatchInfo.tryMerge(bindingAliasMap, partialMatchMap, ImmutableMap.of(placeholderAlias, comparisonRange), predicateMapping, maxMatchMap, GroupByMappings.empty(), ImmutableList.of(), constraints)
                    .map(ImmutableList::of)
                    .orElse(ImmutableList.of());
        }
    }

    @Nonnull
    @Override
    public Compensation compensate(@Nonnull final PartialMatch partialMatch,
                                   @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap,
                                   @Nullable final PullUp pullUp,
                                   @Nonnull final CorrelationIdentifier candidateAlias) {
        final var regularMatchInfo = partialMatch.getRegularMatchInfo();
        final var nestedPullUpPair =
                partialMatch.nestPullUp(pullUp, candidateAlias);
        final var rootOfMatchPullUp = nestedPullUpPair.getKey();
        final var adjustedPullUp = Objects.requireNonNull(nestedPullUpPair.getRight());
        final var bindingAliasMap = regularMatchInfo.getBindingAliasMap();

        final PartialMatch childPartialMatch =
                Objects.requireNonNull(regularMatchInfo
                        .getChildPartialMatchMaybe(innerQuantifier)
                        .orElseThrow(() -> new RecordCoreException("expected a match child")));

        final var childCompensation =
                childPartialMatch.compensate(boundParameterPrefixMap, adjustedPullUp,
                        Objects.requireNonNull(bindingAliasMap.getTarget(innerQuantifier.getAlias())));

        if (childCompensation.isImpossible()) {
            return Compensation.impossibleCompensation();
        }

        final var compensatedResultOptional =
                Compensation.computeResultCompensation(partialMatch, rootOfMatchPullUp);
        if (compensatedResultOptional.isEmpty()) {
            return Compensation.impossibleCompensation();
        }
        final var compensatedResult = compensatedResultOptional.get();
        if (!childCompensation.isNeeded() &&
                !compensatedResult.getResultCompensationFunction().isNeeded()) {
            return Compensation.noCompensation();
        }

        final var unmatchedQuantifiers = partialMatch.getUnmatchedQuantifiers();
        Verify.verify(unmatchedQuantifiers.isEmpty());

        return childCompensation.derived(compensatedResult.isCompensationImpossible(),
                new LinkedIdentityMap<>(),
                getMatchedQuantifiers(partialMatch),
                unmatchedQuantifiers,
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
    public List<? extends QueryPredicate> getPredicates() {
        return predicates;
    }

    @Nonnull
    public static LogicalTypeFilterExpression newInstance(@Nonnull final Set<String> recordTypes, @Nonnull final Quantifier innerQuantifier,
                                                          @Nonnull final Type resultType) {
        return new LogicalTypeFilterExpression(recordTypes, innerQuantifier, resultType, ImmutableList.of());
    }

    @Nonnull
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public static LogicalTypeFilterExpression newInstanceForMatchCandidate(@Nonnull final Set<String> recordTypes,
                                                                           @Nonnull final Set<String> availableRecordTypes,
                                                                           @Nonnull final Quantifier innerQuantifier,
                                                                           @Nonnull final Optional<CorrelationIdentifier> recordTypeKeyAliasMaybe,
                                                                           @Nonnull final Type resultType) {
        Verify.verify(availableRecordTypes.containsAll(recordTypes));

        final var predicateListBuilder = ImmutableList.<QueryPredicate>builder();
        if (recordTypes.size() < availableRecordTypes.size() && recordTypeKeyAliasMaybe.isPresent()) {
            final var recordTypeValue = new RecordTypeValue(QuantifiedRecordValue.of(innerQuantifier));
            final var placeholder = recordTypeValue.asPlaceholder(recordTypeKeyAliasMaybe.get());
            predicateListBuilder.add(placeholder);
        }

        return new LogicalTypeFilterExpression(recordTypes, innerQuantifier, resultType, predicateListBuilder.build());
    }
}
