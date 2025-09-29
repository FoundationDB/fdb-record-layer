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
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.Compensation;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.IdentityBiMap;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentityMap;
import com.apple.foundationdb.record.query.plan.cascades.MatchInfo;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.PullUp;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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

    public LogicalTypeFilterExpression(@Nonnull Set<String> recordTypes, @Nonnull Quantifier innerQuantifier, @Nonnull Type resultType) {
        this.recordTypes = recordTypes;
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
        return new LogicalTypeFilterExpression(getRecordTypes(), Iterables.getOnlyElement(translatedQuantifiers),
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

        return exactlySubsumedBy(candidateExpression, bindingAliasMap, partialMatchMap, translationMapOptional.get());
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
}
