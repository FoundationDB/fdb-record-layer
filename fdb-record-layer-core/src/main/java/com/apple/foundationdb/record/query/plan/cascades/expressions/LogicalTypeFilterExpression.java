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
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap;
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
public class LogicalTypeFilterExpression implements TypeFilterExpression, PlannerGraphRewritable {
    @Nonnull
    private final Set<String> recordTypes;
    @Nonnull
    private final Quantifier inner;
    @Nonnull
    private final Type resultType;

    public LogicalTypeFilterExpression(@Nonnull Set<String> recordTypes, @Nonnull Quantifier inner, @Nonnull Type resultType) {
        this.recordTypes = recordTypes;
        this.inner = inner;
        this.resultType = resultType;
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return QuantifiedObjectValue.of(inner.getAlias(), resultType);
    }

    @Override
    @Nonnull
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(getInner());
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
    public Quantifier getInner() {
        return inner;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public LogicalTypeFilterExpression translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                             final boolean shouldSimplifyValues,
                                                             @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new LogicalTypeFilterExpression(getRecordTypes(),
                Iterables.getOnlyElement(translatedQuantifiers),
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

    @Nonnull
    @Override
    @SuppressWarnings("OptionalIsPresent")
    public Iterable<MatchInfo> subsumedBy(@Nonnull final RelationalExpression candidateExpression,
                                          @Nonnull final AliasMap bindingAliasMap,
                                          @Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap,
                                          @Nonnull final EvaluationContext evaluationContext) {
        if (!isCompatiblyAndCompletelyBound(bindingAliasMap, candidateExpression.getQuantifiers())) {
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
                                   @Nonnull final CorrelationIdentifier nestingAlias) {
        final var matchInfo = partialMatch.getMatchInfo();
        final var regularMatchInfo = partialMatch.getRegularMatchInfo();
        final var adjustedPullUp = partialMatch.nestPullUpForAdjustments(pullUp, nestingAlias);
        final var bindingAliasMap = regularMatchInfo.getBindingAliasMap();

        final PartialMatch childPartialMatch =
                Objects.requireNonNull(regularMatchInfo
                        .getChildPartialMatchMaybe(inner)
                        .orElseThrow(() -> new RecordCoreException("expected a match child")));

        final var childCompensation =
                childPartialMatch.compensate(boundParameterPrefixMap, adjustedPullUp,
                        Objects.requireNonNull(bindingAliasMap.getTarget(inner.getAlias())));

        if (childCompensation.isImpossible() ||
                childCompensation.isNeededForFiltering()) {
            return Compensation.impossibleCompensation();
        }

        final PredicateMultiMap.ResultCompensationFunction resultCompensationFunction;
        if (pullUp != null) {
            resultCompensationFunction = PredicateMultiMap.ResultCompensationFunction.noCompensationNeeded();
        } else {
            final var rootPullUp = adjustedPullUp.getRootPullUp();
            final var maxMatchMap = matchInfo.getMaxMatchMap();
            final var pulledUpResultValueOptional =
                    rootPullUp.pullUpMaybe(maxMatchMap.getQueryResultValue());
            if (pulledUpResultValueOptional.isEmpty()) {
                return Compensation.impossibleCompensation();
            }

            final var pulledUpResultValue = pulledUpResultValueOptional.get();

            resultCompensationFunction =
                    PredicateMultiMap.ResultCompensationFunction.of(baseAlias -> pulledUpResultValue.translateCorrelations(
                            TranslationMap.ofAliases(rootPullUp.getNestingAlias(), baseAlias), false));
        }

        final var unmatchedQuantifiers = partialMatch.getUnmatchedQuantifiers();
        Verify.verify(unmatchedQuantifiers.isEmpty());

        if (!resultCompensationFunction.isNeeded()) {
            return Compensation.noCompensation();
        }

        return childCompensation.derived(false,
                new LinkedIdentityMap<>(),
                getMatchedQuantifiers(partialMatch),
                unmatchedQuantifiers,
                partialMatch.getCompensatedAliases(),
                resultCompensationFunction);
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
