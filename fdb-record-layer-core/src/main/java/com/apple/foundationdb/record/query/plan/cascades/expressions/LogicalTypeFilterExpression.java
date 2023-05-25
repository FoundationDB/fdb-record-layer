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
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.IdentityBiMap;
import com.apple.foundationdb.record.query.plan.cascades.MatchInfo;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMap;
import com.apple.foundationdb.record.query.plan.cascades.PredicateMultiMap;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.predicates.Placeholder;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordTypeValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
    @Nonnull
    private final Set<QueryPredicate> predicates;

    public LogicalTypeFilterExpression(@Nonnull Set<String> recordTypes, @Nonnull RelationalExpression inner, @Nonnull Type resultType) {
        this(recordTypes, Quantifier.forEach(GroupExpressionRef.of(inner)), resultType);
    }

    public LogicalTypeFilterExpression(@Nonnull Set<String> recordTypes, @Nonnull Quantifier inner, @Nonnull Type resultType) {
        this(recordTypes, inner, resultType, ImmutableSet.of());
    }

    public LogicalTypeFilterExpression(@Nonnull Set<String> recordTypes, @Nonnull Quantifier inner, @Nonnull Type resultType, @Nonnull Set<QueryPredicate> predicates) {
        this.recordTypes = recordTypes;
        this.inner = inner;
        this.resultType = resultType;
        this.predicates = ImmutableSet.copyOf(predicates);
    }

    @Nonnull
    public LogicalTypeFilterExpression withPredicate(@Nonnull QueryPredicate predicate) {
        if (predicates.contains(predicate)) {
            return this;
        }
        var builder = ImmutableSet.<QueryPredicate>builderWithExpectedSize(predicates.size() + 1);
        builder.addAll(predicates);
        builder.add(predicate);
        return new LogicalTypeFilterExpression(recordTypes, inner, resultType, builder.build());
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
    public LogicalTypeFilterExpression translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
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
    public Iterable<MatchInfo> subsumedBy(@Nonnull final RelationalExpression candidateExpression,
                                          @Nonnull final AliasMap aliasMap,
                                          @Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap,
                                          @Nonnull final EvaluationContext evaluationContext) {
        final Iterable<MatchInfo> subsumedMatches = exactlySubsumedBy(candidateExpression, aliasMap, partialMatchMap);
        if (!(candidateExpression instanceof LogicalTypeFilterExpression)) {
            return subsumedMatches;
        }
        LogicalTypeFilterExpression typeFilterExpression = (LogicalTypeFilterExpression) candidateExpression;
        if (typeFilterExpression.predicates.stream().noneMatch(predicate -> predicate instanceof Placeholder) || recordTypes.size() != 1) {
            return subsumedMatches;
        }

        // Bind any predicate that can be bound to the record type key
        final String recordTypeName = Iterables.getOnlyElement(recordTypes);
        Comparisons.Comparison comparison = new RecordTypeKeyComparison.RecordTypeComparison(recordTypeName);
        Optional<ComparisonRange> comparisonRangeMaybe = ComparisonRange.from(comparison);
        if (comparisonRangeMaybe.isEmpty()) {
            return subsumedMatches;
        }
        ComparisonRange comparisonRange = comparisonRangeMaybe.get();
        QueryPredicate predicate = new ValuePredicate(new RecordTypeValue(inner.getAlias()), comparison);
        var predicateMapBuilder = PredicateMap.builder();
        var rangeMapBuilder = ImmutableMap.<CorrelationIdentifier, ComparisonRange>builder();
        for (QueryPredicate candidatePredicate : ((LogicalTypeFilterExpression)candidateExpression).predicates) {
            if (!(candidatePredicate instanceof Placeholder)) {
                continue;
            }
            Placeholder candidatePlaceholder = (Placeholder) candidatePredicate;
            Value candidatePredicateValue = candidatePlaceholder.getValue();
            if (!(candidatePredicateValue instanceof RecordTypeValue)) {
                continue;
            }
            RecordTypeValue candidateRecordTypeValue = (RecordTypeValue) candidatePredicateValue;
            if (!aliasMap.getSource(candidateRecordTypeValue.getAlias()).equals(inner.getAlias())) {
                continue;
            }
            predicateMapBuilder.put(predicate, candidatePredicate, PredicateMultiMap.CompensatePredicateFunction.noCompensationNeeded(), ((Placeholder)candidatePredicate).getParameterAlias());
            rangeMapBuilder.put(candidatePlaceholder.getParameterAlias(), comparisonRange);
        }

        return predicateMapBuilder.buildMaybe()
                .flatMap(predicateMap -> MatchInfo.tryMerge(partialMatchMap, rangeMapBuilder.build(), predicateMap, Optional.empty()))
                .map(ImmutableList::of)
                .orElse(ImmutableList.of());
    }

    @Override
    public Compensation compensate(@Nonnull final PartialMatch partialMatch, @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap) {
        final PartialMatch childPartialMatch = Objects.requireNonNull(partialMatch.getMatchInfo().getChildPartialMatch(inner).orElseThrow(() -> new RecordCoreException("expected a match child")));
        return childPartialMatch.compensate(boundParameterPrefixMap);
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
