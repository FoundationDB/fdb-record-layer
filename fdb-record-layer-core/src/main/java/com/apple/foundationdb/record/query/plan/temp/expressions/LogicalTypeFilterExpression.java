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

package com.apple.foundationdb.record.query.plan.temp.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.ComparisonRange;
import com.apple.foundationdb.record.query.plan.temp.Compensation;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.IdentityBiMap;
import com.apple.foundationdb.record.query.plan.temp.MatchInfo;
import com.apple.foundationdb.record.query.plan.temp.PartialMatch;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.explain.Attribute;
import com.apple.foundationdb.record.query.plan.temp.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraphRewritable;
import com.apple.foundationdb.record.query.predicates.Formatter;
import com.apple.foundationdb.record.query.predicates.QuantifiedColumnValue;
import com.apple.foundationdb.record.query.predicates.Type;
import com.apple.foundationdb.record.query.predicates.Value;
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
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
    private final Supplier<List<? extends Value>> resultValuesSupplier;
    @Nullable
    private final List<? extends Type> resultTypes;

    public LogicalTypeFilterExpression(@Nonnull Set<String> recordTypes, @Nonnull RelationalExpression inner) {
        this(recordTypes, Quantifier.forEach(GroupExpressionRef.of(inner)));
    }

    public LogicalTypeFilterExpression(@Nonnull Set<String> recordTypes, @Nonnull Quantifier inner) {
        this(recordTypes, inner, null);
    }

    public LogicalTypeFilterExpression(@Nonnull Set<String> recordTypes, @Nonnull Quantifier inner, @Nullable final List<? extends Type> resultTypes) {
        this.recordTypes = recordTypes;
        this.inner = inner;
        this.resultTypes = resultTypes == null ? null : ImmutableList.copyOf(resultTypes);
        this.resultValuesSupplier = this::computeResultValues;
    }

    @Nonnull
    public List<? extends Value> computeResultValues() {
        if (resultTypes == null) {
            return inner.getFlowedValues();
        }

        final ImmutableList.Builder<Value> resultBuilder = ImmutableList.builder();
        int i = 0;
        for (final QuantifiedColumnValue value : inner.getFlowedValues()) {
            resultBuilder.add(QuantifiedColumnValue.of(value.getAlias(), value.getOrdinalPosition(), resultTypes.get(i)));
            i ++;
        }

        return resultBuilder.build();
    }

    @Nonnull
    @Override
    public List<? extends Value> getResultValues() {
        return resultValuesSupplier.get();
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

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        if (getQuantifiers().size() == 1) {
            final RelationalExpression innerExpression = Iterables.getOnlyElement(Iterables.getOnlyElement(getQuantifiers()).getRangesOver().getMembers());
            if (innerExpression instanceof FullUnorderedScanExpression) {
                return "from(" + getRecordTypes().stream().map(recordType -> "'" + recordType + "'").collect(Collectors.joining(", ")) + ")";
            }
        }

        throw new IllegalStateException("unknown graph structure");
    }

    @Override
    public int getRelationalChildCount() {
        return 1;
    }

    @Nonnull
    private Quantifier getInner() {
        return inner;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public LogicalTypeFilterExpression rebase(@Nonnull final AliasMap translationMap) {
        // we know the following is correct, just Java doesn't
        return (LogicalTypeFilterExpression)TypeFilterExpression.super.rebase(translationMap);
    }

    @Override
    @Nonnull
    public LogicalTypeFilterExpression rebaseWithRebasedQuantifiers(@Nonnull final AliasMap translationMap,
                                                                    @Nonnull final List<Quantifier> rebasedQuantifiers) {
        return new LogicalTypeFilterExpression(getRecordTypes(),
                Iterables.getOnlyElement(rebasedQuantifiers));
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
    public Iterable<MatchInfo> subsumedBy(@Nonnull final RelationalExpression candidateExpression, @Nonnull final AliasMap aliasMap, @Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap) {
        return exactlySubsumedBy(candidateExpression, aliasMap, partialMatchMap);
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
