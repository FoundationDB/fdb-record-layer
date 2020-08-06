/*
 * LogicalFilterExpression.java
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
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.ExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpressionWithPredicate;
import com.apple.foundationdb.record.query.plan.temp.explain.Attribute;
import com.apple.foundationdb.record.query.plan.temp.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.temp.view.Source;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A relational planner expression that represents an unimplemented filter on the records produced by its inner
 * relational planner expression.
 * @see com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan for the fallback implementation
 */
@API(API.Status.EXPERIMENTAL)
public class LogicalFilterExpression implements RelationalExpressionWithChildren, RelationalExpressionWithPredicate, PlannerGraphRewritable {
    @Nonnull
    private final Source baseSource;
    @Nonnull
    private final QueryPredicate filter;
    @Nonnull
    private final Quantifier inner;

    public LogicalFilterExpression(@Nonnull Source baseSource,
                                   @Nonnull QueryPredicate filter,
                                   @Nonnull ExpressionRef<RelationalExpression> inner) {
        this.baseSource = baseSource;
        this.filter = filter;
        this.inner = Quantifier.forEach(inner);
    }

    public LogicalFilterExpression(@Nonnull Source baseSource,
                                   @Nonnull QueryPredicate filter,
                                   @Nonnull Quantifier inner) {
        this.baseSource = baseSource;
        this.filter = filter;
        this.inner = inner;
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(inner);
    }

    @Override
    public int getRelationalChildCount() {
        return 1;
    }

    @Nonnull
    @Override
    public QueryPredicate getPredicate() {
        return filter;
    }

    @Nonnull
    public Source getBaseSource() {
        return baseSource;
    }

    @Nonnull
    @VisibleForTesting
    public Quantifier getInner() {
        return inner;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return filter.getCorrelatedTo();
    }

    @Nonnull
    @Override
    public LogicalFilterExpression rebase(@Nonnull final AliasMap translationMap) {
        // we know the following is correct, just Java doesn't
        return (LogicalFilterExpression)RelationalExpressionWithChildren.super.rebase(translationMap);
    }

    @Nonnull
    @Override
    public LogicalFilterExpression rebaseWithRebasedQuantifiers(@Nonnull final AliasMap translationMap,
                                                                @Nonnull final List<Quantifier> rebasedQuantifiers) {
        return new LogicalFilterExpression(getBaseSource(),
                getPredicate().rebase(translationMap),
                Iterables.getOnlyElement(rebasedQuantifiers));
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        return filter.semanticEquals(((LogicalFilterExpression)otherExpression).getPredicate(), equivalencesMap);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object other) {
        return semanticEquals(other);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(getPredicate());
    }

    @Override
    @Nonnull
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.LogicalOperatorNodeWithInfo(
                        NodeInfo.PREDICATE_FILTER_OPERATOR,
                        ImmutableList.of("WHERE {{pred}}"),
                        ImmutableMap.of("pred", Attribute.gml(getPredicate().toString()))),
                childGraphs);
    }
}
