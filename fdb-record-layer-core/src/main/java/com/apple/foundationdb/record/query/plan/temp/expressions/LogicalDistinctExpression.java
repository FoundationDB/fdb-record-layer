/*
 * LogicalDistinctPlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.explain.InternalPlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.temp.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraph;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;

/**
 * A relational planner expression representing a stream of unique records. This expression has a single child which
 * is also a {@link RelationalExpression}. This expression represents this underlying expression with its result
 * set de-duplicated.
 *
 * @see com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan for the fallback implementation
 */
@API(API.Status.EXPERIMENTAL)
public class LogicalDistinctExpression implements RelationalExpressionWithChildren, InternalPlannerGraphRewritable {
    @Nonnull
    private final Quantifier inner;

    public LogicalDistinctExpression(@Nonnull RelationalExpression inner) {
        this(GroupExpressionRef.of(inner));
    }

    public LogicalDistinctExpression(@Nonnull ExpressionRef<RelationalExpression> innerRef) {
        this(Quantifier.forEach(innerRef));
    }

    public LogicalDistinctExpression(@Nonnull Quantifier inner) {
        this.inner = inner;
    }

    @Override
    public int getRelationalChildCount() {
        return 1;
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(inner);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public LogicalDistinctExpression rebase(@Nonnull final AliasMap translationMap) {
        // we know the following is correct, just Java doesn't
        return (LogicalDistinctExpression)RelationalExpressionWithChildren.super.rebase(translationMap);
    }

    @Nonnull
    @Override
    public LogicalDistinctExpression rebaseWithRebasedQuantifiers(@Nonnull final AliasMap translationMap,
                                                                  @Nonnull final List<Quantifier> rebasedChildren) {
        return new LogicalDistinctExpression(Iterables.getOnlyElement(rebasedChildren));
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return inner.getFlowedObjectValue();
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression otherExpression, @Nonnull final AliasMap equivalences) {
        if (this == otherExpression) {
            return true;
        }
        return getClass() == otherExpression.getClass();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return 31;
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Nonnull
    @Override
    public PlannerGraph rewriteInternalPlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.LogicalOperatorNodeWithInfo(this,
                        NodeInfo.UNORDERED_DISTINCT_OPERATOR,
                        ImmutableList.of(),
                        ImmutableMap.of()),
                childGraphs);
    }
}
