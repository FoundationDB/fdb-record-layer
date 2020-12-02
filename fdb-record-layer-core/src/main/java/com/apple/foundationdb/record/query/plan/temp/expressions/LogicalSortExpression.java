/*
 * LogicalSortExpressionOld.java
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
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.explain.Attribute;
import com.apple.foundationdb.record.query.plan.temp.explain.InternalPlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.temp.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraph;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A relational planner expression that represents an unimplemented sort on the records produced by its inner
 * relational planner expression.
 */
@API(API.Status.EXPERIMENTAL)
public class LogicalSortExpression implements RelationalExpressionWithChildren, InternalPlannerGraphRewritable {
    @Nonnull
    private final KeyExpression sort;

    private final boolean reverse;

    @Nonnull
    private final Quantifier inner;

    public LogicalSortExpression(@Nonnull final KeyExpression sort,
                                 final boolean reverse,
                                 @Nonnull final RelationalExpression inner) {
        this(sort, reverse, Quantifier.forEach(GroupExpressionRef.of(inner)));
    }

    public LogicalSortExpression(@Nonnull final KeyExpression sort,
                                 final boolean reverse,
                                 @Nonnull final Quantifier inner) {
        this.sort = sort;
        this.reverse = reverse;
        this.inner = inner;
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(getInner());
    }

    @Override
    public int getRelationalChildCount() {
        return 1;
    }

    @Nonnull
    public KeyExpression getSort() {
        return sort;
    }

    public boolean isReverse() {
        return reverse;
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
    public LogicalSortExpression rebase(@Nonnull final AliasMap translationMap) {
        // we know the following is correct, just Java doesn't
        return (LogicalSortExpression)RelationalExpressionWithChildren.super.rebase(translationMap);
    }

    @Nonnull
    @Override
    public LogicalSortExpression rebaseWithRebasedQuantifiers(@Nonnull final AliasMap translationMap,
                                                              @Nonnull final List<Quantifier> rebasedQuantifiers) {
        return new LogicalSortExpression(getSort(),
                isReverse(),
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

        final LogicalSortExpression other = (LogicalSortExpression) otherExpression;

        return reverse == other.reverse && !sort.equals(other.sort);
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
    public int hashCodeWithoutChildren() {
        return Objects.hash(getSort(), isReverse());
    }

    @Nonnull
    @Override
    public PlannerGraph rewriteInternalPlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.LogicalOperatorNodeWithInfo(this,
                        NodeInfo.SORT_OPERATOR,
                        ImmutableList.of("BY {{expression}}"),
                        ImmutableMap.of("expression", Attribute.gml(sort.toString()))),
                childGraphs);
    }
}
