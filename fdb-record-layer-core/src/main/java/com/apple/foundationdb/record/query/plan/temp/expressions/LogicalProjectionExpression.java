/*
 * LogicalProjectionExpression.java
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
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.explain.Attribute;
import com.apple.foundationdb.record.query.plan.temp.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraphRewritable;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A relational planner expression that projects its input values.
 */
@API(API.Status.EXPERIMENTAL)
public class LogicalProjectionExpression implements RelationalExpressionWithChildren, PlannerGraphRewritable {
    @Nonnull
    private final List<? extends Value> projectedValues;
    @Nonnull
    private final Quantifier inner;

    public LogicalProjectionExpression(@Nonnull final List<? extends Value> projectedValues,
                                       @Nonnull final Quantifier inner) {
        this.projectedValues = ImmutableList.copyOf(projectedValues);
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
    @VisibleForTesting
    public Quantifier getInner() {
        return inner;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return projectedValues.stream()
                .flatMap(projectedValue -> projectedValue.getCorrelatedTo().stream())
                .collect(ImmutableSet.toImmutableSet());
    }

    @Nonnull
    @Override
    public LogicalProjectionExpression rebase(@Nonnull final AliasMap translationMap) {
        // we know the following is correct, just Java doesn't
        return (LogicalProjectionExpression)RelationalExpressionWithChildren.super.rebase(translationMap);
    }

    @Nonnull
    @Override
    public LogicalProjectionExpression rebaseWithRebasedQuantifiers(@Nonnull final AliasMap translationMap,
                                                                    @Nonnull final List<Quantifier> rebasedQuantifiers) {
        final List<? extends Value> rebasedValue =
                getProjectedValues().stream()
                        .map(projectedValue -> projectedValue.rebase(translationMap))
                        .collect(ImmutableList.toImmutableList());

        return new LogicalProjectionExpression(rebasedValue,
                Iterables.getOnlyElement(rebasedQuantifiers));
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return inner.getFlowedObjectValue();
    }

    @Nonnull
    public List<? extends Value> getProjectedValues() {
        return projectedValues;
    }

    @SuppressWarnings("UnstableApiUsage")
    @Override
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        final LogicalProjectionExpression otherLogicalProjectionExpression = (LogicalProjectionExpression)otherExpression;
        final List<? extends Value> otherProjectedValues = otherLogicalProjectionExpression.getProjectedValues();
        if (projectedValues.size() != otherProjectedValues.size()) {
            return false;
        }
        return Streams.zip(projectedValues.stream(), otherProjectedValues.stream(),
                        (value, otherValue) -> value.semanticEquals(otherValue, equivalencesMap))
                .allMatch(isSame -> isSame);
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
        return Objects.hash(getResultValue());
    }

    @Override
    @Nonnull
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.LogicalOperatorNodeWithInfo(
                        this,
                        NodeInfo.VALUE_COMPUTATION_OPERATOR,
                        ImmutableList.of("COMPUTE {{values}}"),
                        ImmutableMap.of("values",
                                Attribute.gml(getProjectedValues().toString()))),
                childGraphs);
    }
}
