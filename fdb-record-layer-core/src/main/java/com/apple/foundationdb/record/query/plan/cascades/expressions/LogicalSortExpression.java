/*
 * LogicalSortExpression.java
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
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.InternalPlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A relational planner expression that represents an unimplemented sort on the records produced by its inner
 * relational planner expression.
 */
@API(API.Status.EXPERIMENTAL)
public class LogicalSortExpression implements RelationalExpressionWithChildren, InternalPlannerGraphRewritable {
    @Nonnull
    private final List<LogicalSortValue> sortValues;

    @Nonnull
    private final Quantifier inner;

    public LogicalSortExpression(@Nonnull List<LogicalSortValue> sortValues, @Nonnull Quantifier inner) {
        this.sortValues = sortValues;
        this.inner = inner;
    }

    public LogicalSortExpression(@Nonnull List<Value> sortValues,
                                 final boolean reverse,
                                 @Nonnull final Quantifier inner) {
        this(sortValues.stream().map(value -> new LogicalSortValue(value, reverse)).collect(Collectors.toList()), inner);
    }

    @Nonnull
    public static LogicalSortExpression unsorted(@Nonnull final Quantifier inner) {
        return new LogicalSortExpression(Collections.emptyList(), inner);
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
    public List<LogicalSortValue> getSortValues() {
        return sortValues;
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
    public LogicalSortExpression translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                       @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new LogicalSortExpression(getSortValues(),
                Iterables.getOnlyElement(translatedQuantifiers));
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return inner.getFlowedObjectValue();
    }

    @Override
    public boolean semanticEquals(@Nullable final Object other, @Nonnull final AliasMap aliasMap) {
        if (!RelationalExpressionWithChildren.super.semanticEquals(other, aliasMap)) {
            return false;
        }
        LogicalSortExpression otherSort = (LogicalSortExpression)other;
        if (sortValues.size() != otherSort.sortValues.size()) {
            return false;
        }
        for (int i = 0; i < sortValues.size(); i++) {
            if (!sortValues.get(i).semanticEquals(otherSort.sortValues.get(i), aliasMap)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int semanticHashCode() {
        return RelationalExpressionWithChildren.super.semanticHashCode() + sortValues.hashCode();
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }

        if (getClass() != otherExpression.getClass()) {
            return false;
        }

        final LogicalSortExpression other = (LogicalSortExpression) otherExpression;
        return sortValues.equals(other.sortValues);
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
        return sortValues.hashCode();
    }

    @Nonnull
    @Override
    public PlannerGraph rewriteInternalPlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        if (sortValues.isEmpty()) {
            return PlannerGraph.fromNodeAndChildGraphs(
                    new PlannerGraph.LogicalOperatorNodeWithInfo(this,
                            NodeInfo.SORT_OPERATOR,
                            ImmutableList.of("PRESERVE ORDER"),
                            ImmutableMap.of()),
                    childGraphs);
        } else {
            return PlannerGraph.fromNodeAndChildGraphs(
                    new PlannerGraph.LogicalOperatorNodeWithInfo(this,
                            NodeInfo.SORT_OPERATOR,
                            ImmutableList.of("BY {{expression}}"),
                            ImmutableMap.of("expression", Attribute.gml(sortValues.stream().map(LogicalSortValue::toString).collect(Collectors.joining(", "))))),
                    childGraphs);

        }
    }
}
