/*
 * WindowExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.RequestedOrdering;
import com.apple.foundationdb.record.query.plan.cascades.explain.InternalPlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.WindowValue;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A logical window expression that represents applying a window function over partitioned and ordered input tuples.
 */
@API(API.Status.EXPERIMENTAL)
public class WindowExpression extends AbstractRelationalExpressionWithChildren implements InternalPlannerGraphRewritable {

    @Nonnull
    private final WindowValue windowValue;

    @Nonnull
    private final List<Value> partitioningValues;

    @Nonnull
    private final RequestedOrdering requestedOrdering;

    @Nonnull
    private final Quantifier innerQuantifier;

    public WindowExpression(@Nonnull final WindowValue windowValue,
                            @Nonnull final List<Value> partitioningValues,
                            @Nonnull final RequestedOrdering requestedOrdering,
                            @Nonnull final Quantifier innerQuantifier) {
        this.windowValue = windowValue;
        this.partitioningValues = ImmutableList.copyOf(partitioningValues);
        this.requestedOrdering = requestedOrdering;
        this.innerQuantifier = innerQuantifier;
    }

    @Override
    public int getRelationalChildCount() {
        return 1;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        final var builder = ImmutableSet.<CorrelationIdentifier>builder();
        builder.addAll(windowValue.getCorrelatedTo());
        for (final var partitioningValue : partitioningValues) {
            builder.addAll(partitioningValue.getCorrelatedTo());
        }
        return builder.build();
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return windowValue;
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(innerQuantifier);
    }

    @Nonnull
    public Quantifier getInnerQuantifier() {
        return innerQuantifier;
    }

    @Nonnull
    public WindowValue getWindowValue() {
        return windowValue;
    }

    @Nonnull
    public List<Value> getPartitioningValues() {
        return partitioningValues;
    }

    @Nonnull
    public RequestedOrdering getRequestedOrdering() {
        return requestedOrdering;
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression other, @Nonnull final AliasMap equivalences) {
        if (this == other) {
            return true;
        }
        if (getClass() != other.getClass()) {
            return false;
        }
        final var otherWindowExpr = (WindowExpression)other;
        return windowValue.semanticEquals(otherWindowExpr.windowValue, equivalences);
    }

    @Override
    public int computeHashCodeWithoutChildren() {
        return Objects.hash(getResultValue());
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object other) {
        return semanticEquals(other);
    }

    @Nonnull
    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public RelationalExpression translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                      final boolean shouldSimplifyValues,
                                                      @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        Verify.verify(translatedQuantifiers.size() == 1);
        final var translatedWindowValue =
                (WindowValue)windowValue.translateCorrelations(translationMap, shouldSimplifyValues);
        final var translatedPartitioningValues = partitioningValues.stream()
                .map(v -> v.translateCorrelations(translationMap, shouldSimplifyValues))
                .collect(ImmutableList.toImmutableList());
        return new WindowExpression(translatedWindowValue, translatedPartitioningValues, requestedOrdering,
                Iterables.getOnlyElement(translatedQuantifiers));
    }

    @Override
    public String toString() {
        return "Window(" + windowValue + ", partitioning: " + partitioningValues + ", ordering: " + requestedOrdering + ")";
    }

    @Nonnull
    @Override
    public PlannerGraph rewriteInternalPlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.LogicalOperatorNode(this,
                        "WINDOW",
                        List.of("FN {{fn}}", "PARTITION BY {{partitioning}}"),
                        ImmutableMap.of("fn", Attribute.gml(windowValue.toString()),
                                "partitioning", Attribute.gml(partitioningValues.toString()))),
                childGraphs);
    }
}
