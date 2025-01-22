/*
 * LogicalIntersectionExpression.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.ProvidedOrderingPart;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifiers;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.InternalPlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.plans.RecordQuerySetPlan;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A query plan that executes by taking the union of records from two or more compatibly-sorted child plans.
 * To work, each child cursor must order its children the same way according to the comparison key.
 */
@API(API.Status.INTERNAL)
public class LogicalIntersectionExpression implements RelationalExpressionWithChildren.ChildrenAsSet, InternalPlannerGraphRewritable {
    public static final Logger LOGGER = LoggerFactory.getLogger(LogicalIntersectionExpression.class);

    private static final String INTERSECT = "∩"; // U+2229
    /* The current implementations of equals() and hashCode() treat RecordQueryIntersectionPlan as if it were isomorphic under
     * a reordering of its children. In particular, all of the tests assume that a RecordQueryIntersectionPlan with its children
     * reordered is identical. This is accurate in the current implementation (except that the continuation might no longer
     * be valid); if this ever changes, equals() and hashCode() must be updated.
     */
    @Nonnull
    private final List<Quantifier.ForEach> quantifiers;
    @Nonnull
    private final List<ProvidedOrderingPart> comparisonKeyProvidedOrderingParts;
    @Nonnull
    private final Value resultValue;

    @SuppressWarnings("PMD.UnusedFormalParameter")
    private LogicalIntersectionExpression(@Nonnull List<Quantifier.ForEach> quantifiers,
                                          @Nonnull List<ProvidedOrderingPart> comparisonKeyProvidedOrderingParts) {
        this.quantifiers = ImmutableList.copyOf(quantifiers);
        this.comparisonKeyProvidedOrderingParts = ImmutableList.copyOf(comparisonKeyProvidedOrderingParts);
        this.resultValue = RecordQuerySetPlan.mergeValues(quantifiers);
    }

    @Nonnull
    public List<ProvidedOrderingPart> getComparisonKeyProvidedOrderingParts() {
        return comparisonKeyProvidedOrderingParts;
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return quantifiers;
    }

    @Nonnull
    @Override
    public String toString() {
        return quantifiers.stream().map(Quantifier::toString).collect(Collectors.joining(" " + INTERSECT + " "));
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public LogicalIntersectionExpression translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                               final boolean shouldSimplifyValues,
                                                               @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return new LogicalIntersectionExpression(
                Quantifiers.narrow(Quantifier.ForEach.class, translatedQuantifiers),
                getComparisonKeyProvidedOrderingParts());
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return resultValue;
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
        final LogicalIntersectionExpression other = (LogicalIntersectionExpression) otherExpression;
        return comparisonKeyProvidedOrderingParts.equals(other.comparisonKeyProvidedOrderingParts);
    }

    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(final Object other) {
        return semanticEquals(other);
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return getComparisonKeyProvidedOrderingParts().hashCode();
    }

    @Override
    public int getRelationalChildCount() {
        return quantifiers.size();
    }

    /**
     * Construct a new union of two or more compatibly-ordered plans. The resulting plan will return all results that are
     * returned by all of the child plans. Each plan should return results in the same order according to the provided
     * {@code comparisonKey}.
     *
     * @param children the list of plans to take the intersection of
     * @param comparisonKeyProvidedOrderingParts a list of {@link ProvidedOrderingPart}s by which the results of both
     *        plans are ordered
     * @return a new plan that will return the intersection of all results from both child plans
     */
    @Nonnull
    public static LogicalIntersectionExpression from(@Nonnull List<? extends Reference> children, @Nonnull List<ProvidedOrderingPart> comparisonKeyProvidedOrderingParts) {
        if (children.size() < 2) {
            throw new RecordCoreArgumentException("fewer than two children given to intersection expression");
        }

        final ImmutableList.Builder<Quantifier.ForEach> childRefsBuilder = ImmutableList.builder();
        for (final var child : children) {
            childRefsBuilder.add(Quantifier.forEach(child));
        }
        return new LogicalIntersectionExpression(childRefsBuilder.build(), comparisonKeyProvidedOrderingParts);
    }

    @Nonnull
    @Override
    public PlannerGraph rewriteInternalPlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.LogicalOperatorNodeWithInfo(this,
                        NodeInfo.INTERSECTION_OPERATOR,
                        ImmutableList.of("COMPARE BY {{comparisonKey}}"),
                        ImmutableMap.of("comparisonKey", Attribute.gml(comparisonKeyProvidedOrderingParts.toString()))),
                childGraphs);
    }
}
