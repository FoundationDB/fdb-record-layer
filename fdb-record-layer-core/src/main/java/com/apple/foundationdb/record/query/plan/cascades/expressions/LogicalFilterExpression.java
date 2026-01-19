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

package com.apple.foundationdb.record.query.plan.cascades.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.explain.InternalPlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.explain.DefaultExplainSymbolMap;
import com.apple.foundationdb.record.query.plan.explain.WithIndentationsExplainFormatter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A relational planner expression that represents an unimplemented filter on the records produced by its inner
 * relational planner expression.
 * @see com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan for the fallback implementation
 */
@API(API.Status.EXPERIMENTAL)
public class LogicalFilterExpression extends AbstractRelationalExpressionWithChildren implements RelationalExpressionWithPredicates, InternalPlannerGraphRewritable {
    @Nonnull
    private final List<QueryPredicate> queryPredicates;
    @Nonnull
    private final Quantifier inner;
    @Nonnull
    private final Supplier<QueryPredicate> conjunctedPredicateSupplier;

    public LogicalFilterExpression(@Nonnull Iterable<? extends QueryPredicate> queryPredicates,
                                   @Nonnull Quantifier inner) {
        this.queryPredicates = ImmutableList.copyOf(queryPredicates);
        this.inner = inner;
        this.conjunctedPredicateSupplier = Suppliers.memoize(this::computeConjunctedPredicate);
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
    public List<? extends QueryPredicate> getPredicates() {
        return queryPredicates;
    }

    @Nonnull
    @VisibleForTesting
    public Quantifier getInner() {
        return inner;
    }

    @Nonnull
    public QueryPredicate getConjunctedPredicate() {
        return conjunctedPredicateSupplier.get();
    }

    @Nonnull
    private QueryPredicate computeConjunctedPredicate() {
        return AndPredicate.and(getPredicates());
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return queryPredicates.stream()
                .flatMap(queryPredicate -> queryPredicate.getCorrelatedTo().stream())
                .collect(ImmutableSet.toImmutableSet());
    }

    @Nonnull
    @Override
    public LogicalFilterExpression translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                         final boolean shouldSimplifyValues,
                                                         @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        final List<QueryPredicate> rebasedQueryPredicates =
                queryPredicates.stream()
                        .map(queryPredicate -> queryPredicate.translateCorrelations(translationMap,
                                shouldSimplifyValues))
                        .collect(ImmutableList.toImmutableList());

        return new LogicalFilterExpression(rebasedQueryPredicates,
                Iterables.getOnlyElement(translatedQuantifiers));
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return inner.getFlowedObjectValue();
    }

    @Override
    @SuppressWarnings({"UnstableApiUsage", "PMD.CompareObjectsWithEquals"})
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        final LogicalFilterExpression otherLogicalFilterExpression = (LogicalFilterExpression)otherExpression;
        final List<? extends QueryPredicate> otherQueryPredicates = otherLogicalFilterExpression.getPredicates();
        if (queryPredicates.size() != otherQueryPredicates.size()) {
            return false;
        }
        return Streams.zip(queryPredicates.stream(), otherQueryPredicates.stream(),
                (queryPredicate, otherQueryPredicate) -> queryPredicate.semanticEquals(otherQueryPredicate, equivalencesMap))
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
    public int computeHashCodeWithoutChildren() {
        return Objects.hash(getPredicates());
    }

    @Nonnull
    @Override
    public PlannerGraph rewriteInternalPlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        final var explainFormatter =
                new WithIndentationsExplainFormatter(DefaultExplainSymbolMap::new, 7,
                        50, 4);

        final var predicateString =
                "WHERE " + getConjunctedPredicate().explain()
                        .getExplainTokens()
                        .render(explainFormatter);

        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.LogicalOperatorNodeWithInfo(
                        this,
                        NodeInfo.PREDICATE_FILTER_OPERATOR,
                        ImmutableList.of(predicateString),
                        ImmutableMap.of()),
                childGraphs);
    }
}
