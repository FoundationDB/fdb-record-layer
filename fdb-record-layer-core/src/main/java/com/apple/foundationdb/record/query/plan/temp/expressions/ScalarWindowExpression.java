/*
 * SelectExpression.java
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

package com.apple.foundationdb.record.query.plan.temp.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.ComparisonRange;
import com.apple.foundationdb.record.query.plan.temp.Compensation;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.IdentityBiMap;
import com.apple.foundationdb.record.query.plan.temp.MatchInfo;
import com.apple.foundationdb.record.query.plan.temp.PartialMatch;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.explain.InternalPlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraph;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A window aggregate expression, i.e. an expression representing
 * <pre>
 * {@code
 * SELECT RANK(score PARTITIONED BY a), COUNT(x PARTITIONED BY a, b ORDER x) FROM T
 * }
 * </pre>
 */
@API(API.Status.EXPERIMENTAL)
public class ScalarWindowExpression implements RelationalExpressionWithChildren, InternalPlannerGraphRewritable {
    @Nonnull
    private final List<? extends Value> resultValues;
    @Nonnull
    private final Quantifier child;

    public ScalarWindowExpression(@Nonnull List<? extends Value> resultValues,
                                  @Nonnull Quantifier child) {
        this.resultValues = ImmutableList.copyOf(resultValues);
        this.child = child;
    }

    @Nonnull
    @Override
    public List<? extends Value> getResultValues() {
        return resultValues;
    }


    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of(child);
    }

    @Override
    public int getRelationalChildCount() {
        return 1;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedToWithoutChildren() {
        return resultValues.stream().flatMap(resultValue -> resultValue.getCorrelatedTo().stream())
                .collect(ImmutableSet.toImmutableSet());
    }

    @Nonnull
    @Override
    public ScalarWindowExpression rebase(@Nonnull final AliasMap translationMap) {
        return (ScalarWindowExpression)RelationalExpressionWithChildren.super.rebase(translationMap);
    }

    @Nonnull
    @Override
    public ScalarWindowExpression rebaseWithRebasedQuantifiers(@Nonnull final AliasMap translationMap, @Nonnull final List<Quantifier> rebasedQuantifiers) {
        final ImmutableList<Value> rebasedResultValues = resultValues.stream().map(r -> r.rebase(translationMap)).collect(ImmutableList.toImmutableList());
        return new ScalarWindowExpression(rebasedResultValues, Iterables.getOnlyElement(rebasedQuantifiers));
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

    @SuppressWarnings("UnstableApiUsage")
    @Override
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap aliasMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }

        return semanticEqualsForResults(otherExpression, aliasMap);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(getResultValues());
    }

    @Nonnull
    @Override
    public Iterable<MatchInfo> subsumedBy(@Nonnull final RelationalExpression candidateExpression,
                                          @Nonnull final AliasMap aliasMap,
                                          @Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap) {
        return exactlySubsumedBy(candidateExpression, aliasMap, partialMatchMap);
    }

    @Nonnull
    @Override
    public PlannerGraph rewriteInternalPlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.LogicalOperatorNode(this,
                        "Select",
                        ImmutableList.of("SELECT " + resultValues.stream().map(Object::toString).collect(Collectors.joining(", "))),
                        ImmutableMap.of()),
                childGraphs);
    }

    @Override
    public String toString() {
        return "SELECT " + resultValues.stream().map(Object::toString).collect(Collectors.joining(", "));
    }

    @Override
    @SuppressWarnings({"java:S135", "java:S1066"})
    public Compensation compensate(@Nonnull final PartialMatch partialMatch, @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap) {
        final var childPartialMatch = Objects.requireNonNull(partialMatch.getMatchInfo().getChildPartialMatch(child).orElseThrow(() -> new RecordCoreException("expected a match child")));
        return childPartialMatch.compensate(boundParameterPrefixMap);
    }
}
