/*
 * TableFunctionExpression.java
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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.Compensation;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.IdentityBiMap;
import com.apple.foundationdb.record.query.plan.cascades.MatchInfo;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.explain.InternalPlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.StreamingValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.PullUp;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A table function expression that delegates the actual execution to an underlying {@link StreamingValue} which
 * effectively returns a stream of results.
 */
@API(API.Status.EXPERIMENTAL)
public class TableFunctionExpression extends AbstractRelationalExpressionWithoutChildren implements InternalPlannerGraphRewritable {
    private final StreamingValue value;

    public TableFunctionExpression(final StreamingValue value) {
        this.value = value;
    }

    @Override
    public Value getResultValue() {
        return new QueriedValue(value.getResultType());
    }

    @Override
    public Set<Type> getDynamicTypes() {
        return value.getDynamicTypes();
    }

    public StreamingValue getValue() {
        return value;
    }

    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return Collections.emptyList();
    }

    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return value.getCorrelatedTo();
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(RelationalExpression otherExpression,
                                         final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        if (!(otherExpression instanceof TableFunctionExpression)) {
            return false;
        }

        final var otherTableFunctionExpression = (TableFunctionExpression)otherExpression;

        return value.semanticEquals(otherTableFunctionExpression.getValue(), equivalencesMap);
    }

    @Override
    public int computeHashCodeWithoutChildren() {
        return Objects.hash(value);
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
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public TableFunctionExpression translateCorrelations(final TranslationMap translationMap,
                                                   final boolean shouldSimplifyValues,
                                                   final List<? extends Quantifier> translatedQuantifiers) {
        Verify.verify(translatedQuantifiers.isEmpty());
        if (translationMap.definesOnlyIdentities()) {
            return this;
        }
        final Value translatedCollectionValue = value.translateCorrelations(translationMap, shouldSimplifyValues);
        if (translatedCollectionValue != value) {
            return new TableFunctionExpression((StreamingValue)translatedCollectionValue);
        }
        return this;
    }

    @Override
    public Iterable<MatchInfo> subsumedBy(final RelationalExpression candidateExpression,
                                          final AliasMap bindingAliasMap,
                                          final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap,
                                          final EvaluationContext evaluationContext) {
        if (!isCompatiblyAndCompletelyBound(bindingAliasMap, candidateExpression.getQuantifiers())) {
            return ImmutableList.of();
        }

        return exactlySubsumedBy(candidateExpression, bindingAliasMap, partialMatchMap, TranslationMap.empty());
    }

    @Override
    public Compensation compensate(final PartialMatch partialMatch,
                                   final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap,
                                   @Nullable final PullUp pullUp,
                                   final CorrelationIdentifier nestingAlias) {
        // subsumedBy() is based on equality and this expression is always a leaf, thus we return empty here as
        // if there is a match, it's exact
        return Compensation.noCompensation();
    }

    @Override
    public PlannerGraph rewriteInternalPlannerGraph(final List<? extends PlannerGraph> childGraphs) {
        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.LogicalOperatorNode(this,
                        "TFunc",
                        ImmutableList.of(toString()),
                        ImmutableMap.of()),
                childGraphs);
    }

    @Override
    public String toString() {
        return value.toString();
    }
}
