/*
 * TableQueueUnorderedScanExpression.java
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

package com.apple.foundationdb.record.query.plan.cascades.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.cascades.AccessHints;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.Compensation;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.IdentityBiMap;
import com.apple.foundationdb.record.query.plan.cascades.MatchInfo;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.TableQueue;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * TODO expand documentation.
 */
@API(API.Status.EXPERIMENTAL)
public class TableQueueScanExpression implements RelationalExpression, PlannerGraphRewritable {
    @Nonnull
    private final Type flowedType;

    @Nonnull
    private final AccessHints accessHints;

    @Nonnull
    private final TableQueue tableQueue;

    public TableQueueScanExpression(@Nonnull final Type flowedType, @Nonnull final AccessHints accessHints) {
        this(flowedType, accessHints, TableQueue.newInstance());
    }

    public TableQueueScanExpression(@Nonnull final Type flowedType, @Nonnull final AccessHints accessHints,
                                    @Nonnull final TableQueue tableQueue) {
        this.flowedType = flowedType;
        this.accessHints = accessHints;
        this.tableQueue = tableQueue;
    }

    @Nonnull
    public AccessHints getAccessHints() {
        return accessHints;
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return new QueriedValue(flowedType);
    }

    @Nonnull
    public TableQueue getTableQueue() {
        return tableQueue;
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of();
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public TableQueueScanExpression translateCorrelations(@Nonnull final TranslationMap translationMap,
                                                          @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return this;
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression, @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }

        return flowedType.equals(otherExpression.getResultValue().getResultType());
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
        return Objects.hash(flowedType);
    }

    @Override
    public String toString() {
        return "TableQueueUnorderedScan";
    }

    @Nonnull
    @Override
    public Iterable<MatchInfo> subsumedBy(@Nonnull final RelationalExpression candidateExpression,
                                          @Nonnull final AliasMap bindingAliasMap,
                                          @Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap,
                                          @Nonnull final EvaluationContext evaluationContext) {
        // no match.
        return ImmutableList.of();
    }

    @Override
    public Compensation compensate(@Nonnull final PartialMatch partialMatch, @Nonnull final Map<CorrelationIdentifier, ComparisonRange> boundParameterPrefixMap) {
        return Compensation.noCompensation();
    }

    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull List<? extends PlannerGraph> childGraphs) {
        Verify.verify(childGraphs.isEmpty());

        final PlannerGraph.DataNodeWithInfo dataNodeWithInfo;
        dataNodeWithInfo = new PlannerGraph.DataNodeWithInfo(NodeInfo.BASE_DATA,
                getResultType(), ImmutableList.of());

        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.LogicalOperatorNodeWithInfo(this,
                        NodeInfo.TEMPORARY_TABLE,
                        ImmutableList.of(),
                        ImmutableMap.of()),
                ImmutableList.of(PlannerGraph.fromNodeAndChildGraphs(
                        dataNodeWithInfo,
                        childGraphs)));
    }
}
