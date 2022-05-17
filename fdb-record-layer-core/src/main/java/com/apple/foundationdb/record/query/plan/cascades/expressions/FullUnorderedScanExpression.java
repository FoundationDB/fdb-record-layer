/*
 * FullUnorderedScanExpression.java
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
import com.apple.foundationdb.record.query.plan.cascades.AccessHints;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.Compensation;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.IdentityBiMap;
import com.apple.foundationdb.record.query.plan.cascades.MatchInfo;
import com.apple.foundationdb.record.query.plan.cascades.PartialMatch;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
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
 * A planner expression representing a full, unordered scan of the records by primary key, which is the logical version
 * of a {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan} with
 * {@link com.apple.foundationdb.record.query.plan.ScanComparisons#EMPTY}. Unlike a
 * {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan}, a {@code FullUnorderedScanExpression}
 * is not implicitly ordered by the primary key.
 *
 *
 * This expression is useful as the source of records for the initial planner expression produced from a
 * {@link com.apple.foundationdb.record.query.RecordQuery}.
 *
 */
@API(API.Status.EXPERIMENTAL)
public class FullUnorderedScanExpression implements RelationalExpression, PlannerGraphRewritable {
    @Nonnull
    private final Set<String> recordTypes;
    @Nonnull
    private final Type.Record flowedType;

    @Nonnull
    final AccessHints accessHints;

    public FullUnorderedScanExpression(@Nonnull final Set<String> recordTypes, @Nonnull final Type.Record flowedType, @Nonnull final AccessHints accessHints) {
        this.recordTypes = ImmutableSet.copyOf(recordTypes);
        this.flowedType = flowedType;
        this.accessHints = accessHints;
    }

    @Nonnull
    public Set<String> getRecordTypes() {
        return recordTypes;
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
    public FullUnorderedScanExpression translateCorrelations(@Nonnull final TranslationMap translationMap,
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
        if (!recordTypes.equals(((FullUnorderedScanExpression)otherExpression).getRecordTypes())) {
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
        return Objects.hash(recordTypes, flowedType);
    }

    @Override
    public String toString() {
        return "FullUnorderedScan";
    }

    @Nonnull
    @Override
    public Iterable<MatchInfo> subsumedBy(@Nonnull final RelationalExpression candidateExpression, @Nonnull final AliasMap aliasMap, @Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap) {
        if (getClass() != candidateExpression.getClass()) {
            return ImmutableList.of();
        }
        // if query does not contain candidate's indexes, the query cannot be subsumed by the candidate
        if (getAccessHints().satisfies(((FullUnorderedScanExpression)candidateExpression).getAccessHints())) {
            return exactlySubsumedBy(candidateExpression, aliasMap, partialMatchMap);
        } else {
            return ImmutableList.of();
        }
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
                getResultType(),
                ImmutableList.of("record types: {{types}}"),
                ImmutableMap.of("types", Attribute.gml(getRecordTypes().stream().map(Attribute::gml).collect(ImmutableList.toImmutableList()))));

        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.LogicalOperatorNodeWithInfo(this,
                        NodeInfo.SCAN_OPERATOR,
                        ImmutableList.of(),
                        ImmutableMap.of()),
                ImmutableList.of(PlannerGraph.fromNodeAndChildGraphs(
                        dataNodeWithInfo,
                        childGraphs)));
    }
}
