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

package com.apple.foundationdb.record.query.plan.temp.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.ComparisonRange;
import com.apple.foundationdb.record.query.plan.temp.Compensation;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.IdentityBiMap;
import com.apple.foundationdb.record.query.plan.temp.MatchInfo;
import com.apple.foundationdb.record.query.plan.temp.PartialMatch;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.explain.Attribute;
import com.apple.foundationdb.record.query.plan.temp.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraphRewritable;
import com.apple.foundationdb.record.query.predicates.BaseValue;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * A planner expression representing a full, unordered scan of the records by primary key, which is the logical version
 * of a {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan} with
 * {@link com.apple.foundationdb.record.query.plan.ScanComparisons#EMPTY}. Unlike a
 * {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan}, a {@code FullUnorderedScanExpression}
 * is not implicitly ordered by the primary key.
 *
 * <p>
 * This expression is useful as the source of records for the initial planner expression produced from a
 * {@link com.apple.foundationdb.record.query.RecordQuery}.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public class FullUnorderedScanExpression implements RelationalExpression, PlannerGraphRewritable {
    @Nonnull
    private final Set<String> recordTypes;

    public FullUnorderedScanExpression(final Set<String> recordTypes) {
        this.recordTypes = ImmutableSet.copyOf(recordTypes);
    }

    @Nonnull
    public Set<String> getRecordTypes() {
        return recordTypes;
    }

    @Nonnull
    @Override
    public Optional<List<? extends Value>> getResultValues() {
        return Optional.of(ImmutableList.of(new BaseValue()));
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
    public FullUnorderedScanExpression rebase(@Nonnull final AliasMap translationMap) {
        return this;
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression, @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }

        return recordTypes.equals(((FullUnorderedScanExpression)otherExpression).getRecordTypes());
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
        return Objects.hash(recordTypes);
    }

    @Override
    public String toString() {
        return "FullUnorderedScan";
    }

    @Nonnull
    @Override
    public Iterable<MatchInfo> subsumedBy(@Nonnull final RelationalExpression candidateExpression, @Nonnull final AliasMap aliasMap, @Nonnull final IdentityBiMap<Quantifier, PartialMatch> partialMatchMap) {
        return exactlySubsumedBy(candidateExpression, aliasMap, partialMatchMap);
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
