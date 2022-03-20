/*
 * PrimaryScanExpression.java
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

import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.ComparisonRange;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.Quantifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.explain.Attribute;
import com.apple.foundationdb.record.query.plan.temp.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.temp.rules.ImplementPhysicalScanRule;
import com.apple.foundationdb.record.query.predicates.QueriedValue;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A logical version of {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan}.
 *
 * @see ImplementPhysicalScanRule which converts this
 *      to a {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan}
 */
public class PrimaryScanExpression implements RelationalExpression, PlannerGraphRewritable {
    @Nonnull
    private final Set<String> recordTypes;

    @Nonnull
    private final List<ComparisonRange> comparisonRanges;
    private final boolean reverse;

    public PrimaryScanExpression(@Nonnull final Set<String> recordTypes,
                                 @Nonnull final List<ComparisonRange> comparisonRanges,
                                 final boolean reverse) {
        this.recordTypes = ImmutableSet.copyOf(recordTypes);
        this.comparisonRanges = ImmutableList.copyOf(comparisonRanges);
        this.reverse = reverse;
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return ImmutableList.of();
    }

    @Nonnull
    public Set<String> getRecordTypes() {
        return recordTypes;
    }

    @Nonnull
    public List<ComparisonRange> getComparisonRanges() {
        return comparisonRanges;
    }

    public boolean isReverse() {
        return reverse;
    }

    @Nonnull
    public ScanComparisons scanComparisons() {
        ScanComparisons.Builder builder = new ScanComparisons.Builder();
        for (ComparisonRange comparisonRange : comparisonRanges) {
            builder.addComparisonRange(comparisonRange);
        }
        return builder.build();
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public PrimaryScanExpression rebase(@Nonnull final AliasMap translationMap) {
        return this;
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return new QueriedValue();
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull RelationalExpression otherExpression,
                                         @Nonnull final AliasMap equivalencesMap) {
        if (this == otherExpression) {
            return true;
        }
        if (getClass() != otherExpression.getClass()) {
            return false;
        }
        final PrimaryScanExpression otherPrimaryScanExpression = (PrimaryScanExpression) otherExpression;
        return recordTypes.equals(otherPrimaryScanExpression.recordTypes) &&
               comparisonRanges.equals(otherPrimaryScanExpression.comparisonRanges) &&
               reverse == otherPrimaryScanExpression.reverse;
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
        return Objects.hash(recordTypes, comparisonRanges, reverse);
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder("Scan(");
        str.append("[").append(String.join(", ", recordTypes)).append("] ");
        str.append(scanComparisons()).append(" ");
        if (reverse) {
            str.append(" REVERSE");
        }
        return str.toString();
    }

    /**
     * Create a planner graph for better visualization of a query index plan.
     * @return the rewritten planner graph that models the index as a separate node that is connected to the
     *         actual index scan plan node.
     */
    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        Verify.verify(childGraphs.isEmpty());

        final PlannerGraph.DataNodeWithInfo dataNodeWithInfo;
        dataNodeWithInfo = new PlannerGraph.DataNodeWithInfo(NodeInfo.BASE_DATA,
                getResultType(),
                ImmutableList.of("record types: {{recordTypes}}"),
                ImmutableMap.of("recordTypes", Attribute.gml(String.join(", ", recordTypes))));

        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.LogicalOperatorNodeWithInfo(this,
                        NodeInfo.SCAN_OPERATOR,
                        ImmutableList.of("comparison ranges: {{ranges}}"),
                        ImmutableMap.of("ranges", Attribute.gml(comparisonRanges.toString()))),
                ImmutableList.of(PlannerGraph.fromNodeAndChildGraphs(
                        dataNodeWithInfo,
                        childGraphs)));
    }
}
