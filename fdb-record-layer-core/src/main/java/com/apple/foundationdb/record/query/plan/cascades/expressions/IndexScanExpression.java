/*
 * IndexScanExpression.java
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

import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.values.IndexedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A logical version of {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan}.
 */
public class IndexScanExpression implements RelationalExpression, PlannerGraphRewritable {
    @Nonnull
    private final String indexName;
    @Nonnull
    private final IndexScanType scanType;
    @Nonnull
    private final List<ComparisonRange> comparisonRanges;
    private final boolean reverse;

    public IndexScanExpression(@Nonnull final String indexName,
                               @Nonnull IndexScanType scanType,
                               @Nonnull final List<ComparisonRange> comparisonRanges,
                               final boolean reverse) {
        this.indexName = indexName;
        this.scanType = scanType;
        this.comparisonRanges = ImmutableList.copyOf(comparisonRanges);
        this.reverse = reverse;
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        // TODO this is not correct
        return ImmutableList.of();
    }

    @Nonnull
    public String getIndexName() {
        return indexName;
    }

    @Nonnull
    public IndexScanType getScanType() {
        return scanType;
    }

    @Nonnull
    public ScanComparisons scanComparisons() {
        ScanComparisons.Builder builder = new ScanComparisons.Builder();
        for (ComparisonRange comparisonRange : comparisonRanges) {
            builder.addComparisonRange(comparisonRange);
        }
        return builder.build();
    }

    public int getEqualitySize() {
        final int size = comparisonRanges.size();
        if (hasInequality()) {
            return size - 1;
        }
        return size;
    }

    public boolean hasInequality() {
        final int size = comparisonRanges.size();
        Verify.verify(size > 0);
        return comparisonRanges.get(size - 1).isInequality();
    }

    public boolean isReverse() {
        return reverse;
    }

    public int getSargableSize() {
        return comparisonRanges.size();
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public IndexScanExpression translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<Quantifier> translatedQuantifiers) {
        // TODO this may or may not need to be translated depending on the correlations this expression is correlated
        //      to.
        return this;
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return new IndexedValue();
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
        final IndexScanExpression otherIndexScanExpression = (IndexScanExpression) otherExpression;
        return indexName.equals(otherIndexScanExpression.indexName) &&
               scanType.equals(otherIndexScanExpression.scanType) &&
               comparisonRanges.equals(otherIndexScanExpression.comparisonRanges) &&
               reverse == otherIndexScanExpression.reverse;
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
        return Objects.hash(indexName, scanType, comparisonRanges, reverse);
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder("IndexScan(");
        str.append(indexName).append(" ");
        str.append(scanComparisons()).append(" ");
        if (!scanType.equals(IndexScanType.BY_VALUE)) {
            str.append(scanType);
        }
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
        dataNodeWithInfo = new PlannerGraph.DataNodeWithInfo(NodeInfo.INDEX_DATA,
                getResultType(),
                ImmutableList.of("index name: {{indexName}}"),
                ImmutableMap.of("indexName", Attribute.gml(getIndexName())));

        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.LogicalOperatorNodeWithInfo(this,
                        NodeInfo.INDEX_SCAN_OPERATOR,
                        ImmutableList.of("comparison ranges: {{ranges}}"),
                        ImmutableMap.of("ranges", Attribute.gml(comparisonRanges.toString()))),
                ImmutableList.of(PlannerGraph.fromNodeAndChildGraphs(
                        dataNodeWithInfo,
                        childGraphs)));
    }
}
