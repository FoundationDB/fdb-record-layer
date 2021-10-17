/*
 * RecordQueryTextIndexPlan.java
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.TextScan;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.MatchCandidate;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.explain.Attribute;
import com.apple.foundationdb.record.query.plan.temp.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraph;
import com.apple.foundationdb.record.query.predicates.QueriedValue;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * A query plan that can scan text indexes. These work slightly differently than regular indexes
 * in that the comparison on a query might actually be split into multiple queries.
 */
@API(API.Status.INTERNAL)
public class RecordQueryTextIndexPlan implements RecordQueryPlanWithIndex, RecordQueryPlanWithNoChildren {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Text-Index-Plan");

    @Nonnull
    private final String indexName;
    @Nonnull
    private final TextScan textScan;
    private final boolean reverse;

    public RecordQueryTextIndexPlan(@Nonnull String indexName, @Nonnull TextScan textScan,
                                    boolean reverse) {
        this.indexName = indexName;
        this.textScan = textScan;
        this.reverse = reverse;
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<IndexEntry> executeEntries(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context,
                                                                       @Nullable byte[] continuation, @Nonnull ExecuteProperties executeProperties) {
        return textScan.scan(store, context, continuation, executeProperties.asScanProperties(reverse));
    }

    @Nonnull
    @Override
    public String getIndexName() {
        return indexName;
    }

    @Nonnull
    @Override
    public IndexScanType getScanType() {
        return IndexScanType.BY_TEXT_TOKEN;
    }

    @Nonnull
    public TextScan getTextScan() {
        return textScan;
    }

    @Override
    public boolean isReverse() {
        return reverse;
    }

    @Override
    public boolean hasRecordScan() {
        return false;
    }

    @Override
    public boolean hasFullRecordScan() {
        return false;
    }

    @Override
    public boolean hasIndexScan(@Nonnull String indexName) {
        return this.indexName.equals(indexName);
    }

    @Nonnull
    @Override
    public Set<String> getUsedIndexes() {
        return Collections.singleton(indexName);
    }

    @Override
    public boolean hasLoadBykeys() {
        return false;
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_INDEX);
    }

    @Override
    public int getComplexity() {
        return 1;
    }

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.ALL_FIELDS;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public RecordQueryTextIndexPlan rebase(@Nonnull final AliasMap translationMap) {
        return new RecordQueryTextIndexPlan(getIndexName(), getTextScan(), isReverse());
    }

    @Nonnull
    @Override
    public Optional<? extends MatchCandidate> getMatchCandidateOptional() {
        return Optional.empty();
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
        final RecordQueryTextIndexPlan that = (RecordQueryTextIndexPlan)otherExpression;
        return this.reverse == that.reverse &&
               this.indexName.equals(that.indexName) &&
               this.textScan.equals(that.textScan);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object other) {
        return structuralEquals(other);
    }

    @Override
    public int hashCode() {
        return structuralHashCode();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return Objects.hash(indexName, textScan, reverse);
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
                return indexName.hashCode() + textScan.planHash(hashKind) + (reverse ? 1 : 0);
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, indexName, textScan, reverse);
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }

    @Nonnull
    @Override
    public String toString() {
        return "TextIndex(" + textScan.getIndex().getName() + " " + textScan.getGroupingComparisons() + ", " + textScan.getTextComparison() + ", " + textScan.getSuffixComparisons() + ")";
    }

    /**
     * Rewrite the planner graph for better visualization of a query index plan.
     * @return the rewritten planner graph that models the index as a separate node that is connected to the
     *         actual index scan plan node.
     */
    @Nonnull
    @Override
    public PlannerGraph rewritePlannerGraph(@Nonnull final List<? extends PlannerGraph> childGraphs) {
        Verify.verify(childGraphs.isEmpty());
        return createIndexPlannerGraph(this,
                NodeInfo.TEXT_INDEX_SCAN_OPERATOR,
                ImmutableList.of(),
                ImmutableMap.of());
    }

    @Nonnull
    @Override
    public PlannerGraph createIndexPlannerGraph(@Nonnull final RecordQueryPlan identity,
                                                @Nonnull final NodeInfo nodeInfo,
                                                @Nonnull final List<String> additionalDetails,
                                                @Nonnull final Map<String, Attribute> additionalAttributeMap) {
        final ImmutableList.Builder<String> detailsBuilder = ImmutableList.builder();
        final ImmutableMap.Builder<String, Attribute> attributeMapBuilder = ImmutableMap.builder();

        detailsBuilder.addAll(additionalDetails);
        detailsBuilder.add("grouping comparisons: {{groupingComparisons}}",
                "text comparisons: {{textComparisons}}",
                "suffix comparisons: {{suffixComparisons}}");

        attributeMapBuilder.putAll(additionalAttributeMap);

        if (textScan.getGroupingComparisons() != null) {
            attributeMapBuilder.put("groupingComparisons", Attribute.gml(Objects.requireNonNull(textScan.getGroupingComparisons()).toString()));
        } else {
            attributeMapBuilder.put("groupingComparisons", Attribute.gml("none"));
        }
        if (textScan.getSuffixComparisons() != null) {
            attributeMapBuilder.put("suffixComparisons", Attribute.gml(Objects.requireNonNull(textScan.getSuffixComparisons()).toString()));
        } else {
            attributeMapBuilder.put("suffixComparisons", Attribute.gml("none"));
        }
        attributeMapBuilder.put("textComparisons", Attribute.gml(Objects.requireNonNull(textScan.getTextComparison()).toString()));

        final PlannerGraph.Node root =
                new PlannerGraph.OperatorNodeWithInfo(this,
                        nodeInfo,
                        detailsBuilder.build(),
                        attributeMapBuilder.build());
        final PlannerGraph.DataNodeWithInfo source = new PlannerGraph.DataNodeWithInfo(NodeInfo.INDEX_DATA, ImmutableList.of(getIndexName()));
        return PlannerGraph.builder(root)
                .addNode(source)
                .addEdge(source, root, new PlannerGraph.Edge())
                .build();
    }
}
