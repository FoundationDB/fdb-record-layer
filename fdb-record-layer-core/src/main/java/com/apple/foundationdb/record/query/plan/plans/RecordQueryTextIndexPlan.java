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
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.TextScan;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.MatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.ExplainPlanVisitor;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.AbstractRelationalExpressionWithoutChildren;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;

import org.jspecify.annotations.Nullable;

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
public class RecordQueryTextIndexPlan extends AbstractRelationalExpressionWithoutChildren implements RecordQueryPlanWithIndex, RecordQueryPlanWithNoChildren {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Text-Index-Plan");

    private final String indexName;
    private final TextScan textScan;
    private final boolean reverse;

    public RecordQueryTextIndexPlan(String indexName, TextScan textScan,
                                    boolean reverse) {
        this.indexName = indexName;
        this.textScan = textScan;
        this.reverse = reverse;
    }

    @Override
    public <M extends Message> RecordCursor<IndexEntry> executeEntries(FDBRecordStoreBase<M> store, EvaluationContext context,
                                                                       @Nullable byte[] continuation, ExecuteProperties executeProperties) {
        return textScan.scan(store, context, continuation, executeProperties.asScanProperties(reverse));
    }

    @Override
    public String getIndexName() {
        return indexName;
    }

    @Override
    public IndexScanType getScanType() {
        return IndexScanType.BY_TEXT_TOKEN;
    }

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
    public boolean hasIndexScan(String indexName) {
        return this.indexName.equals(indexName);
    }

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

    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.ALL_FIELDS;
    }

    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return ImmutableSet.of();
    }

    @Override
    public RecordQueryTextIndexPlan translateCorrelations(final TranslationMap translationMap,
                                                          final boolean shouldSimplifyValues,
                                                          final List<? extends Quantifier> translatedQuantifiers) {
        // TODO make return this dependent on whether the index scan is correlated according to the translation map
        return this;
    }

    @Override
    public Optional<? extends MatchCandidate> getMatchCandidateMaybe() {
        return Optional.empty();
    }

    @Override
    public RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords getFetchIndexRecords() {
        return RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY;
    }

    @Override
    public Value getResultValue() {
        return new QueriedValue();
    }

    @Override
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    public boolean equalsWithoutChildren(RelationalExpression otherExpression,
                                         final AliasMap equivalencesMap) {
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
    public int computeHashCodeWithoutChildren() {
        return Objects.hash(indexName, textScan, reverse);
    }

    @Override
    public int planHash(final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                return indexName.hashCode() + textScan.planHash(mode) + (reverse ? 1 : 0);
            case FOR_CONTINUATION:
                return PlanHashable.objectsPlanHash(mode, BASE_HASH, indexName, textScan, reverse);
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.getKind() + " is not supported");
        }
    }

    @Override
    public String toString() {
        return ExplainPlanVisitor.toStringForDebugging(this);
    }

    /**
     * Rewrite the planner graph for better visualization of a query index plan.
     * @return the rewritten planner graph that models the index as a separate node that is connected to the
     *         actual index scan plan node.
     */
    @Override
    public PlannerGraph rewritePlannerGraph(final List<? extends PlannerGraph> childGraphs) {
        Verify.verify(childGraphs.isEmpty());
        return createIndexPlannerGraph(this,
                NodeInfo.TEXT_INDEX_SCAN_OPERATOR,
                ImmutableList.of(),
                ImmutableMap.of());
    }

    @Override
    public PlannerGraph createIndexPlannerGraph(final RecordQueryPlan identity,
                                                final NodeInfo nodeInfo,
                                                final List<String> additionalDetails,
                                                final Map<String, Attribute> additionalAttributeMap) {
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
        final PlannerGraph.DataNodeWithInfo source = new PlannerGraph.DataNodeWithInfo(NodeInfo.INDEX_DATA, getResultType(), ImmutableList.of(getIndexName()));
        return PlannerGraph.builder(root)
                .addNode(source)
                .addEdge(source, root, new PlannerGraph.Edge())
                .build();
    }

    @Override
    public Message toProto(final PlanSerializationContext serializationContext) {
        throw new RecordCoreException("serialization of this plan is not supported");
    }

    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(final PlanSerializationContext serializationContext) {
        throw new RecordCoreException("serialization of this plan is not supported");
    }
}
