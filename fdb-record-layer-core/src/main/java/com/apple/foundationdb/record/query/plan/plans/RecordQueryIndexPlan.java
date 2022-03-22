/*
 * RecordQueryIndexPlan.java
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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.temp.AliasMap;
import com.apple.foundationdb.record.query.plan.temp.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.temp.RelationalExpression;
import com.apple.foundationdb.record.query.plan.temp.ScanWithFetchMatchCandidate;
import com.apple.foundationdb.record.query.plan.temp.explain.Attribute;
import com.apple.foundationdb.record.query.plan.temp.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraphRewritable;
import com.apple.foundationdb.record.query.predicates.QueriedValue;
import com.apple.foundationdb.record.query.predicates.Value;
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
 * A query plan that outputs records pointed to by entries in a secondary index within some range.
 */
@API(API.Status.INTERNAL)
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class RecordQueryIndexPlan implements RecordQueryPlanWithNoChildren, RecordQueryPlanWithComparisons, RecordQueryPlanWithIndex, PlannerGraphRewritable {
    protected static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Index-Plan");

    @Nonnull
    protected final String indexName;
    @Nonnull
    protected final IndexScanParameters scanParameters;
    protected final boolean reverse;
    protected final boolean strictlySorted;
    @Nonnull
    private final Optional<? extends ScanWithFetchMatchCandidate> matchCandidateOptional;

    public RecordQueryIndexPlan(@Nonnull final String indexName, @Nonnull final IndexScanParameters scanParameters, final boolean reverse) {
        this(indexName, scanParameters, reverse, false);
    }

    public RecordQueryIndexPlan(@Nonnull final String indexName,
                                @Nonnull final IndexScanParameters scanParameters,
                                final boolean reverse,
                                final boolean strictlySorted) {
        this(indexName, scanParameters, reverse, strictlySorted, Optional.empty());
    }

    public RecordQueryIndexPlan(@Nonnull final String indexName,
                                @Nonnull final IndexScanParameters scanParameters,
                                final boolean reverse,
                                final boolean strictlySorted,
                                @Nonnull final ScanWithFetchMatchCandidate matchCandidate) {
        this(indexName, scanParameters, reverse, strictlySorted, Optional.of(matchCandidate));
    }

    private RecordQueryIndexPlan(@Nonnull final String indexName,
                                 @Nonnull final IndexScanParameters scanParameters,
                                 final boolean reverse,
                                 final boolean strictlySorted,
                                 @Nonnull final Optional<? extends ScanWithFetchMatchCandidate> matchCandidateOptional) {
        this.indexName = indexName;
        this.scanParameters = scanParameters;
        this.reverse = reverse;
        this.strictlySorted = strictlySorted;
        this.matchCandidateOptional = matchCandidateOptional;
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<IndexEntry> executeEntries(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context,
                                                                       @Nullable byte[] continuation, @Nonnull ExecuteProperties executeProperties) {
        final RecordMetaData metaData = store.getRecordMetaData();
        final Index index = metaData.getIndex(indexName);
        final IndexScanBounds scanBounds = scanParameters.bind(store, index, context);
        return store.scanIndex(index, scanBounds, continuation, executeProperties.asScanProperties(reverse));
    }

    @Nonnull
    @Override
    public String getIndexName() {
        return indexName;
    }

    @Nonnull
    public IndexScanParameters getScanParameters() {
        return scanParameters;
    }

    @Nonnull
    @Override
    public IndexScanType getScanType() {
        return scanParameters.getScanType();
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
    public int maxCardinality(@Nonnull RecordMetaData metaData) {
        final Index index = metaData.getIndex(indexName);
        if (index.isUnique() && scanParameters.isUnique(index)) {
            return 1;
        } else {
            return UNKNOWN_MAX_CARDINALITY;
        }
    }

    @Override
    public boolean isStrictlySorted() {
        return strictlySorted;
    }

    @Nonnull
    @Override
    public Optional<? extends ScanWithFetchMatchCandidate> getMatchCandidateOptional() {
        return matchCandidateOptional;
    }

    @Override
    public RecordQueryIndexPlan strictlySorted() {
        return new RecordQueryIndexPlan(indexName, scanParameters, reverse, true);
    }

    @Override
    public boolean hasLoadBykeys() {
        return false;
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public RecordQueryIndexPlan rebase(@Nonnull final AliasMap translationMap) {
        return new RecordQueryIndexPlan(getIndexName(),
                getScanParameters(),
                isReverse(),
                isStrictlySorted(),
                matchCandidateOptional);
    }

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.ALL_FIELDS;
    }

    @Nonnull
    @Override
    public List<? extends Value> getResultValues() {
        return ImmutableList.of(new QueriedValue());
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
        RecordQueryIndexPlan that = (RecordQueryIndexPlan) otherExpression;
        return reverse == that.reverse &&
               strictlySorted == that.strictlySorted &&
               Objects.equals(indexName, that.indexName) &&
               Objects.equals(scanParameters, that.scanParameters);
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
        return Objects.hash(indexName, scanParameters, reverse, strictlySorted);
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        switch (hashKind) {
            case LEGACY:
                return indexName.hashCode() + scanParameters.planHash(hashKind) + (reverse ? 1 : 0);
            case FOR_CONTINUATION:
            case STRUCTURAL_WITHOUT_LITERALS:
                if (scanParameters instanceof IndexScanComparisons) {
                    // Keep hash stable for change in representation.
                    // TODO: If there is another event that changes hashes or they become less critical in tests, this can be removed.
                    return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, indexName, getScanType(), getComparisons(), reverse, strictlySorted);
                } else {
                    return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, indexName, scanParameters, reverse, strictlySorted);
                }
            default:
                throw new UnsupportedOperationException("Hash kind " + hashKind.name() + " is not supported");
        }
    }

    @Nonnull
    @Override
    public String toString() {
        StringBuilder str = new StringBuilder("Index(");
        appendScanDetails(str);
        str.append(")");
        return str.toString();
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_INDEX);
    }

    protected void appendScanDetails(StringBuilder str) {
        str.append(indexName).append(" ").append(scanParameters.getScanDetails());
        if (scanParameters.getScanType() != IndexScanType.BY_VALUE) {
            str.append(" ").append(scanParameters.getScanType());
        }
        if (reverse) {
            str.append(" REVERSE");
        }
    }

    @Override
    public boolean hasComparisons() {
        return scanParameters instanceof IndexScanComparisons;
    }

    @Nonnull
    @Override
    public ScanComparisons getComparisons() {
        if (scanParameters instanceof IndexScanComparisons) {
            return ((IndexScanComparisons)scanParameters).getComparisons();
        } else {
            throw new RecordCoreException("this plan does not use ScanComparisons");
        }
    }

    @Override
    public int getComplexity() {
        return 1;
    }

    /**
     * Create a planner graph for better visualization of a query index plan.
     * @return the rewritten planner graph that models the index as a separate node that is connected to the
     *         actual index scan plan node.
     */
    @Nonnull
    @Override
    public PlannerGraph createIndexPlannerGraph(@Nonnull RecordQueryPlan identity,
                                                @Nonnull final NodeInfo nodeInfo,
                                                @Nonnull final List<String> additionalDetails,
                                                @Nonnull final Map<String, Attribute> additionalAttributeMap) {
        final ImmutableList.Builder<String> detailsBuilder = ImmutableList.builder();
        final ImmutableMap.Builder<String, Attribute> attributeMapBuilder = ImmutableMap.builder();

        detailsBuilder
                .addAll(additionalDetails);
        attributeMapBuilder
                .putAll(additionalAttributeMap);

        scanParameters.getPlannerGraphDetails(detailsBuilder, attributeMapBuilder);

        if (reverse) {
            detailsBuilder.add("direction: {{direction}}");
            attributeMapBuilder.put("direction", Attribute.gml("reversed"));
        }

        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(identity,
                        nodeInfo,
                        detailsBuilder.build(),
                        attributeMapBuilder.build()),
                ImmutableList.of(
                        PlannerGraph.fromNodeAndChildGraphs(
                                new PlannerGraph.DataNodeWithInfo(NodeInfo.INDEX_DATA, ImmutableList.copyOf(getUsedIndexes())),
                                ImmutableList.of())));
    }
}
