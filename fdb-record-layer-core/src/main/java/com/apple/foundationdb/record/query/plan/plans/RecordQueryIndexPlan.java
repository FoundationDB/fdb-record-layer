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
import com.apple.foundationdb.record.cursors.FallbackCursor;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.APIVersion;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexOrphanBehavior;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.ScanWithFetchMatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    public static final Logger LOGGER = LoggerFactory.getLogger(RecordQueryIndexPlan.class);
    protected static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Index-Plan");

    @Nonnull
    protected final String indexName;
    @Nullable
    private final KeyExpression commonPrimaryKey;
    @Nonnull
    protected final IndexScanParameters scanParameters;
    @Nonnull
    private RecordQueryPlannerConfiguration.IndexFetchMethod indexFetchMethod;
    protected final boolean reverse;
    protected final boolean strictlySorted;
    @Nonnull
    private final Optional<? extends ScanWithFetchMatchCandidate> matchCandidateOptional;
    @Nonnull
    private final Type resultType;

    public RecordQueryIndexPlan(@Nonnull final String indexName, @Nonnull final IndexScanParameters scanParameters, final boolean reverse) {
        this(indexName, null, scanParameters, RecordQueryPlannerConfiguration.IndexFetchMethod.SCAN_AND_FETCH, reverse, false);
    }

    public RecordQueryIndexPlan(@Nonnull final String indexName,
                                @Nullable final KeyExpression commonPrimaryKey,
                                @Nonnull final IndexScanParameters scanParameters,
                                @Nonnull final RecordQueryPlannerConfiguration.IndexFetchMethod useIndexPrefetch,
                                final boolean reverse,
                                final boolean strictlySorted) {
        this(indexName, commonPrimaryKey, scanParameters, useIndexPrefetch, reverse, strictlySorted, Optional.empty(), new Type.Any());
    }

    public RecordQueryIndexPlan(@Nonnull final String indexName,
                                @Nullable final KeyExpression commonPrimaryKey,
                                @Nonnull final IndexScanParameters scanParameters,
                                @Nonnull final RecordQueryPlannerConfiguration.IndexFetchMethod indexFetchMethod,
                                final boolean reverse,
                                final boolean strictlySorted,
                                @Nonnull final ScanWithFetchMatchCandidate matchCandidate,
                                @Nonnull final Type.Record resultType) {
        this(indexName, commonPrimaryKey, scanParameters, indexFetchMethod, reverse, strictlySorted, Optional.of(matchCandidate), resultType);
    }

    private RecordQueryIndexPlan(@Nonnull final String indexName,
                                 @Nullable final KeyExpression commonPrimaryKey,
                                 @Nonnull final IndexScanParameters scanParameters,
                                 @Nonnull final RecordQueryPlannerConfiguration.IndexFetchMethod indexFetchMethod,
                                 final boolean reverse,
                                 final boolean strictlySorted,
                                 @Nonnull final Optional<? extends ScanWithFetchMatchCandidate> matchCandidateOptional,
                                 @Nonnull final Type resultType) {
        this.indexName = indexName;
        this.commonPrimaryKey = commonPrimaryKey;
        this.scanParameters = scanParameters;
        this.indexFetchMethod = indexFetchMethod;
        this.reverse = reverse;
        this.strictlySorted = strictlySorted;
        this.matchCandidateOptional = matchCandidateOptional;
        this.resultType = resultType;
        if (indexFetchMethod != RecordQueryPlannerConfiguration.IndexFetchMethod.SCAN_AND_FETCH) {
            if (commonPrimaryKey == null) {
                logDebug("Index remote fetch cannot be used without a primary key. Falling back to regular scan.");
                this.indexFetchMethod = RecordQueryPlannerConfiguration.IndexFetchMethod.SCAN_AND_FETCH;
            }
            if (!scanParameters.getScanType().equals(IndexScanType.BY_VALUE)) {
                logDebug("Index remote fetch can only be used with VALUE index scan. Falling back to regular scan.");
                this.indexFetchMethod = RecordQueryPlannerConfiguration.IndexFetchMethod.SCAN_AND_FETCH;
            }
        }
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context,
                                                                     @Nullable final byte[] continuation, @Nonnull final ExecuteProperties executeProperties) {
        RecordQueryPlannerConfiguration.IndexFetchMethod fetchMethod = indexFetchMethod;
        // Check here to allow for the store API_VERSION to change
        if ((indexFetchMethod != RecordQueryPlannerConfiguration.IndexFetchMethod.SCAN_AND_FETCH) &&
                !store.getContext().getAPIVersion().isAtLeast(APIVersion.API_VERSION_7_1)) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn(KeyValueLogMessage.of("Index remote fetch can only be used with API_VERSION of at least 7.1. Falling back to regular scan.",
                        LogMessageKeys.PLAN_HASH, planHash(PlanHashKind.STRUCTURAL_WITHOUT_LITERALS)));
            }
            fetchMethod = RecordQueryPlannerConfiguration.IndexFetchMethod.SCAN_AND_FETCH;
        }

        switch (fetchMethod) {
            case SCAN_AND_FETCH:
                // Use the default implementation for index scan
                return RecordQueryPlanWithIndex.super.executePlan(store, context, continuation, executeProperties);
            case USE_REMOTE_FETCH:
                // Use index prefetch without fallback
                return executeUsingRemoteFetch(store, context, continuation, executeProperties);
            case USE_REMOTE_FETCH_WITH_FALLBACK:
                // Use Index prefetch and fall back to regular index scan.
                // Using the fallback mechanism here separates the execution part from the planning part
                // (No need to plan again) and from failures at other parts of the execution.
                try {
                    // The fallback cursor will handle failures that happen after the executeUsingIndexPrefetch call
                    return new FallbackCursor<>(
                            executeUsingRemoteFetch(store, context, continuation, executeProperties),
                            () -> RecordQueryPlanWithIndex.super.executePlan(store, context, continuation, executeProperties));
                } catch (Exception ex) {
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn(KeyValueLogMessage.of("Remote Fetch execution failed, falling back to Index scan",
                                LogMessageKeys.PLAN_HASH, planHash(PlanHashKind.STRUCTURAL_WITHOUT_LITERALS)), ex);
                    }
                    return RecordQueryPlanWithIndex.super.executePlan(store, context, continuation, executeProperties);
                }
            default:
                throw new RecordCoreException("Unknown useIndexPrefetch option").addLogInfo("option", indexFetchMethod);
        }
    }

    @Nonnull
    private <M extends Message> RecordCursor<QueryResult> executeUsingRemoteFetch(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context,
                                                                                  @Nullable final byte [] continuation, @Nonnull final ExecuteProperties executeProperties) {
        final RecordMetaData metaData = store.getRecordMetaData();
        final Index index = metaData.getIndex(indexName);
        final IndexScanBounds scanBounds = scanParameters.bind(store, index, context);

        // CommonPrimaryKey is nullable but is protected by the constructor in the case pf index prefetch
        return store.scanIndexRemoteFetch(index, scanBounds, Objects.requireNonNull(getCommonPrimaryKey()), continuation, executeProperties.asScanProperties(isReverse()), IndexOrphanBehavior.ERROR)
                .map(store::queriedRecord)
                .map(QueryResult::fromQueriedRecord);
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

    @Nullable
    public KeyExpression getCommonPrimaryKey() {
        return commonPrimaryKey;
    }

    @Nonnull
    @Override
    public IndexScanType getScanType() {
        return scanParameters.getScanType();
    }

    @Nonnull
    public RecordQueryPlannerConfiguration.IndexFetchMethod getIndexFetchMethod() {
        return indexFetchMethod;
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
        return new RecordQueryIndexPlan(indexName, getCommonPrimaryKey(), scanParameters, getIndexFetchMethod(), reverse, true);
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
                getCommonPrimaryKey(),
                getScanParameters(),
                getIndexFetchMethod(),
                isReverse(),
                isStrictlySorted(),
                matchCandidateOptional,
                resultType);
    }

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.ALL_FIELDS;
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return new QueriedValue(resultType);
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
                int planHash;
                if (scanParameters instanceof IndexScanComparisons) {
                    // Keep hash stable for change in representation.
                    // TODO: If there is another event that changes hashes or they become less critical in tests, this can be removed.
                    planHash = PlanHashable.objectsPlanHash(hashKind, BASE_HASH, indexName, getScanType(), getComparisons(), reverse, strictlySorted);
                } else {
                    planHash = PlanHashable.objectsPlanHash(hashKind, BASE_HASH, indexName, scanParameters, reverse, strictlySorted);
                }
                return planHash;
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
        if (!scanParameters.getScanType().equals(IndexScanType.BY_VALUE)) {
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
                                new PlannerGraph.DataNodeWithInfo(NodeInfo.INDEX_DATA, getResultType(), ImmutableList.copyOf(getUsedIndexes())),
                                ImmutableList.of())));
    }

    private void logDebug(final String staticMessage) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(KeyValueLogMessage.of(staticMessage,
                    LogMessageKeys.PLAN_HASH, planHash(PlanHashKind.STRUCTURAL_WITHOUT_LITERALS)));
        }
    }
}
