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

import com.apple.foundationdb.Range;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.CursorStreamingMode;
import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexFetchMethod;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorEndContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.cursors.FallbackCursor;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.planprotos.PRecordQueryIndexPlan;
import com.apple.foundationdb.record.planprotos.PRecordQueryPlan;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.APIVersion;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexOrphanBehavior;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanRange;
import com.apple.foundationdb.record.provider.foundationdb.KeyValueCursorBase;
import com.apple.foundationdb.record.provider.foundationdb.MultidimensionalIndexScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.UnsupportedRemoteFetchIndexException;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.AggregateIndexMatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRanges;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.FinalMemoizer;
import com.apple.foundationdb.record.query.plan.cascades.MatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.ExplainPlanVisitor;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphRewritable;
import com.apple.foundationdb.record.query.plan.cascades.expressions.AbstractRelationalExpressionWithoutChildren;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A query plan that outputs records pointed to by entries in a secondary index within some range.
 *
 * <p>
 * This scans a larger range internally when {@link IndexScanType#BY_VALUE_OVER_SCAN} is applied.
 * Semantically, this returns the same results as a {@link IndexScanType#BY_VALUE} over the same range.
 * However, internally, it will scan additional data for the purposes of loading that data into an
 * in-memory cache that lives within the FDB client.
 * </p>
 *
 * <p>
 * {@link IndexScanType#BY_VALUE_OVER_SCAN} is most useful in the following situation: a query with
 * parameters is planned once, and then executed multiple times in the same transaction with different
 * parameter values. In this case, the first time the plan is executed, this plan will incur additional
 * I/O in order to place entries in the cache, but subsequent executions of the plan may be able to be
 * served from this cache, saving time overall.
 * </p>
 *
 * <p>
 * This plan's continuations are also designed to be equivalent between {@link IndexScanType#BY_VALUE}
 * and {@link IndexScanType#BY_VALUE_OVER_SCAN} over the same scan range, so a query plan with a
 * {@link IndexScanType#BY_VALUE} can be executed, and then the continuation from that plan can be given
 * to an identical plan that substitutes the index scan with {@link IndexScanType#BY_VALUE_OVER_SCAN} (or vice versa).
 * </p>
 */
@API(API.Status.INTERNAL)
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class RecordQueryIndexPlan extends AbstractRelationalExpressionWithoutChildren implements RecordQueryPlanWithNoChildren,
                                                                                                 RecordQueryPlanWithComparisons,
                                                                                                 RecordQueryPlanWithIndex,
                                                                                                 PlannerGraphRewritable,
                                                                                                 RecordQueryPlanWithMatchCandidate,
                                                                                                 RecordQueryPlanWithConstraint {
    public static final Logger LOGGER = LoggerFactory.getLogger(RecordQueryIndexPlan.class);
    protected static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Record-Query-Index-Plan");

    protected final String indexName;
    @Nullable
    private final KeyExpression commonPrimaryKey;
    protected final IndexScanParameters scanParameters;
    private IndexFetchMethod indexFetchMethod;
    private final FetchIndexRecords fetchIndexRecords;
    protected final boolean reverse;
    protected final boolean strictlySorted;
    private final Optional<? extends MatchCandidate> matchCandidateOptional;
    private final Type resultType;
    private final QueryPlanConstraint constraint;

    @SuppressWarnings("this-escape")
    private final Supplier<ComparisonRanges> comparisonRangesSupplier = Suppliers.memoize(this::computeComparisonRanges);
    private final KeyValueCursorBase.SerializationMode serializationMode;

    public RecordQueryIndexPlan(final String indexName, final IndexScanParameters scanParameters, final boolean reverse) {
        this(indexName, null, scanParameters, IndexFetchMethod.SCAN_AND_FETCH, FetchIndexRecords.PRIMARY_KEY, reverse, false);
    }

    public RecordQueryIndexPlan(final String indexName,
                                @Nullable final KeyExpression commonPrimaryKey,
                                final IndexScanParameters scanParameters,
                                final IndexFetchMethod useIndexPrefetch,
                                final FetchIndexRecords fetchIndexRecords,
                                final boolean reverse,
                                final boolean strictlySorted) {
        this(indexName, commonPrimaryKey, scanParameters, useIndexPrefetch, fetchIndexRecords, reverse, strictlySorted, Optional.empty(), new Type.Any(), QueryPlanConstraint.noConstraint());
    }

    public RecordQueryIndexPlan(final String indexName,
                                @Nullable final KeyExpression commonPrimaryKey,
                                final IndexScanParameters scanParameters,
                                final IndexFetchMethod indexFetchMethod,
                                final FetchIndexRecords fetchIndexRecords,
                                final boolean reverse,
                                final boolean strictlySorted,
                                final MatchCandidate matchCandidate,
                                final Type.Record resultType,
                                final QueryPlanConstraint constraint) {
        this(indexName, commonPrimaryKey, scanParameters, indexFetchMethod, fetchIndexRecords, reverse, strictlySorted, Optional.of(matchCandidate), resultType, constraint);
    }

    protected RecordQueryIndexPlan(final PlanSerializationContext serializationContext,
                                   final PRecordQueryIndexPlan recordQueryIndexPlanProto) {
        this(Objects.requireNonNull(recordQueryIndexPlanProto.getIndexName()),
                recordQueryIndexPlanProto.hasCommonPrimaryKey() ? KeyExpression.fromProto(recordQueryIndexPlanProto.getCommonPrimaryKey()) : null,
                IndexScanParameters.fromIndexScanParametersProto(serializationContext, Objects.requireNonNull(recordQueryIndexPlanProto.getScanParameters())),
                IndexFetchMethod.fromProto(serializationContext, Objects.requireNonNull(recordQueryIndexPlanProto.getIndexFetchMethod())),
                FetchIndexRecords.fromProto(serializationContext, Objects.requireNonNull(recordQueryIndexPlanProto.getFetchIndexRecords())),
                recordQueryIndexPlanProto.getReverse(),
                recordQueryIndexPlanProto.getStrictlySorted(),
                Optional.empty(),
                Type.fromTypeProto(serializationContext, Objects.requireNonNull(recordQueryIndexPlanProto.getResultType())),
                QueryPlanConstraint.fromProto(serializationContext, Objects.requireNonNull(recordQueryIndexPlanProto.getConstraint())));
    }

    @VisibleForTesting
    public RecordQueryIndexPlan(final String indexName,
                                @Nullable final KeyExpression commonPrimaryKey,
                                final IndexScanParameters scanParameters,
                                final IndexFetchMethod indexFetchMethod,
                                final FetchIndexRecords fetchIndexRecords,
                                final boolean reverse,
                                final boolean strictlySorted,
                                final Optional<? extends MatchCandidate> matchCandidateOptional,
                                final Type resultType,
                                final QueryPlanConstraint constraint) {
        this(indexName, commonPrimaryKey, scanParameters, indexFetchMethod, fetchIndexRecords, reverse, strictlySorted, matchCandidateOptional, resultType, constraint, KeyValueCursorBase.SerializationMode.TO_NEW);
    }

    @VisibleForTesting
    @SuppressWarnings("this-escape")
    public RecordQueryIndexPlan(final String indexName,
                                @Nullable final KeyExpression commonPrimaryKey,
                                final IndexScanParameters scanParameters,
                                final IndexFetchMethod indexFetchMethod,
                                final FetchIndexRecords fetchIndexRecords,
                                final boolean reverse,
                                final boolean strictlySorted,
                                final Optional<? extends MatchCandidate> matchCandidateOptional,
                                final Type resultType,
                                final QueryPlanConstraint constraint,
                                final KeyValueCursorBase.SerializationMode serializationMode) {
        this.indexName = indexName;
        this.commonPrimaryKey = commonPrimaryKey;
        this.scanParameters = scanParameters;
        this.indexFetchMethod = indexFetchMethod;
        this.fetchIndexRecords = fetchIndexRecords;
        this.reverse = reverse;
        this.strictlySorted = strictlySorted;
        this.matchCandidateOptional = matchCandidateOptional;
        this.resultType = resultType;
        if (indexFetchMethod != IndexFetchMethod.SCAN_AND_FETCH) {
            if (!scanParameters.getScanType().equals(IndexScanType.BY_VALUE)) {
                logDebug("Index remote fetch can only be used with VALUE index scan. Falling back to regular scan.");
                this.indexFetchMethod = IndexFetchMethod.SCAN_AND_FETCH;
            }
        }
        this.constraint = constraint;
        this.serializationMode = serializationMode;
    }

    @Override
    public <M extends Message> RecordCursor<QueryResult> executePlan(final FDBRecordStoreBase<M> store, final EvaluationContext context,
                                                                     @Nullable final byte[] continuation, final ExecuteProperties executeProperties) {
        IndexFetchMethod fetchMethod = indexFetchMethod;
        // Check here to allow for the store API_VERSION to change
        if ((indexFetchMethod != IndexFetchMethod.SCAN_AND_FETCH) &&
                !store.getContext().getAPIVersion().isAtLeast(APIVersion.API_VERSION_7_1)) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn(KeyValueLogMessage.of("Index remote fetch can only be used with API_VERSION of at least 7.1. Falling back to regular scan.",
                        LogMessageKeys.PLAN_HASH, planHash(PlanHashable.CURRENT_FOR_CONTINUATION)));
            }
            fetchMethod = IndexFetchMethod.SCAN_AND_FETCH;
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
                            lastSuccessfulResult -> fallBackContinueFrom(store, context, continuation, executeProperties, lastSuccessfulResult));
                } catch (UnsupportedRemoteFetchIndexException ex) {
                    // In this case (e.g. the index maintainer does not support remote fetch), log as info
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info(KeyValueLogMessage.of("Remote fetch unsupported, continuing with Index scan",
                                LogMessageKeys.MESSAGE, ex.getMessage(),
                                LogMessageKeys.INDEX_NAME, indexName));
                    }
                    return RecordQueryPlanWithIndex.super.executePlan(store, context, continuation, executeProperties);
                } catch (Exception ex) {
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn(KeyValueLogMessage.of("Remote Fetch execution failed, falling back to Index scan",
                                LogMessageKeys.PLAN_HASH, planHash(PlanHashable.CURRENT_FOR_CONTINUATION)), ex);
                    }
                    return RecordQueryPlanWithIndex.super.executePlan(store, context, continuation, executeProperties);
                }
            default:
                throw new RecordCoreException("Unknown useIndexPrefetch option").addLogInfo("option", indexFetchMethod);
        }
    }

    private <M extends Message> RecordCursor<QueryResult> executeUsingRemoteFetch(final FDBRecordStoreBase<M> store, final EvaluationContext context,
                                                                                  @Nullable final byte [] continuation, final ExecuteProperties executeProperties) {
        final RecordMetaData metaData = store.getRecordMetaData();
        final Index index = metaData.getIndex(indexName);
        final IndexScanBounds scanBounds = scanParameters.bind(store, index, context);
        return store.scanIndexRemoteFetch(index, scanBounds, continuation, executeProperties.asScanProperties(isReverse()), IndexOrphanBehavior.ERROR)
                .map(store::queriedRecord)
                .map(queriedRecord -> QueryResult.fromQueriedRecord(resultType, context, queriedRecord));
    }

    @Override
    public <M extends Message> RecordCursor<IndexEntry> executeEntries(FDBRecordStoreBase<M> store, EvaluationContext context,
                                                                       @Nullable byte[] continuation, ExecuteProperties executeProperties) {
        final RecordMetaData metaData = store.getRecordMetaData();
        final Index index = metaData.getIndex(indexName);
        final IndexScanBounds scanBounds = scanParameters.bind(store, index, context);
        if (!IndexScanType.BY_VALUE_OVER_SCAN.equals(getScanType())) {
            return store.scanIndex(index, scanBounds, continuation, executeProperties.asScanProperties(reverse));
        }

        // Evaluate the scan bounds. Again, this optimization can only be done if we have a scan range
        if (!(scanBounds instanceof IndexScanRange)) {
            return store.scanIndex(index, scanBounds, continuation, executeProperties.asScanProperties(reverse));
        }

        // Try to widen the scan range to include everything up
        IndexScanRange indexScanRange = (IndexScanRange) scanBounds;
        TupleRange tupleScanRange = indexScanRange.getScanRange();
        TupleRange widenedScanRange = widenRange(tupleScanRange);
        if (widenedScanRange == null) {
            // Unable to widen the range. Fall back to the original execution.
            return store.scanIndex(index, scanBounds, continuation, executeProperties.asScanProperties(reverse));
        }

        return executeEntriesWithOverScan(tupleScanRange, widenedScanRange, store, index, continuation, executeProperties);
    }

    // NullAway does not reliably track @Nullable on the byte[] continuation parameter across the call into
    // ByteArrayUtil2.loggable below (a known array-type tracking gap); continuation is narrowed non-null by
    // the surrounding ternary.
    @SuppressWarnings({"resource", "NullAway"})
    private <M extends Message> RecordCursor<IndexEntry> executeEntriesWithOverScan(TupleRange tupleScanRange, TupleRange widenedScanRange,
                                                                                    FDBRecordStoreBase<M> store, Index index,
                                                                                    @Nullable byte[] continuation, ExecuteProperties executeProperties) {
        final byte[] prefixBytes = getRangePrefixBytes(tupleScanRange);
        final IndexScanContinuationConvertor continuationConvertor = new IndexScanContinuationConvertor(prefixBytes, serializationMode);

        // Scan a wider range, and then halt when either this scans outside the given range
        final IndexScanRange newScanRange = new IndexScanRange(IndexScanType.BY_VALUE, widenedScanRange);
        final Range originalKeyRange = tupleScanRange.toRange();
        // Make sure to use ITERATOR mode so that the results are paginated (by the FDB Java bindings). This
        // streaming mode limits the amount of data we might read beyond that which is returned to one page
        // from the database
        final ExecuteProperties newExecuteProperties = executeProperties
                .setDefaultCursorStreamingMode(CursorStreamingMode.ITERATOR);
        final ScanProperties scanProperties = newExecuteProperties.asScanProperties(isReverse());

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(KeyValueLogMessage.of("Executing value index plan with over-scan",
                    LogMessageKeys.INDEX_NAME, indexName,
                    LogMessageKeys.NEXT_CONTINUATION, continuation == null ? "null" : ByteArrayUtil2.loggable(continuation),
                    LogMessageKeys.SCAN_PROPERTIES, scanProperties.toString(),
                    LogMessageKeys.ORIGINAL_RANGE, tupleScanRange.toString(),
                    LogMessageKeys.WIDENED_TUPLE_RANGE, widenedScanRange.toString()));
        }

        return store.scanIndex(index, newScanRange, continuationConvertor.unwrapContinuation(continuation), scanProperties).mapResult(result -> {
            if (!result.hasNext()) {
                RecordCursorContinuation wrappedContinuation = continuationConvertor.wrapContinuation(result.getContinuation());
                return result.withContinuation(wrappedContinuation);
            }
            // Check if the result is in the range the user actually cares about. If so, return it. Otherwise,
            // terminate the scan.
            // hasNext() was checked true above, so get() is guaranteed non-null here.
            IndexEntry entry = Objects.requireNonNull(result.get());
            byte[] keyBytes = entry.getKey().pack();
            if ((isReverse() && ByteArrayUtil.compareUnsigned(originalKeyRange.begin, keyBytes) <= 0) || (!isReverse() && ByteArrayUtil.compareUnsigned(originalKeyRange.end, keyBytes) > 0)) {
                RecordCursorContinuation wrappedContinuation = continuationConvertor.wrapContinuation(result.getContinuation());
                return result.withContinuation(wrappedContinuation);
            } else {
                return RecordCursorResult.exhausted();
            }
        });
    }
    
    @Override
    public String getIndexName() {
        return indexName;
    }

    public IndexScanParameters getScanParameters() {
        return scanParameters;
    }

    @Nullable
    public KeyExpression getCommonPrimaryKey() {
        return commonPrimaryKey;
    }

    @Override
    public IndexScanType getScanType() {
        return scanParameters.getScanType();
    }

    public IndexFetchMethod getIndexFetchMethod() {
        return indexFetchMethod;
    }

    @Override
    public FetchIndexRecords getFetchIndexRecords() {
        return fetchIndexRecords;
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
    public int maxCardinality(RecordMetaData metaData) {
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

    @Override
    public Optional<? extends MatchCandidate> getMatchCandidateMaybe() {
        return matchCandidateOptional;
    }

    @Override
    public RecordQueryIndexPlan strictlySorted(final FinalMemoizer memoizer) {
        return new RecordQueryIndexPlan(indexName, getCommonPrimaryKey(), scanParameters, getIndexFetchMethod(), fetchIndexRecords, reverse, true, matchCandidateOptional, resultType, constraint);
    }

    @Override
    public boolean hasLoadBykeys() {
        return false;
    }

    @Override
    public Set<CorrelationIdentifier> computeCorrelatedToWithoutChildren() {
        return scanParameters.getCorrelatedTo();
    }

    @Override
    public RecordQueryIndexPlan translateCorrelations(final TranslationMap translationMap,
                                                      final boolean shouldSimplifyValues,
                                                      final List<? extends Quantifier> translatedQuantifiers) {
        Verify.verify(translatedQuantifiers.isEmpty());
        if (translationMap.definesOnlyIdentities()) {
            return this;
        }
        return withIndexScanParameters(scanParameters.translateCorrelations(translationMap, shouldSimplifyValues));
    }

    @Override
    public boolean canBeMinimized() {
        return matchCandidateOptional.isPresent();
    }

    @Override
    public RecordQueryIndexPlan minimize(final List<Quantifier.Physical> newQuantifiers) {
        Verify.verify(newQuantifiers.isEmpty());
        return new RecordQueryIndexPlan(indexName, commonPrimaryKey, scanParameters, indexFetchMethod,
                fetchIndexRecords, reverse, strictlySorted, Optional.empty(), resultType,
                constraint);
    }

    protected RecordQueryIndexPlan withIndexScanParameters(final IndexScanParameters newIndexScanParameters) {
        return new RecordQueryIndexPlan(indexName, commonPrimaryKey, newIndexScanParameters, indexFetchMethod,
                fetchIndexRecords, reverse, strictlySorted, matchCandidateOptional, resultType, constraint);
    }

    @Override
    public AvailableFields getAvailableFields() {
        return AvailableFields.ALL_FIELDS;
    }

    @Override
    public Value getResultValue() {
        return new QueriedValue(resultType);
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
        RecordQueryIndexPlan that = (RecordQueryIndexPlan) otherExpression;
        return reverse == that.reverse &&
               strictlySorted == that.strictlySorted &&
               indexFetchMethod == that.indexFetchMethod &&
               fetchIndexRecords == that.fetchIndexRecords &&
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
    public int computeHashCodeWithoutChildren() {
        return Objects.hash(indexName, scanParameters, indexFetchMethod.name(), fetchIndexRecords.name(),
                reverse, strictlySorted);
    }

    @Override
    public int planHash(final PlanHashMode mode) {
        switch (mode.getKind()) {
            case LEGACY:
                return indexName.hashCode() + scanParameters.planHash(mode) + (reverse ? 1 : 0);
            case FOR_CONTINUATION:
                int planHash;
                if (scanParameters instanceof IndexScanComparisons) {
                    // Keep hash stable for change in representation.
                    // TODO: If there is another event that changes hashes or they become less critical in tests, this can be removed.
                    planHash = PlanHashable.objectsPlanHash(mode, BASE_HASH, indexName, getScanType(), getScanComparisons(), reverse, strictlySorted);
                } else {
                    planHash = PlanHashable.objectsPlanHash(mode, BASE_HASH, indexName, scanParameters, reverse, strictlySorted);
                }
                return planHash;
            default:
                throw new UnsupportedOperationException("Hash kind " + mode.name() + " is not supported");
        }
    }

    @Override
    public String toString() {
        return ExplainPlanVisitor.toStringForDebugging(this);
    }

    @Override
    public void logPlanStructure(StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_INDEX);
    }

    @Override
    public boolean hasScanComparisons() {
        return scanParameters.hasScanComparisons();
    }

    @Override
    public ScanComparisons getScanComparisons() {
        Verify.verify(hasScanComparisons());
        return scanParameters.getScanComparisons();
    }

    @Override
    public ComparisonRanges getComparisonRanges() {
        return comparisonRangesSupplier.get();
    }

    private ComparisonRanges computeComparisonRanges() {
        if (scanParameters instanceof MultidimensionalIndexScanComparisons) {
            final MultidimensionalIndexScanComparisons mdIndexScanComparisons =
                    (MultidimensionalIndexScanComparisons)scanParameters;
            final ImmutableList.Builder<ComparisonRange> comparisonRangeBuilder = ImmutableList.builder();
            final ComparisonRanges prefixComparisonRanges =
                    ComparisonRanges.from(mdIndexScanComparisons.getPrefixScanComparisons());
            comparisonRangeBuilder.addAll(prefixComparisonRanges.getRanges());
            final List<ComparisonRange> dimensionComparisonRanges =
                    mdIndexScanComparisons.getDimensionsScanComparisons()
                            .stream()
                            .flatMap(dimensionScanComparisons -> ComparisonRanges.from(dimensionScanComparisons)
                                    .getRanges().stream())
                            .collect(ImmutableList.toImmutableList());
            final ComparisonRanges suffixComparisonRanges =
                    ComparisonRanges.from(mdIndexScanComparisons.getSuffixScanComparisons());
            return new ComparisonRanges(ImmutableList.<ComparisonRange>builder()
                    .addAll(prefixComparisonRanges.getRanges())
                    .addAll(dimensionComparisonRanges)
                    .addAll(suffixComparisonRanges.getRanges())
                    .build());
        }

        return ComparisonRanges.from(getScanComparisons());
    }

    @Override
    public boolean hasComparisonRanges() {
        if (scanParameters instanceof MultidimensionalIndexScanComparisons) {
            return true;
        }
        return hasScanComparisons();
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
    @Override
    public PlannerGraph createIndexPlannerGraph(RecordQueryPlan identity,
                                                final NodeInfo nodeInfo,
                                                final List<String> additionalDetails,
                                                final Map<String, Attribute> additionalAttributeMap) {
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

        final var matchCandidateOptional = getMatchCandidateMaybe();
        final String extendedIndexName;
        if (matchCandidateOptional.isPresent() &&
                matchCandidateOptional.get() instanceof AggregateIndexMatchCandidate) {
            final var aggregateIndexMatchCandidate = (AggregateIndexMatchCandidate)matchCandidateOptional.get();
            extendedIndexName = aggregateIndexMatchCandidate.toString();
        } else {
            extendedIndexName = getIndexName();
        }

        return PlannerGraph.fromNodeAndChildGraphs(
                new PlannerGraph.OperatorNodeWithInfo(identity,
                        nodeInfo,
                        detailsBuilder.build(),
                        attributeMapBuilder.build()),
                ImmutableList.of(
                        PlannerGraph.fromNodeAndChildGraphs(
                                new PlannerGraph.DataNodeWithInfo(NodeInfo.INDEX_DATA, getResultType(), ImmutableList.of(extendedIndexName)),
                                ImmutableList.of())));
    }

    /*
     * Create a fallback cursor that continues a failed remote fetch scan. The continuation will be used in case there
     * is no lastSuccessfulResult. When lastSuccessfulResult is not null, the scan will start from it.
     */
    private <M extends Message> RecordCursor<QueryResult> fallBackContinueFrom(final FDBRecordStoreBase<M> store,
                                                                               final EvaluationContext context,
                                                                               @Nullable final byte[] continuation,
                                                                               final ExecuteProperties executeProperties,
                                                                               @Nullable final RecordCursorResult<QueryResult> lastSuccessfulResult) {
        if (lastSuccessfulResult == null) {
            // The fallbackCursor did not have any result from the primary yet - just fallback to the index scan
            return RecordQueryPlanWithIndex.super.executePlan(store, context, continuation, executeProperties);
        } else {
            // Use the continuation from the last result to continue from
            return RecordQueryPlanWithIndex.super.executePlan(store, context, lastSuccessfulResult.getContinuation().toBytes(), executeProperties);
        }
    }


    private void logDebug(final String staticMessage) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(KeyValueLogMessage.of(staticMessage,
                    LogMessageKeys.PLAN_HASH, planHash(PlanHashable.CURRENT_FOR_CONTINUATION)));
        }
    }

    @Override
    public QueryPlanConstraint getConstraint() {
        return constraint;
    }

    @Override
    public Message toProto(final PlanSerializationContext serializationContext) {
        return toRecordQueryIndexPlanProto(serializationContext);
    }

    public PRecordQueryIndexPlan toRecordQueryIndexPlanProto(final PlanSerializationContext serializationContext) {
        final var builder = PRecordQueryIndexPlan.newBuilder()
                .setIndexName(indexName);
        if (commonPrimaryKey != null) {
            builder.setCommonPrimaryKey(commonPrimaryKey.toKeyExpression());
        }
        builder.setScanParameters(scanParameters.toIndexScanParametersProto(serializationContext));
        builder.setIndexFetchMethod(indexFetchMethod.toProto(serializationContext));
        builder.setFetchIndexRecords(fetchIndexRecords.toProto(serializationContext));
        builder.setReverse(reverse);
        builder.setStrictlySorted(strictlySorted);
        builder.setResultType(resultType.toTypeProto(serializationContext));
        builder.setConstraint(constraint.toProto(serializationContext));
        return builder.build();
    }

    @Override
    public PRecordQueryPlan toRecordQueryPlanProto(final PlanSerializationContext serializationContext) {
        return PRecordQueryPlan.newBuilder().setRecordQueryIndexPlan(toRecordQueryIndexPlanProto(serializationContext)).build();
    }

    public static RecordQueryIndexPlan fromProto(final PlanSerializationContext serializationContext,
                                                 final PRecordQueryIndexPlan recordQueryIndexPlanProto) {
        return new RecordQueryIndexPlan(serializationContext, recordQueryIndexPlanProto);
    }

    /**
     * Continuation convertor to transform the continuations from the physical (wider) scan and the logical (narrower)
     * scan.
     */
    private static class IndexScanContinuationConvertor implements RecordCursor.ContinuationConvertor {
        private final byte[] prefixBytes;
        private final KeyValueCursorBase.SerializationMode serializationMode;

        public IndexScanContinuationConvertor(byte[] prefixBytes, final KeyValueCursorBase.SerializationMode serializationMode) {
            this.prefixBytes = prefixBytes;
            this.serializationMode = serializationMode;
        }

        @Nullable
        @Override
        // NullAway doesn't reliably track @Nullable on byte[] return types; this
        // method is correctly annotated @Nullable above.
        @SuppressWarnings("NullAway")
        public byte[] unwrapContinuation(@Nullable final byte[] continuation) {
            if (continuation == null) {
                return null;
            }
            // Add the prefix back to the inner continuation
            byte[] innerContinuation = KeyValueCursorBase.Continuation.getInnerContinuation(continuation);
            return new KeyValueCursorBase.Continuation(ByteArrayUtil.join(prefixBytes, innerContinuation), 0, serializationMode).toBytes();
        }

        @Override
        // NullAway does not reliably track @Nullable on the byte[] continuationBytes local across the call
        // into ByteArrayUtil.startsWith below (a known array-type tracking gap); continuationBytes is
        // narrowed non-null by the short-circuiting null check just before it.
        @SuppressWarnings("NullAway")
        public RecordCursorContinuation wrapContinuation(final RecordCursorContinuation continuation) {
            if (continuation.isEnd()) {
                return continuation;
            }
            @Nullable byte[] continuationBytes = KeyValueCursorBase.Continuation.getInnerContinuation(continuation.toBytes());
            if (continuationBytes != null && ByteArrayUtil.startsWith(continuationBytes, prefixBytes)) {
                // Strip away the prefix. Note that ByteStrings re-use the underlying ByteArray, so this can
                // save a copy.
                return new IndexScanContinuationConvertor.PrefixRemovingContinuation(continuation, prefixBytes.length, serializationMode);
            } else {
                // This key does not begin with the prefix. Return an END continuation to indicate that the
                // scan is over.
                return RecordCursorEndContinuation.END;
            }
        }

        private static class PrefixRemovingContinuation implements RecordCursorContinuation {
            private final RecordCursorContinuation baseContinuation;
            private final int prefixLength;

            @SuppressWarnings("squid:S3077") // array immutable once initialized, so AtomicByteArray not necessary
            @Nullable
            private volatile byte[] bytes;
            private final KeyValueCursorBase.SerializationMode serializationMode;

            // array immutable once initialized, so AtomicByteArray not
            // necessary; NullAway doesn't reliably track that this
            // field is already declared @Nullable, so it doesn't
            // need constructor initialization.
            @SuppressWarnings({"squid:S3077", "NullAway"})
            private PrefixRemovingContinuation(RecordCursorContinuation baseContinuation, int prefixLength, KeyValueCursorBase.SerializationMode serializationMode) {
                this.baseContinuation = baseContinuation;
                this.prefixLength = prefixLength;
                this.serializationMode = serializationMode;
            }

            @Nullable
            @Override
            public byte[] toBytes() {
                if (bytes == null) {
                    synchronized (this) {
                        if (bytes == null) {
                            bytes = new KeyValueCursorBase.Continuation(KeyValueCursorBase.Continuation.getInnerContinuation(baseContinuation.toBytes()), prefixLength, serializationMode).toBytes();
                        }
                    }
                }
                return bytes;
            }

            @Override
            public ByteString toByteString() {
                @Nullable byte[] bytes1 = toBytes();
                return bytes1 == null ? ByteString.EMPTY : ByteString.copyFrom(bytes1);
            }

            @Override
            public boolean isEnd() {
                return false;
            }
        }
    }

    @Nullable
    private TupleRange widenRange(TupleRange originalRange) {
        if (originalRange.getLowEndpoint() == EndpointType.PREFIX_STRING || originalRange.getHighEndpoint() == EndpointType.PREFIX_STRING) {
            return null;
        }

        // Widen the range so that the we can from the original location to the end of the index. Note that we will
        // stop executing the scan once we stop getting results within the original range, so even though we're requesting
        // a fairly large range, the actual extra work done shouldn't be this bad.
        if (isReverse()) {
            return new TupleRange(null, originalRange.getHigh(), EndpointType.TREE_START, originalRange.getHighEndpoint());
        } else {
            return new TupleRange(originalRange.getLow(), null, originalRange.getLowEndpoint(), EndpointType.TREE_END);
        }
    }

    private static byte[] getRangePrefixBytes(TupleRange tupleRange) {
        byte[] lowBytes = tupleRange.getLow() == null ? null : tupleRange.getLow().pack();
        byte[] highBytes = tupleRange.getHigh() == null ? null : tupleRange.getHigh().pack();
        if (lowBytes == null || highBytes == null) {
            return new byte[0];
        }
        int i = 0;
        while (i < lowBytes.length && i < highBytes.length && lowBytes[i] == highBytes[i]) {
            i++;
        }
        return Arrays.copyOfRange(lowBytes, 0, i);
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PRecordQueryIndexPlan, RecordQueryIndexPlan> {
        @Override
        public Class<PRecordQueryIndexPlan> getProtoMessageClass() {
            return PRecordQueryIndexPlan.class;
        }

        @Override
        public RecordQueryIndexPlan fromProto(final PlanSerializationContext serializationContext,
                                              final PRecordQueryIndexPlan recordQueryIndexPlanProto) {
            return RecordQueryIndexPlan.fromProto(serializationContext, recordQueryIndexPlanProto);
        }
    }
}
