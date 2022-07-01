/*
 * RecordQueryOverscanIndexPlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorEndContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanRange;
import com.apple.foundationdb.record.query.plan.AvailableFields;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.MatchCandidate;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.TranslationMap;
import com.apple.foundationdb.record.query.plan.cascades.explain.Attribute;
import com.apple.foundationdb.record.query.plan.cascades.explain.NodeInfo;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraph;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Variant of {@link RecordQueryIndexPlan} that scans a larger range internally. Semantically, this
 * returns the same results as a {@link RecordQueryIndexPlan} over the same range. However, internally,
 * it will scan additional data for the purposes of loading that data into an in-memory cache that
 * lives within the FDB client.
 *
 * <p>
 * This is most useful in the following situation: a query with parameters is planned once, and then
 * executed multiple times in the same transaction with different parameter values. In this case, the
 * first time the plan is executed, this plan will incur additional I/O in order to place entries in
 * the cache, but subsequent executions of the plan may be able to be served from this cache, saving
 * time overall.
 * </p>
 *
 * <p>
 * This plan's continuations are also designed to be equivalent to a {@link RecordQueryIndexPlan} over the
 * same scan range, so a query plan with a {@link RecordQueryIndexPlan} can be executed, and then the
 * continuation from that plan can be given to an identical plan that substitutes the index scan
 * with this plan (or vice versa).
 * </p>
 */
@API(API.Status.INTERNAL)
public class RecordQueryOverscanIndexPlan implements RecordQueryPlanWithIndex, RecordQueryPlanWithNoChildren {
    private final RecordQueryIndexPlan originalIndexPlan;

    @API(API.Status.INTERNAL)
    public RecordQueryOverscanIndexPlan(RecordQueryIndexPlan originalIndexPlan) {
        this.originalIndexPlan = originalIndexPlan;
    }

    @Nonnull
    @Override
    public <M extends Message> RecordCursor<IndexEntry> executeEntries(@Nonnull final FDBRecordStoreBase<M> store,
                                                                       @Nonnull final EvaluationContext context,
                                                                       @Nullable final byte[] continuation,
                                                                       @Nonnull final ExecuteProperties executeProperties) {
        // This plan can only be applied to BY_VALUE index scans
        if (!IndexScanType.BY_VALUE.equals(originalIndexPlan.getScanType())) {
            return originalIndexPlan.executeEntries(store, context, continuation, executeProperties);
        }

        // Evaluate the scan bounds. Again, this optimization can only be done if we have a scan range
        Index index = store.getRecordMetaData().getIndex(getIndexName());
        IndexScanBounds scanBounds = originalIndexPlan.getScanParameters().bind(store, index, context);
        if (!(scanBounds instanceof IndexScanRange)) {
            return originalIndexPlan.executeEntries(store, context, continuation, executeProperties);
        }

        // Try to widen the scan range to include everything up
        IndexScanRange indexScanRange = (IndexScanRange) scanBounds;
        TupleRange tupleScanRange = indexScanRange.getScanRange();
        TupleRange widenedScanRange = widenRange(tupleScanRange);
        if (widenedScanRange == null) {
            // Unable to widen the range. Fall back to the original execution.
            return originalIndexPlan.executeEntries(store, context, continuation, executeProperties);
        }

        final byte[] prefixBytes = getRangePrefixBytes(tupleScanRange);
        final IndexScanContinuationConvertor continuationConvertor = new IndexScanContinuationConvertor(prefixBytes);

        // Scan a wider range, and then halt when either this scans outside the given range
        final IndexScanRange newScanRange = new IndexScanRange(IndexScanType.BY_VALUE, widenedScanRange);
        final Range originalKeyRange = tupleScanRange.toRange();
        // Make sure to use ITERATOR mode so that the results are paginated (by the FDB Java bindings). This
        // streaming mode limits the amount of data we might read beyond that which is returned to one page
        // from the database
        final ExecuteProperties newExecuteProperties = executeProperties
                .setDefaultCursorStreamingMode(CursorStreamingMode.ITERATOR);
        final ScanProperties scanProperties = newExecuteProperties.asScanProperties(isReverse());
        return store.scanIndex(index, newScanRange, continuationConvertor.unwrapContinuation(continuation), scanProperties).mapResult(result -> {
            if (!result.hasNext()) {
                RecordCursorContinuation wrappedContinuation = continuationConvertor.wrapContinuation(result.getContinuation());
                return result.withContinuation(wrappedContinuation);
            }
            // Check if the result is in the range the user actually cares about. If so, return it. Otherwise,
            // terminate the scan.
            IndexEntry entry = result.get();
            byte[] keyBytes = entry.getKey().pack();
            if ((isReverse() && ByteArrayUtil.compareUnsigned(originalKeyRange.begin, keyBytes) <= 0)
                    || (!isReverse() && ByteArrayUtil.compareUnsigned(originalKeyRange.end, keyBytes) > 0)) {
                RecordCursorContinuation wrappedContinuation = continuationConvertor.wrapContinuation(result.getContinuation());
                return result.withContinuation(wrappedContinuation);
            } else {
                return RecordCursorResult.exhausted();
            }
        });
    }

    /**
     * Continuation convertor to transform the continuations from the physical (wider) scan and the logical (narrower)
     * scan.
     */
    private static class IndexScanContinuationConvertor implements RecordCursor.ContinuationConvertor {
        @Nonnull
        private final byte[] prefixBytes;

        public IndexScanContinuationConvertor(@Nonnull byte[] prefixBytes) {
            this.prefixBytes = prefixBytes;
        }

        @Nullable
        @Override
        public byte[] unwrapContinuation(@Nullable final byte[] continuation) {
            if (continuation == null) {
                return null;
            }
            // Add the prefix back to the inner continuation
            return ByteArrayUtil.join(prefixBytes, continuation);
        }

        @Override
        public RecordCursorContinuation wrapContinuation(@Nonnull final RecordCursorContinuation continuation) {
            if (continuation.isEnd()) {
                return continuation;
            }
            final byte[] continuationBytes = continuation.toBytes();
            if (continuationBytes != null && ByteArrayUtil.startsWith(continuationBytes, prefixBytes)) {
                // Strip away the prefix. Note that ByteStrings re-use the underlying ByteArray, so this can
                // save a copy.
                return new PrefixRemovingContinuation(continuation, prefixBytes.length);
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

            private PrefixRemovingContinuation(RecordCursorContinuation baseContinuation, int prefixLength) {
                this.baseContinuation = baseContinuation;
                this.prefixLength = prefixLength;
            }

            @Nullable
            @Override
            public byte[] toBytes() {
                if (bytes == null) {
                    synchronized (this) {
                        if (bytes == null) {
                            byte[] baseContinuationBytes = baseContinuation.toBytes();
                            if (baseContinuationBytes == null) {
                                return null;
                            }
                            this.bytes = Arrays.copyOfRange(baseContinuationBytes, prefixLength, baseContinuationBytes.length);
                        }
                    }
                }
                return bytes;
            }

            @Nonnull
            @Override
            public ByteString toByteString() {
                return baseContinuation.toByteString().substring(prefixLength);
            }

            @Override
            public boolean isEnd() {
                return false;
            }
        }
    }

    @Nullable
    private TupleRange widenRange(@Nonnull TupleRange originalRange) {
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

    @Nonnull
    public RecordQueryIndexPlan getIndexPlan() {
        return originalIndexPlan;
    }

    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        // This returns the original plan plan's plan hash because the two plans should be semantically
        // identical and continuations should be compatible, so swapping out one plan for the other should
        // always be okay.
        return originalIndexPlan.planHash(hashKind);
    }

    @Nonnull
    @Override
    public Set<CorrelationIdentifier> getCorrelatedTo() {
        return originalIndexPlan.getCorrelatedTo();
    }

    @Nonnull
    @Override
    public Value getResultValue() {
        return originalIndexPlan.getResultValue();
    }

    @Nonnull
    @Override
    public List<? extends Quantifier> getQuantifiers() {
        return originalIndexPlan.getQuantifiers();
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull final RelationalExpression other, @Nonnull final AliasMap equivalences) {
        return originalIndexPlan.equalsWithoutChildren(other, equivalences);
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object o) {
        return structuralEquals(o);
    }

    @Override
    public int hashCode() {
        return structuralHashCode();
    }

    @Override
    public int hashCodeWithoutChildren() {
        return originalIndexPlan.hashCodeWithoutChildren();
    }

    @Override
    public boolean isReverse() {
        return originalIndexPlan.isReverse();
    }

    @Override
    public boolean hasRecordScan() {
        return originalIndexPlan.hasRecordScan();
    }

    @Override
    public boolean hasFullRecordScan() {
        return originalIndexPlan.hasFullRecordScan();
    }

    @Override
    public boolean hasIndexScan(@Nonnull final String indexName) {
        return originalIndexPlan.hasIndexScan(indexName);
    }

    @Nonnull
    @Override
    public Set<String> getUsedIndexes() {
        return originalIndexPlan.getUsedIndexes();
    }

    @Override
    public boolean hasLoadBykeys() {
        return originalIndexPlan.hasLoadBykeys();
    }

    @Override
    public String toString() {
        return "Overscan(" + originalIndexPlan + ")";
    }

    @Override
    public void logPlanStructure(final StoreTimer timer) {
        timer.increment(FDBStoreTimer.Counts.PLAN_OVERSCAN_INDEX);
    }

    @Override
    public int getComplexity() {
        return originalIndexPlan.getComplexity() + 1;
    }

    @Nonnull
    @Override
    public AvailableFields getAvailableFields() {
        return originalIndexPlan.getAvailableFields();
    }

    @Nonnull
    @Override
    public String getIndexName() {
        return originalIndexPlan.getIndexName();
    }

    @Nonnull
    @Override
    public IndexScanType getScanType() {
        return originalIndexPlan.getScanType();
    }

    @Nonnull
    @Override
    public Optional<? extends MatchCandidate> getMatchCandidateOptional() {
        return originalIndexPlan.getMatchCandidateOptional();
    }

    @Nonnull
    @Override
    public RecordQueryPlanWithIndex translateCorrelations(@Nonnull final TranslationMap translationMap, @Nonnull final List<? extends Quantifier> translatedQuantifiers) {
        return originalIndexPlan.translateCorrelations(translationMap, translatedQuantifiers);
    }

    @Nonnull
    @Override
    public PlannerGraph createIndexPlannerGraph(@Nonnull final RecordQueryPlan identity, @Nonnull final NodeInfo nodeInfo, @Nonnull final List<String> additionalDetails, @Nonnull final Map<String, Attribute> additionalAttributeMap) {
        return originalIndexPlan.createIndexPlannerGraph(identity, nodeInfo, additionalDetails, additionalAttributeMap);
    }
}
