/*
 * VectorIndexTestBase.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.hnsw.NodeReference;
import com.apple.foundationdb.async.hnsw.NodeReferenceWithDistance;
import com.apple.foundationdb.half.Half;
import com.apple.foundationdb.linear.AffineOperator;
import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.record.IndexFetchMethod;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.OnlineIndexer;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanOptions;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.vector.TestRecordsVectorsProto;
import com.apple.foundationdb.record.vector.TestRecordsVectorsProto.VectorRecord;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;

/**
 * Common test helpers for vector type indexes. The concrete engine (HNSW or Guardiann) and its per-engine tuning are
 * supplied by subclasses through {@link #indexOptions()}, so the shared index-creation helpers here are engine-agnostic.
 */
@Tag(Tags.RequiresFDB)
public abstract class VectorIndexTestBase extends FDBRecordStoreQueryTestBase {
    private static final Logger logger = LoggerFactory.getLogger(VectorIndexTestBase.class);

    // Max passes of the real merger to drive a vector index's backlog to completion before failing. Each pass is a full
    // OnlineIndexer.mergeIndex(), which itself loops the per-partition claim/drain internally, so one pass usually
    // suffices; the bound guards against a task-enqueues-follow-up loop never converging.
    private static final int MERGE_DRAIN_MAX_PASSES = 200;

    /**
     * The index options a subclass creates its vector indexes with. These select the engine
     * ({@link IndexOptions#VECTOR_ENGINE}) and carry its per-engine tuning plus the shared metric and dimensions.
     * Engine-focused subclasses implement this; the base deliberately does not pick an engine.
     *
     * @return the options to create the shared test indexes with
     */
    @Nonnull
    protected abstract Map<String, String> indexOptions();

    /**
     * The minimum recall@k the shared read scenarios assert. Uniform across engines by default; a subclass may override
     * it if its engine/tuning warrants a different floor.
     *
     * @return the minimum acceptable recall fraction
     */
    protected double minRecall() {
        return 0.8d;
    }

    @CanIgnoreReturnValue
    protected RecordMetaDataBuilder addVectorIndexes(@Nonnull final RecordMetaDataBuilder metaDataBuilder) {
        addUngroupedVectorIndex(metaDataBuilder);
        addGroupedVectorIndex(metaDataBuilder);
        return metaDataBuilder;
    }

    @CanIgnoreReturnValue
    protected RecordMetaDataBuilder addUngroupedVectorIndex(@Nonnull final RecordMetaDataBuilder metaDataBuilder) {
        return addUngroupedVectorIndex(metaDataBuilder, indexOptions());
    }

    @CanIgnoreReturnValue
    protected RecordMetaDataBuilder addUngroupedVectorIndex(@Nonnull final RecordMetaDataBuilder metaDataBuilder,
                                                            @Nonnull final Map<String, String> options) {
        metaDataBuilder.addIndex("VectorRecord",
                new Index("UngroupedVectorIndex", new KeyWithValueExpression(field("vector_data"), 0),
                        IndexTypes.VECTOR, options));
        return metaDataBuilder;
    }

    @CanIgnoreReturnValue
    protected RecordMetaDataBuilder addGroupedVectorIndex(@Nonnull final RecordMetaDataBuilder metaDataBuilder) {
        return addGroupedVectorIndex(metaDataBuilder, indexOptions());
    }

    @CanIgnoreReturnValue
    protected RecordMetaDataBuilder addGroupedVectorIndex(@Nonnull final RecordMetaDataBuilder metaDataBuilder,
                                                          @Nonnull final Map<String, String> options) {
        metaDataBuilder.addIndex("VectorRecord",
                new Index("GroupedVectorIndex", new KeyWithValueExpression(concat(field("group_id"), field("vector_data")), 1),
                        IndexTypes.VECTOR, options));
        return metaDataBuilder;
    }

    protected void openRecordStore(FDBRecordContext context) throws Exception {
        openRecordStore(context, NO_HOOK);
    }

    protected void openRecordStore(final FDBRecordContext context, final RecordMetaDataHook hook) throws Exception {
        createOrOpenRecordStore(context, metaDataFor(hook));
        // In-transaction vs. deferred index maintenance is a runtime, per-store switch (autoMergeDuringCommit). It
        // defaults to false here (deferred, matching production/CK, so merge tests get a real backlog); a subclass whose
        // scenarios have no background merger (e.g. the behavioral suite) overrides maintainIndexesInTransaction().
        recordStore.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(maintainIndexesInTransaction());
    }

    /**
     * Builds the {@link RecordMetaData} the vector-index tests use — the {@code VectorRecord} descriptor with the
     * {@code (group_id, rec_no)} primary key and whatever indexes {@code hook} adds. Shared by {@link #openRecordStore}
     * and the merge helpers so a test's inserts and its merges operate on identical index subspaces.
     * @param hook adds the index(es) under test
     * @return the built metadata
     */
    @Nonnull
    protected RecordMetaData metaDataFor(@Nonnull final RecordMetaDataHook hook) {
        final RecordMetaDataBuilder metaDataBuilder =
                RecordMetaData.newBuilder().setRecords(TestRecordsVectorsProto.getDescriptor());
        metaDataBuilder.getRecordType("VectorRecord").setPrimaryKey(concatenateFields("group_id", "rec_no"));
        hook.apply(metaDataBuilder);
        return metaDataBuilder.getRecordMetaData();
    }

    /**
     * Whether vector-index maintenance should run inside the writing transaction (Guardiann drains its deferred tasks
     * inline) rather than being deferred to a background merge. Defaults to {@code false}; subclasses that exercise
     * end-to-end behavior with no background merger override this to {@code true}.
     * @return whether to enable in-transaction index maintenance for stores opened by this test
     */
    protected boolean maintainIndexesInTransaction() {
        return false;
    }

    /**
     * Opens the record store for {@code metaData} at the test's key-space path, bypassing the {@code recordStore}
     * field/hook machinery — used by the merge helpers, which drive their own {@link OnlineIndexer} transactions.
     * @param context the context to open under
     * @param metaData the metadata to open with
     * @return the opened store
     */
    @Nonnull
    protected FDBRecordStore openStore(@Nonnull final FDBRecordContext context, @Nonnull final RecordMetaData metaData) {
        return getStoreBuilder(context, metaData, Objects.requireNonNull(path)).createOrOpen();
    }

    /**
     * Runs one pass of the real record-layer index merger over {@code indexName} via {@link OnlineIndexer#mergeIndex()}
     * — the same entry a background merge (e.g. CloudKit's) uses. The merger sets the merge session id and drives the
     * per-partition claim/drain loop internally, so tests need not hand-roll that bookkeeping.
     * @param metaData the metadata whose index to merge
     * @param indexName the vector index to merge
     */
    @SuppressWarnings("PMD.CloseResource") // the outer context only builds the store for OnlineIndexer config
    protected void mergeVectorIndexOnce(@Nonnull final RecordMetaData metaData, @Nonnull final String indexName) {
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openStore(context, metaData);
            final Index index = store.getRecordMetaData().getIndex(indexName);
            try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                    .setRecordStore(store)
                    .setIndex(index)
                    .setTimer(new FDBStoreTimer())
                    .build()) {
                indexer.mergeIndex();
            }
        }
    }

    /**
     * Drives {@link #mergeVectorIndexOnce} until {@code indexName} has no outstanding deferred-maintenance work
     * (executing a task can enqueue follow-ups, so a few passes may be needed), failing if it does not converge.
     * @param metaData the metadata whose index to merge
     * @param indexName the vector index to drain
     */
    protected void mergeVectorIndexToCompletion(@Nonnull final RecordMetaData metaData,
                                                @Nonnull final String indexName) throws Exception {
        for (int pass = 0; pass < MERGE_DRAIN_MAX_PASSES; pass++) {
            mergeVectorIndexOnce(metaData, indexName);
            if (!vectorIndexHasOutstandingWork(metaData, indexName)) {
                return;
            }
        }
        throw new AssertionError(String.format("merge did not drain the backlog for %s within %d passes",
                indexName, MERGE_DRAIN_MAX_PASSES));
    }

    /**
     * Whether {@code indexName} still has outstanding deferred-maintenance work, read via its
     * {@link VectorIndexMaintainer}.
     * @param metaData the metadata whose index to check
     * @param indexName the vector index to check
     * @return whether any partition has outstanding tasks
     */
    protected boolean vectorIndexHasOutstandingWork(@Nonnull final RecordMetaData metaData,
                                                    @Nonnull final String indexName) throws Exception {
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = openStore(context, metaData);
            final VectorIndexMaintainer maintainer =
                    (VectorIndexMaintainer)store.getIndexMaintainer(store.getRecordMetaData().getIndex(indexName));
            return maintainer.hasOutstandingWork().get();
        }
    }

    protected static Function<Long, VectorRecord> getRecordGenerator(@Nonnull final Random random,
                                                                     final double nullProbability) {
        return recNo -> {
            final VectorRecord.Builder recordBuilder =
                    VectorRecord.newBuilder()
                            .setRecNo(recNo)
                            .setGroupId(recNo.intValue() % 2);
            if (random.nextDouble() >= nullProbability) {
                final RealVector vector = randomHalfVector(random, 128);
                recordBuilder.setVectorData(ByteString.copyFrom(vector.getRawData()));
            }

            return recordBuilder.build();
        };
    }

    @Nonnull
    protected static HalfRealVector randomHalfVector(final Random random, final int numDimensions) {
        final Half[] componentData = new Half[numDimensions];
        for (int i = 0; i < componentData.length; i++) {
            componentData[i] = Half.valueOf(random.nextFloat());
        }

        return new HalfRealVector(componentData);
    }

    @Nonnull
    protected static HalfRealVector constantHalfVector(final float value, final int numDimensions) {
        final Half[] componentData = new Half[numDimensions];
        for (int i = 0; i < componentData.length; i++) {
            componentData[i] = Half.valueOf(value);
        }

        return new HalfRealVector(componentData);
    }

    protected List<FDBStoredRecord<Message>> saveRandomRecords(final boolean useAsync,
                                                               @Nonnull final RecordMetaDataHook hook,
                                                               @Nonnull final Random random,
                                                               final int numRecords) throws Exception {
        return saveRandomRecords(useAsync, hook, random, numRecords, 0.0d);
    }

    protected List<FDBStoredRecord<Message>> saveRandomRecords(final boolean useAsync,
                                                               @Nonnull final RecordMetaDataHook hook,
                                                               @Nonnull final Random random,
                                                               final int numRecords,
                                                               final double nullProbability) throws Exception {
        final var recordGenerator = getRecordGenerator(random, nullProbability);
        if (useAsync) {
            return asyncBatch(hook, numRecords, 100,
                    recNo -> recordStore.saveRecordAsync(recordGenerator.apply(recNo)));
        } else {
            return batch(hook, numRecords, 100,
                    recNo -> recordStore.saveRecord(recordGenerator.apply(recNo)));
        }
    }

    protected  <M extends Message> List<FDBStoredRecord<M>> batch(final RecordMetaDataHook hook, final int numRecords,
                                                               final int batchSize,
                                                               Function<Long, FDBStoredRecord<M>> recordConsumer) throws Exception {
        final List<FDBStoredRecord<M>> records = Lists.newArrayList();
        while (records.size() < numRecords) {
            try (FDBRecordContext context = openContext()) {
                openRecordStore(context, hook);
                int recNoInBatch;

                for (recNoInBatch = 0; records.size() < numRecords && recNoInBatch < batchSize; recNoInBatch++) {
                    records.add(recordConsumer.apply((long)records.size()));
                }
                commit(context);
                logger.info("committed batch of sync inserts, numRecordsCommitted = {}", records.size());
            }
        }
        return records;
    }

    private <M extends Message> List<FDBStoredRecord<M>>
            asyncBatch(@Nonnull final RecordMetaDataHook hook,
                       final int numRecords,
                       final int batchSize,
                       @Nonnull final Function<Long, CompletableFuture<FDBStoredRecord<M>>> recordConsumer) throws Exception {
        final List<FDBStoredRecord<M>> records = Lists.newArrayList();
        while (records.size() < numRecords) {
            try (FDBRecordContext context = openContext()) {
                openRecordStore(context, hook);
                int recNoInBatch;
                final ArrayList<CompletableFuture<FDBStoredRecord<M>>> futures = Lists.newArrayList();

                for (recNoInBatch = 0; records.size() + recNoInBatch < numRecords && recNoInBatch < batchSize; recNoInBatch++) {
                    futures.add(recordConsumer.apply((long)records.size() + recNoInBatch));
                }

                // wait and then commit
                AsyncUtil.whenAll(futures).get();
                futures.forEach(future -> records.add(future.join()));
                commit(context);
                logger.info("committed batch of async inserts, numRecordsCommitted = {}", records.size());
            }
        }
        return records;
    }

    @Nonnull
    protected static Map<Integer, Set<Long>> trueTopK(@Nonnull final Map<Integer, List<Long>> sortedByDistances,
                                                      final int k) {
        return sortedByDistances.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry ->
                                entry.getValue()
                                        .stream()
                                        .limit(k)
                                        .collect(ImmutableSet.toImmutableSet())));
    }

    @Nonnull
    protected static Map<Integer, List<Long>> groupAndSortByDistances(@Nonnull final List<FDBStoredRecord<Message>> savedRecords,
                                                                      @Nonnull final HalfRealVector queryVector) {
        return sortByDistances(savedRecords, queryVector, Metric.EUCLIDEAN_METRIC)
                .stream()
                .map(NodeReference::getPrimaryKey)
                .map(primaryKey -> primaryKey.getLong(0))
                .collect(Collectors.groupingBy(nodeId -> Math.toIntExact(nodeId) % 2, Collectors.toList()));
    }

    @Nonnull
    protected static <M extends Message> List<NodeReferenceWithDistance>
              sortByDistances(@Nonnull final List<FDBStoredRecord<M>> storedRecords,
                              @Nonnull final RealVector queryVector,
                              @Nonnull final Metric metric) {
        return storedRecords.stream()
                .map(storedRecord -> {
                    final VectorRecord vectorRecord = (VectorRecord)storedRecord.getRecord();
                    final RealVector storedVector =
                            RealVector.fromBytes(vectorRecord.getVectorData().toByteArray());
                    return new NodeReferenceWithDistance(Tuple.from(vectorRecord.getRecNo()),
                            AffineOperator.identity().transform(storedVector),
                            metric.distance(storedVector, queryVector));
                })
                .sorted(Comparator.comparing(NodeReferenceWithDistance::getDistance))
                .collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    protected static RecordQueryIndexPlan createIndexPlan(@Nonnull final HalfRealVector queryVector, final int k,
                                                        @Nonnull final String indexName) {
        final VectorIndexScanComparisons vectorIndexScanComparisons =
                createVectorIndexScanComparisons(queryVector, k, VectorIndexScanOptions.empty());

        final Type.Record baseRecordType =
                Type.Record.fromFieldDescriptorsMap(
                        Type.Record.toFieldDescriptorMap(VectorRecord.getDescriptor().getFields()));

        return new RecordQueryIndexPlan(indexName, field("recNo"),
                vectorIndexScanComparisons, IndexFetchMethod.SCAN_AND_FETCH,
                RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY, false, false,
                Optional.empty(), baseRecordType, QueryPlanConstraint.noConstraint());
    }

    @Nonnull
    protected static VectorIndexScanComparisons createVectorIndexScanComparisons(@Nonnull final HalfRealVector queryVector, final int k,
                                                                               @Nonnull final VectorIndexScanOptions vectorIndexScanOptions) {
        final Comparisons.DistanceRankValueComparison distanceRankComparison =
                new Comparisons.DistanceRankValueComparison(Comparisons.Type.DISTANCE_RANK_LESS_THAN_OR_EQUAL,
                        new LiteralValue<>(Type.Vector.of(false, 16, 128), queryVector),
                        new LiteralValue<>(k), null, null);

        return VectorIndexScanComparisons.byDistance(ScanComparisons.EMPTY,
                distanceRankComparison, vectorIndexScanOptions);
    }

    protected static void logRecord(final long recNo, @Nonnull final ByteString vectorData) {
        if (logger.isInfoEnabled()) {
            logger.info("recNo: {}; vectorData: [{})",
                    recNo, RealVector.fromBytes(vectorData.toByteArray()));
        }
    }
}
