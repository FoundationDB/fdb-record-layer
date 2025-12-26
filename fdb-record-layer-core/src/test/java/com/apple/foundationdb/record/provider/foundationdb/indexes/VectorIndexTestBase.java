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
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.record.vector.TestRecordsVectorsProto;
import com.apple.foundationdb.record.vector.TestRecordsVectorsProto.VectorRecord;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;

/**
 * Common test helpers for vector type indexes.
 */
@Tag(Tags.RequiresFDB)
public class VectorIndexTestBase extends FDBRecordStoreQueryTestBase {
    private static final Logger logger = LoggerFactory.getLogger(VectorIndexTestBase.class);

    @CanIgnoreReturnValue
    protected RecordMetaDataBuilder addVectorIndexes(@Nonnull final RecordMetaDataBuilder metaDataBuilder) {
        addUngroupedVectorIndex(metaDataBuilder);
        addGroupedVectorIndex(metaDataBuilder);
        return metaDataBuilder;
    }

    @CanIgnoreReturnValue
    protected RecordMetaDataBuilder addUngroupedVectorIndex(@Nonnull final RecordMetaDataBuilder metaDataBuilder) {
        metaDataBuilder.addIndex("VectorRecord",
                new Index("UngroupedVectorIndex", new KeyWithValueExpression(field("vector_data"), 0),
                        IndexTypes.VECTOR,
                        ImmutableMap.of(IndexOptions.HNSW_METRIC, Metric.EUCLIDEAN_METRIC.name(),
                                IndexOptions.HNSW_NUM_DIMENSIONS, "128")));
        return metaDataBuilder;
    }

    @CanIgnoreReturnValue
    protected RecordMetaDataBuilder addGroupedVectorIndex(@Nonnull final RecordMetaDataBuilder metaDataBuilder) {
        metaDataBuilder.addIndex("VectorRecord",
                new Index("GroupedVectorIndex", new KeyWithValueExpression(concat(field("group_id"), field("vector_data")), 1),
                        IndexTypes.VECTOR,
                        ImmutableMap.of(IndexOptions.HNSW_METRIC, Metric.EUCLIDEAN_METRIC.name(),
                                IndexOptions.HNSW_NUM_DIMENSIONS, "128")));
        return metaDataBuilder;
    }

    protected void openRecordStore(FDBRecordContext context) throws Exception {
        openRecordStore(context, NO_HOOK);
    }

    protected void openRecordStore(final FDBRecordContext context, final RecordMetaDataHook hook) throws Exception {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsVectorsProto.getDescriptor());
        metaDataBuilder.getRecordType("VectorRecord").setPrimaryKey(concatenateFields("group_id", "rec_no"));
        hook.apply(metaDataBuilder);
        createOrOpenRecordStore(context, metaDataBuilder.getRecordMetaData());
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

    private <M extends Message> List<FDBStoredRecord<M>> batch(final RecordMetaDataHook hook, final int numRecords,
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

    protected static void logRecord(final long recNo, @Nonnull final ByteString vectorData) {
        if (logger.isInfoEnabled()) {
            logger.info("recNo: {}; vectorData: [{})",
                    recNo, RealVector.fromBytes(vectorData.toByteArray()));
        }
    }
}
