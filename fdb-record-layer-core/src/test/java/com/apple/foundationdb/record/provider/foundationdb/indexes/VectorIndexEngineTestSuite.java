/*
 * VectorIndexEngineTestSuite.java
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

import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ExecuteState;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.OnlineIndexer;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanOptions;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.vector.TestRecordsVectorsProto.VectorRecord;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.RandomizedTestUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.ObjectArrays;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Engine-agnostic behavioral tests for vector indexes: write/read, grouped read, insert-read-delete churn,
 * {@code deleteWhere}, and direct index-maintainer scans. Concrete subclasses pin the engine via
 * {@link #indexOptions()} (and may tighten {@link #minRecall()}), so the whole battery runs against each engine.
 */
abstract class VectorIndexEngineTestSuite extends VectorIndexTestBase {
    private static final Logger logger = LoggerFactory.getLogger(VectorIndexEngineTestSuite.class);

    @Nonnull
    static Stream<Arguments> randomSeedsWithAsync() {
        return RandomizedTestUtils.randomSeeds(0xdeadc0deL)
                .flatMap(seed -> Sets.cartesianProduct(ImmutableSet.of(true, false)).stream()
                        .map(arguments -> Arguments.of(ObjectArrays.concat(seed, arguments.toArray()))));
    }

    @Nonnull
    static Stream<Arguments> randomSeedsWithReturnVectors() {
        return RandomizedTestUtils.randomSeeds(0xdeadbeefL)
                .flatMap(seed -> Sets.cartesianProduct(ImmutableSet.of(true, false)).stream()
                        .map(arguments -> Arguments.of(ObjectArrays.concat(seed, arguments.toArray()))));
    }

    @Nonnull
    static Stream<Arguments> randomSeedsWithAsyncAndLimit() {
        return RandomizedTestUtils.randomSeeds(0xdeadc0deL)
                .flatMap(seed -> Sets.cartesianProduct(ImmutableSet.of(true, false),
                                ImmutableSet.of(3, 17, 1000)).stream()
                        .map(arguments -> Arguments.of(ObjectArrays.concat(seed, arguments.toArray()))));
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithAsync")
    void basicWriteReadTest(final long seed, final boolean useAsync) throws Exception {
        final Random random = new Random(seed);
        final List<FDBStoredRecord<Message>> savedRecords =
                saveRandomRecords(useAsync, this::addVectorIndexes, random, 1000, 0.3);
        try (final FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addVectorIndexes);
            for (int l = 0; l < 1000; l ++) {
                final FDBStoredRecord<Message> loadedRecord =
                        recordStore.loadRecord(savedRecords.get(l).getPrimaryKey());

                assertThat(loadedRecord).isNotNull();
                assertThat(loadedRecord.getRecord()).isEqualTo(savedRecords.get(l).getRecord());
            }
            commit(context);
        }
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithAsyncAndLimit")
    void basicWriteIndexReadWithContinuationTest(final long seed, final boolean useAsync, final int limit) throws Exception {
        final int k = 100;
        final Random random = new Random(seed);
        final HalfRealVector queryVector = randomHalfVector(random, 128);

        final List<FDBStoredRecord<Message>> savedRecords =
                saveRandomRecords(useAsync, this::addUngroupedVectorIndex, random, 1000);

        final Set<Long> expectedResults =
                sortByDistances(savedRecords, queryVector, Metric.EUCLIDEAN_METRIC).stream()
                        .limit(k)
                        .map(nodeReferenceWithDistance ->
                                nodeReferenceWithDistance.getPrimaryKey().getLong(0))
                        .collect(ImmutableSet.toImmutableSet());

        final RecordQueryIndexPlan indexPlan =
                createIndexPlan(queryVector, k, "UngroupedVectorIndex");

        checkResults(indexPlan, limit, expectedResults);
    }

    private void checkResults(@Nonnull final RecordQueryIndexPlan indexPlan,
                              final int limit,
                              @Nonnull final Set<Long> expectedResults) throws Exception {
        verifyRebase(indexPlan);
        verifySerialization(indexPlan);

        int allCounter = 0;
        int recallCounter = 0;
        try (final FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addUngroupedVectorIndex);

            byte[] continuation = null;
            do {
                try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor =
                             executeQuery(indexPlan, continuation, Bindings.EMPTY_BINDINGS, limit)) {
                    int numRecords = 0;
                    while (cursor.hasNext()) {
                        final FDBQueriedRecord<Message> rec = cursor.next();
                        final VectorRecord record =
                                VectorRecord.newBuilder()
                                        .mergeFrom(Objects.requireNonNull(rec).getRecord())
                                        .build();
                        numRecords++;
                        allCounter++;
                        if (expectedResults.contains(record.getRecNo())) {
                            recallCounter++;
                        }
                    }
                    if (cursor.getNoNextReason() == RecordCursor.NoNextReason.SOURCE_EXHAUSTED) {
                        continuation = null;
                    } else {
                        continuation = cursor.getContinuation();
                    }
                    if (logger.isInfoEnabled()) {
                        logger.info("ungrouped read {} records, allCounters={}, recallCounters={}", numRecords, allCounter,
                                recallCounter);
                    }
                }
            } while (continuation != null);
            assertThat(allCounter).isEqualTo(expectedResults.size());
            assertThat((double)recallCounter / expectedResults.size()).isGreaterThan(minRecall());
        }
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithAsyncAndLimit")
    void basicWriteIndexReadGroupedWithContinuationTest(final long seed, final boolean useAsync, final int limit) throws Exception {
        final int size = 1000;
        final int k = 100;
        final Random random = new Random(seed);
        final HalfRealVector queryVector = randomHalfVector(random, 128);

        final List<FDBStoredRecord<Message>> savedRecords =
                saveRandomRecords(useAsync, this::addGroupedVectorIndex, random, size);
        final Map<Integer, List<Long>> randomRecords = groupAndSortByDistances(savedRecords, queryVector);
        final Map<Integer, Set<Long>> expectedResults = trueTopK(randomRecords, k);

        final RecordQueryIndexPlan indexPlan = createIndexPlan(queryVector, k, "GroupedVectorIndex");

        checkResultsGrouped(indexPlan, limit, expectedResults);
    }

    private void checkResultsGrouped(@Nonnull final RecordQueryIndexPlan indexPlan, final int limit,
                                     @Nonnull final Map<Integer, Set<Long>> expectedResults) throws Exception {
        verifyRebase(indexPlan);
        verifySerialization(indexPlan);

        try (final FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addGroupedVectorIndex);

            final int[] allCounters = new int[2];
            final int[] recallCounters = new int[2];
            byte[] continuation = null;
            do {
                try (final RecordCursorIterator<FDBQueriedRecord<Message>> cursor =
                             executeQuery(indexPlan, continuation, Bindings.EMPTY_BINDINGS, limit)) {
                    int numRecords = 0;
                    while (cursor.hasNext()) {
                        final FDBQueriedRecord<Message> rec = cursor.next();
                        final VectorRecord record =
                                VectorRecord.newBuilder()
                                        .mergeFrom(Objects.requireNonNull(rec).getRecord())
                                        .build();
                        numRecords++;
                        allCounters[record.getGroupId()]++;
                        if (expectedResults.get(record.getGroupId()).contains(record.getRecNo())) {
                            recallCounters[record.getGroupId()]++;
                        }
                    }
                    if (cursor.getNoNextReason() == RecordCursor.NoNextReason.SOURCE_EXHAUSTED) {
                        continuation = null;
                    } else {
                        continuation = cursor.getContinuation();
                    }
                    if (logger.isInfoEnabled()) {
                        logger.info("grouped read {} records, allCounters={}, recallCounters={}", numRecords, allCounters,
                                recallCounters);
                    }
                }
            } while (continuation != null);

            IntStream.range(0, allCounters.length)
                    .forEach(index -> {
                        assertThat(allCounters[index])
                                .as("allCounters[%d]", index)
                                .satisfies(allCountersAtIndex -> {
                                    assertThat(allCountersAtIndex).isEqualTo(
                                            expectedResults.getOrDefault(index, ImmutableSet.of()).size());
                                });
                        assertThat(recallCounters[index])
                                .as("recallCounters[%d]", index)
                                .satisfies(recallCountersAtIndex -> {
                                    assertThat((double)recallCountersAtIndex /
                                            expectedResults.getOrDefault(index, ImmutableSet.of()).size())
                                            .isGreaterThan(minRecall());
                                });

                    });
        }
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithAsyncAndLimit")
    void insertReadDeleteReadGroupedWithContinuationTest(final long seed, final boolean useAsync, final int limit) throws Exception {
        final int size = 1000;
        Assertions.assertThat(size % 2).isEqualTo(0); // needs to be even
        final int updateBatchSize = 50;
        Assertions.assertThat(size % updateBatchSize).isEqualTo(0); // needs to be divisible

        final int k = 100;
        final Random random = new Random(seed);
        final var savedRecords = saveRandomRecords(useAsync, this::addGroupedVectorIndex, random, size);

        final HalfRealVector queryVector = randomHalfVector(random, 128);

        //
        // Artificially create a lot of churn. Take the first record and flip its vector with the 999th vector,
        // take the second record and flip it with the 998th and so on. We still know the expected ground truth and
        // can compensate for that.
        //
        for (int i = 0; i < size / 2;) {
            try (FDBRecordContext context = openContext()) {
                openRecordStore(context, this::addGroupedVectorIndex);
                for (int b = 0; b < updateBatchSize; b ++) {
                    final int nearerGroupId = i % 2;
                    final FDBStoredRecord<Message> nearer =
                            Objects.requireNonNull(recordStore.loadRecord(Tuple.from(nearerGroupId, i)));
                    final VectorRecord nearerRecord =
                            VectorRecord.newBuilder()
                                    .mergeFrom(nearer.getRecord())
                                    .build();
                    final int furtherRecId = size - i - 1;
                    final int furtherGroupId = furtherRecId % 2;
                    final FDBStoredRecord<Message> further =
                            Objects.requireNonNull(recordStore.loadRecord(Tuple.from(furtherGroupId, furtherRecId)));
                    final VectorRecord furtherRecord = VectorRecord.newBuilder()
                            .mergeFrom(further.getRecord())
                            .build();

                    final Message newNearer = VectorRecord.newBuilder()
                            .setRecNo(nearerRecord.getRecNo())
                            .setGroupId(nearerRecord.getGroupId())
                            .setVectorData(furtherRecord.getVectorData())
                            .build();
                    final Message newFurther = VectorRecord.newBuilder()
                            .setRecNo(furtherRecord.getRecNo())
                            .setGroupId(furtherRecord.getGroupId())
                            .setVectorData(nearerRecord.getVectorData())
                            .build();

                    recordStore.updateRecord(newNearer);
                    recordStore.updateRecord(newFurther);
                    i ++;
                }
                commit(context);
            }
        }

        final List<FDBStoredRecord<Message>> flippedRecords =
                savedRecords
                        .stream()
                        .map(storedRecord -> {
                            final VectorRecord vectorRecord =
                                    VectorRecord.newBuilder()
                                            .mergeFrom(storedRecord.getRecord())
                                            .build();
                            final VectorRecord newVectorRecord =
                                    VectorRecord.newBuilder()
                                            .setGroupId((int)(size - vectorRecord.getRecNo() - 1) % 2)
                                            .setRecNo(size - vectorRecord.getRecNo() - 1)
                                            .setVectorData(vectorRecord.getVectorData())
                                            .build();
                            return FDBStoredRecord.newBuilder()
                                    .setRecord(newVectorRecord)
                                    .setPrimaryKey(storedRecord.getPrimaryKey())
                                    .setRecordType(storedRecord.getRecordType())
                                    .build();
                        })
                        .collect(ImmutableList.toImmutableList());
        final Map<Integer, List<Long>> groupedFlippedRecords = groupAndSortByDistances(flippedRecords, queryVector);
        final Map<Integer, Set<Long>> expectedResults = trueTopK(groupedFlippedRecords, k);

        final RecordQueryIndexPlan indexPlan = createIndexPlan(queryVector, k, "GroupedVectorIndex");

        checkResultsGrouped(indexPlan, limit, expectedResults);
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithAsync")
    void deleteWhereGroupedTest(final long seed, final boolean useAsync) throws Exception {
        final int k = 100;
        final Random random = new Random(seed);
        final HalfRealVector queryVector = randomHalfVector(random, 128);

        final List<FDBStoredRecord<Message>> savedRecords =
                saveRandomRecords(useAsync, this::addGroupedVectorIndex, random, 200);
        final Map<Integer, List<Long>> randomRecords = groupAndSortByDistances(savedRecords, queryVector);
        final Map<Integer, Set<Long>> expectedResults =
                Maps.filterKeys(
                        trueTopK(randomRecords, 200), key -> Objects.requireNonNull(key) % 2 != 0);

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addGroupedVectorIndex);
            recordStore.deleteRecordsWhere(Query.field("group_id").equalsValue(0));
            commit(context);
        }

        final RecordQueryIndexPlan indexPlan = createIndexPlan(queryVector, k, "GroupedVectorIndex");
        checkResultsGrouped(indexPlan, Integer.MAX_VALUE, expectedResults);
    }

    @SuppressWarnings("resource")
    @Test
    void directIndexMaintainerTest() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addGroupedVectorIndex);

            final Index index =
                    Objects.requireNonNull(recordStore.getMetaDataProvider())
                            .getRecordMetaData().getIndex("GroupedVectorIndex");
            final IndexMaintainer indexMaintainer = recordStore.getIndexMaintainer(index);
            Assertions.assertThatThrownBy(() -> indexMaintainer.scan(IndexScanType.BY_VALUE, TupleRange.ALL,
                    null, ScanProperties.FORWARD_SCAN)).isInstanceOf(IllegalStateException.class);
        }
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithReturnVectors")
    void directIndexReadGroupedWithContinuationTest(final long seed, final boolean returnVectors) throws Exception {
        final int size = 1000;
        final int k = 100;
        final Random random = new Random(seed);
        final HalfRealVector queryVector = randomHalfVector(random, 128);

        final List<FDBStoredRecord<Message>> savedRecords =
                saveRandomRecords(true, this::addGroupedVectorIndex, random, size);
        final Map<Integer, List<Long>> randomRecords = groupAndSortByDistances(savedRecords, queryVector);
        final Map<Integer, Set<Long>> expectedResults = trueTopK(randomRecords, k);

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addGroupedVectorIndex);

            final int[] allCounters = new int[2];
            final int[] recallCounters = new int[2];

            final Index index =
                    Objects.requireNonNull(recordStore.getMetaDataProvider())
                            .getRecordMetaData().getIndex("GroupedVectorIndex");
            final IndexMaintainer indexMaintainer = recordStore.getIndexMaintainer(index);
            final VectorIndexScanComparisons vectorIndexScanComparisons =
                    createVectorIndexScanComparisons(queryVector, k,
                            VectorIndexScanOptions.builder()
                                    .putOption(VectorIndexScanOptions.VECTOR_RETURN_VECTORS, returnVectors)
                                    .build());
            final ScanProperties scanProperties = ExecuteProperties.newBuilder()
                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                    .setState(ExecuteState.NO_LIMITS)
                    .setReturnedRowLimit(Integer.MAX_VALUE).build().asScanProperties(false);

            try (final RecordCursor<IndexEntry> cursor =
                         indexMaintainer.scan(vectorIndexScanComparisons.bind(recordStore, index,
                                 EvaluationContext.empty()), null, scanProperties)) {
                final RecordCursorIterator<IndexEntry> cursorIterator = cursor.asIterator();
                int numRecords = 0;
                while (cursorIterator.hasNext()) {
                    final IndexEntry indexEntry = Objects.requireNonNull(cursorIterator.next());
                    numRecords++;
                    final int groupId = Math.toIntExact(indexEntry.getPrimaryKey().getLong(0));
                    final long recNo = Math.toIntExact(indexEntry.getPrimaryKey().getLong(1));
                    allCounters[groupId]++;
                    if (expectedResults.get(groupId).contains(recNo)) {
                        recallCounters[groupId]++;
                    }
                    assertThat(indexEntry.getValue().get(0) != null).isEqualTo(returnVectors);
                }
                if (logger.isInfoEnabled()) {
                    logger.info("(direct) grouped read {} records, allCounters={}, recallCounters={}", numRecords, allCounters,
                            recallCounters);
                }
            }

            assertThat(Ints.asList(allCounters))
                    .allSatisfy(allCounter ->
                            assertThat(allCounter).isEqualTo(k));
            assertThat(Ints.asList(recallCounters))
                    .allSatisfy(recallCounter ->
                            assertThat((double)recallCounter / k).isGreaterThan(minRecall()));
        }
    }

    @Test
    void vectorIndexAttributesNotOptimizedForMutualIndexing() {
        final VectorIndexMaintainerFactory factory = new VectorIndexMaintainerFactory();
        final Index vectorIndex = new Index("test", Key.Expressions.field("field"), IndexTypes.VECTOR);
        Assertions.assertThat(factory.getIndexGeneralAttributes(vectorIndex).isOptimizedForMutualIndexing()).isFalse();
    }

    @Test
    void vectorIndexAllowsPendingWriteQueue() throws Exception {
        // VectorIndexMaintainer.isPendingWriteQueueAllowed() returns true: a vector index may defer writes to the queue.
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addUngroupedVectorIndex);
            final Index index = recordStore.getRecordMetaData().getIndex("UngroupedVectorIndex");
            assertThat(recordStore.getIndexMaintainer(index).isPendingWriteQueueAllowed())
                    .as("a vector index should allow the pending write queue")
                    .isTrue();
            commit(context);
        }
    }

    @Test
    void pendingWriteQueueRoundTripInsert() throws Exception {
        // serializePendingWriteQueue(null, new) -> updateFromQueue re-inserts the entry into an emptied index.
        final HalfRealVector vector = randomHalfVector(new Random(1L), 128);
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addUngroupedVectorIndex);
            final Index index = recordStore.getRecordMetaData().getIndex("UngroupedVectorIndex");
            final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);

            final FDBStoredRecord<Message> stored = saveVectorRecord(1L, vector);
            final Any entry = maintainer.serializePendingWriteQueue(null, stored);

            // Empty the index, then rebuild only this entry from its serialized payload.
            recordStore.clearAndMarkIndexWriteOnly(index).join();
            maintainer.updateFromQueue(entry).join();

            assertThat(nearestRecNos(index, vector, 10)).containsExactly(1L);
            commit(context);
        }
    }

    @Test
    void pendingWriteQueueRoundTripDelete() throws Exception {
        // serializePendingWriteQueue(old, null) -> updateFromQueue removes just that entry, leaving the witness intact.
        final Random random = new Random(2L);
        final HalfRealVector vector1 = randomHalfVector(random, 128);
        final HalfRealVector vector2 = randomHalfVector(random, 128);
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addUngroupedVectorIndex);
            final Index index = recordStore.getRecordMetaData().getIndex("UngroupedVectorIndex");
            final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);

            final FDBStoredRecord<Message> stored1 = saveVectorRecord(1L, vector1);
            saveVectorRecord(2L, vector2); // witness that must survive
            assertThat(nearestRecNos(index, vector1, 10)).containsExactlyInAnyOrder(1L, 2L);

            final Any entry = maintainer.serializePendingWriteQueue(stored1, null);
            maintainer.updateFromQueue(entry).join();

            assertThat(nearestRecNos(index, vector1, 10)).containsExactly(2L);
            commit(context);
        }
    }

    @Test
    void pendingWriteQueueRoundTripUpdate() throws Exception {
        // serializePendingWriteQueue(old, new) for the same primary key -> updateFromQueue removes the old entry and
        // adds the new one in place, actually repositioning pk 1 relative to the untouched witness.
        // Uniform vectors make the ordering unambiguous, so a correct in-place update flips
        // the nearest-neighbor order from {1, 2} to {2, 1}.
        final HalfRealVector query = constantHalfVector(0.1f, 128);
        final HalfRealVector witnessVector = constantHalfVector(0.5f, 128);
        final HalfRealVector oldVector = constantHalfVector(0.11f, 128);
        final HalfRealVector newVector = constantHalfVector(0.9f, 128);
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addUngroupedVectorIndex);
            final Index index = recordStore.getRecordMetaData().getIndex("UngroupedVectorIndex");
            final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);

            saveVectorRecord(2L, witnessVector); // witness that must survive
            final FDBStoredRecord<Message> oldStored = saveVectorRecord(1L, oldVector);
            // Before the update pk 1 sits closest to the query, ahead of the witness.
            assertThat(nearestRecNos(index, query, 10)).containsExactly(1L, 2L);

            // Build the new version without saving it, so the index still holds oldVector when we serialize.
            final Message newRecord = VectorRecord.newBuilder()
                    .setRecNo(1L).setGroupId(0)
                    .setVectorData(ByteString.copyFrom(newVector.getRawData()))
                    .build();
            final FDBStoredRecord<Message> newStored = FDBStoredRecord.newBuilder(newRecord)
                    .setPrimaryKey(oldStored.getPrimaryKey())
                    .setRecordType(oldStored.getRecordType())
                    .build();

            final Any entry = maintainer.serializePendingWriteQueue(oldStored, newStored);
            maintainer.updateFromQueue(entry).join();

            // pk 1 updated in place (no duplicate, no missing node) and repositioned: it is now further from the
            // query than the untouched witness, so the nearest-neighbor order flips to {2, 1}.
            assertThat(nearestRecNos(index, query, 10)).containsExactly(2L, 1L);
            commit(context);
        }
    }

    @Test
    void updateFromQueueDeleteIsIdempotent() throws Exception {
        // Re-draining a delete entry (e.g. a retried indexer) must be a harmless no-op the second time: HNSW.delete
        // completes without error when the node is already gone.
        final Random random = new Random(4L);
        final HalfRealVector vector1 = randomHalfVector(random, 128);
        final HalfRealVector vector2 = randomHalfVector(random, 128);
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addUngroupedVectorIndex);
            final Index index = recordStore.getRecordMetaData().getIndex("UngroupedVectorIndex");
            final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);

            final FDBStoredRecord<Message> stored1 = saveVectorRecord(1L, vector1);
            saveVectorRecord(2L, vector2); // witness that must survive

            final Any entry = maintainer.serializePendingWriteQueue(stored1, null);
            maintainer.updateFromQueue(entry).join();
            maintainer.updateFromQueue(entry).join();

            assertThat(nearestRecNos(index, vector1, 10)).containsExactly(2L);
            commit(context);
        }
    }

    @Test
    void pendingWriteQueueSkipsNullVector() throws Exception {
        // A record with null vector produces an entry with no vector; draining it is a no-op (updateIndexEntry
        // short-circuits on the null vector) and must not disturb the rest of the index.
        final Random random = new Random(5L);
        final HalfRealVector vector1 = randomHalfVector(random, 128);
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addUngroupedVectorIndex);
            final Index index = recordStore.getRecordMetaData().getIndex("UngroupedVectorIndex");
            final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);

            final FDBStoredRecord<Message> stored1 = saveVectorRecord(1L, vector1); // witness with a real vector
            // A record with vector_data unset; build it without saving. Primary key is (group_id, rec_no).
            final Message nullVectorRecord = VectorRecord.newBuilder().setRecNo(2L).setGroupId(0).build();
            final FDBStoredRecord<Message> nullVectorStored = FDBStoredRecord.newBuilder(nullVectorRecord)
                    .setPrimaryKey(Tuple.from(0L, 2L))
                    .setRecordType(stored1.getRecordType())
                    .build();

            final Any entry = maintainer.serializePendingWriteQueue(null, nullVectorStored);
            maintainer.updateFromQueue(entry).join();

            assertThat(nearestRecNos(index, vector1, 10)).containsExactly(1L);
            commit(context);
        }
    }

    @Test
    void writeOnlyWithQueueBuildsUngroupedVectorIndex() throws Exception {
        // End-to-end: every insert is deferred to the pending write queue, then the online indexer builds the index
        // (scanning records) and drains the queue, marking the index readable with high query recall.
        final int size = 200;
        final int k = 50;
        final Random random = new Random(0xf00dL);
        final HalfRealVector queryVector = randomHalfVector(random, 128);

        final var recordGenerator = getRecordGenerator(random, 0.0d);
        final List<FDBStoredRecord<Message>> savedRecords = new ArrayList<>();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addUngroupedVectorIndex);
            recordStore.markIndexWriteOnlyWithQueue("UngroupedVectorIndex").join();
            for (long recNo = 0; recNo < size; recNo++) {
                final Message generated = recordGenerator.apply(recNo);
                savedRecords.add(recordStore.saveRecord(generated));
            }
            commit(context);
        }

        buildIndexAndDrainQueue("UngroupedVectorIndex", this::addUngroupedVectorIndex);

        final Set<Long> expectedResults =
                sortByDistances(savedRecords, queryVector, Metric.EUCLIDEAN_METRIC).stream()
                        .limit(k)
                        .map(nodeReferenceWithDistance -> nodeReferenceWithDistance.getPrimaryKey().getLong(0))
                        .collect(ImmutableSet.toImmutableSet());
        checkResults(createIndexPlan(queryVector, k, "UngroupedVectorIndex"), Integer.MAX_VALUE, expectedResults);
    }

    @Test
    void deleteWhereThroughQueueOnGroupedVectorIndex() throws Exception {
        // deleteRecordsWhere on a group prefix via pending write queue
        final int size = 200;
        final int k = 100;
        final Random random = new Random(0xbeadL);
        final HalfRealVector queryVector = randomHalfVector(random, 128);

        final List<FDBStoredRecord<Message>> savedRecords =
                saveRandomRecords(false, this::addGroupedVectorIndex, random, size);
        final Map<Integer, List<Long>> randomRecords = groupAndSortByDistances(savedRecords, queryVector);
        final Map<Integer, Set<Long>> expectedResults =
                Maps.filterKeys(trueTopK(randomRecords, size), key -> Objects.requireNonNull(key) % 2 != 0);

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addGroupedVectorIndex);
            recordStore.markIndexWriteOnlyWithQueue("GroupedVectorIndex").join();
            recordStore.deleteRecordsWhere(Query.field("group_id").equalsValue(0));
            commit(context);
        }

        buildIndexAndDrainQueue("GroupedVectorIndex", this::addGroupedVectorIndex);

        checkResultsGrouped(createIndexPlan(queryVector, k, "GroupedVectorIndex"), Integer.MAX_VALUE, expectedResults);
    }

    @Nonnull
    private FDBStoredRecord<Message> saveVectorRecord(final long recNo, @Nonnull final HalfRealVector vector) {
        final Message rec = VectorRecord.newBuilder()
                .setRecNo(recNo)
                .setGroupId(0)
                .setVectorData(ByteString.copyFrom(vector.getRawData()))
                .build();
        return recordStore.saveRecord(rec);
    }

    /**
     * Direct (state-independent) probe of the index: the rec-nos of the {@code k} nearest neighbors of the query
     * vector, read straight from the maintainer so it works even while the index is write-only.
     */
    @Nonnull
    private List<Long> nearestRecNos(@Nonnull final Index index, @Nonnull final HalfRealVector queryVector, final int k) {
        final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);
        final var scanBounds = createVectorIndexScanComparisons(queryVector, k, VectorIndexScanOptions.empty())
                .bind(recordStore, index, EvaluationContext.empty());
        final ScanProperties scanProperties = ExecuteProperties.newBuilder()
                .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                .setState(ExecuteState.NO_LIMITS)
                .setReturnedRowLimit(Integer.MAX_VALUE).build().asScanProperties(false);
        try (RecordCursor<IndexEntry> cursor = maintainer.scan(scanBounds, null, scanProperties)) {
            return cursor.map(entry -> entry.getKey().getLong(1)).asList().join();
        }
    }

    private void buildIndexAndDrainQueue(@Nonnull final String indexName,
                                         @Nonnull final RecordMetaDataHook hook) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            final Index index = recordStore.getRecordMetaData().getIndex(indexName);
            try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                    .setRecordStore(recordStore)
                    .setIndex(index)
                    .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                            .setUsePendingWriteQueue(List.of(index))
                            .build())
                    .build()) {
                indexer.buildIndex(true);
            }
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            assertThat(recordStore.isIndexReadable(recordStore.getRecordMetaData().getIndex(indexName))).isTrue();
            commit(context);
        }
    }
}
