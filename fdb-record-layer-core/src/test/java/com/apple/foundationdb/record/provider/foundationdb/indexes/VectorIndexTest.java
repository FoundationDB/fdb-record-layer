/*
 * VectorIndexTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.IndexFetchMethod;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.IndexValidator;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.MetaDataValidator;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactory;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerFactoryRegistry;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanOptions;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.vector.TestRecordsVectorsProto.VectorRecord;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.RandomizedTestUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ObjectArrays;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.protobuf.Message;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;

class VectorIndexTest extends VectorIndexTestBase {
    private static final Logger logger = LoggerFactory.getLogger(VectorIndexTest.class);

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
            assertThat(allCounter).isEqualTo(k);
            assertThat((double)recallCounter / k).isGreaterThan(0.9);
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

        verifyRebase(indexPlan);
        verifySerialization(indexPlan);

        try (FDBRecordContext context = openContext()) {
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
            assertThat(Ints.asList(allCounters))
                    .allSatisfy(allCounter ->
                            assertThat(allCounter).isEqualTo(k));
            assertThat(Ints.asList(recallCounters))
                    .allSatisfy(recallCounter ->
                            assertThat((double)recallCounter / k).isGreaterThan(0.9));
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

        final RecordQueryIndexPlan indexPlan = createIndexPlan(queryVector, k, "GroupedVectorIndex");
        verifyRebase(indexPlan);
        verifySerialization(indexPlan);

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

        try (FDBRecordContext context = openContext()) {
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
                        logger.info("grouped read after deletes and updates {} records, allCounters={}, recallCounters={}", numRecords, allCounters,
                                recallCounters);
                    }
                }
            } while (continuation != null);
            assertThat(Ints.asList(allCounters))
                    .allSatisfy(allCounter ->
                            assertThat(allCounter).isEqualTo(k));
            assertThat(Ints.asList(recallCounters))
                    .allSatisfy(recallCounter ->
                            assertThat((double)recallCounter / k).isGreaterThan(0.9));
        }
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
        final Map<Integer, Set<Long>> expectedResults = trueTopK(randomRecords, 200);

        final RecordQueryIndexPlan indexPlan = createIndexPlan(queryVector, k, "GroupedVectorIndex");

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addGroupedVectorIndex);
            recordStore.deleteRecordsWhere(Query.field("group_id").equalsValue(0));

            final int[] allCounters = new int[2];
            final int[] recallCounters = new int[2];
            try (final RecordCursorIterator<FDBQueriedRecord<Message>> cursor = executeQuery(indexPlan)) {
                while (cursor.hasNext()) {
                    final FDBQueriedRecord<Message> rec = cursor.next();
                    final VectorRecord record =
                            VectorRecord.newBuilder()
                                    .mergeFrom(Objects.requireNonNull(rec).getRecord())
                                    .build();
                    allCounters[record.getGroupId()] ++;
                    if (expectedResults.get(record.getGroupId()).contains(record.getRecNo())) {
                        recallCounters[record.getGroupId()] ++;
                    }
                }
            }
            assertThat(allCounters[0]).isEqualTo(0);
            assertThat(allCounters[1]).isEqualTo(k);

            assertThat((double)recallCounters[0] / k).isEqualTo(0.0);
            assertThat((double)recallCounters[1] / k).isGreaterThan(0.9);
        }
    }

    @Test
    void directIndexValidatorTest() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addGroupedVectorIndex);

            final Index index =
                    Objects.requireNonNull(recordStore.getMetaDataProvider())
                            .getRecordMetaData().getIndex("GroupedVectorIndex");
            final IndexMaintainerFactoryRegistry indexMaintainerRegistry = recordStore.getIndexMaintainerRegistry();
            final MetaDataValidator metaDataValidator =
                    new MetaDataValidator(recordStore.getRecordMetaData(), indexMaintainerRegistry);
            metaDataValidator.validate();

            // validate the allowed changes all at once
            validateIndexEvolution(metaDataValidator, index,
                    ImmutableMap.<String, String>builder()
                            // cannot change those per se but must accept same value
                            .put(IndexOptions.HNSW_METRIC, Metric.EUCLIDEAN_METRIC.name())
                            .put(IndexOptions.HNSW_NUM_DIMENSIONS, "128")
                            .put(IndexOptions.HNSW_USE_INLINING, "false")
                            .put(IndexOptions.HNSW_M, "16")
                            .put(IndexOptions.HNSW_M_MAX, "16")
                            .put(IndexOptions.HNSW_M_MAX_0, "32")
                            .put(IndexOptions.HNSW_EF_CONSTRUCTION, "200")
                            .put(IndexOptions.HNSW_EF_REPAIR, "64")
                            .put(IndexOptions.HNSW_EXTEND_CANDIDATES, "false")
                            .put(IndexOptions.HNSW_KEEP_PRUNED_CONNECTIONS, "false")
                            .put(IndexOptions.HNSW_USE_RABITQ, "false")
                            .put(IndexOptions.HNSW_RABITQ_NUM_EX_BITS, "4")

                            // these are allowed to change in any way
                            .put(IndexOptions.HNSW_SAMPLE_VECTOR_STATS_PROBABILITY, "0.999")
                            .put(IndexOptions.HNSW_MAINTAIN_STATS_PROBABILITY, "0.78")
                            .put(IndexOptions.HNSW_STATS_THRESHOLD, "500")
                            .put(IndexOptions.HNSW_MAX_NUM_CONCURRENT_NODE_FETCHES, "17")
                            .put(IndexOptions.HNSW_MAX_NUM_CONCURRENT_NEIGHBORHOOD_FETCHES, "9")
                            .put(IndexOptions.HNSW_MAX_NUM_CONCURRENT_DELETE_FROM_LAYER, "5").build());

            Assertions.assertThatThrownBy(() -> validateIndexEvolution(metaDataValidator, index,
                    ImmutableMap.of(IndexOptions.HNSW_NUM_DIMENSIONS, "128",
                            IndexOptions.HNSW_METRIC, Metric.EUCLIDEAN_SQUARE_METRIC.name())))
                    .isInstanceOf(MetaDataException.class);

            Assertions.assertThatThrownBy(() -> validateIndexEvolution(metaDataValidator, index,
                    ImmutableMap.of(IndexOptions.HNSW_NUM_DIMENSIONS, "768")))
                    .isInstanceOf(MetaDataException.class);

            Assertions.assertThatThrownBy(() -> validateIndexEvolution(metaDataValidator, index,
                    ImmutableMap.of(IndexOptions.HNSW_NUM_DIMENSIONS, "128",
                            IndexOptions.HNSW_USE_INLINING, "true"))).isInstanceOf(MetaDataException.class);

            Assertions.assertThatThrownBy(() -> validateIndexEvolution(metaDataValidator, index,
                    ImmutableMap.of(IndexOptions.HNSW_NUM_DIMENSIONS, "128",
                            IndexOptions.HNSW_M, "8"))).isInstanceOf(MetaDataException.class);

            Assertions.assertThatThrownBy(() -> validateIndexEvolution(metaDataValidator, index,
                    ImmutableMap.of(IndexOptions.HNSW_NUM_DIMENSIONS, "128",
                            IndexOptions.HNSW_M_MAX, "8"))).isInstanceOf(MetaDataException.class);

            Assertions.assertThatThrownBy(() -> validateIndexEvolution(metaDataValidator, index,
                    ImmutableMap.of(IndexOptions.HNSW_NUM_DIMENSIONS, "128",
                            IndexOptions.HNSW_M_MAX_0, "16"))).isInstanceOf(MetaDataException.class);

            Assertions.assertThatThrownBy(() -> validateIndexEvolution(metaDataValidator, index,
                    ImmutableMap.of(IndexOptions.HNSW_NUM_DIMENSIONS, "128",
                            IndexOptions.HNSW_EF_CONSTRUCTION, "500"))).isInstanceOf(MetaDataException.class);

            Assertions.assertThatThrownBy(() -> validateIndexEvolution(metaDataValidator, index,
                    ImmutableMap.of(IndexOptions.HNSW_NUM_DIMENSIONS, "128",
                            IndexOptions.HNSW_EF_REPAIR, "500"))).isInstanceOf(MetaDataException.class);

            Assertions.assertThatThrownBy(() -> validateIndexEvolution(metaDataValidator, index,
                    ImmutableMap.of(IndexOptions.HNSW_NUM_DIMENSIONS, "128",
                            IndexOptions.HNSW_EXTEND_CANDIDATES, "true"))).isInstanceOf(MetaDataException.class);

            Assertions.assertThatThrownBy(() -> validateIndexEvolution(metaDataValidator, index,
                    ImmutableMap.of(IndexOptions.HNSW_NUM_DIMENSIONS, "128",
                            IndexOptions.HNSW_KEEP_PRUNED_CONNECTIONS, "true")))
                    .isInstanceOf(MetaDataException.class);

            Assertions.assertThatThrownBy(() -> validateIndexEvolution(metaDataValidator, index,
                    ImmutableMap.of(IndexOptions.HNSW_NUM_DIMENSIONS, "128",
                            IndexOptions.HNSW_USE_RABITQ, "true"))).isInstanceOf(MetaDataException.class);

            Assertions.assertThatThrownBy(() -> validateIndexEvolution(metaDataValidator, index,
                    ImmutableMap.of(IndexOptions.HNSW_NUM_DIMENSIONS, "128",
                            IndexOptions.HNSW_RABITQ_NUM_EX_BITS, "1"))).isInstanceOf(MetaDataException.class);
        }
    }

    private void validateIndexEvolution(@Nonnull final MetaDataValidator metaDataValidator,
                                        @Nonnull final Index oldIndex, @Nonnull final Map<String, String> optionsMap) {
        final Index newIndex =
                new Index("GroupedVectorIndex",
                        new KeyWithValueExpression(concat(field("group_id"), field("vector_data")), 1),
                        IndexTypes.VECTOR,
                        optionsMap);

        final IndexMaintainerFactoryRegistry indexMaintainerRegistry = recordStore.getIndexMaintainerRegistry();
        final IndexMaintainerFactory indexMaintainerFactory =
                indexMaintainerRegistry.getIndexMaintainerFactory(oldIndex);

        final IndexValidator validatorForCompatibleNewIndex =
                indexMaintainerFactory.getIndexValidator(newIndex);
        validatorForCompatibleNewIndex.validate(metaDataValidator);
        validatorForCompatibleNewIndex.validateChangedOptions(oldIndex);
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
                                    .putOption(VectorIndexScanOptions.HNSW_RETURN_VECTORS, returnVectors)
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
                            assertThat((double)recallCounter / k).isGreaterThan(0.9));
        }
    }

    @Nonnull
    private static RecordQueryIndexPlan createIndexPlan(@Nonnull final HalfRealVector queryVector, final int k,
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
    private static VectorIndexScanComparisons createVectorIndexScanComparisons(@Nonnull final HalfRealVector queryVector, final int k,
                                                                               @Nonnull final VectorIndexScanOptions vectorIndexScanOptions) {
        final Comparisons.DistanceRankValueComparison distanceRankComparison =
                new Comparisons.DistanceRankValueComparison(Comparisons.Type.DISTANCE_RANK_LESS_THAN_OR_EQUAL,
                        new LiteralValue<>(Type.Vector.of(false, 16, 128), queryVector),
                        new LiteralValue<>(k));

        return VectorIndexScanComparisons.byDistance(ScanComparisons.EMPTY,
                distanceRankComparison, vectorIndexScanOptions);
    }
}
