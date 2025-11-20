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

import com.apple.foundationdb.async.hnsw.NodeReference;
import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.IndexFetchMethod;
import com.apple.foundationdb.record.IndexScanType;
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
import com.apple.test.RandomizedTestUtils;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;

class VectorIndexTest extends VectorIndexTestBase {
    private static final Logger logger = LoggerFactory.getLogger(VectorIndexTest.class);

    @Nonnull
    static Stream<Arguments> randomSeedsWithAsync() {
        return RandomizedTestUtils.randomSeeds(0xdeadc0deL)
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
                saveRecords(useAsync, this::addVectorIndexes, random, 1000, 0.3);
        try (final FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addVectorIndexes);
            for (int l = 0; l < 1000; l ++) {
                final FDBStoredRecord<Message> loadedRecord =
                        recordStore.loadRecord(savedRecords.get(l).getPrimaryKey());

                Assertions.assertThat(loadedRecord).isNotNull();
                Assertions.assertThat(loadedRecord.getRecord()).isEqualTo(savedRecords.get(l).getRecord());
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
                saveRecords(useAsync, this::addUngroupedVectorIndex, random, 1000);

        final Set<Long> expectedResults =
                sortByDistances(savedRecords, queryVector, Metric.EUCLIDEAN_METRIC).stream()
                        .limit(k)
                        .map(nodeReferenceWithDistance ->
                                nodeReferenceWithDistance.getPrimaryKey().getLong(0))
                        .collect(ImmutableSet.toImmutableSet());

        final var indexPlan =
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
            Assertions.assertThat(allCounter).isEqualTo(k);
            Assertions.assertThat((double)recallCounter / k).isGreaterThan(0.9);
        }
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithAsyncAndLimit")
    void basicWriteIndexReadGroupedWithContinuationTest(final long seed, final boolean useAsync, final int limit) throws Exception {
        final int k = 100;
        final Random random = new Random(seed);
        final HalfRealVector queryVector = randomHalfVector(random, 128);

        final Map<Integer, Set<Long>> expectedResults =
                saveRandomRecords(random, this::addGroupedVectorIndex, useAsync, 1000, queryVector);
        final var indexPlan = createIndexPlan(queryVector, k, "GroupedVectorIndex");

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
            Assertions.assertThat(Ints.asList(allCounters))
                    .allSatisfy(allCounter ->
                            Assertions.assertThat(allCounter).isEqualTo(k));
            Assertions.assertThat(Ints.asList(recallCounters))
                    .allSatisfy(recallCounter ->
                            Assertions.assertThat((double)recallCounter / k).isGreaterThan(0.9));
        }
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithAsync")
    void deleteWhereGroupedTest(final long seed, final boolean useAsync) throws Exception {
        final int k = 100;
        final Random random = new Random(seed);
        final HalfRealVector queryVector = randomHalfVector(random, 128);

        final Map<Integer, Set<Long>> expectedResults = saveRandomRecords(random, this::addGroupedVectorIndex,
                useAsync, 200, queryVector);
        final var indexPlan = createIndexPlan(queryVector, k, "GroupedVectorIndex");

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addGroupedVectorIndex);
            recordStore.deleteRecordsWhere(Query.field("group_id").equalsValue(0));

            final int[] allCounters = new int[2];
            final int[] recallCounters = new int[2];
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = executeQuery(indexPlan)) {
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
            Assertions.assertThat(allCounters[0]).isEqualTo(0);
            Assertions.assertThat(allCounters[1]).isEqualTo(k);

            Assertions.assertThat((double)recallCounters[0] / k).isEqualTo(0.0);
            Assertions.assertThat((double)recallCounters[1] / k).isGreaterThan(0.9);
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
                            .put(IndexOptions.HNSW_DETERMINISTIC_SEEDING, "false")
                            .put(IndexOptions.HNSW_METRIC, Metric.EUCLIDEAN_METRIC.name())
                            .put(IndexOptions.HNSW_NUM_DIMENSIONS, "128")
                            .put(IndexOptions.HNSW_USE_INLINING, "false")
                            .put(IndexOptions.HNSW_M, "16")
                            .put(IndexOptions.HNSW_M_MAX, "16")
                            .put(IndexOptions.HNSW_M_MAX_0, "32")
                            .put(IndexOptions.HNSW_EF_CONSTRUCTION, "200")
                            .put(IndexOptions.HNSW_EXTEND_CANDIDATES, "false")
                            .put(IndexOptions.HNSW_KEEP_PRUNED_CONNECTIONS, "false")
                            .put(IndexOptions.HNSW_USE_RABITQ, "false")
                            .put(IndexOptions.HNSW_RABITQ_NUM_EX_BITS, "4")

                            // these are allowed to change in any way
                            .put(IndexOptions.HNSW_SAMPLE_VECTOR_STATS_PROBABILITY, "0.999")
                            .put(IndexOptions.HNSW_MAINTAIN_STATS_PROBABILITY, "0.78")
                            .put(IndexOptions.HNSW_STATS_THRESHOLD, "500")
                            .put(IndexOptions.HNSW_MAX_NUM_CONCURRENT_NODE_FETCHES, "17")
                            .put(IndexOptions.HNSW_MAX_NUM_CONCURRENT_NEIGHBORHOOD_FETCHES, "9").build());

            Assertions.assertThatThrownBy(() -> validateIndexEvolution(metaDataValidator, index,
                    ImmutableMap.of(IndexOptions.HNSW_NUM_DIMENSIONS, "128",
                            IndexOptions.HNSW_DETERMINISTIC_SEEDING, "true"))).isInstanceOf(MetaDataException.class);

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

    @Nonnull
    private static RecordQueryIndexPlan createIndexPlan(@Nonnull final HalfRealVector queryVector, final int k,
                                                        @Nonnull final String indexName) {
        final Comparisons.DistanceRankValueComparison distanceRankComparison =
                new Comparisons.DistanceRankValueComparison(Comparisons.Type.DISTANCE_RANK_LESS_THAN_OR_EQUAL,
                        new LiteralValue<>(Type.Vector.of(false, 16, 128), queryVector),
                        new LiteralValue<>(k));

        final VectorIndexScanComparisons vectorIndexScanComparisons =
                VectorIndexScanComparisons.byDistance(ScanComparisons.EMPTY, distanceRankComparison,
                        VectorIndexScanOptions.empty());

        final var baseRecordType =
                Type.Record.fromFieldDescriptorsMap(
                        Type.Record.toFieldDescriptorMap(VectorRecord.getDescriptor().getFields()));

        return new RecordQueryIndexPlan(indexName, field("recNo"),
                vectorIndexScanComparisons, IndexFetchMethod.SCAN_AND_FETCH,
                RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY, false, false,
                Optional.empty(), baseRecordType, QueryPlanConstraint.noConstraint());
    }

    @Nonnull
    private Map<Integer, Set<Long>> saveRandomRecords(@Nonnull final Random random, @Nonnull final RecordMetaDataHook hook,
                                                      final boolean useAsync, final int numSamples,
                                                      @Nonnull final HalfRealVector queryVector) throws Exception {
        final List<FDBStoredRecord<Message>> savedRecords =
                saveRecords(useAsync, hook, random, numSamples);

        return sortByDistances(savedRecords, queryVector, Metric.EUCLIDEAN_METRIC)
                .stream()
                .map(NodeReference::getPrimaryKey)
                .map(primaryKey -> primaryKey.getLong(0))
                .collect(Collectors.groupingBy(nodeId -> Math.toIntExact(nodeId) % 2, Collectors.toSet()));
    }
}
