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
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ObjectArrays;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.protobuf.Message;
import org.assertj.core.api.Assertions;
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

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;

public class VectorIndexTest extends VectorIndexTestBase {
    private static final Logger logger = LoggerFactory.getLogger(VectorIndexTest.class);

    static Stream<Arguments> randomSeedsWithAsync() {
        return RandomizedTestUtils.randomSeeds(0xdeadc0deL)
                .flatMap(seed -> Sets.cartesianProduct(ImmutableSet.of(true, false)).stream()
                        .map(arguments -> Arguments.of(ObjectArrays.concat(seed, arguments.toArray()))));
    }

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
                saveRecords(useAsync, this::addVectorIndexes, random, 1000);
        try (final FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addVectorIndexes);
            for (int l = 0; l < 1000; l ++) {
                final FDBStoredRecord<Message> loadedRecord =
                        recordStore.loadRecord(savedRecords.get(l).getPrimaryKey());

                Assertions.assertThat(loadedRecord).isNotNull();
                VectorRecord.Builder recordBuilder =
                        VectorRecord.newBuilder();
                recordBuilder.mergeFrom(loadedRecord.getRecord());
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

    @Nonnull
    private static RecordQueryIndexPlan createIndexPlan(@Nonnull final HalfRealVector queryVector, final int k,
                                                        @Nonnull final String indexName) {
        final Comparisons.DistanceRankValueComparison distanceRankComparison =
                new Comparisons.DistanceRankValueComparison(Comparisons.Type.DISTANCE_RANK_LESS_THAN_OR_EQUAL,
                        new LiteralValue<>(Type.Vector.of(false, 16, 128), queryVector),
                        new LiteralValue<>(k));

        final VectorIndexScanComparisons vectorIndexScanComparisons =
                new VectorIndexScanComparisons(ScanComparisons.EMPTY, distanceRankComparison,
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
