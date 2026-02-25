/*
 * VectorIndexIndexingTest.java
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
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.OnlineIndexer;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.vector.TestRecordsVectorsProto.VectorRecord;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests online indexing of vector indexes.
 * These tests verify that vector indexes can be built asynchronously after records are saved.
 */
class VectorIndexIndexingTest extends VectorIndexTestBase {

    @Test
    void buildVectorIndexOnlineUngrouped() throws Exception {
        final Random random = new Random();
        final int numRecords = 500 + random.nextInt(20);
        final int maxSize = 50 + random.nextInt(5) ;

        // Save records without any indexes
        final List<FDBStoredRecord<Message>> savedRecords =
                saveRecordsWithoutIndexing(random, numRecords);

        // Add the vector index and build it online
        final String indexName = "UngroupedVectorIndex";
        final Index vectorIndex = addAndBuildIndexOnline(indexName, this::addUngroupedVectorIndex);

        // Verify that the index was built correctly
        final HalfRealVector queryVector = randomHalfVector(random, 128);
        verifyUngroupedIndexQuery(savedRecords, queryVector, maxSize, vectorIndex);
    }

    @Test
    void buildVectorIndexOnlineGrouped() throws Exception {
        final Random random = new Random();
        final int numRecords = 500 + random.nextInt(20);
        final int maxSize = 50 + random.nextInt(5) ;

        // Save records without any indexes
        final List<FDBStoredRecord<Message>> savedRecords =
                saveRecordsWithoutIndexing(random, numRecords);

        // Add the vector index and build it online
        final String indexName = "GroupedVectorIndex";
        final Index vectorIndex = addAndBuildIndexOnline(indexName, this::addGroupedVectorIndex);

        // Verify index was built correctly
        final HalfRealVector queryVector = randomHalfVector(random, 128);
        verifyGroupedIndexQuery(savedRecords, queryVector, maxSize, vectorIndex);
    }

    @Test
    void buildVectorIndexOnlineMoreUngrouped() throws Exception {
        final Random random = new Random();
        final int initialRecords = 400 + random.nextInt(50);
        final int additionalRecords = 100 + random.nextInt(10);
        final int maxSize = 50 + random.nextInt(10);

        // Save initial records without indexes
        final List<FDBStoredRecord<Message>> initialSaved =
                saveRecordsWithoutIndexing(random, initialRecords);

        // Start building the index (but don't complete it yet)
        final String indexName = "UngroupedVectorIndex";
        addIndexWithoutBuilding(indexName, this::addUngroupedVectorIndex);

        // Save additional records while index exists (this should not build the index)
        final List<FDBStoredRecord<Message>> additionalSaved =
                saveMoreRecords(random, initialRecords, additionalRecords, this::addUngroupedVectorIndex);

        // Complete the index build
        final Index vectorIndex = buildIndexOnline(indexName, this::addUngroupedVectorIndex);

        // Verify all records (both initial and additional) are in the index
        final HalfRealVector queryVector = randomHalfVector(random, 128);
        final List<FDBStoredRecord<Message>> allRecords =
                Stream.concat(initialSaved.stream(), additionalSaved.stream())
                        .collect(Collectors.toList());
        verifyUngroupedIndexQuery(allRecords, queryVector, maxSize, vectorIndex);
    }

    @Test
    void buildVectorIndexOnlineMoreGrouped() throws Exception {
        final Random random = new Random();
        final int initialRecords = 400 + random.nextInt(50);
        final int additionalRecords = 100 + random.nextInt(10);
        final int maxSize = 50 + random.nextInt(10);

        // Save initial records without indexes
        final List<FDBStoredRecord<Message>> initialSaved =
                saveRecordsWithoutIndexing(random, initialRecords);

        // Start building the index (but don't complete it yet)
        final String indexName = "GroupedVectorIndex";
        addIndexWithoutBuilding(indexName, this::addGroupedVectorIndex);

        // Save additional records while index exists (this should not build the index)
        final List<FDBStoredRecord<Message>> additionalSaved =
                saveMoreRecords(random, initialRecords, additionalRecords, this::addGroupedVectorIndex);

        // Complete the index build
        final Index vectorIndex = buildIndexOnline(indexName, this::addGroupedVectorIndex);

        // Verify all records (both initial and additional) are in the index
        final HalfRealVector queryVector = randomHalfVector(random, 128);
        final List<FDBStoredRecord<Message>> allRecords =
                Stream.concat(initialSaved.stream(), additionalSaved.stream())
                        .collect(Collectors.toList());
        verifyGroupedIndexQuery(allRecords, queryVector, maxSize, vectorIndex);
    }

    private List<FDBStoredRecord<Message>> saveRecordsWithoutIndexing(Random random, int numRecords) throws Exception {
        final var recordGenerator = getRecordGenerator(random, 0.0);

        return batch(NO_HOOK, numRecords, 100,
                recNo -> recordStore.saveRecord(recordGenerator.apply(recNo)));
    }

    private List<FDBStoredRecord<Message>> saveMoreRecords(Random random, int startRecNo, int numRecords,
                                                           @Nonnull final RecordMetaDataHook hook) throws Exception {
        final var recordGenerator = getRecordGenerator(random, 0.0);

        return batch(hook, numRecords, 100,
                recNo -> recordStore.saveRecord(recordGenerator.apply(startRecNo + recNo)));
    }

    private Index addAndBuildIndexOnline(
            @Nonnull final String indexName,
            @Nonnull final RecordMetaDataHook indexHook) throws Exception {
        // Add the index definition
        addIndexWithoutBuilding(indexName, indexHook);

        // Build the index using OnlineIndexer
        return buildIndexOnline(indexName, indexHook);
    }

    private void addIndexWithoutBuilding(
            @Nonnull final String indexName,
            @Nonnull final RecordMetaDataHook indexHook) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, indexHook);

            // Index should be in disabled state initially
            final Index index = recordStore.getRecordMetaData().getIndex(indexName);
            assertThat(recordStore.isIndexReadable(index)).isFalse();

            commit(context);
        }
    }

    private Index buildIndexOnline(
            @Nonnull final String indexName,
            @Nonnull final RecordMetaDataHook indexHook) throws Exception {
        Index index;

        // Build the index using OnlineIndexer with the record store
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, indexHook);
            // Get the index from the recordStore
            index = recordStore.getRecordMetaData().getIndex(indexName);
            // Verify that the index will be only built here
            // (this is not a requirement, but only used for this module's tests)
            assertThat(recordStore.isIndexReadable(index)).isFalse();

            try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                    .setRecordStore(recordStore)
                    .setIndex(index)
                    .build()) {
                indexer.buildIndex(true);
            }
            commit(context);
        }

        // Verify that the index is readable
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, indexHook);
            assertThat(recordStore.isIndexReadable(index)).isTrue();
            commit(context);
        }

        return index;
    }

    private void verifyUngroupedIndexQuery(
            @Nonnull final List<FDBStoredRecord<Message>> savedRecords,
            @Nonnull final HalfRealVector queryVector,
            final int maxSize,
            @Nonnull final Index index) throws Exception {

        final Set<Long> expectedResults =
                sortByDistances(savedRecords, queryVector, Metric.EUCLIDEAN_METRIC).stream()
                        .limit(maxSize)
                        .map(ref -> ref.getPrimaryKey().getLong(0))
                        .collect(ImmutableSet.toImmutableSet());

        final RecordQueryIndexPlan indexPlan =
                createIndexPlan(queryVector, maxSize, index.getName());

        int matchCount = 0;

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addUngroupedVectorIndex);

            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor =
                         executeQuery(indexPlan, null, Bindings.EMPTY_BINDINGS, Integer.MAX_VALUE)) {
                while (cursor.hasNext()) {
                    final FDBQueriedRecord<Message> rec = cursor.next();
                    final VectorRecord vectorRecord = VectorRecord.newBuilder()
                            .mergeFrom(Objects.requireNonNull(rec).getRecord())
                            .build();

                    if (expectedResults.contains(vectorRecord.getRecNo())) {
                        matchCount++;
                    }
                }
            }

            assertThat(matchCount).isEqualTo(expectedResults.size());
            commit(context);
        }
    }

    private void verifyGroupedIndexQuery(
            @Nonnull final List<FDBStoredRecord<Message>> savedRecords,
            @Nonnull final HalfRealVector queryVector,
            final int maxSize,
            @Nonnull final Index index) throws Exception {

        final var groupedExpected =
                trueTopK(groupAndSortByDistances(savedRecords, queryVector), maxSize);

        final RecordQueryIndexPlan indexPlan =
                createIndexPlan(queryVector, maxSize, index.getName());
        final int numGroups = groupedExpected.size();

        final int[] matchCounts = new int[numGroups];

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addGroupedVectorIndex);

            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor =
                         executeQuery(indexPlan, null, Bindings.EMPTY_BINDINGS, Integer.MAX_VALUE)) {
                while (cursor.hasNext()) {
                    final FDBQueriedRecord<Message> rec = cursor.next();
                    final VectorRecord vectorRecord = VectorRecord.newBuilder()
                            .mergeFrom(Objects.requireNonNull(rec).getRecord())
                            .build();

                    final int groupId = vectorRecord.getGroupId();
                    assertThat(groupId).isLessThan(numGroups);
                    if (groupedExpected.getOrDefault(groupId, ImmutableSet.of()).contains(vectorRecord.getRecNo())) {
                        matchCounts[groupId]++;
                    }
                }
            }

            for (int groupId = 0; groupId < numGroups; groupId++) {
                assertThat(matchCounts[groupId])
                        .isEqualTo(groupedExpected.getOrDefault(groupId, ImmutableSet.of()).size());
            }

            commit(context);
        }
    }
}
