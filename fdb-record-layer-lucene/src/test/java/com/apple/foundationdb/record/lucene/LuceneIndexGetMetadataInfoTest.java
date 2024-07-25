/*
 * LuceneIndexGetMetadataInfoTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.IndexOperationResult;
import com.apple.foundationdb.record.provider.foundationdb.OnlineIndexer;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.ByteString;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests around {@link LuceneGetMetadataInfo}.
 */
public class LuceneIndexGetMetadataInfoTest extends FDBRecordStoreTestBase {

    static Stream<Arguments> arguments() {
        return Stream.of(true, false)
                .flatMap(justPartitionInfo ->
                        Stream.of(true, false)
                                .map(isGrouped -> Arguments.of(justPartitionInfo, isGrouped)));
    }

    @ParameterizedTest
    @MethodSource("arguments")
    void getMetadata(boolean justPartitionInfo, boolean isGrouped) {
        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(234097L, this::getStoreBuilder, pathManager)
                .setPartitionHighWatermark(-1) // disable partitioning
                .setIsGrouped(isGrouped)
                .build();
        final long start = Instant.now().toEpochMilli();
        for (int i = 0; i < 5; i++) {
            try (FDBRecordContext context = openContext()) {
                dataModel.saveRecords(10, start, context, i);
                commit(context);
            }
        }

        final Set<Tuple> groupingKeys = isGrouped ? dataModel.groupingKeys() : Set.of(Tuple.from());
        for (final Tuple groupingKey : groupingKeys) {
            final LuceneMetadataInfo result = getLuceneMetadataInfo(justPartitionInfo, groupingKey, dataModel, null);
            assertEquals(List.of(), result.getPartitionInfo());
            if (justPartitionInfo) {
                assertEquals(Map.of(), result.getLuceneInfo());
            } else {
                assertEquals(Set.of(0), result.getLuceneInfo().keySet());
                final LuceneMetadataInfo.LuceneInfo luceneInfo = result.getLuceneInfo().get(0);
                assertEquals(dataModel.primaryKeys(groupingKey).size(), luceneInfo.getDocumentCount());
                // When we save, we save all records for a group in a single transaction, so that will result in a
                // single segment, but when the index is not grouped we have 5 transactions, which results in 5
                // segments
                assertThat(luceneInfo.getFiles(), Matchers.hasSize(segmentCountToFileCount(isGrouped ? 1 : 5)));
                assertEquals(1, luceneInfo.getFieldInfoCount());
            }
        }
    }

    @ParameterizedTest
    @MethodSource("arguments")
    void getMetadataPartitioned(boolean justPartitionInfo, boolean isGrouped) {
        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(234097L, this::getStoreBuilder, pathManager)
                .setPartitionHighWatermark(10)
                .setIsGrouped(isGrouped)
                .build();
        final long start = Instant.now().toEpochMilli();
        for (int i = 0; i < 6; i++) {
            try (FDBRecordContext context = openContext()) {
                dataModel.saveRecords(10, start, context, i / 3);
                commit(context);
            }
            explicitMergeIndex(dataModel);
        }

        final Set<Tuple> groupingKeys = isGrouped ? dataModel.groupingKeys() : Set.of(Tuple.from());
        for (final Tuple groupingKey : groupingKeys) {
            final LuceneMetadataInfo result = getLuceneMetadataInfo(justPartitionInfo, groupingKey, dataModel, null);
            final List<LucenePartitionInfoProto.LucenePartitionInfo> partitionInfo = result.getPartitionInfo();
            // most recent is first
            final List<Integer> partitionIds = isGrouped ? List.of(0, 2, 1) : List.of(0, 5, 4, 3, 2, 1);
            assertEquals(partitionIds,
                    partitionInfo.stream().map(info -> info.getId()).collect(Collectors.toList()));
            assertEquals(partitionIds.stream().map(i -> 10).collect(Collectors.toList()),
                    partitionInfo.stream().map(info -> info.getCount()).collect(Collectors.toList()));
            assertPartitionInfosHaveCorrectFromTo(partitionInfo);
            if (justPartitionInfo) {
                assertEquals(Map.of(), result.getLuceneInfo());
            } else {
                assertEquals(Set.copyOf(partitionIds), result.getLuceneInfo().keySet());
                for (final Integer partitionId : partitionIds) {
                    final LuceneMetadataInfo.LuceneInfo luceneInfo = result.getLuceneInfo().get(partitionId);
                    assertEquals(10, luceneInfo.getDocumentCount());
                    assertThat(luceneInfo.getFiles(), Matchers.hasSize(segmentCountToFileCount(1)));
                    assertEquals(1, luceneInfo.getFieldInfoCount());

                    final LuceneMetadataInfo resultForPartition = getLuceneMetadataInfo(
                            justPartitionInfo, groupingKey, dataModel, partitionId);
                    assertEquals(Set.of(partitionId), resultForPartition.getLuceneInfo().keySet());
                    assertEquals(luceneInfo, resultForPartition.getLuceneInfo().get(partitionId));
                }
            }
        }
    }

    @Test
    void getMetadataAfterDelete() {
        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(234097L, this::getStoreBuilder, pathManager)
                .setPartitionHighWatermark(10)
                .setIsGrouped(false)
                .build();
        final long start = Instant.now().toEpochMilli();
        for (int i = 0; i < 6; i++) {
            try (FDBRecordContext context = openContext()) {
                dataModel.saveRecords(10, start, context, i / 3);
                commit(context);
            }
            explicitMergeIndex(dataModel);
        }

        final Tuple groupingKey = Tuple.from();

        try (FDBRecordContext context = openContext()) {
            final Tuple toDelete = dataModel.primaryKeys(groupingKey).stream().findFirst().orElseThrow();
            dataModel.deleteRecord(context, toDelete);
            commit(context);
        }

        final LuceneMetadataInfo result = getLuceneMetadataInfo(false, groupingKey, dataModel, null);
        final List<LucenePartitionInfoProto.LucenePartitionInfo> partitionInfo = result.getPartitionInfo();
        // most recent is first
        final List<Integer> partitionIds = List.of(0, 5, 4, 3, 2, 1);
        assertEquals(partitionIds,
                partitionInfo.stream().map(LucenePartitionInfoProto.LucenePartitionInfo::getId).collect(Collectors.toList()));
        // one will be decremented from 10 down to 9
        final List<Integer> partitionCounts = partitionInfo.stream()
                .map(LucenePartitionInfoProto.LucenePartitionInfo::getCount)
                .collect(Collectors.toList());
        assertThat(partitionCounts,
                Matchers.containsInAnyOrder(9, 10, 10, 10, 10, 10));
        assertPartitionInfosHaveCorrectFromTo(partitionInfo);
        assertEquals(Set.copyOf(partitionIds), result.getLuceneInfo().keySet());
        final int smallerPartition = partitionInfo.stream().filter(partition -> partition.getCount() == 9)
                .map(LucenePartitionInfoProto.LucenePartitionInfo::getId)
                .findFirst().orElseThrow();
        for (final Integer partitionId : partitionIds) {
            final LuceneMetadataInfo.LuceneInfo luceneInfo = result.getLuceneInfo().get(partitionId);
            if (partitionId == smallerPartition) {
                assertEquals(9, luceneInfo.getDocumentCount());
                // one extra file for the `.liv`
                assertThat(luceneInfo.getFiles(), Matchers.hasSize(segmentCountToFileCount(1) + 1));
            } else {
                assertEquals(10, luceneInfo.getDocumentCount());
                assertThat(luceneInfo.getFiles(), Matchers.hasSize(segmentCountToFileCount(1)));
            }
            assertEquals(1, luceneInfo.getFieldInfoCount());
        }
    }

    private static void assertPartitionInfosHaveCorrectFromTo(
            final List<LucenePartitionInfoProto.LucenePartitionInfo> partitionInfo) {
        for (int i = 0; i < partitionInfo.size(); i++) {
            final LucenePartitionInfoProto.LucenePartitionInfo info = partitionInfo.get(i);
            assertLessThan(info.getFrom(), info.getTo());
            if (i > 0) {
                // partition infos come back with most-recent first, this means that the nth partitionInfo will be older
                // than the n-1st, and thus the `to` on the current info should be less than the `from` on the `i-1`
                // info, for example:
                // partitionInfo.get(0) {from=8, to=10}
                // partitionInfo.get(1) {from=5, to=7}
                // partitionInfo.get(2) {from=1, to=4}
                assertLessThan(info.getTo(), partitionInfo.get(i - 1).getFrom());
            }
        }
    }

    private LuceneMetadataInfo getLuceneMetadataInfo(final boolean justPartitionInfo,
                                                     @Nonnull final Tuple groupingKey,
                                                     @Nonnull final LuceneIndexTestDataModel dataModel,
                                                     @Nullable final Integer partitionId) {
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = dataModel.schemaSetup.apply(context);
            final IndexOperationResult indexOperationResult = store.performIndexOperation(dataModel.index.getName(),
                    new LuceneGetMetadataInfo(groupingKey, partitionId, justPartitionInfo));
            assertThat(indexOperationResult, Matchers.instanceOf(LuceneMetadataInfo.class));
            return (LuceneMetadataInfo)indexOperationResult;
        }
    }

    private static void assertLessThan(final ByteString lesserOne, final ByteString greaterOne) {
        assertThat(Tuple.fromBytes(lesserOne.toByteArray()), Matchers.lessThan(Tuple.fromBytes(greaterOne.toByteArray())));
    }

    private static int segmentCountToFileCount(final int segmentCount) {
        return segmentCount * 4 + 1;
    }

    private void explicitMergeIndex(LuceneIndexTestDataModel dataModel) {
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(dataModel.schemaSetup.apply(context));
            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                    .setRecordStore(recordStore)
                    .setIndex(dataModel.index)
                    .setTimer(timer)
                    .build()) {
                indexBuilder.mergeIndex();
            }
        }
    }
}
