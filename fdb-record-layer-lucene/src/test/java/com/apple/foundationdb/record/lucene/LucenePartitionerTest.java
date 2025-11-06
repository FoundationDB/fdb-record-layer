/*
 * LucenePartitionerTest.java
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.RecordCoreInternalException;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreConcurrentTestBase;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Tag(Tags.RequiresFDB)
public class LucenePartitionerTest extends FDBRecordStoreConcurrentTestBase {
    @Test
    void testDecrementCountNegative() throws Exception {
        final long seed = 6647237;
        final int repartitionCount = 3;
        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(seed, this::getStoreBuilder, pathManager)
                .setIsGrouped(true)
                .setIsSynthetic(true)
                .setPrimaryKeySegmentIndexEnabled(true)
                .setPartitionHighWatermark(10)
                .build();

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, repartitionCount)
                .addProp(LuceneRecordContextProperties.LUCENE_MAX_DOCUMENTS_TO_MOVE_DURING_REPARTITIONING, dataModel.nextInt(1000) + repartitionCount)
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, (double)dataModel.nextInt(10) + 2) // it must be at least 2.0
                .build();

        dataModel.saveManyRecords(20, () -> openContext(contextProps), dataModel.nextInt(15) + 1);

        mergeIndex(contextProps, dataModel);

        // decrement partition counts
        try (FDBRecordContext context = openContext(contextProps)) {
            FDBRecordStore recordStore = dataModel.createOrOpenRecordStore(context);
            final LuceneIndexMaintainer indexMaintainer = (LuceneIndexMaintainer)recordStore.getIndexMaintainer(dataModel.index);
            final LucenePartitioner partitioner = indexMaintainer.getPartitioner();
            dataModel.groupingKeyToPrimaryKeyToPartitionKey.keySet().forEach(groupingKey -> {
                final LucenePartitionInfoProto.LucenePartitionInfo firstPartition = partitioner.getAllPartitionMetaInfo(groupingKey).join().stream().findFirst().get();
                Assertions.assertThatThrownBy(() -> partitioner.decrementCountAndSave(groupingKey, 5000, firstPartition.getId()).join())
                        .hasCauseInstanceOf(RecordCoreInternalException.class);
            });
            // Commit here to ensure that the data is not corrupt as a result
            context.commit();
        }

        dataModel.validate(() -> openContext(contextProps));
    }

    @ParameterizedTest
    @BooleanSource
    void testTryRemovingNonEmptyPartition(boolean isGrouped) throws Exception {
        // Test that removeEmptyPartition correctly detects and refuses to remove a partition that still has documents
        final long seed = 123456;
        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(seed, this::getStoreBuilder, pathManager)
                .setIsGrouped(isGrouped)
                .setIsSynthetic(true)
                .setPrimaryKeySegmentIndexEnabled(true)
                .setPartitionHighWatermark(10)
                .build();

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 2)
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, 2.0)
                .build();

        // Create multiple partitions with documents
        try (FDBRecordContext context = openContext(contextProps)) {
            dataModel.saveRecordsToAllGroups(25, context);
            commit(context);
        }

        mergeIndex(contextProps, dataModel);

        // Try to remove a non-empty partition
        try (FDBRecordContext context = openContext(contextProps)) {
            FDBRecordStore recordStore = dataModel.createOrOpenRecordStore(context);
            final LuceneIndexMaintainer indexMaintainer = (LuceneIndexMaintainer)recordStore.getIndexMaintainer(dataModel.index);
            final LucenePartitioner partitioner = indexMaintainer.getPartitioner();

            dataModel.groupingKeyToPrimaryKeyToPartitionKey.keySet().forEach(groupingKey -> {
                // Get partitions for this grouping key
                final List<LucenePartitionInfoProto.LucenePartitionInfo> partitions =
                        partitioner.getAllPartitionMetaInfo(groupingKey).join();
                Assertions.assertThat(partitions).hasSizeGreaterThan(0);

                // Get the first partition (which has documents in the actual Lucene index)
                final LucenePartitionInfoProto.LucenePartitionInfo partitionWithDocs = partitions.get(0);
                // Note: We don't check the count metadata field here because it could theoretically be 0
                // even if documents exist in the index (metadata out of sync). The test verifies that
                // verifyNoDocumentsInPartition() checks the actual index, not just the metadata.

                // Create a RepartitioningContext for removing this partition
                final LucenePartitionInfoProto.LucenePartitionInfo olderPartition = partitions.size() > 1 ? partitions.get(1) : null;
                final LuceneRepartitionPlanner.RepartitioningContext repartitioningContext =
                        new LuceneRepartitionPlanner.RepartitioningContext(
                                groupingKey,
                                partitions.stream().mapToInt(LucenePartitionInfoProto.LucenePartitionInfo::getId).max().orElse(0),
                                partitionWithDocs,
                                olderPartition,
                                null
                        );
                repartitioningContext.countToMove = 0; // Set to 0 to indicate removing empty partition
                repartitioningContext.emptyingPartition = true;

                // Call removeEmptyPartition - it should return 0 because the partition is not empty
                final int result = partitioner.removeEmptyPartition(repartitioningContext);

                // The method should return 0 indicating it did not remove the partition
                Assertions.assertThat(result).isZero();

                // Verify the partition still exists
                final List<LucenePartitionInfoProto.LucenePartitionInfo> partitionsAfter =
                        partitioner.getAllPartitionMetaInfo(groupingKey).join();
                Assertions.assertThat(partitionsAfter).hasSize(partitions.size());
            });

            context.commit();
        }

        // Validate the index is still consistent
        dataModel.validate(() -> openContext(contextProps));
    }

    @ParameterizedTest
    @BooleanSource
    void testMergeNonEmptyPartitionFails(boolean isGrouped) throws Exception {
        // Test that a partition with inconsistent metadata does not get removed
        final long seed = 36548347;
        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(seed, this::getStoreBuilder, pathManager)
                .setIsGrouped(isGrouped)
                .setIsSynthetic(true)
                .setPrimaryKeySegmentIndexEnabled(true)
                .setPartitionHighWatermark(10)
                .build();

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 2)
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, 2.0)
                .build();

        // Create multiple partitions with documents
        try (FDBRecordContext context = openContext(contextProps)) {
            dataModel.saveRecordsToAllGroups(25, context);
            commit(context);
        }

        mergeIndex(contextProps, dataModel);

        Map<Tuple, Integer> partitionsWithZeroCount = new HashMap<>();
        // simulate a partition metadata being 0 (though still has records)
        try (FDBRecordContext context = openContext(contextProps)) {
            FDBRecordStore recordStore = dataModel.createOrOpenRecordStore(context);
            final LuceneIndexMaintainer indexMaintainer = (LuceneIndexMaintainer)recordStore.getIndexMaintainer(dataModel.index);
            final LucenePartitioner partitioner = indexMaintainer.getPartitioner();

            dataModel.groupingKeyToPrimaryKeyToPartitionKey.keySet().forEach(groupingKey -> {
                final List<LucenePartitionInfoProto.LucenePartitionInfo> partitions =
                        partitioner.getAllPartitionMetaInfo(groupingKey).join();
                Assertions.assertThat(partitions).hasSizeGreaterThan(0);
                // Get the first partition (which has documents in the actual Lucene index)
                final LucenePartitionInfoProto.LucenePartitionInfo partition = partitions.get(0);
                // zero out the partition's count
                partitioner.decrementCountAndSave(groupingKey, partition.getCount(), partition.getId());
                partitionsWithZeroCount.put(groupingKey, partition.getId());
            });

            context.commit();
        }

        // Merge index (should not remove the partition)
        mergeIndex(contextProps, dataModel);

        // Ensure partition does not get removed
        try (FDBRecordContext context = openContext(contextProps)) {
            FDBRecordStore recordStore = dataModel.createOrOpenRecordStore(context);
            final LuceneIndexMaintainer indexMaintainer = (LuceneIndexMaintainer)recordStore.getIndexMaintainer(dataModel.index);
            final LucenePartitioner partitioner = indexMaintainer.getPartitioner();

            dataModel.groupingKeyToPrimaryKeyToPartitionKey.keySet().forEach(groupingKey -> {
                final List<LucenePartitionInfoProto.LucenePartitionInfo> partitions =
                        partitioner.getAllPartitionMetaInfo(groupingKey).join();
                Assertions.assertThat(partitions).anyMatch(partition -> partition.getId() == partitionsWithZeroCount.get(groupingKey));
            });
        }
    }

    private void mergeIndex(final RecordLayerPropertyStorage contextProps, final LuceneIndexTestDataModel dataModel) {
        try (FDBRecordContext context = openContext(contextProps)) {
            dataModel.explicitMergeIndex(context, null);
            context.commit();
        }
    }
}
