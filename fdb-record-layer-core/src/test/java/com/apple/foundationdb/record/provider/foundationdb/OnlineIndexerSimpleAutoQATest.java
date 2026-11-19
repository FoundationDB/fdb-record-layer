/*
 * OnlineIndexerSimpleAutoQATest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.indexes.ValueIndexMaintainerWithQueue;
import com.apple.foundationdb.tuple.Tuple;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class OnlineIndexerSimpleAutoQATest extends OnlineIndexerTest {

    private final Index queueIndex = new Index("simple$num_value_2_queue", field("num_value_2"), ValueIndexMaintainerWithQueue.Factory.INDEX_TYPE);
    private final Index countIndex = new Index("simple$count_aqa", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT);

    private void saveSimple(final long recNo, final int numValue2) {
        recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(recNo)
                .setNumValue2(numValue2)
                .build());
    }

    private void populateAndAddIndex(@Nonnull final Index index, final int numRecords) {
        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            for (int i = 0; i < numRecords; i++) {
                saveSimple(i, i * 10);
            }
            context.commit();
        }
        openSimpleMetaData(allIndexesHook(List.of(index)));
        disableAll(List.of(index));
    }

    private void buildQueuedIndexWithoutMarkingReadable() {
        try (OnlineIndexer indexer = newIndexerBuilder(queueIndex)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setUsePendingWriteQueue(List.of(queueIndex)))
                .build()) {
            indexer.buildIndex(false);
        }
        try (FDBRecordContext context = openContext()) {
            assertThat(recordStore.getIndexState(queueIndex)).isEqualTo(IndexState.WRITE_ONLY_WITH_QUEUE);
            context.commit();
        }
        // A user update while the index is still write-only-with-queue: it goes to the pending queue
        try (FDBRecordContext context = openContext()) {
            saveSimple(0, 777);
            context.commit();
        }
    }

    @Nonnull
    private List<Long> primaryKeysForValue(@Nonnull final Index index, final int value) {
        try (FDBRecordContext context = openContext()) {
            final List<Long> primaryKeys = recordStore.scanIndex(index, IndexScanType.BY_VALUE, TupleRange.allOf(Tuple.from(value)), null, ScanProperties.FORWARD_SCAN)
                    .map(entry -> entry.getPrimaryKey().getLong(0))
                    .asList().join();
            context.commit();
            return primaryKeys;
        }
    }

    private void assertQueuedUpdateIsInReadableIndex() {
        try (FDBRecordContext context = openContext()) {
            assertThat(recordStore.getIndexState(queueIndex)).isEqualTo(IndexState.READABLE);
            context.commit();
        }
        assertThat(primaryKeysForValue(queueIndex, 777)).containsExactly(0L);
        assertThat(primaryKeysForValue(queueIndex, 0)).isEmpty();
    }

    @Test
    void markReadableIfBuiltWithPendingQueueKeepsQueuedUpdates() {
        // BUG AQA-28: markReadable/markReadableIfBuilt drain the pending write queue only for indexes cached during a
        // build session, so a new indexer marks a WRITE_ONLY_WITH_QUEUE index READABLE and erases its queued updates.
        // Spec: IndexState.WRITE_ONLY_WITH_QUEUE javadoc; FDBRecordStore.markIndexReadable javadoc.
        // markReadableIfBuilt must not make a write-only-with-queue index readable while dropping its queued user updates
        populateAndAddIndex(queueIndex, 5);
        buildQueuedIndexWithoutMarkingReadable();

        try (OnlineIndexer indexer = newIndexerBuilder(queueIndex).build()) {
            indexer.asyncToSync(FDBStoreTimer.Waits.WAIT_ONLINE_BUILD_INDEX, indexer.markReadableIfBuilt());
        }
        assertQueuedUpdateIsInReadableIndex();
    }

    @Test
    void markReadableWithPendingQueueKeepsQueuedUpdates() {
        // BUG AQA-28: markReadable/markReadableIfBuilt drain the pending write queue only for indexes cached during a
        // build session, so a new indexer marks a WRITE_ONLY_WITH_QUEUE index READABLE and erases its queued updates.
        // Spec: IndexState.WRITE_ONLY_WITH_QUEUE javadoc; FDBRecordStore.markIndexReadable javadoc.
        // OnlineIndexer.markReadable of a fresh indexer must not drop the queued user updates of a write-only-with-queue index
        populateAndAddIndex(queueIndex, 5);
        buildQueuedIndexWithoutMarkingReadable();

        try (OnlineIndexer indexer = newIndexerBuilder(queueIndex)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setUsePendingWriteQueue(List.of(queueIndex)))
                .build()) {
            indexer.asyncToSync(FDBStoreTimer.Waits.WAIT_ONLINE_BUILD_INDEX, indexer.markReadable());
        }
        assertQueuedUpdateIsInReadableIndex();
    }
}
