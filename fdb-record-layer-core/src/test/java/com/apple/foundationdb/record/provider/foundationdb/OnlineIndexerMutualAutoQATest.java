/*
 * OnlineIndexerMutualAutoQATest.java
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

import com.apple.foundationdb.record.IndexBuildProto;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.indexing.IndexingHeartbeat;
import com.apple.foundationdb.synchronizedsession.SynchronizedSessionLockedException;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Additional tests for mutually building indexes with {@link OnlineIndexer}.
 */
@Tag(Tags.Slow)
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class OnlineIndexerMutualAutoQATest extends OnlineIndexerTest {
    private static final String CRASH_MESSAGE = "Intentionally crash during test";

    @Nonnull
    private static List<Index> countAndValueIndexes() {
        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("countIndex", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));
        indexes.add(new Index("valueIndex", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        return indexes;
    }

    private void buildMutuallyAndCrash(List<Index> indexes, List<Tuple> boundaries, int after) {
        openSimpleMetaData(allIndexesHook(indexes));
        final AtomicLong counter = new AtomicLong(0);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes)
                .setLimit(3)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setMutualIndexingBoundaries(boundaries)
                        .build())
                .setConfigLoader(old -> {
                    if (counter.incrementAndGet() > after) {
                        throw new RecordCoreException(CRASH_MESSAGE);
                    }
                    return old;
                })
                .build()) {
            assertThatThrownBy(indexBuilder::buildIndex).hasMessageContaining(CRASH_MESSAGE);
        }
    }

    private void buildMutually(List<Index> indexes, List<Tuple> boundaries) {
        openSimpleMetaData(allIndexesHook(indexes));
        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes)
                .setLimit(3)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setMutualIndexingBoundaries(boundaries)
                        .build())
                .build()) {
            indexBuilder.buildIndex(true);
        }
    }

    private long readCount(Index countIndex) {
        try (FDBRecordContext context = openContext()) {
            final List<IndexEntry> entries = recordStore.scanIndex(countIndex, IndexScanType.BY_GROUP, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                    .asList().join();
            context.commit();
            return entries.isEmpty() ? 0L : entries.get(0).getValue().getLong(0);
        }
    }

    private long countRecords() {
        try (FDBRecordContext context = openContext()) {
            final long count = recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).getCount().join();
            context.commit();
            return count;
        }
    }

    private void saveRecords(long from, long to) {
        try (FDBRecordContext context = openContext()) {
            for (long val = from; val < to; val++) {
                recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(val)
                        .setNumValue2((int)val * 19)
                        .setNumValue3Indexed((int)val * 77)
                        .setNumValueUnique((int)val * 1139)
                        .build());
            }
            context.commit();
        }
    }

    @Test
    void mutualIndexingInterleavedWithDifferentBoundariesCountsEachRecordOnce() {
        // A mutual indexer crashes halfway, records change, then a peer with different (unsorted) boundaries finishes: COUNT must match the records
        final List<Index> indexes = countAndValueIndexes();
        populateData(100);
        openSimpleMetaData(allIndexesHook(indexes));
        disableAll(indexes);

        buildMutuallyAndCrash(indexes, getBoundariesList(100, 10), 4);

        // Modify records while the indexes are write-only: some in built ranges, some in unbuilt ranges
        openSimpleMetaData(allIndexesHook(indexes));
        try (FDBRecordContext context = openContext()) {
            for (long recNo = 0; recNo < 100; recNo += 7) {
                recordStore.deleteRecord(Tuple.from(recNo));
            }
            context.commit();
        }
        saveRecords(100, 120);

        final List<Tuple> otherBoundaries = Arrays.asList(null, Tuple.from(70L), Tuple.from(20L), Tuple.from(45L), Tuple.from(105L), null);
        buildMutually(indexes, otherBoundaries);

        assertReadable(indexes);
        scrubAndValidate(indexes);
        assertThat(readCount(indexes.get(0))).isEqualTo(countRecords());
    }

    @Test
    void mutualIndexingSeveralCrashedPeersLeaveNoHeartbeatsWhenDone() {
        // Several mutual peers crash at different points; the final peer marks readable and no heartbeat of any peer remains
        final List<Index> indexes = countAndValueIndexes();
        populateData(90);
        openSimpleMetaData(allIndexesHook(indexes));
        disableAll(indexes);
        final List<Tuple> boundaries = getBoundariesList(90, 9);

        buildMutuallyAndCrash(indexes, boundaries, 2);
        buildMutuallyAndCrash(indexes, boundaries, 3);

        buildMutually(indexes, boundaries);

        assertReadable(indexes);
        scrubAndValidate(indexes);
        assertThat(readCount(indexes.get(0))).isEqualTo(90L);
        try (FDBRecordContext context = openContext()) {
            for (Index index : indexes) {
                assertThat(IndexingHeartbeat.getIndexingHeartbeats(recordStore, index, 0).join()).isEmpty();
            }
            context.commit();
        }
    }

    @Test
    void mutualIndexingBoundariesWithoutEndpointsNeverMarksPartialIndexReadable() {
        // Boundaries that do not cover the whole records space must not lead to a readable, partly built index
        final List<Index> indexes = countAndValueIndexes();
        populateData(100);
        openSimpleMetaData(allIndexesHook(indexes));
        disableAll(indexes);

        final List<Tuple> boundaries = List.of(Tuple.from(30L), Tuple.from(60L));
        openSimpleMetaData(allIndexesHook(indexes));
        RecordCoreException failure = null;
        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setMutualIndexingBoundaries(boundaries)
                        .build())
                .build()) {
            indexBuilder.buildIndex(true);
        } catch (RecordCoreException ex) {
            failure = ex;
        }

        openSimpleMetaData(allIndexesHook(indexes));
        try (FDBRecordContext context = openContext()) {
            final boolean readable = recordStore.getIndexState(indexes.get(0)).isReadable();
            context.commit();
            if (readable) {
                assertThat(readCount(indexes.get(0))).isEqualTo(100L);
            } else {
                assertThat(failure).isNotNull();
            }
        }

        // A full-range peer can always complete the build
        buildMutually(indexes, getBoundariesList(100, 25));
        assertReadable(indexes);
        scrubAndValidate(indexes);
        assertThat(readCount(indexes.get(0))).isEqualTo(100L);
    }

    @Test
    void mutualIndexingCrashThenSingleTakeoverCountsEachRecordOnce() {
        // A mutual multi-target build crashes; each target is continued by a single by-records indexer (MUTUAL_TO_SINGLE)
        final List<Index> indexes = countAndValueIndexes();
        populateData(80);
        openSimpleMetaData(allIndexesHook(indexes));
        disableAll(indexes);

        buildMutuallyAndCrash(indexes, getBoundariesList(80, 8), 3);
        saveRecords(80, 95);

        openSimpleMetaData(allIndexesHook(indexes));
        for (Index index : indexes) {
            try (OnlineIndexer indexBuilder = newIndexerBuilder(index)
                    .setLimit(4)
                    .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                            .allowTakeoverContinue(List.of(OnlineIndexer.IndexingPolicy.TakeoverTypes.MUTUAL_TO_SINGLE))
                            .build())
                    .build()) {
                indexBuilder.buildIndex(true);
            }
        }

        assertReadable(indexes);
        scrubAndValidate(indexes);
        assertThat(readCount(indexes.get(0))).isEqualTo(95L);
    }

    private void writePeerHeartbeat(List<Index> indexes, UUID peerId, long heartbeatTimeMillis) {
        try (FDBRecordContext context = openContext()) {
            for (Index index : indexes) {
                final byte[] value = IndexBuildProto.IndexBuildHeartbeat.newBuilder()
                        .setInfo(IndexBuildProto.IndexBuildIndexingStamp.Method.MUTUAL_BY_RECORDS.toString())
                        .setCreateTimeMilliseconds(heartbeatTimeMillis)
                        .setHeartbeatTimeMilliseconds(heartbeatTimeMillis)
                        .build().toByteArray();
                context.ensureActive().set(IndexingSubspaces.indexHeartbeatSubspaceBytes(recordStore, index, peerId), value);
            }
            context.commit();
        }
    }

    private boolean anyOngoingBuild(Index index, long leaseMillis) {
        try (FDBRecordContext context = openContext()) {
            final boolean ongoing = OnlineIndexer.checkAnyOngoingOnlineIndexBuildsAsync(recordStore, index, leaseMillis).join();
            context.commit();
            return ongoing;
        }
    }

    @Nonnull
    private OnlineIndexer buildSingleByRecordsAfterMutual(Index index) {
        return newIndexerBuilder(index)
                .setLimit(5)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .allowTakeoverContinue(List.of(OnlineIndexer.IndexingPolicy.TakeoverTypes.MUTUAL_TO_SINGLE))
                        .build())
                .build();
    }

    @Test
    void mutualPeerHeartbeatStaleIsIgnoredWhileFreshIsRespected() {
        // A crashed mutual build has a peer heartbeat: fresh blocks an exclusive takeover, stale (older than the lease) does not
        final Index countIndex = new Index("countIndex", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT);
        final List<Index> indexes = List.of(countIndex);
        populateData(60);
        openSimpleMetaData(allIndexesHook(indexes));
        disableAll(indexes);
        buildMutuallyAndCrash(indexes, getBoundariesList(60, 10), 2);

        final long lease = OnlineIndexOperationConfig.DEFAULT_LEASE_LENGTH_MILLIS;
        final UUID peerId = UUID.randomUUID();
        openSimpleMetaData(allIndexesHook(indexes));
        writePeerHeartbeat(indexes, peerId, System.currentTimeMillis());
        assertThat(anyOngoingBuild(countIndex, lease)).isTrue();
        try (OnlineIndexer indexBuilder = buildSingleByRecordsAfterMutual(countIndex)) {
            assertThatThrownBy(indexBuilder::buildIndex).isInstanceOf(SynchronizedSessionLockedException.class);
        }

        writePeerHeartbeat(indexes, peerId, System.currentTimeMillis() - 3 * lease);
        assertThat(anyOngoingBuild(countIndex, lease)).isFalse();
        try (OnlineIndexer indexBuilder = buildSingleByRecordsAfterMutual(countIndex)) {
            indexBuilder.buildIndex(true);
        }

        assertReadable(indexes);
        assertThat(readCount(countIndex)).isEqualTo(60L);
        try (FDBRecordContext context = openContext()) {
            assertThat(IndexingHeartbeat.getIndexingHeartbeats(recordStore, countIndex, 0).join()).isEmpty();
            context.commit();
        }
    }

    @Test
    void mutualPeersWithFreshHeartbeatFinishTheBuild() {
        // A fresh mutual peer heartbeat does not block another mutual indexer, which finishes the peer's fragments
        final List<Index> indexes = countAndValueIndexes();
        populateData(70);
        openSimpleMetaData(allIndexesHook(indexes));
        disableAll(indexes);
        final List<Tuple> boundaries = getBoundariesList(70, 10);
        buildMutuallyAndCrash(indexes, boundaries, 2);

        openSimpleMetaData(allIndexesHook(indexes));
        writePeerHeartbeat(indexes, UUID.randomUUID(), System.currentTimeMillis());
        writePeerHeartbeat(indexes, UUID.randomUUID(), System.currentTimeMillis() - 3 * OnlineIndexOperationConfig.DEFAULT_LEASE_LENGTH_MILLIS);
        buildMutually(indexes, boundaries);

        assertReadable(indexes);
        scrubAndValidate(indexes);
        assertThat(readCount(indexes.get(0))).isEqualTo(70L);
    }

    @Test
    void mutualIndexingWithEmptyTupleBoundary() {
        // An empty tuple boundary (packs below every primary key) must still lead to a complete build
        final List<Index> indexes = countAndValueIndexes();
        populateData(50);
        openSimpleMetaData(allIndexesHook(indexes));
        disableAll(indexes);

        buildMutually(indexes, Arrays.asList(null, Tuple.from(), Tuple.from(25L), null));

        assertReadable(indexes);
        scrubAndValidate(indexes);
        assertThat(readCount(indexes.get(0))).isEqualTo(50L);
    }

    @Test
    void mutualIndexingEmptyStoreWithPresetBoundaries() {
        // An empty store with preset boundaries (including boundaries of another tuple type) becomes readable with a zero count
        final List<Index> indexes = countAndValueIndexes();
        openSimpleMetaData(allIndexesHook(indexes));
        try (FDBRecordContext context = openContext()) {
            context.commit();
        }
        disableAll(indexes);

        buildMutually(indexes, Arrays.asList(null, Tuple.from(10L), Tuple.from("a"), Tuple.from(20L), null));

        assertReadable(indexes);
        assertThat(readCount(indexes.get(0))).isZero();
        saveRecords(0, 5);
        assertThat(readCount(indexes.get(0))).isEqualTo(5L);
    }

    @Test
    void mutualIndexingSplitLongRecordsAcrossCrash() {
        // Split long records, preset primary key boundaries, and a crash mid-way: each record is counted once
        final List<Index> indexes = countAndValueIndexes();
        final FDBRecordStoreTestBase.RecordMetaDataHook hook = metaDataBuilder -> {
            metaDataBuilder.setSplitLongRecords(true);
            metaDataBuilder.removeIndex("MySimpleRecord$str_value_indexed");
            allIndexesHook(indexes).apply(metaDataBuilder);
        };
        final String longString = "x".repeat(250_000);
        openSimpleMetaData(hook);
        disableAll(indexes);
        try (FDBRecordContext context = openContext()) {
            for (long val = 0; val < 12; val++) {
                recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(val)
                        .setStrValueIndexed(longString)
                        .setNumValueUnique((int)val * 1139)
                        .build());
            }
            context.commit();
        }
        final List<Tuple> boundaries = Arrays.asList(null, Tuple.from(3L), Tuple.from(7L), null);

        final AtomicLong counter = new AtomicLong(0);
        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes)
                .setLimit(1)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setMutualIndexingBoundaries(boundaries)
                        .build())
                .setConfigLoader(old -> {
                    if (counter.incrementAndGet() > 4) {
                        throw new RecordCoreException(CRASH_MESSAGE);
                    }
                    return old;
                })
                .build()) {
            assertThatThrownBy(indexBuilder::buildIndex).hasMessageContaining(CRASH_MESSAGE);
        }
        try (OnlineIndexer indexBuilder = newIndexerBuilder(indexes)
                .setLimit(2)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setMutualIndexingBoundaries(boundaries)
                        .build())
                .build()) {
            indexBuilder.buildIndex(true);
        }

        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            for (Index index : indexes) {
                assertThat(recordStore.getIndexState(index).isReadable()).isTrue();
            }
            context.commit();
        }
        assertThat(readCount(indexes.get(0))).isEqualTo(12L);
        try (FDBRecordContext context = openContext()) {
            final List<IndexEntry> entries = recordStore.scanIndex(indexes.get(1), IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                    .asList().join();
            assertThat(entries).hasSize(12);
            context.commit();
        }
    }
}
