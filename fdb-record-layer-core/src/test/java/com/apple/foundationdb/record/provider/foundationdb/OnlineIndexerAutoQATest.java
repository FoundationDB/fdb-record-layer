/*
 * OnlineIndexerAutoQATest.java
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
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.provider.foundationdb.indexing.IndexingHeartbeat;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Additional tests for {@link OnlineIndexer}.
 */
@Tag(Tags.RequiresFDB)
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class OnlineIndexerAutoQATest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    @RegisterExtension
    final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);

    private FDBDatabase fdb;
    private KeySpacePath path;
    private RecordMetaData metaData;
    private FDBRecordStore recordStore;

    @BeforeEach
    void setUp() {
        final FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();
        factory.setInitialDelayMillis(2L);
        factory.setMaxDelayMillis(4L);
        factory.setMaxAttempts(100);

        fdb = dbExtension.getDatabase();
        fdb.setAsyncToSyncTimeout(5, TimeUnit.MINUTES);
        path = pathManager.createPath(TestKeySpace.RECORD_STORE);
    }

    private void openSimpleMetaData(@Nonnull Index index) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        metaDataBuilder.addIndex("MySimpleRecord", index);
        metaData = metaDataBuilder.getRecordMetaData();
    }

    @Nonnull
    private FDBRecordStore.Builder createStoreBuilder() {
        return FDBRecordStore.newBuilder()
                .setMetaDataProvider(metaData)
                .setFormatVersion(FormatVersion.getMaximumSupportedVersion())
                .setKeySpacePath(path);
    }

    private FDBRecordContext openContext() {
        FDBRecordContext context = fdb.openContext();
        recordStore = createStoreBuilder().setContext(context).createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);
        return context;
    }

    private static void snooze(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    @Test
    void checkAnyOngoingOnlineIndexBuildsStaleHeartbeatReturnsFalse() {
        // FIXED UPSTREAM AQA-1 (#4929): checkAnyOngoingOnlineIndexBuildsAsync compares heartbeatTime < now + lease, which is true for any past
        // heartbeat, so stale sessions count as active. Spec: OnlineIndexer javadoc, "leasingMilliseconds max heartbeat
        // age to be considered an active session".
        // A heartbeat older than the given lease length must not count as an active indexing session
        final Index index = new Index("simple$num_value_2", field("num_value_2"), IndexTypes.VALUE);
        openSimpleMetaData(index);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            new IndexingHeartbeat(UUID.randomUUID(), "stale", 10, false).updateHeartbeat(recordStore, index);
            context.commit();
        }
        snooze(500);
        try (FDBRecordContext context = openContext()) {
            final Map<UUID, IndexBuildProto.IndexBuildHeartbeat> heartbeats =
                    IndexingHeartbeat.getIndexingHeartbeats(recordStore, index, 0).join();
            assertThat(heartbeats).hasSize(1);
            final long age = System.currentTimeMillis() - heartbeats.values().iterator().next().getHeartbeatTimeMilliseconds();
            assertThat(age).isGreaterThan(100L);
            // "leasingMilliseconds max heartbeat age to be considered an 'active session'"
            assertThat(OnlineIndexer.checkAnyOngoingOnlineIndexBuildsAsync(recordStore, index, 100L).join())
                    .as("heartbeat of age %d ms with a lease of 100 ms", age)
                    .isFalse();
            context.commit();
        }
    }

    @Test
    void allowTakeoverContinueEmptyCollectionAllowsNoConversion() {
        // BUG AQA-2: allowTakeoverContinue(Collection) calls EnumSet.copyOf, which throws on an empty non-EnumSet collection.
        // Spec: OnlineIndexer.IndexingPolicy.Builder#allowTakeoverContinue javadoc, "Null or empty set will allow no conversion".
        // The javadoc states that a null or empty set allows no conversion
        assertThatCode(() -> OnlineIndexer.IndexingPolicy.newBuilder()
                .allowTakeoverContinue(List.of())
                .build())
                .doesNotThrowAnyException();
    }

    @Test
    void indexingPolicyToBuilderPreservesAllValues() {
        // A policy rebuilt from toBuilder() must be equivalent to the original
        final Index index = new Index("simple$num_value_2", field("num_value_2"), IndexTypes.VALUE);
        final OnlineIndexer.IndexingPolicy policy = OnlineIndexer.IndexingPolicy.newBuilder()
                .setSourceIndex("src")
                .setSourceIndexSubspaceKey("srcKey")
                .forbidRecordScan()
                .setIfDisabled(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                .setIfWriteOnly(OnlineIndexer.IndexingPolicy.DesiredAction.REBUILD)
                .setIfMismatchPrevious(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                .setIfReadable(OnlineIndexer.IndexingPolicy.DesiredAction.REBUILD)
                .allowUniquePendingState()
                .allowTakeoverContinue(List.of(OnlineIndexer.IndexingPolicy.TakeoverTypes.MUTUAL_TO_SINGLE))
                .setMutualIndexingBoundaries(List.of(Tuple.from(1L), Tuple.from(5L)))
                .setAllowUnblock(true, "blockId")
                .setInitialMergesCountLimit(7)
                .setReverseScanOrder(true)
                .setUsePendingWriteQueue(List.of(index))
                .setPendingWriteQueueIndexesMaxDrainAttempts(3)
                .setUseWriteOnlyWithTracking(List.of(index))
                .build();
        final OnlineIndexer.IndexingPolicy copy = policy.toBuilder().build();

        assertThat(copy.getSourceIndex()).isEqualTo("src");
        assertThat(copy.getSourceIndexSubspaceKey()).isEqualTo("srcKey");
        assertThat(copy.isForbidRecordScan()).isTrue();
        assertThat(copy.getIfDisabled()).isEqualTo(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR);
        assertThat(copy.getIfWriteOnly()).isEqualTo(OnlineIndexer.IndexingPolicy.DesiredAction.REBUILD);
        assertThat(copy.getIfMismatchPrevious()).isEqualTo(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR);
        assertThat(copy.getIfReadable()).isEqualTo(OnlineIndexer.IndexingPolicy.DesiredAction.REBUILD);
        assertThat(copy.isMutual()).isTrue();
        assertThat(copy.shouldAllowUnblock("blockId")).isTrue();
        assertThat(copy.shouldAllowUnblock("other")).isFalse();
        assertThat(copy.getInitialMergesCountLimit()).isEqualTo(7);
        assertThat(copy.isReverseScanOrder()).isTrue();
        assertThat(copy.shouldUsePendingWriteQueue(index)).isTrue();
        assertThat(copy.getPendingWriteQueueIndexesMaxDrainAttempts()).isEqualTo(3);
        assertThat(copy.shouldUseWriteOnlyWithTracking(index)).isTrue();
        final IndexBuildProto.IndexBuildIndexingStamp single = IndexBuildProto.IndexBuildIndexingStamp.newBuilder()
                .setMethod(IndexBuildProto.IndexBuildIndexingStamp.Method.BY_RECORDS).build();
        final IndexBuildProto.IndexBuildIndexingStamp mutual = IndexBuildProto.IndexBuildIndexingStamp.newBuilder()
                .setMethod(IndexBuildProto.IndexBuildIndexingStamp.Method.MUTUAL_BY_RECORDS).build();
        final IndexBuildProto.IndexBuildIndexingStamp multi = IndexBuildProto.IndexBuildIndexingStamp.newBuilder()
                .setMethod(IndexBuildProto.IndexBuildIndexingStamp.Method.MULTI_TARGET_BY_RECORDS).build();
        assertThat(copy.shouldAllowTypeConversionContinue(single, mutual)).isTrue();
        assertThat(copy.shouldAllowTypeConversionContinue(single, multi)).isFalse();
    }

    private void writeRawHeartbeat(@Nonnull Index index, @Nonnull UUID indexerId, long heartbeatTimeMillis) {
        final byte[] value = IndexBuildProto.IndexBuildHeartbeat.newBuilder()
                .setInfo("raw")
                .setCreateTimeMilliseconds(heartbeatTimeMillis)
                .setHeartbeatTimeMilliseconds(heartbeatTimeMillis)
                .build().toByteArray();
        recordStore.ensureContextActive().set(IndexingSubspaces.indexHeartbeatSubspaceBytes(recordStore, index, indexerId), value);
    }

    @Test
    void checkAnyOngoingOnlineIndexBuildsMixedHeartbeats() {
        // Only stale heartbeats means no active session; adding one fresh heartbeat makes the index count as being built
        final Index index = new Index("simple$num_value_2", field("num_value_2"), IndexTypes.VALUE);
        openSimpleMetaData(index);
        final long lease = 10_000L;
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            final long now = System.currentTimeMillis();
            writeRawHeartbeat(index, UUID.randomUUID(), now - 6 * lease);
            writeRawHeartbeat(index, UUID.randomUUID(), now - 3 * lease);
            writeRawHeartbeat(index, UUID.randomUUID(), now - lease - 1);
            writeRawHeartbeat(index, UUID.randomUUID(), 0L);
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            assertThat(OnlineIndexer.checkAnyOngoingOnlineIndexBuildsAsync(recordStore, index, lease).join()).isFalse();
            writeRawHeartbeat(index, UUID.randomUUID(), System.currentTimeMillis() - lease / 2);
            assertThat(OnlineIndexer.checkAnyOngoingOnlineIndexBuildsAsync(recordStore, index, lease).join()).isTrue();
            context.commit();
        }
    }

    @Test
    void checkAnyOngoingOnlineIndexBuildsDefaultLease() {
        // The 2-argument overload uses DEFAULT_LEASE_LENGTH_MILLIS as the maximal heartbeat age of an active session
        final Index index = new Index("simple$num_value_2", field("num_value_2"), IndexTypes.VALUE);
        openSimpleMetaData(index);
        final long lease = OnlineIndexOperationConfig.DEFAULT_LEASE_LENGTH_MILLIS;
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            writeRawHeartbeat(index, UUID.randomUUID(), System.currentTimeMillis() - 2 * lease);
            assertThat(OnlineIndexer.checkAnyOngoingOnlineIndexBuildsAsync(recordStore, index).join()).isFalse();
            writeRawHeartbeat(index, UUID.randomUUID(), System.currentTimeMillis() - lease / 2);
            assertThat(OnlineIndexer.checkAnyOngoingOnlineIndexBuildsAsync(recordStore, index).join()).isTrue();
            context.commit();
        }
    }

    @Test
    void checkAnyOngoingOnlineIndexBuildsInvalidAndZeroLease() {
        // An unparseable heartbeat is not an active session, and with a zero lease no past heartbeat is active
        final Index index = new Index("simple$num_value_2", field("num_value_2"), IndexTypes.VALUE);
        openSimpleMetaData(index);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            recordStore.ensureContextActive().set(
                    IndexingSubspaces.indexHeartbeatSubspaceBytes(recordStore, index, UUID.randomUUID()),
                    new byte[] {(byte)0xff, (byte)0xff, (byte)0xff});
            assertThat(OnlineIndexer.checkAnyOngoingOnlineIndexBuildsAsync(recordStore, index).join()).isFalse();
            writeRawHeartbeat(index, UUID.randomUUID(), System.currentTimeMillis() - 5);
            assertThat(OnlineIndexer.checkAnyOngoingOnlineIndexBuildsAsync(recordStore, index, 0L).join()).isFalse();
            assertThat(OnlineIndexer.checkAnyOngoingOnlineIndexBuildsAsync(recordStore, index, 60_000L).join()).isTrue();
            context.commit();
        }
    }

    @Test
    void checkAnyOngoingOnlineIndexBuildsFutureHeartbeat() {
        // A heartbeat slightly in the future (clock skew) has a negative age, so it is below any positive lease
        final Index index = new Index("simple$num_value_2", field("num_value_2"), IndexTypes.VALUE);
        openSimpleMetaData(index);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            writeRawHeartbeat(index, UUID.randomUUID(), System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5));
            assertThat(OnlineIndexer.checkAnyOngoingOnlineIndexBuildsAsync(recordStore, index, 100L).join()).isTrue();
            context.commit();
        }
    }

    @Test
    void checkAnyOngoingOnlineIndexBuildsInstanceUsesConfiguredLease() {
        // The instance method must use the lease length configured on the indexer
        final Index index = new Index("simple$num_value_2", field("num_value_2"), IndexTypes.VALUE);
        openSimpleMetaData(index);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexWriteOnly(index).join();
            writeRawHeartbeat(index, UUID.randomUUID(), System.currentTimeMillis() - 30_000L);
            context.commit();
        }
        try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                .setDatabase(fdb)
                .setRecordStoreBuilder(createStoreBuilder())
                .addTargetIndex(index)
                .setLeaseLengthMillis(60_000L)
                .build()) {
            assertThat(indexer.checkAnyOngoingOnlineIndexBuildsAsync().join()).isTrue();
        }
        try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                .setDatabase(fdb)
                .setRecordStoreBuilder(createStoreBuilder())
                .addTargetIndex(index)
                .setLeaseLengthMillis(1_000L)
                .build()) {
            assertThat(indexer.checkAnyOngoingOnlineIndexBuildsAsync().join()).isFalse();
        }
    }
}
