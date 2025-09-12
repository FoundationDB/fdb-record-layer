/*
 * IndexingHeartbeatLowLevelTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.indexing;

import com.apple.foundationdb.record.IndexBuildProto;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.IndexingSubspaces;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.foundationdb.synchronizedsession.SynchronizedSessionLockedException;
import com.google.protobuf.Descriptors;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;

class IndexingHeartbeatLowLevelTest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    @RegisterExtension
    final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);
    FDBDatabase fdb;
    KeySpacePath path;
    FDBRecordStore recordStore;
    RecordMetaData metaData;

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

    FDBRecordContext openContext() {
        FDBRecordContext context = fdb.openContext();
        FDBRecordStore.Builder builder = createStoreBuilder()
                .setContext(context);
        recordStore = builder.createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);
        metaData = recordStore.getRecordMetaData();
        return context;
    }

    @Nonnull
    private FDBRecordStore.Builder createStoreBuilder() {
        return FDBRecordStore.newBuilder()
                .setMetaDataProvider(metaData)
                .setKeySpacePath(path);
    }

    void openMetaData(@Nonnull Descriptors.FileDescriptor descriptor, @Nonnull FDBRecordStoreTestBase.RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(descriptor);
        hook.apply(metaDataBuilder);
        metaData = metaDataBuilder.getRecordMetaData();
    }

    void openSimpleMetaData(@Nonnull FDBRecordStoreTestBase.RecordMetaDataHook hook) {
        openMetaData(TestRecords1Proto.getDescriptor(), hook);
    }

    protected static FDBRecordStoreTestBase.RecordMetaDataHook allIndexesHook(List<Index> indexes) {
        return metaDataBuilder -> {
            for (Index index: indexes) {
                metaDataBuilder.addIndex("MySimpleRecord", index);
            }
        } ;
    }

    @Test
    void testHeartbeatQuery() {
        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));
        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);

        final int count = 23;
        IndexingHeartbeat[] heartbeats = new IndexingHeartbeat[count];
        for (int i = 0; i < count; i++) {
            heartbeats[i] = new IndexingHeartbeat(UUID.randomUUID(), "heartbeat" + i, 100 + i, false);
        }

        // populate heartbeats
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            for (var heartbeat : heartbeats) {
                heartbeat.updateHeartbeat(recordStore, indexes.get(0));
                heartbeat.updateHeartbeat(recordStore, indexes.get(1));
            }
            context.commit();
        }

        // Verify query operation
        for (Index index: indexes) {
            try (FDBRecordContext context = openContext()) {
                // Query, unlimited
                Map<UUID, IndexBuildProto.IndexBuildHeartbeat> queried =
                        IndexingHeartbeat.getIndexingHeartbeats(recordStore, index, 0).join();
                Assertions.assertThat(queried).hasSize(count);
                Assertions.assertThat(queried.keySet())
                        .containsExactlyInAnyOrderElementsOf(Arrays.stream(heartbeats).map(heartbeat -> heartbeat.indexerId).collect(Collectors.toList()));
                Assertions.assertThat(queried.values().stream().map(IndexBuildProto.IndexBuildHeartbeat::getInfo))
                        .containsExactlyInAnyOrderElementsOf(Arrays.stream(heartbeats).map(heartbeat -> heartbeat.info).collect(Collectors.toList()));
                Assertions.assertThat(queried.values().stream().map(IndexBuildProto.IndexBuildHeartbeat::getCreateTimeMilliseconds))
                        .containsExactlyInAnyOrderElementsOf(Arrays.stream(heartbeats).map(heartbeat -> heartbeat.createTimeMilliseconds).collect(Collectors.toList()));

                // Query, partial
                queried =
                        IndexingHeartbeat.getIndexingHeartbeats(recordStore, index, 5).join();
                Assertions.assertThat(queried).hasSize(5);
                context.commit();
            }
        }
    }

    @Test
    void testHeartbeatClearing() {
        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));
        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);

        final int count = 10;
        IndexingHeartbeat[] heartbeats = new IndexingHeartbeat[count];
        for (int i = 0; i < count; i++) {
            heartbeats[i] = new IndexingHeartbeat(UUID.randomUUID(), "test", 100 + i, false);
        }

        // populate heartbeats
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            for (var heartbeat : heartbeats) {
                heartbeat.updateHeartbeat(recordStore, indexes.get(0));
                heartbeat.updateHeartbeat(recordStore, indexes.get(1));
            }
            context.commit();
        }

        // Verify clear operation
        Index index = indexes.get(0);
        try (FDBRecordContext context = openContext()) {
            Map<UUID, IndexBuildProto.IndexBuildHeartbeat> queried =
                    IndexingHeartbeat.getIndexingHeartbeats(recordStore, index, 0).join();
            Assertions.assertThat(queried).hasSize(count);

            // clear, partial
            int countDeleted =
                    IndexingHeartbeat.clearIndexingHeartbeats(recordStore, index, 0, 7).join();
            Assertions.assertThat(countDeleted).isEqualTo(7);
            queried =
                    IndexingHeartbeat.getIndexingHeartbeats(recordStore, index, 5).join();
            Assertions.assertThat(queried).hasSize(3);
            context.commit();
        }

        // Verify that the previous clear does not affect other index
        index = indexes.get(1);
        try (FDBRecordContext context = openContext()) {
            Map<UUID, IndexBuildProto.IndexBuildHeartbeat> queried =
                    IndexingHeartbeat.getIndexingHeartbeats(recordStore, index, 100).join();
            Assertions.assertThat(queried).hasSize(count);

            // clear all
            int countDeleted =
                    IndexingHeartbeat.clearIndexingHeartbeats(recordStore, index, 0, 0).join();
            Assertions.assertThat(countDeleted).isEqualTo(count);

            // verify empty
            queried =
                    IndexingHeartbeat.getIndexingHeartbeats(recordStore, index, 0).join();
            Assertions.assertThat(queried).isEmpty();
            context.commit();
        }
    }

    @Test
    void testCheckAndUpdateNonMutual() {
        Index index = new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(List.of(index));
        IndexingHeartbeat heartbeat1 = new IndexingHeartbeat(UUID.randomUUID(), "Test", TimeUnit.SECONDS.toMillis(30), false);

        // Successfully update heartbeat
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            heartbeat1.checkAndUpdateHeartbeat(recordStore, index).join();
            context.commit();
        }

        // Successfully update heartbeat
        try (FDBRecordContext context = openContext()) {
            final Map<UUID, IndexBuildProto.IndexBuildHeartbeat> existingHeartbeats = IndexingHeartbeat.getIndexingHeartbeats(recordStore, index, 0).join();
            Assertions.assertThat(existingHeartbeats).hasSize(1);
            heartbeat1.checkAndUpdateHeartbeat(recordStore, index).join();
            context.commit();
        }

        IndexingHeartbeat heartbeat2 = new IndexingHeartbeat(UUID.randomUUID(), "Test", TimeUnit.SECONDS.toMillis(30), false);
        Assertions.assertThat(heartbeat1.indexerId).isNotEqualTo(heartbeat2.indexerId);
        // Fail to create another non-mutual heartbeat
        try (FDBRecordContext context = openContext()) {
            final Map<UUID, IndexBuildProto.IndexBuildHeartbeat> existingHeartbeats = IndexingHeartbeat.getIndexingHeartbeats(recordStore, index, 0).join();
            Assertions.assertThat(existingHeartbeats).hasSize(1);
            Assertions.assertThatThrownBy(() -> heartbeat2.checkAndUpdateHeartbeat(recordStore, index).join())
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(SynchronizedSessionLockedException.class);
            context.commit();
        }

        // Successfully clear heartbeat1
        try (FDBRecordContext context = openContext()) {
            final Map<UUID, IndexBuildProto.IndexBuildHeartbeat> existingHeartbeats = IndexingHeartbeat.getIndexingHeartbeats(recordStore, index, 0).join();
            Assertions.assertThat(existingHeartbeats).hasSize(1);
            heartbeat1.clearHeartbeat(recordStore, index);
            context.commit();
        }

        // Successfully update heartbeat2
        try (FDBRecordContext context = openContext()) {
            heartbeat2.checkAndUpdateHeartbeat(recordStore, index).join();
            context.commit();
        }

        // Successfully clear heartbeat2
        try (FDBRecordContext context = openContext()) {
            heartbeat2.clearHeartbeat(recordStore, index);
            final Map<UUID, IndexBuildProto.IndexBuildHeartbeat> existingHeartbeats = IndexingHeartbeat.getIndexingHeartbeats(recordStore, index, 0).join();
            Assertions.assertThat(existingHeartbeats).isEmpty();
            context.commit();
        }
    }

    @Test
    void testCheckAndUpdateMutual() {
        Index index = new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(List.of(index));

        final int count = 10;
        IndexingHeartbeat[] heartbeats = new IndexingHeartbeat[count];
        for (int i = 0; i < count; i++) {
            heartbeats[i] = new IndexingHeartbeat(UUID.randomUUID(), "Mutual", TimeUnit.SECONDS.toMillis(100), true);
        }

        // Successfully check//update all heartbeats
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            for (int i = 0; i < 3; i++) {
                for (IndexingHeartbeat heartbeat: heartbeats) {
                    heartbeat.checkAndUpdateHeartbeat(recordStore, index).join();
                }
            }
            context.commit();
        }

        // Check count, clear all
        try (FDBRecordContext context = openContext()) {
            final Map<UUID, IndexBuildProto.IndexBuildHeartbeat> existingHeartbeats = IndexingHeartbeat.getIndexingHeartbeats(recordStore, index, 0).join();
            Assertions.assertThat(existingHeartbeats).hasSize(count);

            for (int i = 0; i < 3; i++) {
                for (IndexingHeartbeat heartbeat: heartbeats) {
                    heartbeat.clearHeartbeat(recordStore, index);
                }
            }
            context.commit();
        }

        // verify cleared
        try (FDBRecordContext context = openContext()) {
            final Map<UUID, IndexBuildProto.IndexBuildHeartbeat> existingHeartbeats = IndexingHeartbeat.getIndexingHeartbeats(recordStore, index, 0).join();
            Assertions.assertThat(existingHeartbeats).isEmpty();
            context.commit();
        }
    }

    @Test
    void testSetHeartbeatAfterOtherHeartbeatExpiration() throws InterruptedException {
        Index index = new Index("versionIndex1", concat(field("num_value_2"), VersionKeyExpression.VERSION), IndexTypes.VERSION);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(List.of(index));
        IndexingHeartbeat heartbeat1 = new IndexingHeartbeat(UUID.randomUUID(), "Test", TimeUnit.SECONDS.toMillis(10), false);

        // Successfully update heartbeat1
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            heartbeat1.checkAndUpdateHeartbeat(recordStore, index).join();
            context.commit();
        }

        // Delay 20, set heartbeat2's lease to 4
        Thread.sleep(20);
        IndexingHeartbeat heartbeat2 = new IndexingHeartbeat(UUID.randomUUID(), "Test", 4, false);
        Assertions.assertThat(heartbeat1.indexerId).isNotEqualTo(heartbeat2.indexerId);

        // heartbeat2 successfully takes over
        try (FDBRecordContext context = openContext()) {
            final Map<UUID, IndexBuildProto.IndexBuildHeartbeat> existingHeartbeats = IndexingHeartbeat.getIndexingHeartbeats(recordStore, index, 0).join();
            Assertions.assertThat(existingHeartbeats).hasSize(1);
            heartbeat2.checkAndUpdateHeartbeat(recordStore, index).join();
            context.commit();
        }
    }

    @Test
    void testFailSetHeartbeatBeforeOtherHeartbeatExpiration() throws InterruptedException {
        Index index = new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(List.of(index));

        final IndexingHeartbeat heartbeatA = new IndexingHeartbeat(UUID.randomUUID(), "a", 500, false);
        final IndexingHeartbeat heartbeatB = new IndexingHeartbeat(UUID.randomUUID(), "b", 5, false);

        // Set heartbeat A
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            heartbeatA.checkAndUpdateHeartbeat(recordStore, index).join();
            context.commit();
        }

        Thread.sleep(100);
        // heartbeatB would have respected heartbeatA's lock for 5 milliseconds only. Now successfully set itself.
        try (FDBRecordContext context = openContext()) {
            heartbeatB.checkAndUpdateHeartbeat(recordStore, index).join();
            context.commit();
        }

        // Expect heartbeatA to fail check/update
        // Note: if become flakey, increase the least time of heartbeatA
        try (FDBRecordContext context = openContext()) {
            Assertions.assertThatThrownBy(() -> heartbeatA.checkAndUpdateHeartbeat(recordStore, index).join())
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(SynchronizedSessionLockedException.class);
            context.commit();
        }
    }

    @Test
    void testHeartbeatClearOldHeartbeats() throws InterruptedException {
        Index index = new Index("versionIndex1", concat(field("num_value_2"), VersionKeyExpression.VERSION), IndexTypes.VERSION);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(List.of(index));
        IndexingHeartbeat heartbeat1 = new IndexingHeartbeat(UUID.randomUUID(), "Test", TimeUnit.SECONDS.toMillis(10), true);
        IndexingHeartbeat heartbeat2 = new IndexingHeartbeat(UUID.randomUUID(), "Test", TimeUnit.SECONDS.toMillis(10), true);
        IndexingHeartbeat heartbeat3 = new IndexingHeartbeat(UUID.randomUUID(), "Test", TimeUnit.SECONDS.toMillis(10), true);

        // Successfully create heartbeat1
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            heartbeat1.checkAndUpdateHeartbeat(recordStore, index).join();
            heartbeat2.checkAndUpdateHeartbeat(recordStore, index).join();
            context.commit();
        }

        // Delay 20, clear anything older than 5 milliseconds
        Thread.sleep(20);
        try (FDBRecordContext context = openContext()) {
            final Map<UUID, IndexBuildProto.IndexBuildHeartbeat> existingHeartbeats = IndexingHeartbeat.getIndexingHeartbeats(recordStore, index, 0).join();
            Assertions.assertThat(existingHeartbeats).hasSize(2);
            final Integer numDeleted = IndexingHeartbeat.clearIndexingHeartbeats(recordStore, index, 5, 0).join();
            Assertions.assertThat(numDeleted).isEqualTo(2);
            context.commit();
        }

        // Now make sure that clear indexing heartbeats does not remove when it should not
        try (FDBRecordContext context = openContext()) {
            heartbeat3.checkAndUpdateHeartbeat(recordStore, index).join();
            final Map<UUID, IndexBuildProto.IndexBuildHeartbeat> existingHeartbeats = IndexingHeartbeat.getIndexingHeartbeats(recordStore, index, 0).join();
            Assertions.assertThat(existingHeartbeats).hasSize(1);
            final Integer numDeleted = IndexingHeartbeat.clearIndexingHeartbeats(recordStore, index, TimeUnit.SECONDS.toMillis(10), 0).join();
            Assertions.assertThat(numDeleted).isZero();
            context.commit();
        }

    }

    @Test
    void testMixedNonMutualThenMutualHeartbeats() {
        // This scenario should never happen because of the indexing type-stamp protection. Testing it anyway...
        Index index = new Index("versionIndex1", concat(field("num_value_2"), VersionKeyExpression.VERSION), IndexTypes.VERSION);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(List.of(index));
        IndexingHeartbeat heartbeatMutual = new IndexingHeartbeat(UUID.randomUUID(), "Test", TimeUnit.SECONDS.toMillis(10), true);
        IndexingHeartbeat heartbeatExclusive = new IndexingHeartbeat(UUID.randomUUID(), "Test", TimeUnit.SECONDS.toMillis(10), false);

        // lock exclusive, then successfully lock mutual.
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            heartbeatExclusive.checkAndUpdateHeartbeat(recordStore, index).join();
            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            // Mutual: Successfully updates
            heartbeatMutual.checkAndUpdateHeartbeat(recordStore, index).join();
            context.commit();
        }
    }

    @Test
    void testMixedMutualThenNonMutualHeartbeats() {
        // This scenario should never happen because of the indexing typestamp protection. Testing it anyway...
        Index index = new Index("versionIndex1", concat(field("num_value_2"), VersionKeyExpression.VERSION), IndexTypes.VERSION);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(List.of(index));
        IndexingHeartbeat heartbeatMutual = new IndexingHeartbeat(UUID.randomUUID(), "Test", TimeUnit.SECONDS.toMillis(10), true);
        IndexingHeartbeat heartbeatExclusive = new IndexingHeartbeat(UUID.randomUUID(), "Test", TimeUnit.SECONDS.toMillis(10), false);

        // lock mutual, then fail to lock exclusive
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            final Map<UUID, IndexBuildProto.IndexBuildHeartbeat> heartbeats = IndexingHeartbeat.getIndexingHeartbeats(recordStore, index, 0).join();
            Assertions.assertThat(heartbeats).isEmpty();
            heartbeatMutual.checkAndUpdateHeartbeat(recordStore, index).join();
            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            Assertions.assertThatThrownBy(() -> heartbeatExclusive.checkAndUpdateHeartbeat(recordStore, index).join())
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(SynchronizedSessionLockedException.class);
            // and clear
            heartbeatMutual.clearHeartbeat(recordStore, index);
            context.commit();
        }
    }

    @Test
    void testUnparseableHeartbeat() {
        Index index = new Index("versionIndex1", concat(field("num_value_2"), VersionKeyExpression.VERSION), IndexTypes.VERSION);
        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(List.of(index));

        // write unparseable data where a heartbeat should exist
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            byte[] key = IndexingSubspaces.indexHeartbeatSubspaceBytes(recordStore, index, UUID.randomUUID());
            byte[] value = "meaningless byte value".getBytes();
            recordStore.ensureContextActive().set(key, value);
            context.commit();
        }

        IndexingHeartbeat heartbeat = new IndexingHeartbeat(UUID.randomUUID(), "Test", TimeUnit.SECONDS.toMillis(30), false);
        try (FDBRecordContext context = openContext()) {
            final Map<UUID, IndexBuildProto.IndexBuildHeartbeat> existingHeartbeats = IndexingHeartbeat.getIndexingHeartbeats(recordStore, index, 0).join();
            Assertions.assertThat(existingHeartbeats).hasSize(1);
            heartbeat.checkAndUpdateHeartbeat(recordStore, index).join();
            context.commit();
        }

        // Make sure that the unparsable heartbeat can be cleared
        try (FDBRecordContext context = openContext()) {
            heartbeat.checkAndUpdateHeartbeat(recordStore, index).join();
            Map<UUID, IndexBuildProto.IndexBuildHeartbeat> existingHeartbeats = IndexingHeartbeat.getIndexingHeartbeats(recordStore, index, 0).join();
            Assertions.assertThat(existingHeartbeats).hasSize(2);
            final Integer numDeleted = IndexingHeartbeat.clearIndexingHeartbeats(recordStore, index, TimeUnit.SECONDS.toMillis(10), 0).join();
            Assertions.assertThat(numDeleted).isEqualTo(1);
            existingHeartbeats = IndexingHeartbeat.getIndexingHeartbeats(recordStore, index, 0).join();
            Assertions.assertThat(existingHeartbeats).hasSize(1);
            Assertions.assertThat(existingHeartbeats.get(heartbeat.indexerId)).isNotNull();
            context.commit();
        }
    }
}
