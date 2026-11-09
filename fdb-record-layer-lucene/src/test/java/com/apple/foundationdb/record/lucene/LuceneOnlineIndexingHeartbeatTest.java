/*
 * LuceneOnlineIndexingHeartbeatTest.java
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.IndexBuildProto;
import com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository;
import com.apple.foundationdb.record.lucene.directory.MockedLuceneIndexMaintainerFactory;
import com.apple.foundationdb.record.lucene.directory.TestingIndexMaintainerRegistry;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexingSubspaces;
import com.apple.foundationdb.record.provider.foundationdb.OnlineIndexer;
import com.apple.foundationdb.record.provider.foundationdb.indexing.IndexingHeartbeat;
import com.apple.foundationdb.synchronizedsession.SynchronizedSessionLockedException;
import com.apple.test.Tags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.SIMPLE_TEXT_SUFFIXES;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.createSimpleDocument;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.SIMPLE_DOC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Tests of the indexing heartbeat of an {@link OnlineIndexer} that merges a Lucene index while building it.
 */
@Tag(Tags.RequiresFDB)
class LuceneOnlineIndexingHeartbeatTest extends FDBLuceneTestBase {
    private TestingIndexMaintainerRegistry registry;
    private InjectedFailureRepository injectedFailures;

    @BeforeEach
    void setup() {
        registry = new TestingIndexMaintainerRegistry();
        injectedFailures = new InjectedFailureRepository();
        registry.overrideFactory(new MockedLuceneIndexMaintainerFactory(injectedFailures));
    }

    @Test
    void buildIndexMergeLongerThanLeaseKeepsHeartbeatAlive() {
        final Index index = SIMPLE_TEXT_SUFFIXES;
        saveRecordsWhileDisabled(index, 30);

        final AtomicBoolean heartbeatAged = new AtomicBoolean(false);
        final AtomicBoolean secondIndexerChecked = new AtomicBoolean(false);
        final AtomicReference<Throwable> secondIndexerRefusal = new AtomicReference<>();
        // Emulate a merge that outlasts the lease: at the start of the first merge, age the first indexer's heartbeat
        // beyond the lease length. Then, while that merge still holds its directory, attempt to start a second indexer.
        injectedFailures.setHook(InjectedFailureRepository.Hooks.LUCENE_BEFORE_MERGE_INDEX, () -> {
            if (heartbeatAged.compareAndSet(false, true)) {
                ageHeartbeats(index, TimeUnit.HOURS.toMillis(1));
            }
        });
        injectedFailures.setHook(InjectedFailureRepository.Hooks.LUCENE_AFTER_MERGE_INDEX, () -> {
            if (heartbeatAged.get() && secondIndexerChecked.compareAndSet(false, true)) {
                secondIndexerRefusal.set(startSecondIndexer(index));
            }
        });

        final Throwable firstIndexerFailure;
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, index);
            try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                    .setRecordStore(recordStore)
                    .setIndex(index)
                    .setLimit(1)
                    .build()) {
                // Captured, as a second indexer that was not refused would complete the build and fail this one
                firstIndexerFailure = catchThrowable(() -> indexer.buildIndex(true));
            }
            context.commit();
        }

        assertThat(secondIndexerChecked).isTrue();
        assertThat(secondIndexerRefusal.get()).isInstanceOf(SynchronizedSessionLockedException.class);
        assertThat(firstIndexerFailure).isNull();
    }

    private void saveRecordsWhileDisabled(final Index index, final int numRecords) {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, index);
            recordStore.markIndexDisabled(index).join();
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, index);
            for (int i = 0; i < numRecords; i++) {
                recordStore.saveRecord(createSimpleDocument(1000L + i, ENGINEER_JOKE, 1));
            }
            context.commit();
        }
    }

    @Nullable
    private Throwable startSecondIndexer(final Index index) {
        // A second, non-mutual, indexer of the same index. It should be refused while the first one is active.
        try (OnlineIndexer secondIndexer = OnlineIndexer.newBuilder()
                .setDatabase(fdb)
                .setRecordStoreBuilder(recordStore.asBuilder())
                .setIndex(index)
                .build()) {
            secondIndexer.buildIndex(true);
            return null;
        } catch (SynchronizedSessionLockedException ex) {
            return ex;
        }
    }

    private void ageHeartbeats(final Index index, final long ageMillis) {
        // Opened directly: the test base's openContext copies this (merge) thread's MDC, which already has a transaction id
        try (FDBRecordContext context = fdb.openContext()) {
            final FDBRecordStore store = recordStore.asBuilder().setContext(context).open();
            final Map<UUID, IndexBuildProto.IndexBuildHeartbeat> heartbeats = context.asyncToSync(
                    FDBStoreTimer.Waits.WAIT_CHECK_ONGOING_ONLINE_INDEX_BUILD,
                    IndexingHeartbeat.getIndexingHeartbeats(store, index, 0));
            assertThat(heartbeats).isNotEmpty();
            final long heartbeatTimeMillis = System.currentTimeMillis() - ageMillis;
            heartbeats.forEach((indexerId, heartbeat) -> context.ensureActive().set(
                    IndexingSubspaces.indexHeartbeatSubspaceBytes(store, index, indexerId),
                    heartbeat.toBuilder().setHeartbeatTimeMilliseconds(heartbeatTimeMillis).build().toByteArray()));
            context.commit();
        }
    }

    private void rebuildIndexMetaData(final FDBRecordContext context, final Index index) {
        recordStore = LuceneIndexTestUtils.rebuildIndexMetaData(context, path, SIMPLE_DOC, index,
                isUseCascadesPlanner(), registry).getLeft();
    }
}
