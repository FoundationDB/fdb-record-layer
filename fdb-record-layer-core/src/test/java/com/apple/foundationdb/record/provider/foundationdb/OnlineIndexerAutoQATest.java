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
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.test.Tags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

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
    void allowTakeoverContinueEmptyCollectionAllowsNoConversion() {
        // BUG AQA-2: allowTakeoverContinue(Collection) calls EnumSet.copyOf, which throws on an empty non-EnumSet collection.
        // Spec: OnlineIndexer.IndexingPolicy.Builder#allowTakeoverContinue javadoc, "Null or empty set will allow no conversion".
        // The javadoc states that a null or empty set allows no conversion
        assertThatCode(() -> OnlineIndexer.IndexingPolicy.newBuilder()
                .allowTakeoverContinue(List.of())
                .build())
                .doesNotThrowAnyException();
    }

    private void writeRawHeartbeat(@Nonnull Index index, @Nonnull UUID indexerId, long heartbeatTimeMillis) {
        final byte[] value = IndexBuildProto.IndexBuildHeartbeat.newBuilder()
                .setInfo("raw")
                .setCreateTimeMilliseconds(heartbeatTimeMillis)
                .setHeartbeatTimeMilliseconds(heartbeatTimeMillis)
                .build().toByteArray();
        recordStore.ensureContextActive().set(IndexingSubspaces.indexHeartbeatSubspaceBytes(recordStore, index, indexerId), value);
    }
}
