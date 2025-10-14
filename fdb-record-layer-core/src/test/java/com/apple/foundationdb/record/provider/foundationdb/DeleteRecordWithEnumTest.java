/*
 * DeleteRecordWithEnumTest.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test a specific problem - when deleting a record with enum (PermitRequestQueueEntry), An index entry containing the
 * enum value appears to be undeleted.
 */
public class DeleteRecordWithEnumTest {

    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    @RegisterExtension
    final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);

    FDBDatabase fdb;
    KeySpacePath path;
    RecordMetaData metaData;
    RecordQueryPlanner planner;
    FDBRecordStore recordStore;
    private IndexMaintenanceFilter indexMaintenanceFilter;

    @BeforeEach
    void setup() {
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
        FDBRecordStore.Builder builder = FDBRecordStore.newBuilder()
                .setMetaDataProvider(metaData)
                .setKeySpacePath(path)
                .setContext(context);
        recordStore = builder.createOrOpen(FDBRecordStoreBase.StoreExistenceCheck.NONE);
        planner = new RecordQueryPlanner(metaData, recordStore.getRecordStoreState(), recordStore.getTimer());
        return context;
    }

    @Test
    void testDeleteRecordWithIndexToEnum() throws ExecutionException, InterruptedException {
        final Index myIndex = new Index("idx_permit_queue_entry_queue_status_vest_task_id",
                Key.Expressions.concatenateFields("queue_id", "status", "vest_at", "task_id", "id"));

        openMetaData(metaDataBuilder -> metaDataBuilder.addIndex("PermitRequestQueueEntry", myIndex));
        populateData();

        try (FDBRecordContext context = openContext()) {
            final List<FDBStoredRecord<Message>> fdbStoredRecords = recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asList().get();
            assertEquals(1L, recordStore.scanIndexRecords(myIndex.getName()).asList().get().size());
            assertEquals(1L, fdbStoredRecords.size());
            recordStore.deleteRecord(fdbStoredRecords.get(0).getPrimaryKey());
            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            assertEquals(0L, recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asList().get().size());
            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            assertEquals(0L, recordStore.scanIndexRecords(myIndex.getName()).asList().get().size());
            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            try (OnlineIndexScrubber scrubber = OnlineIndexScrubber.newBuilder()
                    .setDatabase(fdb)
                    .setRecordStore(recordStore)
                    .setIndex(myIndex)
                    .build()) {
                final long danglingCount = scrubber.scrubDanglingIndexEntries();
                final long missingCount = scrubber.scrubMissingIndexEntries();

                Assertions.assertEquals(0, danglingCount);
                Assertions.assertEquals(0, missingCount);
            }
            context.commit();
        }
    }

    private void populateData() {
        try (FDBRecordContext context = openContext()) {
            final TestRecords1Proto.PermitRequestQueueEntry myRecord = TestRecords1Proto.PermitRequestQueueEntry.newBuilder()
                    .setId("requestId")
                    .setQueueId("queue id")
                    .setTaskId("task id")
                    .setStatus(TestRecords1Proto.PermitRequestQueueEntryStatus.DEFAULT)
                    .setVestAt(123)
                    .setAction("action")
                    .build();
            recordStore.insertRecord(myRecord);
            context.commit();
        }
    }

    private void openMetaData(FDBRecordStoreTestBase.RecordMetaDataHook hook) {
        Descriptors.FileDescriptor descriptor = TestRecords1Proto.getDescriptor();
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(descriptor);
        hook.apply(metaDataBuilder);
        metaData = metaDataBuilder.getRecordMetaData();
    }

}
