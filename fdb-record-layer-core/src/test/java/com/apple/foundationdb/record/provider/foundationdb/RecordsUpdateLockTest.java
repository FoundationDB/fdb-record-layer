/*
 * RecordsUpdateLockTest.java
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

import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.StoreIsLockedForRecordUpdates;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.Comparators;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RecordsUpdateLockTest extends OnlineIndexerTest {

    @BeforeEach
    void beforeEach() {
        this.formatVersion = Comparators.min(FormatVersion.STORE_LOCK_STATE, this.formatVersion);
    }

    @Test
    void testForbidUpdateRecord() {
        // Forbid record updates, assert failure to update records while forbidden
        openSimpleMetaData();
        populateStore(20);

        forbidRecordUpdate();

        try (FDBRecordContext context = openContext()) {
            // Fail to create new or modify existing records
            for (int i: List.of(0, 10, 20, 8888)) {
                TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(i).setNumValue2(i + 100).build();
                assertThrows(StoreIsLockedForRecordUpdates.class, () -> recordStore.saveRecord(record));

                // Dry run should always succeed
                recordStore.dryRunSaveRecordAsync(record, FDBRecordStoreBase.RecordExistenceCheck.NONE).join();
            }
            context.commit();
        }

        allowRecordUpdate();

        // Successfully update records
        try (FDBRecordContext context = openContext()) {
            for (int i = 15; i < 25; i++) {
                TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(i).setNumValue2(i).build();
                recordStore.saveRecord(record);
            }
            context.commit();
        }
    }

    @Test
    void testForbidUpdatesBuildIndex() {
        // Successfully build index under record update lock
        final Index index = new Index("newIndex", field("num_value_2"));
        final FDBRecordStoreTestBase.RecordMetaDataHook hook = metaDataBuilder -> {
            metaDataBuilder.addIndex("MySimpleRecord", index);
        };
        openSimpleMetaData(hook);
        populateStore(23);
        disableAll(List.of(index));

        forbidRecordUpdate();

        // Assert non-readable index
        try (FDBRecordContext context = openContext()) {
            assertFalse(recordStore.getRecordStoreState().allIndexesReadable());
            context.commit();
        }

        // Build index while record updates are forbidden
        try (OnlineIndexer indexBuilder = newIndexerBuilder()
                .setIndex(index)
                .build()) {
            indexBuilder.buildIndex();
        }

        // Assert readable
        assertReadable(index);

        allowRecordUpdate();
    }

    @Test
    void testForbidUpdateRecordsAndBuildIndex() {
        // Set a "forbid record updates" state, verify that records cannot be updates yet indexes can be built
        final Index index = new Index("newIndex", field("num_value_2"));
        final FDBRecordStoreTestBase.RecordMetaDataHook hook = metaDataBuilder -> {
            metaDataBuilder.addIndex("MySimpleRecord", index);
        };
        openSimpleMetaData(hook);
        populateStore(20);
        disableAll(List.of(index));

        forbidRecordUpdate();

        try (FDBRecordContext context = openContext()) {
            // Fail to create new or modify existing records
            for (int i: List.of(0, 10, 20, 8888)) {
                TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(i).setNumValue2(i + 100).build();
                assertThrows(StoreIsLockedForRecordUpdates.class, () -> recordStore.saveRecord(record));

                // Dry run should always succeed
                recordStore.dryRunSaveRecordAsync(record, FDBRecordStoreBase.RecordExistenceCheck.NONE).join();
            }
            context.commit();
        }

        // Assert non-readable index
        try (FDBRecordContext context = openContext()) {
            assertFalse(recordStore.getRecordStoreState().allIndexesReadable());
            context.commit();
        }

        // Build index while record updates are forbidden
        try (OnlineIndexer indexBuilder = newIndexerBuilder()
                .setIndex(index)
                .build()) {
            indexBuilder.buildIndex();
        }

        // Assert readable
        assertReadable(index);

        allowRecordUpdate();

        // Successfully update records
        try (FDBRecordContext context = openContext()) {
            for (int i = 15; i < 25; i++) {
                TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(i).setNumValue2(i).build();
                recordStore.saveRecord(record);
            }
            context.commit();
        }

        // Assert readable index again
        assertReadable(index);
    }

    @Test
    void testForbidUpdatesAndDeleteRecords() {
        // Forbid record updates, assert failure to delete existing records
        final Index index = new Index("newIndex", field("num_value_2"));
        final FDBRecordStoreTestBase.RecordMetaDataHook hook = metaDataBuilder -> {
            metaDataBuilder.addIndex("MySimpleRecord", index);
        };
        openSimpleMetaData(hook);
        populateStore(23);

        forbidRecordUpdate();

        // Make sure that records cannot be updates
        try (FDBRecordContext context = openContext()) {
            // Delete existing records - should fail
            for (int i: List.of(0, 4, 5, 19)) {
                assertThrows(StoreIsLockedForRecordUpdates.class, () -> recordStore.deleteRecord(Tuple.from(i)));
                // Dry run should always succeed
                recordStore.dryRunDeleteRecordAsync(Tuple.from(i)).join();
            }
            // Delete non-existing records - no effect, therefore should succeed
            for (int i: List.of(23, 300, 8888)) {
                recordStore.deleteRecord(Tuple.from(i));
                // Dry run should always succeed
                recordStore.dryRunDeleteRecordAsync(Tuple.from(i)).join();
            }
            context.commit();
        }

        allowRecordUpdate();

        // Successfully delete records
        try (FDBRecordContext context = openContext()) {
            for (int i: List.of(0, 4, 5, 19, 200)) {
                recordStore.deleteRecord(Tuple.from(i));
            }
            context.commit();
        }
    }

    @Test
    void testForbidUpdatesAndDeleteAll() {
        // Forbid record updates, assert failure to delete all records
        openSimpleMetaData();
        populateStore(23);

        forbidRecordUpdate();

        try (FDBRecordContext context = openContext()) {
            // Delete all records - should fail
            assertThrows(StoreIsLockedForRecordUpdates.class, () -> recordStore.deleteAllRecords());
            context.commit();
        }

        allowRecordUpdate();

        // Successfully delete all
        try (FDBRecordContext context = openContext()) {
            recordStore.deleteAllRecords();
            context.commit();
        }
    }

    @Test
    void testForbidUpdatesAndDeleteWhere() {
        // Forbid record updates, assert deleteWhere failure
        openSimpleMetaData();
        populateStore(5);

        forbidRecordUpdate();

        try (FDBRecordContext context = openContext()) {
            // Delete where - should fail (the dummy expression will not be evaluated)
            assertThrows(StoreIsLockedForRecordUpdates.class,
                    () -> recordStore.deleteRecordsWhere(Query.field("RecNo").greaterThan(10)));
            context.commit();
        }

        allowRecordUpdate();
    }

    private void populateStore(int count) {
        try (FDBRecordContext context = openContext()) {
            for (int i = 0; i < count; i++) {
                TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(i).setNumValue2(i).build();
                recordStore.saveRecord(record);
            }
            context.commit();
        }
    }

    private void forbidRecordUpdate() {
        try (FDBRecordContext context = openContext()) {
            recordStore.setStoreLockStateAsync(RecordMetaDataProto.DataStoreInfo.StoreLockState.State.FORBID_RECORD_UPDATE, "testing").join();
            context.commit();
        }
    }

    private void allowRecordUpdate() {
        // Clear update records lock
        try (FDBRecordContext context = openContext()) {
            recordStore.clearStoreLockStateAsync().join();
            context.commit();
        }
    }

}
