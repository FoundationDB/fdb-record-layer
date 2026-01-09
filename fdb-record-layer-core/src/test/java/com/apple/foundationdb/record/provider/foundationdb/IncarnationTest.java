/*
 * IncarnationTest.java
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import java.util.function.IntFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for the incarnation field in the StoreHeader.
 */
class IncarnationTest extends FDBRecordStoreTestBase {

    @Test
    void testGetIncarnationDefault() throws Exception {
        assertIncarnation(0, "Default incarnation should be 0");
    }

    @Test
    void testIncrementIncarnation() throws Exception {
        updateIncarnation(current -> 10);
        updateIncarnation(current -> current + 1);
        assertIncarnation(11, "Incarnation should be 11 after increment");
    }

    @Test
    void testUpdateIncarnationMultipleTimes() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            for (int i = 0; i < 10; i++) {
                recordStore.updateIncarnation(current -> current + 1).join();
            }
            context.commit();
        }

        assertIncarnation(10, "Incarnation should be 3 after multiple updates");
    }

    @Test
    void testUpdateIncarnationToZero() throws Exception {
        updateIncarnation(current -> 0);
        assertIncarnation(0, "Incarnation should be 0");
    }

    @Test
    void testNoChanges() throws Exception {
        updateIncarnation(current -> current);
        assertIncarnation(0, "Incarnation should be 0");
        updateIncarnation(current -> 10);
        updateIncarnation(current -> current);
        assertIncarnation(10, "Incarnation should be 0");
    }

    @Test
    void testDecreaseIncarnation() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertThrows(RecordCoreException.class, () -> recordStore.updateIncarnation(current -> current - 5).join(),
                    "updateIncarnation should throw when attempting to decrease");
            context.commit();
        }
    }

    @Test
    void testIncarnationWithRecordOperations() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.updateIncarnation(current -> 42).join();

            // Add some records
            TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1)
                    .setNumValue2(100)
                    .build();
            recordStore.saveRecord(rec);

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertEquals(42, recordStore.getIncarnation(), "Incarnation should persist with record operations");

            // Verify the record exists
            FDBStoredRecord<Message> storedRecord = recordStore.loadRecord(Tuple.from(1L));
            assertNotNull(storedRecord, "Record should exist");
            TestRecords1Proto.MySimpleRecord.Builder builder = TestRecords1Proto.MySimpleRecord.newBuilder();
            builder.mergeFrom(storedRecord.getRecord());
            assertEquals(1, builder.getRecNo());

            context.commit();
        }
    }

    @Test
    void testIncarnationRequiresCorrectFormatVersion() {
        try (FDBRecordContext context = openContext()) {
            recordStore = getStoreBuilder(context, simpleMetaData(NO_HOOK), path,
                    FormatVersionTestUtils.previous(FormatVersion.INCARNATION))
                    .createOrOpen();
            assertThrows(RecordCoreException.class, () -> recordStore.getIncarnation(),
                    "getIncarnation should throw when format version is too low");
            assertThrows(RecordCoreException.class, () -> recordStore.updateIncarnation(current -> current + 1).join(),
                    "updateIncarnation should throw when format version is too low");
            context.commit();
        }
    }

    private void assertIncarnation(final int expected, final String message) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertEquals(expected, recordStore.getIncarnation(), message);
            context.commit();
        }
    }

    private void updateIncarnation(IntFunction<Integer> update) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.updateIncarnation(update).join();
            context.commit();
        }
    }
}
