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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for the incarnation field in the StoreHeader.
 */
class IncarnationTest extends OnlineIndexerTest {

    @Test
    void testGetIncarnationDefault() {
        // Test that getIncarnation returns 0 when not set
        openSimpleMetaData();

        try (FDBRecordContext context = openContext()) {
            assertEquals(0, recordStore.getIncarnation(), "Default incarnation should be 0");
            context.commit();
        }
    }

    @Test
    void testIncrementIncarnation() {
        // Test incrementing the incarnation value
        openSimpleMetaData();

        // Set initial value
        try (FDBRecordContext context = openContext()) {
            recordStore.updateIncarnation(current -> 10).join();
            context.commit();
        }

        // Increment using updateIncarnation
        try (FDBRecordContext context = openContext()) {
            recordStore.updateIncarnation(current -> current + 1).join();
            context.commit();
        }

        // Verify the increment
        try (FDBRecordContext context = openContext()) {
            assertEquals(11, recordStore.getIncarnation(), "Incarnation should be 11 after increment");
            context.commit();
        }
    }

    @Test
    void testUpdateIncarnationMultipleTimes() {
        // Test updating the incarnation multiple times
        openSimpleMetaData();

        try (FDBRecordContext context = openContext()) {
            for (int i = 0; i < 10; i++) {
                recordStore.updateIncarnation(current -> current + 1).join();
            }
            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            assertEquals(10, recordStore.getIncarnation(), "Incarnation should be 3 after multiple updates");
            context.commit();
        }
    }

    @Test
    void testUpdateIncarnationToZero() {
        // Test setting incarnation to zero explicitly
        openSimpleMetaData();

        try (FDBRecordContext context = openContext()) {
            recordStore.updateIncarnation(current -> 100).join();
            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            recordStore.updateIncarnation(current -> 0).join();
            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            assertEquals(0, recordStore.getIncarnation(), "Incarnation should be 0");
            context.commit();
        }
    }

    @Test
    void testUpdateIncarnationNegativeValue() {
        // Test that setting incarnation to a negative value throws an exception
        openSimpleMetaData();

        try (FDBRecordContext context = openContext()) {
            assertThrows(RecordCoreException.class, () -> recordStore.updateIncarnation(current -> -5).join(),
                    "updateIncarnation should throw when given a negative value");
            context.commit();
        }
    }

    @Test
    void testIncarnationWithRecordOperations() {
        // Test that incarnation persists alongside normal record operations
        openSimpleMetaData();

        try (FDBRecordContext context = openContext()) {
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
            assertEquals(42, recordStore.getIncarnation(), "Incarnation should persist with record operations");

            // Verify the record exists
            FDBStoredRecord<com.google.protobuf.Message> storedRecord = recordStore.loadRecord(
                    com.apple.foundationdb.tuple.Tuple.from(1L)
            );
            assertNotNull(storedRecord, "Record should exist");
            TestRecords1Proto.MySimpleRecord.Builder builder = TestRecords1Proto.MySimpleRecord.newBuilder();
            builder.mergeFrom(storedRecord.getRecord());
            assertEquals(1, builder.getRecNo());

            context.commit();
        }
    }

    @Test
    void testIncarnationRequiresCorrectFormatVersion() {
        // Test that incarnation methods throw an exception when format version is too low
        this.formatVersion = FormatVersionTestUtils.previous(FormatVersion.INCARNATION);
        openSimpleMetaData();

        try (FDBRecordContext context = openContext()) {
            assertThrows(RecordCoreException.class, () -> recordStore.getIncarnation(),
                    "getIncarnation should throw when format version is too low");
            assertThrows(RecordCoreException.class, () -> recordStore.updateIncarnation(current -> current + 1).join(),
                    "updateIncarnation should throw when format version is too low");
            context.commit();
        }
    }
}
