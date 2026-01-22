/*
 * FullStoreLockTest.java
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

import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.StoreIsFullyLockedException;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.UnknownStoreLockStateException;
import com.apple.test.Tags;
import com.google.common.collect.Comparators;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Tag(Tags.RequiresFDB)
class FullStoreLockTest extends FDBRecordStoreTestBase {

    FormatVersion formatVersion = FormatVersion.getMaximumSupportedVersion();

    @BeforeEach
    void beforeEach() {
        this.formatVersion = Comparators.max(FormatVersion.FULL_STORE_LOCK, this.formatVersion);
    }

    @Test
    void testFullStoreLockPreventsOpen() {
        // Create a store and set FULL_STORE lock, then verify it cannot be opened
        try (FDBRecordContext context = openContext()) {
            recordStore = openSimpleRecordStore(context, null, formatVersion);

            // Add some records
            for (int i = 0; i < 10; i++) {
                TestRecords1Proto.MySimpleRecord record = TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(i)
                        .setNumValue2(i * 100)
                        .build();
                recordStore.saveRecord(record);
            }

            // Set FULL_STORE lock
            recordStore.setStoreLockStateAsync(
                    RecordMetaDataProto.DataStoreInfo.StoreLockState.State.FULL_STORE,
                    "Testing full store lock"
            ).join();

            commit(context);
        }

        // Attempt to open the store - should fail
        try (FDBRecordContext context = openContext()) {
            StoreIsFullyLockedException exception = assertThrows(StoreIsFullyLockedException.class, () -> {
                openSimpleRecordStore(context, null, formatVersion);
            });
            assertNotNull(exception.getMessage());
        }
    }

    @Test
    void testUnspecifiedLockStateFailsToOpen() {
        // With FULL_STORE_LOCK format version, UNSPECIFIED state should prevent opening
        try (FDBRecordContext context = openContext()) {
            recordStore = openSimpleRecordStore(context, null, formatVersion);

            // Explicitly set UNSPECIFIED
            recordStore.setStoreLockStateAsync(
                    RecordMetaDataProto.DataStoreInfo.StoreLockState.State.UNSPECIFIED,
                    "Explicitly setting unspecified"
            ).join();

            commit(context);
        }

        // Attempt to open - should fail with UnknownStoreLockStateException
        try (FDBRecordContext context = openContext()) {
            assertThrows(UnknownStoreLockStateException.class, () -> {
                openSimpleRecordStore(context, null, formatVersion);
            });
        }
    }

    @Test
    void testUnknownLockStateFailsToOpen() throws InvalidProtocolBufferException {
        try (FDBRecordContext context = openContext()) {
            recordStore = openSimpleRecordStore(context, null, formatVersion);
            final byte[] storeInfoKey = recordStore.getSubspace().pack(FDBRecordStoreKeyspace.STORE_INFO.id());
            final int lockStateFieldNumber = RecordMetaDataProto.DataStoreInfo.StoreLockState.getDescriptor()
                    .findFieldByName("lock_state").getNumber();
            final UnknownFieldSet.Field unexpectedEnumValue = UnknownFieldSet.Field.newBuilder().addVarint(10345).build();
            final byte[] newStoreInfo = RecordMetaDataProto.DataStoreInfo.parseFrom(
                            context.ensureActive().get(storeInfoKey).join())
                    .toBuilder()
                    .setStoreLockState(
                            RecordMetaDataProto.DataStoreInfo.StoreLockState.newBuilder()
                                    .setReason("Future locking")
                                    .setTimestamp(System.currentTimeMillis())
                                    .setUnknownFields(UnknownFieldSet.newBuilder()
                                            .addField(lockStateFieldNumber, unexpectedEnumValue).build()))
                    .build().toByteArray();
            context.ensureActive().set(storeInfoKey, newStoreInfo);
            commit(context);
        }

        // Attempt to open - should fail with UnknownStoreLockStateException
        try (FDBRecordContext context = openContext()) {
            assertThrows(UnknownStoreLockStateException.class, () -> {
                openSimpleRecordStore(context, null, formatVersion);
            });
        }
    }

    @Test
    void testFullStoreLockWithReason() {
        // Verify that the reason and timestamp are properly stored and accessible
        final String lockReason = "Store locked for maintenance";

        try (FDBRecordContext context = openContext()) {
            recordStore = openSimpleRecordStore(context, null, formatVersion);

            recordStore.setStoreLockStateAsync(
                    RecordMetaDataProto.DataStoreInfo.StoreLockState.State.FULL_STORE,
                    lockReason
            ).join();

            commit(context);
        }

        // Verify the lock state prevents opening and contains the reason
        try (FDBRecordContext context = openContext()) {
            StoreIsFullyLockedException exception = assertThrows(StoreIsFullyLockedException.class, () -> {
                openSimpleRecordStore(context, null, formatVersion);
            });
            // The exception should have logged the reason and timestamp
            assertNotNull(exception.getMessage());
            assertNotNull(exception.getLogInfo());
        }
    }

    @Test
    void testClearFullStoreLock() {
        final String lockReason = "Testing lock clearing";

        // Test setting FULL_STORE lock
        try (FDBRecordContext context = openContext()) {
            recordStore = openSimpleRecordStore(context, null, formatVersion);

            recordStore.setStoreLockStateAsync(
                    RecordMetaDataProto.DataStoreInfo.StoreLockState.State.FULL_STORE,
                    lockReason
            ).join();

            commit(context);
        }

        // Verify cannot open normally
        try (FDBRecordContext context = openContext()) {
            assertThrows(StoreIsFullyLockedException.class, () -> {
                openSimpleRecordStore(context, null, formatVersion);
            });
        }

        // Open with bypass to clear the lock
        try (FDBRecordContext context = openContext()) {
            recordStore = FDBRecordStore.newBuilder()
                    .setContext(context)
                    .setMetaDataProvider(simpleMetaData(null))
                    .setSubspace(path.toSubspace(context))
                    .setFormatVersion(formatVersion)
                    .setBypassFullStoreLockReason(lockReason)
                    .open();

            // Clear the lock
            recordStore.clearStoreLockStateAsync().join();

            commit(context);
        }

        // Verify can now open normally
        try (FDBRecordContext context = openContext()) {
            recordStore = openSimpleRecordStore(context, null, formatVersion);
            commit(context);
        }
    }
}
