/*
 * ExecutePropertiesTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.Transaction;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link ExecuteProperties}.
 */
public class ExecutePropertiesTest {
    private static int ROW_LIMIT = 100;
    private static long TIME_LIMIT = 1000L;
    private static int RECORD_SCAN_LIMIT = 200;

    @Test
    public void testSetDryRun() {
        final ExecuteProperties base = ExecuteProperties.newBuilder()
                .setIsolationLevel(IsolationLevel.SNAPSHOT) // not the default
                .setReturnedRowLimit(ROW_LIMIT)
                .setTimeLimit(TIME_LIMIT)
                .setScannedRecordsLimit(RECORD_SCAN_LIMIT)
                .build();
        // default dryRun = false
        assertFalse(base.isDryRun());
        // set dryRun = true
        ExecuteProperties p1 = base.setDryRun(true);
        assertTrue(p1.isDryRun());
    }

    @Test
    public void testSetLimitsFrom() {
        final ExecuteProperties base = ExecuteProperties.newBuilder()
                .setIsolationLevel(IsolationLevel.SNAPSHOT) // not the default
                .setReturnedRowLimit(ROW_LIMIT)
                .setTimeLimit(TIME_LIMIT)
                .setScannedRecordsLimit(RECORD_SCAN_LIMIT)
                .build();

        final ExecuteProperties merge1 = base.setLimitsFrom(ExecuteProperties.newBuilder()
                .setReturnedRowLimit(ROW_LIMIT + 1)
                .build());
        assertEquals(ROW_LIMIT + 1, merge1.getReturnedRowLimit());
        assertEquals(TIME_LIMIT, merge1.getTimeLimit());
        assertEquals(base.getState(), merge1.getState());

        final ExecuteProperties merge2 = base.setLimitsFrom(ExecuteProperties.newBuilder()
                .setTimeLimit(TIME_LIMIT + 1)
                .build());
        assertEquals(ROW_LIMIT, merge2.getReturnedRowLimit());
        assertEquals(TIME_LIMIT + 1, merge2.getTimeLimit());
        assertEquals(base.getState(), merge2.getState());

        final ExecuteProperties merge3 = base.setLimitsFrom(ExecuteProperties.newBuilder()
                .setScannedRecordsLimit(RECORD_SCAN_LIMIT + 1)
                .build());
        assertEquals(ROW_LIMIT, merge3.getReturnedRowLimit());
        assertEquals(TIME_LIMIT, merge3.getTimeLimit());
        assertNotEquals(base.getState(), merge3.getState());
        for (int i = 0; i < RECORD_SCAN_LIMIT + 1; i++) {
            // verify that the record scan limit really is RECORD_SCAN_LIMIT + 1
            assertTrue(merge3.getState().getRecordScanLimiter().tryRecordScan());
        }
        // verify that the record scan limit really is RECORD_SCAN_LIMIT + 1
        assertFalse(merge3.getState().getRecordScanLimiter().tryRecordScan());
    }

    /**
     * Validate the values returned by an {@link ExecuteProperties} object when no limit is imposed.
     * These are helpfully a mix of 0 and the maximum value for their return type.
     */
    @Test
    public void testGetNoLimits() {
        assertEquals(ExecuteProperties.UNLIMITED_TIME, ExecuteProperties.SERIAL_EXECUTE.getTimeLimit());
        assertEquals(Integer.MAX_VALUE, ExecuteProperties.SERIAL_EXECUTE.getScannedRecordsLimit());
        assertFalse(ExecuteProperties.SERIAL_EXECUTE.getState().getRecordScanLimiter().isEnforcing());
        assertEquals(Long.MAX_VALUE, ExecuteProperties.SERIAL_EXECUTE.getScannedBytesLimit());
        assertFalse(ExecuteProperties.SERIAL_EXECUTE.getState().getByteScanLimiter().isEnforcing());
        assertEquals(Transaction.ROW_LIMIT_UNLIMITED, ExecuteProperties.SERIAL_EXECUTE.getReturnedRowLimit());
        assertEquals(Integer.MAX_VALUE, ExecuteProperties.SERIAL_EXECUTE.getReturnedRowLimitOrMax());

        // Ensure that these these constant ExecuteProperties do not do any scan tracking (this would be
        // confusing to do as they may be shared across many requests).
        assertTrue(ExecuteProperties.SERIAL_EXECUTE.getState().getRecordScanLimiter().tryRecordScan());
        assertEquals(0, ExecuteProperties.SERIAL_EXECUTE.getState().getRecordScanLimiter().getRecordsScanned());
        ExecuteProperties.SERIAL_EXECUTE.getState().getByteScanLimiter().registerScannedBytes(100L);
        assertEquals(0, ExecuteProperties.SERIAL_EXECUTE.getState().getByteScanLimiter().getBytesScanned());
    }

    /**
     * Validate that getting the limits set on an {@link ExecuteProperties} can then be retrieved back.
     */
    @Test
    public void testGetLimits() {
        ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                .setTimeLimit(100L)
                .setScannedBytesLimit(1000L)
                .setScannedRecordsLimit(2)
                .setReturnedRowLimit(200)
                .build();
        assertEquals(100L, executeProperties.getTimeLimit());
        assertEquals(1000L, executeProperties.getScannedBytesLimit());
        assertEquals(2, executeProperties.getScannedRecordsLimit());
        assertEquals(200, executeProperties.getReturnedRowLimit());

        final RecordScanLimiter recordScanLimiter = executeProperties.getState().getRecordScanLimiter();
        assertTrue(recordScanLimiter.isEnforcing());
        assertTrue(recordScanLimiter.tryRecordScan());
        assertTrue(recordScanLimiter.tryRecordScan());
        assertEquals(2, recordScanLimiter.getRecordsScanned());
        assertFalse(recordScanLimiter.tryRecordScan());

        final ByteScanLimiter byteScanLimiter = executeProperties.getState().getByteScanLimiter();
        assertTrue(byteScanLimiter.isEnforcing());
        byteScanLimiter.registerScannedBytes(500L);
        assertTrue(byteScanLimiter.hasBytesRemaining());
        byteScanLimiter.registerScannedBytes(499L);
        assertTrue(byteScanLimiter.hasBytesRemaining());
        assertEquals(999, byteScanLimiter.getBytesScanned());
        byteScanLimiter.registerScannedBytes(37L);
        assertFalse(byteScanLimiter.hasBytesRemaining());
    }

    @Test
    public void testTrackingNotEnforcing() {
        ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                .setReturnedRowLimit(200)
                .build();

        final RecordScanLimiter recordScanLimiter = executeProperties.getState().getRecordScanLimiter();
        assertFalse(recordScanLimiter.isEnforcing());
        assertTrue(recordScanLimiter.tryRecordScan());
        assertTrue(recordScanLimiter.tryRecordScan());
        assertEquals(2, recordScanLimiter.getRecordsScanned());

        final ByteScanLimiter byteScanLimiter = executeProperties.getState().getByteScanLimiter();
        assertFalse(byteScanLimiter.isEnforcing());
        byteScanLimiter.registerScannedBytes(200_000L);
        byteScanLimiter.registerScannedBytes(200_000L);
        assertTrue(byteScanLimiter.hasBytesRemaining());
        assertEquals(400_000L, byteScanLimiter.getBytesScanned());
    }
}
