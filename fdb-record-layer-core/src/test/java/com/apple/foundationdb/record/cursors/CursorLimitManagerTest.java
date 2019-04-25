/*
 * CursorLimitManagerTest.java
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

package com.apple.foundationdb.record.cursors;

import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordScanLimiter;
import com.apple.foundationdb.record.ScanLimitReachedException;
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.TimeScanLimiter;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@link CursorLimitManager}.
 */
public class CursorLimitManagerTest {
    static class FakeTimeLimiter extends TimeScanLimiter {
        public FakeTimeLimiter(long startTime, long timeLimitMillis) {
            super(startTime, timeLimitMillis);
            isTimedOut = false;
        }

        public FakeTimeLimiter() {
            super(0, 0);
            isTimedOut = false;
        }

        @Override
        public boolean tryRecordScan() {
            return !isTimedOut;
        }

        public void timeOut() {
            isTimedOut = true;
        }
    }

    @Test
    public void testRecordScanLimiter() {
        testRecordScanLimiterBase(false);
        testRecordScanLimiterBase(true);
    }

    public void testRecordScanLimiterBase(boolean failOnLimitReached) {
        final int numberOfScans = 12;
        final RecordScanLimiter recordScanLimiter = new RecordScanLimiter(numberOfScans);
        final CursorLimitManager manager = new CursorLimitManager(recordScanLimiter, failOnLimitReached, null, null);

        for (int i = 0; i < numberOfScans; i++) {
            assertTrue(manager.tryRecordScan());
            assertFalse(manager.isStopped());
            assertFalse(manager.getStoppedReason().isPresent());
        }

        if (failOnLimitReached) {
            try {
                TestHelpers.assertThrows("limit on number of key-values scanned per transaction reached", ScanLimitReachedException.class, manager::tryRecordScan, "no_next_reason", RecordCursor.NoNextReason.SCAN_LIMIT_REACHED.toString());
            } catch (Exception e) {
                fail("Unexpected ScanLimitReachedException.");
            }

        } else {
            assertFalse(manager.tryRecordScan());
            assertTrue(manager.isStopped());
            assertEquals(RecordCursor.NoNextReason.SCAN_LIMIT_REACHED, manager.getStoppedReason().get());
        }
    }

    @Test
    public void testTimeLimiter() {
        testTimeLimiterBase(false);
        testTimeLimiterBase(true);
    }

    public void testTimeLimiterBase(boolean failOnLimitReached) {
        final int untilTimeout = 7;
        final FakeTimeLimiter fakeTimeLimiter = new FakeTimeLimiter();
        final CursorLimitManager manager = new CursorLimitManager(null, failOnLimitReached, null, fakeTimeLimiter);

        for (int i = 0; i < untilTimeout; i++) {
            assertTrue(manager.tryRecordScan());
            assertFalse(manager.isStopped());
            assertFalse(manager.getStoppedReason().isPresent());
        }

        fakeTimeLimiter.timeOut();

        if (failOnLimitReached) {
            try {
                TestHelpers.assertThrows("limit on number of key-values scanned per transaction reached", ScanLimitReachedException.class, manager::tryRecordScan, "no_next_reason", RecordCursor.NoNextReason.TIME_LIMIT_REACHED.toString());
            } catch (Exception e) {
                fail("Unexpected ScanLimitReachedException.");
            }
        } else {
            assertFalse(manager.tryRecordScan());
            assertTrue(manager.isStopped());
            assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, manager.getStoppedReason().get());
        }
    }

    @Test
    public void testTimeoutBeforeScanLimit() {
        final int untilTimeout = 7;
        final int numberOfScans = 12;
        final RecordScanLimiter recordScanLimiter = new RecordScanLimiter(numberOfScans);
        final FakeTimeLimiter fakeTimeLimiter = new FakeTimeLimiter();
        final CursorLimitManager manager = new CursorLimitManager(recordScanLimiter, false, null, fakeTimeLimiter);

        for (int i = 0; i < untilTimeout; i++) {
            assertTrue(manager.tryRecordScan());
            assertFalse(manager.isStopped());
            assertFalse(manager.getStoppedReason().isPresent());
        }

        fakeTimeLimiter.timeOut();
        assertFalse(manager.tryRecordScan());
        assertTrue(manager.isStopped());
        assertEquals(RecordCursor.NoNextReason.TIME_LIMIT_REACHED, manager.getStoppedReason().get());
    }

    @Test
    public void testSimultaneousRecordScanLimitAndTimeout() {
        final int numberOfScans = 12;
        final RecordScanLimiter recordScanLimiter = new RecordScanLimiter(numberOfScans);
        final FakeTimeLimiter fakeTimeLimiter = new FakeTimeLimiter();
        final CursorLimitManager manager = new CursorLimitManager(recordScanLimiter, false, null, fakeTimeLimiter);

        for (int i = 0; i < numberOfScans; i++) {
            assertTrue(manager.tryRecordScan());
            assertFalse(manager.isStopped());
            assertFalse(manager.getStoppedReason().isPresent());
        }

        fakeTimeLimiter.timeOut();
        assertFalse(manager.tryRecordScan());
        assertTrue(manager.isStopped());
        // record scan limit takes precedence over time limit
        assertEquals(RecordCursor.NoNextReason.SCAN_LIMIT_REACHED, manager.getStoppedReason().get());
    }
}
