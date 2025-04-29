/*
 * CursorLimitManager.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.ByteScanLimiter;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordScanLimiter;
import com.apple.foundationdb.record.ScanLimitReachedException;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TimeScanLimiter;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.cursors.IntersectionCursor;
import com.apple.foundationdb.record.provider.foundationdb.cursors.UnionCursor;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;

/**
 * Handles the logic of tracking various out-of-band limits on a {@link BaseCursor}.
 * In contrast to the individual cursors, which may react to out-of-band limits in various ways (including ignoring them
 * entirely), the limit manager merely tracks the limit state and provides a coherent answer to the questions:
 * 1) Can the cursor advance without exceeding the limit?
 * 2) What limit has been exceeded?
 *
 * This class also tracks whether or not the base cursor has produced any records. Except when reaching a limit throws
 * an exception, a base cursor is always permitted to load at least one record before it is stopped by an out-of-band
 * limit. This contract ensures that cursors with multiple child cursors (such as {@link UnionCursor}
 * and {@link IntersectionCursor}) can always make progress. This "free initial pass" is provided per cursor, not per
 * limit: all out-band-limits share the same initial pass.
 */
@API(API.Status.UNSTABLE)
public class CursorLimitManager {
    @Nullable
    private final RecordScanLimiter recordScanLimiter;
    private final boolean failOnScanLimitReached;
    private boolean haltedDueToRecordScanLimit = false;
    @Nullable
    private final ByteScanLimiter byteScanLimiter;
    private boolean haltedDueToByteScanLimit = false;
    @Nullable
    private final TimeScanLimiter timeScanLimiter;
    private boolean haltedDueToTimeLimit = false;

    private boolean usedInitialPass = false;

    private static final Optional<RecordCursor.NoNextReason> SCAN_LIMIT_REACHED = Optional.of(RecordCursor.NoNextReason.SCAN_LIMIT_REACHED);
    private static final Optional<RecordCursor.NoNextReason> BYTE_LIMIT_REACHED = Optional.of(RecordCursor.NoNextReason.BYTE_LIMIT_REACHED);
    private static final Optional<RecordCursor.NoNextReason> TIME_LIMIT_REACHED = Optional.of(RecordCursor.NoNextReason.TIME_LIMIT_REACHED);

    public static final CursorLimitManager UNTRACKED = new CursorLimitManager(null, false, null, null);

    @VisibleForTesting
    public CursorLimitManager(@Nullable RecordScanLimiter recordScanLimiter, boolean failOnScanLimitReached,
                              @Nullable ByteScanLimiter byteScanLimiter,
                              @Nullable TimeScanLimiter timeScanLimiter) {
        this.recordScanLimiter = recordScanLimiter;
        this.failOnScanLimitReached = failOnScanLimitReached;
        this.byteScanLimiter = byteScanLimiter;
        this.timeScanLimiter = timeScanLimiter;
    }

    public CursorLimitManager(@Nonnull ScanProperties scanProperties) {
        this(null, scanProperties);
    }

    public CursorLimitManager(@Nullable FDBRecordContext context, @Nonnull ScanProperties scanProperties) {
        this.recordScanLimiter = scanProperties.getExecuteProperties().getState().getRecordScanLimiter();
        this.byteScanLimiter = scanProperties.getExecuteProperties().getState().getByteScanLimiter();
        this.failOnScanLimitReached = scanProperties.getExecuteProperties().isFailOnScanLimitReached();
        if (scanProperties.getExecuteProperties().getTimeLimit() != ExecuteProperties.UNLIMITED_TIME) {
            this.timeScanLimiter = new TimeScanLimiter(context != null ? context.getTransactionCreateTime() : System.currentTimeMillis(),
                    scanProperties.getExecuteProperties().getTimeLimit());
        } else {
            this.timeScanLimiter = null;
        }
    }

    /**
     * Report whether any limit handled by this manager has been exceeded.
     * @return <code>true</code> if any limit has been exceeded and <code>false</code> otherwise
     */
    public boolean isStopped() {
        return getStoppedReason().isPresent();
    }

    /**
     * Report a single reason associated with a limit that has been exceeded. If more than one limit has been exceeded,
     * return the more important one. If no limit has been exceeded, return <code>Optional.emtpy()</code>.
     * @return a reason for a limit that has been exceeded or <code>Optional.empty()</code> if no limit has been exceeded
     */
    public Optional<RecordCursor.NoNextReason> getStoppedReason() {
        if (haltedDueToRecordScanLimit) {
            return SCAN_LIMIT_REACHED;
        } else if (haltedDueToByteScanLimit) {
            return BYTE_LIMIT_REACHED;
        } else if (haltedDueToTimeLimit) {
            return TIME_LIMIT_REACHED;
        }
        return Optional.empty();
    }

    /**
     * Inform the limit manager that a cursor is trying to scan a record. If no limit would be exceeded after scanning
     * another record, return <code>true</code> and update the internal state to reflect that record scan.
     * If a limit would be exceeded by scanning another record, return <code>false</code> and update the state
     * accordingly.
     *
     * @return <code>true</code> if another record scan would not exceed any limit and <code>false</code> if it would
     *
     * @throws ScanLimitReachedException if the scan limit was reached and {@link ExecuteProperties#isFailOnScanLimitReached()}
     */
    public boolean tryRecordScan() {
        haltedDueToRecordScanLimit = recordScanLimiter != null && !recordScanLimiter.tryRecordScan()
                                     && (usedInitialPass || failOnScanLimitReached);
        haltedDueToByteScanLimit = byteScanLimiter != null && !byteScanLimiter.hasBytesRemaining() && usedInitialPass;
        haltedDueToTimeLimit = timeScanLimiter != null && !timeScanLimiter.tryRecordScan() && usedInitialPass;
        final boolean halted = haltedDueToRecordScanLimit || haltedDueToByteScanLimit || haltedDueToTimeLimit;

        if (!halted) {
            usedInitialPass = true;
        } else if (failOnScanLimitReached) {
            throw new ScanLimitReachedException("limit on number of key-values scanned per transaction reached")
                    .addLogInfo("no_next_reason", getStoppedReason().map(Enum::toString).orElse("Unknown."));
        }

        return !halted;
    }

    /**
     * Record that the specified number of bytes have been scanned and update the relevant limiters with this information.
     * @param byteSize the number of bytes scanned
     */
    public void reportScannedBytes(long byteSize) {
        if (byteScanLimiter != null) {
            byteScanLimiter.registerScannedBytes(byteSize);
        }
    }
}
