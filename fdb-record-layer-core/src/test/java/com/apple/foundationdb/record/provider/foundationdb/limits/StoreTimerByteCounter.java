/*
 * StoreTimerByteCounter.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.limits;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A utility class for counting the total number of bytes scanned (as defined by the
 * {@link com.apple.foundationdb.record.ByteScanLimiter}) during the execution of a
 * {@link com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan} by combining several counters from the
 * {@link com.apple.foundationdb.record.provider.common.StoreTimer}. This provides a means for verifying that the
 * number of bytes scanned is actually being limited by the {@code ByteScanLimiter}.
 */
public class StoreTimerByteCounter {
    private static final List<FDBStoreTimer.Counts> DEFAULT_COUNTS = ImmutableList.of(
            FDBStoreTimer.Counts.LOAD_RECORD_KEY_BYTES,
            FDBStoreTimer.Counts.LOAD_RECORD_VALUE_BYTES,
            FDBStoreTimer.Counts.LOAD_INDEX_KEY_BYTES,
            FDBStoreTimer.Counts.LOAD_INDEX_VALUE_BYTES);

    private final List<FDBStoreTimer.Counts> countsWithScannedBytes;

    public StoreTimerByteCounter() {
        this(DEFAULT_COUNTS);
    }

    public StoreTimerByteCounter(List<FDBStoreTimer.Counts> countsWithScannedBytes) {
        this.countsWithScannedBytes = countsWithScannedBytes;
    }

    public long getBytesScanned(@Nonnull final StoreTimer timer) {
        long total = 0L;
        for (FDBStoreTimer.Counts count : countsWithScannedBytes) {
            total += timer.getCount(count);
        }
        return total;
    }

    public long getBytesScanned(@Nonnull FDBRecordContext context) {
        final FDBStoreTimer timer = context.getTimer();
        if (timer == null) {
            throw new RecordCoreException("context must have a timer to test byte scan limit");
        }
        return getBytesScanned(timer);
    }
}
