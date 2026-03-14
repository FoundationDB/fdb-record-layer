/*
 * RepairMissingHeaderOptions.java
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

import com.apple.foundationdb.annotation.API;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Options for {@link FDBRecordStore.Builder#repairMissingHeader(int, FormatVersion, RepairMissingHeaderOptions)}.
 */
@API(API.Status.EXPERIMENTAL)
public class RepairMissingHeaderOptions {

    public static final RepairMissingHeaderOptions DEFAULT = new RepairMissingHeaderOptions(false, null);

    private final boolean leaveIndexesReadable;
    @Nullable
    private final String lockReason;

    private RepairMissingHeaderOptions(final boolean leaveIndexesReadable, @Nullable final String lockReason) {
        this.leaveIndexesReadable = leaveIndexesReadable;
        this.lockReason = lockReason;
    }

    /**
     * When repairing the header, leave the indexes as readable even though they may be corrupt.
     * <p>
     *     As a safety measure, the store will be locked with
     *     {@link com.apple.foundationdb.record.RecordMetaDataProto.DataStoreInfo.StoreLockState.State#FULL_STORE}
     *     and the provided reason.  It is <em>your</em> responsibility to disable the indexes (and optionally rebuild
     *     them) before clearing the lock.
     * </p>
     * @param lockReason the reason for the repair/store lock
     * @return new options for the repair
     */
    public static RepairMissingHeaderOptions leavePotentiallyCorruptedIndexesReadable(@Nonnull final String lockReason) {
        return new RepairMissingHeaderOptions(true, lockReason);
    }

    @API(API.Status.INTERNAL)
    boolean isLeaveIndexesReadable() {
        return leaveIndexesReadable;
    }

    /**
     * Get the lock reason if appropriate.
     * @return the lock reason if appropriate.
     */
    @API(API.Status.INTERNAL)
    @Nonnull
    String getLockReason() {
        return Objects.requireNonNull(lockReason);
    }
}
