/*
 * StoreIsLockedForRecordUpdates.java
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.logging.LogMessageKeys;

import javax.annotation.Nonnull;

/**
 * Error being thrown when attempting to update a record while the store is locked for record updates.
 */
@API(API.Status.EXPERIMENTAL)
public class StoreIsLockedForRecordUpdates extends RecordCoreStorageException {
    private static final long serialVersionUID = -640771754012134421L;

    public StoreIsLockedForRecordUpdates(@Nonnull final RecordStoreState recordStoreState) {
        super("Record Store is locked for record updates",
                LogMessageKeys.SPECIAL_STORE_STATE_LOG_STAMP, recordStoreState.getStoreHeader().getSpecialStoreState().getLogStamp(),
                LogMessageKeys.SPECIAL_STORE_STATE_TIME_STAMP_MILLIS, recordStoreState.getStoreHeader().getSpecialStoreState().getTimeStamp());
    }
}
