/*
 * StoreIsFullyLockedException.java
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

package com.apple.foundationdb.record;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.SubspaceProvider;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Error thrown when attempting to open a store that is in the {@link RecordMetaDataProto.DataStoreInfo.StoreLockState.State#FULL_STORE}
 * lock state. This lock state prevents the store from being opened unless explicitly overridden, providing a strong
 * guarantee that no operations will be performed on the store while it is locked.
 */
@API(API.Status.EXPERIMENTAL)
public class StoreIsFullyLockedException extends RecordCoreStorageException {
    private static final long serialVersionUID = -2341887654789012345L;

    public StoreIsFullyLockedException(@Nonnull final RecordMetaDataProto.DataStoreInfo.StoreLockState storeLockState,
                                       @Nullable LogMessageKeys subspaceLogKey,
                                       @Nullable SubspaceProvider subspaceProvider) {
        super("Record Store is fully locked and cannot be opened",
                LogMessageKeys.STORE_LOCK_STATE_REASON, storeLockState.getReason(),
                LogMessageKeys.STORE_LOCK_STATE_TIMESTAMP_MILLIS, storeLockState.getTimestamp());
        if (subspaceLogKey != null) {
            addLogInfo(subspaceLogKey, subspaceProvider);
        }
    }
}
