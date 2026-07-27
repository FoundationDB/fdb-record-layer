/*
 * DeleteStoreMode.java
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

import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.subspace.Subspace;

import javax.annotation.Nonnull;

/**
 * Enum for parameterizing tests over the two variants of {@link FDBRecordStore#deleteStore}:
 * the (deprecated) synchronous {@code deleteStore} and the newer asynchronous
 * {@code deleteStoreAsync}. Callers dispatch via {@link #deleteStore(FDBRecordContext, KeySpacePath)}
 * or {@link #deleteStore(FDBRecordContext, Subspace)}; in the ASYNC case the returned future is
 * joined so callers observe the same synchronous shape regardless of variant.
 */
public enum DeleteStoreMode {
    SYNC {
        @Override
        public void deleteStore(@Nonnull FDBRecordContext context, @Nonnull KeySpacePath path) {
            FDBRecordStore.deleteStore(context, path);
        }

        @Override
        public void deleteStore(@Nonnull FDBRecordContext context, @Nonnull Subspace subspace) {
            FDBRecordStore.deleteStore(context, subspace);
        }
    },
    ASYNC {
        @Override
        public void deleteStore(@Nonnull FDBRecordContext context, @Nonnull KeySpacePath path) {
            context.asyncToSync(FDBStoreTimer.Waits.WAIT_DELETE_STORE, FDBRecordStore.deleteStoreAsync(context, path));
        }

        @Override
        public void deleteStore(@Nonnull FDBRecordContext context, @Nonnull Subspace subspace) {
            context.asyncToSync(FDBStoreTimer.Waits.WAIT_DELETE_STORE,
                    FDBRecordStore.deleteStoreAsync(context, subspace));
        }
    };

    public abstract void deleteStore(@Nonnull FDBRecordContext context, @Nonnull KeySpacePath path);

    public abstract void deleteStore(@Nonnull FDBRecordContext context, @Nonnull Subspace subspace);
}
