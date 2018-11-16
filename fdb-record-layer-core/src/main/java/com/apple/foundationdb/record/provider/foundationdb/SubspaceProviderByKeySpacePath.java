/*
 * SubspaceProviderByKeySpacePath.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.API;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.subspace.Subspace;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

/**
 * A SubspaceProvider wrapping a key space path. Getting the subspace from this provider might be blocking if it is
 * never retrieved before.
 */
@API(API.Status.MAINTAINED)
public class SubspaceProviderByKeySpacePath implements SubspaceProvider {
    @Nonnull
    private final KeySpacePath keySpacePath;

    @Nonnull
    private final FDBRecordContext context;

    @Nullable
    private CompletableFuture<Subspace> subspaceFuture;

    SubspaceProviderByKeySpacePath(@Nonnull KeySpacePath keySpacePath, @Nonnull FDBRecordContext context) {
        this.keySpacePath = keySpacePath;
        this.context = context;
    }

    @Nonnull
    @Override
    public Subspace getSubspace() {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_KEYSPACE_PATH_RESOLVE, getSubspaceAsync());
    }

    @Nonnull
    @Override
    public CompletableFuture<Subspace> getSubspaceAsync() {
        if (subspaceFuture == null) {
            subspaceFuture = keySpacePath.toSubspaceAsync(context);
        }
        return subspaceFuture;
    }

    @Nonnull
    @Override
    public LogMessageKeys logKey() {
        return LogMessageKeys.KEY_SPACE_PATH;
    }

    @Override
    public String toString() {
        return keySpacePath.toString();
    }
}
