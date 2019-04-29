/*
 * SubspaceProviderBySubspace.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil2;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

/**
 * A SubspaceProvider wrapping a subspace. Getting the subspace from this provider is not blocking.
 */
@API(API.Status.INTERNAL)
public class SubspaceProviderBySubspace implements SubspaceProvider {
    @Nonnull
    private Subspace subspace;
    private int memoizedHashCode = 0;

    @API(API.Status.INTERNAL)
    public SubspaceProviderBySubspace(@Nonnull Subspace subspace) {
        this.subspace = subspace;
    }

    @Nonnull
    @Override
    public Subspace getSubspace(@Nonnull FDBRecordContext context) {
        return subspace;
    }

    @Nonnull
    @Override
    public CompletableFuture<Subspace> getSubspaceAsync(@Nonnull FDBRecordContext context) {
        return CompletableFuture.completedFuture(subspace);
    }

    @Nonnull
    @Override
    public LogMessageKeys logKey() {
        return LogMessageKeys.SUBSPACE;
    }

    @Override
    public String toString(@Nonnull FDBRecordContext context) {
        return toString();
    }

    @Override
    public String toString() {
        return ByteArrayUtil2.loggable(subspace.pack());
    }

    @Override
    public int hashCode() {
        if (memoizedHashCode == 0) {
            memoizedHashCode = subspace.hashCode();
        }
        return memoizedHashCode;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (other == null || !this.getClass().equals(other.getClass())) {
            return false;
        }
        SubspaceProviderBySubspace that = (SubspaceProviderBySubspace) other;
        return subspace.equals(that.subspace);
    }
}
