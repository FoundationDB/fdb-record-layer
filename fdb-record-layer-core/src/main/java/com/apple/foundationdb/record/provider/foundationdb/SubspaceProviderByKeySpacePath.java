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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A SubspaceProvider wrapping a key space path. Getting the subspace from this provider might be blocking if it is
 * never retrieved before.
 */
@API(API.Status.INTERNAL)
public class SubspaceProviderByKeySpacePath implements SubspaceProvider {
    @Nonnull
    private final KeySpacePath keySpacePath;

    @Nonnull
    private final ConcurrentHashMap<Optional<String>, Subspace> databases;

    SubspaceProviderByKeySpacePath(@Nonnull KeySpacePath keySpacePath) {
        this.keySpacePath = keySpacePath;
        databases = new ConcurrentHashMap<>();
    }

    @Nonnull
    @Override
    public Subspace getSubspace(@Nonnull FDBRecordContext context) {
        return context.asyncToSync(FDBStoreTimer.Waits.WAIT_KEYSPACE_PATH_RESOLVE, getSubspaceAsync(context));
    }

    @Nonnull
    @Override
    public CompletableFuture<Subspace> getSubspaceAsync(@Nonnull FDBRecordContext context) {
        CompletableFuture<Subspace> subspaceFuture;
        String clusterFile = context.getDatabase().getClusterFile();
        Optional<String> key = Optional.ofNullable(clusterFile);
        Subspace subspace = databases.get(key);
        if (subspace == null) {
            subspaceFuture = keySpacePath.toSubspaceAsync(context).whenComplete((s, e) -> {
                if (e == null) {
                    this.databases.put(key, s);
                }
            });
        } else {
            subspaceFuture = CompletableFuture.completedFuture(subspace);
        }
        return subspaceFuture;
    }

    @Nonnull
    @Override
    public LogMessageKeys logKey() {
        return LogMessageKeys.KEY_SPACE_PATH;
    }

    @Override
    public String toString(@Nonnull FDBRecordContext context) {
        Optional<String> key = Optional.ofNullable(context.getDatabase().getClusterFile());
        Subspace subspace = databases.get(key);
        if (subspace != null) {
            return keySpacePath.toString(Tuple.fromBytes(subspace.pack()));
        } else {
            return toString();
        }
    }

    @Override
    public String toString() {
        return keySpacePath.toString();
    }

    @Override
    public int hashCode() {
        return keySpacePath.hashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (other == null || !this.getClass().equals(other.getClass())) {
            return false;
        }
        SubspaceProviderByKeySpacePath that = (SubspaceProviderByKeySpacePath) other;
        return keySpacePath.equals(that.keySpacePath);
    }
}
