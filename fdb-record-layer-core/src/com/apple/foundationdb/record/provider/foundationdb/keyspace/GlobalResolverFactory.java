/*
 * GlobalResolverFactory.java
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

package com.apple.foundationdb.record.provider.foundationdb.keyspace;

import com.apple.foundationdb.API;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.layers.interning.ScopedInterningLayer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;

/**
 * <code>GlobalResolverFactory</code> can be used to obtain a handle to the global {@link LocatableResolver} for an FDB cluster.
 * Clusters may be using the default <code>DirectoryLayer</code> from FDB which can be accessed through
 * {@link ScopedDirectoryLayer#global(FDBDatabase)}, or the cluster may have been migrated to the new
 * {@link ScopedInterningLayer} representation. This is determined by checking whether the default global directory
 * layer has been retired (see {@link LocatableResolver#retireLayer()}.
 */
@API(API.Status.EXPERIMENTAL)
public class GlobalResolverFactory {
    private GlobalResolverFactory() {
        throw new UnsupportedOperationException("object should never be instantiated");
    }

    /**
     * Get the global resolver for this FDB cluster. The migration state is state is cached for
     * {@value FDBDatabase#DEFAULT_RESOLVER_STATE_CACHE_REFRESH_SECONDS}.
     * @param timer The timer to use for emitting metrics.
     * @param database The database to target.
     * @return A future that when ready contains the global resolver for the FDB cluster.
     */
    public static CompletableFuture<LocatableResolver> globalResolver(@Nullable final FDBStoreTimer timer,
                                                                      @Nonnull final FDBDatabase database) {
        return ScopedDirectoryLayer
                .global(database)
                .retired(timer)
                .thenApply(isRetired ->
                        isRetired ?
                        ScopedInterningLayer.global(database) : ScopedDirectoryLayer.global(database));
    }

    public static CompletableFuture<LocatableResolver> globalResolver(@Nonnull final FDBDatabase database) {
        return globalResolver(null, database);
    }

    /**
     * Get the global resolver for this FDB cluster. This method bypasses the cache and instead always reads the resolver
     * state.
     * @param context The transaction context for reading the resolver state.
     * @return A future that when ready contains the global resolver for the FDB cluster.
     */
    public static CompletableFuture<LocatableResolver> globalResolverNoCache(@Nonnull final FDBRecordContext context) {
        final FDBDatabase database = context.getDatabase();
        return ScopedDirectoryLayer
                .global(database)
                .retiredSkipCache(context)
                .thenApply(isRetired ->
                        isRetired ?
                        ScopedInterningLayer.global(database) : ScopedDirectoryLayer.global(database));
    }

}
