/*
 * ScopedInterningLayer.java
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

package com.apple.foundationdb.record.provider.foundationdb.layers.interning;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.LocatableResolver;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolvedKeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolverResult;
import com.apple.foundationdb.subspace.Subspace;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * An implementation of {@link LocatableResolver} that is backed by the {@link StringInterningLayer}.
 */
@API(API.Status.MAINTAINED)
public class ScopedInterningLayer extends LocatableResolver {
    private static final byte[] GLOBAL_SCOPE_PREFIX_BYTES = new byte[]{(byte) 0xFC};
    private static final int STATE_SUBSPACE_KEY_SUFFIX = -10;

    @Nonnull
    private CompletableFuture<Subspace> baseSubspaceFuture;
    @Nonnull
    private CompletableFuture<Subspace> nodeSubspaceFuture;
    @Nonnull
    private CompletableFuture<Subspace> stateSubspaceFuture;
    @Nonnull
    private CompletableFuture<StringInterningLayer> interningLayerFuture;

    /**
     * Creates a resolver rooted at the provided <code>KeySpacePath</code>.
     * @param context context used to resolve the provided <code>path</code>. This context
     *   is only used during the construction of this class and at no other point
     * @param path the {@link KeySpacePath} where this resolver is rooted
     * @deprecated use {@link #ScopedInterningLayer(FDBDatabase, ResolvedKeySpacePath)} instead
     */
    @Deprecated
    @API(API.Status.DEPRECATED)
    public ScopedInterningLayer(@Nonnull FDBRecordContext context, @Nonnull KeySpacePath path) {
        this(context.getDatabase(), path, path.toResolvedPathAsync(context));
    }

    /**
     * Creates a resolver rooted at the provided <code>KeySpacePath</code>.
     * @param database database that will be used when resolving values
     * @param path the {@link ResolvedKeySpacePath} where this resolver is rooted
     */
    public ScopedInterningLayer(@Nonnull FDBDatabase database, @Nonnull ResolvedKeySpacePath path) {
        this(database, path.toPath(), CompletableFuture.completedFuture(path));
    }

    private ScopedInterningLayer(@Nonnull FDBDatabase database,
                                 @Nullable KeySpacePath path,
                                 @Nullable CompletableFuture<ResolvedKeySpacePath> resolvedPath) {
        super(database, path, resolvedPath);
        boolean isRootLevel;
        if (path == null && resolvedPath == null) {
            isRootLevel = true;
            this.baseSubspaceFuture = CompletableFuture.completedFuture(new Subspace());
            this.nodeSubspaceFuture = CompletableFuture.completedFuture(new Subspace(GLOBAL_SCOPE_PREFIX_BYTES));
        } else {
            isRootLevel = false;
            this.baseSubspaceFuture = resolvedPath.thenApply(ResolvedKeySpacePath::toSubspace);
            this.nodeSubspaceFuture = baseSubspaceFuture;
        }
        this.stateSubspaceFuture = nodeSubspaceFuture.thenApply(node -> node.get(STATE_SUBSPACE_KEY_SUFFIX));
        this.interningLayerFuture = nodeSubspaceFuture.thenApply(node -> new StringInterningLayer(node, isRootLevel));
    }

    /**
     * Creates a default instance of the scoped interning layer.
     * @param database the {@link FDBDatabase} for this resolver
     * @return the global <code>ScopedInterningLayer</code> for this database
     */
    public static ScopedInterningLayer global(@Nonnull FDBDatabase database) {
        return new ScopedInterningLayer(database, null, null);
    }

    @Override
    protected CompletableFuture<Optional<ResolverResult>> read(@Nonnull FDBRecordContext context, String key) {
        return context.instrument(FDBStoreTimer.Events.INTERNING_LAYER_READ,
                interningLayerFuture.thenCompose(layer -> layer.read(context, key)));
    }

    @Override
    protected CompletableFuture<ResolverResult> create(@Nonnull FDBRecordContext context,
                                                       @Nonnull String key,
                                                       @Nullable byte[] metadata) {
        return context.instrument(FDBStoreTimer.Events.INTERNING_LAYER_CREATE,
                interningLayerFuture.thenCompose(layer -> layer.create(context, key, metadata)));
    }

    @Override
    protected CompletableFuture<Optional<String>> readReverse(FDBStoreTimer timer, Long value) {
        FDBRecordContext context = database.openContext();
        context.setTimer(timer);
        return interningLayerFuture
                .thenCompose(layer -> layer.readReverse(context, value))
                .whenComplete((ignored, th) -> context.close());
    }

    @Override
    protected CompletableFuture<Void> putReverse(@Nonnull final FDBRecordContext context, final long value, @Nonnull final String key) {
        return interningLayerFuture
                .thenApply(layer -> {
                    layer.putReverse(context, value, key);
                    return null;
                });
    }

    @Override
    public CompletableFuture<Void> setMapping(FDBRecordContext context, String key, ResolverResult value) {
        return interningLayerFuture
                .thenCompose(layer -> layer.setMapping(context, key, value));
    }

    @Override
    public CompletableFuture<Void> updateMetadata(@Nonnull FDBRecordContext context, @Nonnull String key, @Nullable byte[] metadata) {
        return interningLayerFuture
                .thenCompose(layer -> layer.updateMetadata(context, key, metadata));
    }

    @Override
    public CompletableFuture<Void> setWindow(long count) {
        return database.runAsync(context -> interningLayerFuture.thenCompose(layer -> layer.setWindow(context, count)),
                Arrays.asList(
                        LogMessageKeys.TRANSACTION_NAME, "ScopedInterningLayer::setWindow",
                        LogMessageKeys.RESOLVER, this));
    }

    @Override
    protected CompletableFuture<Subspace> getStateSubspaceAsync() {
        return stateSubspaceFuture;
    }

    @Override
    @Nonnull
    public CompletableFuture<Subspace> getMappingSubspaceAsync() {
        return interningLayerFuture.thenApply(StringInterningLayer::getMappingSubspace);
    }

    @Override
    @Nonnull
    public CompletableFuture<Subspace> getBaseSubspaceAsync() {
        return baseSubspaceFuture;
    }

    @Override
    @Nonnull
    public ResolverResult deserializeValue(byte[] value) {
        return StringInterningLayer.deserializeValue(value);
    }

}
