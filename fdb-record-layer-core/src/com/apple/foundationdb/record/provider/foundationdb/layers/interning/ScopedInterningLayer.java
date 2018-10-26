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

import com.apple.foundationdb.API;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.LocatableResolver;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ResolverResult;
import com.apple.foundationdb.subspace.Subspace;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * An implementation of {@link LocatableResolver} that is backed by the {@link StringInterningLayer}.
 */
@API(API.Status.INTERNAL)
public class ScopedInterningLayer extends LocatableResolver {
    private static final byte[] GLOBAL_SCOPE_PREFIX_BYTES = new byte[]{(byte) 0xFC};
    private static final int STATE_SUBSPACE_KEY_SUFFIX = -10;

    @Nonnull
    private final StringInterningLayer interningLayer;
    @Nonnull
    private final Subspace baseSubspace;
    @Nonnull
    private final Subspace nodeSubspace;
    @Nonnull
    private final Subspace stateSubspace;
    @Nonnull
    private final String infoString;
    private final int hashCode;

    /**
     * Creates a resolver rooted at the provided <code>KeySpacePath</code>.
     * @param path The {@link KeySpacePath} where this resolver is rooted
     */
    public ScopedInterningLayer(@Nonnull KeySpacePath path) {
        this(path.getContext().getDatabase(), path);
    }

    private ScopedInterningLayer(@Nonnull FDBDatabase database,
                                 @Nullable KeySpacePath path) {
        super(database, path);
        if (path == null) {
            this.baseSubspace = new Subspace();
            this.nodeSubspace = new Subspace(GLOBAL_SCOPE_PREFIX_BYTES);
            this.interningLayer = new StringInterningLayer(nodeSubspace, true);
            this.infoString = "ScopedInterningLayer:GLOBAL";
        } else {
            this.baseSubspace = path.toSubspace();
            this.nodeSubspace = path.toSubspace();
            this.interningLayer = new StringInterningLayer(nodeSubspace, false);
            this.infoString = "ScopedInterningLayer:" + path.toString();
        }
        this.stateSubspace = nodeSubspace.get(STATE_SUBSPACE_KEY_SUFFIX);
        this.hashCode = Objects.hash(ScopedInterningLayer.class, baseSubspace, database);
    }

    /**
     * Creates a default instance of the scoped interning layer.
     * @param database The {@link FDBDatabase} for this resolver
     * @return The global <code>ScopedInterningLayer</code> for this database
     */
    public static ScopedInterningLayer global(@Nonnull FDBDatabase database) {
        return new ScopedInterningLayer(database, null);
    }

    @Override
    protected CompletableFuture<Optional<ResolverResult>> read(@Nonnull FDBRecordContext context, String key) {
        return context.instrument(FDBStoreTimer.Events.INTERNING_LAYER_READ, interningLayer.read(context, key));
    }

    @Override
    protected CompletableFuture<ResolverResult> create(@Nonnull FDBRecordContext context,
                                                       @Nonnull String key,
                                                       @Nullable byte[] metadata) {
        return context.instrument(FDBStoreTimer.Events.INTERNING_LAYER_CREATE, interningLayer.create(context, key, metadata));
    }

    @Override
    protected CompletableFuture<Optional<String>> readReverse(FDBStoreTimer timer, Long value) {
        FDBRecordContext context = database.openContext();
        context.setTimer(timer);
        return interningLayer.readReverse(context, value)
                .whenComplete((ignored, th) -> context.close());
    }

    @Override
    public CompletableFuture<Void> setMapping(FDBRecordContext context, String key, ResolverResult value) {
        return interningLayer.setMapping(context, key, value);
    }

    @Override
    public CompletableFuture<Void> updateMetadata(@Nonnull FDBRecordContext context, @Nonnull String key, @Nullable byte[] metadata) {
        return interningLayer.updateMetadata(context, key, metadata);
    }

    @Override
    public CompletableFuture<Void> setWindow(long count) {
        return database.runAsync(context -> interningLayer.setWindow(context, count));
    }

    @Override
    protected Subspace getStateSubspace() {
        return stateSubspace;
    }

    @Override
    @Nonnull
    public Subspace getMappingSubspace() {
        return interningLayer.getMappingSubspace();
    }

    @Override
    public Subspace getBaseSubspace() {
        return baseSubspace;
    }

    @Override
    public ResolverResult deserializeValue(byte[] value) {
        return interningLayer.deserializeValue(value);
    }

    @Override
    public String toString() {
        // pre-computed in constructor
        return infoString;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj instanceof ScopedInterningLayer) {
            ScopedInterningLayer that = (ScopedInterningLayer) obj;
            return baseSubspace.equals(that.baseSubspace) && this.database.equals(that.database);
        }
        return false;
    }

    @Override
    public int hashCode() {
        // pre-computed in constructor
        return hashCode;
    }
}
