/*
 * ExtendedDirectoryLayer.java
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
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBReverseDirectoryCache;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.layers.interning.HighContentionAllocator;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.primitives.Bytes;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * An implementation of {@link LocatableResolver} that uses a key format that is compatible with the {@link ScopedDirectoryLayer}
 * to keep track of the allocation of strings to integers. A <code>ExtendedDirectoryLayer</code> that shares a base subspace
 * ({@link LocatableResolver#getBaseSubspace()}) with a given {@link ScopedDirectoryLayer} will respect all mappings
 * that were previously allocated and can even inter-operate with a {@link ScopedDirectoryLayer} allocating mappings in
 * parallel. The {@link KeySpacePath} that it is created with will be the root where the node subspace of the directory layer is located.
 */
@API(API.Status.MAINTAINED)
public class ExtendedDirectoryLayer extends LocatableResolver {
    private static final byte[] RESERVED_CONTENT_SUBSPACE_PREFIX = {(byte)0xFD};
    private static final int STATE_SUBSPACE_KEY_SUFFIX = -10;
    private static final Subspace DEFAULT_BASE_SUBSPACE = new Subspace();
    private static final Subspace DEFAULT_NODE_SUBSPACE = new Subspace(
            Bytes.concat(DEFAULT_BASE_SUBSPACE.getKey(), DirectoryLayer.DEFAULT_NODE_SUBSPACE.getKey()));
    private static final Subspace DEFAULT_CONTENT_SUBSPACE = DEFAULT_BASE_SUBSPACE;
    private final boolean isRootLevel;
    private final Subspace baseSubspace;
    private final Subspace nodeSubspace;
    private final Subspace contentSubspace;
    private final Subspace stateSubspace;
    private final String infoString;
    private final int hashCode;

    /**
     * Create an extended directory layer. This constructor utilizes blocking calls and should not
     * be used in an asynchronous context.
     *
     * @param path the path at which the directory layer should store its mappings
     * @deprecated Use {@link #ExtendedDirectoryLayer(FDBRecordContext, KeySpacePath)} instead
     */
    @Deprecated
    @API(API.Status.DEPRECATED)
    public ExtendedDirectoryLayer(@Nonnull KeySpacePath path) {
        this(path.getContext(), path);
    }

    /**
     * Create an extended directory layer. This constructor utilizes blocking calls and should not
     * be used in an asynchronous context.
     *
     * @param context the context that will be used to resolve the provided path into the subspace at which
     *   the directory layer will be created. This context is only used during the construction of the
     *   this class is not subsequently used
     * @param path the path at which the directory layer should store its mappings.
     */
    public ExtendedDirectoryLayer(@Nonnull FDBRecordContext context, @Nonnull KeySpacePath path) {
        this(context.getDatabase(), path, () -> path.toTuple(context));
    }

    private ExtendedDirectoryLayer(@Nonnull FDBDatabase database,
                                   @Nullable KeySpacePath path,
                                   @Nonnull Supplier<Tuple> pathTuple) {
        super(database, path);
        if (path == null) {
            this.isRootLevel = true;
            this.baseSubspace = DEFAULT_BASE_SUBSPACE;
            this.nodeSubspace = DEFAULT_NODE_SUBSPACE;
            this.contentSubspace = DEFAULT_CONTENT_SUBSPACE;
            this.infoString = "ExtendedDirectoryLayer:GLOBAL";
        } else {
            this.isRootLevel = false;
            this.baseSubspace = new Subspace(pathTuple.get());
            this.nodeSubspace = new Subspace(Bytes.concat(baseSubspace.getKey(), DirectoryLayer.DEFAULT_NODE_SUBSPACE.getKey()));
            this.contentSubspace = new Subspace(RESERVED_CONTENT_SUBSPACE_PREFIX);
            this.infoString = "ExtendedDirectoryLayer:" + path.toString();
        }
        this.stateSubspace = nodeSubspace.get(STATE_SUBSPACE_KEY_SUFFIX);
        this.hashCode = Objects.hash(ExtendedDirectoryLayer.class, baseSubspace, database);
    }

    /**
     * Creates a default instance of the extended directory layer.
     *
     * @param database The {@link FDBDatabase} for this resolver
     *
     * @return The global <code>ExtendedDirectoryLayer</code> for this database
     */
    public static ExtendedDirectoryLayer global(@Nonnull FDBDatabase database) {
        return new ExtendedDirectoryLayer(database, null, () -> null);
    }

    @Override
    protected CompletableFuture<ResolverResult> create(@Nonnull FDBRecordContext context, @Nonnull String key, @Nullable byte[] metadata) {
        return context.instrument(FDBStoreTimer.Events.EXTENDED_DIRECTORY_LAYER_CREATE, createInternal(context, key, metadata));
    }

    private CompletableFuture<ResolverResult> createInternal(@Nonnull FDBRecordContext context, String key, @Nullable byte[] metadata) {
        FDBReverseDirectoryCache reverseCache = context.getDatabase().getReverseDirectoryCache();
        HighContentionAllocator hca = getHca(context);
        return hca.allocate(key)
                .thenApply(allocated -> {
                    ResolverResult result = new ResolverResult(allocated, metadata);
                    context.ensureActive().set(getMappingSubspace().pack(key), serializeValue(result));
                    return result;
                })
                .thenCompose(result -> reverseCache.putIfNotExists(context, wrap(key), result.getValue()).thenApply(ignore -> result));
    }

    @Override
    protected CompletableFuture<Optional<ResolverResult>> read(@Nonnull FDBRecordContext context, String key) {
        return context.instrument(FDBStoreTimer.Events.EXTENDED_DIRECTORY_LAYER_READ, readInternal(context, key));
    }

    private CompletableFuture<Optional<ResolverResult>> readInternal(@Nonnull FDBRecordContext context, String key) {
        FDBReverseDirectoryCache reverseCache = context.getDatabase().getReverseDirectoryCache();
        return context.ensureActive().get(getMappingSubspace().pack(key))
                .thenCompose(bytes -> {
                    if (bytes == null) {
                        return CompletableFuture.completedFuture(Optional.empty());
                    }
                    ResolverResult result = deserializeValue(bytes);
                    return reverseCache.putIfNotExists(context, wrap(key), result.getValue())
                            .thenApply(ignore -> Optional.of(result));
                });
    }

    @Override
    protected CompletableFuture<Optional<String>> readReverse(FDBStoreTimer timer, Long value) {
        FDBReverseDirectoryCache reverseCache = database.getReverseDirectoryCache();
        return reverseCache.get(timer, wrap(value));
    }

    private CompletableFuture<Optional<String>> readInReverseCacheSubpspace(FDBStoreTimer timer, Long value) {
        return database.getReverseDirectoryCache().getInReverseDirectoryCacheSubspace(timer, wrap(value));
    }

    @Override
    public CompletableFuture<Void> setMapping(FDBRecordContext context, String key, ResolverResult value) {
        return read(context, key).thenCombine(readInReverseCacheSubpspace(context.getTimer(), value.getValue()), (maybeRead, maybeReverseRead) -> {
            maybeRead.ifPresent(read -> {
                if (!read.equals(value)) {
                    throw new RecordCoreException("mapping already exists with different value")
                            .addLogInfo("keyToSet", key)
                            .addLogInfo("valueToSet", value)
                            .addLogInfo("valueFound", read);
                }
            });
            maybeReverseRead.ifPresent(reverseRead -> {
                if (!reverseRead.equals(key)) {
                    throw new RecordCoreException("reverse mapping already exists with different key")
                            .addLogInfo("keyToSet", key)
                            .addLogInfo("valueToSet", value)
                            .addLogInfo("keyForValueFound", reverseRead);
                }
            });

            if (!maybeRead.isPresent() && !maybeReverseRead.isPresent()) {
                // write the forward mapping
                context.ensureActive().set(getMappingSubspace().pack(key), serializeValue(value));
                // write the reverse mappings, here we need to add it to the allocation subspace and the reverse directory cache
                getHca(context).forceAllocate(key, value.getValue());
                return context.getDatabase().getReverseDirectoryCache().putIfNotExists(context, wrap(key), value.getValue());
            }
            return AsyncUtil.DONE;
        }).thenCompose(Function.identity());
    }

    private HighContentionAllocator getHca(@Nonnull FDBRecordContext context) {
        return isRootLevel ?
               HighContentionAllocator.forRoot(context, getCounterSubspace(), getAllocationSubspace()) :
               new HighContentionAllocator(context, getCounterSubspace(), getAllocationSubspace());
    }

    @Override
    public CompletableFuture<Void> updateMetadata(FDBRecordContext context, String key, byte[] metadata) {
        throw new UnsupportedOperationException("cannot update metadata in ExtendedDirectoryLayer");
    }

    @Override
    public CompletableFuture<Void> setWindow(long count) {
        return database.runAsync(context -> {
            getHca(context).setWindow(count);
            return AsyncUtil.DONE;
        });
    }

    @Override
    @Nonnull
    public Subspace getMappingSubspace() {
        return nodeSubspace.get(nodeSubspace.getKey()).get(0);
    }

    private Subspace getCounterSubspace() {
        return nodeSubspace.get(nodeSubspace.getKey()).get("hca".getBytes(Charset.forName("UTF-8"))).get(0);
    }

    private Subspace getAllocationSubspace() {
        return nodeSubspace.get(nodeSubspace.getKey()).get("hca".getBytes(Charset.forName("UTF-8"))).get(1);
    }

    @Override
    protected Subspace getStateSubspace() {
        return stateSubspace;
    }


    @Override
    public ResolverResult deserializeValue(byte[] value) {
        Tuple unpacked = contentSubspace.unpack(value);
        return unpacked.size() == 1 ?
               new ResolverResult(unpacked.getLong(0), null) :
               new ResolverResult(unpacked.getLong(0), unpacked.getBytes(1));

    }

    private byte[] serializeValue(@Nonnull ResolverResult value) {
        return value.getMetadata() == null ?
               contentSubspace.pack(Tuple.from(value.getValue())) :
               contentSubspace.pack(Tuple.from(value.getValue(), value.getMetadata()));
    }

    @Override
    public Subspace getBaseSubspace() {
        return baseSubspace;
    }

    @Override
    public String toString() {
        // pre-computed in constructor
        return infoString;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj != null && obj instanceof ExtendedDirectoryLayer) {
            ExtendedDirectoryLayer that = (ExtendedDirectoryLayer)obj;
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
