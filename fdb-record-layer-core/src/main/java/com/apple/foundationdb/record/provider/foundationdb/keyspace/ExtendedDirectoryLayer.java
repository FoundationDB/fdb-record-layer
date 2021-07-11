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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
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
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * An implementation of {@link LocatableResolver} that uses a key format that is compatible with the {@link ScopedDirectoryLayer}
 * to keep track of the allocation of strings to integers. A <code>ExtendedDirectoryLayer</code> that shares a base subspace
 * ({@link LocatableResolver#getBaseSubspaceAsync()}) with a given {@link ScopedDirectoryLayer} will respect all mappings
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
    @Nonnull
    private CompletableFuture<Subspace> baseSubspaceFuture;
    @Nonnull
    private CompletableFuture<Subspace> nodeSubspaceFuture;
    @Nonnull
    private CompletableFuture<Subspace> stateSubspaceFuture;
    @Nonnull
    private final Subspace contentSubspace;

    /**
     * Create an extended directory layer.
     *
     * @param database database that will be used when resolving values
     * @param path the path at which the directory layer should store its mappings.
     */
    public ExtendedDirectoryLayer(@Nonnull FDBDatabase database, @Nonnull ResolvedKeySpacePath path) {
        this(database, path.toPath(), CompletableFuture.completedFuture(path));
    }

    private ExtendedDirectoryLayer(@Nonnull FDBDatabase database,
                                   @Nullable KeySpacePath path,
                                   @Nullable CompletableFuture<ResolvedKeySpacePath> resolvedPathFuture) {
        super(database, path, resolvedPathFuture);
        if (path == null && resolvedPathFuture == null) {
            this.isRootLevel = true;
            this.baseSubspaceFuture = CompletableFuture.completedFuture(DEFAULT_BASE_SUBSPACE);
            this.nodeSubspaceFuture = CompletableFuture.completedFuture(DEFAULT_NODE_SUBSPACE);
            this.contentSubspace = DEFAULT_CONTENT_SUBSPACE;
        } else {
            this.isRootLevel = false;
            this.baseSubspaceFuture = resolvedPathFuture.thenApply(ResolvedKeySpacePath::toSubspace);
            this.nodeSubspaceFuture = baseSubspaceFuture.thenApply(base ->
                    new Subspace(Bytes.concat(base.getKey(), DirectoryLayer.DEFAULT_NODE_SUBSPACE.getKey())));
            this.contentSubspace = new Subspace(RESERVED_CONTENT_SUBSPACE_PREFIX);
        }
        this.stateSubspaceFuture = nodeSubspaceFuture.thenApply(node -> node.get(STATE_SUBSPACE_KEY_SUFFIX));
    }

    /**
     * Creates a default instance of the extended directory layer.
     *
     * @param database The {@link FDBDatabase} for this resolver
     *
     * @return The global <code>ExtendedDirectoryLayer</code> for this database
     */
    public static ExtendedDirectoryLayer global(@Nonnull FDBDatabase database) {
        return new ExtendedDirectoryLayer(database, null, null);
    }

    @Override
    protected CompletableFuture<ResolverResult> create(@Nonnull FDBRecordContext context, @Nonnull String key, @Nullable byte[] metadata) {
        return context.instrument(FDBStoreTimer.Events.EXTENDED_DIRECTORY_LAYER_CREATE, createInternal(context, key, metadata));
    }

    private CompletableFuture<ResolverResult> createInternal(@Nonnull FDBRecordContext context, String key, @Nullable byte[] metadata) {
        FDBReverseDirectoryCache reverseCache = context.getDatabase().getReverseDirectoryCache();
        return getHca(context)
                .thenCompose(hca -> hca.allocate(key))
                .thenCompose(allocated -> {
                    ResolverResult result = new ResolverResult(allocated, metadata);
                    return getMappingSubspaceAsync().thenApply(mappingSubspace -> {
                        context.ensureActive().set(mappingSubspace.pack(key), serializeValue(result));
                        return result;
                    });
                })
                .thenCompose(result -> reverseCache.putIfNotExists(context, wrap(key), result.getValue()).thenApply(ignore -> result));
    }

    @Override
    protected CompletableFuture<Optional<ResolverResult>> read(@Nonnull FDBRecordContext context, String key) {
        return context.instrument(FDBStoreTimer.Events.EXTENDED_DIRECTORY_LAYER_READ, readInternal(context, key));
    }

    private CompletableFuture<Optional<ResolverResult>> readInternal(@Nonnull FDBRecordContext context, String key) {
        FDBReverseDirectoryCache reverseCache = context.getDatabase().getReverseDirectoryCache();
        return getMappingSubspaceAsync()
                .thenCompose(mappingSubspace -> context.ensureActive().get(mappingSubspace.pack(key)))
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
    protected CompletableFuture<Void> putReverse(@Nonnull final FDBRecordContext context, final long value, @Nonnull final String key) {
        return database.getReverseDirectoryCache().putOrReplaceForTesting(context, wrap(key), value);
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
                return getMappingSubspaceAsync().thenCombine(getHca(context), (mappingSubspace, hca) -> {
                    // write the forward mapping
                    context.ensureActive().set(mappingSubspace.pack(key), serializeValue(value));
                    // write the reverse mappings, here we need to add it to the allocation subspace and the reverse directory cache
                    hca.forceAllocate(key, value.getValue());
                    return null;
                }).thenCompose(vignore ->
                        context.getDatabase().getReverseDirectoryCache().putIfNotExists(context, wrap(key), value.getValue()));
            }
            return AsyncUtil.DONE;
        }).thenCompose(Function.identity());
    }

    private CompletableFuture<HighContentionAllocator> getHca(@Nonnull FDBRecordContext context) {
        return getCounterSubspaceAsync().thenCombine(getAllocationSubspaceAsync(), (counter, allocation) ->
                isRootLevel ?
                HighContentionAllocator.forRoot(context, counter, allocation) :
                new HighContentionAllocator(context, counter, allocation));
    }

    @Override
    public CompletableFuture<Void> updateMetadata(FDBRecordContext context, String key, byte[] metadata) {
        throw new UnsupportedOperationException("cannot update metadata in ExtendedDirectoryLayer");
    }

    @Override
    public CompletableFuture<Void> setWindow(long count) {
        return database.runAsync(context -> getHca(context).thenAccept(hca -> hca.setWindow(count)),
                Arrays.asList(
                        LogMessageKeys.TRANSACTION_NAME, "ExtendedDirectoryLayer::setWindow",
                        LogMessageKeys.RESOLVER, this));
    }

    @Override
    @Nonnull
    public CompletableFuture<Subspace> getMappingSubspaceAsync() {
        return nodeSubspaceFuture
                .thenApply(nodeSubspace -> nodeSubspace.get(nodeSubspace.getKey()).get(0));
    }

    private CompletableFuture<Subspace> getCounterSubspaceAsync() {
        return nodeSubspaceFuture.thenApply(nodeSubspace ->
                nodeSubspace.get(nodeSubspace.getKey()).get("hca".getBytes(Charset.forName("UTF-8"))).get(0));
    }

    private CompletableFuture<Subspace> getAllocationSubspaceAsync() {
        return nodeSubspaceFuture.thenApply(nodeSubspace ->
                nodeSubspace.get(nodeSubspace.getKey()).get("hca".getBytes(Charset.forName("UTF-8"))).get(1));
    }

    @Override
    protected CompletableFuture<Subspace> getStateSubspaceAsync() {
        return stateSubspaceFuture;
    }


    @Override
    @Nonnull
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
    @Nonnull
    public CompletableFuture<Subspace> getBaseSubspaceAsync() {
        return baseSubspaceFuture;
    }

}
