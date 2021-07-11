/*
 * LocatableResolver.java
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
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ResolverStateProto;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.cursors.LazyCursor;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseRunner;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.KeyValueCursor;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * <code>LocatableResolver</code> represents the concept of a locatable bi-directional mapping between Strings and Longs
 * that is rooted at some location in the FDB key space. Resolvers that are located at different positions will allocate
 * String to Long mappings independently. Subclasses of <code>LocatableResolver</code> are responsible for implementing
 * {@link #create(FDBRecordContext, String)}, {@link #read(FDBRecordContext, String)}
 * and {@link #readReverse(FDBStoreTimer, Long)} operations. See {@link ScopedDirectoryLayer}
 * for an implementation that leverages the FDB directory layer.
 *
 * When initializing, a <code>defaultWriteSafetyCheck</code> can be specified that will be evaluated on writes to determine
 * within the write transaction what the correct {@link LocatableResolver} scope is for the write.
 */
@API(API.Status.MAINTAINED)
public abstract class LocatableResolver {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocatableResolver.class);
    @Nonnull
    protected final FDBDatabase database;
    // NOTE: Once the deprecated code has been removed this should be switched to a ResolvedKeySpacePath
    @Nonnull
    protected final ResolverLocation location;
    protected final int hashCode;

    /**
     * Created a locatable resolver.
     *
     * <p>This constructor may seem a little strange in taking two paths that are, effectively, the same. The
     * encouraged behavior for your resolvers is to create them from a {@link ResolvedKeySpacePath}, however for
     * backward compatibility we still allow resolvers to be created from a {@link FDBRecordContext} and a {@link KeySpacePath}.
     * All of the implementations of <code>LocatableResolver</code> are built such that if they are starting
     * from a <code>KeySpacePath</code>, they will then turn it into a resolved path, then use that to get the subspace,
     * but if they are created from a <code>ResolvedKeySpacePath</code>, they will instead directly get the subspace.
     * This constructor is handed both the path and the potentially completed resolved form of that path, and when
     * logging the path, if the resolved path is completed, will log that since it provides more detail.
     *
     * @param database the database to use for resolution
     * @param path the path at which the resolver has its data located
     * @param resolvedPath the resolved form of the path
     */
    protected LocatableResolver(@Nonnull FDBDatabase database,
                                @Nullable KeySpacePath path,
                                @Nullable CompletableFuture<ResolvedKeySpacePath> resolvedPath) {
        if ((path == null && resolvedPath != null)
                || (resolvedPath == null && path != null)) {
            throw new IllegalArgumentException("Path and resolved path must both be null or neither be null");
        }
        this.database = database;
        this.location = new ResolverLocation(path, resolvedPath);
        this.hashCode = Objects.hash(getClass(), path, database);
    }

    public <T> ScopedValue<T> wrap(T value) {
        return new ScopedValue<>(value, this);
    }

    @Nonnull
    public FDBDatabase getDatabase() {
        return database;
    }

    @Nonnull
    private <T> CompletableFuture<T> runAsync(@Nullable FDBStoreTimer timer,
                                              @Nonnull Function<FDBRecordContext, CompletableFuture<T>> retriable,
                                              Object...additionalLogMessageKeyValues) {
        return database.runAsync(timer, null, context ->
                        // Explicitly get a read version for instrumentation purposes
                        context.getReadVersionAsync().thenCompose(ignore -> retriable.apply(context)),
                Arrays.asList(additionalLogMessageKeyValues));
    }

    @Nonnull
    private <T> CompletableFuture<T> runAsyncBorrowingReadVersion(@Nonnull FDBRecordContext parentContext,
                                                                  @Nonnull Function<FDBRecordContext, CompletableFuture<T>> retriable,
                                                                  Object...additionalLogMessageKeyValues) {
        final FDBDatabaseRunner runner = parentContext.newRunner();
        boolean started = false;
        try {
            final AtomicBoolean first = new AtomicBoolean(true);
            CompletableFuture<T> future = runner.runAsync(childContext -> {
                if (parentContext.isClosed()) {
                    // If the parent context is closed, stop this runner, too
                    throw new FDBDatabaseRunner.RunnerClosed();
                }
                final CompletableFuture<Long> readVersionFuture;
                if (first.get()) {
                    // The first time around, borrow a read version
                    first.set(false);
                    readVersionFuture = parentContext.getReadVersionAsync().thenApply(readVersion -> {
                        childContext.setReadVersion(readVersion);
                        return readVersion;
                    });
                } else {
                    // Other times, get a new read version from the database (explicit here for instrumentation purposes)
                    readVersionFuture = childContext.getReadVersionAsync();
                }
                return readVersionFuture.thenCompose(ignore -> retriable.apply(childContext));
            }, Arrays.asList(additionalLogMessageKeyValues));
            started = true;
            return future.whenComplete((valIgnore, errIgnore) -> runner.close());
        } finally {
            if (!started) {
                runner.close();
            }
        }
    }

    /**
     * Map the String <code>name</code> to a Long within the scope of the path that this object was constructed with.
     * Will return the value that's persisted in FDB or create it if it does not exist.
     *
     * <p>
     * For an instrumented version of this method, see {@link #resolve(FDBStoreTimer, String)}.
     * </p>
     *
     * @param name the value to resolve
     * @return a future for the resolved Long value
     * @see #resolve(FDBStoreTimer, String)
     */
    @Nonnull
    public CompletableFuture<Long> resolve(@Nonnull String name) {
        return resolve((FDBStoreTimer)null, name, ResolverCreateHooks.getDefault());
    }

    /**
     * Map the String <code>name</code> to a Long within the scope of the path that this object was constructed with.
     * Will return the value that's persisted in FDB or create it if it does not exist.
     *
     * @param timer the {@link FDBStoreTimer} used for collecting metrics
     * @param name the value to resolve
     * @return a future for the resolved Long value
     */
    @Nonnull
    public CompletableFuture<Long> resolve(@Nullable FDBStoreTimer timer, @Nonnull String name) {
        return resolve(timer, name, ResolverCreateHooks.getDefault());
    }

    /**
     * Map the String <code>name</code> to a Long within the scope of the path that this object was constructed with.
     * Will return the value that's persisted in FDB or create it if it does not exist. This method may create and commit
     * a separate record context for the lookup, but it will reuse the same
     * {@linkplain FDBRecordContext#getReadVersion() read version} as the provided transaction, so it should not
     * incur all of the overhead of starting a new transaction.
     *
     * <p>
     * This method is {@link API.Status#UNSTABLE} for the same reasons as {@link #resolveWithMetadata(FDBRecordContext, String, ResolverCreateHooks)}.
     * </p>
     *
     * @param context the base context used to
     * @param name the value to resolve
     * @return a future for the resolved Long value
     */
    @API(API.Status.UNSTABLE)
    @Nonnull
    public CompletableFuture<Long> resolve(@Nonnull FDBRecordContext context, @Nonnull String name) {
        return resolve(context, name, ResolverCreateHooks.getDefault());
    }

    /**
     * Map the String <code>name</code> to a Long within the scope of the path that this object was constructed with.
     * Will return the value that's persisted in FDB or create it if it does not exist.
     *
     * <p>
     * For an instrumented version of this method, see {@link #resolve(FDBStoreTimer, String, ResolverCreateHooks)}
     * </p>
     *
     * @param name the value to resolve
     * @param hooks {@link ResolverCreateHooks} to run on create
     * @return a future for the resolved Long value
     * @see #resolve(FDBStoreTimer, String, ResolverCreateHooks)
     */
    @Nonnull
    public CompletableFuture<Long> resolve(@Nonnull String name,
                                           @Nonnull ResolverCreateHooks hooks) {
        return resolve((FDBStoreTimer)null, name, hooks);
    }

    /**
     * Map the String <code>name</code> to a Long within the scope of the path that this object was constructed with.
     * Will return the value that's persisted in FDB or create it if it does not exist.
     *
     * @param timer the {@link FDBStoreTimer} used for collecting metrics
     * @param name the value to resolve
     * @param hooks {@link ResolverCreateHooks} to run on create
     * @return a future for the resolved Long value
     */
    @Nonnull
    public CompletableFuture<Long> resolve(@Nullable FDBStoreTimer timer,
                                           @Nonnull String name,
                                           @Nonnull ResolverCreateHooks hooks) {
        return resolveWithMetadata(timer, name, hooks)
                .thenApply(ResolverResult::getValue);
    }

    /**
     * Map the String <code>name</code> to a Long within the scope of the path that this object was constructed with.
     * Will return the value that's persisted in FDB or create it if it does not exist. This method may create and
     * commit a separate record context for the lookup, but it will reuse the same
     * {@linkplain FDBRecordContext#getReadVersion() read version} as the provided transaction, so it should not
     * incur all of the overhead of starting a new transaction.
     *
     * <p>
     * This method is {@link API.Status#UNSTABLE} for the same reasons as {@link #resolveWithMetadata(FDBRecordContext, String, ResolverCreateHooks)}.
     * </p>
     *
     * @param context the {@link FDBRecordContext} used to base possible child transactions on
     * @param name the value to resolve
     * @param hooks {@link ResolverCreateHooks} to run on create
     * @return a future for the resolved Long value
     */
    @API(API.Status.UNSTABLE)
    @Nonnull
    public CompletableFuture<Long> resolve(@Nonnull FDBRecordContext context,
                                           @Nonnull String name,
                                           @Nonnull ResolverCreateHooks hooks) {
        return resolveWithMetadata(context, name, hooks)
                .thenApply(ResolverResult::getValue);
    }

    /**
     * Map the String <code>name</code> to a {@link ResolverResult} within the scope of the path that this object was
     * constructed with. If we are creating the entry for <code>name</code>, The {@link ResolverCreateHooks} provided
     * as <code>hooks</code> will be run. Any metadata that was already present or created can be seen by calling
     * {@link ResolverResult#getMetadata()} on the returned result.
     * Will return the value that's persisted in FDB or create it if it does not exist.
     * 
     * <p>
     * For an instrumented version of this method, see {@link #resolveWithMetadata(FDBStoreTimer, String, ResolverCreateHooks)}.
     * </p>
     *
     * @param name the value to resolve
     * @param hooks {@link ResolverCreateHooks} to run on create
     * @return a future for the {@link ResolverResult} containing the resolved value and metadata
     * @see #resolveWithMetadata(FDBStoreTimer, String, ResolverCreateHooks)
     */
    @Nonnull
    public CompletableFuture<ResolverResult> resolveWithMetadata(@Nonnull String name,
                                                                 @Nonnull ResolverCreateHooks hooks) {
        return resolveWithMetadata((FDBStoreTimer)null, name, hooks);
    }

    /**
     * Map the String <code>name</code> to a {@link ResolverResult} within the scope of the path that this object was
     * constructed with. If we are creating the entry for <code>name</code>, The {@link ResolverCreateHooks} provided
     * as <code>hooks</code> will be run. Any metadata that was already present or created can be seen by calling
     * {@link ResolverResult#getMetadata()} on the returned result.
     * Will return the value that's persisted in FDB or create it if it does not exist.
     *
     * @param timer the {@link FDBStoreTimer} used for collecting metrics
     * @param name the value to resolve
     * @param hooks {@link ResolverCreateHooks} to run on create
     * @return a future for the {@link ResolverResult} containing the resolved value and metadata
     */
    @Nonnull
    public CompletableFuture<ResolverResult> resolveWithMetadata(@Nullable FDBStoreTimer timer,
                                                                 @Nonnull String name,
                                                                 @Nonnull ResolverCreateHooks hooks) {
        final FDBRecordContext context = database.openContext(null, timer);
        boolean started = false;
        try {
            CompletableFuture<ResolverResult> future = resolveWithMetadata(context, name, hooks);
            started = true;
            return future.whenComplete((valIgnore, errIgnore) -> context.close());
        } finally {
            if (!started) {
                context.close();
            }
        }
    }

    /**
     * Map the String <code>name</code> to a {@link ResolverResult} within the scope of the path that this object was
     * constructed with. If we are creating the entry for <code>name</code>, The {@link ResolverCreateHooks} provided
     * as <code>hooks</code> will be run. Any metadata that was already present or created can be seen by calling
     * {@link ResolverResult#getMetadata()} on the returned result.
     * Will return the value that's persisted in FDB or create it if it does not exist.
     * This may create and commit separate transactions from the given context, but it will reuse the same
     * {@linkplain FDBRecordContext#getReadVersion() read version} as the given context, so it should not incur
     * all of the overhead of starting a new transaction.
     *
     * <p>
     * This method is currently {@link API.Status#UNSTABLE}. The reason is that the thinking on the exact
     * semantics of this method is currently in flux, and this method may be changed in the future to
     * use the same transaction as the one provided. This is a more straightforward API, but it complicates
     * the way that values from this function are cached, as it must be careful not to cache uncommitted results.
     * </p>
     *
     * @param context the {@link FDBRecordContext} used to base possible child transactions on
     * @param name the value to resolve
     * @param hooks {@link ResolverCreateHooks} to run on create
     * @return a future for the {@link ResolverResult} containing the resolved value and metadata
     */
    @API(API.Status.UNSTABLE)
    @Nonnull
    public CompletableFuture<ResolverResult> resolveWithMetadata(@Nonnull FDBRecordContext context, @Nonnull String name, @Nonnull ResolverCreateHooks hooks) {
        if (!context.getDatabase().equals(database)) {
            throw new RecordCoreArgumentException("attempted to resolve value against incorrect database");
        }

        // check the version stored in the resolver state and compare it with what version the cache was created at
        // if we read a version that is ahead of whats stored in FDBDatabase we need to invalidate the cache in FDBDatabase
        // if the cache version in FDBDatabase is a future version we can trust the cache we get from getDirectoryCache
        return getVersion(context)
                .thenApply(database::getDirectoryCache)
                .thenCompose(directoryCache ->
                        resolveWithCache(context, wrap(name), directoryCache, hooks));
    }

    /**
     * Lookup the mapping and metadata for <code>name</code> within the scope of the path that this object was constructed with.
     * Unlike {@link #resolveWithMetadata(FDBRecordContext, String, ResolverCreateHooks)} this method will not attempt to
     * create the mapping if none exists.
     *
     * @param context the transaction to use to access the database
     * @param name the value to resolve
     * @return a future for the {@link ResolverResult}
     * @throws NoSuchElementException if the value does not exist
     */
    @Nonnull
    public CompletableFuture<ResolverResult> mustResolveWithMetadata(@Nonnull FDBRecordContext context, @Nonnull String name) {
        return read(context, name)
                .thenApply(maybeRead ->
                        maybeRead.orElseThrow(() -> new NoSuchElementException(wrap(name).toString())));
    }

    /**
     * Lookup the mapping for <code>name</code> within the scope of the path that this object was constructed with.
     * Unlike {@link #resolve(FDBStoreTimer, String)} this method will not attempt to create the mapping if none exists.
     *
     * @param context the transaction to use to access the database
     * @param name the value to resolve
     * @return a future for the resolved Long value
     * @throws NoSuchElementException if the value does not exist
     */
    public CompletableFuture<Long> mustResolve(@Nonnull FDBRecordContext context, @Nonnull String name) {
        return read(context, name)
                .thenApply(maybeRead ->
                        maybeRead.map(ResolverResult::getValue)
                                .orElseThrow(() -> new NoSuchElementException(wrap(name).toString())));
    }

    /**
     * Lookup the String that maps to the provided value within the scope of the path that this object was constructed with.
     *
     * @param timer the {@link FDBStoreTimer} used for collecting metrics
     * @param value the value of the mapping to lookup
     * @return a future for the name that maps to this value
     * @throws NoSuchElementException if the value is not found
     */
    @Nonnull
    public CompletableFuture<String> reverseLookup(@Nullable FDBStoreTimer timer, @Nonnull Long value) {
        Cache<ScopedValue<Long>, String> inMemoryReverseCache = database.getReverseDirectoryInMemoryCache();
        String cachedValue = inMemoryReverseCache.getIfPresent(wrap(value));
        if (cachedValue != null) {
            return CompletableFuture.completedFuture(cachedValue);
        }

        return readReverse(timer, value)
                .thenApply(maybeRead ->
                        maybeRead.map(read -> {
                            inMemoryReverseCache.put(wrap(value), read);
                            return read;
                        }).orElseThrow(() -> new NoSuchElementException("reverse lookup of " + wrap(value))));
    }

    private CompletableFuture<ResolverResult> resolveWithCache(@Nonnull FDBRecordContext context,
                                                               @Nonnull ScopedValue<String> scopedName,
                                                               @Nonnull Cache<ScopedValue<String>, ResolverResult> directoryCache,
                                                               @Nonnull ResolverCreateHooks hooks) {
        ResolverResult value = directoryCache.getIfPresent(scopedName);
        if (value != null) {
            return CompletableFuture.completedFuture(value);
        }

        return context.instrument(
                FDBStoreTimer.Events.DIRECTORY_READ,
                runAsyncBorrowingReadVersion(context, childContext -> readOrCreateValue(childContext, scopedName.getData(), hooks),
                        LogMessageKeys.TRANSACTION_NAME, "LocatableResolver::readOrCreateValue",
                        LogMessageKeys.RESOLVER, this,
                        LogMessageKeys.RESOLVER_KEY, scopedName.getData())
        ).thenApply(fetched -> {
            directoryCache.put(scopedName, fetched);
            return fetched;
        });
    }

    private CompletableFuture<ResolverResult> readOrCreateValue(@Nonnull FDBRecordContext context,
                                                                @Nonnull String name,
                                                                @Nonnull ResolverCreateHooks hooks) {
        return read(context, name)
                .thenCompose(maybeRead -> maybeRead.map(CompletableFuture::completedFuture)
                        .orElseGet(() -> createIfNotLocked(context, name, hooks)));
    }

    private CompletableFuture<ResolverResult> createIfNotLocked(@Nonnull FDBRecordContext context,
                                                                @Nonnull String key,
                                                                @Nonnull final ResolverCreateHooks hooks) {
        List<CompletableFuture<Boolean>> checks = hooks.getPreWriteChecks().stream()
                .map(hook -> hook.apply(context, this))
                .collect(Collectors.toList());

        final byte[] metadata = hooks.getMetadataHook().apply(key);

        return AsyncUtil.getAll(checks)
                .thenCompose(checkValues -> {
                    if (checkValues.contains(false)) {
                        throw new LocatableResolverLockedException("prewrite check failed")
                                .addLogInfo(LogMessageKeys.RESOLVER_PATH, location)
                                .addLogInfo(LogMessageKeys.RESOLVER_KEY, key);
                    }
                    return loadResolverState(context);
                })
                .thenCombine(getResolverState(context), (readState, cachedState) -> {
                    if (!readState.equals(cachedState)) {
                        LOGGER.warn(KeyValueLogMessage.of("cached state and read stated differ",
                                        LogMessageKeys.RESOLVER_KEY, key,
                                        LogMessageKeys.RESOLVER_PATH, location,
                                        LogMessageKeys.CACHED_STATE, cachedState,
                                        LogMessageKeys.READ_STATE, readState));
                    }
                    return readState;
                })
                .thenCompose(state -> {
                    if (state.getLock() != ResolverStateProto.WriteLock.UNLOCKED) {
                        throw new LocatableResolverLockedException("locatable resolver is not writable")
                                .addLogInfo(LogMessageKeys.RESOLVER_KEY, key)
                                .addLogInfo(LogMessageKeys.RESOLVER_PATH, location)
                                .addLogInfo("lockState", state.getLock());
                    }
                    return create(context, key, metadata);
                });
    }

    @Nonnull
    private CompletableFuture<ResolverStateProto.State> loadResolverState(@Nonnull FDBRecordContext context) {
        // don't use a snapshot read, the lock state shouldn't change frequently but if it does we should
        // fail the transaction and should retry getting the value.
        return context.instrument(FDBStoreTimer.DetailEvents.RESOLVER_STATE_READ,
                getStateSubspaceAsync().thenCompose(stateSubspace ->
                        context.ensureActive().get(stateSubspace.pack())
                        .thenApply(LocatableResolver::deserializeResolverState)));
    }

    @Nonnull
    private CompletableFuture<ResolverStateProto.State> getResolverState(@Nullable FDBStoreTimer timer) {
        return database.getStateForResolver(this, () -> runAsync(timer, this::loadResolverState,
                LogMessageKeys.TRANSACTION_NAME, "LocatableResolver::loadResolverState",
                LogMessageKeys.RESOLVER, this,
                LogMessageKeys.SHARED_READ_VERSION, false));
    }

    @Nonnull
    private CompletableFuture<ResolverStateProto.State> getResolverState(@Nonnull FDBRecordContext context) {
        // Note that this doesn't re-use the same transaction, though it does borrow the read version to avoid another get read version request
        return database.getStateForResolver(this, () -> runAsyncBorrowingReadVersion(context, this::loadResolverState,
                LogMessageKeys.TRANSACTION_NAME, "LocatableResolver::loadResolverState",
                LogMessageKeys.RESOLVER, this,
                LogMessageKeys.SHARED_READ_VERSION, true));
    }

    /**
     * Checks the lock state for the resolver and if it is unlocked puts this {@link LocatableResolver} into a write
     * locked state which will prevent any new entries from being created. If the resolver is not unlocked this will
     * throw a {@link LocatableResolverLockedException}.
     * The lock state is cached for {@value FDBDatabase#DEFAULT_RESOLVER_STATE_CACHE_REFRESH_SECONDS} seconds. When changing the lock state,
     * transactions started at least {@value FDBDatabase#DEFAULT_RESOLVER_STATE_CACHE_REFRESH_SECONDS} seconds after this method succeeds should
     * see the updated state.
     * @return a future that completes when the write lock has been set
     */
    @Nonnull
    public CompletableFuture<Void> exclusiveLock() {
        return updateAndCommitResolverState(StateMutation.EXCLUSIVE_LOCK);
    }

    /**
     * Puts this {@link LocatableResolver} into a write locked state which will prevent any new entries from being created.
     * Does not perform any checks on the current lock state.
     * The lock state is cached for {@value FDBDatabase#DEFAULT_RESOLVER_STATE_CACHE_REFRESH_SECONDS} seconds. When changing the lock state,
     * transactions started at least {@value FDBDatabase#DEFAULT_RESOLVER_STATE_CACHE_REFRESH_SECONDS} seconds after this method succeeds should
     * see the updated state.
     * @return a future that completes when the write lock has been set
     */
    @Nonnull
    public CompletableFuture<Void> enableWriteLock() {
        return updateAndCommitResolverState(StateMutation.LOCK);
    }

    /**
     * Unlocks this {@link LocatableResolver}, so that calls to {@link #resolve(FDBStoreTimer, String)} will create
     * entries if they do not exist. The lock state is cached for {@value FDBDatabase#DEFAULT_RESOLVER_STATE_CACHE_REFRESH_SECONDS} seconds. When
     * changing the lock state, transactions started at least {@value FDBDatabase#DEFAULT_RESOLVER_STATE_CACHE_REFRESH_SECONDS} seconds after this
     * method succeeds should see the updated state.
     * @return a future that completes when the write lock has been cleared
     */
    @Nonnull
    public CompletableFuture<Void> disableWriteLock() {
        return updateAndCommitResolverState(StateMutation.UNLOCK);
    }

    /**
     * Retire this {@link LocatableResolver}, indicating that it should not be used for any future resolve operations.
     * This can be used to indicate that the current resolver has migrated to a new location and that clients should be
     * using that resolver instead.
     * @return a future that completes when the write lock has been cleared
     */
    @Nonnull
    public CompletableFuture<Void> retireLayer() {
        return updateAndCommitResolverState(StateMutation.RETIRE);
    }

    /**
     * Increments the version of the data stored in this directory layer. The last read state is cached for
     * {@value FDBDatabase#DEFAULT_RESOLVER_STATE_CACHE_REFRESH_SECONDS}, meaning it will take up to that long for a successful <code>incrementVersion</code>
     * to be seen by {@link #getVersion(FDBStoreTimer)}.
     * @return A future that completes when the version has been incremented
     */
    @Nonnull
    public CompletableFuture<Void> incrementVersion() {
        return updateAndCommitResolverState(StateMutation.INCREMENT_VERSION);
    }

    /**
     * Gets the version as stored in the state key for this {@link LocatableResolver}. This is used in conjunction with
     * the version of the forward directory cache (see {@link FDBDatabase#getDirectoryCacheVersion()}) to coordinate major changes
     * to data in the {@link LocatableResolver} that require any future reads to directly consult FDB rather than relying
     * on the directory cache. On calls to {@link LocatableResolver#resolve(FDBStoreTimer, String)}
     * the cache version from the {@link FDBDatabase} object is compared with the version stored in the resolver state. If the
     * version in the resolver state is ahead of the version in the database object, the cache associated with that database is
     * invalidated. Note: if the version known by the resolver is <i>behind</i> the version for the cache (e.g. our cached version
     * of the state hasn't been refreshed recently enough to see some change) we don't need to do anything special, we can trust
     * that values from a future version of the cache are backwards compatible to our current version.
     * @param timer The store timer to instrument the transaction with.
     * @return A future that will complete with the value of the current version
     */
    @Nonnull
    public CompletableFuture<Integer> getVersion(@Nullable FDBStoreTimer timer) {
        return runAsync(timer, this::getVersion,
                LogMessageKeys.TRANSACTION_NAME, "LocatableResolver::getVersion",
                LogMessageKeys.RESOLVER, this);
    }

    @Nonnull
    private CompletableFuture<Integer> getVersion(@Nonnull FDBRecordContext context) {
        return getResolverState(context).thenApply(ResolverStateProto.State::getVersion);
    }

    /**
     * Check whether this resolver has been retired. A retired state indicates that this resolver was once active but has
     * since been migrated to a new keyspace.
     * @param timer The store timer to instrument the transaction with.
     * @return A future that will complete with boolean indicating whether this resolver has been retired.
     */
    @Nonnull
    public CompletableFuture<Boolean> retired(@Nullable FDBStoreTimer timer) {
        return getResolverState(timer).thenApply(state -> state.getLock() == ResolverStateProto.WriteLock.RETIRED);
    }

    /**
     * Check whether this resolver has been retired. Skips the resolver state cache and reads the state directly. A
     * retired state indicates that this resolver was once active but has since been migrated to a new keyspace.
     * @param context the transaction to use to access the database
     * @return A future that will complete with boolean indicating whether this resolver has been retired.
     */
    @Nonnull
    public CompletableFuture<Boolean> retiredSkipCache(@Nonnull FDBRecordContext context) {
        return loadResolverState(context).thenApply(state -> state.getLock() == ResolverStateProto.WriteLock.RETIRED);
    }

    /**
     * Transactionally update the metadata for the provided <code>key</code> and (within the same transaction) increment
     * the version of the resolver state (see {@link #incrementVersion()}. An entry <code>key</code> must
     * already exist in the directory, if it does not a {@link NoSuchElementException} will be thrown. If you want to add
     * metadata to a key you are creating see the <code>createHook</code> parameter in
     * {@link #resolveWithMetadata(FDBStoreTimer, String, ResolverCreateHooks)}.
     * @param key the key in the directory to modify
     * @param metadata the new metadata
     * @return a future that will finish when the update and increment operations are complete
     */
    @Nonnull
    public CompletableFuture<Void> updateMetadataAndVersion(@Nonnull final String key,
                                                            @Nullable final byte[] metadata) {
        return runAsync(null, context -> updateMetadata(context, key, metadata)
                .thenCompose(ignore -> updateResolverState(context, StateMutation.INCREMENT_VERSION)),
                LogMessageKeys.TRANSACTION_NAME, "LocatableResolver::updateMetadataAndVersion",
                LogMessageKeys.RESOLVER, this,
                LogMessageKeys.RESOLVER_KEY, key);
    }

    private CompletableFuture<Void> updateAndCommitResolverState(@Nonnull final StateMutation mutation) {
        return runAsync(null, context -> updateResolverState(context, mutation),
                LogMessageKeys.TRANSACTION_NAME, "LocatableResolver::updateAndCommitResolverState",
                LogMessageKeys.RESOLVER, this,
                LogMessageKeys.MUTATION, mutation);
    }

    private CompletableFuture<Void> updateResolverState(@Nonnull final FDBRecordContext context, @Nonnull final StateMutation mutation) {
        return getStateSubspaceAsync()
                .thenApply(Subspace::getKey)
                .thenCompose(stateKey ->
                        context.ensureActive().get(stateKey)
                                .thenApply(LocatableResolver::deserializeResolverState)
                                .thenApply(state -> mutation.apply(state).toByteArray())
                                .thenApply(bytes -> {
                                    context.ensureActive().set(stateKey, bytes);
                                    return null;
                                })
                );
    }

    /**
     * Scan the mapping space used by the resolver, returning all of the key/value mappings that are stored there.
     *
     * @param context the transactional context
     * @param continuation a continuation to continue a previous scan
     * @param scanProperties how the scan is to be performed
     * @return a cursor returning key/value pairs of resolver mappings
     */
    public RecordCursor<ResolverKeyValue> scan(@Nonnull FDBRecordContext context,
                                               @Nullable byte[] continuation,
                                               @Nonnull ScanProperties scanProperties) {
        return new LazyCursor<>(
                getMappingSubspaceAsync().thenApply(mappingSubspace ->
                        KeyValueCursor.Builder.withSubspace(mappingSubspace)
                                .setScanProperties(scanProperties)
                                .setContext(context)
                                .setContinuation(continuation)
                                .build()
                                .map(keyValue ->  new ResolverKeyValue(
                                        mappingSubspace.unpack(keyValue.getKey()).getString(0),
                                        deserializeValue(keyValue.getValue())))),
                context.getExecutor());
    }


    protected abstract CompletableFuture<Optional<ResolverResult>> read(@Nonnull FDBRecordContext context, String key);

    protected abstract CompletableFuture<ResolverResult> create(@Nonnull FDBRecordContext context,
                                                                @Nonnull String key,
                                                                @Nullable byte[] metadata);

    protected final CompletableFuture<ResolverResult> create(@Nonnull FDBRecordContext context,
                                                       @Nonnull String key) {
        return create(context, key, null);
    }

    protected CompletableFuture<Optional<String>> readReverse(@Nonnull FDBRecordContext parentContext, Long value) {
        return readReverse(parentContext.getTimer(), value);
    }

    protected abstract CompletableFuture<Optional<String>> readReverse(FDBStoreTimer timer, Long value);

    protected abstract CompletableFuture<Void> updateMetadata(FDBRecordContext context, String key, byte[] metadata);

    protected abstract CompletableFuture<Void> setMapping(FDBRecordContext context, String key, ResolverResult value);

    @VisibleForTesting
    public final CompletableFuture<Void> setMapping(FDBRecordContext context, String key, Long value) {
        return setMapping(context, key, new ResolverResult(value));
    }

    @VisibleForTesting
    public abstract CompletableFuture<Void> setWindow(long count);

    protected abstract CompletableFuture<Subspace> getStateSubspaceAsync();

    /**
     * Get a {@link CompletableFuture} that will contain the {@link Subspace} where this resolver stores
     * the mappings from <code>key</code> {@link String}s to <code>value</code> {@link Long}. Direct access
     * to this subspace is not needed by general users and extreme care should be taken when interacting with it.
     * @return a future that, when ready, will hold the mapping subspace
     */
    @Nonnull
    public abstract CompletableFuture<Subspace> getMappingSubspaceAsync();

    /**
     * Get a {@link CompletableFuture} that will contain the {@link Subspace} this resolver is rooted
     * at (e.g. the global resolver {@link ExtendedDirectoryLayer#global(FDBDatabase)} has a base
     * subspace at the root of the FDB keyspace. Note that this is not the subspace where the resolver
     * maintains its allocation keys (see {@link #getMappingSubspaceAsync()}).
     * @return a future that, when ready, will hold the base subspace
     */
    @Nonnull
    public abstract CompletableFuture<Subspace> getBaseSubspaceAsync();

    /**
     * Deserialize the raw bytes value stored in the mapping subspace.
     * @param value raw value bytes.
     * @return the deserialized {@link ResolverResult}.
     */
    @Nonnull
    public abstract ResolverResult deserializeValue(byte[] value);

    /**
     * Method used for corrupting the reverse directory in order to write tests for validating/repairing
     * inconsistencies. Note that this method is protected, so the test must be in the same package as
     * as the {@code LocatableResolver} itself in order to be invoked. Note that this method does NOT
     * remove the entry from any in-memory caches -- the caller must take care to ensure that any such
     * caches have been invalidated/cleared as necessary.
     *
     * @param context the transaction context in which to perform the delete
     * @param value the reverse directory value to delete
     *
     * @return a future that completes when the delete operation has completed
     */
    @VisibleForTesting
    protected abstract CompletableFuture<Void> deleteReverseForTesting(@Nonnull FDBRecordContext context, long value);

    /**
     * Explicitly write an entry to the reverse directory for the resolver. This method is only intended for internal
     * use (thus, the {@code protected} qualifier), and is generally only exposed for the purposes of repairing
     * entries that are missing or incorrect in the reverse mapping, or intentionally corrupting an entry for
     * testing of the resolver validation API's.
     *
     * @param context the transaction context in which to perform the delete
     * @param value the reverse directory value to write
     * @param key the key to the forward directory associated with the {@code value}
     *
     * @return a future that completes when the put operation has completed
     */
    protected abstract CompletableFuture<Void> putReverse(@Nonnull FDBRecordContext context, long value, @Nonnull String key);

    @Nonnull
    private static ResolverStateProto.State deserializeResolverState(@Nullable byte[] bytes) {
        if (bytes != null) {
            try {
                return ResolverStateProto.State.parseFrom(bytes);
            } catch (InvalidProtocolBufferException exception) {
                throw new RecordCoreException("invalid state value", exception)
                        .addLogInfo("valueBytes", ByteArrayUtil2.loggable(bytes));
            }
        }
        // if state key is not preset, use default values: unlocked, version=0
        return ResolverStateProto.State.newBuilder().build();
    }

    @Override
    public String toString() {
        return this.getClass().getName() + ":" + location.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || this.getClass() != obj.getClass()) {
            return false;
        }
        LocatableResolver that = this.getClass().cast(obj);

        return Objects.equals(this.database, that.database) && this.location.equals(that.location);
    }

    @Override
    public int hashCode() {
        // pre-computed in constructor
        return hashCode;
    }

    private enum StateMutation {
        LOCK(state -> state.toBuilder().setLock(ResolverStateProto.WriteLock.WRITE_LOCKED).build()),
        UNLOCK(state -> state.toBuilder().setLock(ResolverStateProto.WriteLock.UNLOCKED).build()),
        RETIRE(state -> state.toBuilder().setLock(ResolverStateProto.WriteLock.RETIRED).build()),
        INCREMENT_VERSION(state -> state.toBuilder().setVersion(state.getVersion() + 1).build()),
        EXCLUSIVE_LOCK(state -> {
            if (state.getLock() != ResolverStateProto.WriteLock.UNLOCKED) {
                throw new LocatableResolverLockedException("resolver must be unlocked to get exclusive lock")
                        .addLogInfo("lockState", state.getLock());
            }
            return LOCK.apply(state);
        });

        @Nonnull
        private final Function<ResolverStateProto.State, ResolverStateProto.State> mutation;

        StateMutation(@Nonnull Function<ResolverStateProto.State, ResolverStateProto.State> mutation) {
            this.mutation = mutation;
        }

        @Nonnull
        private ResolverStateProto.State apply(ResolverStateProto.State state) {
            return mutation.apply(state);
        }
    }

    /**
     * Exception thrown when the locatable resolver is locked.
     */
    @SuppressWarnings("serial")
    public static class LocatableResolverLockedException extends RecordCoreException {
        LocatableResolverLockedException(String message) {
            super(message);
        }
    }

    /**
     * Simple class to hide what type of path (if any) the resolver was created with, providing nothing but
     * the ability to log the value.
     */
    private static class ResolverLocation {
        @Nullable KeySpacePath path;
        @Nullable ResolvedKeySpacePath resolvedKeySpacePath;

        public ResolverLocation(@Nullable KeySpacePath path, @Nullable CompletableFuture<ResolvedKeySpacePath> resolvedFuture) {
            if (resolvedFuture != null && resolvedFuture.isDone() && !resolvedFuture.isCompletedExceptionally()) {
                this.resolvedKeySpacePath = resolvedFuture.join();
            }
            this.path = path;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ResolverLocation that = (ResolverLocation)o;
            return Objects.equals(this.path, that.path);
        }

        @Override
        public int hashCode() {
            return path == null ? 0 : path.hashCode();
        }

        @Override
        public String toString() {
            if (resolvedKeySpacePath != null) {
                return resolvedKeySpacePath.toString();
            } else if (path != null) {
                return path.toString();
            }
            return "GLOBAL";
        }
    }
}
