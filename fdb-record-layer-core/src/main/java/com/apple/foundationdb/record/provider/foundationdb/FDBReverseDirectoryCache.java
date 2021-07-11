/*
 * FDBReverseDirectoryCache.java
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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreRetriableTransactionException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.LocatableResolver;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.ScopedValue;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A persistent cache providing reverse lookup facilities from the FDB {@link com.apple.foundationdb.directory.DirectoryLayer}.
 * The implementation only supports reverse lookup of the root entries of the directory layer (those * entries
 * returned by <code>DirectoryLayer.list()</code>.
 *
 * <p><b>Important note</b>: This cache is intended to be used only to retrieve directory layer entries that are
 * known to exist. An attempt to retrieve a directory layer entry that does not exist will result in a full scan
 * of the directory layer for each such request</p>
 */
@API(API.Status.INTERNAL)
public class FDBReverseDirectoryCache {

    private static final Logger LOGGER = LoggerFactory.getLogger(FDBReverseDirectoryCache.class);

    /// The name of the persistent reverse cache directory entry in the directory layer
    public static final String REVERSE_DIRECTORY_CACHE_ENTRY = new String(new byte[]{0x72, 0x65, 0x63, 0x64, 0x62, 0x5f, 0x72, 0x64, 0x5f, 0x63, 0x61, 0x63, 0x68, 0x65}, Charsets.US_ASCII);

    /**
     * The default maximum number of directory entries that will be scanned during a lookup
     * within the context of a single transaction, when this limit is hit, a new one will be
     * started before continuing.
     */
    public static final int MAX_ROWS_PER_TRANSACTION = 10_000;

    /**
     * The maximum number of milliseconds we'll spend scanning the directory during a lookup
     * within the context of a single transaction, when this limit is hit, a new one will be
     * started before continuing.
     */
    public static final long MAX_MILLIS_PER_TRANSACTION = TimeUnit.SECONDS.toMillis(3);

    private FDBDatabase fdb;
    private CompletableFuture<Long> reverseDirectoryCacheEntry;
    // TODO: Log FDBReverseDirectoryCache cache stats to StoreTimer (https://github.com/FoundationDB/fdb-record-layer/issues/12)
    private AtomicLong persistentCacheMissCount = new AtomicLong(0L);
    private AtomicLong persistentCacheHitCount = new AtomicLong(0L);

    private int maxRowsPerTransaction;
    private long maxMillisPerTransaction;

    public FDBReverseDirectoryCache(@Nonnull FDBDatabase fdb) {
        this(fdb, MAX_ROWS_PER_TRANSACTION, MAX_MILLIS_PER_TRANSACTION);
    }

    public FDBReverseDirectoryCache(@Nonnull FDBDatabase fdb, int maxRowsPerTransaction, long maxMillisPerTransaction) {
        this.fdb = fdb;
        this.maxRowsPerTransaction = maxRowsPerTransaction;
        this.maxMillisPerTransaction = maxMillisPerTransaction;
        this.reverseDirectoryCacheEntry = fdb.runAsync(context ->
                DirectoryLayer.getDefault()
                        .createOrOpen(context.ensureActive(), Collections.singletonList(REVERSE_DIRECTORY_CACHE_ENTRY))
                        .thenApply(subspace -> Tuple.fromBytes(subspace.getKey()).getLong(0))
        );
    }

    /**
     * The maximum number of rows scanned in the directory during a lookup
     * within the context of a single transaction, when this limit is hit, a new one will be
     * started before continuing.  The default value is 10,000. A value that is less than or
     * equal to zero indicates no row limit.
     * @return row limit
     */
    public int getMaxRowsPerTransaction() {
        return maxRowsPerTransaction;
    }

    public void setMaxRowsPerTransaction(int maxRowsPerTransaction) {
        this.maxRowsPerTransaction = maxRowsPerTransaction;
    }

    /**
     * The maximum number of milliseconds spent scanning the directory during a lookup
     * within the context of a single transaction, when this limit is hit, a new one will be
     * started before continuing.  The default value is 3000ms (3 seconds). A value that is
     * less than or equal to zero indicates no time limit.
     * @return time limit
     */
    public long getMaxMillisPerTransaction() {
        return maxMillisPerTransaction;
    }

    public void setMaxMillisPerTransaction(long maxMillisPerTransaction) {
        this.maxMillisPerTransaction = maxMillisPerTransaction;
    }

    /**
     * Get the number of reverse lookups in which the persistent cache was found to be missing an entry,
     * resulting in a scan of the directory layer in order to populate the missing entry.
     * @return the number of cache misses
     */
    public long getPersistentCacheMissCount() {
        return persistentCacheMissCount.get();
    }

    /**
     * Get the number of reverse lookups in which the lookup could not be satisfied by the in-memory cache
     * but was satisfied by the persistent reverse lookup cache.
     *
     * @return the number of cache hits
     */
    public long getPersistentCacheHitCount() {
        return persistentCacheHitCount.get();
    }

    /**
     * Clears all of the statistics gathered.
     */
    public void clearStats() {
        persistentCacheMissCount.set(0L);
        persistentCacheHitCount.set(0L);
    }

    /**
     * Retrieves the name of the directory entry corresponding to the value of that entry in the directory layer.
     * Unlike {@link #get(FDBStoreTimer, ScopedValue)}, only checks the reverse directory cache subspace for the key.
     * This can be useful when populating the reverse directory cache, so that we can test for the presence of an element
     * without performing an expensive directory layer scan whenever a value is missing. When performing a reverse lookup
     * of a value added to a {@link LocatableResolver} through normal means, prefer the use of {@link #get(FDBStoreTimer, ScopedValue)}.
     *
     * @param timer {@link FDBStoreTimer} for collecting metrics
     * @param scopedReverseDirectoryKey the value of the entry in the directory layer
     * @return an Optional of the key (path) associated with the provided directory layer value, if no such value exists
     * the Optional is empty
     */
    @Nonnull
    @SuppressWarnings("squid:S2095") // SonarQube doesn't realize that the context is closed in the returned future
    public CompletableFuture<Optional<String>> getInReverseDirectoryCacheSubspace(@Nullable FDBStoreTimer timer, @Nonnull ScopedValue<Long> scopedReverseDirectoryKey) {
        FDBRecordContext context = fdb.openContext();
        context.setTimer(timer);
        return getReverseCacheSubspace(scopedReverseDirectoryKey.getScope())
                .thenCompose(subspace -> getFromSubspace(context, subspace, scopedReverseDirectoryKey))
                .whenComplete((result, exception) -> context.close());
    }

    /**
     * Retrieves the name of the directory entry corresponding to the value of that entry in the directory layer.
     * Note that the retrieval and subsequent population of the stored reverse directory takes place within the
     * context of a new transaction, which means (among other things) that an attempt to <code>get()</code> a
     * directory layer entry that was just created within an as-of-yet uncommitted transaction will not be visible
     * and, thus, result in a <code>NoSuchElementException</code>.
     *
     * @param scopedReverseDirectoryKey the value of the entry in the directory layer
     * @return an Optional of the key (path) associated with the provided directory layer value, if no such value exists
     * the Optional is empty
     */
    @Nonnull
    public CompletableFuture<Optional<String>> get(@Nonnull final ScopedValue<Long> scopedReverseDirectoryKey) {
        return get(null, scopedReverseDirectoryKey);
    }

    /**
     * Retrieves the name of the directory entry corresponding to the value of that entry in the directory layer.
     * Note that the retrieval and subsequent population of the stored reverse directory takes place within the
     * context of a new transaction, which means (among other things) that an attempt to <code>get()</code> a
     * directory layer entry that was just created within an as-of-yet uncommitted transaction will not be visible
     * and, thus, result in a <code>NoSuchElementException</code>.
     *
     * @param timer {@link FDBStoreTimer} for collecting metrics
     * @param scopedReverseDirectoryKey the value of the entry in the directory layer
     * @return an Optional of the key (path) associated with the provided directory layer value, if no such value exists
     * the Optional is empty
     */
    @Nonnull
    @SuppressWarnings("squid:S2095") // SonarQube doesn't realize that the context is closed in the returned future
    public CompletableFuture<Optional<String>> get(@Nullable FDBStoreTimer timer, @Nonnull final ScopedValue<Long> scopedReverseDirectoryKey) {
        FDBRecordContext context = fdb.openContext();
        context.setTimer(timer);
        CompletableFuture<Subspace> reverseCacheSubspaceFuture = getReverseCacheSubspace(scopedReverseDirectoryKey.getScope());
        return reverseCacheSubspaceFuture
                .thenCompose(subspace -> getFromSubspace(context, subspace, scopedReverseDirectoryKey))
                .whenComplete((result, exception) -> context.close());
    }

    /**
     * Helper method to log the cache stats to the StoreTimer.
     * @param context the FDB record context
     * @param event the event to log
     */
    private void logStatsToStoreTimer(@Nonnull final FDBRecordContext context,
                                      @Nonnull FDBStoreTimer.Count event) {
        if (context.getTimer() != null) {
            context.getTimer().increment(event);
        }
    }

    private CompletableFuture<Optional<String>> getFromSubspace(@Nonnull final FDBRecordContext context,
                                                                @Nonnull final Subspace reverseCacheSubspace,
                                                                @Nonnull final ScopedValue<Long> scopedReverseDirectoryKey) {
        Long reverseDirectoryKeyData = scopedReverseDirectoryKey.getData();
        return context.ensureActive().snapshot().get(reverseCacheSubspace.pack(reverseDirectoryKeyData)).thenApply(valueBytes -> {
            if (valueBytes != null) {
                String dirString = Tuple.fromBytes(valueBytes).getString(0);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Cache miss, but found path '" + dirString + "'' in reverse lookup for value '"
                                 + scopedReverseDirectoryKey + "'");
                }
                persistentCacheHitCount.incrementAndGet();
                logStatsToStoreTimer(context, FDBStoreTimer.Counts.REVERSE_DIR_PERSISTENT_CACHE_HIT_COUNT);
                context.close();
                return Optional.of(dirString);
            }
            persistentCacheMissCount.incrementAndGet();
            logStatsToStoreTimer(context, FDBStoreTimer.Counts.REVERSE_DIR_PERSISTENT_CACHE_MISS_COUNT);
            return Optional.empty();
        });
    }

    /**
     * Explicitly add a new entry to the reverse directory cache.
     *
     * @param context the transactional context
     * @param pathKey the name of an entry in the FDB directory, the value of which is retrieved from the directory
     *                and explicitly inserted into the reverse directory cache
     * @return a future that performs the action
     * @throws NoSuchElementException will be thrown by this future if the <code>name</code> provided does not exist in the directory layer.
     */
    public CompletableFuture<Void> put(@Nonnull FDBRecordContext context, @Nonnull ScopedValue<String> pathKey) {
        final LocatableResolver scope = pathKey.getScope();
        final String key = pathKey.getData();

        return getReverseCacheSubspace(scope)
                .thenCompose(reverseCacheSubspace -> scope.mustResolve(context, key)
                        .thenApply(value -> {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug(KeyValueLogMessage.of("Adding value to reverse directory cache",
                                                LogMessageKeys.KEY, pathKey,
                                                LogMessageKeys.VALUE, value));
                            }
                            context.ensureActive().set(reverseCacheSubspace.pack(value), Tuple.from(pathKey.getData()).pack());
                            return null;
                        }));
    }

    /**
     * Add a new entry to the reverse directory cache if it does not already exist. If an entry already exists
     * for the <code>pathString</code> provided, the value for the key in the RDC will be compared against the
     * <code>pathValue</code> provided and an <code>IllegalStateException</code> will be thrown if they differ.
     * Note that no effort is made to determine if the <code>pathValue</code> actually exists in the directory
     * layer itself!
     *
     * @param context the transaction context in which to do the operation
     * @param scopedPathString the name of the entry within the scoped of an FDB directory layer
     * @param pathValue the value of the entry that from that FDB directory layer
     * @return a future that performs the action
     * @throws IllegalStateException if the <code>pathkey</code> provided already exists in the reverse directory
     *                               layer and the <code>pathValue</code> provided does not match that value
     */
    public CompletableFuture<Void> putIfNotExists(@Nonnull FDBRecordContext context,
                                                  @Nonnull ScopedValue<String> scopedPathString,
                                                  @Nonnull Long pathValue) {
        LocatableResolver scope = scopedPathString.getScope();
        String pathString = scopedPathString.getData();
        String cachedString = context.getDatabase().getReverseDirectoryInMemoryCache().getIfPresent(scope.wrap(pathValue));
        if (cachedString != null) {
            if (!cachedString.equals(pathString)) {
                // If the cachedString does not match the resolvedString, then the provided value was probably allocated
                // within this transaction and has not yet been committed and so may have allocated a value that was
                // already allocated for something else. Throw a retriable exception so that it will be retried with a
                // newer read version
                throw new RecordCoreRetriableTransactionException("Provided value for path key does not match existing value in reverse directory layer in-memory cache")
                        .addLogInfo(LogMessageKeys.RESOLVER, scope)
                        .addLogInfo(LogMessageKeys.RESOLVER_PATH, pathString)
                        .addLogInfo(LogMessageKeys.RESOLVER_KEY, pathValue)
                        .addLogInfo(LogMessageKeys.CACHED_KEY, cachedString);
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("In-memory cache contains '" + pathString + "' -> '" + pathValue + "' mapping. No need to put");
            }
            return AsyncUtil.DONE;
        }

        return getReverseCacheSubspace(scope)
                .thenCompose(subspace -> putToSubspace(context, subspace, scopedPathString, pathValue));
    }

    private CompletableFuture<Void> putToSubspace(@Nonnull FDBRecordContext context,
                                                  @Nonnull Subspace reverseCacheSubspace,
                                                  @Nonnull ScopedValue<String> scopedPathString,
                                                  @Nonnull Long pathValue) {
        String pathString = scopedPathString.getData();
        Transaction transaction = context.ensureActive();
        return transaction.snapshot().get(reverseCacheSubspace.pack(pathValue)).thenApply(valueBytes -> {
            if (valueBytes != null) {
                String readValue = Tuple.fromBytes(valueBytes).getString(0);
                if (!readValue.equals(pathString)) {
                    throw new RecordCoreException("Provided value for path key does not match existing value in reverse directory layer cache")
                            .addLogInfo(LogMessageKeys.RESOLVER, scopedPathString.getScope())
                            .addLogInfo(LogMessageKeys.RESOLVER_PATH, pathString)
                            .addLogInfo(LogMessageKeys.RESOLVER_KEY, pathValue)
                            .addLogInfo(LogMessageKeys.CACHED_KEY, readValue);
                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Put unnecessary, found path '" + readValue + "'' in reverse lookup for value '"
                            + pathValue + "'");
                }
                persistentCacheHitCount.incrementAndGet();
                logStatsToStoreTimer(context, FDBStoreTimer.Counts.REVERSE_DIR_PERSISTENT_CACHE_HIT_COUNT);
            } else {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Adding '" + pathValue + "' to reverse lookup with key " + pathString);
                }
                // Take care NOT to place the value in our cache. We don't own the calling context/transaction
                // so it is possible it could fail/rollback leaving our cache inconsistent.
                transaction.set(reverseCacheSubspace.pack(pathValue), Tuple.from(pathString).pack());
                persistentCacheMissCount.incrementAndGet();
                logStatsToStoreTimer(context, FDBStoreTimer.Counts.REVERSE_DIR_PERSISTENT_CACHE_MISS_COUNT);
            }
            return null;
        });
    }

    /**
     * Clears out the persistent reverse directory cache, repopulates it, clears out the in-memory cache
     * and resets cache miss statistics.
     * @param scope name to string resolver
     */
    @VisibleForTesting
    public void rebuild(LocatableResolver scope) {
        try (FDBRecordContext context = fdb.openContext()) {
            Subspace reverseCacheSubspace = fdb.asyncToSync(null, null, getReverseCacheSubspace(scope));
            context.ensureActive().clear(reverseCacheSubspace.range());
            context.getDatabase().clearForwardDirectoryCache();
            persistentCacheMissCount.set(0L);
            persistentCacheHitCount.set(0L);
            populate(context, reverseCacheSubspace);
        }
    }

    /**
     * Wait for any asynchronous work started at object creation time to complete. This should only be
     * used for tests in order to avoid spurious conflicts.
     */
    @VisibleForTesting
    public void waitUntilReadyForTesting() {
        reverseDirectoryCacheEntry.join();
    }

    private CompletableFuture<Subspace> getReverseCacheSubspace(LocatableResolver scope) {
        return reverseDirectoryCacheEntry.thenCombine(scope.getBaseSubspaceAsync(), (entry, subspace) ->
                subspace.subspace(Tuple.from(entry)));
    }

    private void populate(final FDBRecordContext initialContext, Subspace directory) {
        // Range for the directory layer. WARNING, this assumes a bunch about the internals of the
        // directory layer and may want to be re-worked at some point using the DirectoryLayer API's.
        final byte[] prefix = {(byte) 0xFE};
        final Subspace subdirs = new Subspace(Tuple.from(prefix, 0L), prefix);

        fdb.asyncToSync(initialContext.getTimer(), FDBStoreTimer.Waits.WAIT_REVERSE_DIRECTORY_SCAN,
                populate(initialContext, subdirs, directory, null));
    }

    @Nonnull
    private CompletableFuture<byte[]> populate(@Nonnull FDBRecordContext context,
                                               @Nonnull Subspace directorySubspace,
                                               @Nonnull Subspace reverseDirectorySubspace,
                                               @Nullable byte[] continuation) {
        return populateRegion(context, directorySubspace, reverseDirectorySubspace, continuation)
                .thenCompose( nextContinuation -> context.commitAsync().thenCompose( ignored -> {
                    context.close();
                    if (nextContinuation == null) {
                        return CompletableFuture.completedFuture(null);
                    }
                    final FDBRecordContext nextContext = fdb.openContext();
                    return populate(nextContext, directorySubspace, reverseDirectorySubspace, nextContinuation);
                } ));
    }

    @Nonnull
    private CompletableFuture<byte[]> populateRegion(@Nonnull FDBRecordContext context,
                                                     @Nonnull Subspace directorySubspace,
                                                     @Nonnull Subspace reverseDirectorySubspace,
                                                     @Nullable byte[] continuation) {
        final RecordCursor<KeyValue> cursor = KeyValueCursor.Builder.withSubspace(directorySubspace)
                .setContext(context)
                .setRange(TupleRange.ALL)
                .setContinuation(continuation)
                .setScanProperties(new ScanProperties(ExecuteProperties.newBuilder()
                        .setReturnedRowLimit(maxRowsPerTransaction)
                        .setIsolationLevel(IsolationLevel.SNAPSHOT)
                        .build()))
                .build();

        return cursor.forEachResult(result -> {
            final KeyValue kv = result.get();
            final String dirName = directorySubspace.unpack(kv.getKey()).getString(0);
            final Object dirValue = Tuple.fromBytes(kv.getValue()).get(0);

            context.ensureActive().set(reverseDirectorySubspace.pack(dirValue), Tuple.from(dirName).pack());
        }).thenApply(result -> result.getContinuation().toBytes());
    }
}

