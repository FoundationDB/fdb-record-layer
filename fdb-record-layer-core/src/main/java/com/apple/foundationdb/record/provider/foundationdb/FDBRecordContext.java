/*
 * FDBRecordContext.java
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

import com.apple.foundationdb.FDBError;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.util.MapUtils;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CharMatcher;
import com.google.common.base.Utf8;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.charset.Charset;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An open transaction against FDB.
 *
 * <p>
 * All reads and writes to the database are transactional: an open {@code FDBRecordContext} is needed.
 * An {@link FDBDatabase} is needed to open an {@code FDBRecordContext}.
 * </p>
 *
 * <pre><code>
 * final FDBDatabase fdb = FDBDatabaseFactory.instance().getDatabase();
 * try (FDBRecordContext ctx = fdb.openContext()) {
 *     ...
 * }
 * </code></pre>
 *
 * @see FDBRecordStore
 */
@API(API.Status.STABLE)
public class FDBRecordContext extends FDBTransactionContext implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FDBRecordContext.class);
    private static final byte[] META_DATA_VERSION_STAMP_KEY = new byte[]{(byte)0xff, '/', 'm', 'e', 't', 'a', 'd', 'a', 't', 'a', 'V', 'e', 'r', 's', 'i', 'o', 'n'};
    private static final byte[] META_DATA_VERSION_STAMP_VALUE = new byte[FDBRecordVersion.GLOBAL_VERSION_LENGTH + Integer.BYTES];
    private static final long UNSET_VERSION = 0L;

    static {
        Arrays.fill(META_DATA_VERSION_STAMP_VALUE, (byte)0x00);
    }

    /**
     * Internally generated or anonymous commit hooks are prefixed this this value.
     */
    private static final String INTERNAL_COMMIT_HOOK_PREFIX = "@__";
    private static final String AFTER_COMMIT_HOOK_NAME = INTERNAL_COMMIT_HOOK_PREFIX + "afterCommit";

    /**
     * The maximum size for a transaction ID in bytes when serialized as UTF-8. This value is used to determine
     * whether the transaction ID provided in
     * {@link FDBDatabase#openContext(Map, FDBStoreTimer, FDBDatabase.WeakReadSemantics, FDBTransactionPriority, String)}
     * should be truncated or dropped. Note that Java {@link String}s are encoded using UTF-16, so using
     * {@link String#length()} is insufficient to know if the transaction ID will be too large if it contains
     * any non-ASCII characters (though it is recommended that all transaction IDs be printable ASCII characters
     * as those are the ones that render well in the logs). To get the size in UTF-8, one can serialize the
     * string to UTF-8 using {@link String#getBytes(Charset)} or check its encoded size using
     * {@link Utf8#encodedLength(CharSequence)} or an equivalent function.
     *
     * <p>
     * Note that this limit is inherited by the Record Layer from the FoundationDB client. In particular, the
     * {@link com.apple.foundationdb.TransactionOptions#setDebugTransactionIdentifier(String)} method will
     * not accept IDs longer than 100 bytes in length.
     * </p>
     *
     * @see #getTransactionId()
     * @see FDBDatabase#openContext(Map, FDBStoreTimer, FDBDatabase.WeakReadSemantics, FDBTransactionPriority, String)
     * @see com.apple.foundationdb.TransactionOptions#setDebugTransactionIdentifier(String)
     */
    public static final int MAX_TR_ID_SIZE = 100;

    @Nullable
    private CompletableFuture<Long> readVersionFuture;
    private long readVersion = UNSET_VERSION;
    private long committedVersion = UNSET_VERSION;
    private long transactionCreateTime;
    @Nullable
    private final String transactionId;
    @Nullable
    private final Throwable openStackTrace;
    private boolean logged;
    @Nullable
    private byte[] versionStamp;
    @Nonnull
    private AtomicInteger localVersion;
    @Nonnull
    private ConcurrentNavigableMap<byte[], Integer> localVersionCache;
    @Nonnull
    private ConcurrentNavigableMap<byte[], Pair<MutationType, byte[]>> versionMutationCache;
    @Nullable
    private final FDBDatabase.WeakReadSemantics weakReadSemantics;
    @Nonnull
    private final FDBTransactionPriority priority;
    private final long timeoutMillis;
    @Nullable
    private Consumer<FDBStoreTimer.Wait> hookForAsyncToSync = null;
    @Nonnull
    private final Queue<CommitCheckAsync> commitChecks = new ArrayDeque<>();
    @Nonnull
    private final Map<String, CommitCheckAsync> namedCommitChecks = new LinkedHashMap<>();
    @Nonnull
    private final Map<String, PostCommit> postCommits = new LinkedHashMap<>();
    private boolean dirtyStoreState;
    private boolean dirtyMetaDataVersionStamp;
    private long trackOpenTimeNanos;

    protected FDBRecordContext(@Nonnull FDBDatabase fdb,
                               @Nonnull Transaction transaction,
                               @Nonnull FDBRecordContextConfig config) {
        super(fdb, transaction, config.getTimer());
        this.transactionCreateTime = System.currentTimeMillis();
        this.localVersion = new AtomicInteger(0);
        this.localVersionCache = new ConcurrentSkipListMap<>(ByteArrayUtil::compareUnsigned);
        this.versionMutationCache = new ConcurrentSkipListMap<>(ByteArrayUtil::compareUnsigned);
        this.transactionId = getSanitizedId(config);
        this.openStackTrace = config.isSaveOpenStackTrace() ? new Throwable("Not really thrown") : null;

        @Nonnull Transaction tr = ensureActive();
        if (this.transactionId != null) {
            tr.options().setDebugTransactionIdentifier(this.transactionId);
            if (config.isLogTransaction()) {
                logTransaction();
            }
        }

        // If a causal read risky is requested, we set the corresponding transaction option
        this.weakReadSemantics = config.getWeakReadSemantics();
        if (weakReadSemantics != null && weakReadSemantics.isCausalReadRisky()) {
            tr.options().setCausalReadRisky();
        }

        this.priority = config.getPriority();
        switch (priority) {
            case BATCH:
                tr.options().setPriorityBatch();
                break;
            case DEFAULT:
                // Default priority does not need to set any option
                break;
            case SYSTEM_IMMEDIATE:
                tr.options().setPrioritySystemImmediate();
                break;
            default:
                throw new RecordCoreArgumentException("unknown priority level " + priority);
        }

        // Set the transaction timeout based on the config (if set) and the database factory otherwise
        this.timeoutMillis = getTimeoutMillisToSet(fdb, config);
        if (timeoutMillis != FDBDatabaseFactory.DEFAULT_TR_TIMEOUT_MILLIS) {
            // If the value is DEFAULT_TR_TIMEOUT_MILLIS, then this uses the system default and does not need to be set here
            tr.options().setTimeout(timeoutMillis);
        }

        this.dirtyStoreState = false;
    }

    @Nullable
    private static String getSanitizedId(@Nonnull FDBRecordContextConfig config) {
        if (config.getTransactionId() != null) {
            return getSanitizedId(config.getTransactionId());
        } else if (config.getMdcContext() != null) {
            String mdcId = config.getMdcContext().get("uuid");
            return mdcId == null ? null : getSanitizedId(mdcId);
        } else {
            return null;
        }
    }

    @Nullable
    private static String getSanitizedId(@Nonnull String id) {
        try {
            if (Utf8.encodedLength(id) > MAX_TR_ID_SIZE) {
                if (CharMatcher.ascii().matchesAllOf(id)) {
                    // Most of the time, the string will be of ascii characters, so return a truncated ID based on length
                    return id.substring(0, MAX_TR_ID_SIZE - 3) + "...";
                } else {
                    // In theory, we could try and split the UTF-16 string and find a string that fits, but that
                    // is fraught with peril, not the least of which because one might accidentally split a low
                    // surrogate/high surrogate pair.
                    return null;
                }
            } else {
                return id;
            }
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static long getTimeoutMillisToSet(@Nonnull FDBDatabase fdb, @Nonnull FDBRecordContextConfig config) {
        if (config.getTransactionTimeoutMillis() != FDBDatabaseFactory.DEFAULT_TR_TIMEOUT_MILLIS) {
            return config.getTransactionTimeoutMillis();
        } else {
            return fdb.getFactory().getTransactionTimeoutMillis();
        }
    }

    /**
     * Get the ID used by FoundationDB to track this transaction. This can be used as a correlation key to correlate
     * requests with their transactions. If this returns {@code null}, then no ID has been set. This means that
     * it is unsafe to call {@link #logTransaction()} on this context if this method returns {@code null}.
     *
     * <p>
     * This ID is used by FoundationDB internally in a few different places, including the transaction sample, large
     * transaction monitoring, and client trace logs if transaction logging is enabled for that transaction. If the
     * caller already has a notion of "request ID", then one strategy might be to set the transaction's ID to the
     * initiating request's ID so that one can correlate requests and transactions.
     *
     * <p>
     * The transaction ID is limited in size to 100 bytes when encoded as UTF-8. In general, most callers should
     * limit IDs to printable ASCII characters (as those are the only characters that are easily readable in the
     * client trace logs). If the provided ID exceeds 100 bytes, it will be truncated or possibly ignored if
     * truncating the ID cannot be done safely.
     * </p>
     *
     * <p>
     * To set this ID, the user can call either {@link FDBDatabase#openContext(Map, FDBStoreTimer, FDBDatabase.WeakReadSemantics, FDBTransactionPriority, String)}
     * and provided a non-{@code null} transaction ID as a parameter, or the user can call
     * {@link FDBDatabase#openContext(Map, FDBStoreTimer)} or {@link FDBDatabase#openContext(Map, FDBStoreTimer, FDBDatabase.WeakReadSemantics)}
     * and set the "uuid" key to the desired transaction ID in the MDC context. In either case, note that the
     * transaction ID is limited in size to 100 bytes when encoded in UTF-8. In general, most callers should limit
     * IDs to printable ASCII characters (as those are the only characters that are easily readable in the client trace
     * logs). If the provided ID exceeds 100 bytes, it will be truncated or possibly ignored if truncating the ID
     * cannot be done safely.
     * </p>
     *
     * @return the ID used by FoundationDB to track this transaction or {@code null} if not set
     * @see #logTransaction()
     */
    @Nullable
    public String getTransactionId() {
        return transactionId;
    }

    /**
     * Get the timeout time for the underlying transaction. The value returned here is whatever timeout is actually
     * set for this transaction, if one is set through the context's constructor. This can be from either an
     * {@link FDBDatabaseFactory}, an {@link FDBDatabaseRunner}, or an {@link FDBRecordContextConfig}. If this
     * returns {@link FDBDatabaseFactory#DEFAULT_TR_TIMEOUT_MILLIS}, then this indicates that the transaction was
     * set using the default system timeout, which is configured with {@link com.apple.foundationdb.DatabaseOptions#setTransactionTimeout(long)}.
     * As those options can not be inspected through FoundationDB Java bindings, this method cannot return an
     * accurate result. Likewise, if a user explicitly sets the underlying option using {@link com.apple.foundationdb.TransactionOptions#setTimeout(long)},
     * then this method will not return an accurate result.
     *
     * @return the timeout configured for this transaction at its initialization
     * @see FDBDatabaseFactory#setTransactionTimeoutMillis(long)
     * @see FDBDatabaseRunner#setTransactionTimeoutMillis(long)
     * @see FDBRecordContextConfig.Builder#setTransactionTimeoutMillis(long)
     * @see FDBExceptions.FDBStoreTransactionTimeoutException
     */
    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    /**
     * Write the details of this transaction to the FoundationDB client logs. Note that this operation does not do
     * anything if the client has not been configured to emit logs. This should only really be used for debugging
     * purposes, as the messages that are logged here can be rather verbose, and they include all read and written keys
     * and values.
     *
     * <p>
     * All of the transaction's entries will be tagged with this transaction's ID. If an ID has not been set, this
     * method will throw a {@link RecordCoreException}. As a result, the user is encouraged to call
     * {@link #getTransactionId()} before calling this method.
     * </p>
     *
     * NOTE: It is generally better to enable logging at open time via the {@link FDBRecordContextConfig}.
     *
     * @see #getTransactionId()
     * @see FDBDatabaseFactory#setTrace(String, String)
     * @see com.apple.foundationdb.TransactionOptions#setLogTransaction()
     * @see FDBRecordContextConfig.Builder#setLogTransaction(boolean)
     */
    public final void logTransaction() {
        if (transactionId == null) {
            throw new RecordCoreException("Cannot log transaction as ID is not set");
        }
        // TODO: Consider deprecating this method and moving this inline.
        ensureActive().options().setLogTransaction();
        logged = true;
    }

    /**
     * Get whether the current transaction details are logged to the client trace logs. Essentially, this returns
     * if the transaction has been traced or if the user has (successfully) called {@link #logTransaction()}.
     * See {@link #logTransaction()} for more details.
     *
     * @return whether this transaction is logged to the client trace logs
     * @see #logTransaction()
     */
    public boolean isLogged() {
        return logged;
    }

    /**
     * Get the nanosecond time at which this context was opened.
     * @return time opened
     */
    @API(API.Status.INTERNAL)
    @VisibleForTesting
    public long getTrackOpenTimeNanos() {
        return trackOpenTimeNanos;
    }

    /**
     * Set the nanosecond time at which this context was opened.
     * @param trackOpenTimeNanos  time opened
     */
    void setTrackOpenTimeNanos(final long trackOpenTimeNanos) {
        this.trackOpenTimeNanos = trackOpenTimeNanos;
    }

    /**
     * Get any stack track generated when this context was opened.
     * @return stack trace or {@code null}
     */
    @Nullable
    Throwable getOpenStackTrace() {
        return openStackTrace;
    }

    public boolean isClosed() {
        return transaction == null;
    }

    @Override
    public void close() {
        closeTransaction(false);
    }

    synchronized void closeTransaction(boolean openTooLong) {
        if (transaction != null) {
            try {
                transaction.close();
            } finally {
                transaction = null;
                if (trackOpenTimeNanos != 0) {
                    database.untrackOpenContext(this);
                }
                if (timer != null) {
                    timer.increment(FDBStoreTimer.Counts.CLOSE_CONTEXT);
                    if (openTooLong) {
                        timer.increment(FDBStoreTimer.Counts.CLOSE_CONTEXT_OPEN_TOO_LONG);
                    }
                }
            }
        }
    }

    @Nonnull
    private CompletableFuture<Void> injectLatency(@Nonnull FDBLatencySource latencySource) {
        final long latencyMillis = database.getLatencyToInject(latencySource);
        if (latencyMillis <= 0L) {
            return AsyncUtil.DONE;
        }

        return instrument(latencySource.getTimerEvent(), MoreAsyncUtil.delayedFuture(latencyMillis, TimeUnit.MILLISECONDS));
    }

    /**
     * Commit an open transaction.
     */
    public void commit() {
        asyncToSync(FDBStoreTimer.Waits.WAIT_COMMIT, commitAsync());
    }

    /**
     * Async version of {@link #commit}.
     * @return a future that is complete when commit is done
     */
    public CompletableFuture<Void> commitAsync() {
        long startTimeNanos = System.nanoTime();
        ensureActive();
        CompletableFuture<Void> checks = runCommitChecks();
        versionMutationCache.forEach((key, valuePair) ->
                transaction.mutate(valuePair.getLeft(), key, valuePair.getRight()));
        CompletableFuture<byte[]> versionFuture = transaction.getVersionstamp();
        long beforeCommitTimeMillis = System.currentTimeMillis();
        CompletableFuture<Void> commit = MoreAsyncUtil.isCompletedNormally(checks) ?
                                         delayedCommit() :
                                         checks.thenCompose(vignore -> delayedCommit());
        commit = commit.thenCompose(vignore -> {
            // The committed version will be -1 if the transaction is read-only,
            // in which case versionFuture has completed exceptionally with
            // transaction_read_only and thus can be ignored.
            committedVersion = transaction.getCommittedVersion();
            if (committedVersion > 0) {
                // The getVersionstamp() future can complete a tiny bit after the commit() future.
                return versionFuture.thenAccept(vs -> versionStamp = vs);
            } else {
                return AsyncUtil.DONE;
            }
        });
        return commit.whenComplete((v, ex) -> {
            StoreTimer.Event event = FDBStoreTimer.Events.COMMIT;
            try {
                if (ex != null) {
                    event = FDBStoreTimer.Events.COMMIT_FAILURE;
                } else {
                    if (committedVersion > 0) {
                        if (database.isTrackLastSeenVersionOnCommit()) {
                            database.updateLastSeenFDBVersion(beforeCommitTimeMillis, committedVersion);
                        }
                    } else {
                        event = FDBStoreTimer.Events.COMMIT_READ_ONLY;
                    }
                }
            } finally {
                close();
                if (timer != null) {
                    timer.recordSinceNanoTime(event, startTimeNanos);
                }
            }
        }).thenCompose(vignore -> runPostCommits());
    }

    /**
     * Returns a commit that may be delayed due to latency injection.
     */
    private CompletableFuture<Void> delayedCommit() {
        return injectLatency(FDBLatencySource.COMMIT_ASYNC).thenCompose(vignore -> transaction.commit());
    }

    @Override
    @Nonnull
    public Transaction ensureActive() {
        if (transaction == null) {
            throw new RecordCoreStorageException("Transaction is no longer active.");
        }
        return transaction;
    }

    /**
     * Set the read version used by this transaction. All reads to the database will include
     * only changes that were committed at this version or smaller. A transaction's read version
     * can only be set once, so if this function is called multiple times, it will return the
     * previously set read version. If this method is called and another caller has already called
     * {@link #getReadVersionAsync()} or {@link #getReadVersion()}, this method may throw a
     * {@link RecordCoreException} indicating that there is already an outstanding read version request
     * if that request has not yet completed.
     *
     * @param readVersion the read version this transaction should use if is not already set
     * @return this transaction's read version
     * @see Transaction#setReadVersion(long)
     */
    public synchronized long setReadVersion(long readVersion) {
        if (hasReadVersion()) {
            return this.readVersion;
        }
        if (readVersionFuture != null) {
            if (MoreAsyncUtil.isCompletedNormally(readVersionFuture)) {
                return joinNow(readVersionFuture);
            } else {
                throw new RecordCoreException("Cannot set read version as read version request is outstanding");
            }
        }
        ensureActive().setReadVersion(readVersion);
        this.readVersion = readVersion;
        this.readVersionFuture = CompletableFuture.completedFuture(readVersion);
        return readVersion;
    }

    /**
     * Get the read version used by this transaction. All reads to the database will include only changes that
     * were committed at this version or smaller. If the read version has not already been set or gotten, this
     * may require talking to the database. If the read version has already been set or gotten, then this will return
     * with an already completed future.
     *
     * <p>
     * Note that this method is {@code synchronized}, but only creating the future (<em>not</em> waiting on
     * the future) will block other threads. Thus, while it is advised that this method only be called once
     * and by only one caller at a time, if it safe to use this method in asynchronous contexts. If this method is
     * called multiple times, then the same future will be returned each time.
     * </p>
     *
     * @return a future that will contain the read version of this transaction
     * @see Transaction#getReadVersion()
     */
    @Nonnull
    public synchronized CompletableFuture<Long> getReadVersionAsync() {
        if (readVersionFuture != null) {
            return readVersionFuture;
        }
        ensureActive(); // call ensure active here so that we don't inject latency on inactive contexts
        long startTimeMillis = System.currentTimeMillis();
        long startTimeNanos = System.nanoTime();
        CompletableFuture<Long> localReadVersionFuture = injectLatency(FDBLatencySource.GET_READ_VERSION)
                .thenCompose(ignore -> ensureActive().getReadVersion())
                .thenApply(newReadVersion -> {
                    readVersion = newReadVersion;
                    if (database.isTrackLastSeenVersionOnRead()) {
                        database.updateLastSeenFDBVersion(startTimeMillis, newReadVersion);
                    }
                    return newReadVersion;
                });
        // Instrument batch priority transactions and non-batch priority transactions separately as additional latency
        // is expected from back pressure on batch priority transactions.
        FDBStoreTimer.Event grvEvent = FDBTransactionPriority.BATCH.equals(priority) ? FDBStoreTimer.Events.BATCH_GET_READ_VERSION : FDBStoreTimer.Events.GET_READ_VERSION;
        localReadVersionFuture = instrument(grvEvent, localReadVersionFuture, startTimeNanos);
        readVersionFuture = localReadVersionFuture;
        return localReadVersionFuture;
    }

    /**
     * Get the read version used by this transaction. This is a synchronous version of {@link #getReadVersionAsync()}.
     * Note that if the read version has already been set or gotten (either by calling {@link #setReadVersion(long)} or
     * {@link #getReadVersionAsync()} or this method), then the previously set read version is returned immediately,
     * and this method will not block. One can check if the read version has already been set by calling
     * {@link #hasReadVersion()}.
     *
     * @return the read version of this transaction
     * @see #getReadVersionAsync()
     * @see Transaction#getReadVersion()
     */
    @SpotBugsSuppressWarnings(value = "UG_SYNC_SET_UNSYNC_GET", justification = "read only one field and avoid blocking in setReadVersion")
    public long getReadVersion() {
        if (hasReadVersion()) {
            return readVersion;
        }
        return asyncToSync(FDBStoreTimer.Waits.WAIT_GET_READ_VERSION, getReadVersionAsync());
    }

    /**
     * Get whether this transaction's read version has already been set. In particular, this will return
     * {@code true} if someone has explicitly called {@link #setReadVersion(long)} or
     * {@link #getReadVersion()} on this context or if a {@link #getReadVersionAsync()} call has completed, and it will
     * return {@code false} otherwise. If this returns {@code true}, then {@link #getReadVersionAsync()} will return
     * an immediately ready future and {@link #getReadVersion()} is non-blocking.
     *
     * @return whether this transaction's read version has already been set
     * @see #getReadVersionAsync()
     * @see #setReadVersion(long)
     * @see Transaction#getReadVersion()
     */
    public boolean hasReadVersion() {
        return readVersion != UNSET_VERSION;
    }

    @Nonnull
    public ReadTransaction readTransaction(boolean snapshot) {
        if (snapshot) {
            return ensureActive().snapshot();
        } else {
            return ensureActive();
        }
    }

    /**
     * Get the number of milliseconds since context was created.
     * @return the number of milliseconds since context was created
     */
    public long getTransactionAge() {
        return System.currentTimeMillis() - transactionCreateTime;
    }

    public long getTransactionCreateTime() {
        return transactionCreateTime;
    }

    @API(API.Status.INTERNAL)
    public void setDirtyStoreState(boolean dirtyStoreState) {
        this.dirtyStoreState = dirtyStoreState;
    }

    /**
     * Return whether any record store opened with this context has had its cache-able store state modified.
     * This is then used to avoid using the cached state when there have been modifications to the cached state
     * within this transaction. Note that if multiple record stores are opened within a single transaction
     * and one (but not all of them) updates its state, then the other record stores will also eschew the
     * cache.
     *
     * <p>
     * This method is internal to the Record Layer and should not be used by external consumers.
     * </p>
     *
     * @return whether the record store's state has been modified in the course of this transaction
     */
    @API(API.Status.INTERNAL)
    public boolean hasDirtyStoreState() {
        return dirtyStoreState;
    }

    /**
     * A consistency check, such as uniqueness, that can execute asynchronously and is finally checked at or before commit time.
     * @see #addCommitCheck(CommitCheckAsync)
     */
    public interface CommitCheckAsync {
        /**
         * Get whether the check is ready to be tested.
         * @return {@code true} if the check is complete
         */
        default boolean isReady() {
            return false;
        }

        /**
         * Complete the check.
         *
         * This is always called once before {@link #commit} finishes. If {@link #isReady} returns {@code true} earlier,
         * it can be called while processing the transaction.
         * @return a future that will be complete (exceptionally if the check fails) when the check has been performed
         */
        @Nonnull
        CompletableFuture<Void> checkAsync();
    }

    /**
     * A synchronous {@link CommitCheckAsync}.
     *
     * At some point, this class will be deprecated.
     * Please implement {@link CommitCheckAsync} directly or call {@link #addCommitCheck(CompletableFuture)} instead.
     */
    public interface CommitCheck extends CommitCheckAsync {
        @Override
        @Nonnull
        default CompletableFuture<Void> checkAsync() {
            check();
            return AsyncUtil.DONE;
        }

        /**
         * Complete the check.
         *
         * This is always called once before {@link #commit} finishes. If {@link #isReady} returns {@code true} earlier,
         * it can be called while processing the transaction.
         *
         * <p>
         * This method should not block or {@link #commitAsync} will block. It is therefore much
         * better to always implement {@link CommitCheckAsync} or call {@link #addCommitCheck(CompletableFuture)} instead.
         */
        void check();
    }

    /**
     * Add a {@link CommitCheckAsync} to be performed before {@link #commit} finishes.
     *
     * This method is suitable for checks that cannot be started until just before commit.
     * For checks that can be started before {@code addCommitCheck} time, {@link #addCommitCheck(CompletableFuture)}
     * may be more convenient.
     * <p>
     * It is possible for this method to throw an exception caused by an earlier unsuccessful check that has become ready in the meantime.
     * @param check the check to be performed
     */
    public synchronized void addCommitCheck(@Nonnull CommitCheckAsync check) {
        addCommitCheck(null, check);
    }

    public synchronized void addCommitCheck(@Nullable String name, @Nonnull CommitCheckAsync check) {
        while (!commitChecks.isEmpty()) {
            if (commitChecks.peek().isReady()) {
                asyncToSync(FDBStoreTimer.Waits.WAIT_ERROR_CHECK, commitChecks.remove().checkAsync());
                if (name != null) {
                    namedCommitChecks.remove(name);
                }
            } else {
                break;
            }
        }
        commitChecks.add(check);
        if (name != null) {
            namedCommitChecks.putIfAbsent(name, check);
        }
    }

    @SuppressWarnings("unchecked")
    public synchronized <T extends CommitCheckAsync> T getCommitCheck(String name, Class<T> clazz) {
        return (T) namedCommitChecks.get(name);
    }

    public synchronized void removeNamedCheck(String name) {
        namedCommitChecks.remove(name);
    }

    /**
     * Add a check to be completed before {@link #commit} finishes.
     *
     * {@link #commit} will wait for the future to be completed (exceptionally if the check fails)
     * before committing the underlying transaction.
     * <p>
     * It is possible for this method to throw an exception caused by an earlier unsuccessful check that has become ready in the meantime.
     * @param check the check to be performed
     */
    public synchronized void addCommitCheck(@Nonnull CompletableFuture<Void> check) {
        addCommitCheck(new CommitCheckAsync() {
            @Override
            public boolean isReady() {
                return check.isDone();
            }

            @Nonnull
            @Override
            public CompletableFuture<Void> checkAsync() {
                return check;
            }
        });
    }

    /**
     * Run any {@link CommitCheckAsync}s that are still outstanding.
     * @return a future that is complete when all checks have been performed
     */
    @Nonnull
    public CompletableFuture<Void> runCommitChecks() {
        List<CompletableFuture<Void>> futures;
        synchronized (this) {
            if (commitChecks.isEmpty()) {
                return AsyncUtil.DONE;
            } else {
                futures = commitChecks.stream().map(CommitCheckAsync::checkAsync).collect(Collectors.toList());
            }
        }
        return AsyncUtil.whenAll(futures);
    }

    /**
     * A hook to run after commit has completed successfully. Multiple after-commit hooks may be installed, however
     * all are executed sequentially in the order in which they were installed, in a single future.  If you need
     * to perform long-running or potentially concurrent activities post-commit, use {@link PostCommit} instead.
     */
    public interface AfterCommit {
        void run();
    }

    /**
     * A supplier of a future to be executed after the transaction has been successfully committed. When the transaction
     * has been successfully committed, the futures returned by each installed {@code PostCommit} hook will be
     * concurrently invoked.
     */
    public interface PostCommit {
        CompletableFuture<Void> get();
    }

    /**
     * Fetches a post-commit hook, creating a new one if it does not already exist.
     *
     * @param name name of the post-commit hook
     * @param ifNotExists if the post-commit hook has not been previously installed, a function that will be
     *   called to install a new hook by the provided name
     * @return post commit hook
     */
    @Nonnull
    public PostCommit getOrCreatePostCommit(@Nonnull String name, @Nonnull Function<String, PostCommit> ifNotExists) {
        checkPostCommitName(name);
        synchronized (postCommits) {
            return MapUtils.computeIfAbsent(postCommits, name, ifNotExists);
        }
    }

    /**
     * Fetches a previously installed post-commit hook.
     *
     * @param name the name of the post-commit hook
     * @return the post-commit hook, if it was previously installed or {@code null} if there is no hook by the
     *   provided {@code name}
     */
    @Nullable
    public PostCommit getPostCommit(@Nonnull String name) {
        // Callers of the public API cannot "see" anonymous or internal post-commit hooks.
        if (isInternalCommitHookName(name)) {
            return null;
        }

        synchronized (postCommits) {
            return postCommits.get(name);
        }
    }

    /**
     * Adds a new post-commit hook. This method should only be used in cases in which you will be installing the
     * post-commit hook exactly once.  That is, due to race conditions, you should not be doing:
     * <pre>
     *     if (context.getPostCommit("myPostCommit")) {
     *         context.addPostCommit("myPostCommit", () -&gt; ..);
     *     }
     * </pre>
     * if you need this behavior use {@link #getOrCreatePostCommit(String, Function)} instead.
     *
     * @param name name of the post-commit
     * @param postCommit the post commit to install
     */
    public void addPostCommit(@Nonnull String name, @Nonnull PostCommit postCommit) {
        checkPostCommitName(name);
        synchronized (postCommits) {
            if (postCommits.containsKey(name)) {
                throw new RecordCoreArgumentException("Post-commit already exists")
                        .addLogInfo(LogMessageKeys.COMMIT_NAME, name);
            }
            postCommits.put(name, postCommit);
        }
    }

    /**
     * Install an anonymous post-commit hook. A post-commit hook installed in this fashion cannot be retrieved
     * via {@link #getPostCommit(String)}.
     *
     * @param postCommit post-commit hook to install
     */
    public void addPostCommit(@Nonnull PostCommit postCommit) {
        synchronized (postCommits) {
            String name;
            // Yes, a collision is exceedingly unlikely, but...
            do {
                name = INTERNAL_COMMIT_HOOK_PREFIX + "anon-" + (new Random()).nextInt(Integer.MAX_VALUE);
            } while (postCommits.containsKey(name));
            postCommits.put(name, postCommit);
        }
    }

    /**
     * Remove a previously installed post-commit hook.
     * @param name the name of the hook to remove
     * @return {@code null} if the hook does not exist, otherwise the handle to the previously installed hook
     */
    @Nullable
    public PostCommit removePostCommit(@Nonnull String name) {
        checkPostCommitName(name);
        synchronized (postCommits) {
            return postCommits.remove(name);
        }
    }

    @Nonnull
    private CompletableFuture<Void> runPostCommits() {
        synchronized (postCommits) {
            if (postCommits.isEmpty()) {
                return AsyncUtil.DONE;
            }
            List<CompletableFuture<Void>> work = postCommits.values().stream()
                    .map(PostCommit::get)
                    .collect(Collectors.toList());
            postCommits.clear();
            return AsyncUtil.whenAll(work);
        }
    }

    private void checkPostCommitName(@Nonnull String name) {
        if (isInternalCommitHookName(name)) {
            throw new RecordCoreArgumentException("Invalid post-commit name")
                    .addLogInfo(LogMessageKeys.COMMIT_NAME, name);
        }
    }

    private boolean isInternalCommitHookName(@Nonnull String name) {
        return name.startsWith(INTERNAL_COMMIT_HOOK_PREFIX);
    }

    /**
     * Adds code to be executed immediately following a successful commit. All after-commit hooks are run serially
     * within a single future immediately following the completion of the commit future.
     *
     * @param afterCommit code to be executed following successful commit
     */
    public void addAfterCommit(@Nonnull AfterCommit afterCommit) {
        synchronized (postCommits) {
            @Nullable
            AfterCommitPostCommit adapter = (AfterCommitPostCommit) postCommits.get(AFTER_COMMIT_HOOK_NAME);
            if (adapter == null) {
                adapter = new AfterCommitPostCommit();
                postCommits.put(AFTER_COMMIT_HOOK_NAME, adapter);
            }
            adapter.addAfterCommit(afterCommit);
        }
    }

    /**
     * Run all of the after commit hooks.
     *
     * @deprecated this method probably should never have been public
     */
    @Deprecated
    @API(API.Status.DEPRECATED)
    public void runAfterCommits() {
        synchronized (postCommits) {
            @Nullable
            AfterCommitPostCommit adapter = (AfterCommitPostCommit) postCommits.get(AFTER_COMMIT_HOOK_NAME);
            if (adapter != null) {
                adapter.run();
            }
        }
    }

    /**
     * Return the eight byte version assigned to this context at commit time. This version is
     * used internally by the database to determine which transactions should be visible
     * by which reads. (In other words, only transactions assigned a read version greater than
     * or equal to this version will see the effects of this transaction).  If this transaction is read
     * only, then no version will ever be assigned to this commit, so this function will return -1.
     *
     * @return the eight byte version associated with this transaction or <code>null</code>
     * @throws IllegalStateException if this is called prior to the transaction being committed
     */
    public long getCommittedVersion() {
        if (committedVersion == UNSET_VERSION) {
            throw new RecordCoreStorageException("Transaction has not been committed yet.");
        }
        return committedVersion;
    }

    /**
     * Return the ten byte version-stamp assigned to this context at commit time. The first
     * eight bytes will be the big-Endian byte representation of the result of
     * {@link #getCommittedVersion() getCommittedVersion()}. This version is
     * compatible with the "global version" that is required by the {@link FDBRecordVersion}
     * class and can be used to construct a complete record version from an incomplete one.
     * If this transaction is read only, then no version will ever be assigned to this commit, so this
     * function will return <code>null</code>.
     *
     * @return the ten byte global version-stamp associated with this transaction or <code>null</code>
     * @throws IllegalStateException if this is called prior to the transaction being committed
     */
    @Nullable
    @SpotBugsSuppressWarnings(value = {"EI"}, justification = "avoids copy")
    public byte[] getVersionStamp() {
        if (committedVersion == UNSET_VERSION) {
            throw new RecordCoreStorageException("Transaction has not been committed yet.");
        }
        return versionStamp;
    }

    /**
     * Get the database's meta-data version-stamp. This key is somewhat different from other keys in the
     * database in that its value is returned to the client at the same time that the client receives its
     * {@linkplain Transaction#getReadVersion() read version}. This means that reading this key does not
     * require querying any storage server, so the client can use this key as a kind of "cache invalidation" key
     * without needing to worry about the extra reads to this key overloading the backing storage servers (which
     * would be the case for other keys).
     *
     * <p>
     * This key can only be updated by calling {@link #setMetaDataVersionStamp()}, which will set the key
     * to the transaction's {@linkplain #getVersionStamp() version-stamp} when the transaction is committed.
     * If the key is set within the context of this transaction, this method will return {@code null}.
     * </p>
     *
     * @param isolationLevel the isolation level at which to read the key
     * @return a future that will complete with the current value of the meta-data version stamp or {@code null} if it is
     *      unset or has been updated during the course of this transaction
     */
    @Nonnull
    public CompletableFuture<byte[]> getMetaDataVersionStampAsync(@Nonnull IsolationLevel isolationLevel) {
        if (dirtyMetaDataVersionStamp) {
            // Ensure the transaction is active before returning so that if the transaction has been committed, but that
            // transaction also updates the store state of some transaction, the user still gets an error.
            ensureActive();
            return CompletableFuture.completedFuture(null);
        }
        return readTransaction(isolationLevel.isSnapshot()).get(META_DATA_VERSION_STAMP_KEY).handle((val, err) -> {
            if (err == null) {
                return val;
            } else {
                FDBException fdbCause = FDBExceptions.getFDBCause(err);
                if (fdbCause != null && fdbCause.getCode() == FDBError.ACCESSED_UNREADABLE.code()) {
                    // This is the error code that results from reading a key written with a versionstamp,
                    // and in this case, it indicates that the meta-data version key was written prior to the
                    // read being done. This means the store state might be dirty, so return null.
                    dirtyMetaDataVersionStamp = true;
                    return null;
                } else {
                    throw database.mapAsyncToSyncException(err);
                }
            }
        });
    }

    /**
     * A blocking version of {@link #getMetaDataVersionStampAsync(IsolationLevel)}.
     *
     * @param isolationLevel the isolation level at which to read the key
     * @return the current value of the meta-data version stamp or {@code null} if it is unset or has been updated during the course of this transaction
     * @see #getMetaDataVersionStampAsync(IsolationLevel)
     */
    @Nullable
    public byte[] getMetaDataVersionStamp(@Nonnull IsolationLevel isolationLevel) {
        return asyncToSync(FDBStoreTimer.Waits.WAIT_META_DATA_VERSION_STAMP, getMetaDataVersionStampAsync(isolationLevel));
    }

    /**
     * Update the meta-data version-stamp. At commit time, the database will write to this key
     * the commit version-stamp of this transaction. After this has been committed, any subsequent
     * transaction will see an updated value when calling {@link #getMetaDataVersionStamp(IsolationLevel)},
     * and those transactions may use that value to invalidate any stale cache entries using the
     * meta-data version-stamp key. After this method has been called, any calls to {@code getMetaDataVersionStamp()}
     * will return {@code null}. After this context has been committed, one may call {@link #getVersionStamp()}
     * to get the value that this transaction wrote to the database.
     *
     * @see #getMetaDataVersionStampAsync(IsolationLevel)
     * @see #getVersionStamp()
     */
    public void setMetaDataVersionStamp() {
        ensureActive();
        dirtyMetaDataVersionStamp = true;
        transaction.mutate(MutationType.SET_VERSIONSTAMPED_VALUE, META_DATA_VERSION_STAMP_KEY, META_DATA_VERSION_STAMP_VALUE);
    }

    @Nullable
    public <T> T asyncToSync(FDBStoreTimer.Wait event, @Nonnull CompletableFuture<T> async) {
        if (hookForAsyncToSync != null && !MoreAsyncUtil.isCompletedNormally(async)) {
            hookForAsyncToSync.accept(event);
        }
        return database.asyncToSync(timer, event, async);
    }

    /**
     * Join a future following the same logic that <code>asyncToSync()</code> uses to validate that the operation
     * isn't blocking in an asynchronous context.
     *
     * @param future the future to be completed
     * @param <T> the type of the value produced by the future
     * @return the result value
     * @see FDBDatabase#join(CompletableFuture)
     */
    public <T> T join(CompletableFuture<T> future) {
        return database.join(future);
    }

    /**
     * Join a future but validate that the future is already completed. This can be used to unwrap a completed
     * future while allowing for bugs caused by inadvertently waiting on incomplete futures to be caught.
     * In particular, this will throw an exception if the {@link BlockingInAsyncDetection} behavior is set
     * to throw an exception on incomplete futures and otherwise just log that future was waited on.
     *
     * @param future the future that should already be completed
     * @param <T> the type of the value produced by the future
     * @return the result value
     * @see FDBDatabase#joinNow(CompletableFuture)
     */
    public <T> T joinNow(CompletableFuture<T> future) {
        return database.joinNow(future);
    }

    /**
     * Get a future following the same logic that <code>asyncToSync()</code> uses to validate that the operation
     * isn't blocking in an asynchronous context.
     *
     * @param future the future to be completed
     * @param <T> the type of the value produced by the future
     * @return the result value
     * @see FDBDatabase#get(CompletableFuture)
     *
     * @throws java.util.concurrent.CancellationException if the future was cancelled
     * @throws ExecutionException if the future completed exceptionally
     * @throws InterruptedException if the current thread was interrupted
     */
    public <T> T get(CompletableFuture<T> future) throws InterruptedException, ExecutionException {
        return database.get(future);
    }

    public void timeReadSampleKey(byte[] key) {
        if (timer != null) {
            CompletableFuture<Void> future = instrument(FDBStoreTimer.Events.READ_SAMPLE_KEY, ensureActive().get(key))
                    .handle((bytes, ex) -> {
                        if (ex != null
                                && (!(ex instanceof FDBException) ||
                                    ((FDBException) ex).getCode() != FDBError.TRANSACTION_CANCELLED.code())) {
                            LOGGER.warn(KeyValueLogMessage.of("error reading sample key",
                                            LogMessageKeys.KEY, ByteArrayUtil2.loggable(key)),
                                    ex);
                        }
                        return null;
                    });
            addCommitCheck(future);
        }
    }

    @Nullable
    public Map<String, String> getMdcContext() {
        if (getExecutor() instanceof ContextRestoringExecutor) {
            return ((ContextRestoringExecutor)getExecutor()).getMdcContext();
        } else {
            return null;
        }
    }

    /**
     * Get a new {@link FDBDatabaseRunner} that will run contexts similar to this one.
     * <ul>
     * <li>Same {@linkplain FDBDatabase database}</li>
     * <li>Same {@linkplain FDBStoreTimer timer}</li>
     * <li>Same {@linkplain #getMdcContext() MDC context}</li>
     * <li>Same {@linkplain FDBDatabase.WeakReadSemantics weak read semantics}</li>
     * <li>Same {@linkplain FDBTransactionPriority priority}</li>
     * <li>Same {@linkplain #getTimeoutMillis() transaction timeout}</li>
     * </ul>
     * @return a new database runner based on this context
     */
    @Nonnull
    public FDBDatabaseRunner newRunner() {
        FDBDatabaseRunner runner = database.newRunner();
        runner.setTimer(timer);
        runner.setMdcContext(getMdcContext());
        runner.setWeakReadSemantics(weakReadSemantics);
        runner.setPriority(priority);
        runner.setTransactionTimeoutMillis(timeoutMillis);
        return runner;
    }

    /**
     * Claims a local version that is unique within a single transaction.
     * This means that any two calls to this method will return a different
     * value. If the ordering of these calls is deterministic, then it
     * is also guaranteed that the earlier calls will receive a smaller
     * version than the newer calls.
     * @return an integer to version different records added to the database
     */
    public int claimLocalVersion() {
        return localVersion.getAndIncrement();
    }

    /**
     * Register that a record used a given local version.
     * This can then be retrieved from the context using {@link #getLocalVersion(byte[]) getLocalVersion}.
     * The key provided should be the full key to the version, including any subspace
     * prefix bytes.
     *
     * @param recordVersionKey key to associate with the local version
     * @param version the local version of the key
     */
    void addToLocalVersionCache(@Nonnull byte[] recordVersionKey, int version) {
        localVersionCache.put(recordVersionKey, version);
    }

    /**
     * Remove the local version associated with a single record version key.
     * The key provided should be the full key to where the version is stored, including any
     * subspace prefix bytes.
     *
     * @param recordVersionKey the key associated with the local version being cleared
     * @return whether the key was already in the local version cache
     */
    boolean removeLocalVersion(@Nonnull byte[] recordVersionKey) {
        return localVersionCache.remove(recordVersionKey) != null;
    }

    /**
     * Get a local version assigned to some record used within this context.
     * The key provided should be the full key to where the version is stored, including any
     * subspace prefix bytes. If the key has not been associated with any version using
     * {@link #addToLocalVersionCache(byte[], int) addToLocalVersion}, then this
     * will return an unset {@link Optional}.
     *
     * @param recordVersionKey key to retrieve the local version of
     * @return the associated version or an unset {@link Optional}
     */
    @Nonnull
    Optional<Integer> getLocalVersion(@Nonnull byte[] recordVersionKey) {
        return Optional.ofNullable(localVersionCache.get(recordVersionKey));
    }

    /**
     * Add a {@link MutationType#SET_VERSIONSTAMPED_KEY SET_VERSIONSTAMPED_KEY}
     * mutation to be run at commit time. This method is deprecated in favor of
     * {@link #addVersionMutation(MutationType, byte[], byte[])} which
     * behaves like this method except that the choice of <code>SET_VERSIONSTAMPED_KEY</code>
     * as the mutation type must be made explicitly.
     *
     * @param key key bytes for the mutation
     * @param value parameter bytes for the mutation
     * @return the previous value set for the given key or <code>null</code> if unset
     * @deprecated use #addVersionMutation(MutationType, byte[], byte[]) instead
     */
    @Deprecated
    @Nullable
    public byte[] addVersionMutation(@Nonnull byte[] key, @Nonnull byte[] value) {
        return addVersionMutation(MutationType.SET_VERSIONSTAMPED_KEY, key, value);
    }

    /**
     * Add a {@link MutationType#SET_VERSIONSTAMPED_KEY SET_VERSIONSTAMPED_KEY}
     * or {@link MutationType#SET_VERSIONSTAMPED_VALUE SET_VERSIONTSTAMPED_VALUE}
     * mutation to be run at commit time. When called, this updates a local
     * cache of these mutations. The {@link #commitAsync() commitAsync} method
     * will then be sure to flush these mutations to the transaction prior to
     * calling commit.
     *
     * @param mutationType the type of versionstamp mutation
     * @param key key bytes for the mutation
     * @param value parameter bytes for the mutation
     * @return the previous value set for the given key or <code>null</code> if unset
     */
    @Nullable
    public byte[] addVersionMutation(@Nonnull MutationType mutationType, @Nonnull byte[] key, @Nonnull byte[] value) {
        Pair<MutationType, byte[]> valuePair = Pair.of(mutationType, value);
        Pair<MutationType, byte[]> existingPair = versionMutationCache.put(key, valuePair);
        return existingPair != null ? existingPair.getRight() : null;
    }

    /**
     * Remove a {@link MutationType#SET_VERSIONSTAMPED_KEY SET_VERSIONSTAMPED_KEY}
     * mutation that would have been run at commit time. When called, this updates a local
     * cache of these mutations. This will only work as expected if the dummy
     * bytes included that stand in for the versionstamp within the key bytes
     * are equal to the dummy bytes for whatever key is included in the
     * cache already. (For example, one might use entirely <code>0xff</code>
     * bytes for those dummy bytes for all incomplete versions. This is what
     * the {@link FDBRecordVersion} class does.)
     * @param key key bytes appropriate for mutation to set
     * @return the previous value set for the given key or <code>null</code> if unset
     */
    @Nullable
    public byte[] removeVersionMutation(@Nonnull byte[] key) {
        Pair<MutationType, byte[]> existingValue = versionMutationCache.remove(key);
        return existingValue != null ? existingValue.getRight() : null;
    }

    @Nullable
    public byte[] updateVersionMutation(@Nonnull MutationType mutationType, @Nonnull byte[] key, @Nonnull byte[] value,
                                        @Nonnull BiFunction<byte[], byte[], byte[]> remappingFunction) {
        Pair<MutationType, byte[]> valuePair = Pair.of(mutationType, value);
        return versionMutationCache.merge(key, valuePair, (origPair, newPair) -> {
            if (origPair.getLeft().equals(newPair.getLeft())) {
                byte[] newValue = remappingFunction.apply(origPair.getRight(), newPair.getRight());
                return newValue == null ? null : Pair.of(origPair.getLeft(), newValue);
            } else {
                throw new RecordCoreArgumentException("cannot update mutation type for versionstamp operation");
            }
        }).getRight();
    }

    @Nullable
    public FDBDatabase.WeakReadSemantics getWeakReadSemantics() {
        return weakReadSemantics;
    }

    /**
     * Get the priority of this transaction. This is used to determine what rate-limiting rules should be
     * applied to this transaction by the database. In general, {@link FDBTransactionPriority#DEFAULT DEFAULT}
     * priority transactions are favored over {@link FDBTransactionPriority#BATCH BATCH} priority transactions.
     *
     * @return this transaction's priority
     * @see FDBTransactionPriority
     */
    @Nonnull
    public FDBTransactionPriority getPriority() {
        return priority;
    }

    public void setHookForAsyncToSync(@Nonnull Consumer<FDBStoreTimer.Wait> hook) {
        this.hookForAsyncToSync = hook;
    }

    public boolean hasHookForAsyncToSync() {
        return hookForAsyncToSync != null;
    }

    /**
     * Adapter class for accumulating the deprecated "after-commit" hook methods into the new
     * post-commit API's.
     */
    private static class AfterCommitPostCommit implements PostCommit {
        @Nonnull
        private final Queue<AfterCommit> afterCommits = new ArrayDeque<>();

        public synchronized void addAfterCommit(@Nonnull AfterCommit afterCommit) {
            afterCommits.add(afterCommit);
        }

        @Override
        public CompletableFuture<Void> get() {
            return CompletableFuture.runAsync(this::run);
        }

        public synchronized void run() {
            while (!afterCommits.isEmpty()) {
                afterCommits.remove().run();
            }
        }
    }
}
