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

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
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

    private long committedVersion;
    private long transactionCreateTime;
    @Nullable
    private byte[] versionStamp;
    @Nonnull
    private AtomicInteger localVersion;
    @Nonnull
    private ConcurrentNavigableMap<Tuple, Integer> localVersionCache;
    @Nonnull
    private ConcurrentNavigableMap<byte[], Pair<MutationType, byte[]>> versionMutationCache;
    private FDBDatabase.WeakReadSemantics weakReadSemantics;
    @Nullable
    private Consumer<FDBStoreTimer.Wait> hookForAsyncToSync = null;
    @Nonnull
    private final Queue<CommitCheckAsync> commitChecks = new ArrayDeque<>();
    @Nonnull
    private final Queue<AfterCommit> afterCommits = new ArrayDeque<>();
    private boolean dirtyStoreState;

    protected FDBRecordContext(@Nonnull FDBDatabase fdb, @Nullable Map<String, String> mdcContext,
                               boolean transactionIsTraced, @Nullable FDBDatabase.WeakReadSemantics weakReadSemantics) {
        super(fdb, fdb.createTransaction(initExecutor(fdb, mdcContext), mdcContext, transactionIsTraced));
        this.transactionCreateTime = System.currentTimeMillis();
        this.localVersion = new AtomicInteger(0);
        this.localVersionCache = new ConcurrentSkipListMap<>();
        this.versionMutationCache = new ConcurrentSkipListMap<>(ByteArrayUtil::compareUnsigned);

        if (transactionIsTraced) {
            final String uuid = mdcContext == null ? null : mdcContext.get("uuid");
            if (uuid != null) {
                transaction.options().setTransactionLoggingEnable(uuid);
            }
        }

        // If a causal read risky is requested, we set the corresponding transaction option
        if (weakReadSemantics != null && weakReadSemantics.isCausalReadRisky()) {
            transaction.options().setCausalReadRisky();
        }

        this.weakReadSemantics = weakReadSemantics;
        this.dirtyStoreState = false;
    }

    public boolean isClosed() {
        return transaction == null;
    }

    @Override
    public synchronized void close() {
        closeTransaction();
    }

    private void closeTransaction() {
        if (transaction != null) {
            try {
                transaction.close();
            } finally {
                transaction = null;
                if (timer != null) {
                    timer.increment(FDBStoreTimer.Counts.CLOSE_CONTEXT);
                }
            }
        }
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
                    runAfterCommits();
                }
            } finally {
                closeTransaction();
                if (timer != null) {
                    timer.recordSinceNanoTime(event, startTimeNanos);
                }
            }
        });
    }

    /**
     * Returns a commit that may be delayed due to latency injection.
     */
    private CompletableFuture<Void> delayedCommit() {
        return database.injectLatency(FDBLatencySource.COMMIT_ASYNC).thenCompose(vignore -> transaction.commit());
    }

    @Override
    @Nonnull
    public Transaction ensureActive() {
        if (transaction == null) {
            throw new RecordCoreStorageException("Transaction is no longer active.");
        }
        return transaction;
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
        while (!commitChecks.isEmpty()) {
            if (commitChecks.peek().isReady()) {
                asyncToSync(FDBStoreTimer.Waits.WAIT_ERROR_CHECK, commitChecks.remove().checkAsync());
            } else {
                break;
            }
        }
        commitChecks.add(check);
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
     * A hook to run after commit has completed successfully.
     */
    public interface AfterCommit {
        void run();
    }

    public synchronized void addAfterCommit(@Nonnull AfterCommit afterCommit) {
        afterCommits.add(afterCommit);
    }

    public synchronized void runAfterCommits() {
        while (!afterCommits.isEmpty()) {
            afterCommits.remove().run();
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
        if (committedVersion == 0) {
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
        if (committedVersion == 0) {
            throw new RecordCoreStorageException("Transaction has not been committed yet.");
        }
        return versionStamp;
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
     */
    public <T> T join(CompletableFuture<T> future) {
        return database.join(future);
    }

    /**
     * Get a future following the same logic that <code>asyncToSync()</code> uses to validate that the operation
     * isn't blocking in an asynchronous context.
     *
     * @param future the future to be completed
     * @param <T> the type of the value produced by the future
     * @return the result value
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
                        if (ex != null) {
                            LOGGER.warn(KeyValueLogMessage.of("error reading sample key",
                                            LogMessageKeys.KEY, ByteArrayUtil2.loggable(key)),
                                    ex);
                        }
                        return null;
                    });
            addCommitCheck(future);
        }
    }

    // Similar things save the context at Executor.execute(Runnable) time and restore it at Runnable.run() time.
    // That does not work well here, where the originating event is from the common FDB network thread.
    // Instead, restore it from transaction begin time, which captures context reasonably well.

    protected static Executor initExecutor(@Nonnull FDBDatabase fdb, @Nullable Map<String, String> mdcContext) {
        if (mdcContext == null) {
            return fdb.getExecutor();
        } else {
            return new ContextRestoringExecutor(fdb.getExecutor(), mdcContext);
        }
    }

    static class ContextRestoringExecutor implements Executor {
        @Nonnull
        private final Executor delegate;
        @Nonnull
        private final Map<String, String> mdcContext;

        public ContextRestoringExecutor(@Nonnull Executor delegate, @Nonnull Map<String, String> mdcContext) {
            this.delegate = delegate;
            this.mdcContext = mdcContext;
        }

        @Override
        public void execute(Runnable task) {
            if (!(task instanceof ContextRestoringRunnable)) {
                task = new ContextRestoringRunnable(task, mdcContext);
            }
            delegate.execute(task);
        }

        @Nonnull
        public Map<String, String> getMdcContext() {
            return mdcContext;
        }
    }

    static class ContextRestoringRunnable implements Runnable {
        private final Runnable delegate;
        private final Map<String, String> mdcContext;

        public ContextRestoringRunnable(@Nonnull Runnable delegate, @Nonnull Map<String, String> mdcContext) {
            this.delegate = delegate;
            this.mdcContext = mdcContext;
        }

        @Override
        public void run() {
            try {
                restoreMdc(mdcContext);
                delegate.run();
            } finally {
                clearMdc(mdcContext);
            }
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

    static void restoreMdc(@Nonnull Map<String, String> mdcContext) {
        MDC.clear();
        for (Map.Entry<String, String> entry : mdcContext.entrySet()) {
            MDC.put(entry.getKey(), entry.getValue());
        }
    }

    static void clearMdc(@Nonnull Map<String, String> mdcContext) {
        for (String key : mdcContext.keySet()) {
            MDC.remove(key);
        }
    }

    /**
     * Get a new {@link FDBDatabaseRunner} that will run contexts similar to this one.
     * <ul>
     * <li>Same {@linkplain FDBDatabase database}</li>
     * <li>Same {@linkplain FDBStoreTimer timer}</li>
     * <li>Same {@linkplain #getMdcContext() MDC context}</li>
     * <li>Same {@linkplain FDBDatabase.WeakReadSemantics weak read semantics}</li>
     * </ul>
     * @return a new database runner based on this context
     */
    @Nonnull
    public FDBDatabaseRunner newRunner() {
        FDBDatabaseRunner runner = database.newRunner();
        runner.setTimer(timer);
        runner.setMdcContext(getMdcContext());
        runner.setWeakReadSemantics(weakReadSemantics);
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
     * Register that a specific primary key used a given local version.
     * This can then be retrieved from the context using {@link #getLocalVersion(Tuple) getLocalVersion}.
     * @param primaryKey key to associate with the local version
     * @param version the local version of the key
     */
    public void addToLocalVersionCache(@Nonnull Tuple primaryKey, int version) {
        localVersionCache.put(primaryKey, version);
    }

    /**
     * Remove the local version associated with a single primary key.
     *
     * @param primaryKey the key associated with the local version being cleared
     * @return whether the key was already in the local version cache
     */
    public boolean removeLocalVersion(@Nonnull Tuple primaryKey) {
        return localVersionCache.remove(primaryKey) != null;
    }

    /**
     * Get a local version assigned to some primary key used within this context.
     * If the key has not been associated with any version using
     * {@link #addToLocalVersionCache(Tuple, int) addToLocalVersion}, then this
     * will return an unset {@link Optional}.
     * @param primaryKey key to retrieve the local version of
     * @return the associated version or an unset {@link Optional}
     */
    @Nonnull
    public Optional<Integer> getLocalVersion(@Nonnull Tuple primaryKey) {
        return Optional.ofNullable(localVersionCache.get(primaryKey));
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

    public FDBDatabase.WeakReadSemantics getWeakReadSemantics() {
        return weakReadSemantics;
    }

    public void setHookForAsyncToSync(@Nonnull Consumer<FDBStoreTimer.Wait> hook) {
        this.hookForAsyncToSync = hook;
    }

    public boolean hasHookForAsyncToSync() {
        return hookForAsyncToSync != null;
    }
}
