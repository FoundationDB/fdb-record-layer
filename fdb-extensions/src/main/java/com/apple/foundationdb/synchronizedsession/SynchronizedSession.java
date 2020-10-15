/*
 * SynchronizedSession.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.synchronizedsession;

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.util.LogMessageKeys;
import com.apple.foundationdb.util.LoggableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * A {@code SynchronizedSession} is a concept introduced to avoid multiple attempts at performing the same operation
 * (with each attempt opening multiple transactions running concurrently and/or consecutively) from running concurrently
 * and contending for resources. Each attempt corresponds to a session identified by a session ID. Of the sessions with
 * the same lock subspace, only the one holding the lock is allowed to work.
 * <p>
 * Each session should and should only try to acquire the lock when the session is initialized.
 * </p>
 * <p>
 * When a session holds the lock, it is protected from other sessions grabbing the same lock for an extended length of
 * time (a.k.a lease). Another new session can only take lock if the lease of the original lock owner is outdated. (Note
 * a session is allowed to work even if its lease is outdated, as long as no other session takes its lock.) In order to
 * keep the lease, every time a session is used, it needs to update the lease's end time to some time (configured by
 * {@code leaseLengthMillis}) later than current time. (The lease time is used only as an optimization.
 * {@link SynchronizedSession} does not depend on synchronized clocks for the correctness of mutual exclusion.)
 * </p>
 * <p>
 * If a session is not able to acquire the lock during the initialization or loses the lock later, it will get a
 * {@link SynchronizedSessionLockedException}. The session is considered ended when it gets a such exception. It can
 * neither try to acquire the lock again nor commit any work.
 * </p>
 * <p>
 * {@link #initializeSessionAsync} should be used when initializing a session to acquire the lock, while
 * {@link #checkLockAsync(Transaction)} and {@link #updateLockSessionLeaseEndTime(Transaction)} should be used in every
 * other transactions to check the lock and keep the lease. Please refer to <code>SynchronizedSessionRunner</code> in
 * <code>fdb-record-layer-core</code> for an example of using {@link SynchronizedSession} in practice.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public class SynchronizedSession {
    private static final Logger LOGGER = LoggerFactory.getLogger(SynchronizedSession.class);

    @Nonnull
    private Subspace lockSubspace;
    @Nonnull
    private UUID sessionId;
    private long leaseLengthMillis;

    // The UUID stored here indicates which session holds the lock.
    @Nonnull
    private final byte[] lockSessionIdSubspaceKey;
    // The timestamp stored here indicates the session above holds the lock until which time if the lease is not renewed.
    @Nonnull
    private final byte[] lockSessionLeaseEndTimeSubspaceKey;

    // The UUID of the session owning the lock.
    private static final Object LOCK_SESSION_ID_KEY = 0L;
    // The time point after which the lock can be taken by others.
    private static final Object LOCK_SESSION_TIME_KEY = 1L;

    /**
     * Construct a session. Remember to call {@link #initializeSessionAsync(Transaction)} if the {@code sessionId} is
     * newly generated.
     * @param lockSubspace the lock for which this session contends
     * @param sessionId session ID
     * @param leaseLengthMillis length between last access and lease's end time in milliseconds
     */
    public SynchronizedSession(@Nonnull Subspace lockSubspace, @Nonnull UUID sessionId, long leaseLengthMillis) {
        this.lockSubspace = lockSubspace;
        this.sessionId = sessionId;
        this.leaseLengthMillis = leaseLengthMillis;
        lockSessionIdSubspaceKey = lockSessionIdSubspaceKey(lockSubspace);
        lockSessionLeaseEndTimeSubspaceKey = lockSessionLeaseEndTimeSubspaceKey(lockSubspace);
    }

    private static byte[] lockSessionIdSubspaceKey(@Nonnull Subspace lockSubspace) {
        return lockSubspace.subspace(Tuple.from(LOCK_SESSION_ID_KEY)).pack();
    }

    private static byte[] lockSessionLeaseEndTimeSubspaceKey(@Nonnull Subspace lockSubspace) {
        return lockSubspace.subspace(Tuple.from(LOCK_SESSION_TIME_KEY)).pack();
    }

    /**
     * Initialize the session by acquiring the lock. This should be invoked before a new session is ever used.
     * @param tr transaction to use
     * @return a future that will return {@code null} when the session is initialized
     */
    public CompletableFuture<Void> initializeSessionAsync(@Nonnull Transaction tr) {
        // Though sessionTime is not necessarily needed in some cases, it's read in parallel with the lockSessionId read
        // in the hope of that the FDB client then batches those two operations together into a single request.
        return getLockSessionId(tr).thenAcceptBoth(getLockSessionTime(tr.snapshot()), (lockSessionId, sessionTime) -> {
            if (lockSessionId == null) {
                // If there was no lock, can get the lock.
                takeSessionLock(tr);
            } else if (lockSessionId.equals(sessionId)) {
                // This should never happen.
                throw new LoggableException("session id already exists in subspace")
                        .addLogInfo(LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(lockSubspace.getKey()))
                        .addLogInfo(LogMessageKeys.SESSION_ID, sessionId);
            } else {
                if (sessionTime == null) {
                    LOGGER.warn("Session ID is set but session time is not",
                            LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(lockSubspace.getKey()),
                            LogMessageKeys.SESSION_ID, sessionId);
                    // This is unexpected, but if it does occur, we may want to correct it by letting the new session
                    // to take the lock.
                    takeSessionLock(tr);
                } else if (sessionTime < System.currentTimeMillis()) {
                    // The old lease was outdated, can get the lock.
                    takeSessionLock(tr);
                } else {
                    throw new SynchronizedSessionLockedException("Failed to initialize the session because of an existing session in progress")
                            .addLogInfo(LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(lockSubspace.getKey()))
                            .addLogInfo(LogMessageKeys.SESSION_ID, sessionId)
                            .addLogInfo(LogMessageKeys.EXISTING_SESSION, lockSessionId)
                            .addLogInfo(LogMessageKeys.EXISTING_SESSION_EXPIRE_TIME, sessionTime);
                }
            }
        });
    }

    private void takeSessionLock(@Nonnull Transaction tr) {
        setLockSessionId(tr);
        updateLockSessionLeaseEndTime(tr);
    }

    /**
     * Get session ID.
     * @return session ID
     */
    @Nonnull
    public UUID getSessionId() {
        return sessionId;
    }

    /**
     * Check if the session still holds the lock. This should be invoked in every transaction in the session to follow
     * the contract.
     * @param tr transaction to use
     * @return a future that will return {@code null} when the lock is checked
     */
    public CompletableFuture<Void> checkLockAsync(@Nonnull Transaction tr) {
        return getLockSessionId(tr)
                .thenCompose(lockSessionId -> {
                    if (!sessionId.equals(lockSessionId)) { // Note sessionId is nonnull and lockSessionId is nullable.
                        throw new SynchronizedSessionLockedException("Failed to continue the session")
                                .addLogInfo(LogMessageKeys.SUBSPACE, ByteArrayUtil2.loggable(lockSubspace.getKey()))
                                .addLogInfo(LogMessageKeys.SESSION_ID, sessionId)
                                .addLogInfo(LogMessageKeys.EXISTING_SESSION, lockSessionId);
                    }
                    return AsyncUtil.DONE;
                });
    }

    /**
     * End the session by releasing the lock if it still holds the lock. Do nothing otherwise.
     * @param tr transaction to use
     * @return a future that will return {@code null} when the lock is no longer this session
     */
    public CompletableFuture<Void> releaseLock(@Nonnull Transaction tr) {
        return getLockSessionId(tr).thenApply(lockSessionId -> {
            if (sessionId.equals(lockSessionId)) {
                tr.clear(lockSubspace.range());
            }
            return null;
        });
    }

    /**
     * End any active session on the lock subspace by releasing the lock no matter whether this session holds the lock
     * or not.
     * <p>
     * It only takes place when the given transaction is committed. It will only be enforced when the other processes
     * holding the lock go to check the lease in later transactions, where they will fail with
     * {@link SynchronizedSessionLockedException}.
     * </p>
     * @param tr transaction to use
     */
    public void endAnySession(@Nonnull Transaction tr) {
        endAnySession(tr, lockSubspace);
    }

    /**
     * End any active session on the given lock subspace by releasing the lock.
     * <p>
     * It only takes place when the given transaction is committed. It will only be enforced when the other processes
     * holding the lock go to check the lease in later transactions, where they will fail with
     * {@link SynchronizedSessionLockedException}.
     * </p>
     * @param tr transaction to use
     * @param lockSubspace the lock whose active session needs to be ended
     */
    public static void endAnySession(@Nonnull Transaction tr, @Nonnull Subspace lockSubspace) {
        tr.clear(lockSubspace.range());
    }


    /**
     * Check if there is any active session on the given lock subspace, so that a new session would not able to be initialized.
     * @param tr transaction to use
     * @param lockSubspace the lock whose active session needs to be checked
     * @return {@code true} if there is any active session, otherwise {@code false}
     */
    public static CompletableFuture<Boolean> checkAnySession(@Nonnull Transaction tr, @Nonnull Subspace lockSubspace) {
        // It is false in the situations where initializeSessionAsync of a new session ID would takeSessionLock.
        return getLockSessionId(tr, lockSubspace).thenCombineAsync(getLockSessionTime(tr.snapshot(), lockSubspace), (lockSessionId, sessionTime) -> {
            if (lockSessionId == null) {
                return false;
            } else {
                if (sessionTime == null) {
                    return false;
                } else if (sessionTime < System.currentTimeMillis()) {
                    return false;
                } else {
                    return true;
                }
            }
        });
    }

    private static CompletableFuture<UUID> getLockSessionId(@Nonnull Transaction tr, @Nonnull Subspace lockSubspace) {
        return tr.get(lockSessionIdSubspaceKey(lockSubspace))
                .thenApply(value -> value == null ? null : Tuple.fromBytes(value).getUUID(0));
    }

    private CompletableFuture<UUID> getLockSessionId(@Nonnull Transaction tr) {
        return getLockSessionId(tr, lockSubspace);
    }

    private void setLockSessionId(@Nonnull Transaction tr) {
        tr.set(lockSessionIdSubspaceKey, Tuple.from(sessionId).pack());
    }

    private static CompletableFuture<Long> getLockSessionTime(@Nonnull ReadTransaction tr, @Nonnull Subspace lockSubspace) {
        return tr.get(lockSessionLeaseEndTimeSubspaceKey(lockSubspace))
                .thenApply(value -> value == null ? null : Tuple.fromBytes(value).getLong(0));
    }

    // There may be multiple threads working in a same session, in which case the session time is being written
    // frequently. To avoid unnecessary races:
    // - The session time should not be read while working in the session, so that all work transactions can write to
    //   it blindly and not conflict with each other.
    // - The session time should be updated to the max value when being updated by concurrent transactions, so that the
    //   final value comes not from whoever gets committed last but whoever writes the largest value
    // - When the session time is read during session initialization, it should be a snapshot read so it will not have
    //   conflicts with working transactions (which write to session time).
    private CompletableFuture<Long> getLockSessionTime(@Nonnull ReadTransaction tr) {
        return getLockSessionTime(tr, lockSubspace);
    }

    /**
     * Update the lease's end time. This should be invoked in every transaction in the session to keep the session
     * alive.
     * @param tr transaction to use
     */
    public void updateLockSessionLeaseEndTime(@Nonnull Transaction tr) {
        long leaseEndTime = System.currentTimeMillis() + leaseLengthMillis;
        // Use BYTE_MAX rather than MAX because `Tuple`s write their integers in big Endian.
        tr.mutate(MutationType.BYTE_MAX, lockSessionLeaseEndTimeSubspaceKey, Tuple.from(leaseEndTime).pack());
    }
}
