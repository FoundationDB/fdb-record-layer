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

package com.apple.foundationdb.record.provider.foundationdb.synchronizedsession;

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.synchronizedsession.SynchronizedSessionLockedException;
import com.apple.foundationdb.tuple.ByteArrayUtil;
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
 * <p>
 * {@link SynchronizedSession} is a concept introduced to avoid multiple attempts (an attempt contains multiple
 * transactions running concurrently and/or consecutively) working together. Each attempt corresponds to a session
 * identified by a session ID. Of the sessions with the same lock subspace, only the one holds the lock is allowed to
 * work.
 * </p>
 * <p>
 * Each session should and should only try to acquire the lock when the session is initialized.
 * </p>
 * <p>
 * When a session holds the lock, it is protected to hold it for an extended length of time (a.k.a lease). Another new
 * session can only take lock if the lease of the original lock owner is outdated. (Note a session is allowed to work
 * even if its lease is outdated, as long as no other session takes its lock.) In order to keep the lease, every time a
 * session is used, it needs to update the lease's end time to something a period (configured by
 * {@code leaseLengthMillis}) later than current time.
 * </p>
 * <p>
 * If a session is not able to acquire the lock during the initialization or lost the lock during work later, it will
 * get a {@link SynchronizedSessionLockedException}. The session is considered ended when it gets a such exception. It
 * can neither try to acquire the lock again nor commit any work.
 * </p>
 * <p>
 * {@link #initializeSessionAsync} should be used when initializing a session to acquire the lock, while
 * {@link #checkLockAsync(Transaction)} and {@link #updateLockSessionLeaseEndTime(Transaction)} should be used in every
 * other transactions to check the lock and keep the lease. Please refer to {@link SynchronizedSessionRunner} for an
 * example of using {@link SynchronizedSession} in practice.
 * </p>
 * <p>
 * TODO: This class should be moved to {@link com.apple.foundationdb.synchronizedsession} in {@code fdb-extensions}
 * after {@code org.slf4j} is added as a dependency in next minor version.
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
     * Construct a session. Remember to {@link #initializeSessionAsync(Transaction)} if the {@code sessionId} is newly
     * generated.
     * @param lockSubspace the lock for which this session contends
     * @param sessionId session ID
     * @param leaseLengthMillis length between last access and lease's end time in milliseconds
     */
    public SynchronizedSession(@Nonnull Subspace lockSubspace, @Nonnull UUID sessionId, long leaseLengthMillis) {
        this.lockSubspace = lockSubspace;
        this.sessionId = sessionId;
        this.leaseLengthMillis = leaseLengthMillis;
        lockSessionIdSubspaceKey = lockSubspace.subspace(Tuple.from(LOCK_SESSION_ID_KEY)).pack();
        lockSessionLeaseEndTimeSubspaceKey = lockSubspace.subspace(Tuple.from(LOCK_SESSION_TIME_KEY)).pack();
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
     * End the session by releasing the lock if it still holds the lock. Do thing otherwise.
     * @param tr transaction to use
     * @return a future that will return {@code null} when the lock is no longer this session
     */
    public CompletableFuture<Void> releaseLock(@Nonnull Transaction tr) {
        return getLockSessionId(tr).thenApply(lockSessionId -> {
            if (sessionId.equals(lockSessionId)) {
                tr.clear(lockSubspace.pack(), ByteArrayUtil.strinc(lockSubspace.pack()));
            }
            return null;
        });
    }

    private CompletableFuture<UUID> getLockSessionId(@Nonnull Transaction tr) {
        return tr.get(lockSessionIdSubspaceKey)
                .thenApply(value -> value == null ? null : Tuple.fromBytes(value).getUUID(0));
    }

    private void setLockSessionId(@Nonnull Transaction tr) {
        tr.set(lockSessionIdSubspaceKey, Tuple.from(sessionId).pack());
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
        return tr.get(lockSessionLeaseEndTimeSubspaceKey)
                .thenApply(value -> value == null ? null : Tuple.fromBytes(value).getLong(0));
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
