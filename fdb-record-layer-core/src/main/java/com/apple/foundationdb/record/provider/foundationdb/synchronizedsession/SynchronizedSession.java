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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.ByteArrayUtil2;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

@API(API.Status.EXPERIMENTAL)
class SynchronizedSession {
    @Nonnull
    private Subspace lockSubspace;
    @Nonnull
    private UUID sessionId;
    private long leaseLengthMill;

    @Nonnull
    private final byte[] lockSessionIdSubspaceKey;
    @Nonnull
    private final byte[] lockSessionTimeSubspaceKey;

    // The UUID of the session owning the lock.
    private static final Object LOCK_SESSION_ID_KEY = 0L;
    // The time point after which the lock can be taken by others.
    private static final Object LOCK_SESSION_TIME_KEY = 1L;

    SynchronizedSession(@Nonnull Subspace lockSubspace, @Nonnull UUID sessionId, long leaseLengthMill) {
        this.lockSubspace = lockSubspace;
        this.sessionId = sessionId;
        this.leaseLengthMill = leaseLengthMill;
        lockSessionIdSubspaceKey = lockSubspace.subspace(Tuple.from(LOCK_SESSION_ID_KEY)).pack();
        lockSessionTimeSubspaceKey = lockSubspace.subspace(Tuple.from(LOCK_SESSION_TIME_KEY)).pack();
    }

    // Take the lock when the session is initialized.
    CompletableFuture<Void> initializeSession(@Nonnull FDBRecordContext context) {
        Transaction tr = context.ensureActive();
        return getLockSessionId(tr).thenCompose(lockSessionId -> {
            if (lockSessionId == null) {
                // If there was no lock, can get the lock.
                return takeSessionLock(tr);
            } else if (lockSessionId.equals(sessionId)) {
                // This should never happen.
                throw new RecordCoreException("session id already exists in subspace")
                        .addLogInfo(LogMessageKeys.SUBSPACE_KEY, ByteArrayUtil2.loggable(lockSubspace.getKey()))
                        .addLogInfo(LogMessageKeys.UUID, sessionId);
            } else {
                // This is snapshot read so it will not affect all working transactions writing to it.
                return getLockSessionTime(tr.snapshot()).thenCompose(sessionTime -> {
                    long currentTime = System.currentTimeMillis();
                    if (sessionTime < currentTime) {
                        // The old lease was outdated, can get the lock.
                        return takeSessionLock(tr);
                    } else {
                        throw new SynchronizedSessionExpiredException("Failed to initialize the session")
                                .addLogInfo(LogMessageKeys.SUBSPACE_KEY, ByteArrayUtil2.loggable(lockSubspace.getKey()))
                                .addLogInfo(LogMessageKeys.UUID, sessionId);
                    }
                });
            }
        });
    }

    private CompletionStage<Void> takeSessionLock(Transaction tr) {
        setLockSessionId(tr);
        setLockSessionTime(tr);
        return AsyncUtil.DONE;
    }

    // Check and renew the lock when the session in being used.
    // TODO: Maybe the time should be updated even if the work is failed. For example, online indexer may fail
    // in the first a few transactions to find the optimal number of records to scan in one transcation.
    <T> Function<FDBRecordContext, T> workInSession(
            @Nonnull Function<? super FDBRecordContext, ? extends T> work) {
        return context -> {
            context.asyncToSync(FDBStoreTimer.Waits.WAIT_CHECK_SYNC_SESSION, checkLock(context));
            T result = work.apply(context);
            setLockSessionTime(context.ensureActive());
            return result;
        };
    }

    <T> Function<? super FDBRecordContext, CompletableFuture<? extends T>> workInSessionAsync(
            @Nonnull Function<? super FDBRecordContext, CompletableFuture<? extends T>> work) {
        return context -> checkLock(context)
                .thenCompose(vignore -> work.apply(context))
                .thenApply(result -> {
                    setLockSessionTime(context.ensureActive());
                    return result;
                });
    }

    @Nonnull
    UUID getSessionId() {
        return sessionId;
    }

    private CompletableFuture<Void> checkLock(FDBRecordContext context) {
        return getLockSessionId(context.ensureActive())
                .thenCompose(lockSessionId -> {
                    if (!sessionId.equals(lockSessionId)) { // sessionId is nonnull while lockSessionId is nullable
                        throw new SynchronizedSessionExpiredException("Failed to continue the session")
                                .addLogInfo(LogMessageKeys.SUBSPACE_KEY, ByteArrayUtil2.loggable(lockSubspace.getKey()))
                                .addLogInfo(LogMessageKeys.UUID, sessionId)
                                .addLogInfo("lockSessionId", lockSessionId);
                    }
                    return AsyncUtil.DONE;
                });
    }

    void close(FDBRecordContext context) {
        context.ensureActive().clear(lockSubspace.pack(), ByteArrayUtil.strinc(lockSubspace.pack()));
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
        return tr.get(lockSessionTimeSubspaceKey)
                .thenApply(value -> Tuple.fromBytes(value).getLong(0));
    }

    private void setLockSessionTime(@Nonnull Transaction tr) {
        long leaseEndTime = System.currentTimeMillis() + leaseLengthMill;
        // Use BYTE_MAX rather than MAX because `Tuple`s write their integers in big Endian.
        tr.mutate(MutationType.BYTE_MAX, lockSessionTimeSubspaceKey, Tuple.from(leaseEndTime).pack());
    }
}
