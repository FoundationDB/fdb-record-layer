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
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseRunner;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

@API(API.Status.EXPERIMENTAL)
public class SynchronizedSession {
    @Nonnull
    private Subspace lockSubspace;
    @Nonnull
    private FDBDatabaseRunner runner;

    @Nullable
    private UUID sessionId = null;

    // TODO: configurable?
    private static final long LEASE_PERIOD_MILL = 60_000;

    private static final Object LOCK_SESSION_ID_KEY = 0L;
    private static final Object LOCK_SESSION_TIME_KEY = 1L;

    public SynchronizedSession(@Nonnull Subspace lockSubspace, @Nonnull FDBDatabaseRunner runner) {
        this.lockSubspace = lockSubspace;
        this.runner = runner;
    }

    // A synchronized session keeps its look by updating timestamp in each of its transaction, so do not initialize the
    // session until it is being actively used.
    public SynchronizedSessionRunner initializeSessionAndCreateRunner() {
        boolean success = runner.run(context -> runner.asyncToSync(FDBStoreTimer.Waits.WAIT_INITIALIZE_SYNCHRONIZED_SESSION, initializeSession(context)));
        if (success) {
            return new SynchronizedSessionRunner(runner, this);
        } else {
            throw new SynchronizedSessionExpiredException("failed to initialize the session");
        }
    }

    public SynchronizedSessionRunner reuseSessionAndCreateRunner(@Nonnull UUID sessionId) {
        this.sessionId = sessionId;
        return new SynchronizedSessionRunner(runner, this);
    }

    // Return true if get lease otherwise false.
    private CompletableFuture<Boolean> initializeSession(@Nonnull FDBRecordContext context) {
        if (sessionId != null) {
            throw new RecordCoreException("SynchronizedSession has been initialized");
        }
        sessionId = UUID.randomUUID();
        Transaction tr = context.ensureActive();
        return getLockSessionId(tr).thenCompose(lockSessionId -> {
            if (lockSessionId == null) {
                // If there was no lock, can get the lock.
                return getSessionLock(tr);
            } else if (lockSessionId == sessionId) {
                // This should never happen.
                throw new RecordCoreException("session id already exists");
            } else {
                // This is snapshot read so it will not affect all working transactions writing to it.
                return getLockSessionTime(tr.snapshot()).thenCompose(sessionTime -> {
                    long currentTime = System.currentTimeMillis();
                    // TODO: Deal with clock skews.
                    if (sessionTime + LEASE_PERIOD_MILL < currentTime) {
                        // The old lease was outdated, can get the lock.
                        return getSessionLock(tr);
                    } else {
                        return AsyncUtil.READY_FALSE;
                    }
                });
            }
        });
    }

    <T> Function<FDBRecordContext, T> workInSession(
            @Nonnull Function<? super FDBRecordContext, ? extends T> work) {
        return context -> context.asyncToSync(FDBStoreTimer.Waits.WAIT_RETRY_DELAY,
                workInSessionAsync(ctx -> CompletableFuture.completedFuture(work.apply(ctx))).apply(context));
    }

    <T> Function<? super FDBRecordContext, CompletableFuture<? extends T>> workInSessionAsync(
            @Nonnull Function<? super FDBRecordContext, CompletableFuture<? extends T>> work) {
        return context -> getLockSessionId(context.ensureActive())
                .thenCompose(lockSessionId -> {
                    if (lockSessionId != sessionId) {
                        throw new SynchronizedSessionExpiredException("failed to continue the session");
                    }
                    return AsyncUtil.DONE;
                })
                .thenCompose(vignore -> work.apply(context))
                .thenApply(result -> {
                    setLockSessionTime(context.ensureActive());
                    return result;
                });
    }

    private CompletionStage<Boolean> getSessionLock(Transaction tr) {
        setLockSessionId(tr);
        setLockSessionTime(tr);
        return tr.commit().thenCompose(vignore -> AsyncUtil.READY_TRUE);
    }

    private CompletableFuture<UUID> getLockSessionId(@Nonnull Transaction tr) {
        final byte[] lockSessionIdSubspaceKey = lockSubspace.subspace(Tuple.from(LOCK_SESSION_ID_KEY)).pack();
        return tr.get(lockSessionIdSubspaceKey)
                .thenApply(value -> Tuple.fromBytes(value).getUUID(0));
    }

    private void setLockSessionId(@Nonnull Transaction tr) {
        final byte[] lockSessionIdSubspaceKey = lockSubspace.subspace(Tuple.from(LOCK_SESSION_ID_KEY)).pack();
        tr.set(lockSessionIdSubspaceKey, Tuple.from(sessionId).pack());
    }

    // There may be multiple threads working in a same session, in which case the session time is being wrote
    // frequently. To avoid unnecessary races:
    // - The session time should not be read during working in the session, so that all work transactions can write to
    //   it blindly and not conflict with each other.
    // - The session time should be updated to the max value when being updated by concurrent transactions, so that the
    //   final value comes from not whoever that gets committed that comes last but whoever writes the larger value
    // - When the session time is read during session initialization, it should be a snapshot read so it will not have
    //   conflicts with working transactions (which write to session time).
    private CompletableFuture<Long> getLockSessionTime(@Nonnull ReadTransaction tr) {
        final byte[] lockSessionTimeSubspaceKey = lockSubspace.subspace(Tuple.from(LOCK_SESSION_TIME_KEY)).pack();
        return tr.get(lockSessionTimeSubspaceKey)
                .thenApply(value -> Tuple.fromBytes(value).getLong(0));
    }

    private void setLockSessionTime(@Nonnull Transaction tr) {
        final byte[] lockSessionTimeSubspaceKey = lockSubspace.subspace(Tuple.from(LOCK_SESSION_TIME_KEY)).pack();
        long currentTime = System.currentTimeMillis();
        // The timestamp is stored as long so little-endian comparison should be used.
        tr.mutate(MutationType.MAX, lockSessionTimeSubspaceKey, Tuple.from(currentTime).pack());
    }



}
