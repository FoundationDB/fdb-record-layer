/*
 * VectorIndexMergeLock.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;

/**
 * A per-partition-prefix lease that lets concurrent vector-index merges divide the partitions among themselves instead
 * of colliding: a merge claims a prefix by writing a lease, and other merges that see a live lease skip that prefix
 * rather than racing into the same (expensive) drain and rolling one back. Modeled on the Lucene directory file lock
 * (a committed {@code (ownerId, timestamp)} value with window-based expiry and steal-if-stale), but with no Lucene or
 * {@code AgilityContext} dependency.
 * <p>
 * The lease lives in the index's <em>secondary</em> subspace under {@link #MERGE_LOCK_DISCRIMINATOR}, keyed by the
 * partition prefix (empty for an unpartitioned index), alongside {@link VectorIndexTaskCounts}. It is a coordination
 * hint, not the correctness mechanism: double-drain is still prevented by FDB conflict detection on the task queue.
 * The two operations are deliberately split across transactions — a merge invocation <em>claims</em> a prefix (a cheap
 * blind write) and returns so the claim commits and becomes visible; only a later invocation, after re-reading and
 * confirming it is still the owner, performs the expensive drain. Because a drain requires a pre-committed, re-verified
 * lease, and only one owner id can be the committed value, no two merges drain the same prefix at once.
 * <p>
 * The owner id must be stable across a merge run's re-invocations (each is a fresh transaction and maintainer) so a
 * process recognizes its own lease; it comes from the indexing session (see
 * {@code IndexDeferredMaintenanceControl.getMergeSessionId()}). A crashed holder stops refreshing its lease; once the
 * lease ages past {@code leaseWindowMillis} it reads as free and another process (or a future run) can reclaim it.
 * <p>
 * One correctness concern the lease itself does carry is against a concurrent {@code deleteWhere}: a blind lease
 * {@link #acquire} committing after a {@code deleteWhere} emptied the group would orphan a lease into it. A single,
 * index-wide <em>delete-guard</em> key closes this — {@code acquire} read-conflicts it and
 * {@link #addDeleteWhereConflicts deleteWhere} write-conflicts it (plus clears any lease under the deleted prefix), so
 * a claim racing a delete aborts while acquires still never conflict with each other. See those two methods.
 */
final class VectorIndexMergeLock {
    private static final String MERGE_LOCK_DISCRIMINATOR = "mergeLock";
    /** Discriminator for the single, index-wide delete-guard conflict key (see {@link #addDeleteWhereConflicts}). */
    private static final String DELETE_GUARD_DISCRIMINATOR = "mergeLockDeleteGuard";
    /** Default lease window: comfortably longer than the gap between a holder's merge invocations (incl. driver
     * back-off), short enough to reclaim a crashed holder's prefix promptly. */
    static final long DEFAULT_LEASE_WINDOW_MILLIS = 60_000L;

    @Nonnull
    private final Subspace lockSubspace;
    @Nonnull
    private final byte[] deleteGuardKey;
    @Nonnull
    private final UUID ownerId;
    private final long leaseWindowMillis;
    @Nonnull
    private final LongSupplier clock;

    VectorIndexMergeLock(@Nonnull final Subspace indexSecondarySubspace, @Nonnull final UUID ownerId,
                         final long leaseWindowMillis, @Nonnull final LongSupplier clock) {
        this.lockSubspace = indexSecondarySubspace.subspace(Tuple.from(MERGE_LOCK_DISCRIMINATOR));
        this.deleteGuardKey = deleteGuardKeyFor(indexSecondarySubspace);
        this.ownerId = ownerId;
        this.leaseWindowMillis = leaseWindowMillis;
        this.clock = clock;
    }

    /**
     * Snapshot-reads the current <em>live</em> owner of {@code prefix}'s lease, or {@code null} if the prefix is
     * claimable — either no lease exists, or the lease has aged out of the {@code [now - window, now + window]} band
     * (too old, or implausibly far in the future) and is treated as stale. The read is at snapshot isolation so the
     * skip decision adds no read conflicts.
     * @param context the merge context
     * @param prefix the partition prefix
     * @return a future of the live owner id, or {@code null} if the prefix is free/stale (claimable)
     */
    @Nonnull
    CompletableFuture<UUID> currentOwner(@Nonnull final FDBRecordContext context, @Nonnull final Tuple prefix) {
        return context.readTransaction(true).get(lockSubspace.pack(prefix))
                .thenApply(value -> {
                    if (value == null) {
                        return null;
                    }
                    final Tuple decoded = Tuple.fromBytes(value);
                    final long timestampMillis = decoded.getLong(1);
                    final long now = clock.getAsLong();
                    if (timestampMillis <= now - leaseWindowMillis || timestampMillis >= now + leaseWindowMillis) {
                        // Stale (holder stopped refreshing, or its clock is implausibly far off): treat as free.
                        return null;
                    }
                    return decoded.getUUID(0);
                });
    }

    /**
     * Claims (or refreshes) {@code prefix}'s lease for this owner with a fresh timestamp. A blind write: two racing
     * claimants both write cheaply (last-writer-wins) and neither drains, so the loser simply discovers on its next
     * invocation (its re-read no longer matches) that it is not the owner. The caller must have determined via
     * {@link #currentOwner} that the prefix is free/stale or already its own.
     * <p>
     * The lease write is deliberately blind — but we additionally register a read-conflict on the single, index-wide
     * delete-guard key so that this transaction (which, for a fresh claim, otherwise reads only at snapshot isolation
     * and would never conflict with anything) fails if a concurrent {@link #addDeleteWhereConflicts deleteWhere}
     * commits. That is the one thing that must abort a claim: writing a lease into a group a concurrent
     * {@code deleteWhere} just emptied would orphan it. Because only {@code deleteWhere} writes the guard and only
     * {@code acquire} reads it, two acquires never conflict with each other (both leave the guard unwritten) and a
     * claim never conflicts with a normal insert/delete (which touch {@code "taskCount"}/the partition, not the
     * guard) — so merges still spread across prefixes via blind, last-writer-wins lease writes.
     * @param context the merge context
     * @param prefix the partition prefix
     */
    @SuppressWarnings("PMD.CloseResource")
    void acquire(@Nonnull final FDBRecordContext context, @Nonnull final Tuple prefix) {
        final Transaction transaction = context.ensureActive();
        transaction.set(lockSubspace.pack(prefix), Tuple.from(ownerId, clock.getAsLong()).pack());
        transaction.addReadConflictKey(deleteGuardKey);
    }

    /**
     * Releases {@code prefix}'s lease if (and only if) it is still held by this owner. Called once the prefix's queue
     * is fully drained so another merge may pick it up immediately rather than waiting for expiry.
     * @param context the merge context
     * @param prefix the partition prefix
     * @return a future that completes when the release (if any) has been staged in the transaction
     */
    @Nonnull
    CompletableFuture<Void> release(@Nonnull final FDBRecordContext context, @Nonnull final Tuple prefix) {
        final byte[] key = lockSubspace.pack(prefix);
        return context.ensureActive().get(key).thenAccept(value -> {
            if (value != null && ownerId.equals(Tuple.fromBytes(value).getUUID(0))) {
                context.ensureActive().clear(key);
            }
        });
    }

    /**
     * The single, index-wide delete-guard key: a coordination coordinate (never materialized as data) that
     * {@code deleteWhere} write-conflicts and {@link #acquire} read-conflicts, so a merge claim aborts if it races a
     * {@code deleteWhere}. One key for the whole index (independent of prefix) keeps this trivially correct for a
     * partial grouping-prefix delete — no range covering the affected partitions is needed.
     */
    @Nonnull
    private static byte[] deleteGuardKeyFor(@Nonnull final Subspace indexSecondarySubspace) {
        return indexSecondarySubspace.pack(Tuple.from(DELETE_GUARD_DISCRIMINATOR));
    }

    /**
     * Registers, on a {@code deleteWhere} transaction, the conflicts that keep the merge lease consistent with the
     * group(s) being removed. Static because {@code deleteWhere} has no merge owner id / clock (it is not a merge).
     * Two parts, closing the two race orderings against a concurrent blind {@link #acquire}:
     * <ol>
     *   <li>Clear any lease under {@code prefix} ({@code "mergeLock"} discriminator), completing the secondary-subspace
     *       removal that {@code deleteWhere} already does for {@code "taskCount"} and the partition. This wipes a lease
     *       an {@code acquire} committed <em>before</em> this delete (last-writer-wins vs the blind lease {@code set}).</li>
     *   <li>Write-conflict the index-wide delete-guard key, which {@link #acquire} read-conflicts. This aborts an
     *       {@code acquire} that would otherwise commit <em>after</em> this delete and re-write a lease into the emptied
     *       group.</li>
     * </ol>
     * @param transaction the {@code deleteWhere} transaction
     * @param indexSecondarySubspace the index's secondary subspace (where the lease and guard live)
     * @param prefix the grouping prefix being deleted
     */
    static void addDeleteWhereConflicts(@Nonnull final Transaction transaction,
                                        @Nonnull final Subspace indexSecondarySubspace,
                                        @Nonnull final Tuple prefix) {
        final Subspace lockSubspace = indexSecondarySubspace.subspace(Tuple.from(MERGE_LOCK_DISCRIMINATOR));
        transaction.clear(Range.startsWith(lockSubspace.pack(prefix)));
        transaction.addWriteConflictKey(deleteGuardKeyFor(indexSecondarySubspace));
    }
}
