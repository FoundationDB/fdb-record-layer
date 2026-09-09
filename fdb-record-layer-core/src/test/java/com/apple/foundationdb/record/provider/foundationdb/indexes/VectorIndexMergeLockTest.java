/*
 * VectorIndexMergeLockTest.java
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

import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link VectorIndexMergeLock}'s lease semantics — acquire, refuse-while-live, steal-once-stale, and
 * release — driven by an injected clock so expiry is exercised deterministically without sleeping. The whole exchange
 * runs inside a single transaction (read-your-writes makes an uncommitted lease visible to a subsequent read), which is
 * enough to test the acquire/steal/release logic; cross-transaction visibility is FDB's own guarantee.
 */
class VectorIndexMergeLockTest extends VectorIndexTestBase {
    private static final long WINDOW_MILLIS = 10_000L;

    @Nonnull
    @Override
    protected Map<String, String> indexOptions() {
        return ImmutableMap.of(
                IndexOptions.VECTOR_ENGINE, VectorIndexEngineKind.GUARDIANN.name(),
                IndexOptions.VECTOR_METRIC, Metric.EUCLIDEAN_METRIC.name(),
                IndexOptions.VECTOR_NUM_DIMENSIONS, "128");
    }

    @Test
    void leaseAcquireRefuseStealRelease() throws Exception {
        final UUID ownerA = UUID.randomUUID();
        final UUID ownerB = UUID.randomUUID();
        final AtomicLong clock = new AtomicLong(1_000_000L);
        final Tuple prefix = Tuple.from(); // an unpartitioned/empty prefix keys at the subspace itself

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addUngroupedVectorIndex);
            final Subspace secondary =
                    recordStore.indexSecondarySubspace(recordStore.getRecordMetaData().getIndex("UngroupedVectorIndex"));
            final VectorIndexMergeLock lockA = new VectorIndexMergeLock(secondary, ownerA, WINDOW_MILLIS, clock::get);
            final VectorIndexMergeLock lockB = new VectorIndexMergeLock(secondary, ownerB, WINDOW_MILLIS, clock::get);

            // Free to start.
            assertThat(lockA.currentOwner(context, prefix).get()).as("no lease yet").isNull();

            // A claims it.
            lockA.acquire(context, prefix);
            assertThat(lockA.currentOwner(context, prefix).get()).isEqualTo(ownerA);
            // B sees A's live lease and is refused (owner is A, not free).
            assertThat(lockB.currentOwner(context, prefix).get())
                    .as("B must see A's live lease").isEqualTo(ownerA);

            // Time passes but stays within the window: still A's, still refused to B.
            clock.addAndGet(WINDOW_MILLIS - 1);
            assertThat(lockB.currentOwner(context, prefix).get()).isEqualTo(ownerA);

            // Past the window: A's lease is stale, so the prefix reads as free and B may steal it.
            clock.addAndGet(2);
            assertThat(lockB.currentOwner(context, prefix).get()).as("stale lease reads as free").isNull();
            lockB.acquire(context, prefix);
            assertThat(lockB.currentOwner(context, prefix).get()).isEqualTo(ownerB);

            // A can no longer release it (it is B's now); the lease stays B's.
            lockA.release(context, prefix).get();
            assertThat(lockB.currentOwner(context, prefix).get())
                    .as("A must not release a lease it no longer owns").isEqualTo(ownerB);

            // B releases its own lease, leaving the prefix free.
            lockB.release(context, prefix).get();
            assertThat(lockB.currentOwner(context, prefix).get()).as("released -> free").isNull();
        }
    }

    /**
     * The refresh flow: re-{@link VectorIndexMergeLock#acquire acquiring} a lease this owner already holds re-stamps it
     * with a fresh timestamp — which is exactly what {@code drainOwnedPrefix} does each invocation to keep its lease
     * live while it drains. So a holder that keeps refreshing never loses its prefix even as real time marches past the
     * original lease window; and the refresh is not permanent — once a full window elapses since the <em>last</em>
     * refresh, the lease reads stale again and another owner may steal it.
     */
    @Test
    void refreshExtendsTheLeasePastItsOriginalWindow() throws Exception {
        final UUID ownerA = UUID.randomUUID();
        final UUID ownerB = UUID.randomUUID();
        final AtomicLong clock = new AtomicLong(1_000_000L);
        final Tuple prefix = Tuple.from();

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addUngroupedVectorIndex);
            final Subspace secondary =
                    recordStore.indexSecondarySubspace(recordStore.getRecordMetaData().getIndex("UngroupedVectorIndex"));
            final VectorIndexMergeLock lockA = new VectorIndexMergeLock(secondary, ownerA, WINDOW_MILLIS, clock::get);
            final VectorIndexMergeLock lockB = new VectorIndexMergeLock(secondary, ownerB, WINDOW_MILLIS, clock::get);

            // A claims the prefix; the lease is stamped with the current time.
            final long acquiredAt = clock.get();
            lockA.acquire(context, prefix);
            assertThat(lockA.currentOwner(context, prefix).get()).isEqualTo(ownerA);

            // Still within the window, A refreshes: acquire() re-stamps the lease with the now-later time.
            clock.addAndGet(WINDOW_MILLIS / 2);
            lockA.acquire(context, prefix);
            assertThat(lockA.currentOwner(context, prefix).get())
                    .as("a refresh by the owner keeps the lease its own").isEqualTo(ownerA);

            // Advance past the ORIGINAL expiry but not a full window past the refresh: the lease is still live because
            // the refresh moved its timestamp forward, so B is still refused. This is the property the steal test lacks.
            clock.addAndGet(WINDOW_MILLIS - 1);
            assertThat(clock.get())
                    .as("we are past when the un-refreshed lease would have expired")
                    .isGreaterThan(acquiredAt + WINDOW_MILLIS);
            assertThat(lockB.currentOwner(context, prefix).get())
                    .as("the refreshed lease outlives its original window, so B still sees A").isEqualTo(ownerA);

            // A full window after the last refresh, the lease finally reads stale and B may steal it.
            clock.addAndGet(WINDOW_MILLIS);
            assertThat(lockB.currentOwner(context, prefix).get())
                    .as("a window after the last refresh, the lease is stale/free again").isNull();
        }
    }

    /**
     * The dangerous ordering the delete-guard exists for: a {@code deleteWhere} commits first, then a merge claim's
     * blind {@code acquire} tries to commit. The claim's read-conflict on the (index-wide) guard — which
     * {@code deleteWhere} write-conflicts — must abort it, so it cannot orphan a lease into the just-emptied group. The
     * claim pins its read version first, exactly as the real claim invocation does via its snapshot scans.
     */
    @Test
    void claimConflictsWithConcurrentDeleteWhere() throws Exception {
        final Subspace secondary = secondarySubspace();
        final Tuple prefix = Tuple.from();
        try (FDBRecordContext claim = openContext();
                FDBRecordContext delete = openContext()) {
            claim.getReadVersion();
            newLock(secondary).acquire(claim, prefix);

            VectorIndexMergeLock.addDeleteWhereConflicts(delete.ensureActive(), secondary, prefix);
            delete.commit();

            assertThatThrownBy(claim::commit)
                    .as("a merge claim must abort when a concurrent deleteWhere commits first")
                    .satisfies(e -> assertThat(FDBExceptions.isOrHasCause(e,
                            FDBExceptions.FDBStoreTransactionConflictException.class)).isTrue());
        }
    }

    /**
     * Two concurrent claims must NOT conflict with each other: {@code acquire} only ever reads the guard (never writes
     * it) and the lease writes are blind last-writer-wins, so both commit — preserving the design where concurrent
     * merges spread across prefixes without contending.
     */
    @Test
    void concurrentClaimsDoNotConflict() throws Exception {
        final Subspace secondary = secondarySubspace();
        final Tuple prefix = Tuple.from();
        try (FDBRecordContext claimA = openContext();
                FDBRecordContext claimB = openContext()) {
            claimA.getReadVersion();
            claimB.getReadVersion();
            newLock(secondary).acquire(claimA, prefix);
            newLock(secondary).acquire(claimB, prefix);
            claimB.commit();
            claimA.commit(); // must not conflict with claimB's concurrent claim
        }
    }

    /**
     * The other ordering: a claim commits first, then {@code deleteWhere}. The delete's lease clear must wipe the lease
     * the earlier claim wrote, so no orphan survives.
     */
    @Test
    void deleteWhereWipesAnEarlierClaimLease() throws Exception {
        final Subspace secondary = secondarySubspace();
        final Tuple prefix = Tuple.from();
        try (FDBRecordContext claim = openContext()) {
            newLock(secondary).acquire(claim, prefix);
            claim.commit();
        }
        try (FDBRecordContext delete = openContext()) {
            VectorIndexMergeLock.addDeleteWhereConflicts(delete.ensureActive(), secondary, prefix);
            delete.commit();
        }
        try (FDBRecordContext check = openContext()) {
            assertThat(newLock(secondary).currentOwner(check, prefix).get())
                    .as("deleteWhere must clear a lease an earlier claim wrote").isNull();
        }
    }

    @Nonnull
    private Subspace secondarySubspace() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addUngroupedVectorIndex);
            return recordStore.indexSecondarySubspace(
                    recordStore.getRecordMetaData().getIndex("UngroupedVectorIndex"));
        }
    }

    @Nonnull
    private static VectorIndexMergeLock newLock(@Nonnull final Subspace secondary) {
        return new VectorIndexMergeLock(secondary, UUID.randomUUID(), WINDOW_MILLIS, System::currentTimeMillis);
    }
}
