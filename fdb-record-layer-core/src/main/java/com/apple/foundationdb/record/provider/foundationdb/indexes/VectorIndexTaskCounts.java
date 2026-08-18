/*
 * VectorIndexTaskCounts.java
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

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.CloseableAsyncIterator;
import com.apple.foundationdb.async.MoreAsyncUtil;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serial;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A conflict-free register of how much deferred maintenance work a (Guardiann) vector index has outstanding, so a merge
 * driver can find and gate that work without skip-scanning every partition.
 * <p>
 * The register lives in the index's <em>secondary</em> subspace (separate from the actual vector data structure
 * data in the primary subspace of the index which is partitioned by grouping prefix), and cleared with the index on
 * rebuild — see {@code FDBRecordStore.clearIndexData}). It is a single per-partition-prefix count under
 * {@link VectorIndexSecondarySubspaceKeys#TASK_COUNTS} — {@code prefix -> outstanding tasks} — whose key
 * is dropped the moment the count returns to zero, so the entries present are exactly the prefixes with work (an
 * unpartitioned index has just the single empty-prefix entry). "Is there any work?" is therefore simply "is that map
 * non-empty?", which needs no separate total counter.
 * <p>
 * The count is maintained with atomic {@link MutationType#ADD} mutations (so concurrent inserts/drains commute and never
 * conflict on the counter), paired with a {@link MutationType#COMPARE_AND_CLEAR} that removes a key once it hits zero
 * (also conflict-free), in the same transaction as the task enqueue/execute that moved it, and read with a snapshot (so
 * discovery adds no read conflicts). The task queue itself remains the source of truth; the count tracks it exactly
 * under normal operation, since a task's enqueue/execute and its counter mutation commit and roll back together.
 */
final class VectorIndexTaskCounts {
    private static final Logger logger = LoggerFactory.getLogger(VectorIndexTaskCounts.class);

    // The 8-byte little-endian encoding of zero — the same width the ADD counters use — as the COMPARE_AND_CLEAR
    // operand that drops a per-prefix entry the instant its count returns to zero.
    private static final byte[] ZERO_COUNT = new byte[Long.BYTES];

    @Nonnull
    private final Subspace perPrefixSubspace;

    VectorIndexTaskCounts(@Nonnull final Subspace indexSecondarySubspace) {
        this.perPrefixSubspace =
                indexSecondarySubspace.subspace(Tuple.from(VectorIndexSecondarySubspaceKeys.TASK_COUNTS));
    }

    /**
     * A handle bound to one partition prefix that the maintenance write listener uses to bump the counts as tasks are
     * enqueued and executed.
     * @param prefix the partition prefix (empty for an unpartitioned index)
     * @return a register handle for {@code prefix}
     */
    @Nonnull
    TaskCountRegister registerFor(@Nonnull final Tuple prefix) {
        return new TaskCountRegister(this, prefix);
    }

    /**
     * Adds {@code +1} to the prefix's count, in {@code transaction}.
     * @param transaction the transaction the enqueue happened in
     * @param prefix the partition prefix
     */
    void increment(@Nonnull final Transaction transaction, @Nonnull final Tuple prefix) {
        adjust(transaction, prefix, FDBRecordStore.LITTLE_ENDIAN_INT64_ONE);
    }

    /**
     * Adds {@code -1} to the prefix's count, in {@code transaction}.
     * @param transaction the transaction the execution happened in
     * @param prefix the partition prefix
     */
    void decrement(@Nonnull final Transaction transaction, @Nonnull final Tuple prefix) {
        adjust(transaction, prefix, FDBRecordStore.LITTLE_ENDIAN_INT64_MINUS_ONE);
    }

    private void adjust(@Nonnull final Transaction transaction, @Nonnull final Tuple prefix,
                        @Nonnull final byte[] delta) {
        final byte[] key = perPrefixSubspace.pack(prefix);
        // ADD the delta, then drop the key if the result is exactly zero, so a count that returns to zero leaves no
        // lingering entry. COMPARE_AND_CLEAR sees the post-ADD value (same key, same transaction, mutations applied in
        // order) and, being an atomic mutation, adds no read conflict — so the counter stays conflict-free. A
        // concurrent mutation that commits first just leaves the value non-zero and the clear becomes a no-op; on an
        // increment the value never lands on zero, so it is a no-op there too.
        transaction.mutate(MutationType.ADD, key, delta);
        transaction.mutate(MutationType.COMPARE_AND_CLEAR, key, ZERO_COUNT);
    }

    /**
     * Clears the per-prefix counts for every prefix that begins with {@code prefix} (a possibly-partial group prefix, as
     * used by {@code deleteWhere}), dropping the outstanding-work entries for whole groups that are being removed.
     * @param transaction the transaction to clear in
     * @param prefix the (possibly partial) prefix whose counts to remove
     */
    void clearPrefix(@Nonnull final Transaction transaction, @Nonnull final Tuple prefix) {
        transaction.clear(Range.startsWith(perPrefixSubspace.pack(prefix)));
    }

    /**
     * Snapshot-reads the register and streams the partition prefixes that currently have outstanding work
     * (count &gt; 0), each paired with its count. The read streams lazily (an {@link AsyncIterator} rather than a
     * materialized list) so a merge driver can walk partitions and stop as soon as its budget is spent, without ever
     * pulling every partition into memory.
     * @param snapshot a snapshot read view (adds no read conflicts)
     * @param executor the executor to advance the filtering iterator on
     * @return an iterator over the prefixes with outstanding tasks and their counts
     */
    @Nonnull
    CloseableAsyncIterator<PrefixTaskCount> prefixesWithOutstandingWork(@Nonnull final ReadTransaction snapshot,
                                                                        @Nonnull final Executor executor) {
        final AsyncIterator<PrefixTaskCount> counts =
                AsyncUtil.mapIterator(snapshot.getRange(range()).iterator(),
                        keyValue -> new PrefixTaskCount(perPrefixSubspace.unpack(keyValue.getKey()),
                                decodeCount(keyValue.getValue())));
        // A count is dropped as soon as it hits zero, so this normally yields only positive entries. A zero that
        // lingers is benign and filtered out with a warning; a strictly-negative count, however, cannot occur while the
        // count stays coupled to the task space (each enqueue/execute moves it in the same transaction as the task
        // write, so it equals the outstanding-task count and is >= 0). A negative means that coupling was broken, so
        // surface it and let the caller disable the index rather than merge on corrupt accounting.
        return MoreAsyncUtil.filterRemaining(executor, counts, prefixTaskCount -> {
            final long count = prefixTaskCount.count();
            if (count > 0L) {
                return true;
            }
            if (count < 0L) {
                throw new NegativeTaskCountException(prefixTaskCount.prefix(), count);
            }
            if (logger.isWarnEnabled()) {
                logger.warn(KeyValueLogMessage.of("task count for prefix is not positive",
                        LogMessageKeys.DEFERRED_TASK_COUNT, count));
            }
            return false;
        });
    }

    /**
     * Snapshot-reads whether any prefix has outstanding work. Since a count is dropped the instant it hits zero, the
     * per-prefix map holds only positive entries, so the presence of any entry means work.
     * @param snapshot a snapshot read view
     * @return whether any prefix has outstanding tasks
     */
    @Nonnull
    CompletableFuture<Boolean> hasOutstandingWork(@Nonnull final ReadTransaction snapshot) {
        // Ask FDB for at most one entry (not the whole, unbounded map) and test the resulting list for emptiness.
        return snapshot.getRange(range(), 1).asList()
                .thenApply(keyValues -> !keyValues.isEmpty());
    }

    @Nonnull
    private Range range() {
        // startsWith(getKey()) rather than range(): range() begins at getKey()+0x00 and would skip the empty-prefix
        // entry of an unpartitioned index (whose key is the subspace key itself).
        return Range.startsWith(perPrefixSubspace.getKey());
    }

    private static long decodeCount(@Nullable final byte[] value) {
        return value == null ? 0L : AtomicMutation.Standard.decodeUnsignedLong(value);
    }

    /**
     * Snapshot-reads the outstanding task count for a single {@code prefix}. Read-your-writes-aware within the
     * transaction — the atomic {@code ADD}/{@code COMPARE_AND_CLEAR} mutations of a drain in flight are reflected — so a
     * merge can ask "is this partition empty now?" immediately after draining it (including any follow-up tasks the
     * drain enqueued).
     * @param snapshot a snapshot read view
     * @param prefix the partition prefix
     * @return a future of the current outstanding count for {@code prefix} ({@code 0} if the entry is absent)
     */
    @Nonnull
    CompletableFuture<Long> countFor(@Nonnull final ReadTransaction snapshot, @Nonnull final Tuple prefix) {
        return snapshot.get(perPrefixSubspace.pack(prefix)).thenApply(value -> {
            final long count = decodeCount(value);
            // A strictly-negative count cannot arise while the count stays coupled to the task space (each enqueue/
            // execute moves it in the same transaction as the task write), so a negative means that coupling was broken
            // and the accounting is corrupt; surface it so the caller can disable the index rather than treat the
            // prefix as drained (a plain <= 0 would silently release the lease on corrupt state).
            if (count < 0L) {
                throw new NegativeTaskCountException(prefix, count);
            }
            return count;
        });
    }

    /**
     * Thrown when a per-prefix task count decodes to a strictly-negative value — a corrupt, "impossible" state. It is
     * not the {@code ADD}/{@code COMPARE_AND_CLEAR} mutations that keep the count non-negative (those only make it
     * conflict-free); it stays {@code >= 0} because it is coupled to the task space — every enqueue increments and every
     * execute decrements in the same transaction as the task write, so a healthy count equals the number of outstanding
     * tasks for the prefix. A negative therefore means some code broke that symmetry (e.g. a decrement with no task
     * removed, an enqueue that skipped the increment, or two tasks colliding on one key), and the accounting can no
     * longer be trusted. Package-visible so {@link VectorIndexMaintainer} can recognize it on the merge path and disable
     * the index.
     */
    @SuppressWarnings("java:S110") // inherits RecordCoreException's (deep) exception hierarchy by design
    static final class NegativeTaskCountException extends RecordCoreException {
        @Serial
        private static final long serialVersionUID = 1L;

        NegativeTaskCountException(@Nonnull final Tuple prefix, final long count) {
            super("vector index deferred-task count is negative",
                    LogMessageKeys.DEFERRED_TASK_COUNT, count,
                    LogMessageKeys.PARTITION_ID, prefix);
        }
    }
}
