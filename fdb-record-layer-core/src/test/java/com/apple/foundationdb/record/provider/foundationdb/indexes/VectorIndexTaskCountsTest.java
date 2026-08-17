/*
 * VectorIndexTaskCountsTest.java
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

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/**
 * Unit tests for {@link VectorIndexTaskCounts} in isolation — the conflict-free per-prefix register of outstanding
 * deferred-maintenance work — exercising its own contract directly (constructed over an index's secondary subspace,
 * driven with raw transactions) rather than through the engine/maintainer. Covers adding, draining (including the
 * zero-drop), read-your-writes visibility within a transaction, empty-prefix (unpartitioned) discoverability and
 * per-group independence, the {@code deleteWhere} range clear, and the conflict-freedom the atomic {@code ADD} counters
 * are built for.
 */
@Tag(Tags.RequiresFDB)
class VectorIndexTaskCountsTest extends VectorIndexTestBase {
    @Nonnull
    @Override
    protected Map<String, String> indexOptions() {
        return ImmutableMap.of(
                IndexOptions.VECTOR_ENGINE, VectorIndexEngineKind.GUARDIANN.name(),
                IndexOptions.VECTOR_METRIC, Metric.EUCLIDEAN_METRIC.name(),
                IndexOptions.VECTOR_NUM_DIMENSIONS, "128");
    }

    @Test
    void addingRaisesTheCountAndPersists() throws Exception {
        final VectorIndexTaskCounts counts = new VectorIndexTaskCounts(secondarySubspace());
        final Tuple prefix = Tuple.from(7L);

        try (FDBRecordContext context = openContext()) {
            counts.increment(context.ensureActive(), prefix);
            counts.increment(context.ensureActive(), prefix);
            counts.increment(context.ensureActive(), prefix);
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            assertThat(counts.countFor(context.readTransaction(true), prefix).get())
                    .as("three enqueues must persist as a count of three").isEqualTo(3L);
            assertThat(counts.hasOutstandingWork(context.readTransaction(true)).get())
                    .as("a positive count means there is outstanding work").isTrue();
        }
    }

    @Test
    void decrementingToZeroDropsTheEntry() throws Exception {
        final VectorIndexTaskCounts counts = new VectorIndexTaskCounts(secondarySubspace());
        final Tuple prefix = Tuple.from(7L);

        try (FDBRecordContext context = openContext()) {
            counts.increment(context.ensureActive(), prefix);
            counts.increment(context.ensureActive(), prefix);
            context.commit();
        }
        // Drain one of the two: the count drops to one, still present.
        try (FDBRecordContext context = openContext()) {
            counts.decrement(context.ensureActive(), prefix);
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            assertThat(counts.countFor(context.readTransaction(true), prefix).get()).isEqualTo(1L);
            assertThat(counts.hasOutstandingWork(context.readTransaction(true)).get()).isTrue();
        }
        // Drain the last one: the count returns to zero, so the entry is dropped entirely.
        try (FDBRecordContext context = openContext()) {
            counts.decrement(context.ensureActive(), prefix);
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            assertThat(counts.countFor(context.readTransaction(true), prefix).get())
                    .as("a count back at zero reads as zero (its entry was dropped)").isEqualTo(0L);
            assertThat(counts.hasOutstandingWork(context.readTransaction(true)).get())
                    .as("no entries means no outstanding work").isFalse();
            assertThat(collectPrefixes(counts, context))
                    .as("a dropped prefix must not appear in discovery").isEmpty();
        }
    }

    @Test
    void readsAreReadYourWritesAwareWithinTheTransaction() throws Exception {
        final VectorIndexTaskCounts counts = new VectorIndexTaskCounts(secondarySubspace());
        final Tuple prefix = Tuple.from(7L);

        // Everything below happens in one uncommitted transaction: each atomic mutation must be visible to a later
        // (snapshot) read in that same transaction — this is what lets a merge drain a prefix and then ask, in the same
        // transaction, whether it is now empty.
        try (FDBRecordContext context = openContext()) {
            counts.increment(context.ensureActive(), prefix);
            assertThat(counts.countFor(context.readTransaction(true), prefix).get())
                    .as("an ADD is visible to a later read in the same transaction").isEqualTo(1L);

            counts.increment(context.ensureActive(), prefix);
            assertThat(counts.countFor(context.readTransaction(true), prefix).get()).isEqualTo(2L);
            assertThat(counts.hasOutstandingWork(context.readTransaction(true)).get()).isTrue();

            counts.decrement(context.ensureActive(), prefix);
            counts.decrement(context.ensureActive(), prefix);
            assertThat(counts.countFor(context.readTransaction(true), prefix).get())
                    .as("returning to zero drops the entry, visible within the same transaction").isEqualTo(0L);
            assertThat(counts.hasOutstandingWork(context.readTransaction(true)).get()).isFalse();
        }
    }

    /**
     * The empty (unpartitioned) prefix is the awkward case for discovery: its entry keys at the register's subspace key
     * itself, so {@code prefixesWithOutstandingWork} must scan {@code Range.startsWith(getKey())} rather than the
     * subspace {@code .range()} (which begins just past that key and would skip it). This is the one test that exercises
     * that guard: an empty-prefix entry is counted, surfaced by discovery, and — like any other prefix — dropped from
     * discovery once it drains to zero.
     */
    @Test
    void emptyPrefixIsDiscoverableAndDrainsLikeAnyOther() throws Exception {
        final VectorIndexTaskCounts counts = new VectorIndexTaskCounts(secondarySubspace());
        final Tuple unpartitioned = Tuple.from(); // an unpartitioned index keys its sole entry at the subspace itself

        try (FDBRecordContext context = openContext()) {
            counts.increment(context.ensureActive(), unpartitioned);
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            assertThat(counts.countFor(context.readTransaction(true), unpartitioned).get()).isEqualTo(1L);
            assertThat(counts.hasOutstandingWork(context.readTransaction(true)).get()).isTrue();
            assertThat(collectPrefixes(counts, context))
                    .as("the empty-prefix entry keys at the subspace key itself, so discovery must scan "
                            + "startsWith(getKey()) — the subspace range would skip it")
                    .containsOnly(entry(unpartitioned, 1L));
        }
        // Draining the sole empty prefix to zero drops even the subspace-key entry: no entries, no outstanding work.
        try (FDBRecordContext context = openContext()) {
            counts.decrement(context.ensureActive(), unpartitioned);
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            assertThat(collectPrefixes(counts, context))
                    .as("draining the empty prefix to zero drops the subspace-key entry from discovery").isEmpty();
            assertThat(counts.hasOutstandingWork(context.readTransaction(true)).get())
                    .as("no entries means no outstanding work").isFalse();
        }
    }

    /**
     * A grouped index tracks each group's prefix independently: two groups accrue their own counts, both are surfaced by
     * discovery, and draining one group's tasks to zero drops only that group while the other's count survives. Because
     * "is there work?" is just "is the per-prefix map non-empty?", a still-backlogged sibling group keeps the index's
     * outstanding-work status true — no index-wide total is involved.
     */
    @Test
    void groupPrefixesAreTrackedIndependently() throws Exception {
        final VectorIndexTaskCounts counts = new VectorIndexTaskCounts(secondarySubspace());
        final Tuple groupOne = Tuple.from(1L);
        final Tuple groupTwo = Tuple.from(2L);

        try (FDBRecordContext context = openContext()) {
            counts.increment(context.ensureActive(), groupOne);
            counts.increment(context.ensureActive(), groupTwo);
            counts.increment(context.ensureActive(), groupTwo);
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            assertThat(counts.countFor(context.readTransaction(true), groupOne).get()).isEqualTo(1L);
            assertThat(counts.countFor(context.readTransaction(true), groupTwo).get()).isEqualTo(2L);
            assertThat(collectPrefixes(counts, context))
                    .as("each group prefix is discoverable with its own count")
                    .containsOnly(entry(groupOne, 1L), entry(groupTwo, 2L));
        }
        // Draining one group to zero drops only that group; the other group's backlog is untouched.
        try (FDBRecordContext context = openContext()) {
            counts.decrement(context.ensureActive(), groupTwo);
            counts.decrement(context.ensureActive(), groupTwo);
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            assertThat(collectPrefixes(counts, context))
                    .as("the drained group drops out; the independent one remains")
                    .containsOnly(entry(groupOne, 1L));
            assertThat(counts.hasOutstandingWork(context.readTransaction(true)).get())
                    .as("a still-backlogged sibling group keeps the index's outstanding-work status true").isTrue();
        }
    }

    @Test
    void concurrentIncrementsDoNotConflict() throws Exception {
        final VectorIndexTaskCounts counts = new VectorIndexTaskCounts(secondarySubspace());
        final Tuple prefix = Tuple.from(7L);

        // Two transactions that both bump the same prefix must both commit: the count is an atomic ADD, which commutes
        // and takes no read conflict, so concurrent inserts/drains never contend on the counter.
        try (FDBRecordContext a = openContext();
                FDBRecordContext b = openContext()) {
            a.getReadVersion();
            b.getReadVersion();
            counts.increment(a.ensureActive(), prefix);
            counts.increment(b.ensureActive(), prefix);
            b.commit();
            a.commit(); // must not conflict with b's concurrent increment
        }
        try (FDBRecordContext context = openContext()) {
            assertThat(counts.countFor(context.readTransaction(true), prefix).get())
                    .as("both conflict-free increments must land").isEqualTo(2L);
        }
    }

    /**
     * The decrement counterpart to {@link #concurrentIncrementsDoNotConflict}: two concurrent drains of the same prefix
     * both commit (the counter is an atomic {@code ADD}, which commutes and takes no read conflict) — and when they
     * cross zero together, the {@code COMPARE_AND_CLEAR} still drops the entry exactly once regardless of commit order,
     * leaving no stale zero or negative. This exercises the {@code ADD} + {@code COMPARE_AND_CLEAR} interplay under
     * concurrency that an increment never triggers (its clear is always a no-op), and is the concurrent counterpart to
     * the sequential {@link #decrementingToZeroDropsTheEntry}.
     */
    @Test
    void concurrentDecrementsDoNotConflictAndDropAtZero() throws Exception {
        final VectorIndexTaskCounts counts = new VectorIndexTaskCounts(secondarySubspace());
        final Tuple prefix = Tuple.from(7L);

        // Seed a count of two so the two concurrent decrements land it exactly on zero.
        try (FDBRecordContext context = openContext()) {
            counts.increment(context.ensureActive(), prefix);
            counts.increment(context.ensureActive(), prefix);
            context.commit();
        }
        try (FDBRecordContext a = openContext();
                FDBRecordContext b = openContext()) {
            a.getReadVersion();
            b.getReadVersion();
            counts.decrement(a.ensureActive(), prefix);
            counts.decrement(b.ensureActive(), prefix);
            b.commit();
            a.commit(); // must not conflict with b's concurrent decrement
        }
        try (FDBRecordContext context = openContext()) {
            assertThat(counts.countFor(context.readTransaction(true), prefix).get())
                    .as("two conflict-free decrements crossing zero drop the entry (reads back as zero)").isEqualTo(0L);
            assertThat(counts.hasOutstandingWork(context.readTransaction(true)).get())
                    .as("the dropped entry means no outstanding work").isFalse();
        }
    }

    @Test
    void clearPrefixDropsAGroupsCount() throws Exception {
        final VectorIndexTaskCounts counts = new VectorIndexTaskCounts(secondarySubspace());
        final Tuple groupOne = Tuple.from(1L);
        final Tuple groupTwo = Tuple.from(2L);

        try (FDBRecordContext context = openContext()) {
            counts.increment(context.ensureActive(), groupOne);
            counts.increment(context.ensureActive(), groupOne);
            counts.increment(context.ensureActive(), groupTwo);
            context.commit();
        }
        // deleteWhere removing group 1 clears that group's outstanding-work count outright (not a decrement).
        try (FDBRecordContext context = openContext()) {
            counts.clearPrefix(context.ensureActive(), groupOne);
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            assertThat(collectPrefixes(counts, context))
                    .as("clearing a group removes its count and leaves the other groups untouched")
                    .containsOnly(entry(groupTwo, 1L));
        }
    }

    /**
     * A strictly-negative count is impossible under the conflict-free ADD/COMPARE_AND_CLEAR accounting, so a
     * point read of one must surface {@link VectorIndexTaskCounts.NegativeTaskCountException} rather than quietly read
     * it as "drained" — that exception is what lets a merge disable the corrupt index instead of releasing its lease
     * on bad state. A lone decrement of an absent counter is the simplest way to drive it below zero (ADD {@code -1}
     * to an absent key leaves {@code -1}).
     */
    @Test
    void countForThrowsOnANegativeTaskCount() throws Exception {
        final VectorIndexTaskCounts counts = new VectorIndexTaskCounts(secondarySubspace());
        final Tuple prefix = Tuple.from(7L);

        try (FDBRecordContext context = openContext()) {
            counts.decrement(context.ensureActive(), prefix);
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            assertThatThrownBy(() -> counts.countFor(context.readTransaction(true), prefix).get())
                    .as("a negative count must surface as NegativeTaskCountException, not read as drained")
                    .hasRootCauseInstanceOf(VectorIndexTaskCounts.NegativeTaskCountException.class);
        }
    }

    /**
     * The same guard on the discovery path: streaming the prefixes with outstanding work must surface a negative count
     * rather than emit it as if it were real work, so the merge driver disables the index instead of trying to drain a
     * corrupt prefix.
     */
    @Test
    void discoveryThrowsOnANegativeTaskCount() throws Exception {
        final VectorIndexTaskCounts counts = new VectorIndexTaskCounts(secondarySubspace());
        final Tuple prefix = Tuple.from(7L);

        try (FDBRecordContext context = openContext()) {
            counts.decrement(context.ensureActive(), prefix);
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            assertThatThrownBy(() -> collectPrefixes(counts, context))
                    .as("outstanding-work discovery must surface a negative count rather than emit it")
                    .hasRootCauseInstanceOf(VectorIndexTaskCounts.NegativeTaskCountException.class);
        }
    }

    /**
     * A lingering <em>zero</em> is benign, not corrupt: the accounting only stops distinguishing "healthy" from
     * "corrupt" at strictly-negative. A zero is normally dropped the instant it is reached, so force a raw zero entry
     * to exercise the branch — a point read reads it as zero (never throwing) and discovery filters it out with a
     * warning rather than treating it as the fatal negative case.
     */
    @Test
    void lingeringZeroCountIsFilteredNotFatal() throws Exception {
        final Subspace secondary = secondarySubspace();
        final VectorIndexTaskCounts counts = new VectorIndexTaskCounts(secondary);
        final Tuple prefix = Tuple.from(7L);

        // Write the 8-byte little-endian zero the ADD counters use directly, since COMPARE_AND_CLEAR would otherwise
        // drop a genuine zero the moment it is reached.
        try (FDBRecordContext context = openContext()) {
            context.ensureActive().set(countKey(secondary, prefix), new byte[Long.BYTES]);
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            assertThat(counts.countFor(context.readTransaction(true), prefix).get())
                    .as("a lingering zero reads as zero, never throwing").isEqualTo(0L);
            assertThat(collectPrefixes(counts, context))
                    .as("a lingering zero is filtered out of discovery, not fatal").isEmpty();
        }
    }

    /**
     * The on-disk counter key {@link VectorIndexTaskCounts} maintains for {@code prefix}: the index's secondary
     * subspace, decorated with the {@code TASK_COUNTS} prefix, packed with the partition prefix — reconstructed here so
     * a test can plant a raw value the public {@code increment}/{@code decrement} API cannot produce.
     */
    @Nonnull
    private static byte[] countKey(@Nonnull final Subspace secondary, @Nonnull final Tuple prefix) {
        return secondary.subspace(Tuple.from(VectorIndexSecondarySubspaceKeys.TASK_COUNTS)).pack(prefix);
    }

    /**
     * Materializes the register's outstanding-work discovery into a {@code prefix -> count} map so a test can assert on
     * exactly which prefixes are present and with what counts.
     */
    @Nonnull
    private static Map<Tuple, Long> collectPrefixes(@Nonnull final VectorIndexTaskCounts counts,
                                                    @Nonnull final FDBRecordContext context) throws Exception {
        final Map<Tuple, Long> byPrefix = new HashMap<>();
        for (final PrefixTaskCount prefixTaskCount : AsyncUtil.collectRemaining(
                counts.prefixesWithOutstandingWork(context.readTransaction(true), context.getExecutor())).get()) {
            byPrefix.put(prefixTaskCount.prefix(), prefixTaskCount.count());
        }
        return byPrefix;
    }

    /**
     * A clean secondary subspace to run each test's register against, taken from a freshly opened (per-test) store's
     * ungrouped vector index — the same subspace the engine would hand {@link VectorIndexTaskCounts}.
     */
    @Nonnull
    private Subspace secondarySubspace() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, this::addUngroupedVectorIndex);
            return recordStore.indexSecondarySubspace(
                    recordStore.getRecordMetaData().getIndex("UngroupedVectorIndex"));
        }
    }
}
