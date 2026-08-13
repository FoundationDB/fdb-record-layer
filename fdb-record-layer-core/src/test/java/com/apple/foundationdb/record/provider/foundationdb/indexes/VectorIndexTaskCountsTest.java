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
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

/**
 * Unit tests for {@link VectorIndexTaskCounts} in isolation — the conflict-free per-prefix register of outstanding
 * deferred-maintenance work — exercising its own contract directly (constructed over an index's secondary subspace,
 * driven with raw transactions) rather than through the engine/maintainer. Covers adding, draining (including the
 * zero-drop), read-your-writes visibility within a transaction, unpartitioned (empty-prefix) vs partitioned prefixes,
 * the {@code deleteWhere} range clear, and the conflict-freedom the atomic {@code ADD} counters are built for.
 */
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

    @Test
    void unpartitionedAndPartitionedPrefixesAreTrackedIndependently() throws Exception {
        final VectorIndexTaskCounts counts = new VectorIndexTaskCounts(secondarySubspace());
        final Tuple unpartitioned = Tuple.from();    // an unpartitioned index keys at the subspace itself
        final Tuple partitioned = Tuple.from(7L);

        try (FDBRecordContext context = openContext()) {
            counts.increment(context.ensureActive(), unpartitioned);
            counts.increment(context.ensureActive(), partitioned);
            counts.increment(context.ensureActive(), partitioned);
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            assertThat(counts.countFor(context.readTransaction(true), unpartitioned).get()).isEqualTo(1L);
            assertThat(counts.countFor(context.readTransaction(true), partitioned).get()).isEqualTo(2L);
            // Discovery must surface both — including the empty-prefix entry, which is keyed at the subspace key itself
            // (the reason discovery scans startsWith(getKey()) rather than the subspace range).
            assertThat(collectPrefixes(counts, context))
                    .as("both the unpartitioned and partitioned prefixes are discoverable, each with its own count")
                    .containsOnly(entry(unpartitioned, 1L), entry(partitioned, 2L));
        }
        // Draining the partitioned prefix to zero leaves only the unpartitioned entry; work still remains overall.
        try (FDBRecordContext context = openContext()) {
            counts.decrement(context.ensureActive(), partitioned);
            counts.decrement(context.ensureActive(), partitioned);
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            assertThat(collectPrefixes(counts, context))
                    .as("the drained partitioned prefix drops out; the independent unpartitioned one remains")
                    .containsOnly(entry(unpartitioned, 1L));
            assertThat(counts.hasOutstandingWork(context.readTransaction(true)).get()).isTrue();
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
