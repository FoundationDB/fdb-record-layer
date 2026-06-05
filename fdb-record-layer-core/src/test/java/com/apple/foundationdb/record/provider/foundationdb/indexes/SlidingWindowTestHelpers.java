/*
 * SlidingWindowTestHelpers.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.half.Half;
import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexPredicate;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanOptions;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test helpers and fluent matchers for sliding-window indexes.
 * Holds the probe utilities (vector construction, delegate scans for HNSW and value indexes,
 * sliding-window snapshot) and the {@link SlidingWindowAssert} / {@link DelegateAssert}
 * fluent DSL.
 */
public final class SlidingWindowTestHelpers {

    private SlidingWindowTestHelpers() {
    }

    @Nonnull
    public static HalfRealVector makeVector(final float... values) {
        final Half[] components = new Half[values.length];
        for (int i = 0; i < values.length; i++) {
            components[i] = Half.valueOf(values[i]);
        }
        return new HalfRealVector(components);
    }

    @Nonnull
    public static HalfRealVector sampleVector() {
        return makeVector(0.5f, 0.5f, 0.4f, 0.1f);
    }

    /**
     * Scans the HNSW index with a broad query to find all indexed records,
     * optionally restricted to a single group.
     */
    @Nonnull
    public static Set<Long> scanIndexRecNos(@Nonnull final FDBRecordStore recordStore,
                                            @Nonnull final String indexName,
                                            @Nullable final Tuple groupingKey) {
        final Index index = recordStore.getRecordMetaData().getIndex(indexName);
        final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);
        final HalfRealVector queryVector = makeVector(0.5f, 0.5f, 0.5f, 0.5f);

        final double actualDistance = new Metric.EuclideanMetric().distance(queryVector.getData(), sampleVector().getData());
        final int limit = (int)(3 /*safety*/ + actualDistance); // overestimate limit to guarantee retrieval of all vectors.

        final TupleRange range = groupingKey == null ? TupleRange.ALL : TupleRange.allOf(groupingKey);
        final VectorIndexScanBounds bounds = new VectorIndexScanBounds(
                range,
                Comparisons.Type.DISTANCE_RANK_LESS_THAN_OR_EQUAL,
                queryVector,
                limit,
                VectorIndexScanOptions.empty());
        return maintainer.scan(bounds, null, ScanProperties.FORWARD_SCAN)
                .asList()
                .join()
                .stream()
                .map(e -> e.getPrimaryKey().getLong(0))
                .collect(Collectors.toSet());
    }

    /**
     * Scans a value-index delegate (BY_VALUE) and returns the primary keys of every entry,
     * optionally restricted to a single group.
     */
    @Nonnull
    public static Set<Long> scanIndexRecNosViaValueIndex(@Nonnull final FDBRecordStore recordStore,
                                                        @Nonnull final String indexName,
                                                        @Nullable final Tuple groupingKey) {
        final Index index = recordStore.getRecordMetaData().getIndex(indexName);
        final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);
        final TupleRange range = groupingKey == null ? TupleRange.ALL : TupleRange.allOf(groupingKey);
        return maintainer.scan(IndexScanType.BY_VALUE, range, null, ScanProperties.FORWARD_SCAN)
                .asList()
                .join()
                .stream()
                .map(e -> e.getPrimaryKey().getLong(0))
                .collect(Collectors.toSet());
    }

    /**
     * Returns a snapshot of the sliding-window state for the ungrouped index.
     */
    @Nonnull
    public static SlidingWindow slidingWindow(@Nonnull final FDBRecordStore recordStore,
                                              @Nonnull final String indexName) {
        return groupedSlidingWindow(recordStore, indexName, null);
    }

    /**
     * Returns a snapshot of the sliding-window state for the given group.
     * Captures the window counter, the delegate (HNSW) recNos, and the recNos
     * the entries subspace claims are on the window side of the boundary —
     * scoped to the same group across all three.
     */
    @Nonnull
    public static SlidingWindow groupedSlidingWindow(@Nonnull final FDBRecordStore recordStore,
                                                     @Nonnull final String indexName,
                                                     @Nullable final Tuple groupingKey) {
        final IndexPredicate.RowNumberWindowPredicate.Direction direction =
                directionOf(recordStore.getRecordMetaData().getIndex(indexName));
        return new SlidingWindow(readWindowCount(recordStore, indexName, groupingKey),
                                 scanIndexRecNos(recordStore, indexName, groupingKey),
                                 windowSideEntryRecNos(recordStore, indexName, groupingKey, direction));
    }

    /**
     * Returns a snapshot of the sliding-window state for the ungrouped index,
     * enumerating the delegate via a BY_VALUE scan (use this when the delegate is
     * a value index, not HNSW).
     */
    @Nonnull
    public static SlidingWindow slidingWindowViaValueIndex(@Nonnull final FDBRecordStore recordStore,
                                                          @Nonnull final String indexName) {
        return groupedSlidingWindowViaValueIndex(recordStore, indexName, null);
    }

    /**
     * Returns a snapshot of the sliding-window state for the given group,
     * enumerating the delegate via a BY_VALUE scan (use this when the delegate is
     * a value index, not HNSW).
     */
    @Nonnull
    public static SlidingWindow groupedSlidingWindowViaValueIndex(@Nonnull final FDBRecordStore recordStore,
                                                                 @Nonnull final String indexName,
                                                                 @Nullable final Tuple groupingKey) {
        final IndexPredicate.RowNumberWindowPredicate.Direction direction =
                directionOf(recordStore.getRecordMetaData().getIndex(indexName));
        return new SlidingWindow(readWindowCount(recordStore, indexName, groupingKey),
                                 scanIndexRecNosViaValueIndex(recordStore, indexName, groupingKey),
                                 windowSideEntryRecNos(recordStore, indexName, groupingKey, direction));
    }

    /**
     * Extracts the {@link IndexPredicate.RowNumberWindowPredicate.Direction} from
     * a sliding-window index's predicate tree. The predicate may be the qualifier
     * directly or a child of an {@code AndPredicate}.
     */
    @Nonnull
    private static IndexPredicate.RowNumberWindowPredicate.Direction directionOf(@Nonnull final Index index) {
        final IndexPredicate predicate = index.getPredicate();
        if (predicate instanceof IndexPredicate.RowNumberWindowPredicate) {
            return ((IndexPredicate.RowNumberWindowPredicate) predicate).getDirection();
        }
        if (predicate instanceof IndexPredicate.AndPredicate) {
            for (IndexPredicate child : ((IndexPredicate.AndPredicate) predicate).getChildren()) {
                if (child instanceof IndexPredicate.RowNumberWindowPredicate) {
                    return ((IndexPredicate.RowNumberWindowPredicate) child).getDirection();
                }
            }
        }
        throw new IllegalStateException(
                "index '" + index.getName() + "' is not a sliding-window index — no RowNumberWindowPredicate found");
    }

    private static long readWindowCount(@Nonnull final FDBRecordStore recordStore,
                                        @Nonnull final String indexName,
                                        @Nullable final Tuple groupingKey) {
        final Index index = recordStore.getRecordMetaData().getIndex(indexName);
        Subspace swSubspace = recordStore.indexSlidingWindowSubspace(index);
        if (groupingKey != null) {
            swSubspace = swSubspace.subspace(groupingKey);
        }
        final Subspace metaSubspace = swSubspace.subspace(Tuple.from()).subspace(Tuple.from(1));
        final byte[] counterKey = metaSubspace.pack(Tuple.from(3));
        final byte[] counterBytes = recordStore.ensureContextActive().get(counterKey).join();
        if (counterBytes == null) {
            return 0L;
        }
        return decodeLong(counterBytes);
    }

    /**
     * Reads the entries subspace and returns the primary keys of entries that fall on
     * the window side of the boundary — i.e. the records the maintainer's bookkeeping
     * claims are inside the window. For a maintainer in a consistent state, this set
     * must exactly equal the delegate's contents; a divergence means the entries
     * subspace and the delegate are out of sync.
     */
    @Nonnull
    public static Set<Long> windowSideEntryRecNos(@Nonnull final FDBRecordStore recordStore,
                                                  @Nonnull final String indexName,
                                                  @Nullable final Tuple groupingKey,
                                                  @Nonnull final IndexPredicate.RowNumberWindowPredicate.Direction direction) {
        final Index index = recordStore.getRecordMetaData().getIndex(indexName);
        Subspace swSubspace = recordStore.indexSlidingWindowSubspace(index);
        if (groupingKey != null) {
            swSubspace = swSubspace.subspace(groupingKey);
        }
        final Subspace partitionSubspace = swSubspace.subspace(Tuple.from());
        final Subspace entriesSubspace = partitionSubspace.subspace(Tuple.from(0));
        final Subspace metaSubspace = partitionSubspace.subspace(Tuple.from(1));

        final byte[] boundaryBytes =
                recordStore.ensureContextActive().get(metaSubspace.pack(Tuple.from(4))).join();
        if (boundaryBytes == null) {
            // No boundary set => no window
            return Set.of();
        }
        final Tuple boundaryEntryKey = Tuple.fromBytes(boundaryBytes);

        return recordStore.ensureContextActive().getRange(entriesSubspace.range())
                .asList().join().stream()
                .filter(kv -> isOnWindowSide(entriesSubspace.unpack(kv.getKey()), boundaryEntryKey, direction))
                .map(kv -> Tuple.fromBytes(kv.getValue()).getLong(0))
                .collect(Collectors.toSet());
    }

    private static boolean isOnWindowSide(@Nonnull final Tuple entryKey,
                                          @Nonnull final Tuple boundaryEntryKey,
                                          @Nonnull final IndexPredicate.RowNumberWindowPredicate.Direction direction) {
        final int cmp = entryKey.compareTo(boundaryEntryKey);
        // ASC/MIN: window = entries ≤ boundary. DESC/MAX: window = entries ≥ boundary.
        return direction == IndexPredicate.RowNumberWindowPredicate.Direction.ASC ? cmp <= 0 : cmp >= 0;
    }

    /**
     * Snapshot of the sliding-window state. Carries the window counter, the
     * delegate's recNos (HNSW or value index, depending on which builder
     * captured the snapshot), and the recNos from the entries subspace that
     * fall on the window side of the boundary. Chained assertions like
     * {@code .underlyingValueIndex().containsInAnyOrder(...)} or
     * {@code .hasEntriesOf(...)} stay scoped to the same group.
     */
    record SlidingWindow(long size,
                         @Nonnull Set<Long> hnswRecNos,
                         @Nonnull Set<Long> windowEntryRecNos) {
    }

    /**
     * Fluent assertion over a {@link SlidingWindow} probe. Static-import
     * {@link #assertThat(SlidingWindow)} to use the chained API. An optional
     * description set via {@link #as(String)} is prefixed to any failure message
     * and propagated to {@link #underlyingHnsw()}.
     */
    public static final class SlidingWindowAssert {
        @Nonnull
        private final SlidingWindow window;
        @Nullable
        private final String description;

        private SlidingWindowAssert(@Nonnull final SlidingWindow window, @Nullable final String description) {
            this.window = window;
            this.description = description;
        }

        @Nonnull
        public static SlidingWindowAssert assertThat(@Nonnull final SlidingWindow window) {
            return new SlidingWindowAssert(window, null);
        }

        /**
         * Attaches a description that will be prefixed to any failure message
         * produced by subsequent assertions in this chain (including those on
         * the {@link DelegateAssert} returned by {@link #underlyingHnsw()}).
         */
        @Nonnull
        public SlidingWindowAssert as(@Nonnull final String description) {
            return new SlidingWindowAssert(window, description);
        }

        @Nonnull
        public SlidingWindowAssert hasSizeOf(final int expectedSize) {
            assertEquals(expectedSize, window.size(),
                    describe(description,
                            "Sliding window should have size " + expectedSize + " but was " + window.size()));
            return this;
        }

        /**
         * Asserts that the entries subspace's window-side recNos exactly match
         * {@code expectedRecNos}. The window-side set is the maintainer's own
         * claim about which records belong inside the window — pinning down the
         * invariant that it must agree with the delegate.
         */
        @Nonnull
        public SlidingWindowAssert hasEntriesOf(final long... expectedRecNos) {
            final Set<Long> expected = LongStream.of(expectedRecNos).boxed().collect(Collectors.toSet());
            assertEquals(expected, window.windowEntryRecNos(),
                    describe(description,
                            "Entries subspace (window side) should contain " + expected
                                    + " but was " + window.windowEntryRecNos()));
            return this;
        }

        /**
         * Returns a fluent assertion over the HNSW state captured by this probe,
         * scoped to the same group. The {@link #as(String) description} (if any)
         * is propagated.
         */
        @Nonnull
        public DelegateAssert underlyingHnsw() {
            return new DelegateAssert("HNSW", window.hnswRecNos(), description);
        }

        /**
         * Returns a fluent assertion over the value-index delegate state captured
         * by this probe, scoped to the same group. The {@link #as(String) description}
         * (if any) is propagated.
         */
        @Nonnull
        public DelegateAssert underlyingValueIndex() {
            return new DelegateAssert("Value index", window.hnswRecNos(), description);
        }
    }

    /**
     * Fluent assertion over a snapshot of recNos in the underlying delegate index.
     * The {@code label} is woven into failure messages (e.g. {@code "HNSW"} vs
     * {@code "Value index"}) so the same shape works for any delegate type.
     */
    public static final class DelegateAssert {
        @Nonnull
        private final String label;
        @Nonnull
        private final Set<Long> recNos;
        @Nullable
        private final String description;

        DelegateAssert(@Nonnull final String label,
                       @Nonnull final Set<Long> recNos,
                       @Nullable final String description) {
            this.label = label;
            this.recNos = recNos;
            this.description = description;
        }

        /**
         * Attaches a description that will be prefixed to any failure message
         * produced by subsequent assertions in this chain.
         */
        @Nonnull
        public DelegateAssert as(@Nonnull final String description) {
            return new DelegateAssert(label, recNos, description);
        }

        @Nonnull
        public DelegateAssert containsInAnyOrder(final long... expectedRecNos) {
            final Set<Long> expected = LongStream.of(expectedRecNos).boxed().collect(Collectors.toSet());
            assertEquals(expected, recNos,
                    describe(description, label + " should contain " + expected + " but was " + recNos));
            return this;
        }

        @Nonnull
        public DelegateAssert contains(final long expectedRecNo) {
            assertTrue(recNos.contains(expectedRecNo),
                    describe(description, label + " should contain " + expectedRecNo + " but was " + recNos));
            return this;
        }

        @Nonnull
        public DelegateAssert isEmpty() {
            assertTrue(recNos.isEmpty(),
                    describe(description, label + " should be empty but contained " + recNos));
            return this;
        }
    }

    @Nonnull
    private static String describe(@Nullable final String description, @Nonnull final String defaultMessage) {
        return description == null ? defaultMessage : description + System.lineSeparator() + defaultMessage;
    }

    private static long decodeLong(byte[] bytes) {
        return ByteBuffer.wrap(bytes)
                .order(ByteOrder.LITTLE_ENDIAN)
                .getLong();
    }

    // ===== Concurrent-transaction fluent harness =====

    /**
     * Entry point for the fluent concurrent-transaction harness. Mirrors the manual
     * pattern of opening N contexts, binding the test's {@code recordStore} field to
     * each one in turn, running per-context actions, and committing in declared order.
     *
     * <p>Example:</p>
     * <pre>{@code
     * concurrent(this, ctx -> openStore(ctx, 5, Direction.DESC))
     *         .tx("A", () -> rec(4, 400))
     *         .tx("B", () -> rec(5, 500))
     *         .commitAll()
     *         .expectNoConflicts();
     * }</pre>
     *
     * @param testBase the test instance whose {@code openContext()} / {@code commit()}
     *                 / {@code recordStore} are driven by the harness
     * @param storeOpener binds the test's {@code recordStore} field to the given context
     *                    (typically {@code ctx -> openStore(ctx, ...)})
     */
    @Nonnull
    public static ConcurrentScenario concurrent(@Nonnull final FDBRecordStoreTestBase testBase,
                                                @Nonnull final ContextSetup storeOpener) {
        return new ConcurrentScenario(testBase, storeOpener);
    }

    /**
     * Binds a context to the {@link FDBRecordStore} instance under test (e.g. by calling
     * the test's {@code openStore(ctx, ...)}). Invoked once per registered transaction
     * before its action runs.
     */
    @FunctionalInterface
    public interface ContextSetup {
        void setup(@Nonnull FDBRecordContext context) throws Exception;
    }

    /**
     * Action run inside a transaction. The test's {@code recordStore} is bound to the
     * matching context before the action runs, so callers can use the test's normal
     * helpers (e.g. {@code rec(...)}) without thinking about which context is active.
     */
    @FunctionalInterface
    public interface ThrowingRunnable {
        void run() throws Exception;
    }

    /**
     * A scenario builder. Each {@link #tx(String, ThrowingRunnable)} call registers a
     * named transaction; {@link #commitAll()} opens all contexts, runs the actions in
     * declared order against their respective stores, then commits each context in
     * declared order — capturing per-tx commit outcomes for later assertions.
     */
    public static final class ConcurrentScenario {
        @Nonnull
        private final FDBRecordStoreTestBase testBase;
        @Nonnull
        private final ContextSetup storeOpener;
        @Nonnull
        private final List<NamedTx> transactions = new ArrayList<>();

        ConcurrentScenario(@Nonnull final FDBRecordStoreTestBase testBase,
                           @Nonnull final ContextSetup storeOpener) {
            this.testBase = testBase;
            this.storeOpener = storeOpener;
        }

        /**
         * Registers a named transaction. The {@code action} runs against a freshly
         * opened context whose store has been wired up via the scenario's store opener.
         */
        @Nonnull
        public ConcurrentScenario tx(@Nonnull final String name, @Nonnull final ThrowingRunnable action) {
            transactions.add(new NamedTx(name, action));
            return this;
        }

        /**
         * Opens all registered contexts (so they overlap), runs each action with its
         * context's store bound, then commits each context in declared order. Commit
         * failures are captured per-tx rather than bubbled out, so subsequent commits
         * still attempt to run and the resulting {@link CommitOutcome} can describe the
         * full picture.
         */
        @Nonnull
        public CommitOutcome commitAll() {
            return runAndCommit(transactions);
        }

        /**
         * Same as {@link #commitAll()} but shuffles the commit order using the given
         * {@link Random}. Useful when the test asserts an outcome that should hold
         * regardless of which transaction commits first; pair with
         * {@code @ParameterizedTest @RandomSeedSource} so the test can vary the seed
         * across runs while still being reproducible from a logged seed on failure.
         */
        @Nonnull
        public CommitOutcome commitInAnyOrder(@Nonnull final Random random) {
            final List<NamedTx> shuffled = new ArrayList<>(transactions);
            Collections.shuffle(shuffled, random);
            return runAndCommit(shuffled);
        }

        @Nonnull
        private CommitOutcome runAndCommit(@Nonnull final List<NamedTx> commitOrder) {
            for (NamedTx tx : transactions) {
                tx.context = testBase.openContext();
            }
            try {
                for (NamedTx tx : transactions) {
                    storeOpener.setup(tx.context);
                    tx.action.run();
                }
                for (NamedTx tx : commitOrder) {
                    try {
                        testBase.commit(tx.context);
                    } catch (Throwable t) {
                        tx.commitError = t;
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("scenario action failed before commit phase", e);
            } finally {
                for (NamedTx tx : transactions) {
                    if (tx.context != null) {
                        tx.context.close();
                    }
                }
            }
            return new CommitOutcome(transactions);
        }
    }

    /**
     * Result of {@link ConcurrentScenario#commitAll()}. Provides expectation-style
     * assertions over the per-tx commit outcomes.
     */
    public static final class CommitOutcome {
        @Nonnull
        private final List<NamedTx> transactions;

        CommitOutcome(@Nonnull final List<NamedTx> transactions) {
            this.transactions = transactions;
        }

        /**
         * Asserts that every registered transaction committed successfully (no
         * conflicts, no other commit-time failures).
         */
        @Nonnull
        public CommitOutcome expectNoConflicts() {
            for (NamedTx tx : transactions) {
                if (tx.commitError != null) {
                    throw new AssertionError(
                            "Expected no commit conflicts but tx '" + tx.name + "' failed: " + tx.commitError,
                            tx.commitError);
                }
            }
            return this;
        }

        /**
         * Asserts that the named transaction failed to commit (typically with a
         * conflict). Other transactions are not checked.
         */
        @Nonnull
        public CommitOutcome expectConflictOn(@Nonnull final String txName) {
            final NamedTx tx = find(txName);
            if (tx.commitError == null) {
                throw new AssertionError(
                        "Expected tx '" + txName + "' to fail commit, but it committed successfully");
            }
            return this;
        }

        /**
         * Asserts that the named transaction committed successfully.
         */
        @Nonnull
        public CommitOutcome expectCommitted(@Nonnull final String txName) {
            final NamedTx tx = find(txName);
            if (tx.commitError != null) {
                throw new AssertionError(
                        "Expected tx '" + txName + "' to commit, but it failed: " + tx.commitError,
                        tx.commitError);
            }
            return this;
        }

        @Nonnull
        private NamedTx find(@Nonnull final String txName) {
            return transactions.stream()
                    .filter(t -> t.name.equals(txName))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("Unknown tx: " + txName));
        }
    }

    private static final class NamedTx {
        @Nonnull final String name;
        @Nonnull final ThrowingRunnable action;
        @Nullable FDBRecordContext context;
        @Nullable Throwable commitError;

        NamedTx(@Nonnull final String name, @Nonnull final ThrowingRunnable action) {
            this.name = name;
            this.action = action;
        }
    }

    public static void verifySlidingWindowInvariant(@Nonnull final FDBRecordStore recordStore,
                                                    @Nonnull final String indexName,
                                                    final int windowSize,
                                                    @Nonnull final IndexPredicate.RowNumberWindowPredicate.Direction direction,
                                                    final long countUpperBound) {
        final Index index = recordStore.getRecordMetaData().getIndex(indexName);
        final IndexPredicate.RowNumberWindowPredicate predicate = findRowNumberWindowPredicate(index.getPredicate());
        assertNotNull(predicate, "index " + indexName + " is missing a RowNumberWindowPredicate");

        final int windowKeyColumnSize = predicate.getOrderingKey().getColumnSize();

        final List<Tuple> entries = scanWindowEntries(recordStore, indexName, null);
        final Tuple boundary = readBoundaryKey(recordStore, indexName, null);
        final long count = readWindowCount(recordStore, indexName, null);
        final List<IndexEntry> delegateEntries = scanValueIndexEntries(recordStore, indexName);
        final Set<Tuple> delegateEntriesForWindow = delegateEntries.stream().map(IndexEntry::getKey).collect(Collectors.toCollection(TreeSet::new));

        if (entries.isEmpty()) {
            assertNull(boundary, "boundary should be null when entries subspace is empty");
            assertEquals(0L, count, "count should be 0 when entries subspace is empty");
            assertTrue(delegateEntries.isEmpty(), "delegate should be empty when entries subspace is empty but contained: " + delegateEntriesForWindow);
            return;
        }

        assertNotNull(boundary, "boundary must not be null when entries subspace is non-empty");
        assertTrue(entries.contains(boundary),
                "boundary " + boundary + " is not present among entries " + entries);

        final Comparator<Tuple> comparator = direction == IndexPredicate.RowNumberWindowPredicate.Direction.ASC
                                             ? Comparator.naturalOrder()
                                             : Comparator.reverseOrder();
        final List<Tuple> inWindow = new ArrayList<>();
        final List<Tuple> overflow = new ArrayList<>();
        for (Tuple entry : entries) {
            // in-window iff entry is better-or-equal-to boundary (good side, inclusive of the boundary itself)
            if (comparator.compare(entry, boundary) <= 0) {
                inWindow.add(entry);
            } else {
                overflow.add(entry);
            }
        }

        final Set<Tuple> inWindowEntries = inWindow.stream()
                //.map(e -> TupleHelpers.subTuple(e, windowKeyColumnSize, e.size()).getLong(0))
                .collect(Collectors.toCollection(TreeSet::new));
        final Set<Tuple> overflowEntries = overflow.stream()
                //.map(e -> TupleHelpers.subTuple(e, windowKeyColumnSize, e.size()).getLong(0))
                .collect(Collectors.toCollection(TreeSet::new));

        assertEquals(inWindowEntries, delegateEntriesForWindow,
                "delegate pks must equal in-window pks (boundary-separates invariant). "
                        + "\nboundary=" + boundary + ", \ninWindow=" + inWindowEntries + ", \ndelegate=" + delegateEntriesForWindow +
                        ", \noverflow=" + overflowEntries);

        for (Tuple overflowEntry : overflowEntries) {
            assertTrue(!delegateEntriesForWindow.contains(overflowEntry),
                    "overflow pk " + overflowEntry + " unexpectedly present in delegate " + delegateEntriesForWindow);
        }

        assertEquals((long) inWindow.size(), count,
                "persisted counter " + count + " does not match in-window size " + inWindow.size());

        assertTrue(count <= countUpperBound,
                "count " + count + " exceeded upper bound " + countUpperBound + " (windowSize=" + windowSize + ")");

        for (Tuple entry : entries) {
            final Tuple pkTuple = TupleHelpers.subTuple(entry, windowKeyColumnSize, entry.size());
            assertTrue(recordStore.recordExists(pkTuple),
                    "orphan entry: window references pk " + pkTuple + " but no such record exists");
        }
    }

    /**
     * Scans the underlying value delegate index and returns the set of primary key longs
     * present in the delegate. Mirrors {@link #scanIndexRecNos(FDBRecordStore, String, Tuple)}
     * but for a value-backed sliding window where the test schema uses a single-column
     * {@code int64} primary key.
     */
    @Nonnull
    public static List<IndexEntry> scanValueIndexEntries(@Nonnull final FDBRecordStore recordStore,
                                                         @Nonnull final String indexName) {
        final Index index = recordStore.getRecordMetaData().getIndex(indexName);
        final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);
        try (var cursor = maintainer.scan(IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)) {
            return cursor.asList().join();
        }
    }

    @Nullable
    private static IndexPredicate.RowNumberWindowPredicate findRowNumberWindowPredicate(@Nullable final IndexPredicate predicate) {
        if (predicate == null) {
            return null;
        }
        if (predicate instanceof IndexPredicate.RowNumberWindowPredicate) {
            return (IndexPredicate.RowNumberWindowPredicate) predicate;
        }
        if (predicate instanceof IndexPredicate.AndPredicate) {
            for (IndexPredicate child : ((IndexPredicate.AndPredicate) predicate).getChildren()) {
                final IndexPredicate.RowNumberWindowPredicate found = findRowNumberWindowPredicate(child);
                if (found != null) {
                    return found;
                }
            }
        }
        return null;
    }

    /**
     * Reads the boundary entry key for the (possibly grouped) sliding window, or {@code null}
     * if no boundary has been established yet (e.g. the window is empty).
     */
    @Nullable
    static Tuple readBoundaryKey(@Nonnull final FDBRecordStore recordStore,
                                 @Nonnull final String indexName,
                                 @Nullable final Tuple groupingKey) {
        final Subspace metaSubspace = metaSubspace(recordStore, indexName, groupingKey);
        final byte[] boundaryKey = metaSubspace.pack(Tuple.from(4));
        final byte[] boundaryBytes = recordStore.ensureContextActive().get(boundaryKey).join();
        if (boundaryBytes == null) {
            return null;
        }
        return Tuple.fromBytes(boundaryBytes);
    }

    /**
     * Range-scans the sliding-window entries subspace and returns every entry key in ascending
     * tuple order. Each entry key is {@code [windowValue..., primaryKey...]}.
     */
    @Nonnull
    static List<Tuple> scanWindowEntries(@Nonnull final FDBRecordStore recordStore,
                                         @Nonnull final String indexName,
                                         @Nullable final Tuple groupingKey) {
        final Subspace entriesSubspace = entriesSubspace(recordStore, indexName, groupingKey);
        return recordStore.ensureContextActive().getRange(entriesSubspace.range()).asList().join()
                .stream()
                .map(kv -> entriesSubspace.unpack(kv.getKey()))
                .collect(Collectors.toList());
    }

    @Nonnull
    private static Subspace metaSubspace(@Nonnull final FDBRecordStore recordStore,
                                         @Nonnull final String indexName,
                                         @Nullable final Tuple groupingKey) {
        final Index index = recordStore.getRecordMetaData().getIndex(indexName);
        Subspace swSubspace = recordStore.indexSlidingWindowSubspace(index);
        if (groupingKey != null) {
            swSubspace = swSubspace.subspace(groupingKey);
        }
        return swSubspace.subspace(Tuple.from()).subspace(Tuple.from(1));
    }

    @Nonnull
    private static Subspace entriesSubspace(@Nonnull final FDBRecordStore recordStore,
                                            @Nonnull final String indexName,
                                            @Nullable final Tuple groupingKey) {
        final Index index = recordStore.getRecordMetaData().getIndex(indexName);
        Subspace swSubspace = recordStore.indexSlidingWindowSubspace(index);
        if (groupingKey != null) {
            swSubspace = swSubspace.subspace(groupingKey);
        }
        return swSubspace.subspace(Tuple.from()).subspace(Tuple.from(0));
    }
}
