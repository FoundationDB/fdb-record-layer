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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanOptions;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test helpers and fluent matchers for sliding-window HNSW indexes.
 * Holds the probe utilities (vector construction, HNSW scan, sliding-window
 * snapshot) and the {@link SlidingWindowAssert} / {@link HnswAssert} fluent DSL.
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
     * Returns a snapshot of the sliding-window state for the ungrouped index.
     */
    @Nonnull
    public static SlidingWindow slidingWindow(@Nonnull final FDBRecordStore recordStore,
                                              @Nonnull final String indexName) {
        return groupedSlidingWindow(recordStore, indexName, null);
    }

    /**
     * Returns a snapshot of the sliding-window state for the given group.
     * Captures both the window counter and the underlying HNSW recNos for that
     * group, so chained assertions on the underlying index stay group-scoped.
     */
    @Nonnull
    public static SlidingWindow groupedSlidingWindow(@Nonnull final FDBRecordStore recordStore,
                                                     @Nonnull final String indexName,
                                                     @Nullable final Tuple groupingKey) {
        return new SlidingWindow(readWindowCount(recordStore, indexName, groupingKey),
                                 scanIndexRecNos(recordStore, indexName, groupingKey));
    }

    /**
     * Reads the persisted window counter for the given (possibly grouped) sliding window.
     * Package-private so other test helpers can reuse it without going through the full
     * {@link SlidingWindow} snapshot helper.
     */
    static long readWindowCount(@Nonnull final FDBRecordStore recordStore,
                                @Nonnull final String indexName,
                                @Nullable final Tuple groupingKey) {
        final Subspace metaSubspace = metaSubspace(recordStore, indexName, groupingKey);
        final byte[] counterKey = metaSubspace.pack(Tuple.from(3));
        final byte[] counterBytes = recordStore.ensureContextActive().get(counterKey).join();
        if (counterBytes == null) {
            return 0L;
        }
        return Tuple.fromBytes(counterBytes).getLong(0);
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

    /**
     * Scans the underlying value delegate index and returns the set of primary key longs
     * present in the delegate. Mirrors {@link #scanIndexRecNos(FDBRecordStore, String, Tuple)}
     * but for a value-backed sliding window where the test schema uses a single-column
     * {@code int64} primary key.
     */
    @Nonnull
    public static Set<Long> scanValueIndexRecNos(@Nonnull final FDBRecordStore recordStore,
                                                 @Nonnull final String indexName) {
        final Index index = recordStore.getRecordMetaData().getIndex(indexName);
        final IndexMaintainer maintainer = recordStore.getIndexMaintainer(index);
        try (var cursor = maintainer.scan(IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)) {
            return cursor.asList().join().stream()
                    .map(IndexEntry::getPrimaryKey)
                    .map(pk -> pk.getLong(0))
                    .collect(Collectors.toSet());
        }
    }

    /**
     * Asserts the central sliding-window invariant: the boundary key separates every entry
     * present in the delegate index from every entry tracked in the entries subspace but
     * not in the delegate.
     *
     * <p>Strict invariants (always asserted):</p>
     * <ul>
     *     <li>If the entries subspace is empty: count == 0, boundary is null, delegate empty.</li>
     *     <li>Otherwise the boundary appears as a real entry in the entries subspace.</li>
     *     <li>The set of primary keys in the delegate index equals the set of primary keys of
     *         "in-window" entries (entries on the good side of the boundary, inclusive).</li>
     *     <li>No "overflow" entry's primary key is present in the delegate.</li>
     *     <li>The persisted counter matches the number of in-window entries.</li>
     *     <li>Every primary key referenced by the entries subspace corresponds to an existing
     *         record in the store (no orphans).</li>
     * </ul>
     *
     * <p>Bounded-drift invariant: the persisted counter may exceed {@code windowSize} because
     * snapshot reads on the counter allow two concurrent inserts to both observe
     * {@code count < windowSize} and both add to the delegate. What must hold is that the
     * counter does not grow unboundedly; the caller supplies a sensible upper bound (e.g.
     * {@code windowSize + numWorkers}).</p>
     *
     * <p>This helper is intended for sliding-window indexes whose underlying delegate is a
     * value index keyed by a single {@code int64} primary key column.</p>
     */
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
        final Set<Long> delegatePks = scanValueIndexRecNos(recordStore, indexName);

        if (entries.isEmpty()) {
            assertNull(boundary, "boundary should be null when entries subspace is empty");
            assertEquals(0L, count, "count should be 0 when entries subspace is empty");
            assertTrue(delegatePks.isEmpty(), "delegate should be empty when entries subspace is empty but contained: " + delegatePks);
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

        final Set<Long> inWindowPks = inWindow.stream()
                .map(e -> TupleHelpers.subTuple(e, windowKeyColumnSize, e.size()).getLong(0))
                .collect(Collectors.toCollection(HashSet::new));
        final Set<Long> overflowPks = overflow.stream()
                .map(e -> TupleHelpers.subTuple(e, windowKeyColumnSize, e.size()).getLong(0))
                .collect(Collectors.toCollection(HashSet::new));

        assertEquals(inWindowPks, delegatePks,
                "delegate pks must equal in-window pks (boundary-separates invariant). "
                        + "boundary=" + boundary + ", inWindow=" + inWindowPks + ", delegate=" + delegatePks);

        for (Long overflowPk : overflowPks) {
            assertTrue(!delegatePks.contains(overflowPk),
                    "overflow pk " + overflowPk + " unexpectedly present in delegate " + delegatePks);
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
     * Snapshot of the sliding-window state. Carries both the window counter and
     * the underlying HNSW recNos so chained assertions like
     * {@code .underlyingHnsw().containsInAnyOrder(...)} stay scoped to the same
     * group.
     */
    record SlidingWindow(long size, @Nonnull Set<Long> hnswRecNos) {
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
         * the {@link HnswAssert} returned by {@link #underlyingHnsw()}).
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
         * Returns a fluent assertion over the HNSW state captured by this probe,
         * scoped to the same group. The {@link #as(String) description} (if any)
         * is propagated.
         */
        @Nonnull
        public HnswAssert underlyingHnsw() {
            return new HnswAssert(window.hnswRecNos(), description);
        }
    }

    /**
     * Fluent assertion over a snapshot of HNSW recNos.
     */
    public static final class HnswAssert {
        @Nonnull
        private final Set<Long> recNos;
        @Nullable
        private final String description;

        HnswAssert(@Nonnull final Set<Long> recNos) {
            this(recNos, null);
        }

        HnswAssert(@Nonnull final Set<Long> recNos, @Nullable final String description) {
            this.recNos = recNos;
            this.description = description;
        }

        /**
         * Attaches a description that will be prefixed to any failure message
         * produced by subsequent assertions in this chain.
         */
        @Nonnull
        public HnswAssert as(@Nonnull final String description) {
            return new HnswAssert(recNos, description);
        }

        @Nonnull
        public HnswAssert containsInAnyOrder(final long... expectedRecNos) {
            final Set<Long> expected = LongStream.of(expectedRecNos).boxed().collect(Collectors.toSet());
            assertEquals(expected, recNos,
                    describe(description, "HNSW should contain " + expected + " but was " + recNos));
            return this;
        }

        @Nonnull
        public HnswAssert contains(final long expectedRecNo) {
            assertTrue(recNos.contains(expectedRecNo),
                    describe(description, "HNSW should contain " + expectedRecNo + " but was " + recNos));
            return this;
        }

        @Nonnull
        public HnswAssert isEmpty() {
            assertTrue(recNos.isEmpty(),
                    describe(description, "HNSW should be empty but contained " + recNos));
            return this;
        }
    }

    @Nonnull
    private static String describe(@Nullable final String description, @Nonnull final String defaultMessage) {
        return description == null ? defaultMessage : description + System.lineSeparator() + defaultMessage;
    }
}
