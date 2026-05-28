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
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.VectorIndexScanOptions;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
        return Tuple.fromBytes(counterBytes).getLong(0);
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
