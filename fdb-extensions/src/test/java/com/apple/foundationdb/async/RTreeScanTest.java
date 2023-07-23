/*
 * RTreeScanTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.async;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBTestBase;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.NetworkOptions;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.async.RTreeModificationTest.Item;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.PathUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Streams;
import org.davidmoten.hilbert.HilbertCurve;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Tests for {@link RTree}.
 */
@Tag(Tags.RequiresFDB)
public class RTreeScanTest extends FDBTestBase {
    private static final Logger logger = LoggerFactory.getLogger(RTreeScanTest.class);

    private static final int NUM_SAMPLES = 10_000;

    private static final int NUM_QUERIES = 100;
    private static Database db;
    private static DirectorySubspace rtSubspace;
    @Nullable
    private static Item[] items;

    private static final boolean TRACE = false;

    @BeforeAll
    public static void setUpDb() throws Exception {
        FDB fdb = FDB.instance();
        if (TRACE) {
            NetworkOptions options = fdb.options();
            options.setTraceEnable("/tmp");
            options.setTraceLogGroup("RTreeTest");
        }
        db = fdb.open();
        rtSubspace = DirectoryLayer.getDefault().createOrOpen(db, PathUtil.from(RTree.class.getSimpleName())).get();
        db.run(tr -> {
            tr.clear(Range.startsWith(rtSubspace.getKey()));
            return null;
        });
        items = RTreeModificationTest.randomInserts(db, rtSubspace, NUM_SAMPLES);
    }

    @AfterAll
    public static void closeDb() {
        db.close();
    }

    @Nonnull
    public static Stream<Arguments> queries() {
        final Random random = new Random(1);
        final ImmutableList.Builder<Arguments> argumentsBuilder = ImmutableList.builder();
        for (int i = 0; i < NUM_QUERIES; i ++) {
            int area = random.nextInt(1000 * 1000);

            int min = area / 1000 + 1;
            int width = random.nextInt(1000 - min + 1) + min;
            int height = area / width;

            long xLow = random.nextInt(1000 - width + 1);
            long yLow = random.nextInt(1000 - height + 1);

            argumentsBuilder.add(Arguments.of(new RTree.Rectangle(Tuple.from(xLow, yLow, xLow + width, yLow + height))));
        }
        return argumentsBuilder.build().stream();
    }

    @ParameterizedTest
    @MethodSource("queries")
    public void queryWithFilters(@Nonnull final RTree.Rectangle query) {
        final Predicate<RTree.Rectangle> mbrPredicate =
                rectangle -> rectangle.isOverlapping(query);

        int numPointsSatisfyingQuery = 0;
        int numPointsSatisfyingQueryX = 0;
        int numPointsSatisfyingQueryY = 0;
        final long queryLowX = ((Number)query.getLow(0)).longValue();
        final long queryHighX = ((Number)query.getHigh(0)).longValue();
        final long queryLowY = ((Number)query.getLow(1)).longValue();
        final long queryHighY = ((Number)query.getHigh(1)).longValue();

        for (int i = 0; i < NUM_SAMPLES; i++) {
            final RTree.Point point = Objects.requireNonNull(items)[i].getPoint();
            if (query.contains(point)) {
                numPointsSatisfyingQuery ++;
            }

            final long pointX = ((Number)point.getCoordinate(0)).longValue();
            final long pointY = ((Number)point.getCoordinate(1)).longValue();
            if (queryLowX <= pointX && pointX <= queryHighX) {
                numPointsSatisfyingQueryX ++;
            }

            if (queryLowY <= pointY && pointY <= queryHighY) {
                numPointsSatisfyingQueryY ++;
            }
        }

        final OnReadCounters onReadCounters = new OnReadCounters();
        final RTree rt = new RTree(rtSubspace, ForkJoinPool.commonPool(), RTree.DEFAULT_CONFIG, RTree::newSequentialNodeId, onReadCounters);

        final AtomicLong nresults = new AtomicLong(0L);
        db.run(tr -> {
            AsyncUtil.forEachRemaining(rt.scan(tr, mbrPredicate), itemSlot -> {
                if (query.contains(itemSlot.getPosition())) {
                    nresults.incrementAndGet();
                }
            }).join();
            return null;
        });

        Assertions.assertEquals(numPointsSatisfyingQuery, nresults.get());

        logger.trace("nresults = {}", nresults.get());
        logger.trace("expected nresults = {}", numPointsSatisfyingQuery);
        logger.trace("num points satisfying X range in query = {}", numPointsSatisfyingQueryX);
        logger.trace("num points satisfying Y range in query = {}", numPointsSatisfyingQueryY);

        onReadCounters.logCounters();

        final double ffPredicate = (double)nresults.get() / NUM_SAMPLES;
        logger.trace("ff of predicate = {}", ffPredicate);
        final double ffSargable = (double)onReadCounters.getReadSlotCounter() / NUM_SAMPLES;
        logger.trace("ff of sargable = {}", ffSargable);
        final double overread = (ffSargable / ffPredicate - 1d) * 100d;
        logger.trace("over-read = {}%", overread);
    }

    @SuppressWarnings({"UnstableApiUsage", "ResultOfMethodCallIgnored"})
    @ParameterizedTest
    @MethodSource("queries")
    public void queryTopNWithFilters(@Nonnull final RTree.Rectangle query) {
        final Comparator<Item> itemComparator =
                Comparator.<Item>comparingLong(item -> item.getPoint().getCoordinates().getLong(0))
                        .thenComparing(Item::getKey);
        final MinMaxPriorityQueue<Item> expectedResultsQueue = MinMaxPriorityQueue.orderedBy(itemComparator).maximumSize(5).create();
        final TopNTraversal topNTraversal = new TopNTraversal(query, 5);

        for (int i = 0; i < NUM_SAMPLES; ++i) {
            final RTree.Point point = Objects.requireNonNull(items)[i].getPoint();
            if (query.contains(point)) {
                expectedResultsQueue.add(items[i]);
            }
        }
        final OnReadCounters onReadCounters = new OnReadCounters();
        final RTree rt = new RTree(rtSubspace, ForkJoinPool.commonPool(), RTree.DEFAULT_CONFIG, RTree::newSequentialNodeId, onReadCounters);
        final AtomicLong nresults = new AtomicLong(0L);
        db.run(tr -> {
            AsyncUtil.forEachRemaining(rt.scan(tr, topNTraversal), itemSlot -> {
                if (query.contains(itemSlot.getPosition())) {
                    topNTraversal.addItemSlot(itemSlot);
                    nresults.incrementAndGet();
                }
            }).join();
            return null;
        });

        final List<Item> expectedResults = inOrder(expectedResultsQueue);
        final List<RTree.ItemSlot> topNItemSlots = inOrder(topNTraversal.getQueue());

        Assertions.assertEquals(expectedResults.size(), topNItemSlots.size());
        Streams.zip(expectedResults.stream(), topNItemSlots.stream(),
                (expected, actual) -> {
                    Assertions.assertEquals(expected.getPoint(), actual.getPosition());
                    Assertions.assertEquals(expected.getKey(), actual.getKey());
                    return 1;
                }).allMatch(r -> true);

        onReadCounters.logCounters();
    }

    //
    // Helpers
    //
    private static <T> List<T> inOrder(Queue<T> queue) {
        final ImmutableList.Builder<T> resultBuilder = ImmutableList.builder();
        while (!queue.isEmpty()) {
            resultBuilder.add(queue.poll());
        }
        return resultBuilder.build();
    }

    @SuppressWarnings("UnstableApiUsage")
    private static class TopNTraversal implements Predicate<RTree.Rectangle> {
        private static final Comparator<RTree.ItemSlot> comparator =
                Comparator.<RTree.ItemSlot>comparingLong(itemSlot -> itemSlot.getPosition().getCoordinates().getLong(0))
                        .thenComparing(itemSlot -> itemSlot.getKey());

        @Nonnull
        private RTree.Rectangle query;
        private final int num;
        @Nonnull
        private final MinMaxPriorityQueue<RTree.ItemSlot> queue;

        @SuppressWarnings("UnstableApiUsage")
        public TopNTraversal(@Nonnull final RTree.Rectangle query, final int num) {
            this.query = query;
            this.num = num;
            this.queue = MinMaxPriorityQueue.orderedBy(comparator).maximumSize(num).create();
        }

        @Nonnull
        public MinMaxPriorityQueue<RTree.ItemSlot> getQueue() {
            return queue;
        }

        @Override
        public boolean test(final RTree.Rectangle rectangle) {
            return rectangle.isOverlapping(query);
        }

        public void addItemSlot(@Nonnull final RTree.ItemSlot itemSlot) {
            queue.add(itemSlot);

            if (queue.size() == num) {
                final RTree.ItemSlot maximumItemSlot = Objects.requireNonNull(queue.peekLast());

                if (comparator.compare(maximumItemSlot, itemSlot) >= 0) {
                    // maximum item slot must be somewhere between minX and maxX
                    final Tuple ranges = query.getRanges();
                    final Tuple newRanges = Tuple.from(ranges.get(0), ranges.get(1), maximumItemSlot.getPosition().getCoordinate(0), ranges.get(3));
                    this.query = new RTree.Rectangle(newRanges);
                }
            }
        }
    }

    @Test
    void name() {
        final HilbertCurve hc = HilbertCurve.bits(63).dimensions(2);
        System.out.println(hc.index( 0, 1));
        System.out.println(index(63, 0, 1));

        //System.out.println(hc.index( Long.MAX_VALUE, 0));
        System.out.println(index(64, Long.MAX_VALUE, 0));
        System.out.println(index(64, Long.MAX_VALUE, 1));

        System.out.println(shiftCoordinate(-1));
        System.out.println(shiftCoordinate(0));
        System.out.println(shiftCoordinate(1));
        System.out.println(shiftCoordinate(Long.MIN_VALUE));
        System.out.println(shiftCoordinate(Long.MAX_VALUE));

        System.out.println("==========================");
        System.out.println(index(64, shiftCoordinate(Long.MIN_VALUE), shiftCoordinate(Long.MIN_VALUE)));
        System.out.println(index(64, shiftCoordinate(Long.MAX_VALUE), shiftCoordinate(Long.MIN_VALUE)));
    }

    static long shiftCoordinate(long coordinate) {
        return coordinate < 0
               ? (Long.MAX_VALUE - (-coordinate - 1L))
               : (coordinate | (1L << 63));
    }

    public static BigInteger index(int bits, long... point) {
        return toIndex(bits, transposedIndex(bits, point));
    }

    static BigInteger toIndex(int bits, long... transposedIndex) {
        int length = bits * transposedIndex.length;
        byte[] b = new byte[length / 8 + 1];
        int bIndex = length - 1;
        long mask = 1L << (bits - 1);
        for (int i = 0; i < bits; i++) {
            for (int j = 0; j < transposedIndex.length; j++) {
                if ((transposedIndex[j] & mask) != 0) {
                    b[b.length - 1 - bIndex / 8] |= 1 << (bIndex % 8);
                }
                bIndex--;
            }
            mask >>>= 1;
        }
        // b is expected to be BigEndian
        return new BigInteger(1, b);
    }

    static long[] transposedIndex(int bits, long... unsignedPoint) {
        final long M = 1L << (bits - 1);
        final int n = unsignedPoint.length; // n: Number of dimensions
        final long[] x = Arrays.copyOf(unsignedPoint, n);
        long p;
        long q;
        long t;
        int i;
        // Inverse undo
        for (q = M; q != 1; q >>>= 1) {
            p = q - 1;
            for (i = 0; i < n; i++) {
                if ((x[i] & q) != 0) {
                    x[0] ^= p; // invert
                } else {
                    t = (x[0] ^ x[i]) & p;
                    x[0] ^= t;
                    x[i] ^= t;
                }
            }
        } // exchange
        // Gray encode
        for (i = 1; i < n; i++) {
            x[i] ^= x[i - 1];
        }
        t = 0;
        for (q = M; q != 1; q >>>= 1) {
            if ((x[n - 1] & q) != 0) {
                t ^= q - 1;
            }
        }
        for (i = 0; i < n; i++) {
            x[i] ^= t;
        }

        return x;
    }

    static class OnReadCounters implements RTree.OnReadListener {
        private final AtomicLong readSlotCounter = new AtomicLong(0);
        private final AtomicLong readLeafSlotCounter = new AtomicLong(0);
        private final AtomicLong readIntermediateSlotCounter = new AtomicLong(0);

        private final AtomicLong readNodesCounter = new AtomicLong(0);
        private final AtomicLong readLeafNodesCounter = new AtomicLong(0);
        private final AtomicLong readIntermediateNodesCounter = new AtomicLong(0);

        public void resetCounters() {
            readSlotCounter.set(0L);
            readLeafSlotCounter.set(0L);
            readIntermediateSlotCounter.set(0L);
            readNodesCounter.set(0L);
            readLeafNodesCounter.set(0L);
            readIntermediateNodesCounter.set(0L);
        }

        public long getReadSlotCounter() {
            return readSlotCounter.get();
        }

        public long getReadLeafSlotCounter() {
            return readLeafSlotCounter.get();
        }

        public long getReadIntermediateSlotCounter() {
            return readIntermediateSlotCounter.get();
        }

        public long getReadNodesCounter() {
            return readNodesCounter.get();
        }

        public long getReadLeafNodesCounter() {
            return readLeafNodesCounter.get();
        }

        public long getReadIntermediateNodesCounter() {
            return readIntermediateNodesCounter.get();
        }

        public void logCounters() {
            logger.info("num read slots = {}", readSlotCounter.get());
            logger.info("num read leaf slots = {}", readLeafSlotCounter.get());
            logger.info("num read intermediate slots = {}", readIntermediateSlotCounter.get());
            logger.info("num read nodes = {}", readNodesCounter.get());
            logger.info("num read leaf nodes = {}", readLeafNodesCounter.get());
            logger.info("num read intermediate nodes = {}", readIntermediateNodesCounter.get());
        }

        @Override
        public void onRead(@Nonnull final byte[] nodeId, @Nonnull final RTree.Kind nodeKind,
                           @Nonnull final List<KeyValue> keyValues) {
            readSlotCounter.addAndGet(keyValues.size());
            readNodesCounter.incrementAndGet();
            if (nodeKind == RTree.Kind.LEAF) {
                readLeafSlotCounter.addAndGet(keyValues.size());
                readLeafNodesCounter.incrementAndGet();
            } else {
                Verify.verify(nodeKind == RTree.Kind.INTERMEDIATE);
                readIntermediateSlotCounter.addAndGet(keyValues.size());
                readIntermediateNodesCounter.incrementAndGet();
            }
        }
    }
}
