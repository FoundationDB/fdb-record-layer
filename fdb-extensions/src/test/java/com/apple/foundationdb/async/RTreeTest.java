/*
 * RankedSetTest.java
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
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.PathUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.MinMaxPriorityQueue;
import org.davidmoten.hilbert.HilbertCurve;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Tests for {@link RankedSet}.
 */
@Tag(Tags.RequiresFDB)
public class RTreeTest extends FDBTestBase {
    private static final Logger logger = LoggerFactory.getLogger(RTreeTest.class);

    private static final int NUM_SAMPLES = 10_000;

    private static final int NUM_QUERIES = 1;
    private static Database db;
    private static DirectorySubspace rtSubspace;

    private static Tuple[] keys;
    private static Tuple[] values;
    private static RTree.Point[] points;

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
            tr.clear(rtSubspace.range());
            return null;
        });
        //randomInserts(db, rtSubspace);
        bitemporalInserts(db, rtSubspace);
    }

    @AfterAll
    public static void closeDb() {
        db.close();
    }

    public static void randomInserts(@Nonnull final Database db, @Nonnull final DirectorySubspace rtSubspace) {
        final Random random = new Random(0);
        final HilbertCurve hc = HilbertCurve.bits(63).dimensions(2);
        keys = new Tuple[NUM_SAMPLES];
        values = new Tuple[NUM_SAMPLES];
        points = new RTree.Point[NUM_SAMPLES];
        final BigInteger[] hvs = new BigInteger[NUM_SAMPLES];

        for (int i = 0; i < NUM_SAMPLES; ++i) {
            keys[i] = Tuple.from(i);
            values[i] = Tuple.from("value" + i);
            points[i] = new RTree.Point(Tuple.from((long)random.nextInt(1000), (long)random.nextInt(1000)));
            hvs[i] = hc.index((long)points[i].getCoordinate(0), (long)points[i].getCoordinate(1));
        }

        final RTree rt = new RTree(rtSubspace, ForkJoinPool.commonPool());
        final int numInsertsPerBatch = 1_000;
        for (int i = 0; i < NUM_SAMPLES; ) {
            final int batchStart = i; // lambdas
            i += db.run(tr -> {
                int j;
                for (j = 0; j < numInsertsPerBatch; j ++) {
                    final int index = batchStart + j;
                    if (index == NUM_SAMPLES) {
                        break;
                    }
                    rt.insert(tr, points[index], hvs[index], keys[index], values[index]).join();
                }

                return j;
            });
        }
    }

    public static void bitemporalInserts(@Nonnull final Database db, @Nonnull final DirectorySubspace rtSubspace) throws Exception {
        final int smear = 100;
        final Random random = new Random(0);
        final HilbertCurve hc = HilbertCurve.bits(63).dimensions(2);
        keys = new Tuple[NUM_SAMPLES];
        values = new Tuple[NUM_SAMPLES];
        points = new RTree.Point[NUM_SAMPLES];
        final BigInteger[] hvs = new BigInteger[NUM_SAMPLES];

        try (FileWriter fw = new FileWriter("points.csv", false);
                BufferedWriter bw = new BufferedWriter(fw);
                PrintWriter out = new PrintWriter(bw)) {
            final double step = (double)1000 / NUM_SAMPLES;
            double current = 0.0d;
            for (int i = 0; i < NUM_SAMPLES; ++i) {
                long x;
                long y;
                do {
                    x = (int)current + random.nextInt(2 * smear) - smear;
                    y = (int)current + random.nextInt(2 * smear) - smear;
                } while (x < 0 || y < 0 || x > 1000 || y > 1000);
                out.println(x + "," + y);
                keys[i] = Tuple.from(i);
                values[i] = Tuple.from("value" + i);
                points[i] = new RTree.Point(Tuple.from(x, y));
                hvs[i] = hc.index((long)points[i].getCoordinate(0), (long)points[i].getCoordinate(1));
                current += step;
            }
        }

        final RTree rt = new RTree(rtSubspace, ForkJoinPool.commonPool());
        final int numInsertsPerBatch = 1_000;
        for (int i = 0; i < NUM_SAMPLES; ) {
            final int batchStart = i; // lambdas
            i += db.run(tr -> {
                int j;
                for (j = 0; j < numInsertsPerBatch; j ++) {
                    final int index = batchStart + j;
                    if (index == NUM_SAMPLES) {
                        break;
                    }
                    rt.insert(tr, points[index], hvs[index], keys[index], values[index]).join();
                }

                return j;
            });
        }
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
    public void queryWithFilters(@Nonnull final RTree.Rectangle query) throws Exception {
        final Predicate<RTree.Rectangle> mbrPredicate =
                rectangle -> rectangle.isOverlapping(query);

        int numPointsSatisfyingQuery = 0;
        int numPointsSatisfyingQueryX = 0;
        int numPointsSatisfyingQueryY = 0;
        final long queryLowX = ((Number)query.getLow(0)).longValue();
        final long queryHighX = ((Number)query.getHigh(0)).longValue();
        final long queryLowY = ((Number)query.getLow(1)).longValue();
        final long queryHighY = ((Number)query.getHigh(1)).longValue();

        for (int i = 0; i < NUM_SAMPLES; ++i) {
            if (query.contains(points[i])) {
                numPointsSatisfyingQuery ++;
            }

            final long pointX = ((Number)points[i].getCoordinate(0)).longValue();
            final long pointY = ((Number)points[i].getCoordinate(1)).longValue();
            if (queryLowX <= pointX && pointX <= queryHighX) {
                numPointsSatisfyingQueryX ++;
            }

            if (queryLowY <= pointY && pointY <= queryHighY) {
                numPointsSatisfyingQueryY ++;
            }
        }

        final InstrumentedRTree rt = newRTree();
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

//        logger.info("nresults = {}", nresults.get());
//        logger.info("expected nresults = {}", numPointsSatisfyingQuery);
//        logger.info("num points satisfying X range in query = {}", numPointsSatisfyingQueryX);
//        logger.info("num points satisfying Y range in query = {}", numPointsSatisfyingQueryY);

//        rt.logCounters();

        final double ffPredicate = (double)nresults.get() / NUM_SAMPLES;
//        logger.info("ff of predicate = {}", ffPredicate);
        final double ffSargable = (double)rt.getReadSlotCounter() / NUM_SAMPLES;
//        logger.info("ff of sargable = {}", ffSargable);
        final double overread = (ffSargable / ffPredicate - 1d) * 100d;
//        logger.info("over-read = {}%", overread);

        try (FileWriter fw = new FileWriter("myfile.txt", true);
                BufferedWriter bw = new BufferedWriter(fw);
                PrintWriter out = new PrintWriter(bw))
        {
            out.println(NUM_SAMPLES + "," + query.area().doubleValue() / 1000 / 1000 * 100.0d + "," + nresults + "," + numPointsSatisfyingQueryX + "," + numPointsSatisfyingQueryY + "," + rt.getReadSlotCounter());
        }
        //System.err.println(NUM_SAMPLES + "," + nresults + "," + numPointsSatisfyingQueryX + "," + numPointsSatisfyingQueryY + "," + ffPredicate + "," + ffSargable);
    }

    @ParameterizedTest
    @MethodSource("queries")
    public void queryTopNWithFilters(@Nonnull final RTree.Rectangle query) throws Exception {
        final TopNTraversal topNTraversal = new TopNTraversal(query, 3);

        int numPointsSatisfyingQuery = 0;
        int numPointsSatisfyingQueryX = 0;
        int numPointsSatisfyingQueryY = 0;
        final long queryLowX = ((Number)query.getLow(0)).longValue();
        final long queryHighX = ((Number)query.getHigh(0)).longValue();
        final long queryLowY = ((Number)query.getLow(1)).longValue();
        final long queryHighY = ((Number)query.getHigh(1)).longValue();

        for (int i = 0; i < NUM_SAMPLES; ++i) {
            if (query.contains(points[i])) {
                numPointsSatisfyingQuery ++;
            }

            final long pointX = ((Number)points[i].getCoordinate(0)).longValue();
            final long pointY = ((Number)points[i].getCoordinate(1)).longValue();
            if (queryLowX <= pointX && pointX <= queryHighX) {
                numPointsSatisfyingQueryX ++;
            }

            if (queryLowY <= pointY && pointY <= queryHighY) {
                numPointsSatisfyingQueryY ++;
            }
        }

        final InstrumentedRTree rt = newRTree();
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

        topNTraversal.getTopNItemSlots().forEach(System.out::println);

        //Assertions.assertEquals(numPointsSatisfyingQuery, nresults.get());

        final double ffPredicate = (double)nresults.get() / NUM_SAMPLES;
        final double ffSargable = (double)rt.getReadSlotCounter() / NUM_SAMPLES;
        final double overread = (ffSargable / ffPredicate - 1d) * 100d;

        rt.logCounters();

//        try (FileWriter fw = new FileWriter("myfile.txt", true);
//                BufferedWriter bw = new BufferedWriter(fw);
//                PrintWriter out = new PrintWriter(bw))
//        {
//            out.println(NUM_SAMPLES + "," + query.area().doubleValue() / 1000 / 1000 * 100.0d + "," + nresults + "," + numPointsSatisfyingQueryX + "," + numPointsSatisfyingQueryY + "," + rt.getReadSlotCounter());
//        }
    }

    //
    // Helpers
    //

    private InstrumentedRTree newRTree() {
        return new InstrumentedRTree(rtSubspace);
    }

    private static class InstrumentedRTree extends RTree {
        private final AtomicLong readSlotCounter = new AtomicLong(0);
        private final AtomicLong readLeafSlotCounter = new AtomicLong(0);
        private final AtomicLong readIntermediateSlotCounter = new AtomicLong(0);

        private final AtomicLong readNodesCounter = new AtomicLong(0);
        private final AtomicLong readLeafNodesCounter = new AtomicLong(0);
        private final AtomicLong readIntermediateNodesCounter = new AtomicLong(0);

        public InstrumentedRTree(final Subspace subspace) {
            super(subspace, ForkJoinPool.commonPool(), RTree.DEFAULT_CONFIG, RTree::newSequentialNodeId);
            resetCounters();
        }

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

        @Nonnull
        @Override
        protected RTree.Node nodeFromKeyValues(final byte[] nodeId, final List<KeyValue> keyValues) {
            final RTree.Node resultNode = super.nodeFromKeyValues(nodeId, keyValues);
            readSlotCounter.addAndGet(keyValues.size());
            readNodesCounter.incrementAndGet();
            if (resultNode.getKind() == Kind.LEAF) {
                readLeafSlotCounter.addAndGet(keyValues.size());
                readLeafNodesCounter.incrementAndGet();
            } else {
                Verify.verify(resultNode.getKind() == Kind.INTERMEDIATE);
                readIntermediateSlotCounter.addAndGet(keyValues.size());
                readIntermediateNodesCounter.incrementAndGet();
            }
            return resultNode;
        }
    }

    @SuppressWarnings("UnstableApiUsage")
    private static class TopNTraversal implements Predicate<RTree.Rectangle> {
        private static Comparator<RTree.ItemSlot> comparator =
                Comparator.<RTree.ItemSlot>comparingLong(itemSlot -> itemSlot.getPosition().getCoordinates().getLong(0))
                        .thenComparing(itemSlot -> itemSlot.getKey());

        @Nonnull
        private RTree.Rectangle query;
        @Nonnull
        private final int n;
        @Nonnull
        private final MinMaxPriorityQueue<RTree.ItemSlot> topN;

        @SuppressWarnings("UnstableApiUsage")
        public TopNTraversal(@Nonnull final RTree.Rectangle query, @Nonnull final int n) {
            this.query = query;
            this.n = n;
            this.topN = MinMaxPriorityQueue.orderedBy(comparator).maximumSize(n).create();
        }

        @Nonnull
        public List<RTree.ItemSlot> getTopNItemSlots() {
            return new ArrayList<>(topN);
        }

        @Override
        public boolean test(final RTree.Rectangle rectangle) {
            return rectangle.isOverlapping(query);
        }

        public void addItemSlot(@Nonnull final RTree.ItemSlot itemSlot) {
            topN.add(itemSlot);

            if (false && topN.size() == n) {
                final RTree.ItemSlot maximumItemSlot = Objects.requireNonNull(topN.peekLast());

                if (comparator.compare(maximumItemSlot, itemSlot) >= 0) {
                    // maximum item slot must be somewhere between minX and maxX
                    final Tuple ranges = query.getRanges();
                    final Tuple newRanges = Tuple.from(ranges.get(0), ranges.get(1), maximumItemSlot.getPosition().getCoordinate(0), ranges.get(3));
                    this.query = new RTree.Rectangle(newRanges);
                }
            }
        }
    }
}
