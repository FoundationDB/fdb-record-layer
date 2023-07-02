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
import com.apple.foundationdb.directory.PathUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.base.Verify;
import org.davidmoten.hilbert.HilbertCurve;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.math.BigInteger;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
 * Tests for {@link RankedSet}.
 */
@Tag(Tags.RequiresFDB)
public class RTreeTest extends FDBTestBase {
    private static final Logger logger = LoggerFactory.getLogger(RTreeTest.class);
    private Database db;
    private Subspace rtSubspace;

    private RTree.Config config = RTree.DEFAULT_CONFIG;
    private static final boolean TRACE = false;

    @BeforeEach
    public void setUp() throws Exception {
        FDB fdb = FDB.instance();
        if (TRACE) {
            NetworkOptions options = fdb.options();
            options.setTraceEnable("/tmp");
            options.setTraceLogGroup("RTreeTest");
        }
        this.db = fdb.open();
        this.rtSubspace = DirectoryLayer.getDefault().createOrOpen(db, PathUtil.from(getClass().getSimpleName())).get();
        db.run(tr -> {
            tr.clear(rtSubspace.range());
            return null;
        });
    }

    @ParameterizedTest
    @ValueSource(ints = { 100000 })
    public void randomInserts(int numSamples) {
        final HilbertCurve hc = HilbertCurve.bits(63).dimensions(2);
        final Random r = new Random(0);
        final Tuple[] keys = new Tuple[numSamples];
        final Tuple[] values = new Tuple[numSamples];
        final RTree.Point[] points = new RTree.Point[numSamples];
        final BigInteger[] hvs = new BigInteger[numSamples];
        final RTree.Rectangle query = new RTree.Rectangle(Tuple.from(250, 250, 750, 750));
        //final RTree.Rectangle query = new RTree.Rectangle(Tuple.from(0, 0, 500, 500));
        final Predicate<RTree.Rectangle> mbrPredicate =
                rectangle -> rectangle.isOverlapping(query);

        int numPointsSatisfyingQuery = 0;
        int numPointsSatisfyingQueryX = 0;
        int numPointsSatisfyingQueryY = 0;
        final long queryLowX = ((Number)query.getLow(0)).longValue();
        final long queryHighX = ((Number)query.getHigh(0)).longValue();
        final long queryLowY = ((Number)query.getLow(1)).longValue();
        final long queryHighY = ((Number)query.getHigh(1)).longValue();

        for (int i = 0; i < numSamples; ++i) {
            keys[i] = Tuple.from(i);
            values[i] = Tuple.from("value" + i);
            points[i] = new RTree.Point(Tuple.from((long)r.nextInt(1000), (long)r.nextInt(1000)));

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

            hvs[i] = hc.index((long)points[i].getCoordinate(0), (long)points[i].getCoordinate(1));
        }

        final InstrumentedRTree rt = newRTree();
        final int numInsertsPerBatch = 1_000;
        for (int i = 0; i < numSamples; ) {
            final int batchStart = i; // lambdas
            i += db.run(tr -> {
                int j;
                for (j = 0; j < numInsertsPerBatch; j ++) {
                    final int index = batchStart + j;
                    if (index == numSamples) {
                        break;
                    }
                    rt.insert(tr, hvs[index], keys[index], values[index], points[index]).join();
                }
                
                return j;
            });
        }



//        db.run(tr -> {
//            AsyncUtil.forEachRemaining(rt.scan(tr, mbr -> true), itemSlot -> {
//                System.out.println(itemSlot.getKey() + "; " + itemSlot.getHilbertValue());
//            }).join();
//            return null;
//        });

        rt.resetCounters();

        final AtomicLong nresults = new AtomicLong(0L);
        db.run(tr -> {
            AsyncUtil.forEachRemaining(rt.scan(tr, mbrPredicate), itemSlot -> {
                nresults.incrementAndGet();
            }).join();
            return null;
        });

        logger.info("nresults = {}", nresults.get());
        logger.info("expected nresults = {}", numPointsSatisfyingQuery);
        logger.info("num points satisfying X range in query = {}", numPointsSatisfyingQueryX);
        logger.info("num points satisfying Y range in query = {}", numPointsSatisfyingQueryY);

        rt.logCounters();
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
}
