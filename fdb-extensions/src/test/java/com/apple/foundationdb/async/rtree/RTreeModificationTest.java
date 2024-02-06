/*
 * RTreeModificationTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.async.rtree;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBTestBase;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.NetworkOptions;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.PathUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Tests testing insert/update/deletes of data into/in/from {@link RTree}s.
 */
@Tag(Tags.RequiresFDB)
@Tag(Tags.Slow)
public class RTreeModificationTest extends FDBTestBase {
    private static final Logger logger = LoggerFactory.getLogger(RTreeModificationTest.class);

    private static final int NUM_TEST_RUNS = 5;
    private static final int NUM_SAMPLES = 10_000;

    private Database db;
    private DirectorySubspace rtSubspace;
    private DirectorySubspace rtSecondarySubspace;

    private static final boolean TRACE = false;

    @BeforeEach
    public void setUpDb() throws Exception {
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
        rtSecondarySubspace = DirectoryLayer.getDefault().createOrOpen(db, PathUtil.from(RTree.class.getSimpleName(), "secondary")).get();
        db.run(tr -> {
            tr.clear(Range.startsWith(rtSecondarySubspace.getKey()));
            return null;
        });
    }

    @AfterEach
    public void closeDb() {
        db.close();
    }

    @ParameterizedTest
    @MethodSource("numSamplesAndSeeds")
    public void testAllDeleted(final RTree.Config config, final long seed, final int numSamples) {
        final RTreeScanTest.OnWriteCounters onWriteCounters = new RTreeScanTest.OnWriteCounters();
        final RTreeScanTest.OnReadCounters onReadCounters = new RTreeScanTest.OnReadCounters();

        final RTree rTree = new RTree(rtSubspace, rtSecondarySubspace, ForkJoinPool.commonPool(), config,
                RTreeHilbertCurveHelpers::hilbertValue, NodeHelpers::newSequentialNodeId, onWriteCounters,
                onReadCounters);
        final long startTs = System.nanoTime();
        final Item[] items = randomInserts(db, rTree, seed, numSamples);
        final long endTs = System.nanoTime();
        onWriteCounters.logCounters();
        onReadCounters.logCounters();
        validateRTree(db, rTree);
        onWriteCounters.resetCounters();
        onReadCounters.resetCounters();

        final int numDeletesPerBatch = 1_000;
        for (int i = 0; i < numSamples; ) {
            final int batchStart = i; // lambdas
            i += db.run(tr -> {
                int j;
                for (j = 0; j < numDeletesPerBatch; j ++) {
                    final int index = batchStart + j;
                    if (index == numSamples) {
                        break;
                    }
                    rTree.delete(tr, items[index].getPoint(), items[index].getKeySuffix()).join();
                }

                return j;
            });
        }

        final AtomicLong nresults = new AtomicLong(0);
        db.run(tr -> {
            AsyncUtil.forEachRemaining(rTree.scan(tr, mbr -> true, (l, h) -> true), itemSlot -> nresults.incrementAndGet()).join();
            return null;
        });

        onWriteCounters.logCounters();
        onReadCounters.logCounters();

        Assertions.assertEquals(0, nresults.get());

        // Check that there are no slots left that may have gotten orphaned
        List<KeyValue> keyValues =
                db.run(tr -> tr.getRange(Range.startsWith(rTree.getStorageAdapter().getSubspace().getKey())).asList().join());
        Assertions.assertTrue(keyValues.isEmpty());

        final Subspace secondarySubspace = rTree.getStorageAdapter().getSecondarySubspace();
        if (secondarySubspace != null) {
            keyValues = db.run(tr -> tr.getRange(Range.startsWith(secondarySubspace.getKey())).asList().join());
            Assertions.assertTrue(keyValues.isEmpty());
        }

        validateRTree(db, rTree);
    }

    @ParameterizedTest
    @MethodSource("numSamplesAndNumDeletes")
    public void testRandomDeletes(@Nonnull final RTree.Config config, final long seed, final int numSamples, final int numDeletes) {
        final RTreeScanTest.OnReadCounters onReadCounters = new RTreeScanTest.OnReadCounters();
        final RTree rTree = new RTree(rtSubspace, rtSecondarySubspace, ForkJoinPool.commonPool(), config,
                RTreeHilbertCurveHelpers::hilbertValue, NodeHelpers::newSequentialNodeId, OnWriteListener.NOOP,
                onReadCounters);
        final Item[] items = randomInserts(db, rTree, seed, numSamples);
        validateRTree(db, rTree);
        onReadCounters.resetCounters();

        final int numDeletesPerBatch = 1_000;
        for (int i = 0; i < numDeletes; ) {
            final int batchStart = i; // lambdas
            i += db.run(tr -> {
                int j;
                for (j = 0; j < numDeletesPerBatch; j ++) {
                    final int index = batchStart + j;
                    if (index == numDeletes) {
                        break;
                    }
                    rTree.delete(tr, items[index].getPoint(), items[index].getKeySuffix()).join();
                }

                return j;
            });
        }

        final AtomicLong nresults = new AtomicLong(0);
        db.run(tr -> {
            AsyncUtil.forEachRemaining(rTree.scan(tr, mbr -> true, (l, h) -> true), itemSlot -> nresults.incrementAndGet()).join();
            return null;
        });
        Assertions.assertEquals(numSamples - numDeletes, nresults.get());

        validateRTree(db, rTree);
        onReadCounters.resetCounters();
    }

    @Test
    void dumpRTree() {
        final OnReadListener onReadListener = new OnReadListener() {
            @Override
            public <T extends Node> CompletableFuture<T> onAsyncRead(@Nonnull final CompletableFuture<T> future) {
                return future.thenApply(node -> {
                    if (node instanceof IntermediateNode) {
                        final IntermediateNode intermediateNode = (IntermediateNode)node;
                        IntermediateNode parentNode = intermediateNode.getParentNode();
                        int depth = 0;
                        while (parentNode != null) {
                            depth ++;
                            parentNode = parentNode.getParentNode();
                        }
                        parentNode = intermediateNode.getParentNode();
                        if (parentNode != null) {
                            final ChildSlot childSlot = parentNode.getSlot(intermediateNode.getSlotIndexInParent());
                            logger.info(depth + "," + childSlot.getMbr().toPlotString());
                        } else {
                            logger.info(depth + "," + "everything");
                        }
                    }
                    return node;
                });
            }
        };

        final RTree rTree = new RTree(rtSubspace, rtSecondarySubspace, ForkJoinPool.commonPool(),
                new RTree.ConfigBuilder().build(),
                RTreeHilbertCurveHelpers::hilbertValue, NodeHelpers::newSequentialNodeId, OnWriteListener.NOOP,
                onReadListener);

        bitemporalInserts(db, rTree, 1, 10_000);
        validateRTree(db, rTree);
    }

    //
    // Helpers
    //

    public static Stream<Arguments> numSamplesAndSeeds() {
        final Random random = new Random(0);
        final ImmutableList.Builder<Arguments> argumentsBuilder = ImmutableList.builder();
        for (int i = 0; i < NUM_TEST_RUNS; i ++) {
            final int numSamples = random.nextInt(NUM_SAMPLES) + 1;
            final long seed = random.nextLong();
            argumentsBuilder.add(Arguments.of(new RTree.ConfigBuilder().setMinM(16).setMaxM(32).setUseNodeSlotIndex(false).setStorage(RTree.Storage.BY_SLOT).build(), seed, numSamples));
            argumentsBuilder.add(Arguments.of(new RTree.ConfigBuilder().setMinM(16).setMaxM(32).setUseNodeSlotIndex(true).setStorage(RTree.Storage.BY_SLOT).build(), seed, numSamples));
            argumentsBuilder.add(Arguments.of(new RTree.ConfigBuilder().setMinM(16).setMaxM(32).setUseNodeSlotIndex(false).setStorage(RTree.Storage.BY_NODE).build(), seed, numSamples));
            argumentsBuilder.add(Arguments.of(new RTree.ConfigBuilder().setMinM(16).setMaxM(32).setUseNodeSlotIndex(true).setStorage(RTree.Storage.BY_NODE).build(), seed, numSamples));
        }
        return argumentsBuilder.build().stream();
    }

    public static Stream<Arguments> numSamplesAndNumDeletes() {
        final Random random = new Random(System.currentTimeMillis());
        final ImmutableList.Builder<Arguments> argumentsBuilder = ImmutableList.builder();
        for (int i = 0; i < NUM_TEST_RUNS; i ++) {
            final int numSamples = random.nextInt(NUM_SAMPLES + 1);
            final int numDeletes = random.nextInt(numSamples + 1);
            final long seed = random.nextLong();
            argumentsBuilder.add(Arguments.of(new RTree.ConfigBuilder().setUseNodeSlotIndex(false).setMinM(4).setMaxM(8).setStorage(RTree.Storage.BY_SLOT).build(), seed, numSamples, numDeletes));
            argumentsBuilder.add(Arguments.of(new RTree.ConfigBuilder().setUseNodeSlotIndex(true).setMinM(4).setMaxM(8).setStorage(RTree.Storage.BY_SLOT).build(), seed, numSamples, numDeletes));
            argumentsBuilder.add(Arguments.of(new RTree.ConfigBuilder().setUseNodeSlotIndex(false).setMinM(4).setMaxM(8).setStorage(RTree.Storage.BY_NODE).build(), seed, numSamples, numDeletes));
            argumentsBuilder.add(Arguments.of(new RTree.ConfigBuilder().setUseNodeSlotIndex(true).setMinM(4).setMaxM(8).setStorage(RTree.Storage.BY_NODE).build(), seed, numSamples, numDeletes));
        }
        return argumentsBuilder.build().stream();
    }

    static Item[] randomInserts(@Nonnull final Database db, @Nonnull final RTree rTree, final long seed,
                                final int numSamples) {
        final Random random = new Random(seed);
        final Item[] items = new Item[numSamples];
        for (int i = 0; i < numSamples; ++i) {
            final RTree.Point point = new RTree.Point(Tuple.from((long)random.nextInt(1000), (long)random.nextInt(1000)));
            items[i] = new Item(point, Tuple.from(i), Tuple.from("value" + i));
        }

        insertData(db, rTree, items);
        return items;
    }

    static Item[] randomInsertsWithNulls(@Nonnull final Database db, @Nonnull final RTree rTree,
                                         final long seed, final int numSamples) {
        final Random random = new Random(seed);
        final Item[] items = new Item[numSamples];
        for (int i = 0; i < numSamples; ++i) {
            final Long x = random.nextFloat() < 0.01 ? null : (Long)(long)random.nextInt(1000);
            final Long y = random.nextFloat() < 0.01 ? null : (Long)(long)random.nextInt(1000);

            final RTree.Point point = new RTree.Point(Tuple.from(x, y));
            items[i] = new Item(point, Tuple.from(i), Tuple.from("value" + i));
        }

        insertData(db, rTree, items);
        return items;
    }

    static Item[] bitemporalInserts(@Nonnull final Database db, @Nonnull RTree rTree, final long seed, int numSamples) {
        final int smear = 100;
        final Random random = new Random(seed);
        final Item[] items = new Item[numSamples];

        final double step = (double)1000 / numSamples;
        double current = 0.0d;
        for (int i = 0; i < numSamples; ++i) {
            long x;
            long y;
            do {
                x = (int)current + random.nextInt(2 * smear) - smear;
                y = (int)current + random.nextInt(2 * smear) - smear;
            } while (x < 0 || y < 0 || x > 1000 || y > 1000);

            final RTree.Point point = new RTree.Point(Tuple.from(x, y));
            items[i] = new Item(point, Tuple.from(i), Tuple.from("value" + i));

            current += step;
        }

        insertData(db, rTree, items);
        return items;
    }

    static void insertData(@Nonnull final Database db, @Nonnull final RTree rTree, @Nonnull final Item[] items) {
        final int numInsertsPerBatch = 1_000;
        for (int i = 0; i < items.length; ) {
            final int batchStart = i; // lambdas
            final int numRecordsInserted = db.run(tr -> {
                int j;
                for (j = 0; j < numInsertsPerBatch; j ++) {
                    final int index = batchStart + j;
                    if (index == items.length) {
                        break;
                    }
                    rTree.insertOrUpdate(tr, items[index].getPoint(), items[index].getKeySuffix(), items[index].getValue()).join();
                }

                return j;
            });
            i += numRecordsInserted;
            logger.info("batch of data inserted; numRecordsInserted = {}, totalNumRecordsInserted = {}", numRecordsInserted, i);
        }
    }

    static void validateRTree(@Nonnull final Database db, @Nonnull final RTree rt) {
        rt.validate(db);
    }

    static class Item {
        @Nonnull
        private final RTree.Point point;
        @Nonnull
        private final Tuple keySuffix;
        @Nonnull
        private final Tuple value;

        public Item(@Nonnull final RTree.Point point, @Nonnull final Tuple keySuffix, @Nonnull final Tuple value) {
            this.point = point;
            this.keySuffix = keySuffix;
            this.value = value;
        }

        @Nonnull
        public RTree.Point getPoint() {
            return point;
        }

        @Nonnull
        public Tuple getKeySuffix() {
            return keySuffix;
        }

        @Nonnull
        public Tuple getValue() {
            return value;
        }
    }
}
