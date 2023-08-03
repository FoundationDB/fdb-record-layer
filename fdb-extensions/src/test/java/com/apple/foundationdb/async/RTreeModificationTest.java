/*
 * RTreeModificationTest.java
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
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.PathUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Tests for {@link RTree}.
 */
@Tag(Tags.RequiresFDB)
public class RTreeModificationTest extends FDBTestBase {
    private static final int NUM_TEST_RUNS = 10;
    private static final int NUM_SAMPLES = 10_000;

    private Database db;
    private DirectorySubspace rtSubspace;

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
    }

    @AfterEach
    public void closeDb() {
        db.close();
    }

    @ParameterizedTest
    @ValueSource(ints = {10, 100, 1000, 10_000})
    public void testAllDeleted(final int numSamples) {
        final Item[] items = randomInserts(db, rtSubspace, numSamples);
        final RTreeScanTest.OnReadCounters onReadCounters = new RTreeScanTest.OnReadCounters();
        final RTree rt = new RTree(rtSubspace, ForkJoinPool.commonPool(), RTree.DEFAULT_CONFIG,
                RTreeHilbertCurveHelpers::hilbertValue, RTree::newSequentialNodeId, RTree.OnWriteListener.NOOP,
                onReadCounters);
        validateRTree(db, rt);
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
                    rt.delete(tr, items[index].getPoint(), items[index].getKeySuffix()).join();
                }

                return j;
            });
        }

        final AtomicLong nresults = new AtomicLong(0);
        db.run(tr -> {
            AsyncUtil.forEachRemaining(rt.scan(tr, mbr -> true), itemSlot -> nresults.incrementAndGet()).join();
            return null;
        });

        Assertions.assertEquals(0, nresults.get());

        // Check that there are no slots left that may have gotten orphaned
        final List<KeyValue> keyValues =
                db.run(tr -> tr.getRange(Range.startsWith(rt.getStorageAdapter().getSubspace().getKey())).asList().join());
        Assertions.assertTrue(keyValues.isEmpty());

        validateRTree(db, rt);
        onReadCounters.resetCounters();
    }

    @ParameterizedTest
    @MethodSource("numSamplesAndNumDeletes")
    public void testRandomDeletes(final int numSamples, final int numDeletes) {
        final Item[] items = randomInserts(db, rtSubspace, numSamples);
        final RTreeScanTest.OnReadCounters onReadCounters = new RTreeScanTest.OnReadCounters();
        final RTree rt = new RTree(rtSubspace, ForkJoinPool.commonPool(), RTree.DEFAULT_CONFIG,
                RTreeHilbertCurveHelpers::hilbertValue, RTree::newSequentialNodeId, RTree.OnWriteListener.NOOP,
                onReadCounters);
        validateRTree(db, rt);
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
                    rt.delete(tr, items[index].getPoint(), items[index].getKeySuffix()).join();
                }

                return j;
            });
        }

        final AtomicLong nresults = new AtomicLong(0);
        db.run(tr -> {
            AsyncUtil.forEachRemaining(rt.scan(tr, mbr -> true), itemSlot -> nresults.incrementAndGet()).join();
            return null;
        });
        Assertions.assertEquals(numSamples - numDeletes, nresults.get());

        validateRTree(db, rt);
        onReadCounters.resetCounters();
    }

    //
    // Helpers
    //

    public static Stream<Arguments> numSamplesAndNumDeletes() {
        final Random random = new Random(1);
        final ImmutableList.Builder<Arguments> argumentsBuilder = ImmutableList.builder();
        for (int i = 0; i < NUM_TEST_RUNS; i ++) {
            final int numSamples = random.nextInt(NUM_SAMPLES + 1);
            final int numDeletes = random.nextInt(numSamples + 1);
            argumentsBuilder.add(Arguments.of(numSamples, numDeletes));
        }
        return argumentsBuilder.build().stream();
    }

    static Item[] randomInserts(@Nonnull final Database db, @Nonnull final DirectorySubspace rtSubspace, int numSamples) {
        final Random random = new Random(0);
        final Item[] items = new Item[numSamples];
        for (int i = 0; i < numSamples; ++i) {
            final RTree.Point point = new RTree.Point(Tuple.from((long)random.nextInt(1000), (long)random.nextInt(1000)));
            items[i] = new Item(point, Tuple.from(i), Tuple.from("value" + i));
        }

        insertData(db, rtSubspace, items);
        return items;
    }

    static Item[] randomInsertsWithNulls(@Nonnull final Database db, @Nonnull final DirectorySubspace rtSubspace, int numSamples) {
        final Random random = new Random(0);
        final Item[] items = new Item[numSamples];
        for (int i = 0; i < numSamples; ++i) {
            final Long x = random.nextFloat() < 0.01 ? null : (Long)(long)random.nextInt(1000);
            final Long y = random.nextFloat() < 0.01 ? null : (Long)(long)random.nextInt(1000);

            final RTree.Point point = new RTree.Point(Tuple.from(x, y));
            items[i] = new Item(point, Tuple.from(i), Tuple.from("value" + i));
        }

        insertData(db, rtSubspace, items);
        return items;
    }

    static Item[] bitemporalInserts(@Nonnull final Database db, @Nonnull final DirectorySubspace rtSubspace, int numSamples) {
        final int smear = 100;
        final Random random = new Random(0);
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

        insertData(db, rtSubspace, items);
        return items;
    }

    static void insertData(final @Nonnull Database db, final @Nonnull DirectorySubspace rtSubspace, @Nonnull final Item[] items) {
        final RTree rt = new RTree(rtSubspace, ForkJoinPool.commonPool(), RTreeHilbertCurveHelpers::hilbertValue);
        final int numInsertsPerBatch = 1_000;
        for (int i = 0; i < items.length; ) {
            final int batchStart = i; // lambdas
            i += db.run(tr -> {
                int j;
                for (j = 0; j < numInsertsPerBatch; j ++) {
                    final int index = batchStart + j;
                    if (index == items.length) {
                        break;
                    }
                    rt.insertOrUpdate(tr, items[index].getPoint(), items[index].getKeySuffix(), items[index].getValue()).join();
                }

                return j;
            });
        }
    }

    static void validateRTree(@Nonnull final Database db, @Nonnull final RTree rt) {
        db.run(tr -> {
            rt.validate(tr).join();
            return null;
        });
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
