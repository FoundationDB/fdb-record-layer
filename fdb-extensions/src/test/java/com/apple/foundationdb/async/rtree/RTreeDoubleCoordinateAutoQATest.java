/*
 * RTreeDoubleCoordinateAutoQATest.java
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

package com.apple.foundationdb.async.rtree;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.test.TestDatabaseExtension;
import com.apple.foundationdb.test.TestExecutors;
import com.apple.foundationdb.test.TestSubspaceExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Additional tests for {@code double} coordinates in {@link RTree} and {@link RTreeHilbertCurveHelpers}.
 */
@Tag(Tags.RequiresFDB)
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class RTreeDoubleCoordinateAutoQATest {
    private static final double[] SPECIAL_DOUBLES = {
            Double.NEGATIVE_INFINITY, -Double.MAX_VALUE, -1.0E300, -180.0, -1.0, -Double.MIN_VALUE, -0.0,
            0.0, Double.MIN_VALUE, 1.0E-300, 1.0, 90.0, 1.0E300, Double.MAX_VALUE, Double.POSITIVE_INFINITY,
            Double.NaN};

    @RegisterExtension
    static final TestDatabaseExtension dbExtension = new TestDatabaseExtension();
    @RegisterExtension
    TestSubspaceExtension rtSubspace = new TestSubspaceExtension(dbExtension);
    @RegisterExtension
    TestSubspaceExtension rtSecondarySubspace = new TestSubspaceExtension(dbExtension);

    private Database db;

    @BeforeEach
    void setUpDb() {
        db = dbExtension.getDatabase();
    }

    static Stream<Arguments> configs() {
        return Stream.of(
                Arguments.of(RTree.newConfigBuilder().setMinM(4).setMaxM(8).setUseNodeSlotIndex(false)
                        .setStorage(RTree.Storage.BY_SLOT).build()),
                Arguments.of(RTree.newConfigBuilder().setMinM(4).setMaxM(8).setUseNodeSlotIndex(true)
                        .setStorage(RTree.Storage.BY_SLOT).build()),
                Arguments.of(RTree.newConfigBuilder().setMinM(4).setMaxM(8).setUseNodeSlotIndex(false)
                        .setStorage(RTree.Storage.BY_NODE).build()),
                Arguments.of(RTree.newConfigBuilder().setMinM(4).setMaxM(8).setUseNodeSlotIndex(true)
                        .setStorage(RTree.Storage.BY_NODE).setStoreHilbertValues(false).build()));
    }

    @ParameterizedTest
    @MethodSource("configs")
    void insertScanDeleteSpecialDoublePoints(@Nonnull final RTree.Config config) {
        // inserts 2D points built from special doubles (+-0.0, +-inf, NaN, extremes), validates the tree,
        // checks the full scan returns everything in (Hilbert value, key) order, then deletes all
        final RTree rTree = newRTree(config);
        final List<RTreeModificationTest.Item> items = new ArrayList<>();
        int i = 0;
        for (final double x : SPECIAL_DOUBLES) {
            for (final double y : SPECIAL_DOUBLES) {
                items.add(new RTreeModificationTest.Item(new RTree.Point(Tuple.from(x, y)), Tuple.from(i),
                        Tuple.from("v" + i)));
                i++;
            }
        }
        insert(rTree, items);
        rTree.validate(db);

        final List<ItemSlot> scanned = scanAll(rTree);
        Assertions.assertThat(scanned).hasSize(items.size());
        assertHilbertOrder(scanned);
        Assertions.assertThat(scanned.stream().map(ItemSlot::getKeySuffix).collect(Collectors.toSet()))
                .hasSize(items.size());

        db.run(tr -> {
            for (final RTreeModificationTest.Item item : items) {
                rTree.delete(tr, item.getPoint(), item.getKeySuffix()).join();
            }
            return null;
        });
        Assertions.assertThat(scanAll(rTree)).isEmpty();
        assertStorageEmpty(rTree);
    }

    @ParameterizedTest
    @MethodSource("configs")
    void boxScanWithDoublesMatchesBruteForce(@Nonnull final RTree.Config config) {
        // a scan pruned by Rectangle.isOverlapping must find every point contained in the query box,
        // also for points with negative coordinates and -0.0
        final RTree rTree = newRTree(config);
        final Random random = new Random(42);
        final List<RTreeModificationTest.Item> items = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            final double x = random.nextInt(5) == 0 ? -0.0 : (random.nextDouble() - 0.5) * 4.0;
            final double y = random.nextInt(5) == 0 ? 0.0 : (random.nextDouble() - 0.5) * 4.0;
            items.add(new RTreeModificationTest.Item(new RTree.Point(Tuple.from(x, y)), Tuple.from(i),
                    Tuple.from("v" + i)));
        }
        insert(rTree, items);
        rTree.validate(db);

        final RTree.Rectangle query = new RTree.Rectangle(Tuple.from(-1.0, -0.0, 0.0, 1.0));
        final List<Tuple> expected = items.stream()
                .filter(item -> query.contains(item.getPoint()))
                .map(RTreeModificationTest.Item::getKeySuffix)
                .collect(Collectors.toList());
        final List<Tuple> actual = db.run(tr -> AsyncUtil.collectRemaining(
                rTree.scan(tr, mbr -> mbr.isOverlapping(query), (l, h) -> true)).join())
                .stream()
                .filter(itemSlot -> query.contains(itemSlot.getPosition()))
                .map(ItemSlot::getKeySuffix)
                .collect(Collectors.toList());
        Assertions.assertThat(expected).isNotEmpty();
        Assertions.assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
    }

    @ParameterizedTest
    @MethodSource("configs")
    void mixedLongDoubleAndNullCoordinates(@Nonnull final RTree.Config config) {
        // coordinates of one dimension mixing Long, Double and null must still give a valid tree that
        // scans in Hilbert order and resumes correctly from (lastHilbertValue, lastKey)
        final RTree rTree = newRTree(config);
        final Random random = new Random(7);
        final List<RTreeModificationTest.Item> items = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            items.add(new RTreeModificationTest.Item(new RTree.Point(Tuple.from(randomCoordinate(random),
                    randomCoordinate(random))), Tuple.from(i), Tuple.from("v" + i)));
        }
        insert(rTree, items);
        rTree.validate(db);

        final List<ItemSlot> all = scanAll(rTree);
        Assertions.assertThat(all).hasSize(items.size());
        assertHilbertOrder(all);

        final ItemSlot middle = all.get(all.size() / 2);
        final List<ItemSlot> rest = db.run(tr -> AsyncUtil.collectRemaining(
                rTree.scan(tr, middle.getHilbertValue(), middle.getKey(), mbr -> true, (l, h) -> true)).join());
        // the scan resumes at leaf granularity; like RTreeIndexHelper, the caller drops items up to the watermark
        Assertions.assertThat(rest.stream()
                        .filter(itemSlot -> itemSlot.compareHilbertValueAndKey(middle.getHilbertValue(), middle.getKey()) > 0)
                        .map(ItemSlot::getKeySuffix).collect(Collectors.toList()))
                .containsExactlyElementsOf(all.subList(all.size() / 2 + 1, all.size()).stream()
                        .map(ItemSlot::getKeySuffix).collect(Collectors.toList()));

        db.run(tr -> {
            for (final RTreeModificationTest.Item item : items) {
                rTree.delete(tr, item.getPoint(), item.getKeySuffix()).join();
            }
            return null;
        });
        assertStorageEmpty(rTree);
    }

    @Test
    void hilbertValueOrdersNaNAndInfinitiesLikeDoubleCompare() {
        // toSortableLong promises Double.compare ordering; in 1D the Hilbert value must then order
        // -inf < -MAX and MAX < +inf < NaN
        final double[] values = {Double.NEGATIVE_INFINITY, -Double.MAX_VALUE, Double.MAX_VALUE,
                Double.POSITIVE_INFINITY, Double.NaN};
        BigInteger previous = null;
        for (final double value : values) {
            final BigInteger current = RTreeHilbertCurveHelpers.hilbertValue(new RTree.Point(Tuple.from(value)));
            if (previous != null) {
                Assertions.assertThat(current).isGreaterThan(previous);
            }
            previous = current;
        }
    }

    private static Object randomCoordinate(@Nonnull final Random random) {
        switch (random.nextInt(4)) {
            case 0:
                return null;
            case 1:
                return (long)(random.nextInt(21) - 10);
            default:
                return (double)(random.nextInt(21) - 10) + (random.nextBoolean() ? 0.5 : 0.0);
        }
    }

    @Nonnull
    private RTree newRTree(@Nonnull final RTree.Config config) {
        return new RTree(rtSubspace.getSubspace(), rtSecondarySubspace.getSubspace(),
                TestExecutors.defaultThreadPool(), config, RTreeHilbertCurveHelpers::hilbertValue,
                NodeHelpers::newSequentialNodeId, OnWriteListener.NOOP, OnReadListener.NOOP);
    }

    private void insert(@Nonnull final RTree rTree, @Nonnull final List<RTreeModificationTest.Item> items) {
        db.run(tr -> {
            for (final RTreeModificationTest.Item item : items) {
                rTree.insertOrUpdate(tr, item.getPoint(), item.getKeySuffix(), item.getValue()).join();
            }
            return null;
        });
    }

    @Nonnull
    private List<ItemSlot> scanAll(@Nonnull final RTree rTree) {
        return db.run(tr -> AsyncUtil.collectRemaining(rTree.scan(tr, mbr -> true, (l, h) -> true)).join());
    }

    private static void assertHilbertOrder(@Nonnull final List<ItemSlot> slots) {
        for (int i = 1; i < slots.size(); i++) {
            final ItemSlot previous = slots.get(i - 1);
            final ItemSlot current = slots.get(i);
            Assertions.assertThat(current.compareHilbertValueAndKey(previous.getHilbertValue(), previous.getKey()))
                    .as("slot %s after %s", current, previous)
                    .isPositive();
        }
    }

    private void assertStorageEmpty(@Nonnull final RTree rTree) {
        final List<KeyValue> keyValues =
                db.run(tr -> tr.getRange(Range.startsWith(rTree.getStorageAdapter().getSubspace().getKey())).asList().join());
        Assertions.assertThat(keyValues).isEmpty();
        final Subspace secondarySubspace = rTree.getStorageAdapter().getSecondarySubspace();
        if (secondarySubspace != null) {
            final List<KeyValue> secondaryKeyValues =
                    db.run(tr -> tr.getRange(Range.startsWith(secondarySubspace.getKey())).asList().join());
            Assertions.assertThat(secondaryKeyValues).isEmpty();
        }
    }
}
