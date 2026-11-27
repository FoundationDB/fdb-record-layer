/*
 * GeospatialRTreeScanBoundsAutoQATest.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.async.rtree.RTree;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.tuple.Tuple;
import org.junit.jupiter.api.Test;

import static com.apple.foundationdb.record.provider.foundationdb.GeospatialRTreeScanBounds.encodeCoordinate;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Additional boundary tests for {@link GeospatialRTreeScanBounds}.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class GeospatialRTreeScanBoundsAutoQATest {
    private static final long SCALE = 10_000_000L;

    private static RTree.Point point(final double latitude, final double longitude, final long scale) {
        return new RTree.Point(Tuple.from(encodeCoordinate(latitude, scale), encodeCoordinate(longitude, scale)));
    }

    // Degenerate MBR covering exactly one stored point, laid out as (lowLat, lowLon, highLat, highLon).
    private static RTree.Rectangle pointMbr(final RTree.Point point) {
        return new RTree.Rectangle(Tuple.from(point.getCoordinate(0), point.getCoordinate(1),
                point.getCoordinate(0), point.getCoordinate(1)));
    }

    @Test
    void withinDistanceZeroRadiusAtAntimeridianMatchesCenterStoredAsPlus180() {
        // BUG AQA-15: center longitude +180 is normalized to -180, and with radius 0 the only rectangle is at -180, so a
        // point stored at +180 is pruned although containsPosition accepts it. Spec: overlapsMbrApproximately javadoc
        // (only false positives allowed) and withinDistance javadoc ("exact center cell").
        // A point stored at exactly the requested center (lon = +180) is accepted by the exact filter, so the
        // radius-0 "exact center cell" must not be pruned just because the center was normalized to -180.
        final GeospatialRTreeScanBounds bounds =
                GeospatialRTreeScanBounds.withinDistance(TupleRange.ALL, 10.0, 180.0, 0.0, SCALE);
        final RTree.Point center = point(10.0, 180.0, SCALE);
        assertThat(bounds.containsPosition(center)).isTrue();
        assertThat(bounds.overlapsMbrApproximately(pointMbr(center))).isTrue();
    }
}
