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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Random;

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

    @ParameterizedTest
    @ValueSource(longs = {1L, 10L, 1_000L, 10_000_000L})
    void overlapsMbrApproximatelyNeverPrunesContainedPoint(final long scale) {
        // Randomized: any stored point that containsPosition accepts must also survive MBR pruning.
        final Random random = new Random(0x5eedL + scale);
        for (int i = 0; i < 2_000; i++) {
            final double centerLatitude = random.nextDouble() * 180.0 - 90.0;
            final double centerLongitude = random.nextDouble() * 360.0 - 180.0;
            final double radius = Math.pow(10.0, 1.0 + random.nextDouble() * 6.5);
            final GeospatialRTreeScanBounds bounds = GeospatialRTreeScanBounds.withinDistance(TupleRange.ALL,
                    centerLatitude, centerLongitude, radius, scale);
            final double spreadDegrees = Math.min(360.0, 2.0 * Math.toDegrees(radius / GeospatialRTreeScanBounds.EARTH_RADIUS_METERS) + 2.0);
            for (int j = 0; j < 50; j++) {
                final double latitude = Math.max(-90.0, Math.min(90.0,
                        centerLatitude + (random.nextDouble() - 0.5) * spreadDegrees));
                double longitude = centerLongitude + (random.nextDouble() - 0.5) * spreadDegrees * 4.0;
                longitude = longitude - 360.0 * Math.floor((longitude + 180.0) / 360.0);
                final RTree.Point stored = point(latitude, longitude, scale);
                if (bounds.containsPosition(stored)) {
                    assertThat(bounds.overlapsMbrApproximately(pointMbr(stored)))
                            .as("center (%s, %s) radius %s point (%s, %s) scale %s",
                                    centerLatitude, centerLongitude, radius, latitude, longitude, scale)
                            .isTrue();
                }
            }
        }
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

    @Test
    void withinDistanceRadiusBeyondHalfCircumferenceMatchesAntipode() {
        // A radius larger than half the earth's circumference covers the whole globe, including the antipode.
        final double halfCircumference = Math.PI * GeospatialRTreeScanBounds.EARTH_RADIUS_METERS;
        final GeospatialRTreeScanBounds bounds =
                GeospatialRTreeScanBounds.withinDistance(TupleRange.ALL, 30.0, 45.0, halfCircumference + 1_000.0, SCALE);
        final RTree.Point antipode = point(-30.0, -135.0, SCALE);
        assertThat(bounds.containsPosition(antipode)).isTrue();
        assertThat(bounds.overlapsMbrApproximately(pointMbr(antipode))).isTrue();
        assertThat(bounds.overlapsMbrApproximately(pointMbr(point(-90.0, 180.0, SCALE)))).isTrue();
    }
}
