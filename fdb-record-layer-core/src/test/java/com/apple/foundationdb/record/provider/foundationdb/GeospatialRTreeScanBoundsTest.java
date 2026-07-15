/*
 * GeospatialRTreeScanBoundsTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.indexes.GeospatialRTreeIndexHelper;
import com.apple.foundationdb.tuple.Tuple;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.provider.foundationdb.GeospatialRTreeScanBounds.decodeCoordinate;
import static com.apple.foundationdb.record.provider.foundationdb.GeospatialRTreeScanBounds.encodeCoordinate;
import static com.apple.foundationdb.record.provider.foundationdb.GeospatialRTreeScanBounds.haversineMeters;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/**
 * Tests for the geodesic and fixed-point logic backing {@link GeospatialRTreeScanBounds}. These exercise the
 * within-distance predicate directly and do not require a running FDB.
 */
class GeospatialRTreeScanBoundsTest {
    private static final long SCALE = 10_000_000L;

    private static RTree.Point point(final double latitude, final double longitude) {
        return new RTree.Point(Tuple.from(encodeCoordinate(latitude, SCALE), encodeCoordinate(longitude, SCALE)));
    }

    // Rectangle ranges are laid out as (lowLat, lowLon, highLat, highLon).
    private static RTree.Rectangle rectangle(final double lowLatitude, final double lowLongitude,
                                             final double highLatitude, final double highLongitude) {
        return new RTree.Rectangle(Tuple.from(encodeCoordinate(lowLatitude, SCALE), encodeCoordinate(lowLongitude, SCALE),
                encodeCoordinate(highLatitude, SCALE), encodeCoordinate(highLongitude, SCALE)));
    }

    @Test
    void encodeCoordinateRoundTripsWithinPrecision() {
        final double latitude = 37.4223878;
        assertThat(decodeCoordinate(encodeCoordinate(latitude, SCALE), SCALE)).isCloseTo(latitude, within(1.0 / SCALE));
    }

    @Test
    void haversineMetersMatchesOneDegreeOfLatitude() {
        // One degree of latitude is a fixed arc length regardless of longitude.
        final double expected = GeospatialRTreeScanBounds.EARTH_RADIUS_METERS * Math.toRadians(1.0);
        assertThat(haversineMeters(0.0, 0.0, 1.0, 0.0)).isCloseTo(expected, within(1.0));
    }

    @Test
    void containsPositionExcludesRectangleCorner() {
        final GeospatialRTreeScanBounds bounds =
                GeospatialRTreeScanBounds.withinDistance(TupleRange.ALL, 0.0, 0.0, 100_000.0, SCALE);
        final double deltaDegrees = Math.toDegrees(100_000.0 / GeospatialRTreeScanBounds.EARTH_RADIUS_METERS);

        // A point well inside the circle is contained.
        assertThat(bounds.containsPosition(point(0.5, 0.0))).isTrue();
        // The corner of the bounding rectangle lies outside the circle (distance ~= sqrt(2) * radius).
        assertThat(bounds.containsPosition(point(deltaDegrees, deltaDegrees))).isFalse();
    }

    @Test
    void overlapsMbrApproximatelyReportsCornerCell() {
        final GeospatialRTreeScanBounds bounds =
                GeospatialRTreeScanBounds.withinDistance(TupleRange.ALL, 0.0, 0.0, 100_000.0, SCALE);
        // The corner cell overlaps the covering rectangle even though the circle excludes its points.
        assertThat(bounds.overlapsMbrApproximately(rectangle(0.85, 0.85, 0.89, 0.89))).isTrue();
        // A cell far outside the covering rectangle is pruned.
        assertThat(bounds.overlapsMbrApproximately(rectangle(10.0, 10.0, 11.0, 11.0))).isFalse();
    }

    @Test
    void straddlingAntimeridianCoversBothSides() {
        final GeospatialRTreeScanBounds bounds =
                GeospatialRTreeScanBounds.withinDistance(TupleRange.ALL, 0.0, 179.9, 100_000.0, SCALE);

        // Eastern segment, just west of +180.
        assertThat(bounds.overlapsMbrApproximately(rectangle(-0.1, 179.85, 0.1, 179.95))).isTrue();
        // Wrapped segment, just east of -180.
        assertThat(bounds.overlapsMbrApproximately(rectangle(-0.1, -179.35, 0.1, -179.25))).isTrue();
        // A cell on the far side of the globe is pruned.
        assertThat(bounds.overlapsMbrApproximately(rectangle(-0.1, 0.0, 0.1, 1.0))).isFalse();

        // A point reached only through the wrap is within the true distance.
        assertThat(bounds.containsPosition(point(0.0, -179.95))).isTrue();
    }

    @Test
    void poleCoverageSpansAllLongitudes() {
        final GeospatialRTreeScanBounds bounds =
                GeospatialRTreeScanBounds.withinDistance(TupleRange.ALL, 89.9, 0.0, 100_000.0, SCALE);

        // With the pole inside the circle, every meridian is in range.
        assertThat(bounds.overlapsMbrApproximately(rectangle(89.5, 150.0, 89.9, 151.0))).isTrue();
        assertThat(bounds.overlapsMbrApproximately(rectangle(89.5, -30.0, 89.9, -29.0))).isTrue();
    }

    @Test
    void withinDistanceRejectsOutOfRangeLatitude() {
        assertThatThrownBy(() -> GeospatialRTreeScanBounds.withinDistance(TupleRange.ALL, 91.0, 0.0, 1000.0, SCALE))
                .isInstanceOf(RecordCoreArgumentException.class);
    }

    @Test
    void getScaleReturnsDefaultAndConfiguredValues() {
        final KeyExpression coordinates = concat(field("lat"), field("lon"));
        final Index defaultIndex = new Index("geo", coordinates, IndexTypes.GEOSPATIAL_RTREE);
        assertThat(GeospatialRTreeIndexHelper.getScale(defaultIndex)).isEqualTo(10_000_000L);

        final Index coarseIndex = new Index("geo", coordinates, IndexTypes.GEOSPATIAL_RTREE,
                Map.of(IndexOptions.GEOSPATIAL_RTREE_PRECISION_DIGITS, "3"));
        assertThat(GeospatialRTreeIndexHelper.getScale(coarseIndex)).isEqualTo(1_000L);
    }

    @Test
    void getGroupingCountReflectsGrouping() {
        final KeyExpression ungrouped = concat(field("lat"), field("lon"));
        assertThat(GeospatialRTreeIndexHelper.getGroupingCount(ungrouped)).isEqualTo(0);

        final KeyExpression grouped = concat(field("lat"), field("lon")).groupBy(field("region"));
        assertThat(GeospatialRTreeIndexHelper.getGroupingCount(grouped)).isEqualTo(1);
    }
}
