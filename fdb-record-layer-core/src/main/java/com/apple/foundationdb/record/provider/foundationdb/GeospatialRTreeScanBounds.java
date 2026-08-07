/*
 * GeospatialRTreeScanBounds.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.rtree.RTree;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * {@link IndexScanBounds} for the {@link IndexScanType#BY_DISTANCE} scan of a geospatial R-tree index: points within a
 * great-circle distance of a center. Coordinates are ordered latitude (dimension {@code 0}) then longitude (dimension
 * {@code 1}), both stored as fixed-point {@code long}s at the index's precision scale.
 *
 * <p>
 * The query circle is over-approximated by one or more axis-aligned rectangles that together contain it: one in the
 * common case, two when the circle straddles the antimeridian (±180° longitude), and a full-longitude band when the
 * circle reaches a pole. The disjunction of these rectangles drives R-tree node pruning
 * ({@link #overlapsMbrApproximately}). Because the rectangles over-cover (their corners lie outside the circle),
 * {@link #containsPosition} applies the exact great-circle (haversine) distance as a per-point filter.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public class GeospatialRTreeScanBounds implements IndexScanBounds {
    // Mean Earth radius (meters), matching the haversine distance used for the exact filter.
    public static final double EARTH_RADIUS_METERS = 6_371_008.8;

    private static final double MIN_LATITUDE = -90.0;
    private static final double MAX_LATITUDE = 90.0;
    private static final double MIN_LONGITUDE = -180.0;
    private static final double MAX_LONGITUDE = 180.0;
    private static final double FULL_CIRCLE_DEGREES = 360.0;

    @Nonnull
    private final TupleRange prefixRange;
    @Nonnull
    private final TupleRange suffixRange;
    private final double centerLatitude;
    private final double centerLongitude;
    private final double radiusMeters;
    private final long scale;
    @Nonnull
    private final List<FixedRectangle> rectangles;

    private GeospatialRTreeScanBounds(@Nonnull final TupleRange prefixRange,
                                      final double centerLatitude,
                                      final double centerLongitude,
                                      final double radiusMeters,
                                      final long scale,
                                      @Nonnull final TupleRange suffixRange) {
        if (centerLatitude < MIN_LATITUDE || centerLatitude > MAX_LATITUDE) {
            throw new RecordCoreArgumentException("center latitude out of range")
                    .addLogInfo(LogMessageKeys.VALUE, centerLatitude);
        }
        this.prefixRange = prefixRange;
        this.suffixRange = suffixRange;
        this.centerLatitude = centerLatitude;
        this.centerLongitude = normalizeLongitude(centerLongitude);
        this.radiusMeters = radiusMeters;
        this.scale = scale;
        this.rectangles = computeRectangles(this.centerLatitude, this.centerLongitude, radiusMeters, scale);
    }

    /**
     * Create scan bounds selecting points within {@code radiusMeters} of a center.
     * @param prefixRange range constraining the grouping columns (use {@link TupleRange#ALL} when the index is not
     *        grouped)
     * @param centerLatitude center latitude in degrees, in {@code [-90, 90]}
     * @param centerLongitude center longitude in degrees; normalized into {@code [-180, 180]}
     * @param radiusMeters great-circle radius in meters; non-positive values match only the exact center cell
     * @param scale the index precision scale (see {@link #encodeCoordinate(double, long)})
     * @param suffixRange range constraining the key suffix (typically {@link TupleRange#ALL})
     * @return the scan bounds
     */
    @Nonnull
    public static GeospatialRTreeScanBounds withinDistance(@Nonnull final TupleRange prefixRange,
                                                           final double centerLatitude,
                                                           final double centerLongitude,
                                                           final double radiusMeters,
                                                           final long scale,
                                                           @Nonnull final TupleRange suffixRange) {
        return new GeospatialRTreeScanBounds(prefixRange, centerLatitude, centerLongitude, radiusMeters, scale,
                suffixRange);
    }

    /**
     * Convenience overload that does not constrain the key suffix.
     * @param prefixRange range constraining the grouping columns
     * @param centerLatitude center latitude in degrees
     * @param centerLongitude center longitude in degrees
     * @param radiusMeters great-circle radius in meters
     * @param scale the index precision scale
     * @return the scan bounds
     */
    @Nonnull
    public static GeospatialRTreeScanBounds withinDistance(@Nonnull final TupleRange prefixRange,
                                                           final double centerLatitude,
                                                           final double centerLongitude,
                                                           final double radiusMeters,
                                                           final long scale) {
        return withinDistance(prefixRange, centerLatitude, centerLongitude, radiusMeters, scale, TupleRange.ALL);
    }

    @Nonnull
    @Override
    public IndexScanType getScanType() {
        return IndexScanType.BY_DISTANCE;
    }

    @Nonnull
    public TupleRange getPrefixRange() {
        return prefixRange;
    }

    @Nonnull
    public TupleRange getSuffixRange() {
        return suffixRange;
    }

    /**
     * Test whether a node's minimum bounding rectangle overlaps the query circle's covering rectangles. May report
     * false positives (the corners of a covering rectangle lie outside the circle); those are removed by
     * {@link #containsPosition}.
     * @param mbr the node minimum bounding rectangle, in fixed-point coordinates
     * @return {@code true} if the R-tree should descend into this node
     */
    public boolean overlapsMbrApproximately(@Nonnull final RTree.Rectangle mbr) {
        final long mbrLowLat = ((Number)mbr.getLow(0)).longValue();
        final long mbrHighLat = ((Number)mbr.getHigh(0)).longValue();
        final long mbrLowLon = ((Number)mbr.getLow(1)).longValue();
        final long mbrHighLon = ((Number)mbr.getHigh(1)).longValue();
        for (final FixedRectangle rectangle : rectangles) {
            if (rectangle.overlaps(mbrLowLat, mbrHighLat, mbrLowLon, mbrHighLon)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Test whether a stored point lies within the query circle using the exact great-circle distance.
     * @param position the stored point, in fixed-point coordinates
     * @return {@code true} if the point is within the radius
     */
    public boolean containsPosition(@Nonnull final RTree.Point position) {
        final Object latitudeObject = position.getCoordinate(0);
        final Object longitudeObject = position.getCoordinate(1);
        if (latitudeObject == null || longitudeObject == null) {
            return false;
        }
        final double latitude = decodeCoordinate(((Number)latitudeObject).longValue(), scale);
        final double longitude = decodeCoordinate(((Number)longitudeObject).longValue(), scale);
        return haversineMeters(centerLatitude, centerLongitude, latitude, longitude) <= radiusMeters;
    }

    /**
     * Convert a degree value to the fixed-point {@code long} stored in the R-tree.
     * @param degrees the coordinate in degrees
     * @param scale the index precision scale
     * @return the fixed-point representation
     */
    public static long encodeCoordinate(final double degrees, final long scale) {
        return Math.round(degrees * scale);
    }

    /**
     * Recover the degree value from its fixed-point {@code long} representation. Lossy below the index precision.
     * @param fixed the fixed-point coordinate
     * @param scale the index precision scale
     * @return the coordinate in degrees
     */
    public static double decodeCoordinate(final long fixed, final long scale) {
        return (double)fixed / (double)scale;
    }

    /**
     * Great-circle distance between two lat/lon points using the haversine formula. Correct across the antimeridian.
     * @param latitude1 first point latitude in degrees
     * @param longitude1 first point longitude in degrees
     * @param latitude2 second point latitude in degrees
     * @param longitude2 second point longitude in degrees
     * @return the distance in meters
     */
    public static double haversineMeters(final double latitude1, final double longitude1,
                                         final double latitude2, final double longitude2) {
        final double phi1 = Math.toRadians(latitude1);
        final double phi2 = Math.toRadians(latitude2);
        final double sinHalfDPhi = Math.sin(Math.toRadians(latitude2 - latitude1) / 2.0);
        final double sinHalfDLambda = Math.sin(Math.toRadians(longitude2 - longitude1) / 2.0);
        final double a = sinHalfDPhi * sinHalfDPhi + Math.cos(phi1) * Math.cos(phi2) * sinHalfDLambda * sinHalfDLambda;
        return 2.0 * EARTH_RADIUS_METERS * Math.asin(Math.min(1.0, Math.sqrt(a)));
    }

    // Fold an arbitrary longitude into [-180, 180) branchlessly (a floating-point loop counter would be flagged).
    private static double normalizeLongitude(final double longitude) {
        return longitude - FULL_CIRCLE_DEGREES * Math.floor((longitude - MIN_LONGITUDE) / FULL_CIRCLE_DEGREES);
    }

    @Nonnull
    private static List<FixedRectangle> computeRectangles(final double centerLatitude,
                                                          final double centerLongitude,
                                                          final double radiusMeters,
                                                          final long scale) {
        final double angularRadius = Math.max(0.0, radiusMeters) / EARTH_RADIUS_METERS;
        final double deltaLatitudeDegrees = Math.toDegrees(angularRadius);
        double lowLatitude = centerLatitude - deltaLatitudeDegrees;
        double highLatitude = centerLatitude + deltaLatitudeDegrees;
        final boolean coversPole = highLatitude >= MAX_LATITUDE || lowLatitude <= MIN_LATITUDE;
        lowLatitude = Math.max(lowLatitude, MIN_LATITUDE);
        highLatitude = Math.min(highLatitude, MAX_LATITUDE);

        final ImmutableList.Builder<FixedRectangle> builder = ImmutableList.builder();
        final double cosCenterLatitude = Math.cos(Math.toRadians(centerLatitude));
        // A circle that reaches a pole (or is wide enough that cos(lat) can no longer bound its longitude extent)
        // spans every meridian, so a single full-longitude band covers it.
        final double sinRatio = cosCenterLatitude <= 0.0
                                ? Double.POSITIVE_INFINITY
                                : Math.sin(angularRadius) / cosCenterLatitude;
        if (coversPole || angularRadius >= Math.PI / 2.0 || sinRatio >= 1.0) {
            builder.add(new FixedRectangle(lowLatitude, highLatitude, MIN_LONGITUDE, MAX_LONGITUDE, scale));
            return builder.build();
        }

        final double deltaLongitudeDegrees = Math.toDegrees(Math.asin(sinRatio));
        final double westLongitude = centerLongitude - deltaLongitudeDegrees;
        final double eastLongitude = centerLongitude + deltaLongitudeDegrees;
        if (westLongitude < MIN_LONGITUDE) {
            // Wraps across the antimeridian to the west: split into the wrapped eastern segment and the near segment.
            builder.add(new FixedRectangle(lowLatitude, highLatitude,
                    westLongitude + FULL_CIRCLE_DEGREES, MAX_LONGITUDE, scale));
            builder.add(new FixedRectangle(lowLatitude, highLatitude, MIN_LONGITUDE, eastLongitude, scale));
        } else if (eastLongitude > MAX_LONGITUDE) {
            builder.add(new FixedRectangle(lowLatitude, highLatitude, westLongitude, MAX_LONGITUDE, scale));
            builder.add(new FixedRectangle(lowLatitude, highLatitude,
                    MIN_LONGITUDE, eastLongitude - FULL_CIRCLE_DEGREES, scale));
        } else {
            builder.add(new FixedRectangle(lowLatitude, highLatitude, westLongitude, eastLongitude, scale));
        }
        return builder.build();
    }

    /**
     * A fixed-point axis-aligned rectangle over (latitude, longitude), inclusive on all sides.
     */
    private static final class FixedRectangle {
        private final long lowLatitude;
        private final long highLatitude;
        private final long lowLongitude;
        private final long highLongitude;

        private FixedRectangle(final double lowLatitude, final double highLatitude,
                               final double lowLongitude, final double highLongitude, final long scale) {
            this.lowLatitude = encodeCoordinate(lowLatitude, scale);
            this.highLatitude = encodeCoordinate(highLatitude, scale);
            this.lowLongitude = encodeCoordinate(lowLongitude, scale);
            this.highLongitude = encodeCoordinate(highLongitude, scale);
        }

        private boolean overlaps(final long mbrLowLat, final long mbrHighLat,
                                 final long mbrLowLon, final long mbrHighLon) {
            return mbrHighLat >= lowLatitude && mbrLowLat <= highLatitude &&
                   mbrHighLon >= lowLongitude && mbrLowLon <= highLongitude;
        }
    }
}
