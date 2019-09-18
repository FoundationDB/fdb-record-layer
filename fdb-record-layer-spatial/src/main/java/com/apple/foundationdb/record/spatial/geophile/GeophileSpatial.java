/*
 * GeophileSpatial.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.spatial.geophile;

import com.apple.foundationdb.annotation.API;
import com.geophile.z.Space;
import com.geophile.z.SpatialObject;
import com.geophile.z.spatialobject.jts.JTS;
import com.geophile.z.spatialobject.jts.JTSSpatialObject;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequenceFilter;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import org.locationtech.jts.io.geojson.GeoJsonReader;
import org.locationtech.jts.io.geojson.GeoJsonWriter;
import org.slf4j.bridge.SLF4JBridgeHandler;

import javax.annotation.Nonnull;

/**
 * Helper methods for interfacing with Geophile, specifically for implementing geospatial with lat, lon coordinates.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("checkstyle:abbreviationaswordinname")    // Allow WKB, WKT
class GeophileSpatial {
    /*
      The lat/lon coordinate system is

      - latitude: -90.0 to +90.0
      - longitude: -180.0 to 180.0 with wraparound

      The interleave pattern is [lon, lat, lon, lat, ..., lon], reflecting the fact that longitude covers a numeric range
      twice that of latitude.
    */

    public static final int LAT_LON_DIMENSIONS = 2;
    public static final double MIN_LAT = -90;
    public static final double MAX_LAT = 90;
    public static final double MIN_LON = -180;
    public static final double MAX_LON = 180;
    private static final int LAT_BITS = 28;
    private static final int LON_BITS = 29;

    private GeophileSpatial() {
    }

    static {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();
        java.util.logging.Logger.getLogger("").setLevel(java.util.logging.Level.FINEST);
    }

    /**
     * The default latitude, longitude space.
     * @return a new space
     */
    public static Space createLatLonSpace() {
        int[] interleave = new int[LAT_BITS + LON_BITS];
        int dimension = 1; // Start with lon, as described above
        for (int d = 0; d < LAT_BITS + LON_BITS; d++) {
            interleave[d] = dimension;
            dimension = 1 - dimension;
        }
        return Space.newSpace(new double[]{MIN_LAT, MIN_LON},
                              new double[]{MAX_LAT, MAX_LON},
                              new int[]{LAT_BITS, LON_BITS},
                              interleave);
    }

    public static long shuffle(Space space, double x, double y) {
        com.geophile.z.spatialobject.d2.Point point = new com.geophile.z.spatialobject.d2.Point(x, y);
        long[] zValues = new long[1];
        space.decompose(point, zValues);
        return zValues[0];
    }

    public static void shuffle(Space space, SpatialObject spatialObject, long[] zs) {
        space.decompose(spatialObject, zs);
    }

    public static String serializeGeoJson(JTSSpatialObject spatialObject) {
        return io.get().geoJsonWriter().write(spatialObject.geometry());
    }

    public static SpatialObject deserializeGeoJson(Space space, String string) throws ParseException {
        return deserializeGeoJson(space, string, false);
    }

    public static SpatialObject deserializeGeoJson(Space space, String string, boolean swapLatLong) throws ParseException {
        Geometry geometry = io.get().geoJsonReader().read(string);
        if (swapLatLong) {
            geometry = swapLatLong(geometry);
        }
        return
            geometry instanceof Point
            ? JTS.spatialObject(space, (Point) geometry)
            : JTS.spatialObject(space, geometry);
    }

    public static byte[] serializeWKB(JTSSpatialObject spatialObject) {
        return io.get().wkbWriter().write(spatialObject.geometry());
    }

    public static SpatialObject deserializeWKB(Space space, byte[] bytes) throws ParseException {
        return deserializeWKB(space, bytes, false);
    }

    public static SpatialObject deserializeWKB(Space space, byte[] bytes, boolean swapLatLong) throws ParseException {
        Geometry geometry = io.get().wkbReader().read(bytes);
        if (swapLatLong) {
            geometry = swapLatLong(geometry);
        }
        return
            geometry instanceof Point
            ? JTS.spatialObject(space, (Point) geometry)
            : JTS.spatialObject(space, geometry);
    }

    public static String serializeWKT(JTSSpatialObject spatialObject) {
        return io.get().wktWriter().write(spatialObject.geometry());
    }

    public static SpatialObject deserializeWKT(Space space, String string) throws ParseException {
        return deserializeWKT(space, string, false);
    }

    public static SpatialObject deserializeWKT(Space space, String string, boolean swapLatLong) throws ParseException {
        Geometry geometry = io.get().wktReader().read(string);
        if (swapLatLong) {
            geometry = swapLatLong(geometry);
        }
        return
            geometry instanceof Point
            ? JTS.spatialObject(space, (Point) geometry)
            : JTS.spatialObject(space, geometry);
    }

    // Wrap thread-unsafe I/O classes in a thread-local.

    private static final ThreadLocal<IO> io = new ThreadLocal<IO>() {
            @Override
            protected IO initialValue() {
                return new IO();
            }
        };

    private static class IO {
        private final GeometryFactory factory = new GeometryFactory();
        private GeoJsonReader geoJsonReader;
        private GeoJsonWriter geoJsonWriter;
        private WKBReader wkbReader;
        private WKBWriter wkbWriter;
        private WKTReader wktReader;
        private WKTWriter wktWriter;

        public GeoJsonReader geoJsonReader() {
            if (geoJsonReader == null) {
                geoJsonReader = new GeoJsonReader(factory);
            }
            return geoJsonReader;
        }

        public GeoJsonWriter geoJsonWriter() {
            if (geoJsonWriter == null) {
                geoJsonWriter = new GeoJsonWriter();
            }
            return geoJsonWriter;
        }

        public WKBReader wkbReader() {
            if (wkbReader == null) {
                wkbReader = new WKBReader(factory);
            }
            return wkbReader;
        }

        public WKBWriter wkbWriter() {
            if (wkbWriter == null) {
                wkbWriter = new WKBWriter();
            }
            return wkbWriter;
        }

        public WKTReader wktReader() {
            if (wktReader == null) {
                wktReader = new WKTReader(factory);
            }
            return wktReader;
        }

        public WKTWriter wktWriter() {
            if (wktWriter == null) {
                wktWriter = new WKTWriter();
            }
            return wktWriter;
        }

    }

    /**
     * Convert long/lat to lat/long.
     *
     * Many JTS serialized forms are the former and the default space is the latter.
     * @param geometry the geometry to be transformed
     * @return the transformed geometry
     */
    public static Geometry swapLatLong(@Nonnull Geometry geometry) {
        geometry.apply(new CoordinateSequenceFilter() {
            @Override
            public void filter(CoordinateSequence seq, int i) {
                double x = seq.getCoordinate(i).x;
                double y = seq.getCoordinate(i).y;
                seq.setOrdinate(i, 0, y);
                seq.setOrdinate(i, 1, x);
            }

            @Override
            public boolean isGeometryChanged() {
                return true;
            }

            @Override
            public boolean isDone() {
                return false;
            }
        });
        return geometry;
    }

}
