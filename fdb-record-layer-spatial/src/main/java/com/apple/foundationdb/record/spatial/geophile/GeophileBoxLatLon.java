/*
 * BoxLatLon.java
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
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.geophile.z.SpatialObject;
import com.geophile.z.spatialobject.d2.Box;

/**
 * Spatial objects for boxes with possible wraparound.
 */
@API(API.Status.EXPERIMENTAL)
public class GeophileBoxLatLon {
    private static final double CIRCLE = 360;

    private GeophileBoxLatLon() {
    }

    /**
     * Get a box with possible wraparound.
     * @param latLo low latitude value
     * @param latHi high latitude value
     * @param lonLo low longitude value
     * @param lonHi high longitude value
     * @return a new box representing the area between the given boundaries
     */
    public static SpatialObject newBox(double latLo,
                                       double latHi,
                                       double lonLo,
                                       double lonHi) {
        latLo = fixLat(latLo);
        latHi = fixLat(latHi);
        lonLo = fixLon(lonLo);
        lonHi = fixLon(lonHi);
        try {
            return
                lonLo <= lonHi
                ? new Box(latLo, latHi, lonLo, lonHi)
                : new GeophileBoxLatLonWithWraparound(latLo, latHi, lonLo, lonHi);
        } catch (IllegalArgumentException ex) {
            throw new RecordCoreArgumentException("cannot make box", ex)
                    .addLogInfo("latLo", latLo)
                    .addLogInfo("latHi", latHi)
                    .addLogInfo("lonLo", lonLo)
                    .addLogInfo("lonHi", lonHi);
        }
    }

    // Query boxes are specified as center point +- delta, where delta <= 360.
    // This calculation can put us past min/max lon.
    private static double fixLon(double lon) {
        if (lon > GeophileSpatial.MAX_LON + CIRCLE || lon < GeophileSpatial.MIN_LON - CIRCLE) {
            throw new RecordCoreArgumentException("Invalid longitude").addLogInfo("longitude", lon);
        }
        if (lon < GeophileSpatial.MIN_LON) {
            lon += CIRCLE;
        } else if (lon > GeophileSpatial.MAX_LON) {
            lon -= CIRCLE;
        }
        return lon;
    }

    // Fix lat by truncating at +/-90
    private static double fixLat(double lat) {
        if (lat > GeophileSpatial.MAX_LAT + CIRCLE || lat < GeophileSpatial.MIN_LAT - CIRCLE) {
            throw new RecordCoreArgumentException("Invalid latitude").addLogInfo("latitude", lat);
        }
        if (lat > GeophileSpatial.MAX_LAT) {
            lat = GeophileSpatial.MAX_LAT;
        } else if (lat < GeophileSpatial.MIN_LAT) {
            lat = GeophileSpatial.MIN_LAT;
        }
        return lat;
    }
}
