/*
 * BoxLatLonWithWraparound.java
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

import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.geophile.z.Space;
import com.geophile.z.SpatialObject;
import com.geophile.z.space.Region;
import com.geophile.z.space.RegionComparison;
import com.geophile.z.spatialobject.d2.Box;

import java.nio.ByteBuffer;

class GeophileBoxLatLonWithWraparound implements SpatialObject {
    private final Box left;
    private final Box right;

    public GeophileBoxLatLonWithWraparound(double latLo, double latHi, double lonLo, double lonHi) {
        if (lonLo <= lonHi) {
            throw new RecordCoreArgumentException("box does not wrap around")
                    .addLogInfo("latLo", latLo)
                    .addLogInfo("latHi", latHi)
                    .addLogInfo("lonLo", lonLo)
                    .addLogInfo("lonHi", lonHi);
        }
        left = new Box(latLo, latHi, GeophileSpatial.MIN_LON, lonHi);
        right = new Box(latLo, latHi, lonLo, GeophileSpatial.MAX_LON);
    }

    @Override
    public double[] arbitraryPoint() {
        return left.arbitraryPoint();
    }

    @Override
    public int maxZ() {
        return left.maxZ();
    }

    @Override
    public boolean containedBy(Region region) {
        // Only the topmost region can contain a box with wraparound
        return region.level() == 0;
    }

    @Override
    public boolean containedBy(Space space) {
        return left.containedBy(space) && right.containedBy(space);
    }

    @Override
    public RegionComparison compare(Region region) {
        RegionComparison cL = left.compare(region);
        RegionComparison cR = right.compare(region);
        if (cL == RegionComparison.REGION_INSIDE_OBJECT &&
                cR == RegionComparison.REGION_INSIDE_OBJECT) {
            throw new IllegalStateException("Cannot be inside two disjoint boxes");
        } else if (cL == RegionComparison.REGION_INSIDE_OBJECT ||
                       cR == RegionComparison.REGION_INSIDE_OBJECT) {
            return RegionComparison.REGION_INSIDE_OBJECT;
        } else if (cL == RegionComparison.REGION_OUTSIDE_OBJECT &&
                       cR == RegionComparison.REGION_OUTSIDE_OBJECT) {
            return RegionComparison.REGION_OUTSIDE_OBJECT;
        } else {
            return RegionComparison.REGION_OVERLAPS_OBJECT;
        }
    }

    @Override
    public void readFrom(ByteBuffer buffer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeTo(ByteBuffer buffer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "(" + left.yLo() + ":" + left.yHi() + ", " + right.xLo() + ":" + left.xHi() + ")";
    }

}
