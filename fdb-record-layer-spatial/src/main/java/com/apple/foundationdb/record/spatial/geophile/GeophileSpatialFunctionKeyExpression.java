/*
 * SpatialFunctionKeyExpression.java
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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.FunctionKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.geophile.z.Space;
import com.geophile.z.SpatialObject;
import com.geophile.z.spatialobject.d2.Point;
import com.google.protobuf.Message;
import org.locationtech.jts.io.ParseException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Spatial function key expressions.
 */
@API(API.Status.EXPERIMENTAL)
public abstract class GeophileSpatialFunctionKeyExpression extends FunctionKeyExpression {

    protected GeophileSpatialFunctionKeyExpression(@Nonnull String name, @Nonnull KeyExpression arguments) {
        super(name, arguments);
    }

    @Nullable
    protected abstract SpatialObject parseSpatialObject(@Nonnull Space space, @Nonnull Key.Evaluated arguments) throws ParseException;

    @Nonnull
    @Override
    public <M extends Message> List<Key.Evaluated> evaluateFunction(@Nullable FDBRecord<M> record, @Nullable Message message, @Nonnull Key.Evaluated arguments) {
        // TODO: How might we really get this out of the index maintainer? From the evaluation context?
        //  Or should it be part of the key expression itself?
        Space space = GeophileIndexMaintainer.SPACE_LAT_LON;
        SpatialObject spatialObject;
        try {
            spatialObject = parseSpatialObject(space, arguments);
        } catch (ParseException ex) {
            throw new RecordCoreException(ex);
        }
        if (spatialObject == null) {
            return Collections.singletonList(Key.Evaluated.NULL);
        }
        long[] zs = new long[spatialObject.maxZ()];
        GeophileSpatial.shuffle(space, spatialObject, zs);
        List<Key.Evaluated> result = new ArrayList<>(zs.length);
        for (long z : zs) {
            if (z == Space.Z_NULL) {
                break;
            }
            result.add(Key.Evaluated.scalar(z));
        }
        return result;
    }

    @Override
    public int getMinArguments() {
        return 1;
    }

    @Override
    public int getMaxArguments() {
        return 2;
    }

    @Override
    public boolean createsDuplicates() {
        return true;
    }

    @Override
    public int getColumnSize() {
        return 1;
    }

    @Nonnull
    protected boolean shouldSwapLatLong(@Nonnull Key.Evaluated arguments) {
        return arguments.size() > 1 && arguments.getObject(1, Boolean.class);
    }

    /**
     * A geospatial point.
     *
     * <code>GEO_POINT_Z(latitude, longitude)</code>
     */
    public static class GeoPointZ extends GeophileSpatialFunctionKeyExpression {
        public GeoPointZ(@Nonnull String name, @Nonnull KeyExpression arguments) {
            super(name, arguments);
        }

        @Nullable
        @Override
        protected SpatialObject parseSpatialObject(@Nonnull Space space, @Nonnull Key.Evaluated arguments) {
            Double latitude = arguments.getNullableDouble(0);
            Double longitude = arguments.getNullableDouble(1);
            if (latitude == null || longitude == null) {
                return null;
            } else {
                return new Point(latitude, longitude);
            }
        }

        @Override
        public int getMinArguments() {
            return 2;
        }

        @Override
        public int getMaxArguments() {
            return 2;
        }
    }

    /**
     * A serialized JTS geometry in GeoJson text format.
     *
     * <code>GEO_JSON_Z(json_string)</code>
     */
    public static class GeoJsonZ extends GeophileSpatialFunctionKeyExpression {
        public GeoJsonZ(@Nonnull String name, @Nonnull KeyExpression arguments) {
            super(name, arguments);
        }

        @Nullable
        @Override
        protected SpatialObject parseSpatialObject(@Nonnull Space space, @Nonnull Key.Evaluated arguments) throws ParseException {
            String json = arguments.getString(0);
            if (json == null) {
                return null;
            } else {
                return GeophileSpatial.deserializeGeoJson(space, json, shouldSwapLatLong(arguments));
            }
        }
    }

    /**
     * A serialized JTS geometry in WKB binary format.
     *
     * <code>GEO_WKB_Z(wkb_bytes)</code>
     */
    @SuppressWarnings("checkstyle:abbreviationaswordinname")    // Allow WKB
    public static class GeoWKBZ extends GeophileSpatialFunctionKeyExpression {
        public GeoWKBZ(@Nonnull String name, @Nonnull KeyExpression arguments) {
            super(name, arguments);
        }

        @Nullable
        @Override
        protected SpatialObject parseSpatialObject(@Nonnull Space space, @Nonnull Key.Evaluated arguments) throws ParseException {
            byte[] wkb = arguments.getObject(0, byte[].class);
            if (wkb == null) {
                return null;
            } else {
                return GeophileSpatial.deserializeWKB(space, wkb, shouldSwapLatLong(arguments));
            }
        }
    }

    /**
     * A serialized JTS geometry in WKT text format.
     *
     * <code>GEO_WKT_Z(wkt_string)</code>
     */
    @SuppressWarnings("checkstyle:abbreviationaswordinname")    // Allow WKT
    public static class GeoWKTZ extends GeophileSpatialFunctionKeyExpression {
        public GeoWKTZ(@Nonnull String name, @Nonnull KeyExpression arguments) {
            super(name, arguments);
        }

        @Nullable
        @Override
        protected SpatialObject parseSpatialObject(@Nonnull Space space, @Nonnull Key.Evaluated arguments) throws ParseException {
            String wkt = arguments.getString(0);
            if (wkt == null) {
                return null;
            } else {
                return GeophileSpatial.deserializeWKT(space, wkt, shouldSwapLatLong(arguments));
            }
        }
    }

}
