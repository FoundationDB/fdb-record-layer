/*
 * SpatialIndexMaintainer.java
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

package com.apple.foundationdb.record.geophile;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.indexes.StandardIndexMaintainer;
import com.geophile.z.Space;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The index maintainer class for (geo-)spatial indexes.
 *
 */
@API(API.Status.EXPERIMENTAL)
public class SpatialIndexMaintainer extends StandardIndexMaintainer {
    // TODO: In order to make this parametric on index options, there needs to be a way to pass it down
    //  to the evaluation function.
    public static final Space SPACE_LAT_LON = GeophileSpatial.createLatLonSpace();
    @Nonnull
    private final Space space;

    public SpatialIndexMaintainer(IndexMaintainerState state) {
        super(state);
        this.space = SPACE_LAT_LON;
    }

    @Nonnull
    public Space getSpace() {
        return space;
    }

    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scan(@Nonnull IndexScanType scanType, @Nonnull TupleRange range, @Nullable byte[] continuation, @Nonnull ScanProperties scanProperties) {
        if (scanType == SpatialScanTypes.GO_TO_Z) {
            return scan(range, continuation, scanProperties);
        } else {
            throw new RecordCoreException("This index can only be scanned by a spatial cursor");
        }
    }

    // NOTE: does not use Index / SpatialIndex abstraction to get entries to store for evaluateIndex. That might be one way
    //  to pass down the Space, but would make additional prefix keys more difficult.

}
