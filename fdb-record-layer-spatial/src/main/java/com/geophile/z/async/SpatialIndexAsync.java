/*
 * SpatialIndexAsync.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2019 Apple Inc. and the FoundationDB project authors
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

package com.geophile.z.async;

import com.geophile.z.Record;
import com.geophile.z.Space;
import com.geophile.z.SpatialObject;
import com.geophile.z.space.SpaceImpl;

/**
 * Asynchronous version of {@link com.geophile.z.SpatialIndex}.
 * @param <RECORD> type for spatial records
 */
@SuppressWarnings({"checkstyle:ClassTypeParameterName", "PMD.GenericsNaming", "checkstyle:MethodTypeParameterName"})
public class SpatialIndexAsync<RECORD extends Record>
{
    // Class state

    protected static final int USE_SPATIAL_OBJECT_MAX_Z = -1;

    // Object state

    protected final SpaceImpl space;
    protected final IndexAsync<RECORD> index;
    protected final Options options;

    /**
     * Returns the {@link com.geophile.z.Space} associated with this SpatialIndex.
     * @return The {@link com.geophile.z.Space} associated with this SpatialIndex.
     */
    public final Space space()
    {
        return space;
    }

    /**
     * Creates a SpatialIndexAsync. The index
     * should never be manipulated directly at any time. It is intended to be maintained and searched only
     * through the interface of this class.
     * @param space The {@link Space} containing the {@link SpatialObject}s to be indexed.
     * @param index The {@link IndexAsync} that will store the indexed {@link SpatialObject}s.
     * @param <RECORD> type for spatial records
     * @return A new SpatialIndexAsync
     */
    public static <RECORD extends Record> SpatialIndexAsync<RECORD> newSpatialIndexAsync(Space space,
                                                                                         IndexAsync<RECORD> index) {
        return newSpatialIndexAsync(space, index, Options.DEFAULT);
    }

    /**
     * Creates a SpatialIndexAsync. The index
     * should never be manipulated directly at any time. It is intended to be maintained and searched only
     * through the interface of this class.
     * @param space The {@link Space} containing the {@link SpatialObject}s to be indexed.
     * @param index The {@link IndexAsync} that will store the indexed {@link SpatialObject}s.
     * @param options index {@link Options} for new index.
     * @param <RECORD> type for spatial records
     * @return A new SpatialIndexAsync.
     */
    public static <RECORD extends Record> SpatialIndexAsync<RECORD> newSpatialIndexAsync(Space space,
                                                                                         IndexAsync<RECORD> index,
                                                                                         Options options) {
        return new SpatialIndexImplAsync<>((SpaceImpl) space, index, options);
    }

    // For use by subclasses

    protected SpatialIndexAsync(SpaceImpl space, IndexAsync<RECORD> index, Options options)
    {
        this.space = space;
        this.index = index;
        this.options = options;
    }

    // Inner classes

    /**
     * Options for {@link #newSpatialIndexAsync(Space, IndexAsync, Options)}.
     */
    public enum Options {
        DEFAULT, SINGLE_CELL
    }
}
