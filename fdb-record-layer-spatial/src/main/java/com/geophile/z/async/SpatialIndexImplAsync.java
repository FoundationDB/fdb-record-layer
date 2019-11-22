/*
 * SpatialIndexImplAsync.java
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
import com.geophile.z.space.SpaceImpl;

/**
 * Asynchronous version of {@link com.geophile.z.space.SpatialIndexImpl}.
 * @param <RECORD> type for spatial records
 */
@SuppressWarnings({"checkstyle:ClassTypeParameterName", "PMD.GenericsNaming"})
public class SpatialIndexImplAsync<RECORD extends Record> extends SpatialIndexAsync<RECORD>
{
    // Object state

    private final boolean singleCell;

    // Object interface

    @Override
    public String toString()
    {
        return index.toString();
    }

    // SpatialIndexAsync interface

    public boolean singleCell()
    {
        return singleCell;
    }

    public IndexAsync<RECORD> index()
    {
        return index;
    }

    public SpatialIndexImplAsync(SpaceImpl space, IndexAsync<RECORD> index, Options options)
    {
        super(space, index, options);
        singleCell = options == Options.SINGLE_CELL;
    }
}
