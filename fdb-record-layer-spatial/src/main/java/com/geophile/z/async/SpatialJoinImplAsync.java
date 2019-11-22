/*
 * SpatialJoinInputAsync.java
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

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.geophile.z.Pair;
import com.geophile.z.Record;
import com.geophile.z.SpatialJoinException;
import com.geophile.z.SpatialObject;

/**
 * Asynchronous version of {@code com.geophile.z.spatialjoin.SpatialJoinImpl}.
 * @param <LEFT> the type on the left
 * @param <RIGHT> the type on the right
 */
@SuppressWarnings({"checkstyle:MethodTypeParameterName", "PMD.GenericsNaming"})
public class SpatialJoinImplAsync<LEFT, RIGHT> extends SpatialJoinAsync<LEFT, RIGHT>
{
    private static final String SINGLE_CELL_OPTIMIZATION_PROPERTY = "singlecellopt";

    private final Duplicates duplicates;
    private final Filter<LEFT, RIGHT> filter;
    private final InputObserver leftObserver;
    private final InputObserver rightObserver;

    public SpatialJoinImplAsync(Duplicates duplicates,
                                Filter<LEFT, RIGHT> filter,
                                InputObserver leftObserver,
                                InputObserver rightObserver)
    {
        if (duplicates == null) {
            throw new IllegalArgumentException();
        }
        this.duplicates = duplicates;
        this.filter = filter == null ? (r, s) -> true : filter;
        this.leftObserver = leftObserver;
        this.rightObserver = rightObserver;
    }

    @Override
    @SuppressWarnings("unchecked")
    @SpotBugsSuppressWarnings("BC_UNCONFIRMED_CAST")
    public <LEFT_RECORD extends Record, RIGHT_RECORD extends Record> IteratorAsync<Pair<LEFT_RECORD, RIGHT_RECORD>> iterator(SpatialIndexAsync<LEFT_RECORD> leftSpatialIndex,
                                                                                                                             SpatialIndexAsync<RIGHT_RECORD> rightSpatialIndex)
    {
        if (!leftSpatialIndex.space().equals(rightSpatialIndex.space())) {
            throw new SpatialJoinException("Attempt to join spatial indexes with incompatible spaces");
        }
        IteratorAsync<Pair<LEFT_RECORD, RIGHT_RECORD>> iterator =
                SpatialJoinIteratorAsync.pairIteratorAsync((SpatialIndexImplAsync<LEFT_RECORD>) leftSpatialIndex,
                                                           (SpatialIndexImplAsync<RIGHT_RECORD>) rightSpatialIndex,
                                                           (Filter<LEFT_RECORD, RIGHT_RECORD>)filter,
                                                           leftObserver,
                                                           rightObserver);
        if (duplicates == SpatialJoinAsync.Duplicates.EXCLUDE) {
            iterator = new DuplicateEliminatingIteratorAsync<Pair<LEFT_RECORD, RIGHT_RECORD>>(iterator);
        }
        return iterator;
    }

    @Override
    @SuppressWarnings("unchecked")
    @SpotBugsSuppressWarnings("BC_UNCONFIRMED_CAST")
    public <RECORD extends Record> IteratorAsync<RECORD> iterator(final SpatialObject query,
                                                                  SpatialIndexAsync<RECORD> data)
    {
        IteratorAsync<RECORD> iterator = SpatialJoinIteratorAsync.spatialObjectIteratorAsync(query,
                                                                                             (SpatialIndexImplAsync) data,
                                                                                             (Filter<SpatialObject, RECORD>)filter,
                                                                                             leftObserver,
                                                                                             rightObserver);
        if (duplicates == SpatialJoinAsync.Duplicates.EXCLUDE) {
            iterator = new DuplicateEliminatingIteratorAsync<>(iterator);
        }
        return iterator;
    }

    public static boolean singleCellOptimization()
    {
        return Boolean.valueOf(System.getProperty(SINGLE_CELL_OPTIMIZATION_PROPERTY, "true"));
    }
}
