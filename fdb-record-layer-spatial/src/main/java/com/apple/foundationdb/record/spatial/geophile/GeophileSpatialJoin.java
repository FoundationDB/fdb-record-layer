/*
 * GeophileSpatialJoin.java
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.tuple.Tuple;
import com.geophile.z.Index;
import com.geophile.z.Space;
import com.geophile.z.SpatialIndex;
import com.geophile.z.SpatialJoin;
import com.geophile.z.SpatialObject;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Iterator;
import java.util.function.BiFunction;

/**
 * Generate {@link RecordCursor} from {@link GeophileIndexMaintainer} using Geophile {@link SpatialJoin}.
 */
class GeophileSpatialJoin {
    @Nonnull
    private final SpatialJoin spatialJoin;
    @Nonnull
    private final FDBRecordStore store;
    @Nonnull
    private final EvaluationContext context;

    GeophileSpatialJoin(@Nonnull SpatialJoin spatialJoin, @Nonnull FDBRecordStore store, @Nonnull EvaluationContext context) {
        this.spatialJoin = spatialJoin;
        this.store = store;
        this.context = context;
    }

    @Nonnull
    public SpatialIndex<GeophileRecordImpl> getSpatialIndex(@Nonnull String indexName) {
        return getSpatialIndex(indexName, ScanComparisons.EMPTY);
    }

    @Nonnull
    public SpatialIndex<GeophileRecordImpl> getSpatialIndex(@Nonnull String indexName,
                                                            @Nonnull ScanComparisons prefixComparisons) {
        return getSpatialIndex(indexName, prefixComparisons, GeophileRecordImpl::new);
    }

    @Nonnull
    public SpatialIndex<GeophileRecordImpl> getSpatialIndex(@Nonnull String indexName,
                                                            @Nonnull ScanComparisons prefixComparisons,
                                                            @Nonnull BiFunction<IndexEntry, Tuple, GeophileRecordImpl> recordFunction) {
        if (!prefixComparisons.isEquality()) {
            throw new RecordCoreArgumentException("prefix comparisons must only have equality");
        }
        // TODO: Add a FDBRecordStoreBase.getIndexMaintainer String overload to do this.
        final IndexMaintainer indexMaintainer = store.getIndexMaintainer(store.getRecordMetaData().getIndex(indexName));
        final TupleRange prefixRange = prefixComparisons.toTupleRange(store, context);
        final Tuple prefix = prefixRange.getLow();  // Since this is an equality, will match getHigh(), too.
        final Index<GeophileRecordImpl> index = new GeophileIndexImpl(indexMaintainer, prefix, recordFunction);
        final Space space = ((GeophileIndexMaintainer)indexMaintainer).getSpace();
        try {
            return SpatialIndex.newSpatialIndex(space, index);
        } catch (IOException ex) {
            throw new RecordCoreException("Unexpected IO exception", ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RecordCoreException(ex);
        }
    }

    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    public RecordCursor<IndexEntry> recordCursor(@Nonnull SpatialObject spatialObject,
                                                 @Nonnull SpatialIndex<GeophileRecordImpl> spatialIndex) {
        // TODO: This is a synchronous implementation using Iterators. A proper RecordCursor implementation needs
        //  Geophile async extensions. Also need to pass down executeProperties.
        final Iterator<GeophileRecordImpl> iterator;
        try {
            iterator = spatialJoin.iterator(spatialObject, spatialIndex);
        } catch (IOException ex) {
            throw new RecordCoreException("Unexpected IO exception", ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RecordCoreException(ex);
        }
        final RecordCursor<GeophileRecordImpl> recordCursor = RecordCursor.fromIterator(store.getExecutor(), iterator);
        return recordCursor.map(GeophileRecordImpl::getIndexEntry);
    }

    @Nonnull
    @SuppressWarnings("PMD.CloseResource")
    public RecordCursor<Pair<IndexEntry, IndexEntry>> recordCursor(@Nonnull SpatialIndex<GeophileRecordImpl> left,
                                                                   @Nonnull SpatialIndex<GeophileRecordImpl> right) {
        // TODO: This is a synchronous implementation using Iterators. A proper RecordCursor implementation needs
        //  Geophile async extensions. Also need to pass down executeProperties.
        final Iterator<com.geophile.z.Pair<GeophileRecordImpl, GeophileRecordImpl>> iterator;
        try {
            iterator = spatialJoin.iterator(left, right);
        } catch (IOException ex) {
            throw new RecordCoreException("Unexpected IO exception", ex);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RecordCoreException(ex);
        }
        final RecordCursor<com.geophile.z.Pair<GeophileRecordImpl, GeophileRecordImpl>> recordCursor = RecordCursor.fromIterator(store.getExecutor(), iterator);
        return recordCursor.map(p -> Pair.of(p.left().getIndexEntry(), p.right().getIndexEntry()));
    }

}
