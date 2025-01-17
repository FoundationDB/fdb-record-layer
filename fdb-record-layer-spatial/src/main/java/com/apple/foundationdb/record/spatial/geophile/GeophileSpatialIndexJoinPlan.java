/*
 * GeophileSpatialIndexJoinPlan.java
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
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.IndexOrphanBehavior;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.util.pair.Pair;
import com.geophile.z.SpatialIndex;
import com.geophile.z.SpatialJoin;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;

/**
 * Something like a query plan for joining spatial indexes.
 *
 * This is not a real {@code RecordQueryPlan} because the signature of the cursor can't match
 * until queries support actual joins.
 */
@API(API.Status.EXPERIMENTAL)
public class GeophileSpatialIndexJoinPlan {
    @Nonnull
    private final String leftIndexName;
    @Nonnull
    private final ScanComparisons leftPrefixComparisons;
    @Nonnull
    private final String rightIndexName;
    @Nonnull
    private final ScanComparisons rightPrefixComparisons;

    public GeophileSpatialIndexJoinPlan(@Nonnull String leftIndexName, @Nonnull ScanComparisons leftPrefixComparisons,
                                        @Nonnull String rightIndexName, @Nonnull ScanComparisons rightPrefixComparisons) {
        this.leftIndexName = leftIndexName;
        this.leftPrefixComparisons = leftPrefixComparisons;
        this.rightIndexName = rightIndexName;
        this.rightPrefixComparisons = rightPrefixComparisons;
    }

    @Nonnull
    public <M extends Message> RecordCursor<Pair<FDBIndexedRecord<M>, FDBIndexedRecord<M>>> execute(@Nonnull FDBRecordStoreBase<M> store, @Nonnull EvaluationContext context) {
        final SpatialJoin spatialJoin = SpatialJoin.newSpatialJoin(SpatialJoin.Duplicates.INCLUDE);
        final GeophileSpatialJoin geophileSpatialJoin = new GeophileSpatialJoin(spatialJoin, store.getUntypedRecordStore(), context);
        final SpatialIndex<GeophileRecordImpl> leftSpatialIndex = geophileSpatialJoin.getSpatialIndex(leftIndexName, leftPrefixComparisons);
        final SpatialIndex<GeophileRecordImpl> rightSpatialIndex = geophileSpatialJoin.getSpatialIndex(rightIndexName, rightPrefixComparisons);
        return fetchIndexRecords(store, geophileSpatialJoin.recordCursor(leftSpatialIndex, rightSpatialIndex));
    }

    // TODO: Probably once there is a real join cursor signature, something like this is a method on the store and loadIndexEntryRecord doesn't need to be public.
    @Nonnull
    public <M extends Message> RecordCursor<Pair<FDBIndexedRecord<M>, FDBIndexedRecord<M>>> fetchIndexRecords(@Nonnull FDBRecordStoreBase<M> store,
                                                                                                              @Nonnull RecordCursor<Pair<IndexEntry, IndexEntry>> indexCursor) {
        return indexCursor.mapPipelined(pair ->
                        store.loadIndexEntryRecord(pair.getLeft(), IndexOrphanBehavior.ERROR)
                                .thenCombine(store.loadIndexEntryRecord(pair.getRight(), IndexOrphanBehavior.ERROR), Pair::of),
                store.getPipelineSize(PipelineOperation.INDEX_TO_RECORD));
    }

}
