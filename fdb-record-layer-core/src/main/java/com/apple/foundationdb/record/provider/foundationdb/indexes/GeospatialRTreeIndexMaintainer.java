/*
 * GeospatialRTreeIndexMaintainer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.rtree.RTree;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.GeospatialRTreeScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanBounds;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.Lists;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.apple.foundationdb.record.provider.foundationdb.GeospatialRTreeScanBounds.decodeCoordinate;
import static com.apple.foundationdb.record.provider.foundationdb.GeospatialRTreeScanBounds.encodeCoordinate;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.GeospatialRTreeIndexHelper.COORDINATE_DIMENSIONS;

/**
 * An index maintainer for a geospatial index that stores {@code double} latitude/longitude coordinates in a Hilbert
 * {@link RTree}. Coordinates are converted to fixed-point {@code long}s at the index precision scale so that the
 * R-tree's Hilbert value calculation, which operates on {@code long}s, applies unchanged.
 *
 * <p>
 * The index root key expression yields, in order, zero or more grouping columns followed by exactly two coordinate
 * columns: latitude then longitude. Each distinct grouping tuple is backed by its own R-tree, stored under a subspace
 * keyed by that tuple. The R-tree item's key suffix carries the (trimmed) primary key so that entries map back to
 * records.
 * </p>
 *
 * <p>
 * The scan and maintenance mechanics are shared with {@link MultidimensionalIndexMaintainer} through
 * {@link RTreeIndexHelper}; this class only supplies the fixed-point encoding/decoding of coordinates.
 * </p>
 *
 * @see GeospatialRTreeScanBounds for the within-distance scan
 */
@API(API.Status.EXPERIMENTAL)
public class GeospatialRTreeIndexMaintainer extends StandardIndexMaintainer {
    @Nonnull
    private final RTree.Config config;
    private final long scale;

    public GeospatialRTreeIndexMaintainer(@Nonnull final IndexMaintainerState state) {
        super(state);
        this.config = GeospatialRTreeIndexHelper.getConfig(state.index);
        this.scale = GeospatialRTreeIndexHelper.getScale(state.index);
    }

    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scan(@Nonnull final IndexScanBounds scanBounds, @Nullable final byte[] continuation,
                                         @Nonnull final ScanProperties scanProperties) {
        if (!scanBounds.getScanType().equals(IndexScanType.BY_DISTANCE)) {
            throw new RecordCoreException("Can only scan geospatial R-tree index by distance.");
        }
        if (!(scanBounds instanceof GeospatialRTreeScanBounds)) {
            throw new RecordCoreException("Need proper geospatial R-tree index scan bounds.");
        }
        final GeospatialRTreeScanBounds geoScanBounds = (GeospatialRTreeScanBounds)scanBounds;
        final int prefixSize = GeospatialRTreeIndexHelper.getGroupingCount(state.index.getRootExpression());

        return RTreeIndexHelper.scan(state, config, prefixSize, geoScanBounds.getPrefixRange(),
                geoScanBounds.getSuffixRange(), geoScanBounds::overlapsMbrApproximately,
                geoScanBounds::containsPosition, this::decodeCoordinates, continuation, scanProperties);
    }

    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scan(@Nonnull final IndexScanType scanType, @Nonnull final TupleRange range,
                                         @Nullable final byte[] continuation, @Nonnull final ScanProperties scanProperties) {
        throw new RecordCoreException("index maintainer does not support this scan api");
    }

    @Override
    protected <M extends Message> CompletableFuture<Void> updateIndexKeys(@Nonnull final FDBIndexableRecord<M> savedRecord,
                                                                          final boolean remove,
                                                                          @Nonnull final List<IndexEntry> indexEntries) {
        final int prefixSize = GeospatialRTreeIndexHelper.getGroupingCount(state.index.getRootExpression());
        return RTreeIndexHelper.update(state, config, prefixSize, COORDINATE_DIMENSIONS, savedRecord, remove,
                indexEntries, this::encodePoint);
    }

    @Override
    public boolean canDeleteWhere(@Nonnull final QueryToKeyMatcher matcher, @Nonnull final Key.Evaluated evaluated) {
        if (!super.canDeleteWhere(matcher, evaluated)) {
            return false;
        }
        return evaluated.size() <= GeospatialRTreeIndexHelper.getGroupingCount(state.index.getRootExpression());
    }

    @Override
    public CompletableFuture<Void> deleteWhere(@Nonnull final Transaction tr, @Nonnull final Tuple prefix) {
        Verify.verify(GeospatialRTreeIndexHelper.getGroupingCount(state.index.getRootExpression()) >= prefix.size());
        return super.deleteWhere(tr, prefix).thenApply(v -> {
            // NOTE: Range.startsWith(), Subspace.range() and so on cover keys *strictly* within the range, but we sometimes
            // store data at the prefix key itself.
            final Subspace nodeSlotIndexSubspace = RTreeIndexHelper.nodeSlotIndexSubspace(state);
            final byte[] key = nodeSlotIndexSubspace.pack(prefix);
            state.context.clear(new Range(key, ByteArrayUtil.strinc(key)));
            return v;
        });
    }

    /**
     * Build an R-tree point from the latitude/longitude columns, converting each {@code double} to fixed-point. A null
     * coordinate is preserved as {@code null} (the R-tree substitutes a sentinel when computing the Hilbert value).
     */
    @Nonnull
    private RTree.Point encodePoint(@Nonnull final List<Object> coordinateItems) {
        final List<Object> encoded = Lists.newArrayListWithCapacity(coordinateItems.size());
        for (final Object coordinate : coordinateItems) {
            encoded.add(coordinate == null ? null : encodeCoordinate(((Number)coordinate).doubleValue(), scale));
        }
        return new RTree.Point(Tuple.fromList(encoded));
    }

    @Nonnull
    private List<Object> decodeCoordinates(@Nonnull final Tuple coordinates) {
        final List<Object> decoded = Lists.newArrayListWithCapacity(coordinates.size());
        for (int i = 0; i < coordinates.size(); i++) {
            final Object coordinate = coordinates.get(i);
            decoded.add(coordinate == null ? null : decodeCoordinate(((Number)coordinate).longValue(), scale));
        }
        return decoded;
    }
}
