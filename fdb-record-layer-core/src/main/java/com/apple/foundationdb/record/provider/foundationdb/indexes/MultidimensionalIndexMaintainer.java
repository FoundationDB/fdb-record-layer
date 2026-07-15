/*
 * MultidimensionalIndexMaintainer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2023 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.metadata.expressions.DimensionsKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanBounds;
import com.apple.foundationdb.record.provider.foundationdb.MultidimensionalIndexScanBounds;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * An index maintainer for keeping a multidimensional {@link RTree}. The dimension columns are {@code long}s that are
 * stored directly as R-tree point coordinates.
 *
 * <p>
 * The scan and maintenance mechanics are shared with {@link GeospatialRTreeIndexMaintainer} through
 * {@link RTreeIndexHelper}; this class only supplies the {@link DimensionsKeyExpression}-driven prefix/dimensions
 * split and validation of the coordinate columns.
 * </p>
 */
@API(API.Status.EXPERIMENTAL)
public class MultidimensionalIndexMaintainer extends StandardIndexMaintainer {
    @Nonnull
    private final RTree.Config config;

    public MultidimensionalIndexMaintainer(IndexMaintainerState state) {
        super(state);
        this.config = MultiDimensionalIndexHelper.getConfig(state.index);
    }

    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scan(@Nonnull final IndexScanBounds scanBounds, @Nullable final byte[] continuation,
                                         @Nonnull final ScanProperties scanProperties) {
        if (!scanBounds.getScanType().equals(IndexScanType.BY_VALUE)) {
            throw new RecordCoreException("Can only scan multidimensional index by value.");
        }
        if (!(scanBounds instanceof MultidimensionalIndexScanBounds)) {
            throw new RecordCoreException("Need proper multidimensional index scan bounds.");
        }
        final MultidimensionalIndexScanBounds mDScanBounds = (MultidimensionalIndexScanBounds)scanBounds;
        final int prefixSize = getDimensionsKeyExpression(state.index.getRootExpression()).getPrefixSize();

        return RTreeIndexHelper.scan(state, config, prefixSize, mDScanBounds.getPrefixRange(),
                mDScanBounds.getSuffixRange(), mDScanBounds::overlapsMbrApproximately, mDScanBounds::containsPosition,
                Tuple::getItems, continuation, scanProperties);
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
        final DimensionsKeyExpression dimensionsKeyExpression = getDimensionsKeyExpression(state.index.getRootExpression());
        return RTreeIndexHelper.update(state, config, dimensionsKeyExpression.getPrefixSize(),
                dimensionsKeyExpression.getDimensionsSize(), savedRecord, remove, indexEntries,
                coordinateItems -> validatePoint(new RTree.Point(Tuple.fromList(coordinateItems))));
    }

    @Override
    public boolean canDeleteWhere(@Nonnull final QueryToKeyMatcher matcher, @Nonnull final Key.Evaluated evaluated) {
        if (!super.canDeleteWhere(matcher, evaluated)) {
            return false;
        }
        return evaluated.size() <= getDimensionsKeyExpression(state.index.getRootExpression()).getPrefixSize();
    }

    @Override
    public CompletableFuture<Void> deleteWhere(@Nonnull final Transaction tr, @Nonnull final Tuple prefix) {
        Verify.verify(getDimensionsKeyExpression(state.index.getRootExpression()).getPrefixSize() >= prefix.size());
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
     * Traverse from the root of a key expression of a multidimensional index to the {@link DimensionsKeyExpression}.
     * @param root the root {@link KeyExpression} of the index definition
     * @return a {@link DimensionsKeyExpression}
     */
    @Nonnull
    public static DimensionsKeyExpression getDimensionsKeyExpression(@Nonnull final KeyExpression root) {
        if (root instanceof KeyWithValueExpression) {
            KeyExpression innerKey = ((KeyWithValueExpression)root).getInnerKey();
            while (innerKey instanceof ThenKeyExpression) {
                innerKey = ((ThenKeyExpression)innerKey).getChildren().get(0);
            }
            if (innerKey instanceof DimensionsKeyExpression) {
                return (DimensionsKeyExpression)innerKey;
            }
            throw new RecordCoreException("structure of multidimensional index is not supported");
        }
        return (DimensionsKeyExpression)root;
    }

    @Nonnull
    private static RTree.Point validatePoint(@Nonnull RTree.Point point) {
        for (int d = 0; d < point.getNumDimensions(); d ++) {
            Object coordinate = point.getCoordinate(d);
            Preconditions.checkArgument(coordinate == null || coordinate instanceof Long,
                    "dimension coordinates must be of type long");
        }
        return point;
    }
}
