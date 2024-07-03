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

package com.apple.foundationdb.record.spatial.geophile;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpressionWithChild;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.indexes.StandardIndexMaintainer;
import com.geophile.z.Space;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * The index maintainer class for (geo-)spatial indexes.
 *
 */
@API(API.Status.EXPERIMENTAL)
public class GeophileIndexMaintainer extends StandardIndexMaintainer {
    @Nonnull
    private final Space space;

    public GeophileIndexMaintainer(IndexMaintainerState state) {
        super(state);
        this.space = getSpatialFunction(state.index).getSpace();
    }

    // If the bottom-right child is GeophileSpatialFunctionKeyExpression, return it. Else error.
    @Nonnull
    static GeophileSpatialFunctionKeyExpression getSpatialFunction(@Nonnull Index index) {
        KeyExpression rootKey = index.getRootExpression();
        if (rootKey instanceof KeyWithValueExpression) {
            rootKey = ((KeyWithValueExpression)rootKey).getKeyExpression();
        }
        final List<KeyExpression> components = rootKey.normalizeKeyForPositions();
        final KeyExpression rightComponent = components.get(components.size() - 1);
        KeyExpression bottomComponent = rightComponent;
        while (true) {
            if (bottomComponent instanceof GeophileSpatialFunctionKeyExpression) {
                return (GeophileSpatialFunctionKeyExpression)bottomComponent;
            }
            if (bottomComponent instanceof KeyExpressionWithChild) {
                bottomComponent = ((KeyExpressionWithChild)bottomComponent).getChild();
                continue;
            }
            throw new KeyExpression.InvalidExpressionException(
                    "need spatial key expression for index type",
                    LogMessageKeys.INDEX_TYPE, index.getType(),
                    LogMessageKeys.INDEX_NAME, index.getName(),
                    LogMessageKeys.INDEX_KEY, index.getRootExpression());
        }
    }

    @Nonnull
    public Space getSpace() {
        return space;
    }

    @Nonnull
    @Override
    public RecordCursor<IndexEntry> scan(@Nonnull IndexScanType scanType, @Nonnull TupleRange range, @Nullable byte[] continuation, @Nonnull ScanProperties scanProperties) {
        if (scanType.equals(GeophileScanTypes.GO_TO_Z)) {
            return scan(range, continuation, scanProperties);
        } else {
            throw new RecordCoreException("This index can only be scanned by a spatial cursor");
        }
    }

    // NOTE: does not use Geophile's own Index / SpatialIndex abstraction to get entries to store for evaluateIndex.
}
