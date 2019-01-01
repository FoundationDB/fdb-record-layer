/*
 * GeophileCursorImpl.java
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

import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.tuple.Tuple;
import com.geophile.z.Cursor;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.BiFunction;

/**
 * Adapt {@link SpatialIndexMaintainer} to Geophile {@link Cursor}.
 *
 * <p>This is a synchronous implementation, with a blocking {@code next} method.
 * Proper integration requires something like the following.<ul>
 * <li>An asynchronous version of {@code Cursor}, based on {@code RecordCursor},
 * with an {@code onNext} returning a future result.</li>
 * <li>An asynchronous version of {@code SpatialJoinInput} using that whose {@code cursorNext}
 * does not take effect right away.</li>
 * <li>An asynchronous version of {@code SpatialJoinIterator}, implementing {@code RecordCursor},
 * that waits for both sides to be ready from each input before {@code findPairs} can decide what to do.</li>
 * </ul>
 *
 * <p>Likewise, this does not have proper continuation support.
 * A spatial join continuation needs to remember the following.<ul>
 * <li>any pending items to be output immediately on resume</li>
 * <li>open nested items on each side</li>
 * <li>restart continuation / Z value for each side</li>
 * </ul>
 */
class GeophileCursorImpl extends Cursor<GeophileRecordImpl> {
    @Nonnull
    private final IndexMaintainer indexMaintainer;
    @Nullable
    private final Tuple prefix;
    @Nonnull
    private final BiFunction<IndexEntry, Tuple, GeophileRecordImpl> recordFunction;
    private RecordCursor<IndexEntry> recordCursor;

    GeophileCursorImpl(@Nonnull GeophileIndexImpl index, @Nonnull IndexMaintainer indexMaintainer, @Nullable Tuple prefix,
                       @Nonnull BiFunction<IndexEntry, Tuple, GeophileRecordImpl> recordFunction) {
        super(index);
        this.indexMaintainer = indexMaintainer;
        this.prefix = prefix;
        this.recordFunction = recordFunction;
    }

    @Nullable
    @Override
    public GeophileRecordImpl next() throws InterruptedException {
        if (recordCursor == null) {
            throw new IllegalStateException("cannot call next before goTo");
        }
        // For now, using synchronous API.
        RecordCursorResult<IndexEntry> next = recordCursor.getNext();
        if (next.hasNext()) {
            return recordFunction.apply(next.get(), prefix);
        } else {
            return null;
        }
    }

    @Override
    public void goTo(@Nonnull GeophileRecordImpl key) {
        // TODO: For many kinds of spatial joins, it should be possible to pick an max Z value as well.
        //  This does not affect correctness, but without it the underlying key-value store does extra work.
        TupleRange range = new TupleRange(Tuple.from(key.z()), null, EndpointType.RANGE_INCLUSIVE, EndpointType.TREE_END);
        if (prefix != null) {
            range = range.prepend(prefix);
        }
        recordCursor = indexMaintainer.scan(SpatialScanTypes.GO_TO_Z, range, null, ScanProperties.FORWARD_SCAN);
    }

    @Override
    public boolean deleteCurrent() {
        throw new UnsupportedOperationException("delete not supported");
    }
}
