/*
 * GeophileIndexImpl.java
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

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.tuple.Tuple;
import com.geophile.z.Cursor;
import com.geophile.z.Index;
import com.geophile.z.Record;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.BiFunction;

/**
 * Adapt {@link GeophileIndexMaintainer} to Geophile {@link Index}.
 */
class GeophileIndexImpl extends Index<GeophileRecordImpl> {
    @Nonnull
    private final IndexMaintainer indexMaintainer;
    @Nullable
    private final Tuple prefix;
    @Nonnull
    private final BiFunction<IndexEntry, Tuple, GeophileRecordImpl> recordFunction;

    GeophileIndexImpl(@Nonnull IndexMaintainer indexMaintainer, @Nullable Tuple prefix,
                      @Nonnull BiFunction<IndexEntry, Tuple, GeophileRecordImpl> recordFunction) {
        this.indexMaintainer = indexMaintainer;
        this.prefix = prefix;
        this.recordFunction = recordFunction;
    }

    @Override
    public void add(GeophileRecordImpl record) {
        throw new UnsupportedOperationException("add not supported");
    }

    @Override
    public boolean remove(long z, Record.Filter<GeophileRecordImpl> filter) {
        throw new UnsupportedOperationException("remove not supported");
    }

    @Override
    public Cursor<GeophileRecordImpl> cursor() {
        return new GeophileCursorImpl(this, indexMaintainer, prefix, recordFunction);
    }

    @Override
    public GeophileRecordImpl newRecord() {
        return new GeophileRecordImpl(null, null);
    }

    @Override
    public boolean blindUpdates() {
        return false;
    }

    @Override
    public boolean stableRecords() {
        return true;
    }
}
