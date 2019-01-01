/*
 * GeophileRecordImpl.java
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

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.tuple.Tuple;
import com.geophile.z.Record;
import com.geophile.z.Space;
import com.geophile.z.SpatialObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Adapt {@link IndexEntry} to Geophile {@link Record}.
 */
@SuppressWarnings("checkstyle:membername")  // z is a fundamental concept
class GeophileRecordImpl implements Record {
    @Nullable
    private final IndexEntry indexEntry;
    private long z;

    GeophileRecordImpl(@Nullable IndexEntry indexEntry, @Nullable Tuple prefix) {
        this.indexEntry = indexEntry;
        this.z = indexEntry == null ? Space.Z_NULL : indexEntry.getKey().getLong(prefix == null ? 0 : prefix.size());
    }

    @Nullable
    public IndexEntry getIndexEntry() {
        return indexEntry;
    }

    @Nonnull
    public SpatialObject spatialObject() {
        throw new UnsupportedOperationException("this index does not have covering spatial objects");
    }

    @Override
    public long z() {
        return z;
    }

    @Override
    public void z(long z) {
        this.z = z;
    }

    @Override
    public void copyTo(Record record) {
        throw new UnsupportedOperationException("records are stable; no need for copying");
    }

    @Override
    public String toString() {
        return indexEntry + "@" + z;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GeophileRecordImpl that = (GeophileRecordImpl)o;
        return Objects.equals(indexEntry, that.indexEntry);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexEntry);
    }
}
