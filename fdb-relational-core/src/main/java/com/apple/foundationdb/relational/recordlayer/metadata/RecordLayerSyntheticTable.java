/*
 * RecordLayerSyntheticTable.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.metadata;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.metadata.SyntheticTable;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Set;

/**
 * Base class for synthetic tables in the relational layer: a virtual table backed by a record-layer synthetic record
 * type ({@code UnnestedRecordType} and, eventually, joined types) with one or more indexes maintained on it.
 *
 * @see RecordLayerUnnestedSyntheticTable
 */
@API(API.Status.EXPERIMENTAL)
public abstract sealed class RecordLayerSyntheticTable implements SyntheticTable
        permits RecordLayerUnnestedSyntheticTable, RecordLayerJoinedSyntheticTable {

    @Nonnull
    final Type.Record type;

    @Nonnull
    private final Set<RecordLayerIndex> indexes;

    protected RecordLayerSyntheticTable(@Nonnull final Set<RecordLayerIndex> indexes,
                                        @Nonnull final Type.Record type) {
        this.indexes = Set.copyOf(indexes);
        this.type = type;
    }

    @Nonnull
    @Override
    public String getName() {
        return Objects.requireNonNull(type.getName());
    }

    @Nonnull
    public Type.Record getType() {
        return type;
    }

    @Nonnull
    @Override
    public Set<RecordLayerIndex> getIndexes() {
        return indexes;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }
        final RecordLayerSyntheticTable that = (RecordLayerSyntheticTable) o;
        return Objects.equals(type, that.type) && Objects.equals(indexes, that.indexes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, indexes);
    }

    public interface Builder {
        @Nonnull
        Builder addIndex(@Nonnull RecordLayerIndex index);

        @Nonnull
        RecordLayerSyntheticTable build();
    }
}
