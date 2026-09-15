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

import com.apple.foundationdb.record.util.ProtoUtils;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.relational.api.metadata.SyntheticTable;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.Set;

/**
 * Base class for synthetic tables in the relational layer: a virtual table backed by a record-layer synthetic record
 * type ({@code UnnestedRecordType} and, eventually, joined types) with one or more indexes maintained on it.
 *
 * <p>It is generated from an index definition the stored tables cannot express, rather than declared, so it carries no
 * SQL text and is not reachable by name from a query -- which is why it is a {@link SyntheticTable} and not a
 * {@link com.apple.foundationdb.relational.api.metadata.View}.
 *
 * @see RecordLayerUnnestedSyntheticTable
 */
@API(API.Status.EXPERIMENTAL)
public abstract sealed class RecordLayerSyntheticTable implements SyntheticTable
        permits RecordLayerUnnestedSyntheticTable {

    @Nonnull
    private final String name;

    @Nonnull
    private final Set<RecordLayerIndex> indexes;

    protected RecordLayerSyntheticTable(@Nonnull final String name,
                                       @Nonnull final Set<RecordLayerIndex> indexes) {
        this.name = name;
        this.indexes = ImmutableSet.copyOf(indexes);
    }

    @Nonnull
    @Override
    public String getName() {
        return name;
    }

    /**
     * The name the synthetic record type carries in the protobuf descriptor, derived from {@link #getName()} the same way
     * a column's storage name is derived from its declared name. The declared name comes from user identifiers -- the
     * index name, for an unnested table -- and so need not be a legal protobuf identifier on its own.
     *
     * @return the protobuf-compliant form of this table's name
     */
    @Nonnull
    public String getStorageName() {
        return ProtoUtils.toProtoBufCompliantName(name);
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
        return Objects.equals(name, that.name) && Objects.equals(indexes, that.indexes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, indexes);
    }

    public interface Builder {
        @Nonnull
        Builder addIndex(@Nonnull RecordLayerIndex index);

        @Nonnull
        RecordLayerSyntheticTable build();
    }
}
