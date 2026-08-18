/*
 * RecordLayerUnnestedSyntheticTable.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.util.ProtoUtils;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * A synthetic record type that unnests a repeated/array field of a stored record type. Each
 * stored record produces one synthetic record, and the index fans out over the array elements.
 *
 * <p>Both scalar ({@code STRING ARRAY}) and struct array types use the same underlying wrapper
 * message ({@code { repeated <element_type> values; }}) as the nested constituent descriptor.
 * The nesting expression always navigates to the wrapper with {@code FanType.None}; FanOut is
 * expressed in the index key expression.
 */
@API(API.Status.EXPERIMENTAL)
public final class RecordLayerUnnestedSyntheticTable extends RecordLayerSyntheticTable {

    @Nonnull
    private final String alias;

    @Nonnull
    private final String parentTableName;

    @Nonnull
    private final String parentTableStorageName;

    @Nonnull
    private final List<NestedConstituent> constituents;

    private RecordLayerUnnestedSyntheticTable(@Nonnull final String name,
                                    @Nonnull final String alias,
                                    @Nonnull final String parentTableName,
                                    @Nonnull final String parentTableStorageName,
                                    @Nonnull final List<NestedConstituent> constituents,
                                    @Nonnull final Set<RecordLayerIndex> indexes) {
        super(name, indexes);
        this.alias = alias;
        this.parentTableName = parentTableName;
        this.parentTableStorageName = parentTableStorageName;
        this.constituents = ImmutableList.copyOf(constituents);
    }

    /** Correlation alias of the parent (stored) constituent, e.g. {@code "row"}. */
    @Nonnull
    public String getAlias() {
        return alias;
    }

    @Nonnull
    public String getParentTableName() {
        return parentTableName;
    }

    @Nonnull
    public String getParentTableStorageName() {
        return parentTableStorageName;
    }

    @Nonnull
    public List<NestedConstituent> getConstituents() {
        return constituents;
    }

    @Nonnull
    @Override
    public String getDescription() {
        // Synthesized from constituents — e.g.:
        // SELECT SQ.* FROM "T" AS "row", (SELECT * FROM "row"."TAGS") AS SQ
        final StringBuilder sb = new StringBuilder("SELECT ");
        for (int i = 0; i < constituents.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(constituents.get(i).getAlias()).append(".*");
        }
        sb.append(" FROM \"").append(parentTableName).append("\" AS \"").append(alias).append("\"");
        for (final NestedConstituent nested : constituents) {
            sb.append(", (SELECT * FROM \"").append(alias).append("\".\"")
              .append(nested.getArrayFieldStorageName()).append("\") AS ").append(nested.getAlias());
        }
        return sb.toString();
    }

    @Override
    public void accept(@Nonnull final com.apple.foundationdb.relational.api.metadata.Visitor visitor) {
        super.accept(visitor);
    }

    /**
     * A nested constituent of an {@link RecordLayerUnnestedSyntheticTable}, representing one array fan-out.
     * The {@code parentAlias} is the alias of the constituent from which this one is unnested — for a
     * single-level unnesting this is the stored-record parent alias; for chained unnesting it is the
     * alias of the immediately preceding nested constituent.
     */
    @API(API.Status.EXPERIMENTAL)
    public static final class NestedConstituent {

        @Nonnull
        private final String alias;

        @Nonnull
        private final String parentAlias;

        @Nonnull
        private final String arrayFieldStorageName;

        public NestedConstituent(@Nonnull final String alias,
                                 @Nonnull final String parentAlias,
                                 @Nonnull final String arrayFieldStorageName) {
            this.alias = alias;
            this.parentAlias = parentAlias;
            this.arrayFieldStorageName = arrayFieldStorageName;
        }

        /** Correlation alias of this constituent, e.g. {@code "SQ"}. */
        @Nonnull
        public String getAlias() {
            return alias;
        }

        /** Alias of the constituent from which this one is unnested. */
        @Nonnull
        public String getParentAlias() {
            return parentAlias;
        }

        /** Proto storage name of the array field on the parent constituent. */
        @Nonnull
        public String getArrayFieldStorageName() {
            return arrayFieldStorageName;
        }
    }

    @Nonnull
    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * Builder for {@link RecordLayerUnnestedSyntheticTable}.
     *
     * <p>Use {@link #setParentTableType(Type.Record)} as the primary way to set the parent table,
     * mirroring {@link RecordLayerIndex.Builder#setTableType(Type.Record)}.
     */
    public static final class Builder implements RecordLayerSyntheticTable.Builder {

        @Nullable
        private String name;
        @Nullable
        private String alias;
        @Nullable
        private String parentTableName;
        @Nullable
        private String parentTableStorageName;
        @Nonnull
        private final List<NestedConstituent> constituents = new ArrayList<>();
        @Nonnull
        private final ImmutableSet.Builder<RecordLayerIndex> indexes = ImmutableSet.builder();

        @Nonnull
        public Builder setName(@Nonnull final String name) {
            this.name = name;
            return this;
        }

        @Nonnull
        public Builder setAlias(@Nonnull final String alias) {
            this.alias = alias;
            return this;
        }

        @Nonnull
        public Builder setParentTableName(@Nonnull final String parentTableName) {
            this.parentTableName = parentTableName;
            return this;
        }

        @Nonnull
        public Builder setParentTableStorageName(@Nonnull final String parentTableStorageName) {
            this.parentTableStorageName = parentTableStorageName;
            return this;
        }

        /** Mirrors {@link RecordLayerIndex.Builder#setTableType(Type.Record)}. */
        @Nonnull
        public Builder setParentTableType(@Nonnull final Type.Record tableType) {
            return setParentTableName(tableType.getName())
                    .setParentTableStorageName(tableType.getStorageName());
        }

        @Nonnull
        public Builder addConstituent(@Nonnull final NestedConstituent constituent) {
            constituents.add(constituent);
            return this;
        }

        @Nonnull
        @Override
        public Builder addIndex(@Nonnull final RecordLayerIndex index) {
            indexes.add(index);
            return this;
        }

        @Nonnull
        @Override
        public RecordLayerUnnestedSyntheticTable build() {
            Assert.notNullUnchecked(name, "unnested type name is not set");
            Assert.notNullUnchecked(alias, "parent constituent alias is not set");
            Assert.notNullUnchecked(parentTableName, "parent table name is not set");
            if (parentTableStorageName == null) {
                parentTableStorageName = ProtoUtils.toProtoBufCompliantName(parentTableName);
            }
            Assert.thatUnchecked(!constituents.isEmpty(), "unnested type has no nested constituents");
            return new RecordLayerUnnestedSyntheticTable(name, alias, parentTableName, parentTableStorageName,
                    constituents, indexes.build());
        }
    }
}
