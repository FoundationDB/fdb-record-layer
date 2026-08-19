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
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A synthetic record type that unnests one or more struct array fields of a stored record type. Each
 * combination of a stored record and one element from each unnested array forms a single synthetic record,
 * so an index key can reference several fields of the same element without fanning out over the array once
 * per reference.
 *
 * <p>Only struct arrays become constituents; a scalar array contributes at most one key column, so it stays
 * a fan-out in the index key expression. A constituent's array lives either on the stored record or, for
 * chained unnesting, on another constituent's element type. The nesting expression that reaches the elements
 * carries the {@code FanOut}, and its shape follows how the array is stored: a nullable array is wrapped as
 * {@code { repeated T values; }}, while a non-nullable one is a plain repeated field.
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
        // e.g. SELECT "row".*, SQ.* FROM "T" AS "row", (SELECT * FROM "row"."TAGS") AS SQ
        // A synthetic record is the stored record together with one element from each constituent, so the parent
        // is projected alongside the constituents — index keys on this type reference its fields too.
        final StringBuilder sb = new StringBuilder("SELECT ");
        sb.append(Stream.concat(Stream.of('"' + alias + '"'),
                        constituents.stream().map(NestedConstituent::getAlias))
                .map(projected -> projected + ".*")
                .collect(Collectors.joining(", ")));
        sb.append(" FROM \"").append(parentTableName).append("\" AS \"").append(alias).append("\"");
        for (final NestedConstituent nested : constituents) {
            // The array is on whichever constituent owns it, which for a chained unnesting is another
            // nested constituent rather than the stored record.
            sb.append(", (SELECT * FROM \"").append(nested.getParentAlias()).append("\".\"")
              .append(String.join("\".\"", nested.getFieldPath())).append("\") AS ").append(nested.getAlias());
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
        private final KeyExpression nestingExpression;

        public NestedConstituent(@Nonnull final String alias,
                                 @Nonnull final String parentAlias,
                                 @Nonnull final KeyExpression nestingExpression) {
            this.alias = alias;
            this.parentAlias = parentAlias;
            this.nestingExpression = nestingExpression;
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

        /**
         * Expression navigating from the owning constituent's record down to this constituent's elements. This is
         * the same expression the record layer stores and evaluates, kept verbatim so that a synthetic type read
         * back out of {@link com.apple.foundationdb.record.RecordMetaData} round-trips exactly.
         */
        @Nonnull
        public KeyExpression getNestingExpression() {
            return nestingExpression;
        }

        /**
         * The chain of proto field names that {@link #getNestingExpression()} walks, e.g. {@code [scores]} for a
         * plain repeated field, or {@code [scores, values]} for a nullable array stored wrapped.
         */
        @Nonnull
        public List<String> getFieldPath() {
            final ImmutableList.Builder<String> fieldPath = ImmutableList.builder();
            KeyExpression remaining = nestingExpression;
            while (remaining instanceof NestingKeyExpression nesting) {
                fieldPath.add(nesting.getParent().getFieldName());
                remaining = nesting.getChild();
            }
            if (remaining instanceof FieldKeyExpression fieldKeyExpression) {
                fieldPath.add(fieldKeyExpression.getFieldName());
            } else {
                throw Assert.failUnchecked("unsupported nesting expression '" + nestingExpression
                        + "' for constituent '" + alias + "'");
            }
            return fieldPath.build();
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
