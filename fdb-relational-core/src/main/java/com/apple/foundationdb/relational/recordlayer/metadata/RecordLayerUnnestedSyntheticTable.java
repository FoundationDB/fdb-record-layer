/*
 * RecordLayerUnnestedSyntheticTable.java
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
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

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

    private RecordLayerUnnestedSyntheticTable(@Nonnull final String alias,
                                              @Nonnull final String parentTableName,
                                              @Nonnull final String parentTableStorageName,
                                              @Nonnull final List<NestedConstituent> constituents,
                                              @Nonnull final Set<RecordLayerIndex> indexes,
                                              @Nonnull final Type.Record recordType) {
        super(indexes, recordType);
        this.alias = alias;
        this.parentTableName = parentTableName;
        this.parentTableStorageName = parentTableStorageName;
        this.constituents = ImmutableList.copyOf(constituents);
    }

    @Nonnull
    public String getAlias() {
        return alias;
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
    public Set<String> getUnderlyingTableNames() {
        return Set.of(parentTableName);
    }

    @Override
    public boolean equals(final Object o) {
        return o instanceof RecordLayerUnnestedSyntheticTable that
                && super.equals(o)
                && Objects.equals(alias, that.alias)
                && Objects.equals(parentTableName, that.parentTableName)
                && Objects.equals(parentTableStorageName, that.parentTableStorageName)
                && Objects.equals(constituents, that.constituents);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), alias, parentTableName, parentTableStorageName, constituents);
    }

    /**
     * A nested constituent of an {@link RecordLayerUnnestedSyntheticTable}, representing one array fan-out.
     * The {@code parentAlias} is the alias of the constituent from which this one is unnested. For a
     * single-level unnesting this is the stored-record parent alias; for chained unnesting it is the
     * alias of the immediately preceding nested constituent.
     */
    public static final class NestedConstituent {

        @Nonnull
        private final String alias;

        @Nonnull
        private final String parentAlias;

        @Nonnull
        private final KeyExpression nestingExpression;

        @Nonnull
        private final List<String> fieldPath;

        public NestedConstituent(@Nonnull final String alias,
                                 @Nonnull final String parentAlias,
                                 @Nonnull final KeyExpression nestingExpression) {
            this.alias = alias;
            this.parentAlias = parentAlias;
            this.nestingExpression = nestingExpression;
            this.fieldPath = computeFieldPath(alias, nestingExpression);
        }

        @Nonnull
        public String getAlias() {
            return alias;
        }

        @Nonnull
        public String getParentAlias() {
            return parentAlias;
        }

        /**
         * Expression navigating from the owning constituent's record down to this constituent's elements. This is
         * the same expression the record layer stores and evaluates.
         */
        @Nonnull
        public KeyExpression getNestingExpression() {
            return nestingExpression;
        }

        @Override
        public boolean equals(final Object o) {
            return o instanceof NestedConstituent that
                    && Objects.equals(alias, that.alias)
                    && Objects.equals(parentAlias, that.parentAlias)
                    && Objects.equals(nestingExpression, that.nestingExpression);
        }

        @Override
        public int hashCode() {
            return Objects.hash(alias, parentAlias, nestingExpression);
        }

        /**
         * The chain of proto field names that {@link #getNestingExpression()} walks, e.g. {@code [scores]} for a
         * plain repeated field, or {@code [scores, values]} for a nullable array stored wrapped.
         */
        @Nonnull
        public List<String> getFieldPath() {
            return fieldPath;
        }

        /**
         * Walks the nesting expression into the chain of field names the serializer follows to reach the element
         * descriptor.
         *
         * @param alias the constituent's alias, for the rejection message
         * @param nestingExpression the expression navigating to the constituent's elements
         *
         * @return the field names the expression walks, outermost first
         */
        @Nonnull
        private static List<String> computeFieldPath(@Nonnull final String alias,
                                                    @Nonnull final KeyExpression nestingExpression) {
            final ImmutableList.Builder<String> fieldPath = ImmutableList.builder();
            KeyExpression remaining = nestingExpression;
            while (remaining instanceof NestingKeyExpression nesting) {
                fieldPath.add(nesting.getParent().getFieldName());
                remaining = nesting.getChild();
            }
            Assert.thatUnchecked(remaining instanceof FieldKeyExpression, ErrorCode.INVALID_SCHEMA_TEMPLATE,
                    "unsupported nesting expression '%s' for constituent '%s'", nestingExpression, alias);
            fieldPath.add(((FieldKeyExpression)remaining).getFieldName());
            return fieldPath.build();
        }
    }

    @Nonnull
    public static Builder newBuilder(Type.Record type) {
        return new Builder().setType(type);
    }

    /**
     * Builder for {@link RecordLayerUnnestedSyntheticTable}.
     * <p>
     * {@link #build()} checks the constituents to form a tree rooted at the stored record. It rejects a
     * table with no nested constituent, a duplicate alias, and a constituent whose parent alias is not the parent
     * constituent's or an earlier constituent's.
     */
    public static final class Builder implements RecordLayerSyntheticTable.Builder {

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
        @Nullable
        private Type.Record type;

        @Nonnull
        public Builder setType(@Nonnull final Type.Record type) {
            this.type = type;
            return this;
        }

        @Nonnull
        public Builder setAlias(@Nonnull final String alias) {
            this.alias = alias;
            return this;
        }

        @Nonnull
        public Builder setParentTableType(@Nonnull final Type.Record tableType) {
            this.parentTableName = tableType.getName();
            this.parentTableStorageName = tableType.getStorageName();
            return this;
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
            Assert.notNullUnchecked(alias, "parent constituent alias is not set");
            Assert.notNullUnchecked(parentTableName, "parent table type is not set");
            Assert.notNullUnchecked(type, "type is not set");
            Assert.thatUnchecked(!constituents.isEmpty(), "unnested type has no nested constituents");
            final Set<String> aliases = new LinkedHashSet<>();
            aliases.add(alias);
            for (final NestedConstituent constituent : constituents) {
                Assert.thatUnchecked(aliases.contains(constituent.getParentAlias()),
                        ErrorCode.INVALID_SCHEMA_TEMPLATE,
                        "constituent parent alias '%s' is not a known alias", constituent.getParentAlias());
                final var isUnique = aliases.add(constituent.getAlias());
                Assert.thatUnchecked(isUnique, ErrorCode.INVALID_SCHEMA_TEMPLATE,
                        "duplicate constituent alias '%s' in unnested type", constituent.getAlias());
            }
            return new RecordLayerUnnestedSyntheticTable(alias, parentTableName, parentTableStorageName,
                    constituents, indexes.build(), type);
        }
    }
}
