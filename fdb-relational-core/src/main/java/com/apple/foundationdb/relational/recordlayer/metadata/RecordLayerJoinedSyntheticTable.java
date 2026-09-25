/*
 * RecordLayerJoinedSyntheticTable.java
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
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.util.ProtoUtils;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A synthetic table that joins two or more stored record types. Each combination of stored records satisfying every
 * join condition forms one synthetic record, so an index key can reference fields of several tables at once.
 *
 * <p>Every constituent is inner-joined; outer joins are not supported (yet). A join condition is an equality between a
 * column of one constituent and a column of another, which is exactly what
 * {@link com.apple.foundationdb.record.metadata.JoinedRecordTypeBuilder} accepts.
 */
@API(API.Status.EXPERIMENTAL)
public final class RecordLayerJoinedSyntheticTable extends RecordLayerSyntheticTable {

    @Nonnull
    private final List<JoinedConstituent> constituents;

    @Nonnull
    private final List<JoinCondition> joinConditions;

    private RecordLayerJoinedSyntheticTable(@Nonnull final List<JoinedConstituent> constituents,
                                            @Nonnull final List<JoinCondition> joinConditions,
                                            @Nonnull final Set<RecordLayerIndex> indexes,
                                            @Nonnull final Type.Record type) {
        super(indexes, type);
        this.constituents = List.copyOf(constituents);
        this.joinConditions = List.copyOf(joinConditions);
    }

    /**
     * The joined constituents, in the order they were registered.
     *
     * @return the constituents
     */
    @Nonnull
    public List<JoinedConstituent> getConstituents() {
        return constituents;
    }

    /**
     * The equalities relating the constituents.
     *
     * @return the join conditions
     */
    @Nonnull
    public List<JoinCondition> getJoinConditions() {
        return joinConditions;
    }

    /**
     * Every joined table, since a write to any of them maintains this table's indexes.
     */
    @Nonnull
    @Override
    public Set<String> getUnderlyingTableNames() {
        return constituents.stream()
                .map(JoinedConstituent::tableName)
                .collect(Collectors.toUnmodifiableSet());
    }

    @Override
    public boolean equals(final Object o) {
        if (!super.equals(o)) {
            return false;
        }
        final RecordLayerJoinedSyntheticTable that = (RecordLayerJoinedSyntheticTable) o;
        return Objects.equals(constituents, that.constituents)
                && Objects.equals(joinConditions, that.joinConditions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), constituents, joinConditions);
    }

    /**
     * One constituent of a {@link RecordLayerJoinedSyntheticTable}: a stored record type together with the correlation
     * the index definition referenced it by, which is also how the index key names it.
     *
     * @param alias the alias the constituent is registered under
     * @param tableName the joined table
     * @param tableStorageName the joined table by its protobuf storage name, which is how the record layer refers to it
     */
    @API(API.Status.EXPERIMENTAL)
    public record JoinedConstituent(@Nonnull String alias, @Nonnull String tableName,
                                    @Nonnull String tableStorageName) {
    }

    /**
     * An equality between a column of one constituent and a column of another. The expressions are the ones the record
     * layer stores and evaluates, kept verbatim so that a joined table read back out of
     * {@link com.apple.foundationdb.record.RecordMetaData} round-trips exactly.
     *
     * @param leftAlias the constituent the left column reads from
     * @param leftExpression the left column, relative to that constituent's record
     * @param rightAlias the constituent the right column reads from
     * @param rightExpression the right column, relative to that constituent's record
     */
    @API(API.Status.EXPERIMENTAL)
    public record JoinCondition(@Nonnull String leftAlias, @Nonnull KeyExpression leftExpression,
                                @Nonnull String rightAlias, @Nonnull KeyExpression rightExpression) {
    }

    @Nonnull
    public static Builder newBuilder(@Nonnull final Type.Record type) {
        return new Builder(type);
    }

    /**
     * Builder for {@link RecordLayerJoinedSyntheticTable}.
     */
    public static final class Builder implements RecordLayerSyntheticTable.Builder {

        @Nonnull
        private final List<JoinedConstituent> constituents = new ArrayList<>();
        @Nonnull
        private final List<JoinCondition> joinConditions = new ArrayList<>();
        @Nonnull
        private final ImmutableSet.Builder<RecordLayerIndex> indexes = ImmutableSet.builder();
        @Nonnull
        private final Type.Record type;

        private Builder(@Nonnull final Type.Record type) {
            this.type = type;
        }

        @Nonnull
        public Builder addConstituent(@Nonnull final JoinedConstituent constituent) {
            constituents.add(constituent);
            return this;
        }

        /**
         * Adds a constituent for a stored table, mirroring {@link RecordLayerIndex.Builder#setTableType(Type.Record)}.
         *
         * @param alias the correlation the definition referenced the table by
         * @param tableType the stored table
         *
         * @return this builder
         */
        @Nonnull
        public Builder addConstituent(@Nonnull final String alias, @Nonnull final Type.Record tableType) {
            return addConstituent(new JoinedConstituent(alias, tableType.getName(),
                    tableType.getStorageName() == null
                    ? ProtoUtils.toProtoBufCompliantName(tableType.getName())
                    : tableType.getStorageName()));
        }

        @Nonnull
        public Builder addJoinCondition(@Nonnull final JoinCondition joinCondition) {
            joinConditions.add(joinCondition);
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
        public RecordLayerJoinedSyntheticTable build() {
            Assert.thatUnchecked(constituents.size() >= 2,
                    "joined table must have at least two constituents, found " + constituents.size());
            final Set<String> aliases = new LinkedHashSet<>();
            for (final JoinedConstituent constituent : constituents) {
                Assert.thatUnchecked(aliases.add(constituent.alias()),
                        "duplicate constituent alias '" + constituent.alias() + "' in joined table");
            }
            // A condition naming an unregistered alias cannot be registered on a JoinedRecordType, so it is caught here
            // rather than deeper in the record layer.
            for (final JoinCondition joinCondition : joinConditions) {
                Assert.thatUnchecked(aliases.contains(joinCondition.leftAlias()),
                        "join condition references unknown constituent '" + joinCondition.leftAlias() + "'");
                Assert.thatUnchecked(aliases.contains(joinCondition.rightAlias()),
                        "join condition references unknown constituent '" + joinCondition.rightAlias() + "'");
            }
            return new RecordLayerJoinedSyntheticTable(constituents, joinConditions, indexes.build(), type);
        }
    }
}
