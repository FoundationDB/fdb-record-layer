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
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
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

    @Nonnull
    private final Supplier<Fingerprint<RecordLayerJoinedSyntheticTable>> fingerprint;

    @Nonnull
    private final Supplier<JoinGraph> joinGraph;

    private RecordLayerJoinedSyntheticTable(@Nonnull final List<JoinedConstituent> constituents,
                                            @Nonnull final List<JoinCondition> joinConditions,
                                            @Nonnull final Set<RecordLayerIndex> indexes,
                                            @Nonnull final Type.Record type) {
        super(indexes, type);
        this.constituents = List.copyOf(constituents);
        this.joinConditions = List.copyOf(joinConditions);
        this.fingerprint = Suppliers.memoize(() -> generateFingerprint(this.constituents, this.joinConditions));
        this.joinGraph = Suppliers.memoize(() -> new JoinGraph(this.constituents, this.joinConditions));
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

    /**
     * {@inheritDoc}
     *
     * <p>See {@link #generateFingerprint}.
     */
    @Nonnull
    @Override
    public Fingerprint<RecordLayerJoinedSyntheticTable> getFingerprint() {
        return fingerprint.get();
    }

    /**
     * How a joined table of the given constituents and join conditions corresponds to existing joined tables of the same
     * fingerprint, without building it.
     *
     * @param constituents the joined constituents
     * @param joinConditions the equalities relating them
     * @return for an existing joined table of the same fingerprint, for each of the given aliases the alias of the
     * constituent it corresponds to, or empty if the existing table does not produce the same synthetic records
     */
    @Nonnull
    public static Function<RecordLayerJoinedSyntheticTable, Optional<Map<String, String>>> matcher(
            @Nonnull final List<JoinedConstituent> constituents, @Nonnull final List<JoinCondition> joinConditions) {
        final var graph = new JoinGraph(constituents, joinConditions);
        return existing -> graph.matchOnto(existing.joinGraph.get());
    }

    /**
     * The fingerprint of a joined table of the given constituents and join conditions.
     *
     * @param constituents the joined constituents
     * @param joinConditions the equalities relating them
     * @return the fingerprint
     */
    @Nonnull
    public static Fingerprint<RecordLayerJoinedSyntheticTable> generateFingerprint(
            @Nonnull final List<JoinedConstituent> constituents, @Nonnull final List<JoinCondition> joinConditions) {
        return new JoinedFingerprint(joinConditions.size(), constituents.stream()
                .map(JoinedConstituent::tableStorageName)
                .collect(ImmutableMultiset.toImmutableMultiset()));
    }

    @Nonnull
    @Override
    public RecordLayerJoinedSyntheticTable withIndex(@Nonnull final RecordLayerIndex index) {
        return new RecordLayerJoinedSyntheticTable(constituents, joinConditions, indexesWith(index), type);
    }

    @Override
    public boolean equals(final Object o) {
        return o instanceof RecordLayerJoinedSyntheticTable that
                && super.equals(o)
                && Objects.equals(constituents, that.constituents)
                && Objects.equals(joinConditions, that.joinConditions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), constituents, joinConditions);
    }

    /**
     * The constituents of a joined table as a graph: a vertex per constituent, labelled with the storage name of its
     * table, and an edge per join condition, labelled with the column each side reads.
     *
     * <p>A correspondence is sought the way the planner matches quantifiers: every bijection pairing constituents of
     * the same table is enumerated, and the first one under which the join conditions are the same is taken.
     */
    public static final class JoinGraph {

        /**
         * The aliases of the constituents in registration order, which is the order the bijections are enumerated in.
         */
        @Nonnull
        private final ImmutableSet<CorrelationIdentifier> aliases;

        @Nonnull
        private final Map<CorrelationIdentifier, String> storageNameByAlias;

        /**
         * For each ordered pair of constituents, the edges between them, each oriented from the first constituent to the
         * second as the column it reads on either side.
         */
        @Nonnull
        private final Map<NonnullPair<CorrelationIdentifier, CorrelationIdentifier>, ImmutableMultiset<List<KeyExpression>>> edgesByPair;

        JoinGraph(@Nonnull final List<JoinedConstituent> constituents,
                          @Nonnull final List<JoinCondition> joinConditions) {
            this.aliases = constituents.stream()
                    .map(JoinedConstituent::alias)
                    .map(CorrelationIdentifier::of)
                    .collect(ImmutableSet.toImmutableSet());
            this.storageNameByAlias = constituents.stream()
                    .collect(ImmutableMap.toImmutableMap(constituent -> CorrelationIdentifier.of(constituent.alias()),
                            JoinedConstituent::tableStorageName));
            final Map<NonnullPair<CorrelationIdentifier, CorrelationIdentifier>, ImmutableMultiset.Builder<List<KeyExpression>>> edges = new HashMap<>();
            for (final var condition : joinConditions) {
                final var left = CorrelationIdentifier.of(condition.leftAlias());
                final var right = CorrelationIdentifier.of(condition.rightAlias());
                edges.computeIfAbsent(NonnullPair.of(left, right), ignored -> ImmutableMultiset.builder())
                        .add(List.of(condition.leftExpression(), condition.rightExpression()));
                edges.computeIfAbsent(NonnullPair.of(right, left), ignored -> ImmutableMultiset.builder())
                        .add(List.of(condition.rightExpression(), condition.leftExpression()));
            }
            this.edgesByPair = edges.entrySet().stream()
                    .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().build()));
        }

        /**
         * Matches this graph onto another one of the same fingerprint.
         *
         * @param other a graph of a joined table of the same fingerprint, so of as many constituents of the same tables
         * and as many join conditions
         * @return for each alias of this graph, the alias of the constituent of {@code other} it corresponds to, or empty
         * if there is no such correspondence
         */
        @Nonnull
        private Optional<Map<String, String>> matchOnto(@Nonnull final JoinGraph other) {
            // The match predicate only sees the bindings of the constituents an alias depends on, and constituents
            // depend on none, so it can only pair constituents of the same table; the join conditions are compared once
            // a bijection is complete.
            final Iterable<AliasMap> bijections = AliasMap.emptyMap().findCompleteMatches(
                    aliases, ignored -> ImmutableSet.of(),
                    other.aliases, ignored -> ImmutableSet.of(),
                    (alias, otherAlias, ignored) ->
                            storageNameByAlias.get(alias).equals(other.storageNameByAlias.get(otherAlias)));
            return Streams.stream(bijections)
                    .filter(bijection -> preservesJoinConditions(bijection, other))
                    .findFirst()
                    .map(bijection -> bijection.entrySet().stream()
                            .collect(ImmutableMap.toImmutableMap(entry -> entry.getKey().getId(),
                                    entry -> entry.getValue().getId())));
        }

        /**
         * Whether every two constituents are related by the same join conditions as their counterparts under the
         * bijection. Both graphs have as many join conditions, so the other one has none beyond those compared here.
         */
        private boolean preservesJoinConditions(@Nonnull final AliasMap bijection, @Nonnull final JoinGraph other) {
            for (final var pairAndEdges : edgesByPair.entrySet()) {
                final var pair = pairAndEdges.getKey();
                final var otherEdges = other.edgesByPair.getOrDefault(
                        NonnullPair.of(bijection.getTarget(pair.getLeft()), bijection.getTarget(pair.getRight())),
                        ImmutableMultiset.of());
                if (!pairAndEdges.getValue().equals(otherEdges)) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * What two joined tables must share to produce the same synthetic records up to a renaming of their constituents:
     * the number of join conditions, and the tables joined, by storage name, each as many times as it is joined, so
     * that their number is that of the constituents and a self-join counts every copy. It is equal for many other joined
     * tables too, so {@link #matcher} compares the join conditions themselves.
     *
     * @param joinConditionCount the number of join conditions
     * @param tableStorageNames the storage name of each constituent's table
     */
    @API(API.Status.EXPERIMENTAL)
    public record JoinedFingerprint(int joinConditionCount, @Nonnull ImmutableMultiset<String> tableStorageNames)
            implements Fingerprint<RecordLayerJoinedSyntheticTable> {
        @Nonnull
        private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Joined-Fingerprint");

        @Nonnull
        @Override
        public Class<RecordLayerJoinedSyntheticTable> getTableClass() {
            return RecordLayerJoinedSyntheticTable.class;
        }

        @Override
        public boolean equals(final Object o) {
            return o instanceof JoinedFingerprint that
                    && joinConditionCount == that.joinConditionCount
                    && tableStorageNames.equals(that.tableStorageNames);
        }

        /**
         * Spelled out, as the hash code of a record is left to the implementation, while those of the base hash's name
         * and of a multiset of strings are specified.
         */
        @Override
        public int hashCode() {
            return Objects.hash(BASE_HASH, joinConditionCount, tableStorageNames);
        }
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
         * @return this builder
         */
        @Nonnull
        public Builder addConstituent(@Nonnull final String alias, @Nonnull final Type.Record tableType) {
            return addConstituent(new JoinedConstituent(alias, tableType.getName(),
                    Objects.requireNonNull(tableType.getStorageName())));
        }

        @Nonnull
        public Builder addJoinCondition(@Nonnull final JoinCondition joinCondition) {
            joinConditions.add(joinCondition);
            return this;
        }

        @Nonnull
        @Override
        public Type.Record getType() {
            return type;
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
            Assert.thatUnchecked(constituents.size() >= 2, ErrorCode.INVALID_SCHEMA_TEMPLATE,
                    () -> "joined table must have at least two constituents, found " + constituents.size());
            final Set<String> aliases = new LinkedHashSet<>();
            for (final JoinedConstituent constituent : constituents) {
                Assert.thatUnchecked(aliases.add(constituent.alias()), ErrorCode.INVALID_SCHEMA_TEMPLATE,
                        "duplicate constituent alias '%s' in joined table", constituent.alias());
            }
            for (final JoinCondition joinCondition : joinConditions) {
                Assert.thatUnchecked(aliases.contains(joinCondition.leftAlias()), ErrorCode.INVALID_SCHEMA_TEMPLATE,
                        () -> "join condition references unknown constituent '" + joinCondition.leftAlias() + "'");
                Assert.thatUnchecked(aliases.contains(joinCondition.rightAlias()), ErrorCode.INVALID_SCHEMA_TEMPLATE,
                        () -> "join condition references unknown constituent '" + joinCondition.rightAlias() + "'");
                Assert.thatUnchecked(!joinCondition.leftAlias().equals(joinCondition.rightAlias()),
                        ErrorCode.INVALID_SCHEMA_TEMPLATE,
                        "join condition relates constituent '%s' to itself", joinCondition.leftAlias());
                Assert.thatUnchecked(joinCondition.leftExpression().getColumnSize()
                                == joinCondition.rightExpression().getColumnSize(),
                        ErrorCode.INVALID_SCHEMA_TEMPLATE,
                        "join condition sides of constituents '%s' and '%s' are not the same size",
                        joinCondition.leftAlias(), joinCondition.rightAlias());
            }
            return new RecordLayerJoinedSyntheticTable(constituents, joinConditions, indexes.build(), type);
        }
    }
}
