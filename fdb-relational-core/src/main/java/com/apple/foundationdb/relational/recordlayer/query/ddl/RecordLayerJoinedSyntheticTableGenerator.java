/*
 * RecordLayerJoinedSyntheticTableGenerator.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query.ddl;

import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValueAndRanges;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.SimpleValueVisitor;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.recordlayer.metadata.DataTypeUtils;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerJoinedSyntheticTable;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSyntheticTable;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.IdentityHashMap;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;

/**
 * Builds the joined synthetic table an index is defined on, for a definition whose columns come from more than one
 * stored table.
 *
 * <p>Every constituent is inner-joined, one per joined table in the order the plan found them. The equalities relating
 * the tables become the synthetic table's join conditions — a joined record is precisely the combination of stored
 * records that satisfies them, so there is nothing left to filter.
 */
final class RecordLayerJoinedSyntheticTableGenerator implements SyntheticTableGenerator {

    private static final String JOINED_TABLE_NAME_PREFIX = "__joined_";

    /**
     * Prefixes the alias of each joined constituent, numbered by the order its table was found in.
     */
    private static final String CONSTITUENT_ALIAS_PREFIX = "joined_";

    /**
     * The joined tables, by the correlation each is bound to, in the order the plan found them. That order is the order
     * the constituents are registered in, and so is what the index key's constituent names refer to.
     */
    @Nonnull
    private final Map<CorrelationIdentifier, ConstituentInfo> constituents;

    /**
     * What the plan's quantifiers stand for, which is the shape {@link #checkSupported} judges.
     */
    @Nonnull
    private final QuantifierValues quantifierValues;

    /**
     * The equalities relating the constituents, and the filters the definition applies on top of them. The filters are
     * stored with the index and evaluated against the joined record, so that only the joined records the definition
     * asks for are indexed.
     *
     * <p>Split on demand rather than on construction, so that {@link #checkSupported} gets to reject an unsupported
     * shape before the definition's predicates are read as a join.
     */
    @Nonnull
    private final Supplier<SplitPredicates> split;

    @Nonnull
    private final RecordLayerSchemaTemplate.Builder schemaTemplateBuilder;

    @Nonnull
    private final String syntheticTableName;

    @Nonnull
    private final Supplier<Type.Record> syntheticType;

    private RecordLayerJoinedSyntheticTableGenerator(@Nonnull final Map<CorrelationIdentifier, ConstituentInfo> constituents,
                                       @Nullable final QueryPredicate predicate,
                                       @Nonnull final QuantifierValues quantifierValues,
                                       @Nonnull final RecordLayerSchemaTemplate.Builder schemaTemplateBuilder,
                                       @Nonnull final String syntheticTableName) {
        this.constituents = constituents;
        this.quantifierValues = quantifierValues;
        this.split = Suppliers.memoize(() -> splitPredicates(predicate, constituents.keySet()));
        this.schemaTemplateBuilder = schemaTemplateBuilder;
        this.syntheticTableName = syntheticTableName;
        this.syntheticType = Suppliers.memoize(this::computeSyntheticType);
    }

    /**
     * A generator for the index's joined synthetic table, the definition having been found to range over more than one
     * stored table.
     *
     * @param schemaTemplateBuilder the metadata the stored record types are looked up in
     * @param spec what the index is made of, whose predicate carries the join conditions
     * @param indexName the name the definition gives the index
     * @param quantifierValues what the plan's quantifiers stand for, and which of them are stored tables
     *
     * @return a generator for the synthetic table, which every join needs
     */
    @Nonnull
    static Optional<RecordLayerJoinedSyntheticTableGenerator> initIfNeeded(@Nonnull final RecordLayerSchemaTemplate.Builder schemaTemplateBuilder,
                                                                          @Nonnull final IndexSpec spec,
                                                                          @Nonnull final String indexName,
                                                                          @Nonnull final QuantifierValues quantifierValues) {
        final var constituents = nameConstituents(quantifierValues.getStoredConstituents());
        // A synthetic table exists for one index, so its name carries that index's.
        final var syntheticTableName = JOINED_TABLE_NAME_PREFIX + indexName;
        return Optional.of(new RecordLayerJoinedSyntheticTableGenerator(constituents, spec.predicate(),
                quantifierValues, schemaTemplateBuilder, syntheticTableName));
    }

    /**
     * Assigns each joined table a constituent alias derived from the definition: the position its table was found at,
     * so the same DDL always yields the same metadata.
     *
     * @param storedConstituents the joined tables, by the correlation each is bound to
     *
     * @return the same tables, each with the alias it is registered under
     */
    @Nonnull
    private static Map<CorrelationIdentifier, ConstituentInfo> nameConstituents(
            @Nonnull final Map<CorrelationIdentifier, String> storedConstituents) {
        final var named = new LinkedHashMap<CorrelationIdentifier, ConstituentInfo>();
        var ordinal = 0;
        for (final var entry : storedConstituents.entrySet()) {
            named.put(entry.getKey(),
                    new ConstituentInfo(CONSTITUENT_ALIAS_PREFIX + ordinal++, entry.getValue()));
        }
        // Insertion-ordered: the ordinals above, the synthetic type's field order and the order constituents
        // are registered in all follow it, so Map.copyOf would not do.
        return Collections.unmodifiableMap(named);
    }

    /**
     * One joined constituent: the alias it is registered under, and the stored record type it stands for.
     */
    private record ConstituentInfo(@Nonnull String alias, @Nonnull String recordTypeName) {
    }

    /**
     * Rejects what a joined synthetic table cannot express, separately from the definitions
     * {@link IndexSpec#checkValidity} rejects for any index.
     *
     * @param spec what the index is made of
     */
    @Override
    public void checkSupported(@Nonnull final IndexSpec spec) {
        // Stated in the order that gives the clearest reason first: what the shape is, before what it lacks.
        Assert.thatUnchecked(!quantifierValues.hasOuterJoin(), ErrorCode.UNSUPPORTED_OPERATION,
                "Unsupported index definition, an outer join is not supported on an index over a joined synthetic table");
        Assert.thatUnchecked(quantifierValues.storedConstituentsShareOneSelect(), ErrorCode.UNSUPPORTED_OPERATION,
                "Unsupported index definition, the joined tables must be selected from directly rather than through a subquery");
        final var projection = spec.projection();
        Assert.thatUnchecked(projection.aggregate() == null, ErrorCode.UNSUPPORTED_OPERATION,
                "Unsupported index definition, an aggregate cannot be defined on a joined synthetic table");
        // The row version belongs to a stored record, and a joined record has none of its own: taking one constituent's
        // would make the index depend on which side it was read through.
        Assert.thatUnchecked(projection.versionValues().isEmpty(), ErrorCode.UNSUPPORTED_OPERATION,
                "Unsupported index definition, a version column cannot be part of an index over a joined synthetic table");
    }

    /**
     * Splits the definition's predicates into the equalities that relate two constituents, which become the synthetic
     * table's join conditions, and everything else, which stays a filter on the joined record.
     *
     * <p>Both DDL spellings arrive here identically: an {@code INNER JOIN ... ON} condition is conjoined into the
     * {@code WHERE} predicate while the query graph is built, so either way the join appears as an equality predicate
     * relating two constituents.
     */
    @Nonnull
    private static SplitPredicates splitPredicates(@Nullable final QueryPredicate predicate,
                                                   @Nonnull final Set<CorrelationIdentifier> constituents) {
        // Without a condition the synthetic table would be the full cross product of its constituents, which is never
        // what an index wants and would be maintained on every insert into either table.
        final var conjunction = Assert.notNullUnchecked(predicate, ErrorCode.UNSUPPORTED_OPERATION,
                "Unsupported index definition, an index over more than one table requires a join condition relating them");
        final var joinConditions = ImmutableList.<JoinCondition>builder();
        final var residuals = ImmutableList.<QueryPredicate>builder();
        for (final var conjunct : conjuncts(conjunction)) {
            final var joinCondition = asJoinCondition(conjunct, constituents);
            if (joinCondition.isPresent()) {
                joinConditions.add(joinCondition.get());
            } else {
                residuals.add(conjunct);
            }
        }
        final var result = new SplitPredicates(joinConditions.build(), residuals.build());
        Assert.thatUnchecked(!result.joinConditions().isEmpty(), ErrorCode.UNSUPPORTED_OPERATION,
                "Unsupported index definition, an index over more than one table requires a join condition relating them");
        return result;
    }

    @Nonnull
    private static List<QueryPredicate> conjuncts(@Nonnull final QueryPredicate predicate) {
        // A single condition arrives on its own; several are conjoined, and each conjunct is considered on its own.
        return predicate instanceof AndPredicate
               ? ImmutableList.copyOf(((AndPredicate)predicate).getChildren())
               : ImmutableList.of(predicate);
    }

    /**
     * Whether a conjunct is an equality between columns of two different constituents, and so a join condition rather
     * than a filter. Anything else — a comparison against a literal, an inequality, two columns of one constituent — is
     * a filter on the joined record and is left to {@link #rewriteResidual}.
     */
    @Nonnull
    private static Optional<JoinCondition> asJoinCondition(@Nonnull final QueryPredicate predicate,
                                                           @Nonnull final Set<CorrelationIdentifier> constituents) {
        if (!(predicate instanceof PredicateWithValueAndRanges)) {
            return Optional.empty();
        }
        final var withRanges = (PredicateWithValueAndRanges)predicate;
        final var comparisons = withRanges.getComparisons();
        if (comparisons.size() != 1) {
            return Optional.empty();
        }
        final var comparison = comparisons.get(0);
        if (comparison.getType() != Comparisons.Type.EQUALS || !(comparison instanceof Comparisons.ValueComparison)) {
            return Optional.empty();
        }
        final var left = constituentColumnOf(withRanges.getValue(), constituents);
        final var right = constituentColumnOf(((Comparisons.ValueComparison)comparison).getComparandValue(), constituents);
        if (left.isEmpty() || right.isEmpty() || left.get().alias().equals(right.get().alias())) {
            return Optional.empty();
        }
        return Optional.of(new JoinCondition(left.get().alias(), left.get().keyExpression(),
                right.get().alias(), right.get().keyExpression()));
    }

    /**
     * The constituent a plain column reference reads from, and the key expression navigating to it within that
     * constituent's record. Empty when the value is not a column of one constituent.
     */
    @Nonnull
    private static Optional<ConstituentColumn> constituentColumnOf(@Nonnull final Value value,
                                                                   @Nonnull final Set<CorrelationIdentifier> constituents) {
        if (!(value instanceof FieldValue)) {
            return Optional.empty();
        }
        final var fieldValue = (FieldValue)value;
        final var correlations = fieldValue.getCorrelatedTo();
        if (correlations.size() != 1) {
            return Optional.empty();
        }
        final var alias = correlations.iterator().next();
        if (!constituents.contains(alias)) {
            return Optional.empty();
        }
        final var accessors = fieldValue.getFieldPath().getFieldAccessors();
        if (accessors.isEmpty()) {
            return Optional.empty();
        }
        KeyExpression keyExpression = null;
        for (int i = accessors.size() - 1; i >= 0; i--) {
            final var storageName = Assert.notNullUnchecked(accessors.get(i).getField().getFieldStorageName());
            keyExpression = keyExpression == null ? field(storageName) : field(storageName).nest(keyExpression);
        }
        return Optional.of(new ConstituentColumn(alias, Objects.requireNonNull(keyExpression)));
    }

    /**
     * A filter the definition applies on top of the join, re-expressed against the synthetic record so that it can be
     * stored with the index and evaluated as the joined record is maintained.
     */
    @Nonnull
    private QueryPredicate rewriteResidual(@Nonnull final QueryPredicate residual) {
        Assert.thatUnchecked(residual instanceof PredicateWithValueAndRanges, ErrorCode.UNSUPPORTED_OPERATION,
                "Unsupported index definition, a filter on an index over a joined synthetic table must compare a column against a constant");
        final var withRanges = (PredicateWithValueAndRanges)residual;
        // Only the column side is re-rooted, so a comparison whose other side reads a constituent would be left naming a
        // correlation the synthetic record does not have. Rejected rather than silently mis-stored.
        for (final var comparison : withRanges.getComparisons()) {
            Assert.thatUnchecked(comparison.getCorrelatedTo().stream().noneMatch(constituents::containsKey),
                    ErrorCode.UNSUPPORTED_OPERATION,
                    "Unsupported index definition, a filter on an index over a joined synthetic table cannot compare two of the joined tables");
        }
        final var rewriter = new Rewriter(new QueriedValue(getType()));
        return withRanges.withValue(Objects.requireNonNull(withRanges.getValue().acceptVisitor(rewriter)));
    }

    /**
     * The join conditions the definition states, and the filters it applies on top of them.
     */
    private record SplitPredicates(@Nonnull List<JoinCondition> joinConditions,
                                   @Nonnull List<QueryPredicate> residuals) {
    }

    /**
     * The same index, made of the joined synthetic table rather than the stored records: it is the record type the index
     * is on, and the projection and ordering are resolved against it. The predicate is dropped, having been consumed as
     * the join conditions.
     *
     * @param spec what the index is made of, resolved against the stored records
     *
     * @return the same index, in the synthetic table's coordinates
     */
    @Nonnull
    @Override
    public IndexSpec rewrite(@Nonnull final IndexSpec spec) {
        Assert.isNullUnchecked(spec.groupBy(), "group by on an index over a joined synthetic table");
        final var residualPredicates = split.get().residuals();
        // Normalised after re-rooting, so that what is checked to be storable is the form actually stored.
        final var residual = residualPredicates.isEmpty()
                             ? null
                             : IndexPredicates.normalize(residualPredicates.stream()
                                     .map(this::rewriteResidual)
                                     .collect(ImmutableList.toImmutableList()));
        return new IndexSpec(spec.scanCount(), spec.table(), residual, null,
                spec.orderBy() == null ? null : rewrite(spec.orderBy()),
                new IndexSpec.Projection(rewrite(spec.projection().values())));
    }

    /**
     * Ordering, resolved against the synthetic table.
     */
    @Nonnull
    private IndexSpec.OrderBy rewrite(@Nonnull final IndexSpec.OrderBy orderBy) {
        final var values = orderBy.values();
        final var rewritten = rewrite(values);
        final Map<Value, String> functions = new IdentityHashMap<>();
        for (int i = 0; i < values.size(); i++) {
            final var function = orderBy.orderingFunctions().get(values.get(i));
            if (function != null) {
                functions.put(rewritten.get(i), function);
            }
        }
        return new IndexSpec.OrderBy(rewritten, functions);
    }

    @Nonnull
    private List<Value> rewrite(@Nonnull final List<Value> values) {
        final var rewriter = new Rewriter(new QueriedValue(getType()));
        return values.stream()
                .map(value -> Objects.requireNonNull(value.acceptVisitor(rewriter)))
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Re-roots a column at the synthetic table, naming the constituent it was read from. The constituent's correlation
     * survives resolution — {@link QuantifierValues} stops there for exactly this reason — so it is what says which of
     * the joined tables the column came from.
     */
    private final class Rewriter implements SimpleValueVisitor<Value> {

        @Nonnull
        private final Value root;

        private Rewriter(@Nonnull final Value root) {
            this.root = root;
        }

        @Nonnull
        @Override
        public Value evaluateAtValue(@Nonnull final Value value, @Nonnull final List<Value> childResults) {
            throw Assert.failUnchecked(ErrorCode.UNSUPPORTED_OPERATION,
                    "Unsupported index definition, an index over a joined synthetic table supports only plain column references");
        }

        @Nonnull
        @Override
        public Value visitFieldValue(@Nonnull final FieldValue fieldValue) {
            final var correlations = fieldValue.getCorrelatedTo();
            Assert.thatUnchecked(correlations.size() == 1, ErrorCode.UNSUPPORTED_OPERATION,
                    "Unsupported index definition, an index key column must come from exactly one joined table");
            final var alias = correlations.iterator().next();
            final var constituent = Assert.notNullUnchecked(constituents.get(alias), ErrorCode.UNSUPPORTED_OPERATION,
                    "Unsupported index definition, an index key column must come from one of the joined tables");
            final var names = ImmutableList.<String>builder();
            names.add(constituent.alias());
            fieldValue.getFieldPath().getFieldAccessors()
                    .forEach(accessor -> names.add(accessor.getField().getFieldName()));
            return FieldValue.ofFieldNames(root, names.build());
        }
    }

    /**
     * The alias the given correlation's table is registered under.
     */
    @Nonnull
    private String aliasOf(@Nonnull final CorrelationIdentifier correlation) {
        return Assert.notNullUnchecked(constituents.get(correlation)).alias();
    }

    @Nonnull
    @Override
    public Type.Record getType() {
        return syntheticType.get();
    }

    /**
     * The synthetic record: one field per joined table, named by the constituent's alias, so that a column of it is an
     * ordinary field path.
     */
    @Nonnull
    private Type.Record computeSyntheticType() {
        final var fields = ImmutableList.<DataType.StructType.Field>builder();
        var fieldNumber = 1;
        for (final var constituent : constituents.values()) {
            final var tableType = schemaTemplateBuilder.findTableByStorageName(constituent.recordTypeName()).getDatatype();
            fields.add(DataType.StructType.Field.from(constituent.alias(), tableType, fieldNumber++));
        }
        return (Type.Record)DataTypeUtils.toRecordLayerType(
                DataType.StructType.from(syntheticTableName, fields.build(), false));
    }

    /**
     * Builds the synthetic table: one inner-joined constituent per joined table, and one join for each equality
     * relating them.
     *
     * @return the synthetic table, which the caller has to register alongside the index
     */
    @Nonnull
    @Override
    public RecordLayerSyntheticTable.Builder generate() {
        final var builder = RecordLayerJoinedSyntheticTable.newBuilder(syntheticType.get());
        constituents.values().forEach(constituent ->
                builder.addConstituent(constituent.alias(),
                        schemaTemplateBuilder.findTableByStorageName(constituent.recordTypeName()).getType()));
        split.get().joinConditions().forEach(condition ->
                builder.addJoinCondition(new RecordLayerJoinedSyntheticTable.JoinCondition(
                        aliasOf(condition.leftAlias()), condition.leftExpression(),
                        aliasOf(condition.rightAlias()), condition.rightExpression())));
        return builder;
    }

    /**
     * One side of a join condition: the constituent it reads from, and the column within that constituent's record.
     */
    private record ConstituentColumn(@Nonnull CorrelationIdentifier alias, @Nonnull KeyExpression keyExpression) {
    }

    /**
     * An equality between a column of one constituent and a column of another.
     */
    private record JoinCondition(@Nonnull CorrelationIdentifier leftAlias, @Nonnull KeyExpression leftExpression,
                                 @Nonnull CorrelationIdentifier rightAlias, @Nonnull KeyExpression rightExpression) {
    }
}
