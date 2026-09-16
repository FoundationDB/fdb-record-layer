/*
 * RecordLayerUnnestedSyntheticTableGenerator.java
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

import com.apple.foundationdb.record.metadata.UnnestedRecordType;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QueriedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.SimpleValueVisitor;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.recordlayer.metadata.DataTypeUtils;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSyntheticTable;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerUnnestedSyntheticTable;

import java.util.function.Supplier;
import com.google.common.base.Suppliers;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.NullableArrayUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Generates the unnested synthetic table an index has to be defined on.
 * <p>
 * An index with an unnesting can be maintained from the stored table, with a fan-out key expression. This only works
 * while every column read through one unnesting sits in a contiguous run of the index key, because those columns are
 * emitted under a single navigation into the array. Two or more columns reached through the same unnesting at
 * non-adjacent positions cannot be: a fan-out would have to be emitted twice and would range over the array twice.
 * Those indexes are defined on a synthetic table instead, whose constituents are navigated with
 * {@link KeyExpression.FanType#None} and so may be referenced at any number of key positions.
 * <p>
 * {@link #initIfNeeded} answers whether an index needs one at all, and yields a generator only when it does. Nothing is
 * built until {@link #generate} is called, so an index on a stored table costs no more than the decision. What cannot be
 * defined on a synthetic table is rejected by {@link IndexSpec#checkValidity}, with every other rejection.
 */
final class RecordLayerUnnestedSyntheticTableGenerator {

    /**
     * Prefixes the name of a synthetic table, keeping it out of the space of names a user can declare.
     */
    private static final String UNNESTED_TABLE_NAME_PREFIX = "__unnested_";

    /**
     * Alias of the parent (stored record) constituent. Constituent aliases are persisted in the metadata, so they are
     * derived from the index definition alone rather than taken from the plan's correlation identifiers, whose values
     * depend on how many quantifiers the JVM has allocated and so differ between runs of the same DDL.
     */
    private static final String PARENT_CONSTITUENT_ALIAS = "parent";

    /**
     * Prefixes the alias of each nested constituent, which is numbered by the order its unnesting was found in. The
     * record layer reserves {@code "__"} for constituent names of its own, so this cannot carry that prefix.
     */
    private static final String NESTED_CONSTITUENT_ALIAS_PREFIX = "unnesting_";

    /**
     * Name of the struct that holds the constituent positions. The descriptor nests it inside the synthetic message, so
     * the name only has to be unique within it; it matches what {@code UnnestedRecordTypeBuilder} calls it.
     */
    private static final String POSITIONS_TYPE_NAME = "Positions";

    /**
     * Every unnesting the plan performs, composed from what {@link QuantifierValues} recorded, keyed by marker.
     */
    @Nonnull
    private final Map<Integer, UnnestingInfo> unnestings;

    @Nonnull
    private final String parentAlias;

    /**
     * The stored table the synthetic table's parent constituent stands for.
     */
    @Nonnull
    private final RecordLayerTable parentTable;

    @Nonnull
    private final String syntheticTableName;

    /**
     * Computed once: the key is resolved against this type and it also names the table the index is on, so it is asked
     * for more than once per index.
     */
    @Nonnull
    private final Supplier<Type.Record> syntheticType;

    private RecordLayerUnnestedSyntheticTableGenerator(@Nonnull final Map<Integer, UnnestingInfo> unnestings,
                                     @Nonnull final String parentAlias,
                                     @Nonnull final RecordLayerTable parentTable,
                                     @Nonnull final String syntheticTableName) {
        this.unnestings = unnestings;
        this.parentAlias = parentAlias;
        this.parentTable = parentTable;
        this.syntheticTableName = syntheticTableName;
        this.syntheticType = Suppliers.memoize(this::computeSyntheticType);
    }

    /**
     * A generator for the index's synthetic table, if it needs one at all.
     * <p>
     * Every unnesting a column is read through counts, not only the innermost, so chained unnesting can require a
     * synthetic table even when no innermost unnesting is itself split. Only struct arrays are considered, since a
     * scalar array cannot be a constituent.
     *
     * @param spec what the index is made of
     * @param indexName the name the definition gives the index
     * @param quantifierValues what the plan's quantifiers stand for, and the unnestings it performs
     *
     * @return a generator for the synthetic table, empty when the index is maintained from the stored table
     */
    @Nonnull
    static Optional<RecordLayerUnnestedSyntheticTableGenerator> initIfNeeded(@Nonnull final IndexSpec spec,
                                                           @Nonnull final String indexName,
                                                           @Nonnull final QuantifierValues quantifierValues) {
        final var unnestings = composeUnnestings(quantifierValues);
        if (!isNeededFor(spec, unnestings)) {
            return Optional.empty();
        }
        final var parentTable = spec.table();
        // Currently, the synthetic table name is derived from the record type. This may or may not be true in the
        // future.
        final var syntheticTableName = UNNESTED_TABLE_NAME_PREFIX + parentTable.getType().getName() + "_" + indexName;
        return Optional.of(new RecordLayerUnnestedSyntheticTableGenerator(unnestings, PARENT_CONSTITUENT_ALIAS,
                parentTable, syntheticTableName));
    }

    /**
     * The markers of every unnesting a value is read through, transitively, outermost first. A column carries the
     * marker of each unnesting on its path.
     * <p>
     * For a table {@code A(k, p P array)} whose element type holds {@code Q(y) array}, and the definition
     * <pre>
     * FROM A AS a, (SELECT * FROM a.p) AS b, (SELECT * FROM b.q) AS c
     * </pre>
     * If {@code a.p} is marked 1 and {@code b.q} is marked 2,  each column resolves to a path whose
     * {@link QuantifierValues.AnnotatedAccessor}s give, in the order the path walks them rather than in marker order:
     * <pre>
     * c.y  resolves to  base().P.Q.Y  -&gt;  [1, 2]
     * b.x  resolves to  base().P.X    -&gt;  [1]
     * a.k  resolves to  base().K      -&gt;  []
     * </pre>
     *
     * @param value a value resolved down to the base record
     *
     * @return the markers traversed, empty if there are none
     */
    @Nonnull
    private static List<Integer> unnestingMarkers(@Nonnull final Value value) {
        final var markers = ImmutableList.<Integer>builder();
        if (value instanceof FieldValue) {
            for (final var accessor : ((FieldValue)value).getFieldPath().getFieldAccessors()) {
                if (accessor instanceof QuantifierValues.AnnotatedAccessor) {
                    markers.add(((QuantifierValues.AnnotatedAccessor)accessor).getMarker());
                }
            }
        }
        for (final var child : value.getChildren()) {
            markers.addAll(unnestingMarkers(child));
        }
        return markers.build();
    }

    /**
     * Composes what {@link QuantifierValues} recorded at each explode into what the unnesting means.
     *
     * @param quantifierValues what the plan's quantifiers stand for, and what it explodes
     *
     * @return every unnesting the plan performs, keyed by marker
     */
    @Nonnull
    private static Map<Integer, UnnestingInfo> composeUnnestings(@Nonnull final QuantifierValues quantifierValues) {
        // Only a struct array becomes a constituent, so only those are named, and they are numbered by their order among
        // the constituents rather than by marker: markers count scalar unnestings too, which would leave gaps.
        final var explodes = quantifierValues.getExplodes();
        final Map<Integer, String> aliasByMarker = new LinkedHashMap<>();
        for (int marker = 0; marker < explodes.size(); marker++) {
            if (arrayTypeOf(explodes.get(marker)).getElementType() instanceof Type.Record) {
                aliasByMarker.put(marker, NESTED_CONSTITUENT_ALIAS_PREFIX + aliasByMarker.size());
            }
        }
        final var result = ImmutableMap.<Integer, UnnestingInfo>builder();
        // a marker is an explode's position, so the index is the key
        for (int marker = 0; marker < explodes.size(); marker++) {
            final var collectionValue = explodes.get(marker);
            result.put(marker, new UnnestingInfo(aliasByMarker.get(marker),
                    owningAlias(marker, collectionValue, quantifierValues, aliasByMarker),
                    collectionValue.getFieldPath()));
        }
        return result.build();
    }

    /**
     * The alias of the constituent that owns the array an explode ranges over: the innermost struct-array unnesting
     * enclosing it, or the parent constituent when the array hangs off the stored record. Only a struct array is named,
     * so an unnamed enclosing unnesting is a scalar one and cannot be the owner.
     */
    @Nonnull
    private static String owningAlias(final int marker,
                                      @Nonnull final FieldValue collectionValue,
                                      @Nonnull final QuantifierValues quantifierValues,
                                      @Nonnull final Map<Integer, String> aliasByMarker) {
        final var markers = unnestingMarkers(quantifierValues.resolve(collectionValue));
        for (int i = markers.indexOf(marker) - 1; i >= 0; i--) {
            final var enclosing = aliasByMarker.get(markers.get(i));
            if (enclosing != null) {
                return enclosing;
            }
        }
        return PARENT_CONSTITUENT_ALIAS;
    }

    @Nonnull
    private static Type.Array arrayTypeOf(@Nonnull final FieldValue collectionValue) {
        return (Type.Array)collectionValue.getFieldPath().getLastFieldType();
    }

    /**
     * One unnesting the plan performs.
     *
     * <p>A struct array becomes a constituent of the synthetic table, navigated by {@link #arrayElements()} from
     * {@code owningAlias}. A scalar array cannot be a constituent, since its elements have no fields to reference, so
     * the same expression is instead emitted as a fan-out inside the owning constituent.
     *
     * @param alias the constituent's alias, or {@code null} for a scalar array, which is not a constituent
     * @param owningAlias the constituent the unnested array lives on
     * @param arrayPath the path to the array being unnested, relative to the record that owns it -- the owning
     * constituent, which is not necessarily the stored record. An array reached through non-repeated fields makes this
     * more than one hop.
     */
    private record UnnestingInfo(@Nullable String alias, @Nonnull String owningAlias,
                                @Nonnull FieldValue.FieldPath arrayPath) {

        @Nonnull
        private Type.Array arrayType() {
            return (Type.Array)arrayPath.getLastFieldType();
        }

        @Nonnull
        public KeyExpression arrayElements() {
            return NullableArrayUtils.arrayElements(arrayPath.getFieldAccessors().stream()
                            .map(accessor -> accessor.getField().getFieldStorageName())
                            .collect(ImmutableList.toImmutableList()),
                    arrayType().isNullable());
        }

        public boolean structArray() {
            return arrayType().getElementType() instanceof Type.Record;
        }

        @Nonnull
        public DataType.StructType structElementType() {
            return (DataType.StructType)DataTypeUtils.toRelationalType(
                    Objects.requireNonNull(arrayType().getElementType()));
        }
    }

    /**
     * The same index, made of the synthetic table rather than the stored record: it is the record type the index is on,
     * and the projection and ordering are resolved against it.
     *
     * @param spec what the index is made of, resolved against the stored record
     *
     * @return the same index, in the synthetic table's coordinates
     */
    @Nonnull
    public IndexSpec rewrite(@Nonnull final IndexSpec spec) {
        Assert.isNullUnchecked(spec.predicate(), ErrorCode.UNSUPPORTED_OPERATION,
                "predicate on an index over an unnested synthetic table");
        Assert.isNullUnchecked(spec.groupBy(), ErrorCode.UNSUPPORTED_OPERATION,
                "group by on an index over an unnested synthetic table");
        // The slot names the stored table the index reads from, which the synthetic table is built over, so it carries
        // through unchanged; what the index is defined on is the synthetic type, which the caller takes from here.
        return new IndexSpec(spec.scanCount(), spec.table(), null, null,
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

    /**
     * Rewrites the columns from the stored record's coordinates into the synthetic table's, so that they can be
     * translated as ordinary field paths.
     *
     * @param values the index key columns, resolved against the stored record
     *
     * @return the same columns, resolved against the synthetic table
     */
    @Nonnull
    private List<Value> rewrite(@Nonnull final List<Value> values) {
        final var rewriter = new Rewriter(new QueriedValue(getSyntheticType()));
        return values.stream()
                .map(value -> Objects.requireNonNull(value.acceptVisitor(rewriter)))
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Re-roots a column at the synthetic table. Anything that is not a plain column reference reaches
     * {@link #evaluateAtValue} and is rejected there.
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
            throw new RelationalException(
                    "Unsupported index definition, an index over an unnested synthetic table supports only plain column references",
                    ErrorCode.UNSUPPORTED_OPERATION).toUncheckedWrappedException();
        }

        @Nonnull
        @Override
        public Value visitFieldValue(@Nonnull final FieldValue fieldValue) {
            final var accessors = fieldValue.getFieldPath().getFieldAccessors();
            int innermostIdx = -1;
            for (int i = accessors.size() - 1; i >= 0; i--) {
                if (accessors.get(i) instanceof QuantifierValues.AnnotatedAccessor) {
                    innermostIdx = i;
                    break;
                }
            }
            final var names = ImmutableList.<String>builder();
            final List<FieldValue.ResolvedAccessor> remaining;
            @Nullable final UnnestingInfo innermost;
            if (innermostIdx < 0) {
                // a column of the stored record itself
                innermost = null;
                names.add(parentAlias);
                remaining = accessors;
            } else {
                final var marker = ((QuantifierValues.AnnotatedAccessor)accessors.get(innermostIdx)).getMarker();
                innermost = Assert.notNullUnchecked(unnestings.get(marker), "unknown unnesting in index definition");
                if (innermost.structArray()) {
                    // the unnesting is a constituent: name it, and continue from its element type
                    names.add(innermost.alias());
                    remaining = accessors.subList(innermostIdx + 1, accessors.size());
                } else {
                    // a scalar array is not a constituent: it stays a field of whichever constituent owns it
                    names.add(innermost.owningAlias());
                    remaining = accessors.subList(innermostIdx, accessors.size());
                }
            }
            remaining.forEach(accessor -> names.add(accessor.getField().getFieldName()));
            final var rewritten = FieldValue.ofFieldNames(root, names.build());
            if (innermost == null || innermost.structArray()) {
                return rewritten;
            }
            // Re-tag the scalar array's accessor: the marker is what says this array is reached through an unnest, which is
            // what lets it be emitted as a fan-out rather than rejected.
            final var rewrittenAccessors = new ArrayList<>(rewritten.getFieldPath().getFieldAccessors());
            final var last = rewrittenAccessors.size() - 1;
            rewrittenAccessors.set(last, QuantifierValues.AnnotatedAccessor.of(rewrittenAccessors.get(last),
                    ((QuantifierValues.AnnotatedAccessor)accessors.get(innermostIdx)).getMarker()));
            return FieldValue.ofFields(rewritten.getChild(), new FieldValue.FieldPath(rewrittenAccessors));
        }
    }

    @Nonnull
    Type.Record getSyntheticType() {
        return syntheticType.get();
    }

    @Nonnull
    private Type.Record computeSyntheticType() {
        final var parentType = parentTable.getDatatype();
        final var fields = ImmutableList.<DataType.StructType.Field>builder();
        int fieldNumber = 1;
        fields.add(DataType.StructType.Field.from(parentAlias, parentType, fieldNumber++));
        final var positions = ImmutableList.<DataType.StructType.Field>builder();
        int positionNumber = 1;
        for (final var info : unnestings.values()) {
            if (info.structArray()) {
                // Nullable, because the descriptor declares every constituent field optional. The composed type has to
                // agree with it, or a reloaded template's synthetic table is unequal to the one the DDL built.
                fields.add(DataType.StructType.Field.from(info.alias(),
                        info.structElementType().withNullable(true), fieldNumber++));
                positions.add(DataType.StructType.Field.from(info.alias(),
                        DataType.Primitives.NULLABLE_LONG.type(), positionNumber++));
            }
        }
        fields.add(DataType.StructType.Field.from(UnnestedRecordType.POSITIONS_FIELD,
                DataType.StructType.from(POSITIONS_TYPE_NAME, positions.build(), true), fieldNumber));
        return (Type.Record)DataTypeUtils.toRecordLayerType(
                DataType.StructType.from(syntheticTableName, fields.build(), false));
    }

    /**
     * Builds the synthetic table: the stored record as parent constituent, and one nested constituent per unnested
     * struct array, each navigated from the constituent that owns its array.
     *
     * @return the synthetic table, which the caller has to register alongside the index
     */
    @Nonnull
    public RecordLayerSyntheticTable.Builder generate() {
        final var builder = RecordLayerUnnestedSyntheticTable.newBuilder(syntheticType.get())
                .setAlias(parentAlias)
                .setParentTableType(parentTable.getType());
        unnestings.values().stream()
                .filter(UnnestingInfo::structArray)
                .forEach(info -> builder.addConstituent(new RecordLayerUnnestedSyntheticTable.NestedConstituent(
                        info.alias(), info.owningAlias(), info.arrayElements())));
        return builder;
    }

    /**
     * Whether every scalar unnesting the key reads through is referenced at no more than one key position. A scalar
     * array cannot be a constituent, so each reference is emitted as its own fan-out over the array; two of them would
     * range over it independently and yield a cross-product of one view column against itself, which nothing else on
     * this path would catch. An index on the stored table is left to the trie's disconnected-reference guard, which
     * rejects the same shape there.
     *
     * @param values the index key columns
     *
     * @return whether no scalar unnesting is referenced twice
     */
    public boolean scalarUnnestingsReferencedOnce(@Nonnull final List<Value> values) {
        // Distinct per position, since one value can read through the same unnesting more than once (e.g. `M.x + M.y`);
        // what is counted has to be a number of key positions.
        final var scalarMarkers = values.stream()
                .flatMap(value -> unnestingMarkers(value).stream().distinct())
                .filter(marker -> {
                    final var info = unnestings.get(marker);
                    return info == null || !info.structArray();
                })
                .collect(ImmutableList.toImmutableList());
        return scalarMarkers.size() == ImmutableSet.copyOf(scalarMarkers).size();
    }

    /**
     * Whether the index key reads two or more columns through one unnesting at non-adjacent positions, and so has to be
     * defined on a synthetic table. Visible to {@link IndexSpec}, which rejects what cannot be defined on one.
     */
    private static boolean isNeededFor(@Nonnull final IndexSpec spec,
                               @Nonnull final Map<Integer, UnnestingInfo> unnestings) {
        final Map<Integer, Integer> firstPositions = new LinkedHashMap<>();
        final Map<Integer, Integer> lastPositions = new LinkedHashMap<>();
        final Map<Integer, Integer> counts = new LinkedHashMap<>();
        final List<Value> keyValues = spec.rootValues();
        for (int i = 0; i < keyValues.size(); i++) {
            // Distinct markers per position: the counts below must be a number of key positions, and one value can
            // read through the same unnesting more than once (e.g. `M.x + M.y`).
            for (final var marker : ImmutableSet.copyOf(unnestingMarkers(keyValues.get(i)))) {
                final var info = unnestings.get(marker);
                // skip scalar arrays, which cannot be constituents
                if (info == null || !info.structArray()) {
                    continue;
                }
                firstPositions.putIfAbsent(marker, i);
                lastPositions.put(marker, i);
                counts.merge(marker, 1, Integer::sum);
            }
        }
        return counts.entrySet().stream().anyMatch(entry ->
                lastPositions.get(entry.getKey()) - firstPositions.get(entry.getKey()) + 1 != entry.getValue());
    }
}
