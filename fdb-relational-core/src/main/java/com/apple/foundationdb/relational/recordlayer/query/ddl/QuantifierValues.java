/*
 * QuantifierValues.java
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.SimpleExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.OuterJoinExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.SimpleValueVisitor;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.util.Assert;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * What every quantifier of an index-defining plan stands for, and with it the ability to resolve a value written in terms
 * of those quantifiers down to the base record. Collected in a pass of its own, ahead of anything that needs it.
 */
final class QuantifierValues {

    @Nonnull
    private final Map<CorrelationIdentifier, Value> valuesByQuantifier;

    /**
     * The array each explode of the plan ranges over, in the order they were found. An explode's {@link AnnotatedAccessor}
     * marker <em>is</em> its position here.
     */
    @Nonnull
    private final List<FieldValue> explodes;

    /**
     * The stored record type behind each type-filter quantifier, by the correlation it is bound to, in the order they
     * were found. One is an ordinary single-table definition; two or more make the plan a join, and then each
     * correlation becomes a constituent of a joined synthetic table.
     */
    @Nonnull
    private final Map<CorrelationIdentifier, String> storedConstituents;

    private final boolean hasOuterJoin;

    /**
     * The select each stored table is a quantifier of. More than one means they sit in nested selects, and so cannot be
     * related to each other by any one select's predicates.
     */
    @Nonnull
    private final Set<RelationalExpression> storedConstituentOwners;

    private QuantifierValues(@Nonnull final Map<CorrelationIdentifier, Value> valuesByQuantifier,
                             @Nonnull final List<FieldValue> explodes,
                             @Nonnull final Map<CorrelationIdentifier, String> storedConstituents,
                             final boolean hasOuterJoin,
                             @Nonnull final Set<RelationalExpression> storedConstituentOwners) {
        this.valuesByQuantifier = valuesByQuantifier;
        this.explodes = explodes;
        // Insertion-ordered on purpose: the order the tables were found decides the constituent ordinals,
        // and Map.copyOf would not preserve it.
        this.storedConstituents = Collections.unmodifiableMap(new LinkedHashMap<>(storedConstituents));
        this.hasOuterJoin = hasOuterJoin;
        this.storedConstituentOwners = Set.copyOf(storedConstituentOwners);
    }

    /**
     * Whether the plan ranges over more than one stored record type, and so defines its index on a joined synthetic
     * table rather than on a stored one.
     *
     * @return whether this is a join
     */
    public boolean isJoin() {
        return storedConstituents.size() > 1;
    }

    /**
     * The stored record types the plan joins, by the correlation each is bound to, in the order found. Only meaningful
     * when {@link #isJoin()}.
     *
     * @return the joined tables, by correlation
     */
    @Nonnull
    public Map<CorrelationIdentifier, String> getStoredConstituents() {
        return storedConstituents;
    }

    /**
     * Whether the plan contains an outer join, which a joined synthetic table cannot represent since every constituent
     * of one is inner-joined.
     *
     * @return whether an outer join was found
     */
    public boolean hasOuterJoin() {
        return hasOuterJoin;
    }

    /**
     * Whether every stored table is selected from directly rather than through a nested select.
     *
     * @return whether all stored tables belong to one select
     */
    public boolean storedConstituentsShareOneSelect() {
        return storedConstituentOwners.size() <= 1;
    }

    /**
     * The array each explode of the plan ranges over, in the order they were found.
     *
     * @return what the traversal saw at each explode
     */
    @Nonnull
    public List<FieldValue> getExplodes() {
        return explodes;
    }

    /**
     * Collects the mapping for a plan.
     *
     * @param expression the root of the index-defining plan
     *
     * @return what each of its quantifiers stands for
     */
    @Nonnull
    public static QuantifierValues collect(@Nonnull final RelationalExpression expression) {
        final var collector = new Collector();
        return new QuantifierValues(collector.visit(expression), collector.explodes,
                collector.storedConstituents, collector.hasOuterJoin, collector.storedConstituentOwners);
    }

    /**
     * Resolves a value down to the base record and simplifies it.
     *
     * @param value a value written in terms of the plan's quantifiers
     *
     * @return the same value with every quantifier replaced by what it stands for
     */
    @Nonnull
    public Value resolve(@Nonnull final Value value) {
        return dereference(value).simplify(EvaluationContext.empty(), AliasMap.emptyMap(), Set.of());
    }

    @Nonnull
    private Value dereference(@Nonnull final Value value) {
        return Objects.requireNonNull(value.acceptVisitor(new Dereferencer()));
    }

    /**
     * Replaces every quantifier with what it stands for, rebuilding the values above it.
     */
    private final class Dereferencer implements SimpleValueVisitor<Value> {

        @Nonnull
        @Override
        public Value evaluateAtValue(@Nonnull final Value value, @Nonnull final List<Value> childResults) {
            // a leaf stands for itself
            return childResults.isEmpty() ? value : value.withChildren(childResults);
        }

        @Nonnull
        @Override
        public Value visitQuantifiedObjectValue(@Nonnull final QuantifiedObjectValue element) {
            // A joined constituent stands for itself: resolving it would reduce every constituent to the same base
            // record, and with it which of the joined tables a column was read from -- the one thing a joined synthetic
            // table is built out of. The unnested path recovers that from its markers; a join has no array, so the
            // correlation is all that carries it.
            if (isJoin() && storedConstituents.containsKey(element.getAlias())) {
                return element;
            }
            // what a quantifier stands for may reference another
            return visit(Assert.notNullUnchecked(valuesByQuantifier.get(element.getAlias())));
        }
    }

    /**
     * The traversal. It contributes what a node's own quantifiers stand for, merges that with its children, and validates
     * nothing.
     */
    private static final class Collector implements SimpleExpressionVisitor<Map<CorrelationIdentifier, Value>> {

        /**
         * The array each explode ranges over, in the order they were found. A position doubles as the explode's marker,
         * which only has to be distinct -- it numbers the unnestings so two unnestings of one array field compare
         * unequal, and never reaches the key expression.
         */
        @Nonnull
        private final List<FieldValue> explodes = new ArrayList<>();

        /**
         * The stored record type behind each type-filter quantifier, in the order found.
         */
        @Nonnull
        private final Map<CorrelationIdentifier, String> storedConstituents = new LinkedHashMap<>();

        private boolean hasOuterJoin;

        @Nonnull
        private final Set<RelationalExpression> storedConstituentOwners = new LinkedHashSet<>();

        @Nonnull
        @Override
        public Map<CorrelationIdentifier, Value> evaluateAtExpression(@Nonnull final RelationalExpression expression,
                                                                      @Nonnull final List<Map<CorrelationIdentifier, Value>> childResults) {
            final var merged = merge(childResults);
            if (expression instanceof OuterJoinExpression) {
                hasOuterJoin = true;
            }
            for (final var quantifier : expression.getQuantifiers()) {
                final var rangesOver = quantifier.getRangesOver().get();
                if (rangesOver instanceof LogicalTypeFilterExpression typeFilter) {
                    final var recordTypes = typeFilter.getRecordTypes();
                    // A filter over anything but a single type is left for IndexSpec to report, as it always has.
                    if (recordTypes.size() == 1) {
                        storedConstituents.putIfAbsent(quantifier.getAlias(), recordTypes.iterator().next());
                        storedConstituentOwners.add(expression);
                    }
                }
                // a quantifier over an explode stands for the collection being unnested, not for the explode's result
                merged.put(quantifier.getAlias(), rangesOver instanceof ExplodeExpression
                                                  ? unnestedCollectionValue((ExplodeExpression)rangesOver)
                                                  : rangesOver.getResultValue());
            }
            return merged;
        }

        @Nonnull
        @Override
        public Map<CorrelationIdentifier, Value> evaluateAtRef(@Nonnull final Reference ref,
                                                               @Nonnull final List<Map<CorrelationIdentifier, Value>> memberResults) {
            return merge(memberResults);
        }

        @Nonnull
        private Value unnestedCollectionValue(@Nonnull final ExplodeExpression explode) {
            final var collectionValue = explode.getCollectionValue();
            if (!(collectionValue instanceof final FieldValue field)) {
                return collectionValue;
            }
            final var marker = explodes.size();
            final var fieldAccessors = new ArrayList<>(field.getFieldPath().getFieldAccessors());
            fieldAccessors.set(fieldAccessors.size() - 1,
                    AnnotatedAccessor.of(fieldAccessors.get(fieldAccessors.size() - 1), marker));
            final var annotated = FieldValue.ofFields(field.getChild(), new FieldValue.FieldPath(fieldAccessors));
            explodes.add(annotated);
            return annotated;
        }

        @Nonnull
        private static Map<CorrelationIdentifier, Value> merge(@Nonnull final List<Map<CorrelationIdentifier, Value>> results) {
            final var merged = new LinkedHashMap<CorrelationIdentifier, Value>();
            results.forEach(merged::putAll);
            return merged;
        }
    }

    /**
     * A {@link FieldValue.ResolvedAccessor} tagged with which unnesting it came from, which distinguishes two unnestings
     * of the same array field and marks the field as reached through an unnest.
     * <p>
     * That tag is the marker: a small integer handed to each explode of the plan in the order the traversal finds them,
     * so that it is also the explode's position in {@link QuantifierValues#getExplodes()}. It is stamped onto the last
     * accessor of the field path that reaches the array, and so travels along with every value later built from that
     * path; a plain {@link FieldValue.ResolvedAccessor} in that position means the field was not reached through an
     * unnest at all. Two markers therefore mean two unnestings even where the field paths are identical, as in
     * {@code FROM T1, T1.A X, T1.A Y}. A consumer recovers the markers a key column reads through and looks each one up
     * against the explode it names, which is how it tells what a column was unnested from.
     */
    static final class AnnotatedAccessor extends FieldValue.ResolvedAccessor {

        private final int marker;

        private AnnotatedAccessor(@Nonnull final Type.Record.Field field, final int ordinal, final int marker) {
            super(field, ordinal);
            this.marker = marker;
        }

        /**
         * Which unnesting of the plan the field this accessor reaches was unnested by.
         *
         * @return the marker, which is that unnesting's position in {@link QuantifierValues#getExplodes()}
         */
        int getMarker() {
            return marker;
        }

        @Nonnull
        static AnnotatedAccessor of(@Nonnull final FieldValue.ResolvedAccessor accessor, final int marker) {
            return new AnnotatedAccessor(accessor.getField(), accessor.getOrdinal(), marker);
        }

        // NOTE: equals is asymmetric with ResolvedAccessor, which compares ordinal only. Annotated and plain
        // accessors must therefore never be keys of the same map.
        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass() || !super.equals(other)) {
                return false;
            }
            return marker == ((AnnotatedAccessor)other).marker;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), marker);
        }
    }
}
