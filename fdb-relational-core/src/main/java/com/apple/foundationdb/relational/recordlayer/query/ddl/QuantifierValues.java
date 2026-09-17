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
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.SimpleValueVisitor;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.relational.util.Assert;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * What every quantifier of an index-defining plan stands for, and with it the ability to resolve a value written in terms
 * of those quantifiers down to the base record. Collected in a pass of its own, ahead of anything that needs it.
 */
final class QuantifierValues {

    @Nonnull
    private final Map<CorrelationIdentifier, Value> valuesByQuantifier;

    private QuantifierValues(@Nonnull final Map<CorrelationIdentifier, Value> valuesByQuantifier) {
        this.valuesByQuantifier = valuesByQuantifier;
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
        return new QuantifierValues(Assert.notNullUnchecked(new Collector().visit(expression)));
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
         * Numbers the unnestings, so that two unnestings of the same array field compare unequal. Only distinctness
         * matters; the number never reaches the key expression.
         */
        @Nonnull
        private final AtomicInteger explodeCounter = new AtomicInteger(0);

        @Nonnull
        @Override
        public Map<CorrelationIdentifier, Value> evaluateAtExpression(@Nonnull final RelationalExpression expression,
                                                                      @Nonnull final List<Map<CorrelationIdentifier, Value>> childResults) {
            final var merged = merge(childResults);
            for (final var quantifier : expression.getQuantifiers()) {
                final var rangesOver = quantifier.getRangesOver().get();
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
            final var marker = explodeCounter.incrementAndGet();
            final var collectionValue = explode.getCollectionValue();
            if (!(collectionValue instanceof FieldValue)) {
                return collectionValue;
            }
            final var field = (FieldValue)collectionValue;
            final var fieldAccessors = new ArrayList<>(field.getFieldPath().getFieldAccessors());
            fieldAccessors.set(fieldAccessors.size() - 1,
                    AnnotatedAccessor.of(fieldAccessors.get(fieldAccessors.size() - 1), marker));
            return FieldValue.ofFields(field.getChild(), new FieldValue.FieldPath(fieldAccessors));
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
     */
    static final class AnnotatedAccessor extends FieldValue.ResolvedAccessor {

        private final int marker;

        private AnnotatedAccessor(@Nonnull final Type.Record.Field field, final int ordinal, final int marker) {
            super(field, ordinal);
            this.marker = marker;
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
