/*
 * BooleanValue.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;
import java.util.Set;

/**
 * Shim class to translate objects of type {@link Value} to {@link QueryPredicate}.
 */
public interface BooleanValue extends Value {
    @Nonnull
    @Override
    default Type getResultType() {
        return Type.primitiveType(Type.TypeCode.BOOLEAN);
    }

    /**
     * Translates the {@link BooleanValue} into a {@link QueryPredicate}.
     *
     * @param typeRepository a type repository that can be passed to e.g. compile-time evaluable functions
     * @param localAliases set of aliases which are immediately visible to the expression.
     * @return A {@link QueryPredicate} that is equivalent to this {@link BooleanValue} expression.
     */
    Optional<QueryPredicate> toQueryPredicate(@Nullable TypeRepository typeRepository,
                                              @Nonnull Set<CorrelationIdentifier> localAliases);

    /**
     * Translates the {@link BooleanValue} into a {@link QueryPredicate}.
     *
     * @param typeRepository a type repository that can be passed to e.g. compile-time evaluable functions
     * @param localAlias the alias that is immediately visible to the expression.
     * @return A {@link QueryPredicate} that is equivalent to this {@link BooleanValue} expression.
     */
    default Optional<QueryPredicate> toQueryPredicate(@Nullable TypeRepository typeRepository,
                                              @Nonnull CorrelationIdentifier localAlias) {
        return toQueryPredicate(typeRepository, ImmutableSet.of(localAlias));
    }

    /**
     * Translates an arbitrary boolean-typed {@code value} into an equivalent {@link QueryPredicate}, even if
     * {@code value} doesn't implement {@link BooleanValue} itself (e.g. a boolean literal, constant, or column).
     * Callers that combine boolean operands (e.g. {@link AndOrValue#toQueryPredicate}) should use this for each
     * operand rather than casting to {@link BooleanValue} directly.
     * @param value a boolean-typed value, which may or may not implement {@link BooleanValue}
     * @param typeRepository a type repository that can be passed to e.g. compile-time evaluable functions
     * @param localAliases set of aliases which are immediately visible to the expression
     * @return a {@link QueryPredicate} that is equivalent to {@code value}
     */
    @Nonnull
    static Optional<QueryPredicate> toQueryPredicate(@Nonnull final Value value,
                                                      @Nullable final TypeRepository typeRepository,
                                                      @Nonnull final Set<CorrelationIdentifier> localAliases) {
        // A `BooleanValue` can convert itself into a `QueryPredicate`.
        if (value instanceof BooleanValue) {
            return ((BooleanValue)value).toQueryPredicate(typeRepository, localAliases);
        }

        // A NULL predicate matches nothing, so map it to `ConstantPredicate.NULL`. Check the class and the type,
        // because one SQL NULL arrives in two shapes: `NULL` in the query text becomes a `NullValue`, while a
        // parameter bound to `null` becomes a `ConstantObjectValue` of the special NULL type.
        if (value instanceof NullValue || value.getResultType().isNull()) {
            return Optional.of(ConstantPredicate.NULL);
        }

        // A plain boolean `LiteralValue` (including one whose literal is itself `null`) folds to the
        // corresponding `ConstantPredicate`.
        if (value instanceof LiteralValue<?>) {
            return Optional.of(ConstantPredicate.of((Boolean)((LiteralValue<?>)value).getLiteralValue()));
        }

        // Everything else (a bound constant, a parameter, a column, ...) is lifted into a `ValuePredicate`
        // performing a `«value» = TRUE` comparison.
        return Optional.of(new ValuePredicate(value, new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, true)));
    }
}

