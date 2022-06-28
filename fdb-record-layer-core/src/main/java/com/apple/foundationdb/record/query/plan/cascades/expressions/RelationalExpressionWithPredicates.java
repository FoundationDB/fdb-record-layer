/*
 * RelationalExpressionWithPredicates.java
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

package com.apple.foundationdb.record.query.plan.cascades.expressions;

import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValue;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

/**
 * A (relational) expression that has a predicate on it.
 */
public interface RelationalExpressionWithPredicates extends RelationalExpression {
    @Nonnull
    List<? extends QueryPredicate> getPredicates();

    @Nonnull
    @Override
    default Set<Type> getDynamicTypes() {
        final ImmutableSet.Builder<Type> resultBuilder = ImmutableSet.builder();

        resultBuilder.addAll(RelationalExpression.super.getDynamicTypes());

        for (final QueryPredicate predicate : getPredicates()) {
            final Set<Type> typesForPredicate =
                    predicate.fold(p -> {
                        if (p instanceof PredicateWithValue) {
                            return ((PredicateWithValue)p).getValue().getDynamicTypes();
                        }
                        return ImmutableSet.<Type>of();
                    }, (thisTypes, childTypeSets) -> {
                        final ImmutableSet.Builder<Type> nestedBuilder = ImmutableSet.builder();
                        for (final Set<Type> childTypes : childTypeSets) {
                            nestedBuilder.addAll(childTypes);
                        }
                        nestedBuilder.addAll(thisTypes);
                        return nestedBuilder.build();
                    });
            resultBuilder.addAll(typesForPredicate);
        }

        return resultBuilder.build();
    }

    @Nonnull
    default ImmutableSet<FieldValue> fieldValuesFromPredicates() {
        return fieldValuesFromPredicates(getPredicates());
    }

    /**
     * Return all {@link FieldValue}s contained in the predicates handed in.
     * @param predicates a collection of predicates
     * @return a set of {@link FieldValue}s
     */
    @Nonnull
    static ImmutableSet<FieldValue> fieldValuesFromPredicates(@Nonnull final Collection<? extends QueryPredicate> predicates) {
        return fieldValuesFromPredicates(predicates, queryPredicate -> true);
    }

    /**
     * Return all {@link FieldValue}s contained in the predicates handed in.
     * @param predicates a collection of predicates
     * @param filteringPredicate an actual predicate performing additional filtering for the kinds of
     *        {@link PredicateWithValue}s the caller is interested in
     * @return a set of {@link FieldValue}s
     */
    @Nonnull
    static ImmutableSet<FieldValue> fieldValuesFromPredicates(@Nonnull final Collection<? extends QueryPredicate> predicates,
                                                              @Nonnull final Predicate<PredicateWithValue> filteringPredicate) {
        return predicates
                .stream()
                .flatMap(predicate -> {
                    final Iterable<? extends QueryPredicate> filters =
                            predicate.filter(p -> p instanceof PredicateWithValue && filteringPredicate.test((PredicateWithValue)p));
                    return StreamSupport.stream(filters.spliterator(), false)
                            .map(p -> (PredicateWithValue)p)
                            .flatMap(predicateWithValue ->
                                    StreamSupport.stream(predicateWithValue.getValue()
                                            .filter(v -> v instanceof FieldValue).spliterator(), false))
                            .map(value -> (FieldValue)value);
                })
                .map(fieldValue -> {
                    final Set<CorrelationIdentifier> fieldCorrelatedTo = fieldValue.getChild().getCorrelatedTo();
                    // TODO make better as the field can currently only handle exactly one correlated alias
                    final var alias = Iterables.getOnlyElement(fieldCorrelatedTo);
                    return (FieldValue)fieldValue.rebase(AliasMap.of(alias, CorrelationIdentifier.UNGROUNDED));
                })
                .collect(ImmutableSet.toImmutableSet());
    }
}
