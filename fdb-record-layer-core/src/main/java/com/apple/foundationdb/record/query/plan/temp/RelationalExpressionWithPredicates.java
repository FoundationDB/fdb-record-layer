/*
 * RelationalExpressionWithPredicates.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.record.query.predicates.FieldValue;
import com.apple.foundationdb.record.query.predicates.PredicateWithValue;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.stream.StreamSupport;

/**
 * A (relational) expression that has a predicate on it.
 */
public interface RelationalExpressionWithPredicates extends RelationalExpression {
    @Nonnull
    List<QueryPredicate> getPredicates();

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
        return predicates
                .stream()
                .flatMap(predicate -> {
                    final Iterable<? extends QueryPredicate> filters =
                            predicate.filter(p -> p instanceof PredicateWithValue);
                    return StreamSupport.stream(filters.spliterator(), false)
                            .map(p -> (PredicateWithValue)p)
                            .flatMap(predicateWithValue -> StreamSupport.stream(predicateWithValue.getValue()
                                    .filter(v -> v instanceof FieldValue).spliterator(), false))
                            .map(value -> (FieldValue)value);
                })
                .map(fieldValue -> (FieldValue)fieldValue.rebase(AliasMap.of(fieldValue.getChild().getAlias(), CorrelationIdentifier.UNGROUNDED)))
                .collect(ImmutableSet.toImmutableSet());
    }
}
