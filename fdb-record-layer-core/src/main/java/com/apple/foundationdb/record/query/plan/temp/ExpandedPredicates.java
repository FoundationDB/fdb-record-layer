/*
 * ExpandedPredicates.java
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

import com.apple.foundationdb.record.query.plan.temp.expressions.SelectExpression;
import com.apple.foundationdb.record.query.predicates.AndPredicate;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;

/**
 * Class to abstract behavior when query expansion is applied to query components or key expressions.
 */
public class ExpandedPredicates {
    @Nonnull
    private final Collection<QueryPredicate> predicates;

    @Nonnull
    private final Collection<Quantifier> quantifier;

    private ExpandedPredicates(@Nonnull final Collection<QueryPredicate> predicate, @Nonnull final Collection<Quantifier> quantifier) {
        this.predicates = predicate;
        this.quantifier = quantifier;
    }

    @Nonnull
    public Collection<QueryPredicate> getPredicates() {
        return predicates;
    }

    @Nonnull
    public Collection<Quantifier> getQuantifiers() {
        return quantifier;
    }

    @Nonnull
    public QueryPredicate asAndPredicate() {
        return AndPredicate.and(getPredicates());
    }

    public static ExpandedPredicates empty() {
        return new ExpandedPredicates(ImmutableList.of(), ImmutableList.of());
    }

    public static ExpandedPredicates withPredicate(@Nonnull final QueryPredicate predicate) {
        return new ExpandedPredicates(ImmutableList.of(predicate), ImmutableList.of());
    }

    public static ExpandedPredicates withQuantifier(@Nonnull final Quantifier quantifier) {
        return new ExpandedPredicates(ImmutableList.of(), ImmutableList.of(quantifier));
    }

    public static ExpandedPredicates withPredicateAndQuantifier(@Nonnull final QueryPredicate predicate, @Nonnull final Quantifier quantifier) {
        return new ExpandedPredicates(ImmutableList.of(predicate), ImmutableList.of(quantifier));
    }

    public static ExpandedPredicates fromOthers(@Nonnull ExpandedPredicates... expandedPredicates) {
        return fromOthers(Arrays.asList(expandedPredicates));
    }

    public static ExpandedPredicates fromOthers(@Nonnull Collection<ExpandedPredicates> expandedPredicates) {
        final ImmutableList.Builder<QueryPredicate> predicatesBuilder = ImmutableList.builder();
        final ImmutableList.Builder<Quantifier> quantifiersBuilder = ImmutableList.builder();
        for (final ExpandedPredicates expandedPredicate : expandedPredicates) {
            predicatesBuilder.addAll(expandedPredicate.getPredicates());
            quantifiersBuilder.addAll(expandedPredicate.getQuantifiers());
        }
        return new ExpandedPredicates(predicatesBuilder.build(), quantifiersBuilder.build());
    }

    public static ExpandedPredicates fromOtherWithPredicate(@Nonnull final QueryPredicate predicate, @Nonnull final Collection<Quantifier> quantifiers) {
        return new ExpandedPredicates(ImmutableList.of(predicate), quantifiers);
    }

    @Nonnull
    public SelectExpression buildSelectWithBase(final Quantifier baseQuantifier) {
        return new SelectExpression(ImmutableList.<Quantifier>builder().add(baseQuantifier).addAll(getQuantifiers()).build(),
                ImmutableList.copyOf(getPredicates()));
    }
}
