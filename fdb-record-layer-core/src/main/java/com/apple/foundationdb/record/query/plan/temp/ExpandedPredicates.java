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
import com.apple.foundationdb.record.query.predicates.ValueComparisonRangePredicate.Placeholder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Class to abstract behavior when query expansion is applied to query components or key expressions.
 */
public class ExpandedPredicates {
    @Nonnull
    private final List<QueryPredicate> predicates;

    @Nonnull
    private final List<Quantifier> quantifiers;

    @Nonnull
    private final List<Placeholder> placeholders;

    private ExpandedPredicates(@Nonnull final List<QueryPredicate> predicates,
                               @Nonnull final List<Quantifier> quantifiers,
                               @Nonnull final List<Placeholder> placeholders) {
        this.predicates = ImmutableList.copyOf(predicates);
        this.quantifiers = ImmutableList.copyOf(quantifiers);
        this.placeholders = ImmutableList.copyOf(placeholders);
    }

    @Nonnull
    public List<QueryPredicate> getPredicates() {
        return predicates;
    }

    @Nonnull
    public List<Quantifier> getQuantifiers() {
        return quantifiers;
    }

    @Nonnull
    public List<Placeholder> getPlaceholders() {
        return placeholders;
    }

    @Nonnull
    public QueryPredicate asAndPredicate() {
        return AndPredicate.and(getPredicates());
    }

    @Nonnull
    public ExpandedPredicates withPredicate(@Nonnull final QueryPredicate predicate) {
        return new ExpandedPredicates(ImmutableList.of(predicate), quantifiers, placeholders);
    }

    @Nonnull
    public Sealed seal() {
        final ExpandedPredicates expandedPredicates;
        if (!placeholders.isEmpty()) {
            // There may be placeholders in the current (local) expansion step that are equivalent to each other but we
            // don't know that yet.
            final ImmutableSet<QueryPredicate> localPredicates = ImmutableSet.copyOf(getPredicates());
            final List<Placeholder> resultPlaceHolders = Lists.newArrayList(placeholders);
            final List<Pair<Placeholder, Integer>> localPlaceHolderPairs =
                    IntStream.range(0, placeholders.size())
                            .mapToObj(i -> Pair.of(placeholders.get(i), i))
                            .filter(p -> localPredicates.contains(p.getKey()))
                            .collect(Collectors.toList());

            final List<QueryPredicate> resultPredicates = Lists.newArrayList();
            for (final QueryPredicate queryPredicate : getPredicates()) {
                if (queryPredicate instanceof Placeholder) {
                    final Placeholder localPlaceHolder = (Placeholder)queryPredicate;
                    final AliasMap identities = AliasMap.identitiesFor(localPlaceHolder.getCorrelatedTo());
                    final Iterator<Pair<Placeholder, Integer>> iterator = localPlaceHolderPairs.iterator();
                    int foundAtOrdinal = -1;
                    while (iterator.hasNext()) {
                        final Pair<Placeholder, Integer> currentPlaceholderPair = iterator.next();
                        final Placeholder currentPlaceHolder = currentPlaceholderPair.getKey();
                        if (localPlaceHolder.semanticEqualsWithoutParameterAlias(currentPlaceHolder, identities)) {
                            if (foundAtOrdinal < 0) {
                                foundAtOrdinal = currentPlaceholderPair.getRight();
                                resultPredicates.add(currentPlaceHolder);
                            } else {
                                resultPlaceHolders.set(currentPlaceholderPair.getRight(), resultPlaceHolders.get(foundAtOrdinal));
                            }
                            iterator.remove();
                        }
                    }
                } else {
                    resultPredicates.add(queryPredicate);
                }
            }

            expandedPredicates = new ExpandedPredicates(resultPredicates, getQuantifiers(), resultPlaceHolders);
        } else {
            expandedPredicates = new ExpandedPredicates(getPredicates(), getQuantifiers(), ImmutableList.of());
        }
        return expandedPredicates.new Sealed();
    }

    @Nonnull
    public SelectExpression buildSelectWithBase(final Quantifier baseQuantifier) {
        return seal().buildSelectWithBase(baseQuantifier);
    }

    @Nonnull
    public static ExpandedPredicates empty() {
        return new ExpandedPredicates(ImmutableList.of(), ImmutableList.of(), ImmutableList.of());
    }

    @Nonnull
    public static ExpandedPredicates ofPredicate(@Nonnull final QueryPredicate predicate) {
        return new ExpandedPredicates(ImmutableList.of(predicate), ImmutableList.of(), ImmutableList.of());
    }

    @Nonnull
    public static ExpandedPredicates ofPlaceholderPredicate(@Nonnull final Placeholder placeholder) {
        return new ExpandedPredicates(ImmutableList.of(placeholder), ImmutableList.of(), ImmutableList.of(placeholder));
    }

    @Nonnull
    public static ExpandedPredicates ofQuantifier(@Nonnull final Quantifier quantifier) {
        return new ExpandedPredicates(ImmutableList.of(), ImmutableList.of(quantifier), ImmutableList.of());
    }

    @Nonnull
    public static ExpandedPredicates ofPredicateAndQuantifier(@Nonnull final QueryPredicate predicate, @Nonnull final Quantifier quantifier) {
        return new ExpandedPredicates(ImmutableList.of(predicate), ImmutableList.of(quantifier), ImmutableList.of());
    }

    @Nonnull
    public static ExpandedPredicates ofPlaceholderAndQuantifier(@Nonnull final Placeholder placeholder, @Nonnull final Quantifier quantifier) {
        return new ExpandedPredicates(ImmutableList.of(placeholder), ImmutableList.of(quantifier), ImmutableList.of(placeholder));
    }

    @Nonnull
    public static ExpandedPredicates ofOthers(@Nonnull ExpandedPredicates... expandedPredicates) {
        return ofOthers(ImmutableList.copyOf(expandedPredicates));
    }

    @Nonnull
    public static ExpandedPredicates ofOthers(@Nonnull List<ExpandedPredicates> expandedPredicates) {
        final ImmutableList.Builder<QueryPredicate> predicatesBuilder = ImmutableList.builder();
        final ImmutableList.Builder<Quantifier> quantifiersBuilder = ImmutableList.builder();
        final ImmutableList.Builder<Placeholder> placeholdersBuilder = ImmutableList.builder();
        for (final ExpandedPredicates expandedPredicate : expandedPredicates) {
            predicatesBuilder.addAll(expandedPredicate.getPredicates());
            quantifiersBuilder.addAll(expandedPredicate.getQuantifiers());
            placeholdersBuilder.addAll(expandedPredicate.getPlaceholders());
        }
        return new ExpandedPredicates(predicatesBuilder.build(), quantifiersBuilder.build(), placeholdersBuilder.build());
    }

    /**
     * A sealed version of {@link ExpandedPredicates} that has already reconciled duplicate place holders.
     */
    public class Sealed {
        @Nonnull
        public SelectExpression buildSelectWithBase(final Quantifier baseQuantifier) {
            final ImmutableList<Quantifier> allQuantifiers =
                    ImmutableList.<Quantifier>builder()
                            .add(baseQuantifier)
                            .addAll(getQuantifiers()).build();

            return new SelectExpression(allQuantifiers, getPredicates());
        }

        @Nonnull
        public List<QueryPredicate> getPredicates() {
            return predicates;
        }

        @Nonnull
        public List<Quantifier> getQuantifiers() {
            return quantifiers;
        }

        @Nonnull
        public List<Placeholder> getPlaceholders() {
            return placeholders;
        }

        @Nonnull
        public QueryPredicate asAndPredicate() {
            return ExpandedPredicates.this.asAndPredicate();
        }

        @Nonnull
        public ExpandedPredicates derivedWithQuantifier(@Nonnull final Quantifier quantifier) {
            return new ExpandedPredicates(ImmutableList.of(), ImmutableList.of(quantifier), placeholders);
        }
    }
}
