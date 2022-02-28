/*
 * GraphExpansion.java
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
import com.apple.foundationdb.record.query.predicates.QuantifiedColumnValue;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.predicates.Value;
import com.apple.foundationdb.record.query.predicates.ValueComparisonRangePredicate;
import com.apple.foundationdb.record.query.predicates.ValueComparisonRangePredicate.Placeholder;
import com.google.common.base.Verify;
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
 * Class to abstract behavior when query expansion is applied to query components or key expressions. An object of this
 * class represents a conglomeration for result values, predicates, quantifiers before those elements are unified and
 * eventually find their home in a {@link SelectExpression}.
 *
 * Another way of thinking about this class is to conceptualize logic implemented here as a pre stage to actually
 * creating expressions, as a builder of a {@link SelectExpression}s of sorts. The fundamental difference between
 * a builder in the regular sense and this class is that it also provides getters to all elements and the ability to
 * merge other {@code GraphExpansion}s in a additive manner.
 *
 * This class also hides some data cleansing process, in particular related to potential duplicity on place holders,
 * which must be corrected (de-duplicated) before the place holders are used in the predicates of e.g. a match candidate.
 */
public class GraphExpansion implements KeyExpressionVisitor.Result {
    /**
     * A list of values representing the result of this expansion, if sealed and built.
     */
    @Nonnull
    private final List<Value> resultValues;

    /**
     * A list of predicates that need to be applied when this expansion is built and sealed. The resulting filter
     * will use the logical conjunct of all predicates to filter the flowed records.
     */
    @Nonnull
    private final List<QueryPredicate> predicates;

    /**
     * A list of quantifiers that the result of this expansion will range over.
     */
    @Nonnull
    private final List<Quantifier> quantifiers;

    /**
     * A list of all placeholders added during the expansion of the associated {@link MatchCandidate}.
     */
    @Nonnull
    private final List<Placeholder> placeholders;

    private GraphExpansion(@Nonnull final List<? extends Value> resultValues,
                           @Nonnull final List<? extends QueryPredicate> predicates,
                           @Nonnull final List<? extends Quantifier> quantifiers,
                           @Nonnull final List<? extends Placeholder> placeholders) {
        this.resultValues = ImmutableList.copyOf(resultValues);
        this.predicates = ImmutableList.copyOf(predicates);
        this.quantifiers = ImmutableList.copyOf(quantifiers);
        this.placeholders = ImmutableList.copyOf(placeholders);
    }

    @Nonnull
    public List<Value> getResultValues() {
        return resultValues;
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
    public List<CorrelationIdentifier> getPlaceholderAliases() {
        return placeholders
                .stream()
                .map(ValueComparisonRangePredicate.Placeholder::getAlias)
                .collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    public QueryPredicate asAndPredicate() {
        return AndPredicate.and(getPredicates());
    }

    @Nonnull
    public GraphExpansion withPredicate(@Nonnull final QueryPredicate predicate) {
        return new GraphExpansion(this.resultValues, ImmutableList.of(predicate), this.quantifiers, this.placeholders);
    }

    /**
     * Method to <em>seal</em> a graph expansion in an instance of {@link Sealed}. A sealed graph expansion is immutable
     * and can only be used to (repeatedly) build actual expressions.
     * A graph expansion object may contain duplicate information that have been added to it by callers. That is allowed
     * and supported. In fact, in most cases, duplicates among e.g. {@link QueryPredicate}s come from merging individual
     * {@link GraphExpansion}s into more complex ones. This method normalizes all elements beforehand in order to
     * eventually return a sealed version of itself (which is not allowed to contain duplicates).
     *
     * @return a sealed graph expansion
     */
    @Nonnull
    public Sealed seal() {
        final GraphExpansion graphExpansion;
        if (!placeholders.isEmpty()) {
            // There may be placeholders in the current (local) expansion step that are equivalent to each other, but we
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

            graphExpansion = new GraphExpansion(resultValues, resultPredicates, getQuantifiers(), resultPlaceHolders);
        } else {
            graphExpansion = new GraphExpansion(resultValues, getPredicates(), getQuantifiers(), ImmutableList.of());
        }
        return graphExpansion.new Sealed();
    }

    @Nonnull
    public SelectExpression buildSelect() {
        return seal().buildSelect();
    }

    @Nonnull
    public SelectExpression buildSelectWithBase(final Quantifier baseQuantifier) {
        return seal().buildSelectWithBase(baseQuantifier);
    }

    @Nonnull
    public static GraphExpansion empty() {
        return of(ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), ImmutableList.of());
    }

    @Nonnull
    public static GraphExpansion ofQuantifier(@Nonnull final Quantifier quantifier) {
        return of(ImmutableList.of(), ImmutableList.of(), ImmutableList.of(quantifier), ImmutableList.of());
    }

    @Nonnull
    public static GraphExpansion ofPredicate(@Nonnull final QueryPredicate predicate) {
        return of(ImmutableList.of(), ImmutableList.of(predicate), ImmutableList.of(), ImmutableList.of());
    }

    @Nonnull
    public static GraphExpansion ofResultValue(@Nonnull final Value resultValue) {
        return of(ImmutableList.of(resultValue), ImmutableList.of(), ImmutableList.of(), ImmutableList.of());
    }

    @Nonnull
    public static GraphExpansion ofResultValueAndQuantifier(@Nonnull final Value resultValue, @Nonnull final Quantifier quantifier) {
        return of(ImmutableList.of(resultValue), ImmutableList.of(), ImmutableList.of(quantifier), ImmutableList.of());
    }

    @Nonnull
    public static GraphExpansion ofResultValueAndPlaceholder(@Nonnull final Value resultValue,
                                                             @Nonnull final Placeholder placeholder) {
        return of(ImmutableList.of(resultValue), ImmutableList.of(placeholder), ImmutableList.of(), ImmutableList.of(placeholder));
    }

    @Nonnull
    public static GraphExpansion ofPredicateAndQuantifier(@Nonnull final QueryPredicate predicate, @Nonnull final Quantifier quantifier) {
        return of(ImmutableList.of(), ImmutableList.of(predicate), ImmutableList.of(quantifier), ImmutableList.of());
    }

    @Nonnull
    public static GraphExpansion of(@Nonnull final List<? extends Value> resultValues,
                                    @Nonnull final List<? extends QueryPredicate> predicates,
                                    @Nonnull final List<? extends Quantifier> quantifiers,
                                    @Nonnull final List<? extends Placeholder> placeholders) {
        return new GraphExpansion(resultValues, predicates, quantifiers, placeholders);
    }
    
    @Nonnull
    public static GraphExpansion ofOthers(@Nonnull List<GraphExpansion> graphExpansions) {
        final ImmutableList.Builder<Value> resultValuesBuilder = ImmutableList.builder();
        final ImmutableList.Builder<QueryPredicate> predicatesBuilder = ImmutableList.builder();
        final ImmutableList.Builder<Quantifier> quantifiersBuilder = ImmutableList.builder();
        final ImmutableList.Builder<Placeholder> placeholdersBuilder = ImmutableList.builder();
        for (final GraphExpansion expandedPredicate : graphExpansions) {
            resultValuesBuilder.addAll(expandedPredicate.getResultValues());
            predicatesBuilder.addAll(expandedPredicate.getPredicates());
            quantifiersBuilder.addAll(expandedPredicate.getQuantifiers());
            placeholdersBuilder.addAll(expandedPredicate.getPlaceholders());
        }
        return new GraphExpansion(resultValuesBuilder.build(),
                predicatesBuilder.build(),
                quantifiersBuilder.build(),
                placeholdersBuilder.build());
    }

    /**
     * A sealed version of {@link GraphExpansion} that has already reconciled duplicate place holders.
     */
    public class Sealed {
        @Nonnull
        public SelectExpression buildSelect() {
            return buildSelectWithQuantifiers(getQuantifiers());
        }

        @Nonnull
        public SelectExpression buildSelectWithBase(final Quantifier baseQuantifier) {
            final ImmutableList<Quantifier> allQuantifiers =
                    ImmutableList.<Quantifier>builder()
                            .add(baseQuantifier)
                            .addAll(getQuantifiers()).build();

            return buildSelectWithQuantifiers(allQuantifiers);
        }

        @Nonnull
        private SelectExpression buildSelectWithQuantifiers(final List<Quantifier> quantifiers) {
            final ImmutableList<? extends QuantifiedColumnValue> pulledUpResultValues =
                    quantifiers
                            .stream()
                            .filter(quantifier -> quantifier instanceof Quantifier.ForEach)
                            .flatMap(quantifier -> quantifier.getFlowedValues().stream())
                            .collect(ImmutableList.toImmutableList());

            final ImmutableList<Value> allResultValues =
                    ImmutableList.<Value>builder()
                            .addAll(pulledUpResultValues)
                            .addAll(getResultValues())
                            .build();

            return new SelectExpression(allResultValues, quantifiers, getPredicates());
        }

        @Nonnull
        public List<Value> getResultValues() {
            return resultValues;
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
            return GraphExpansion.this.asAndPredicate();
        }

        @Nonnull
        public GraphExpansion derivedWithQuantifier(@Nonnull final Quantifier quantifier) {
            Verify.verify(quantifier instanceof Quantifier.ForEach);
            return new GraphExpansion(ImmutableList.of(),
                    ImmutableList.of(),
                    ImmutableList.of(quantifier),
                    placeholders);
        }
    }
}
