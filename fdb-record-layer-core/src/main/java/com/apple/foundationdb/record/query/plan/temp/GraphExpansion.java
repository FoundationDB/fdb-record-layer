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

import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.temp.expressions.SelectExpression;
import com.apple.foundationdb.record.query.predicates.AndPredicate;
import com.apple.foundationdb.record.query.predicates.ExistsPredicate;
import com.apple.foundationdb.record.query.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.predicates.Value;
import com.apple.foundationdb.record.query.predicates.ValueComparisonRangePredicate;
import com.apple.foundationdb.record.query.predicates.ValueComparisonRangePredicate.Placeholder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
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
    private final ImmutableList<Value> resultValues;

    /**
     * A list of predicates that need to be applied when this expansion is built and sealed. The resulting filter
     * will use the logical conjunct of all predicates to filter the flowed records.
     */
    @Nonnull
    private final ImmutableList<QueryPredicate> predicates;

    /**
     * A list of quantifiers that the result of this expansion will range over.
     */
    @Nonnull
    private final ImmutableList<Quantifier> quantifiers;

    /**
     * A list of all placeholders added during the expansion of the associated {@link MatchCandidate}.
     */
    @Nonnull
    private final ImmutableList<Placeholder> placeholders;

    private GraphExpansion(@Nonnull final ImmutableList<Value> resultValues,
                           @Nonnull final ImmutableList<QueryPredicate> predicates,
                           @Nonnull final ImmutableList<Quantifier> quantifiers,
                           @Nonnull final ImmutableList<Placeholder> placeholders) {
        this.resultValues = resultValues;
        this.predicates = predicates;
        this.quantifiers = quantifiers;
        this.placeholders = placeholders;
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

    @Nonnull
    public GraphExpansion withBase(@Nonnull final Quantifier.ForEach quantifier) {
        return GraphExpansion.ofOthers(ofQuantifier(quantifier), this);
    }

    @Nonnull
    public Builder toBuilder() {
        final var builder = builder();
        builder.addAllResultValues(resultValues);
        builder.addAllPredicates(predicates);
        builder.addAllQuantifiers(quantifiers);
        builder.addAllPlaceholders(placeholders);
        return builder;
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
        final ImmutableList.Builder<Value> allResultValuesBuilder = ImmutableList.builder();
        allResultValuesBuilder.addAll(resultValues);
        final ImmutableList<Value> allResultValues = allResultValuesBuilder.build();

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

            final ImmutableList.Builder<QueryPredicate> resultPredicates = new ImmutableList.Builder<>();
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

            graphExpansion = new GraphExpansion(allResultValues, resultPredicates.build(), quantifiers, ImmutableList.copyOf(resultPlaceHolders));
        } else {
            graphExpansion = new GraphExpansion(allResultValues, predicates, quantifiers, ImmutableList.of());
        }
        return graphExpansion.new Sealed();
    }

    @Nonnull
    public SelectExpression buildSelect() {
        return seal().buildSelect();
    }

    @Nonnull
    public static GraphExpansion empty() {
        return builder().build();
    }

    @Nonnull
    public static GraphExpansion ofQuantifier(@Nonnull final Quantifier quantifier) {
        return builder().addQuantifier(quantifier).build();
    }

    @Nonnull
    public static GraphExpansion ofPredicate(@Nonnull final QueryPredicate predicate) {
        return builder().addPredicate(predicate).build();
    }

    @Nonnull
    public static GraphExpansion ofResultValue(@Nonnull final Value resultValue) {
        return builder().addResultValue(resultValue).build();
    }

    @Nonnull
    public static GraphExpansion ofResultValueAndPlaceholder(@Nonnull final Value resultValue,
                                                             @Nonnull final Placeholder placeholder) {
        return builder().addResultValue(resultValue).addPredicate(placeholder).addPlaceholder(placeholder).build();
    }

    @Nonnull
    public static GraphExpansion ofExists(@Nonnull final Quantifier.Existential existentialQuantifier,
                                          @Nonnull final QueryComponent alternativeComponent) {
        final var existsPredicate = new ExistsPredicate(existentialQuantifier.getAlias(), alternativeComponent);
        return of(ImmutableList.of(), ImmutableList.of(existsPredicate), ImmutableList.of(existentialQuantifier), ImmutableList.of());
    }

    @Nonnull
    public static GraphExpansion ofPlaceholderAndQuantifier(@Nonnull final Placeholder placeholder, @Nonnull final Quantifier quantifier) {
        return of(ImmutableList.of(), ImmutableList.of(placeholder), ImmutableList.of(quantifier), ImmutableList.of(placeholder));
    }

    @Nonnull
    public static GraphExpansion of(@Nonnull final ImmutableList<Value> resultValues,
                                    @Nonnull final ImmutableList<QueryPredicate> predicates,
                                    @Nonnull final ImmutableList<Quantifier> quantifiers,
                                    @Nonnull final ImmutableList<Placeholder> placeholders) {
        return new GraphExpansion(resultValues, predicates, quantifiers, placeholders);
    }

    @Nonnull
    public static GraphExpansion ofOthers(@Nonnull GraphExpansion graphExpansion, @Nonnull GraphExpansion... otherExpansions) {
        final ImmutableList.Builder<GraphExpansion> graphExpansionsBuilder = ImmutableList.builder();
        graphExpansionsBuilder.add(graphExpansion);
        graphExpansionsBuilder.addAll(Arrays.asList(otherExpansions));
        return ofOthers(graphExpansionsBuilder.build());
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

    @Nonnull
    public static Builder builder() {
        return new Builder();
    }

    /**
     * A sealed version of {@link GraphExpansion} that has already reconciled duplicate placeholders.
     */
    public class Sealed {
        @Nonnull
        public SelectExpression buildSelect() {
            return new SelectExpression(RecordConstructorValue.flattenRecords(resultValues), quantifiers, getPredicates());
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
        public Builder builderWithInheritedPlaceholders() {
            return builder().addAllPlaceholders(placeholders);
        }
    }

    public static class Builder {
        /**
         * A list of values representing the result of this expansion, if sealed and built.
         */
        @Nonnull
        private final ImmutableList.Builder<Value> resultValues;

        /**
         * A list of predicates that need to be applied when this expansion is built and sealed. The resulting filter
         * will use the logical conjunct of all predicates to filter the flowed records.
         */
        @Nonnull
        private final ImmutableList.Builder<QueryPredicate> predicates;

        /**
         * A list of quantifiers that the result of this expansion will range over.
         */
        @Nonnull
        private final ImmutableList.Builder<Quantifier> quantifiers;

        /**
         * A list of all placeholders added during the expansion of the associated {@link MatchCandidate}.
         */
        @Nonnull
        private final ImmutableList.Builder<Placeholder> placeholders;

        private Builder() {
            resultValues = new ImmutableList.Builder<>();
            predicates = new ImmutableList.Builder<>();
            quantifiers = new ImmutableList.Builder<>();
            placeholders = new ImmutableList.Builder<>();
        }

        @Nonnull
        public Builder addResultValue(@Nonnull final Value resultValue) {
            Objects.requireNonNull(resultValue);
            resultValues.add(resultValue);
            return this;
        }

        @Nonnull
        public Builder addAllResultValues(@Nonnull final List<? extends Value> resultValue) {
            resultValue.forEach(Objects::requireNonNull);
            resultValues.addAll(resultValue);
            return this;
        }

        @Nonnull
        public Builder addPredicate(@Nonnull final QueryPredicate predicate) {
            predicates.add(predicate);
            return this;
        }

        @Nonnull
        public Builder addAllPredicates(@Nonnull final List<? extends QueryPredicate> addPredicates) {
            predicates.addAll(addPredicates);
            return this;
        }

        @Nonnull
        public Builder addQuantifier(@Nonnull final Quantifier quantifier) {
            quantifiers.add(quantifier);
            return this;
        }

        @Nonnull
        public Builder addAllQuantifiers(@Nonnull final List<? extends Quantifier> addQuantifiers) {
            addQuantifiers.forEach(this::addQuantifier);
            return this;
        }

        @Nonnull
        public Builder pullUpQuantifier(@Nonnull final Quantifier quantifier) {
            quantifiers.add(quantifier);
            resultValues.addAll(quantifier.getFlowedValues());
            return this;
        }

        @Nonnull
        public Builder pullUpAllQuantifiers(@Nonnull final List<? extends Quantifier> addQuantifiers) {
            addQuantifiers.forEach(this::pullUpQuantifier);
            return this;
        }

        @Nonnull
        public Builder addPlaceholder(@Nonnull final Placeholder placeholder) {
            placeholders.add(placeholder);
            return this;
        }

        @Nonnull
        public Builder addAllPlaceholders(@Nonnull final List<? extends Placeholder> addPlaceholders) {
            placeholders.addAll(addPlaceholders);
            return this;
        }

        @Nonnull
        public GraphExpansion build() {
            return new GraphExpansion(resultValues.build(), predicates.build(), quantifiers.build(), placeholders.build());
        }
    }
}
