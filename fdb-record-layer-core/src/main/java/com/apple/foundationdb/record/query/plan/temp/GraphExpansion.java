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
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import java.util.Arrays;
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
 * which must be corrected (de-duplicated) before the placeholders are used in the predicates of e.g. a match candidate.
 */
public class GraphExpansion implements KeyExpressionVisitor.Result {
    /**
     * A list of columns, i.e., fields and values representing the result of this expansion, if sealed and built.
     */
    @Nonnull
    private final ImmutableList<Column<? extends Value>> resultColumns;

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

    private GraphExpansion(@Nonnull final List<Column<? extends Value>> resultColumns,
                           @Nonnull final List<QueryPredicate> predicates,
                           @Nonnull final List<Quantifier> quantifiers,
                           @Nonnull final List<Placeholder> placeholders) {
        this.resultColumns = ImmutableList.copyOf(resultColumns);
        this.predicates = ImmutableList.copyOf(predicates);
        this.quantifiers = ImmutableList.copyOf(quantifiers);
        this.placeholders = ImmutableList.copyOf(placeholders);
    }

    @Nonnull
    public List<Column<? extends Value>> getResultColumns() {
        return resultColumns;
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
        return new GraphExpansion(this.resultColumns, ImmutableList.of(predicate), this.quantifiers, this.placeholders);
    }

    @Nonnull
    public GraphExpansion withBase(@Nonnull final Quantifier.ForEach quantifier) {
        return GraphExpansion.ofOthers(ofQuantifier(quantifier), this);
    }

    @Nonnull
    public Builder toBuilder() {
        final var builder = builder();
        builder.addAllResultColumns(resultColumns);
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
        final GraphExpansion graphExpansion;

        final var seenFieldNames = Sets.<String>newHashSet();
        final var duplicateFieldNamesBuilder = ImmutableSet.<String>builder();
        for (final Column<? extends Value> resultColumn : resultColumns) {
            final var fieldNameOptional = resultColumn.getField().getFieldNameOptional();
            fieldNameOptional.ifPresent(fieldName -> {
                if (!seenFieldNames.add(fieldName)) {
                    duplicateFieldNamesBuilder.add(fieldName);
                }
            });
        }

        final var duplicateFieldNames = duplicateFieldNamesBuilder.build();

        final var normalizedResultColumns =
                resultColumns.stream()
                        .map(resultColumn -> {
                            final var fieldNameOptional = resultColumn.getField().getFieldNameOptional();
                            // no name, all good
                            if (fieldNameOptional.isEmpty()) {
                                return resultColumn;
                            }
                            final var fieldName = fieldNameOptional.get();
                            if (!duplicateFieldNames.contains(fieldName)) {
                                return resultColumn;
                            }

                            // create an anonymous column
                            return Column.unnamedOf(resultColumn.getValue());
                        })
                        .collect(ImmutableList.toImmutableList());

        if (!placeholders.isEmpty()) {
            // There may be placeholders in the current (local) expansion step that are equivalent to each other, but we
            // don't know that yet.
            final var localPredicates = ImmutableSet.copyOf(getPredicates());
            final var resultPlaceHolders = Lists.newArrayList(placeholders);
            final var localPlaceHolderPairs =
                    IntStream.range(0, placeholders.size())
                            .mapToObj(i -> Pair.of(placeholders.get(i), i))
                            .filter(p -> localPredicates.contains(p.getKey()))
                            .collect(Collectors.toList());

            final ImmutableList.Builder<QueryPredicate> resultPredicates = new ImmutableList.Builder<>();
            for (final QueryPredicate queryPredicate : getPredicates()) {
                if (queryPredicate instanceof Placeholder) {
                    final var localPlaceHolder = (Placeholder)queryPredicate;
                    final var identities = AliasMap.identitiesFor(localPlaceHolder.getCorrelatedTo());
                    final var iterator = localPlaceHolderPairs.iterator();
                    int foundAtOrdinal = -1;
                    while (iterator.hasNext()) {
                        final var currentPlaceholderPair = iterator.next();
                        final var currentPlaceHolder = currentPlaceholderPair.getKey();
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

            graphExpansion = new GraphExpansion(normalizedResultColumns, resultPredicates.build(), quantifiers, ImmutableList.copyOf(resultPlaceHolders));
        } else {
            graphExpansion = new GraphExpansion(normalizedResultColumns, predicates, quantifiers, ImmutableList.of());
        }
        return graphExpansion.new Sealed();
    }

    @Nonnull
    public SelectExpression buildSelect() {
        return seal().buildSelect();
    }

    @Nonnull
    public SelectExpression buildSimpleSelectOverQuantifier(@Nonnull final Quantifier.ForEach overQuantifier) {
        return seal().buildSimpleSelectOverQuantifier(overQuantifier);
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
    public static GraphExpansion ofResultColumn(@Nonnull final Column<? extends Value> resultColumn) {
        return builder().addResultColumn(resultColumn).build();
    }

    @Nonnull
    public static GraphExpansion ofResultColumnAndPlaceholder(@Nonnull final Column<? extends Value> resultColumn,
                                                              @Nonnull final Placeholder placeholder) {
        return builder().addResultColumn(resultColumn).addPredicate(placeholder).addPlaceholder(placeholder).build();
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
    public static GraphExpansion of(@Nonnull final List<Column<? extends Value>> resultColumns,
                                    @Nonnull final List<QueryPredicate> predicates,
                                    @Nonnull final List<Quantifier> quantifiers,
                                    @Nonnull final List<Placeholder> placeholders) {
        return new GraphExpansion(resultColumns, predicates, quantifiers, placeholders);
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
        final var resultColumnsBuilder = ImmutableList.<Column<? extends Value>>builder();
        final var predicatesBuilder = ImmutableList.<QueryPredicate>builder();
        final var quantifiersBuilder = ImmutableList.<Quantifier>builder();
        final var placeholdersBuilder = ImmutableList.<Placeholder>builder();
        for (final GraphExpansion expandedPredicate : graphExpansions) {
            resultColumnsBuilder.addAll(expandedPredicate.getResultColumns());
            predicatesBuilder.addAll(expandedPredicate.getPredicates());
            quantifiersBuilder.addAll(expandedPredicate.getQuantifiers());
            placeholdersBuilder.addAll(expandedPredicate.getPlaceholders());
        }
        return new GraphExpansion(resultColumnsBuilder.build(),
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
            return new SelectExpression(RecordConstructorValue.ofColumns(resultColumns), quantifiers, getPredicates());
        }

        @Nonnull
        public SelectExpression buildSimpleSelectOverQuantifier(@Nonnull final Quantifier.ForEach overQuantifier) {
            final var forEachQuantifiers = quantifiers.stream()
                    .filter(quantifier -> quantifier instanceof Quantifier.ForEach)
                    .collect(ImmutableList.toImmutableList());
            Verify.verify(forEachQuantifiers.size() == 1);
            Verify.verify(Iterables.getOnlyElement(forEachQuantifiers) == overQuantifier);
            return new SelectExpression(overQuantifier.getFlowedObjectValue(), quantifiers, getPredicates());
        }

        @Nonnull
        public List<Column<? extends Value>> getResultColumns() {
            return resultColumns;
        }

        @Nonnull
        public List<Value> getResultValues() {
            return resultColumns.stream().map(Column::getValue).collect(ImmutableList.toImmutableList());
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
         * A list of columns representing the result of this expansion, if sealed and built.
         */
        @Nonnull
        private final ImmutableList.Builder<Column<? extends Value>> resultColumns;

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
            resultColumns = new ImmutableList.Builder<>();
            predicates = new ImmutableList.Builder<>();
            quantifiers = new ImmutableList.Builder<>();
            placeholders = new ImmutableList.Builder<>();
        }

        @Nonnull
        public Builder addResultValue(@Nonnull final Value resultValue) {
            addResultColumn(Column.unnamedOf(resultValue));
            return this;
        }

        @Nonnull
        public Builder addAllResultValues(@Nonnull final List<? extends Value> addResultValues) {
            addResultValues.forEach(this::addResultValue);
            return this;
        }

        @Nonnull
        public Builder addResultColumn(@Nonnull final Column<? extends Value> resultColumn) {
            resultColumns.add(resultColumn);
            return this;
        }

        @Nonnull
        public Builder addAllResultColumns(@Nonnull final List<Column<? extends Value>> addResultColumns) {
            addResultColumns.forEach(this::addResultColumn);
            return builder();
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
            resultColumns.addAll(quantifier.getFlowedColumns());
            return this;
        }

        @Nonnull
        public Builder pullUpAllQuantifiers(@Nonnull final List<? extends Quantifier> addQuantifiers) {
            addQuantifiers.forEach(this::pullUpQuantifier);
            return this;
        }

        @Nonnull
        public Builder pullUpAllExistingQuantifiers() {
            quantifiers.build().stream().filter(qun -> qun instanceof Quantifier.ForEach).forEach(qun -> resultColumns.addAll(qun.getFlowedColumns()));
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
            return new GraphExpansion(resultColumns.build(), predicates.build(), quantifiers.build(), placeholders.build());
        }
    }
}
