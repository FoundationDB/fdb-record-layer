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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.Placeholder;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValueAndRanges;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ExistentialValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Function;
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
public class GraphExpansion {
    /**
     * A list of columns, i.e., fields and values representing the result of this expansion, if sealed and built.
     */
    private final ImmutableList<Column<? extends Value>> resultColumns;

    /**
     * A list of predicates that need to be applied when this expansion is built and sealed. The resulting filter
     * will use the logical conjunct of all predicates to filter the flowed records.
     */
    private final ImmutableList<QueryPredicate> predicates;

    /**
     * A list of quantifiers that the result of this expansion will range over.
     */
    private final ImmutableList<Quantifier> quantifiers;

    /**
     * A list of all placeholders added during the expansion of the associated {@link MatchCandidate}.
     */
    private final ImmutableList<Placeholder> placeholders;

    private GraphExpansion(final List<Column<? extends Value>> resultColumns,
                           final List<QueryPredicate> predicates,
                           final List<Quantifier> quantifiers,
                           final List<Placeholder> placeholders) {
        this.resultColumns = ImmutableList.copyOf(resultColumns);
        this.predicates = ImmutableList.copyOf(predicates);
        this.quantifiers = ImmutableList.copyOf(quantifiers);
        this.placeholders = ImmutableList.copyOf(placeholders);
    }

    public List<Column<? extends Value>> getResultColumns() {
        return resultColumns;
    }

    public List<QueryPredicate> getPredicates() {
        return predicates;
    }

    public List<Quantifier> getQuantifiers() {
        return quantifiers;
    }

    public List<Placeholder> getPlaceholders() {
        return placeholders;
    }

    public List<CorrelationIdentifier> getPlaceholderAliases() {
        return placeholders
                .stream()
                .map(Placeholder::getParameterAlias)
                .collect(ImmutableList.toImmutableList());
    }

    public QueryPredicate asAndPredicate() {
        return AndPredicate.and(getPredicates());
    }

    public GraphExpansion withPredicate(final QueryPredicate predicate) {
        return new GraphExpansion(this.resultColumns, ImmutableList.of(predicate), this.quantifiers, this.placeholders);
    }

    public GraphExpansion withBase(final Quantifier.ForEach quantifier) {
        return GraphExpansion.ofOthers(ofQuantifier(quantifier), this);
    }

    public boolean isTrivialSelect() {
        return quantifiers.size() == 1 && predicates.isEmpty() && resultColumns.isEmpty();
    }

    public Builder toBuilder() {
        final var builder = builder();
        builder.addAllResultColumns(resultColumns);
        builder.addAllPredicates(predicates);
        builder.addAllQuantifiers(quantifiers);
        builder.addAllPlaceholders(placeholders);
        return builder;
    }

    public Sealed distinctSeal() {
        return seal(true);
    }

    public Sealed seal() {
        return seal(false);
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
    public Sealed seal(final boolean isDistinctColumns) {
        final GraphExpansion graphExpansion;

        final var seenFieldNames = Sets.<String>newHashSet();
        final var duplicateFieldNamesBuilder = ImmutableSet.<String>builder();
        final var seenResultValues = isDistinctColumns ? Sets.<Value>newHashSet() : null;
        final var distinctResultColumnsBuilder =
                isDistinctColumns ? ImmutableList.<Column<? extends Value>>builder() : null;
        for (final Column<? extends Value> resultColumn : resultColumns) {
            final var fieldNameOptional = resultColumn.getField().getFieldNameOptional();
            final boolean isDuplicateValue;
            if (seenResultValues != null) {
                isDuplicateValue = !seenResultValues.add(resultColumn.getValue());
                if (!isDuplicateValue) {
                    distinctResultColumnsBuilder.add(resultColumn);
                }
            } else {
                isDuplicateValue = false;
            }
            if (fieldNameOptional.isPresent() && !isDuplicateValue) {
                final var fieldName = fieldNameOptional.get();
                if (!seenFieldNames.add(fieldName)) {
                    duplicateFieldNamesBuilder.add(fieldName);
                }
            }
        }

        final var duplicateFieldNames = duplicateFieldNamesBuilder.build();
        var normalizedResultColumns = distinctResultColumnsBuilder != null
                                      ? distinctResultColumnsBuilder.build() : resultColumns;

        normalizedResultColumns =
                normalizedResultColumns.stream()
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
            //
            // There may be placeholders appearing multiple times, potentially, with ranges. The list of placeholders
            // determines the parameters of the match candidate and in turn the keys of the index. There is a direct
            // correspondence between the position of the placeholder in the result placeholder and the position of the
            // sargable key part in the index.
            // There are two kinds of duplicates in the original list: First, there may be multiple placeholders using
            // the same parameter alias, second, there may be the placeholders using the same value but different
            // parameter aliases.
            // In a first step, we need to combine the ranges of all duplicate placeholders (by parameter alias) into
            // a list containing only the unique (and newly combined) placeholders. Then we need to modify the resulting
            // placeholders such that for each set of duplicate placeholders (by value) in the list of result
            // placeholders we replace each duplicate with the combined placeholder. That eliminates the duplicated
            // parameter aliases.
            //
            // Example:
            // placeholders:
            //     ( ..... , x -> p0, x-> p1, y -> p2, x -> p0 & < 30, y -> p3, ... )
            // resulting placeholders:
            //     ( ..... , x -> p0 & < 30, x -> p0 & < 30, y -> p2, y -> p2 ... )
            // Note that all three occurrences in the original placeholder list x -> ... are replaced by the combined
            // placeholder thus eliminating the references to p1, p2 and p3 in the resulting list.
            //
            final var localPredicates = ImmutableSet.copyOf(getPredicates());
            final var localValues =
                    localPredicates.stream()
                            .flatMap(predicate -> predicate.narrowMaybe(PredicateWithValueAndRanges.class).stream())
                            .map(PredicateWithValueAndRanges::getValue)
                            .collect(ImmutableList.toImmutableList());

            final var uniquePlaceholders =
                    ImmutableList.copyOf(placeholders.stream()
                            .collect(Collectors.toMap(Placeholder::getParameterAlias, v -> v,
                                    (left, right) -> left.withExtraRanges(right.getRanges()), LinkedHashMap::new)).values());

            final var resultPlaceHolders = Lists.newArrayList(uniquePlaceholders);
            final var localPlaceHolderPairs =
                    IntStream.range(0, uniquePlaceholders.size())
                            .mapToObj(i -> NonnullPair.of(uniquePlaceholders.get(i), i))
                            .filter(placeholderWithIndex -> localValues.contains(placeholderWithIndex.getKey().getValue()))
                            .collect(Collectors.toList());

            final ImmutableList.Builder<QueryPredicate> resultPredicates = new ImmutableList.Builder<>();
            for (final QueryPredicate queryPredicate : getPredicates()) {
                if (queryPredicate instanceof Placeholder) {
                    final var localPlaceHolder = (Placeholder)queryPredicate;
                    final var identities = AliasMap.emptyMap();
                    final var iterator = localPlaceHolderPairs.iterator();
                    int foundAtOrdinal = -1;
                    while (iterator.hasNext()) {
                        final var currentPlaceholderPair = iterator.next();
                        final var currentPlaceholder = currentPlaceholderPair.getLeft();
                        if (localPlaceHolder.getValue().semanticEquals(currentPlaceholder.getValue(), identities)) {
                            if (foundAtOrdinal < 0) {
                                foundAtOrdinal = currentPlaceholderPair.getRight();
                                resultPredicates.add(currentPlaceholder);
                                resultPlaceHolders.set(foundAtOrdinal, currentPlaceholder);
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

    public SelectExpression buildSelect() {
        return seal().buildSelect();
    }

    public SelectExpression buildSimpleSelectOverQuantifier(final Quantifier.ForEach overQuantifier) {
        return seal().buildSimpleSelectOverQuantifier(overQuantifier);
    }

    public SelectExpression buildSelectWithResultValue(final Value resultValue) {
        return seal().buildSelectWithResultValue(resultValue);
    }

    public static GraphExpansion empty() {
        return builder().build();
    }

    public static GraphExpansion ofQuantifier(final Quantifier quantifier) {
        return builder().addQuantifier(quantifier).build();
    }

    public static GraphExpansion ofPredicate(final QueryPredicate predicate) {
        return builder().addPredicate(predicate).build();
    }

    public static GraphExpansion ofResultColumn(final Column<? extends Value> resultColumn) {
        return builder().addResultColumn(resultColumn).build();
    }

    public static GraphExpansion ofResultColumnAndPlaceholder(final Column<? extends Value> resultColumn,
                                                              final Placeholder placeholder) {
        return builder().addResultColumn(resultColumn).addPredicate(placeholder).addPlaceholder(placeholder).build();
    }

    public static GraphExpansion ofExists(final Quantifier.Existential existentialQuantifier) {
        final var existsPredicate = new ExistentialValuePredicate(QuantifiedObjectValue.of(existentialQuantifier), new Comparisons.NullComparison(Comparisons.Type.NOT_NULL));
        return of(ImmutableList.of(), ImmutableList.of(existsPredicate), ImmutableList.of(existentialQuantifier), ImmutableList.of());
    }

    public static GraphExpansion ofPlaceholder(final Placeholder placeholder) {
        return builder().addPredicate(placeholder).addPlaceholder(placeholder).build();
    }

    public static GraphExpansion of(final List<Column<? extends Value>> resultColumns,
                                    final List<QueryPredicate> predicates,
                                    final List<Quantifier> quantifiers,
                                    final List<Placeholder> placeholders) {
        return new GraphExpansion(resultColumns, predicates, quantifiers, placeholders);
    }

    public static GraphExpansion ofOthers(GraphExpansion graphExpansion, GraphExpansion... otherExpansions) {
        final ImmutableList.Builder<GraphExpansion> graphExpansionsBuilder = ImmutableList.builder();
        graphExpansionsBuilder.add(graphExpansion);
        graphExpansionsBuilder.addAll(Arrays.asList(otherExpansions));
        return ofOthers(graphExpansionsBuilder.build());
    }

    public static GraphExpansion ofOthers(List<GraphExpansion> graphExpansions) {
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

    public static Builder builder() {
        return new Builder();
    }

    /**
     * A sealed version of {@link GraphExpansion} that has already reconciled duplicate placeholders.
     */
    public class Sealed {
        public SelectExpression buildSelect() {
            return new SelectExpression(RecordConstructorValue.ofColumns(resultColumns), quantifiers, getPredicates());
        }

        public SelectExpression buildSimpleSelectOverQuantifier(final Quantifier.ForEach overQuantifier) {
            return buildSelectWithResultValue(overQuantifier.getFlowedObjectValue());
        }

        public SelectExpression buildSelectWithResultValue(final Value resultValue) {
            Verify.verify(resultColumns.isEmpty());
            return new SelectExpression(resultValue, quantifiers, getPredicates());
        }

        public List<Column<? extends Value>> getResultColumns() {
            return resultColumns;
        }

        public List<Value> getResultValues() {
            return resultColumns.stream().map(Column::getValue).collect(ImmutableList.toImmutableList());
        }

        public List<QueryPredicate> getPredicates() {
            return predicates;
        }

        public List<Quantifier> getQuantifiers() {
            return quantifiers;
        }

        public List<Placeholder> getPlaceholders() {
            return placeholders;
        }

        public QueryPredicate asAndPredicate() {
            return GraphExpansion.this.asAndPredicate();
        }

        public Builder builderWithInheritedPlaceholders() {
            return builder().addAllPlaceholders(placeholders);
        }
    }

    /**
     * Builder for {@code GraphExpansion}.
     */
    public static class Builder {
        /**
         * A list of columns representing the result of this expansion, if sealed and built.
         */
        private ImmutableList.Builder<Column<? extends Value>> resultColumns;

        /**
         * A list of predicates that need to be applied when this expansion is built and sealed. The resulting filter
         * will use the logical conjunct of all predicates to filter the flowed records.
         */
        private ImmutableList.Builder<QueryPredicate> predicates;

        /**
         * A list of quantifiers that the result of this expansion will range over.
         */
        private ImmutableList.Builder<Quantifier> quantifiers;

        /**
         * A list of all placeholders added during the expansion of the associated {@link MatchCandidate}.
         */
        private ImmutableList.Builder<Placeholder> placeholders;

        private Builder() {
            resultColumns = new ImmutableList.Builder<>();
            predicates = new ImmutableList.Builder<>();
            quantifiers = new ImmutableList.Builder<>();
            placeholders = new ImmutableList.Builder<>();
        }

        public Builder addResultValue(final Value resultValue) {
            addResultColumn(Column.unnamedOf(resultValue));
            return this;
        }

        public Builder addAllResultValues(final Iterable<? extends Value> addResultValues) {
            addResultValues.forEach(this::addResultValue);
            return this;
        }

        public Builder addResultColumn(final Column<? extends Value> resultColumn) {
            resultColumns.add(resultColumn);
            return this;
        }

        public Builder addAllResultColumns(final Iterable<? extends Column<? extends Value>> addResultColumns) {
            addResultColumns.forEach(this::addResultColumn);
            return this;
        }

        public Builder removeAllResultColumns() {
            resultColumns = new ImmutableList.Builder<>();
            return this;
        }

        public Builder addPredicate(final QueryPredicate predicate) {
            predicates.add(predicate);
            return this;
        }

        public Builder addAllPredicates(final Iterable<? extends QueryPredicate> addPredicates) {
            predicates.addAll(addPredicates);
            return this;
        }

        public Builder removeAllPredicates() {
            predicates = new ImmutableList.Builder<>();
            return this;
        }

        public Builder replacePlaceholder(final Function<Placeholder, Placeholder> replacementFunction) {
            final ImmutableList<Placeholder> currentPlaceholders = placeholders.build();
            placeholders = new ImmutableList.Builder<>();
            for (final Placeholder placeholder : currentPlaceholders) {
                placeholders.add(replacementFunction.apply(placeholder));
            }
            return this;
        }

        public Builder addQuantifier(final Quantifier quantifier) {
            quantifiers.add(quantifier);
            return this;
        }

        public Builder addAllQuantifiers(final Iterable<? extends Quantifier> addQuantifiers) {
            addQuantifiers.forEach(this::addQuantifier);
            return this;
        }

        public Builder removeAllQuantifiers() {
            quantifiers = new ImmutableList.Builder<>();
            return this;
        }

        public Builder pullUpQuantifier(final Quantifier quantifier) {
            quantifiers.add(quantifier);
            resultColumns.addAll(quantifier.getFlowedColumns());
            return this;
        }

        public Builder pullUpAllQuantifiers(final Iterable<? extends Quantifier> addQuantifiers) {
            addQuantifiers.forEach(this::pullUpQuantifier);
            return this;
        }

        public Builder addPlaceholder(final Placeholder placeholder) {
            placeholders.add(placeholder);
            return this;
        }

        public Builder addAllPlaceholders(final Iterable<? extends Placeholder> addPlaceholders) {
            placeholders.addAll(addPlaceholders);
            return this;
        }

        public Builder removeAllPlaceholders() {
            placeholders = new ImmutableList.Builder<>();
            return this;
        }

        public GraphExpansion build() {
            return new GraphExpansion(resultColumns.build(), predicates.build(), quantifiers.build(), placeholders.build());
        }
    }
}
