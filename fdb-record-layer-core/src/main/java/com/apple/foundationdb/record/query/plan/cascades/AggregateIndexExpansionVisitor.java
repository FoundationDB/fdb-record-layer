/*
 * AggregateIndexExpansionVisitor.java
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

package com.apple.foundationdb.record.query.plan.cascades;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.MatchableSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.Placeholder;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValueAndRanges;
import com.apple.foundationdb.record.query.plan.cascades.values.CountValue;
import com.apple.foundationdb.record.query.plan.cascades.values.EmptyValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.IndexOnlyAggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NumericAggregationValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.Values;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Expands an aggregate index into a {@link MatchCandidate}. The generation will expand a {@link KeyExpression} into a
 * group by triplet QGM comprising a select-where, group-by-expression, and select-having, the triplet is followed
 * by an optional {@link MatchableSortExpression} that defines the sort order of the match candidate stream of records.
 */
public class AggregateIndexExpansionVisitor extends KeyExpressionExpansionVisitor
                                            implements ExpansionVisitor<KeyExpressionExpansionVisitor.VisitorState> {
    @Nonnull
    private static final Supplier<Map<String, BuiltInFunction<? extends Value>>> aggregateMap = Suppliers.memoize(AggregateIndexExpansionVisitor::computeAggregateMap);

    @Nonnull
    private final Index index;

    @Nonnull
    private final Collection<RecordType> recordTypes;

    @Nonnull
    private final GroupingKeyExpression groupingKeyExpression;

    private final int columnPermutations;

    /**
     * Constructs a new instance of {@link AggregateIndexExpansionVisitor}.
     *
     * @param index The target index.
     * @param recordTypes The indexed record types.
     */
    public AggregateIndexExpansionVisitor(@Nonnull final Index index, @Nonnull final Collection<RecordType> recordTypes) {
        Preconditions.checkArgument(aggregateMap.get().containsKey(index.getType()));
        Preconditions.checkArgument(index.getRootExpression() instanceof GroupingKeyExpression);
        this.index = index;
        this.groupingKeyExpression = ((GroupingKeyExpression)index.getRootExpression());
        this.recordTypes = recordTypes;
        @Nullable String permutationOption = index.getOption(IndexOptions.PERMUTED_SIZE_OPTION);
        this.columnPermutations = permutationOption == null ? 0 : Integer.parseInt(permutationOption);
    }

    public boolean isPermuted() {
        return IndexTypes.PERMUTED_MAX.equals(index.getType()) || IndexTypes.PERMUTED_MIN.equals(index.getType());
    }

    /**
     * Creates a new match candidate representing the aggregate index.
     *
     * @param baseQuantifierSupplier a quantifier supplier to create base data access
     * @param ignored the primary key of the data object the caller wants to access, this parameter is ignored since
     *        an aggregate index does not possess primary key information, must be {@code null}.
     * @param isReverse an indicator whether the result set is expected to be returned in reverse order.
     * @return A match candidate representing the aggregate index.
     */
    @Nonnull
    @Override
    public MatchCandidate expand(@Nonnull final Supplier<Quantifier.ForEach> baseQuantifierSupplier,
                                 @Nullable final KeyExpression ignored,
                                 final boolean isReverse) {
        Verify.verify(ignored == null);
        final var baseQuantifier = baseQuantifierSupplier.get();

        // 0. create a base expansion to resolve the key expression to columns with appropriate quantifiers
        final var baseExpansion = constructBaseExpansion(baseQuantifier);

        // 1. create a SELECT-WHERE expression.
        final var selectWhereQunAndPlaceholders = constructSelectWhereAndPlaceholders(baseQuantifier, baseExpansion);

        // 2. create a GROUP-BY expression on top.
        final var groupByQun = constructGroupBy(selectWhereQunAndPlaceholders.getLeft(), baseExpansion);

        // 3. construct SELECT-HAVING with SORT on top.
        final var selectHavingAndPlaceholderAliases = constructSelectHaving(groupByQun, selectWhereQunAndPlaceholders.getRight());
        final var selectHaving = selectHavingAndPlaceholderAliases.getLeft();
        final var placeHolderAliases = selectHavingAndPlaceholderAliases.getRight();

        // 4. add sort on top, if necessary, this will be absorbed later on as an ordering property of the match candidate.
        final var maybeWithSort = placeHolderAliases.isEmpty()
                ? Reference.of(selectHaving) // single group, sort by constant
                : Reference.of(new MatchableSortExpression(placeHolderAliases, isReverse, selectHaving));

        final var traversal = Traversal.withRoot(maybeWithSort);
        return new AggregateIndexMatchCandidate(index,
                traversal,
                placeHolderAliases,
                recordTypes,
                baseQuantifier.getFlowedObjectType(),
                groupByQun.getRangesOver().get().getResultValue(),
                selectHaving.getResultValue());
    }

    @Nonnull
    private GraphExpansion constructBaseExpansion(@Nonnull final Quantifier.ForEach baseQuantifier) {
        final var state = VisitorState.of(Lists.newArrayList(), Lists.newArrayList(), baseQuantifier, ImmutableList.of(), groupingKeyExpression.getGroupingCount(), 0);
        return pop(groupingKeyExpression.getWholeKey().expand(push(state)));
    }

    @Nonnull
    private NonnullPair<Quantifier, List<Placeholder>> constructSelectWhereAndPlaceholders(@Nonnull final Quantifier.ForEach baseQuantifier, @Nonnull final GraphExpansion baseExpansion) {
        final var allExpansionsBuilder = ImmutableList.<GraphExpansion>builder();
        allExpansionsBuilder.add(GraphExpansion.ofQuantifier(baseQuantifier));

        // add the SELECT-WHERE part, where we expose grouping and grouped columns, allowing query fragments that governs
        // only these columns to properly bind to this part, similar to how value indices work.
        final ImmutableList.Builder<CorrelationIdentifier> placeholders = ImmutableList.builder();
        placeholders.addAll(baseExpansion.getPlaceholderAliases());

        if (index.hasPredicate()) {
            final var filteredIndexPredicate = Objects.requireNonNull(index.getPredicate()).toPredicate(baseQuantifier.getFlowedObjectValue());
            final var valueRangesMaybe = IndexPredicateExpansion.dnfPredicateToRanges(filteredIndexPredicate);
            final var predicateExpansionBuilder = GraphExpansion.builder();
            if (valueRangesMaybe.isEmpty()) { // could not create DNF, store the predicate as-is.
                allExpansionsBuilder.add(GraphExpansion.ofPredicate(filteredIndexPredicate));
            } else {
                final var valueRanges = valueRangesMaybe.get();
                for (final var value : valueRanges.keySet()) {
                    // we check if the predicate value is a placeholder, if so, create a placeholder, otherwise, add it as a constraint.
                    final var maybePlaceholder = baseExpansion.getPlaceholders()
                            .stream()
                            .filter(existingPlaceholder -> existingPlaceholder.getValue().semanticEquals(value, AliasMap.emptyMap()))
                            .findFirst();
                    if (maybePlaceholder.isEmpty()) {
                        predicateExpansionBuilder.addPredicate(PredicateWithValueAndRanges.ofRanges(value, ImmutableSet.copyOf(valueRanges.get(value))));
                    } else {
                        predicateExpansionBuilder.addPlaceholder(maybePlaceholder.get().withExtraRanges(ImmutableSet.copyOf(valueRanges.get(value))));
                    }
                }
            }
            allExpansionsBuilder.add(predicateExpansionBuilder.build());
        }

        // flow all underlying quantifiers in their own QOV columns.
        final var builder = GraphExpansion.builder();
        // we need to refer to the following column later on in GroupByExpression, but since its ordinal position is fixed, we can simply refer
        // to it using an ordinal FieldAccessor (we do the same in plan generation).
        Stream.concat(Stream.of(baseQuantifier), baseExpansion.getQuantifiers().stream())
                .forEach(qun -> {
                    final var quantifiedValue = QuantifiedObjectValue.of(qun.getAlias(), qun.getFlowedObjectType());
                    builder.addResultColumn(Column.unnamedOf(quantifiedValue));
                });
        builder.addAllPlaceholders(baseExpansion.getPlaceholders());
        builder.addAllPredicates(baseExpansion.getPredicates());
        builder.addAllQuantifiers(baseExpansion.getQuantifiers());
        allExpansionsBuilder.add(builder.build());

        return NonnullPair.of(Quantifier.forEach(Reference.of(GraphExpansion.ofOthers(allExpansionsBuilder.build()).buildSelect())), baseExpansion.getPlaceholders());
    }

    @Nonnull
    private Quantifier constructGroupBy(@Nonnull final Quantifier selectWhereQun, @Nonnull final GraphExpansion baseExpansion) {
        if (groupingKeyExpression.getGroupedCount() > 1) {
            throw new UnsupportedOperationException("aggregate index is expected to contain exactly one aggregation, however it contains " + groupingKeyExpression.getGroupedCount() + " aggregations");
        }
        final Value groupedValue = groupingKeyExpression.getGroupedCount() == 0
                                   ? EmptyValue.empty()
                                   : baseExpansion.getResultColumns().get(groupingKeyExpression.getGroupingCount()).getValue();

        // construct aggregation RCV
        final Value argument;
        if (groupedValue instanceof EmptyValue) {
            argument = RecordConstructorValue.ofColumns(ImmutableList.of());
        } else if (groupedValue instanceof FieldValue) {
            final var aliasMap = AliasMap.identitiesFor(Sets.union(selectWhereQun.getCorrelatedTo(),
                    groupedValue.getCorrelatedTo()));
            final var result = selectWhereQun.getRangesOver().get().getResultValue()
                    .pullUp(List.of(groupedValue), aliasMap, ImmutableSet.of(), selectWhereQun.getAlias());
            if (!result.containsKey(groupedValue)) {
                throw new RecordCoreException("could not pull grouped value " + groupedValue)
                        .addLogInfo(LogMessageKeys.VALUE, groupedValue);
            }
            argument = result.get(groupedValue);
        } else {
            throw new RecordCoreException("unable to plan group by with non-field value")
                    .addLogInfo(LogMessageKeys.VALUE, groupedValue);
        }
        final var aggregateValue = (Value)aggregateMap.get().get(index.getType()).encapsulate(ImmutableList.of(argument));

        // add an RCV column representing the grouping columns as the first result set column
        // also, make sure to set the field type names correctly for each field value in the grouping keys RCV.

        final var groupingValues = baseExpansion.getResultColumns().subList(0, groupingKeyExpression.getGroupingCount())
                .stream()
                .map(Column::getValue)
                .collect(ImmutableList.toImmutableList());
        final var selectQunValue = selectWhereQun.getRangesOver().get().getResultValue();
        final var aliasMap = AliasMap.identitiesFor(Sets.union(selectQunValue.getCorrelatedTo(), groupingValues.stream().flatMap(v -> v.getCorrelatedTo().stream()).collect(ImmutableSet.toImmutableSet())));
        final var pulledUpGroupingValuesMap = selectQunValue.pullUp(groupingValues, aliasMap, ImmutableSet.of(), selectWhereQun.getAlias());
        final var pulledUpGroupingValues = groupingValues.stream().map(groupingValue -> {
            if (!pulledUpGroupingValuesMap.containsKey(groupingValue)) {
                throw new RecordCoreException("could not pull grouping value " + groupingValue)
                        .addLogInfo(LogMessageKeys.VALUE, groupingValue);
            }
            return pulledUpGroupingValuesMap.get(groupingValue);
        }).collect(ImmutableList.toImmutableList());

        // construct grouping column(s) value, the grouping column is _always_ fixed at position-0 in the underlying select-where.
        final var groupingColsValue = RecordConstructorValue.ofUnnamed(pulledUpGroupingValues);
        if (groupingColsValue.getResultType().getFields().isEmpty()) {
            return Quantifier.forEach(Reference.of(
                    new GroupByExpression(null, RecordConstructorValue.ofUnnamed(ImmutableList.of(aggregateValue)),
                            GroupByExpression::nestedResults, selectWhereQun)));
        } else {
            return Quantifier.forEach(Reference.of(
                    new GroupByExpression(groupingColsValue, RecordConstructorValue.ofUnnamed(ImmutableList.of(aggregateValue)),
                            GroupByExpression::nestedResults, selectWhereQun)));
        }
    }

    @Nonnull
    private NonnullPair<SelectExpression, List<CorrelationIdentifier>> constructSelectHaving(@Nonnull final Quantifier groupByQun,
                                                                                             @Nonnull final List<Placeholder> selectWherePlaceholders) {
        // the grouping value in GroupByExpression comes first (if set).
        @Nullable final var groupingValueReference =
                (groupByQun.getRangesOver().get() instanceof GroupByExpression && ((GroupByExpression)groupByQun.getRangesOver().get()).getGroupingValue() == null)
                ? null
                : FieldValue.ofOrdinalNumber(groupByQun.getFlowedObjectValue(), 0);

        final var aggregateValueReference = FieldValue.ofOrdinalNumberAndFuseIfPossible(FieldValue.ofOrdinalNumber(groupByQun.getFlowedObjectValue(), groupingValueReference == null ? 0 : 1), 0);

        final var placeholderAliases = ImmutableList.<CorrelationIdentifier>builder();
        final var selectHavingGraphExpansionBuilder = GraphExpansion.builder().addQuantifier(groupByQun);
        final List<Value> groupingValues = groupingValueReference == null ? Collections.emptyList() : Values.deconstructRecord(groupingValueReference);
        if (groupingValueReference != null) {
            int i = 0;
            for (final var groupingValue : groupingValues) {
                final var field = (FieldValue)groupingValue;
                final var placeholder = groupingValue.asPlaceholder(selectWherePlaceholders.get(i++).getParameterAlias());
                placeholderAliases.add(placeholder.getParameterAlias());
                selectHavingGraphExpansionBuilder
                        .addResultColumn(Column.unnamedOf(field))
                        .addPlaceholder(placeholder)
                        .addPredicate(placeholder);
            }
        }
        selectHavingGraphExpansionBuilder.addResultColumn(Column.unnamedOf(aggregateValueReference)); // TODO should we also add the aggregate reference as a placeholder?
        final List<CorrelationIdentifier> finalPlaceholders;
        if (isPermuted()) {
            Placeholder placeholder = Placeholder.newInstance(aggregateValueReference, newParameterAlias());
            placeholderAliases.add(placeholder.getParameterAlias());
            selectHavingGraphExpansionBuilder.addPlaceholder(placeholder).addPredicate(placeholder);
            if (columnPermutations > 0) {
                List<CorrelationIdentifier> unpermutedAliases = placeholderAliases.build();
                finalPlaceholders = ImmutableList.<CorrelationIdentifier>builder()
                        .addAll(unpermutedAliases.subList(0, groupingValues.size() - columnPermutations))
                        .add(placeholder.getParameterAlias())
                        .addAll(unpermutedAliases.subList(groupingValues.size() - columnPermutations, groupingValues.size()))
                        .build();
            } else {
                finalPlaceholders = placeholderAliases.build();
            }
        } else {
            finalPlaceholders = placeholderAliases.build();
        }
        return NonnullPair.of(selectHavingGraphExpansionBuilder.build().buildSelect(), finalPlaceholders);
    }

    @Nonnull
    private static Map<String, BuiltInFunction<? extends Value>> computeAggregateMap() {
        final ImmutableMap.Builder<String, BuiltInFunction<? extends Value>> mapBuilder = ImmutableMap.builder();
        mapBuilder.put(IndexTypes.MAX_EVER_LONG, new IndexOnlyAggregateValue.MaxEverFn());
        mapBuilder.put(IndexTypes.MIN_EVER_LONG, new IndexOnlyAggregateValue.MinEverFn());
        mapBuilder.put(IndexTypes.MAX_EVER_TUPLE, new IndexOnlyAggregateValue.MaxEverFn());
        mapBuilder.put(IndexTypes.MIN_EVER_TUPLE, new IndexOnlyAggregateValue.MinEverFn());
        mapBuilder.put(IndexTypes.SUM, new NumericAggregationValue.SumFn());
        mapBuilder.put(IndexTypes.COUNT, new CountValue.CountFn());
        mapBuilder.put(IndexTypes.COUNT_NOT_NULL, new CountValue.CountFn());
        mapBuilder.put(IndexTypes.PERMUTED_MAX, new NumericAggregationValue.MaxFn());
        mapBuilder.put(IndexTypes.PERMUTED_MIN, new NumericAggregationValue.MinFn());
        return mapBuilder.build();
    }
}
