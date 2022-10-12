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

import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.MatchableSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValueComparisonRangePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.CountValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NumericAggregationValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.Values;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Expands an aggregate index into a {@link MatchCandidate}. The generation will expand a {@link KeyExpression} into a
 * group by triplet QGM comprising a select-where, group-by-expression, and select-having, the triplet is followed
 * by a {@link MatchableSortExpression} that defines the sort order of the match candidate output data.
 */
public class AggregateIndexExpansionVisitor extends KeyExpressionExpansionVisitor
                                            implements ExpansionVisitor<KeyExpressionExpansionVisitor.VisitorState> {

    @Nonnull
    private final Index index;

    @Nonnull
    private final Collection<RecordType> recordTypes;

    @Nonnull
    private final GroupingKeyExpression groupingKeyExpression;

    /**
     * Constructs a new instance of {@link AggregateIndexExpansionVisitor}.
     * @param index The target index.
     * @param recordTypes The indexed record types.
     */
    public AggregateIndexExpansionVisitor(@Nonnull final Index index, @Nonnull final Collection<RecordType> recordTypes) {
        Preconditions.checkArgument(allowedIndexTypes.contains(index.getType()));
        Preconditions.checkArgument(index.getRootExpression() instanceof GroupingKeyExpression);
        this.index = index;
        this.groupingKeyExpression = ((GroupingKeyExpression)index.getRootExpression());
        this.recordTypes = recordTypes;
    }

    /**
     * Creates a new match candidate.
     *
     * @param baseQuantifierSupplier a quantifier supplier to create base data access
     * @param ignored the primary key of the data object the caller wants to access, this parameter is ignored.
     * @param isReverse an indicator whether the result set is expected to be returned in reverse order
     * @return A match candidate.
     */
    @Nonnull
    @Override
    public MatchCandidate expand(@Nonnull final java.util.function.Supplier<Quantifier.ForEach> baseQuantifierSupplier,
                                 @Nullable final KeyExpression ignored,
                                 final boolean isReverse) {

        final var baseQuantifier = baseQuantifierSupplier.get();
        final var groupByKeyExpr = ((GroupingKeyExpression)index.getRootExpression());
        final var groupingAndGroupedCols = getGroupingAndGroupedColumns(groupByKeyExpr, baseQuantifier, baseQuantifier.getFlowedObjectType());
        final var groupingColsFieldName = CorrelationIdentifier.uniqueID();

        // 1. create a SELECT-WHERE expression.
        final var selectWhereQun = constructSelectWhere(baseQuantifier, groupingColsFieldName, groupingAndGroupedCols);

        // 2. create a GROUP-BY expression on top.
        final var groupByQun = constructGroupBy(baseQuantifier, groupingColsFieldName, groupingAndGroupedCols, selectWhereQun);

        // 3. construct SELECT-HAVING with SORT on top.
        final var selectHavingAndPlaceholderAliases = constructSelectHavingWithSort(groupByQun);
        final var selectHaving = selectHavingAndPlaceholderAliases.getLeft();
        final var placeHolderAliases = selectHavingAndPlaceholderAliases.getRight();

        final var maybeWithSort = placeHolderAliases.isEmpty()
                ? GroupExpressionRef.of(selectHaving) // single group, sort by constant
                : GroupExpressionRef.of(new MatchableSortExpression(placeHolderAliases, isReverse, selectHaving));

        final var traversal = ExpressionRefTraversal.withRoot(maybeWithSort);
        return new AggregateIndexMatchCandidate(index,
                traversal,
                placeHolderAliases /*sargable aliases*/,
                recordTypes,
                baseQuantifier.getFlowedObjectType(),
                baseQuantifier.getAlias(),
                ((GroupingKeyExpression)index.getRootExpression()).getGroupingCount(),
                groupByQun.getRangesOver().get().getResultValue(),
                selectHaving.getResultValue());
    }

    @Nonnull
    private Quantifier constructSelectWhere(@Nonnull final Quantifier.ForEach baseQuantifier, @Nonnull final CorrelationIdentifier groupByFieldName, final List<? extends Value> groupingKeyNames) {
        final var allExpansionsBuilder = ImmutableList.<GraphExpansion>builder();

        // 1. start with the base quantifier.
        allExpansionsBuilder.add(GraphExpansion.ofQuantifier(baseQuantifier));

        // 2. add the SELECT-WHERE part, where we expose grouping and grouped columns, allowing query fragments that
        //    governs only these columns to properly bind to this part, similar to how value indices work.
        final var keyValues = Lists.<Value>newArrayList();
        final var valueValues = Lists.<Value>newArrayList();
        final var state = VisitorState.of(keyValues, valueValues, baseQuantifier, ImmutableList.of(), 0, 0);
        final var selectWhereGraphExpansion = pop(groupingKeyExpression.getWholeKey().expand(push(state)));

        // 2.1. add an RCV column representing the grouping columns as the first result set column
        final var groupingValue = RecordConstructorValue.ofColumns(groupingKeyNames.subList(0, groupingKeyExpression.getGroupingCount()).stream().map(v -> Column.of(((FieldValue)v).getLastField(), v)).collect(Collectors.toList()));

        // 2.2. flow all underlying quantifiers in their own QOV columns.
        final var builder = GraphExpansion.builder();
        builder.addResultColumn(Column.of(Type.Record.Field.of(groupingValue.getResultType(), Optional.of(groupByFieldName.getId())), groupingValue));
        Stream.concat(Stream.of(baseQuantifier), selectWhereGraphExpansion.getQuantifiers().stream()).forEach(qun -> {
            final var quantifiedValue = QuantifiedObjectValue.of(qun.getAlias(), qun.getFlowedObjectType());
            builder.addResultColumn(Column.of(Type.Record.Field.of( quantifiedValue.getResultType(), Optional.of(qun.getAlias().getId())), quantifiedValue));
        });
        builder.addAllPlaceholders(selectWhereGraphExpansion.getPlaceholders());
        builder.addAllPredicates(selectWhereGraphExpansion.getPredicates());
        builder.addAllQuantifiers(selectWhereGraphExpansion.getQuantifiers());
        allExpansionsBuilder.add(builder.build());

        return Quantifier.forEach(GroupExpressionRef.of(GraphExpansion.ofOthers(allExpansionsBuilder.build()).buildSelect()));
    }

    @Nonnull
    private Quantifier constructGroupBy(@Nonnull final Quantifier.ForEach baseQuantifier,
                                        @Nonnull final CorrelationIdentifier groupByFieldName, @Nonnull final List<? extends Value> groupingKeyNames,
                                        @Nonnull final Quantifier selectWhereQun) {
        final var groupedValue = groupingKeyNames.subList(groupingKeyExpression.getGroupingCount(), groupingKeyNames.size());
        Verify.verify(groupedValue.size() == 1);
        final int[] cnt = {0};
        final var aggregateValue = RecordConstructorValue.ofColumns(groupedValue.stream().map(gv -> {
            final var prefixedFieldName = Lists.newArrayList(baseQuantifier.getAlias().getId());
            prefixedFieldName.addAll(((FieldValue)gv).getFieldPathNames());
            final var groupedFieldReference = FieldValue.ofFieldNames(selectWhereQun.getFlowedObjectValue(), prefixedFieldName);
            return (AggregateValue)functionMap.get(index.getType()).encapsulate(TypeRepository.newBuilder(), List.of(groupedFieldReference));
        }).map(av -> Column.of(Type.Record.Field.of(av.getResultType(), Optional.of(index.getType() + "_agg_" + cnt[0]++)), av)).collect(Collectors.toList()));
        final var groupingColsValue = FieldValue.ofFieldName(selectWhereQun.getFlowedObjectValue(), groupByFieldName.getId());

        if (groupingColsValue.getResultType() instanceof Type.Record && ((Type.Record)groupingColsValue.getResultType()).getFields().isEmpty()) {
            return Quantifier.forEach(GroupExpressionRef.of(new GroupByExpression(aggregateValue, null, selectWhereQun)));
        } else {
            return Quantifier.forEach(GroupExpressionRef.of(new GroupByExpression(aggregateValue, groupingColsValue, selectWhereQun)));
        }
    }

    @Nonnull
    private Pair<SelectExpression, List<CorrelationIdentifier>> constructSelectHavingWithSort(@Nonnull final Quantifier groupByQun) {
        // the grouping value in GroupByExpression comes first if set.
        @Nullable final var groupingValueReference =
                (groupByQun.getRangesOver().get() instanceof GroupByExpression && ((GroupByExpression)groupByQun.getRangesOver().get()).getGroupingValue() == null)
                ? null
                : FieldValue.ofOrdinalNumber(groupByQun.getFlowedObjectValue(), 0);

        final var aggregateValueReference = FieldValue.ofOrdinalNumber(FieldValue.ofOrdinalNumber(groupByQun.getFlowedObjectValue(), groupingValueReference == null ? 0 : 1), 0);

        final var placeholderAliases = ImmutableList.<CorrelationIdentifier>builder();
        final var selectHavingGraphExpansionBuilder = GraphExpansion.builder().addQuantifier(groupByQun);
        if (groupingValueReference != null) {
            Values.deconstructRecord(groupingValueReference).forEach(v -> {
                final var field = (FieldValue)v;
                final var placeholder = v.asPlaceholder(CorrelationIdentifier.uniqueID(ValueComparisonRangePredicate.Placeholder.class));
                placeholderAliases.add(placeholder.getAlias());
                selectHavingGraphExpansionBuilder
                        .addResultColumn(Column.of(field.getLastField(), field))
                        .addPlaceholder(placeholder)
                        .addPredicate(placeholder);
            });
        }
        selectHavingGraphExpansionBuilder.addResultColumn(Column.of(aggregateValueReference.getLastField(), aggregateValueReference)); // we should also add the aggregate reference as a placeholder.
        return Pair.of(selectHavingGraphExpansionBuilder.build().buildSelect(), placeholderAliases.build());
    }

    @Nonnull
    private List<? extends Value> getGroupingAndGroupedColumns(@Nonnull final GroupingKeyExpression groupingKeysExpression,
                                                                       @Nonnull final Quantifier.ForEach innerBaseQuantifier,
                                                                       @Nonnull final Type type) {
        final var wholeKeyExpression = groupingKeysExpression.getWholeKey();

        final List<Value> groupingAndArgumentValues = new ArrayList<>();
        final VisitorState initialState =
                VisitorState.of(groupingAndArgumentValues,
                        Lists.newArrayList(),
                        innerBaseQuantifier,
                        ImmutableList.of(),
                        -1,
                        0);

        return Value.fromKeyExpressions(groupingKeysExpression.normalizeKeyForPositions(), innerBaseQuantifier.getAlias(), type);
    }

    public static boolean isAggregateIndex(@Nonnull final String indexType) {
        return allowedIndexTypes.contains(indexType);
    }

    private static final Map<String, BuiltInFunction<? extends Value>> functionMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    static {
        functionMap.put(IndexTypes.MAX_EVER_LONG, new NumericAggregationValue.MaxFn());
        functionMap.put(IndexTypes.MIN_EVER_LONG, new NumericAggregationValue.MinFn());
        functionMap.put(IndexTypes.SUM, new NumericAggregationValue.SumFn());
        functionMap.put(IndexTypes.COUNT, new CountValue.CountFn());
    }

    private static final Set<String> allowedIndexTypes = new LinkedHashSet<>();

    static {
        allowedIndexTypes.add(IndexTypes.COUNT);
        allowedIndexTypes.add(IndexTypes.SUM);
        allowedIndexTypes.add(IndexTypes.MIN_EVER_LONG);
        allowedIndexTypes.add(IndexTypes.MAX_EVER_LONG);
    }
}
