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
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
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
import com.apple.foundationdb.record.query.plan.cascades.values.IndexBackedAggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NumericAggregationValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.Values;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Expands an aggregate index into a {@link MatchCandidate}. The generation will expand a {@link KeyExpression} into a
 * group by triplet QGM comprising a select-where, group-by-expression, and select-having, the triplet is followed
 * by an optional {@link MatchableSortExpression} that defines the sort order of the match candidate stream of records.
 */
public class AggregateIndexExpansionVisitor extends KeyExpressionExpansionVisitor
                                            implements ExpansionVisitor<KeyExpressionExpansionVisitor.VisitorState> {

    @Nonnull
    private static final Supplier<Set<String>> allowedIndexTypes = Suppliers.memoize(AggregateIndexExpansionVisitor::computeAllowedIndexesMap);

    @Nonnull
    private static final Supplier<Map<String, BuiltInFunction<? extends Value>>> aggregateMap = Suppliers.memoize(AggregateIndexExpansionVisitor::computeAggregateMap);

    @Nonnull
    private final Index index;

    @Nonnull
    private final Collection<RecordType> recordTypes;

    @Nonnull
    private final GroupingKeyExpression groupingKeyExpression;

    /**
     * Constructs a new instance of {@link AggregateIndexExpansionVisitor}.
     *
     * @param index The target index.
     * @param recordTypes The indexed record types.
     */
    public AggregateIndexExpansionVisitor(@Nonnull final Index index, @Nonnull final Collection<RecordType> recordTypes) {
        Preconditions.checkArgument(allowedIndexTypes.get().contains(index.getType()));
        Preconditions.checkArgument(index.getRootExpression() instanceof GroupingKeyExpression);
        this.index = index;
        this.groupingKeyExpression = ((GroupingKeyExpression)index.getRootExpression());
        this.recordTypes = recordTypes;
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
    public MatchCandidate expand(@Nonnull final java.util.function.Supplier<Quantifier.ForEach> baseQuantifierSupplier,
                                 @Nullable final KeyExpression ignored,
                                 final boolean isReverse) {
        Verify.verify(ignored == null);
        final var baseQuantifier = baseQuantifierSupplier.get();
        final var groupingAndGroupedCols = Value.fromKeyExpressions(groupingKeyExpression.normalizeKeyForPositions(), baseQuantifier.getAlias(), baseQuantifier.getFlowedObjectType());
        final var groupingValues = groupingAndGroupedCols.subList(0, groupingKeyExpression.getGroupingCount());
        final var groupedValues = groupingAndGroupedCols.subList(groupingKeyExpression.getGroupingCount(), groupingAndGroupedCols.size());

        if (groupedValues.size() > 1) {
            throw new UnsupportedOperationException(String.format("aggregate index is expected to contain exactly one aggregation, however it contains %d aggregations", groupedValues.size()));
        }

        // 1. create a SELECT-WHERE expression.
        final var selectWhereQun = constructSelectWhere(baseQuantifier, groupingValues);

        // 2. create a GROUP-BY expression on top.
        final var groupByQun = constructGroupBy(baseQuantifier.getAlias(), groupedValues, selectWhereQun);

        // 3. construct SELECT-HAVING with SORT on top.
        final var selectHavingAndPlaceholderAliases = constructSelectHaving(groupByQun);
        final var selectHaving = selectHavingAndPlaceholderAliases.getLeft();
        final var placeHolderAliases = selectHavingAndPlaceholderAliases.getRight();

        // 4. add sort on top, if necessary, this will be absorbed later on as an ordering property of the match candidate.
        final var maybeWithSort = placeHolderAliases.isEmpty()
                ? GroupExpressionRef.of(selectHaving) // single group, sort by constant
                : GroupExpressionRef.of(new MatchableSortExpression(placeHolderAliases, isReverse, selectHaving));

        final var traversal = ExpressionRefTraversal.withRoot(maybeWithSort);
        return new AggregateIndexMatchCandidate(index,
                traversal,
                placeHolderAliases,
                recordTypes,
                baseQuantifier.getFlowedObjectType(),
                groupByQun.getRangesOver().get().getResultValue(),
                selectHaving.getResultValue());
    }

    @Nonnull
    private Quantifier constructSelectWhere(@Nonnull final Quantifier.ForEach baseQuantifier, final List<? extends Value> groupingValues) {
        final var allExpansionsBuilder = ImmutableList.<GraphExpansion>builder();
        allExpansionsBuilder.add(GraphExpansion.ofQuantifier(baseQuantifier));

        // add the SELECT-WHERE part, where we expose grouping and grouped columns, allowing query fragments that governs
        // only these columns to properly bind to this part, similar to how value indices work.
        final var keyValues = Lists.<Value>newArrayList();
        final var valueValues = Lists.<Value>newArrayList();
        final var state = VisitorState.of(keyValues, valueValues, baseQuantifier, ImmutableList.of(), 0, 0);
        final var selectWhereGraphExpansion = pop(groupingKeyExpression.getWholeKey().expand(push(state)));

        // add an RCV column representing the grouping columns as the first result set column
        final var groupingValue = RecordConstructorValue.ofColumns(groupingValues
                .stream()
                .map(v -> Column.of(((FieldValue)v).getLastField(), v))
                .collect(Collectors.toList()));

        // flow all underlying quantifiers in their own QOV columns.
        final var builder = GraphExpansion.builder();
        builder.addResultColumn(Column.of(Type.Record.Field.ofAutoNamed(groupingValue.getResultType()), groupingValue));
        Stream.concat(Stream.of(baseQuantifier), selectWhereGraphExpansion.getQuantifiers().stream())
                .forEach(qun -> {
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
    private Quantifier constructGroupBy(@Nonnull final CorrelationIdentifier baseQuantifierCorrelationIdentifier,
                                        @Nonnull final List<? extends Value> groupedValue,
                                        @Nonnull final Quantifier selectWhereQun) {
        // construct aggregation RCV
        final int[] cnt = {0};
        final var aggregateValue = RecordConstructorValue.ofColumns(groupedValue.stream().map(gv -> {
            final var prefixedFieldPath = Stream.concat(Stream.of(baseQuantifierCorrelationIdentifier.getId()), ((FieldValue)gv).getFieldPathNames().stream()).collect(Collectors.toList());
            final var groupedFieldReference = FieldValue.ofFieldNames(selectWhereQun.getFlowedObjectValue(), prefixedFieldPath);
            return (AggregateValue)aggregateMap.get().get(index.getType()).encapsulate(TypeRepository.newBuilder(), List.of(groupedFieldReference));
        }).map(av -> Column.of(Type.Record.Field.of(av.getResultType(), Optional.of(generateAggregateFieldName(cnt[0]++))), av)).collect(Collectors.toList()));

        // construct grouping column(s) value
        final var groupingColumnName = getGroupingColumnName(selectWhereQun);
        final var groupingColsValue = FieldValue.ofFieldName(selectWhereQun.getFlowedObjectValue(), groupingColumnName);

        if (groupingColsValue.getResultType() instanceof Type.Record && ((Type.Record)groupingColsValue.getResultType()).getFields().isEmpty()) {
            return Quantifier.forEach(GroupExpressionRef.of(new GroupByExpression(aggregateValue, null, selectWhereQun)));
        } else {
            return Quantifier.forEach(GroupExpressionRef.of(new GroupByExpression(aggregateValue, groupingColsValue, selectWhereQun)));
        }
    }

    @Nonnull
    private String generateAggregateFieldName(int fieldIdx) {
        return index.getName() + "_" + index.getType() + "_agg_" + fieldIdx;
    }

    @Nonnull
    private Pair<SelectExpression, List<CorrelationIdentifier>> constructSelectHaving(@Nonnull final Quantifier groupByQun) {
        // the grouping value in GroupByExpression comes first (if set).
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
        selectHavingGraphExpansionBuilder.addResultColumn(Column.of(aggregateValueReference.getLastField(), aggregateValueReference)); // TODO should we also add the aggregate reference as a placeholder?
        return Pair.of(selectHavingGraphExpansionBuilder.build().buildSelect(), placeholderAliases.build());
    }

    @Nonnull
    private static String getGroupingColumnName(@Nonnull final Quantifier selectWhereQun) {
        final var flowedValues = selectWhereQun.getFlowedValues();
        Verify.verify(!flowedValues.isEmpty());
        return flowedValues.get(0).getLastField().getFieldName();
    }

    @Nonnull
    private static Set<String> computeAllowedIndexesMap() {
        final ImmutableSet.Builder<String> setBuilder = ImmutableSet.builder();
        setBuilder.add(IndexTypes.COUNT);
        setBuilder.add(IndexTypes.SUM);
        setBuilder.add(IndexTypes.MIN_EVER_LONG);
        setBuilder.add(IndexTypes.MAX_EVER_LONG);
        return setBuilder.build();
    }

    @Nonnull
    private static Map<String, BuiltInFunction<? extends Value>> computeAggregateMap() {
        final ImmutableMap.Builder<String, BuiltInFunction<? extends Value>> mapBuilder = ImmutableMap.builder();
        mapBuilder.put(IndexTypes.MAX_EVER_LONG, new IndexBackedAggregateValue.MaxEverFn());
        mapBuilder.put(IndexTypes.MIN_EVER_LONG, new IndexBackedAggregateValue.MinEverFn());
        mapBuilder.put(IndexTypes.SUM, new NumericAggregationValue.SumFn());
        mapBuilder.put(IndexTypes.COUNT, new CountValue.CountFn());
        return mapBuilder.build();
    }

    public static boolean isAggregateIndex(@Nonnull final String indexType, final KeyExpression rootExpression) {
        // todo: we currently can not handle COUNT(*) aggregate indexes.
        if (!(rootExpression instanceof GroupingKeyExpression)) {
            return false;
        }
        final var groupingKeyExpression = (GroupingKeyExpression)rootExpression;
        if (groupingKeyExpression.getWholeKey() instanceof EmptyKeyExpression) {
            return false; // TODO: the case for COUNT(*)
        }
        return allowedIndexTypes.get().contains(indexType);
    }
}
