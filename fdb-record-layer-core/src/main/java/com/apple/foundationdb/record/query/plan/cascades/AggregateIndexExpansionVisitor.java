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
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.MatchableSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValueComparisonRangePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.CountValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NumericAggregationValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Expands an aggregate index into a {@link MatchCandidate}.
 */
public class AggregateIndexExpansionVisitor extends KeyExpressionExpansionVisitor
                                            implements ExpansionVisitor<KeyExpressionExpansionVisitor.VisitorState> {

    @Nonnull
    private final Index index;

    @Nonnull
    private final Collection<RecordType> recordTypes;

    @Nonnull
    private final Supplier<KeyExpression> groupedExpressionSupplier;

    @Nonnull
    private final Supplier<KeyExpression> groupingExpressionSupplier;

    public AggregateIndexExpansionVisitor(@Nonnull final Index index, @Nonnull final Collection<RecordType> recordTypes) {
        Preconditions.checkArgument(allowedIndexTypes.contains(index.getType()));
        Preconditions.checkArgument(index.getRootExpression() instanceof GroupingKeyExpression);
        this.groupedExpressionSupplier = () -> ((GroupingKeyExpression)(index.getRootExpression())).getGroupedSubKey();
        this.groupingExpressionSupplier = () -> ((GroupingKeyExpression)(index.getRootExpression())).getGroupingSubKey();
        this.index = index;
        this.recordTypes = recordTypes;
    }

    @Nonnull
    @Override
    public MatchCandidate expand(@Nonnull final java.util.function.Supplier<Quantifier.ForEach> baseQuantifierSupplier,
                                 @Nullable final KeyExpression primaryKey,
                                 final boolean isReverse) {

        final var baseQuantifier = baseQuantifierSupplier.get();
        final var allExpansionsBuilder = ImmutableList.<GraphExpansion>builder();

        // 1. start with the base quantifier.
        allExpansionsBuilder.add(GraphExpansion.ofQuantifier(baseQuantifier));

        // 2. add the SELECT-WHERE part, where we expose grouping and grouped columns, allowing query fragments that
        //    governs only these columns to properly bind to this part, similar to how value indices work.
        final var groupingAndGroupedExpr = ((GroupingKeyExpression)index.getRootExpression()).getWholeKey();
        final var keyValues = Lists.<Value>newArrayList();
        final var valueValues = Lists.<Value>newArrayList();
        final var state = VisitorState.of(keyValues, valueValues, baseQuantifier, ImmutableList.of(), 0, 0);
        final var selectWhereGraphExpansion = pop(groupingAndGroupedExpr.expand(push(state)));



        // 2.1. add another group to the list of results representing the grouping columns.
        final var groupByFieldName = CorrelationIdentifier.uniqueID();
        final var groupingKeyNames = getGroupingKeyNames(groupingExpressionSupplier.get());
        final var groupingValue = RecordConstructorValue.ofUnnamed(
                groupingKeyNames.stream().map(n -> FieldValue.ofFieldName(baseQuantifier.getFlowedObjectValue(), n)).collect(Collectors.toList()));
        allExpansionsBuilder.add(selectWhereGraphExpansion.toBuilder().addResultColumn(Column.of(Type.Record.Field.of(groupingValue.getResultType(), Optional.of(groupByFieldName.getId())), groupingValue)).build());
        final var underlying = GraphExpansion.ofOthers(allExpansionsBuilder.build());
        final var selectWhereQun = Quantifier.forEach(GroupExpressionRef.of(underlying.buildSelect()));
        // 3. create a GROUP-BY expression on top.
        final var groupedValue = FieldValue.ofFieldName(selectWhereQun.getFlowedObjectValue(), ((FieldKeyExpression)groupedExpressionSupplier.get()).getFieldName());
        final var aggregateValue = (AggregateValue)functionMap.get(index.getType()).encapsulate(TypeRepository.newBuilder(), List.of(groupedValue));

        final var underlyingGroupByField = FieldValue.ofFieldName(selectWhereQun.getFlowedObjectValue(), groupByFieldName.getId());

        final var groupByExpression = new GroupByExpression(aggregateValue, underlyingGroupByField, selectWhereQun);
        final var groupByQun = Quantifier.forEach(GroupExpressionRef.of(groupByExpression));

        final var aggregateValueReference = FieldValue.ofOrdinalNumber(groupByQun.getFlowedObjectValue(), 1);
        final var aggregateValuePlaceHolder = aggregateValueReference.asPlaceholder(CorrelationIdentifier.uniqueID(ValueComparisonRangePredicate.Placeholder.class));

        final var groupingValueReference = FieldValue.ofOrdinalNumber(groupByQun.getFlowedObjectValue(), 0);

        final var selectHavingGraphExpansion = GraphExpansion.builder().addQuantifier(groupByQun)
                .addResultValue(FieldValue.ofOrdinalNumber(groupByQun.getFlowedObjectValue(), 0))
                .addResultValue(aggregateValueReference)
                .addPlaceholder(aggregateValuePlaceHolder)
                .addPredicate(aggregateValuePlaceHolder)
                .build();
        final var aggregationAlias = selectHavingGraphExpansion.getPlaceholderAliases();
        final var sorted = new MatchableSortExpression(selectHavingGraphExpansion.getPlaceholderAliases(), isReverse, selectHavingGraphExpansion.buildSelect());
        final var traversal = ExpressionRefTraversal.withRoot(GroupExpressionRef.of(sorted));
        return new AggregateIndexMatchCandidate(index, traversal, aggregationAlias, recordTypes, baseQuantifier.getFlowedObjectType());
    }

    @Nonnull
    private static List<String> getGroupingKeyNames(@Nonnull final KeyExpression groupingKeysExpression) {
        if (groupingKeysExpression instanceof ThenKeyExpression) {
            return ((ThenKeyExpression)groupingKeysExpression).getChildren().stream().map(AggregateIndexExpansionVisitor::getGroupingKeyNames).flatMap(Collection::stream).collect(Collectors.toList());
        } else if (groupingKeysExpression instanceof FieldKeyExpression) {
            return List.of(((FieldKeyExpression)groupingKeysExpression).getFieldName());
        } else {
            throw new RecordCoreException(String.format("unexpected grouping key type: %s", groupingKeysExpression.getClass().getName()));
        }
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
