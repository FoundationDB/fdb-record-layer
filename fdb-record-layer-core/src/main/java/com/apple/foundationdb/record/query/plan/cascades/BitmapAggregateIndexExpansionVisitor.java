/*
 * BitmapAggregateIndexExpansionVisitor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.provider.foundationdb.indexes.BitmapValueIndexMaintainer;
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.Placeholder;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.BuiltInFunctionCatalog;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NumericAggregationValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;

/**
 * The bitmap aggregate index expansion visitor.
 */
public class BitmapAggregateIndexExpansionVisitor extends AggregateIndexExpansionVisitor {
    /**
     * Constructs a new instance of {@link BitmapAggregateIndexExpansionVisitor}.
     *
     * @param index The target index.
     * @param recordTypes The indexed record types.
     */
    public BitmapAggregateIndexExpansionVisitor(@Nonnull final Index index, @Nonnull final Collection<RecordType> recordTypes) {
        super(index, recordTypes);
        Verify.verify(IndexTypes.BITMAP_VALUE.equals(index.getType()));
    }

    @Nonnull
    @Override
    protected NonnullPair<Quantifier, List<Placeholder>> constructGroupBy(@Nonnull final Quantifier selectWhereQun, @Nonnull final GraphExpansion baseExpansion) {
        if (groupingKeyExpression.getGroupedCount() != 1) {
            throw new UnsupportedOperationException("bitmap aggregate index is expected to contain exactly one grouped expression, however it contains " + groupingKeyExpression.getGroupedCount() + " aggregations");
        }
        final Value groupedValue = baseExpansion.getResultColumns().get(groupingKeyExpression.getGroupingCount()).getValue();

        // construct aggregation RCV
        final Value argument;
        if (groupedValue instanceof FieldValue) {
            final var aliasMap = AliasMap.identitiesFor(Sets.union(selectWhereQun.getCorrelatedTo(),
                    groupedValue.getCorrelatedTo()));
            final var result = selectWhereQun.getRangesOver().get().getResultValue()
                    .pullUp(List.of(groupedValue), EvaluationContext.empty(), aliasMap, ImmutableSet.of(),
                            selectWhereQun.getAlias());
            if (!result.containsKey(groupedValue)) {
                throw new RecordCoreException("could not pull grouped value " + groupedValue)
                        .addLogInfo(LogMessageKeys.VALUE, groupedValue);
            }
            argument = Iterables.getOnlyElement(result.get(groupedValue));
        } else {
            throw new UnsupportedOperationException("unable to plan group by with non-field value " + groupedValue);
        }

        final var bitmapConstructAggFunc = BuiltInFunctionCatalog.getFunctionSingleton(NumericAggregationValue.BitmapConstructAggFn.class).orElseThrow();
        final var bitmapBitPositionFunc = BuiltInFunctionCatalog.getFunctionSingleton(ArithmeticValue.BitmapBitPositionFn.class).orElseThrow();
        final String sizeArgument = index.getOption(IndexOptions.BITMAP_VALUE_ENTRY_SIZE_OPTION);
        final int entrySize = sizeArgument != null ? Integer.parseInt(sizeArgument) : BitmapValueIndexMaintainer.DEFAULT_ENTRY_SIZE;
        final var entrySizeValue = LiteralValue.ofScalar(entrySize);

        final var aggregateValue = (Value)bitmapConstructAggFunc.encapsulate(ImmutableList.of(bitmapBitPositionFunc.encapsulate(ImmutableList.of(argument, entrySizeValue))));
        // add an RCV column representing the grouping columns as the first result set column
        // also, make sure to set the field type names correctly for each field value in the grouping keys RCV.

        final var groupingValues = baseExpansion.getResultColumns().subList(0, groupingKeyExpression.getGroupingCount())
                .stream()
                .map(Column::getValue)
                .collect(ImmutableList.toImmutableList());
        final var bitmapBitPosition = BuiltInFunctionCatalog.getFunctionSingleton(ArithmeticValue.BitmapBucketOffsetFn.class).orElseThrow();
        final var implicitGroupingValue = (Value)bitmapBitPosition.encapsulate(ImmutableList.of(argument, entrySizeValue));
        final var placeHolder = Placeholder.newInstanceWithoutRanges(implicitGroupingValue, newParameterAlias());

        final var selectQunValue = selectWhereQun.getRangesOver().get().getResultValue();
        final var aliasMap = AliasMap.identitiesFor(Sets.union(selectQunValue.getCorrelatedTo(), groupingValues.stream().flatMap(v -> v.getCorrelatedTo().stream()).collect(ImmutableSet.toImmutableSet())));
        final var pulledUpGroupingValuesMap = selectQunValue.pullUp(groupingValues,
                EvaluationContext.empty(), aliasMap, ImmutableSet.of(), selectWhereQun.getAlias());
        final var explicitPulledUpGroupingValues = groupingValues.stream().map(groupingValue -> {
            if (!pulledUpGroupingValuesMap.containsKey(groupingValue)) {
                throw new RecordCoreException("could not pull grouping value " + groupingValue)
                        .addLogInfo(LogMessageKeys.VALUE, groupingValue);
            }
            return Iterables.getOnlyElement(pulledUpGroupingValuesMap.get(groupingValue));
        }).collect(ImmutableList.toImmutableList());

        final var pulledUpGroupingValues = ImmutableList.<Value>builder().addAll(explicitPulledUpGroupingValues).add(implicitGroupingValue).build();
        final var groupingColsValue = RecordConstructorValue.ofUnnamed(pulledUpGroupingValues);
        return NonnullPair.of(Quantifier.forEach(Reference.initialOf(
                new GroupByExpression(groupingColsValue, RecordConstructorValue.ofUnnamed(ImmutableList.of(aggregateValue)),
                        GroupByExpression::nestedResults, selectWhereQun))), ImmutableList.of(placeHolder));
    }
}
