/*
 * IndexGenerator.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexPredicate;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FunctionKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import com.apple.foundationdb.record.query.combinatorics.TopologicalSort;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.IndexPredicateExpansion;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.properties.ReferencesAndDependenciesProperty;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.CountValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.IndexableAggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NumericAggregationValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.StreamableAggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.ValueWithChild;
import com.apple.foundationdb.record.query.plan.cascades.values.Values;
import com.apple.foundationdb.record.query.plan.cascades.values.VersionValue;
import com.apple.foundationdb.record.query.plan.planning.BooleanPredicateNormalizer;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.NullableArrayUtils;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.empty;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.metadata.Key.Expressions.keyWithValue;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * Generates a {@link KeyExpression} from a given query plan.
 */
@SuppressWarnings({"PMD.TooManyStaticImports", "OptionalUsedAsFieldOrParameterType"})
public final class IndexGenerator {

    private static final String BITMAP_BIT_POSITION = "bitmap_bit_position";
    private static final String BITMAP_BUCKET_OFFSET = "bitmap_bucket_offset";

    /**
     * Map from each correlation in the query plan to its list of results.
     */
    @Nonnull
    private final IdentityHashMap<CorrelationIdentifier, Value> correlatedKeyExpressions = new IdentityHashMap<>();

    @Nonnull
    private final List<RelationalExpression> relationalExpressions;

    @Nonnull
    private final RelationalExpression relationalExpression;

    private final boolean useLegacyBasedExtremumEver;

    private IndexGenerator(@Nonnull RelationalExpression relationalExpression, boolean useLegacyBasedExtremumEver) {
        collectQuantifiers(relationalExpression);
        final var partialOrder = ReferencesAndDependenciesProperty.evaluate(Reference.of(relationalExpression));
        relationalExpressions =
                TopologicalSort.anyTopologicalOrderPermutation(partialOrder)
                        .orElseThrow(() -> new RelationalException("graph has cycles", ErrorCode.UNSUPPORTED_OPERATION).toUncheckedWrappedException())
                        .stream()
                        .map(Reference::get)
                        .collect(toList());
        this.relationalExpression = relationalExpression;
        this.useLegacyBasedExtremumEver = useLegacyBasedExtremumEver;
    }

    @Nonnull
    public RecordLayerIndex generate(@Nonnull String indexName, boolean isUnique, @Nonnull Type.Record tableType, boolean containsNullableArray) {
        final var indexBuilder = RecordLayerIndex.newBuilder()
                .setName(indexName)
                .setTableName(getRecordTypeName())
                .setUnique(isUnique);

        collectQuantifiers(relationalExpression);

        final var partialOrder = ReferencesAndDependenciesProperty.evaluate(Reference.of(relationalExpression));
        final var expressionRefs =
                TopologicalSort.anyTopologicalOrderPermutation(partialOrder)
                        .orElseThrow(() -> new RecordCoreException("graph has cycles")).stream().map(Reference::get).collect(toList());

        checkValidity(expressionRefs);

        // add predicates
        final var predicate = getTopLevelPredicate(Lists.reverse(expressionRefs));
        if (predicate != null) {
            indexBuilder.setPredicate(IndexPredicate.fromQueryPredicate(predicate).toProto());
        }

        final var simplifiedValues = collectResultValues(relationalExpression.getResultValue());

        final var unsupportedAggregates = simplifiedValues.stream().filter(sv -> sv instanceof StreamableAggregateValue && !(sv instanceof IndexableAggregateValue)).collect(toList());
        Assert.thatUnchecked(unsupportedAggregates.isEmpty(), ErrorCode.UNSUPPORTED_OPERATION,
                () -> String.format("Unsupported aggregate index definition containing non-indexable aggregation (%s), consider using a value index on the aggregated column instead.", unsupportedAggregates.stream().map(Objects::toString).collect(joining(","))));

        Assert.thatUnchecked(simplifiedValues.stream().allMatch(sv -> sv instanceof FieldValue || sv instanceof IndexableAggregateValue || sv instanceof VersionValue || sv instanceof ArithmeticValue));
        final var aggregateValues = simplifiedValues.stream().filter(sv -> sv instanceof IndexableAggregateValue).collect(toList());
        final var fieldValues = simplifiedValues.stream().filter(sv -> !(sv instanceof IndexableAggregateValue)).collect(toList());
        final var versionValues = simplifiedValues.stream().filter(sv -> sv instanceof VersionValue).map(sv -> (VersionValue) sv).collect(toList());
        Assert.thatUnchecked(versionValues.size() <= 1, ErrorCode.UNSUPPORTED_OPERATION, "Cannot have index with more than one version column");
        final Map<Value, String> orderingFunctions = new IdentityHashMap<>();
        final var orderByValues = getOrderByValues(relationalExpression, orderingFunctions);
        if (aggregateValues.isEmpty()) {
            indexBuilder.setIndexType(versionValues.isEmpty() ? IndexTypes.VALUE : IndexTypes.VERSION);
            Assert.thatUnchecked(orderByValues.stream().allMatch(sv -> sv instanceof FieldValue || sv instanceof VersionValue || sv instanceof ArithmeticValue), ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, order by must be a subset of projection list");
            if (fieldValues.size() > 1) {
                Assert.thatUnchecked(!orderByValues.isEmpty(), ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, value indexes must have an order by clause at the top level");
            }
            final var reordered = reorderValues(fieldValues, orderByValues);
            final var expression = generate(reordered, orderingFunctions);
            final var splitPoint = orderByValues.isEmpty() ? -1 : orderByValues.size();
            if (splitPoint != -1 && splitPoint < fieldValues.size()) {
                indexBuilder.setKeyExpression(KeyExpression.fromProto(NullableArrayUtils.wrapArray(keyWithValue(expression, splitPoint).toKeyExpression(), tableType, containsNullableArray)));
            } else {
                indexBuilder.setKeyExpression(KeyExpression.fromProto(NullableArrayUtils.wrapArray(expression.toKeyExpression(), tableType, containsNullableArray)));
            }
        } else {
            Assert.thatUnchecked(aggregateValues.size() == 1, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, multiple group by aggregations found");
            final var aggregateValue = (AggregateValue) aggregateValues.get(0);
            int aggregateOrderIndex = -1;
            if (!orderByValues.isEmpty()) {
                boolean inOrder = true;
                Iterator<Value> fieldIterator = fieldValues.iterator();
                for (int i = 0; i < orderByValues.size(); i++) {
                    Value value = orderByValues.get(i);
                    if (value.equals(aggregateValue)) {
                        if (aggregateOrderIndex >= 0) {
                            Assert.failUnchecked(ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, aggregate can appear only once in ordering clause");
                        }
                        aggregateOrderIndex = i;
                    } else if (fieldIterator.hasNext()) {
                        Value expectedField = fieldIterator.next();
                        if (!value.equals(expectedField)) {
                            inOrder = false;
                            break;
                        }
                    } else {
                        inOrder = false;
                        break;
                    }
                }
                if (fieldIterator.hasNext() || !inOrder) {
                    Assert.failUnchecked(ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, attempt to create a covering aggregate index");
                }
            }
            final Optional<KeyExpression> groupingKeyExpression = fieldValues.isEmpty() ? Optional.empty() : Optional.of(generate(fieldValues, orderingFunctions));
            final var indexExpressionAndType = generateAggregateIndexKeyExpression(aggregateValue, groupingKeyExpression);
            final String indexType = Objects.requireNonNull(indexExpressionAndType.getRight());
            indexBuilder.setIndexType(indexType);
            indexBuilder.setKeyExpression(KeyExpression.fromProto(NullableArrayUtils.wrapArray(indexExpressionAndType.getLeft().toKeyExpression(), tableType, containsNullableArray)));
            if (indexType.equals(IndexTypes.PERMUTED_MIN) || indexType.equals(IndexTypes.PERMUTED_MAX)) {
                int permutedSize = aggregateOrderIndex < 0 ? 0 : (fieldValues.size() - aggregateOrderIndex);
                indexBuilder.setOption(IndexOptions.PERMUTED_SIZE_OPTION, permutedSize);
            } else if (aggregateOrderIndex >= 0) {
                Assert.failUnchecked(ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition. Cannot order " + indexType + " index by aggregate value");
            }
        }
        return indexBuilder.build();
    }

    @Nonnull
    private List<Value> collectResultValues(@Nonnull Value value) {
        final var resultValues = simplify(value);
        final var isSingleAggregation = resultValues.size() == 1 && resultValues.get(0) instanceof IndexableAggregateValue;
        final var maybeGroupBy = relationalExpressions.stream().filter(exp -> exp instanceof GroupByExpression).findFirst();
        if (maybeGroupBy.isPresent()) {
            // if the final result value contains nothing but the aggregation value, add the grouping values to it.
            final var groupBy = (GroupByExpression) maybeGroupBy.get();
            final var groupingValues = groupBy.getGroupingValue();
            final var adjustResultValues = adjustGroupByFieldPaths(resultValues, groupBy);
            if (isSingleAggregation) {
                if (groupingValues == null) {
                    return adjustResultValues;
                } else {
                    final var simplifiedGroupingValues = Values.deconstructRecord(groupingValues).stream().map(this::dereference).map(v -> v.simplify(AliasMap.emptyMap(), Set.of()));
                    return Stream.concat(adjustResultValues.stream(), simplifiedGroupingValues).collect(toList());
                }
            } else {
                // Make sure the grouping values and the result values are consistent
                if (groupingValues == null) {
                    // This shouldn't happen unless there's more than one indexable aggregate value
                    Assert.failUnchecked(ErrorCode.UNSUPPORTED_OPERATION, "Grouping values absent from aggregate result value");
                }
                final var simplifiedGroupingValues = Values.deconstructRecord(groupingValues).stream().map(this::dereference).map(v -> v.simplify(AliasMap.emptyMap(), Set.of())).iterator();
                for (Value resultValue : resultValues) {
                    if (resultValue instanceof IndexableAggregateValue) {
                        continue;
                    }
                    if (!simplifiedGroupingValues.hasNext()) {
                        Assert.failUnchecked(ErrorCode.UNSUPPORTED_OPERATION, "Aggregate result value contains values missing from the grouping expression");
                    }
                    Value groupingValue = simplifiedGroupingValues.next();
                    if (!resultValue.equals(groupingValue)) {
                        Assert.failUnchecked(ErrorCode.UNSUPPORTED_OPERATION, "Aggregate result value does not align with grouping value");
                    }
                }
                if (simplifiedGroupingValues.hasNext()) {
                    Assert.failUnchecked(ErrorCode.UNSUPPORTED_OPERATION, "Grouping value absent from aggregate result value");
                }
                return adjustResultValues;
            }
        } else {
            return resultValues;
        }
    }

    @Nonnull
    private static List<Value> adjustGroupByFieldPaths(@Nonnull List<Value> resultValues,
                                                       @Nonnull GroupByExpression groupByExpression) {
        /*
         * This strips the root of the field path from every FieldValue that is referencing an attribute from the
         * underlying SELECT-WHERE expression.
         * This is to enable the construction of a valid KeyExpression; it is valid because only single-sourced are
         * currently allowed in aggregate indexes, in other words, there is no room for ambiguity, even after removing
         * the root.
         */
        final var selectWhereQun = groupByExpression.getQuantifiers().get(0);
        return resultValues.stream().map(resultValue -> resultValue.replace(value -> {
            if (!(value instanceof FieldValue)) {
                return value;
            }
            final FieldValue fieldValue = (FieldValue) value;
            if (!(fieldValue.getChild() instanceof QuantifiedObjectValue)) {
                return value;
            }
            final QuantifiedObjectValue quantifiedObjectValue = (QuantifiedObjectValue) fieldValue.getChild();
            if (!quantifiedObjectValue.getAlias().equals(selectWhereQun.getAlias())) {
                return value;
            }
            final var fieldAccessors = fieldValue.getFieldPath().getFieldAccessors();
            return FieldValue.ofFields(fieldValue.getChild(), new FieldValue.FieldPath(fieldAccessors.subList(1, fieldAccessors.size())));
        })).collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    private List<Value> simplify(@Nonnull Value value) {
        return Values.deconstructRecord(value)
                .stream()
                .map(this::dereference)
                .map(v -> v.simplify(AliasMap.emptyMap(), Set.of()))
                .collect(toList());
    }

    @Nonnull
    private List<Value> getOrderByValues(@Nonnull RelationalExpression relationalExpression,
                                         @Nonnull Map<Value, String> orderingFunctions) {
        if (relationalExpression instanceof LogicalSortExpression) {
            final var logicalSortExpression = (LogicalSortExpression) relationalExpression;
            final var reverseAliasMap = AliasMap.ofAliases(Quantifier.current(), logicalSortExpression.getQuantifiers().get(0).getAlias());
            final ImmutableList.Builder<Value> values = ImmutableList.builder();
            for (var orderingPart : logicalSortExpression.getOrdering().getOrderingParts()) {
                final String orderingFunction;
                switch (orderingPart.getSortOrder()) {
                    case ASCENDING:
                        orderingFunction = null;
                        break;
                    case DESCENDING:
                        orderingFunction = "order_desc_nulls_last";
                        break;
                    case ASCENDING_NULLS_LAST:
                        orderingFunction = "order_asc_nulls_last";
                        break;
                    case DESCENDING_NULLS_FIRST:
                        orderingFunction = "order_desc_nulls_first";
                        break;
                    default:
                        orderingFunction = null;
                        break;
                }
                if (orderingPart.getValue().getResultType().getTypeCode() == Type.TypeCode.RECORD) {
                    for (Value value : Values.deconstructRecord(orderingPart.getValue())) {
                        final var rebased = dereference(value.rebase(reverseAliasMap))
                                .simplify(AliasMap.emptyMap(), Set.of());
                        values.add(rebased);
                        if (orderingFunction != null) {
                            orderingFunctions.put(rebased, orderingFunction);
                        }
                    }
                } else {
                    final Value rebased = dereference(orderingPart.getValue().rebase(reverseAliasMap))
                            .simplify(AliasMap.emptyMap(), Set.of());
                    values.add(rebased);
                    if (orderingFunction != null) {
                        orderingFunctions.put(rebased, orderingFunction);
                    }
                }
            }
            return values.build();
        }
        return List.of();
    }

    private static List<Value> reorderValues(@Nonnull List<Value> values, @Nonnull List<Value> orderByValues) {
        Assert.thatUnchecked(values.size() >= orderByValues.size());
        if (orderByValues.isEmpty()) {
            return values;
        }
        final var remaining = values.stream().filter(v -> !orderByValues.contains(v)).collect(ImmutableList.toImmutableList());
        return ImmutableList.<Value>builder().addAll(orderByValues).addAll(remaining).build();
    }

    @SuppressWarnings({"OptionalIsPresent", "deprecation"})
    @Nonnull
    private NonnullPair<KeyExpression, String> generateAggregateIndexKeyExpression(@Nonnull AggregateValue aggregateValue,
                                                                                   @Nonnull Optional<KeyExpression> maybeGroupingExpression) {
        Assert.thatUnchecked(aggregateValue instanceof IndexableAggregateValue);
        final var indexableAggregateValue = (IndexableAggregateValue) aggregateValue;
        final var child = Iterables.getOnlyElement(aggregateValue.getChildren());
        var indexTypeName = indexableAggregateValue.getIndexTypeName();
        final KeyExpression groupedValue;
        final GroupingKeyExpression keyExpression;
        // COUNT(*) is a special case.
        if (aggregateValue instanceof CountValue && indexTypeName.equals(IndexTypes.COUNT)) {
            if (maybeGroupingExpression.isPresent()) {
                keyExpression = new GroupingKeyExpression(maybeGroupingExpression.get(), 0);
            } else {
                keyExpression = new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0);
            }
        } else if (aggregateValue instanceof NumericAggregationValue.BitmapConstructAgg && indexTypeName.equals(IndexTypes.BITMAP_VALUE)) {
            Assert.thatUnchecked(child instanceof FieldValue || child instanceof ArithmeticValue, "Unsupported index definition, expecting a column argument in aggregation function");
            groupedValue = generate(List.of(child), Collections.emptyMap());
            // only support bitmap_construct_agg(bitmap_bit_position(column))
            // doesn't support bitmap_construct_agg(column)
            Assert.thatUnchecked(groupedValue instanceof FunctionKeyExpression, "Unsupported index definition, expecting a bitmap_bit_position function in bitmap_construct_agg function");
            final FunctionKeyExpression functionGroupedValue = (FunctionKeyExpression) groupedValue;
            Assert.thatUnchecked(BITMAP_BIT_POSITION.equals(functionGroupedValue.getName()), "Unsupported index definition, expecting a bitmap_bit_position function in bitmap_construct_agg function");
            final var groupedColumnValue = ((ThenKeyExpression) ((FunctionKeyExpression) groupedValue).getArguments()).getChildren().get(0);

            if (maybeGroupingExpression.isPresent()) {
                final var afterRemove = removeBitmapBucketOffset(maybeGroupingExpression.get());
                if (afterRemove == null) {
                    keyExpression = ((FieldKeyExpression) groupedColumnValue).ungrouped();
                } else {
                    keyExpression = ((FieldKeyExpression) groupedColumnValue).groupBy(afterRemove);
                }
            } else {
                throw Assert.failUnchecked("Unsupported index definition, unexpected grouping expression " + groupedValue);
            }
        } else {
            Assert.thatUnchecked(child instanceof FieldValue, "Unsupported index definition, expecting a column argument in aggregation function");
            groupedValue = generate(List.of(child), Collections.emptyMap());
            Assert.thatUnchecked(groupedValue instanceof FieldKeyExpression || groupedValue instanceof ThenKeyExpression);
            if (maybeGroupingExpression.isPresent()) {
                keyExpression = (groupedValue instanceof FieldKeyExpression) ?
                        ((FieldKeyExpression) groupedValue).groupBy(maybeGroupingExpression.get()) :
                        ((ThenKeyExpression) groupedValue).groupBy(maybeGroupingExpression.get());
            } else {
                keyExpression = (groupedValue instanceof FieldKeyExpression) ?
                        ((FieldKeyExpression) groupedValue).ungrouped() :
                        ((ThenKeyExpression) groupedValue).ungrouped();
            }
        }
        // special handling of min_ever and max_ever, depending on index attributes we either create the
        // long-based version or the tuple-based version.
        if (indexTypeName.equals(IndexTypes.MAX_EVER)) {
            if (useLegacyBasedExtremumEver) {
                final var indexValue = Iterables.getOnlyElement(indexableAggregateValue.getChildren());
                Verify.verify(indexValue.getResultType().isNumeric(), "only numeric types allowed in " + IndexTypes.MAX_EVER_LONG + " aggregation operation");
                indexTypeName = IndexTypes.MAX_EVER_LONG;
            } else {
                indexTypeName = IndexTypes.MAX_EVER_TUPLE;
            }
        } else if (indexTypeName.equals(IndexTypes.MIN_EVER)) {
            if (useLegacyBasedExtremumEver) {
                final var indexValue = Iterables.getOnlyElement(indexableAggregateValue.getChildren());
                Verify.verify(indexValue.getResultType().isNumeric(), "only numeric types allowed in " + IndexTypes.MIN_EVER_LONG + " aggregation operation");
                indexTypeName = IndexTypes.MIN_EVER_LONG;
            } else {
                indexTypeName = IndexTypes.MIN_EVER_TUPLE;
            }
        }
        return NonnullPair.of(keyExpression, indexTypeName);
    }

    /*
    remove bitmap_bucket_offset(col) from groupingExpression if it exists
    return null if groupingExpression only contains bitmap_bucket_offset(col)
     */
    @Nullable
    private KeyExpression removeBitmapBucketOffset(@Nonnull KeyExpression groupingExpression) {
        // groupingExpression looks like [*, bitmap_bucket_offset(C)+], so it is either a ThenKeyExpression or a FunctionKeyExpression
        Assert.thatUnchecked(groupingExpression instanceof ThenKeyExpression || groupingExpression instanceof FunctionKeyExpression, "Unsupported index definition, expecting column or function arguments in group by");
        if (groupingExpression instanceof ThenKeyExpression) {
            List<KeyExpression> groupingChildren = ((ThenKeyExpression) groupingExpression).getChildren();
            // check if the last one is bitmap_bucket_offset function, otherwise throws exception
            Assert.thatUnchecked(groupingChildren.get(groupingChildren.size() - 1) instanceof FunctionKeyExpression && BITMAP_BUCKET_OFFSET.equals(((FunctionKeyExpression) groupingChildren.get(groupingChildren.size() - 1)).getName()), "Unsupported index definition, expecting the last element in group by to be a bitmap_bucket_offset function");
            // a ThenKeyExpression has at least 2 children
            if (groupingChildren.size() >= 3) {
                return new ThenKeyExpression(groupingChildren, 0, groupingChildren.size() - 1);
            } else {
                return groupingChildren.get(0);
            }
        } else {
            if (BITMAP_BUCKET_OFFSET.equals(((FunctionKeyExpression) groupingExpression).getName())) {
                return null;
            } else {
                return groupingExpression;
            }
        }
    }

    @Nonnull
    private KeyExpression generate(@Nonnull List<Value> fields, @Nonnull Map<Value, String> orderingFunctions) {
        if (fields.isEmpty()) {
            return EmptyKeyExpression.EMPTY;
        } else if (fields.size() == 1) {
            return toKeyExpression(fields.get(0), orderingFunctions);
        }

        List<FieldValueTrieNode> trieNodes = new ArrayList<>(fields.size());
        List<KeyExpression> components = new ArrayList<>(fields.size());
        PeekingIterator<Value> valueIterator = Iterators.peekingIterator(fields.iterator());
        while (valueIterator.hasNext()) {
            if (!(valueIterator.peek() instanceof FieldValue)) {
                components.add(toKeyExpression(valueIterator.next(), orderingFunctions));
            } else {
                FieldValueTrieNode trieNode = FieldValueTrieNode.computeTrieForValues(FieldValue.FieldPath.empty(), valueIterator);
                trieNode.validateNoOverlaps(trieNodes);
                trieNodes.add(trieNode);

                components.add(toKeyExpression(trieNode, orderingFunctions));
            }
        }

        if (components.size() == 1) {
            return components.get(0);
        } else {
            return concat(components);
        }
    }

    @Nonnull
    private KeyExpression toKeyExpression(Value value, Map<Value, String> orderingFunctions) {
        var expr = toKeyExpression(value);
        if (orderingFunctions.containsKey(value)) {
            return function(orderingFunctions.get(value), expr);
        } else {
            return expr;
        }
    }

    @Nonnull
    private KeyExpression toKeyExpression(@Nonnull Value value) {
        if (value instanceof VersionValue) {
            return VersionKeyExpression.VERSION;
        } else if (value instanceof FieldValue) {
            FieldValue fieldValue = (FieldValue) value;
            return toKeyExpression(fieldValue.getFieldPath().getFieldAccessors().stream().map(acc -> Pair.of(acc.getName(), acc.getType())).collect(toList()));
        } else if (value instanceof ArithmeticValue) {
            var children = value.getChildren();
            var builder = ImmutableList.<KeyExpression>builder();
            for (Value child : children) {
                builder.add(toKeyExpression(child));
            }
            final List<KeyExpression> argumentList = builder.build();
            KeyExpression argumentExpr;
            if (argumentList.isEmpty()) {
                argumentExpr = empty();
            } else if (argumentList.size() == 1) {
                argumentExpr = argumentList.get(0);
            } else {
                argumentExpr = concat(argumentList);
            }
            return function(((ArithmeticValue) value).getLogicalOperator().name().toLowerCase(Locale.ROOT), argumentExpr);
        } else if (value instanceof LiteralValue<?>) {
            return Key.Expressions.value(((LiteralValue<?>) value).getLiteralValue());
        } else {
            Assert.failUnchecked(ErrorCode.UNSUPPORTED_OPERATION, "unable to construct expression");
            return null;
        }
    }

    @Nonnull
    private static KeyExpression toKeyExpression(@Nonnull FieldValueTrieNode trieNode,
                                                 @Nonnull Map<Value, String> orderingFunctions) {
        Assert.notNullUnchecked(trieNode.getChildrenMap());
        Assert.thatUnchecked(!trieNode.getChildrenMap().isEmpty());

        final var childrenMap = trieNode.getChildrenMap();
        final var exprConstituents = childrenMap.keySet().stream().map(key -> {
            final var expr = toKeyExpression(Objects.requireNonNull(key.getName()), key.getType());
            final var value = childrenMap.get(key);
            if (value.getChildrenMap() != null) {
                return expr.nest(toKeyExpression(value, orderingFunctions));
            } else if (orderingFunctions.containsKey(value.getValue())) {
                return function(orderingFunctions.get(value.getValue()), expr);
            } else {
                return expr;
            }
        }).map(v -> (KeyExpression) v).collect(toList());
        if (exprConstituents.size() == 1) {
            return exprConstituents.get(0);
        } else {
            return concat(exprConstituents);
        }
    }

    private void checkValidity(@Nonnull List<? extends RelationalExpression> expressions) {

        // there must be exactly one type full-unordered-scan, no joins, no self-joins.
        final var numScans = expressions.stream().filter(r -> r instanceof FullUnorderedScanExpression).count();
        Assert.thatUnchecked(numScans == 1, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, %s iteration generator found", numScans == 0 ? "no" : "more than one");

        // there must be at most a single group by
        final var numGroupBy = expressions.stream().filter(r -> r instanceof GroupByExpression).count();
        Assert.thatUnchecked(numGroupBy <= 1, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, multiple group by expressions found");

        // there can be only one aggregation in group by expression (maybe we can relax this in the future).
        final var groupByContainsOneAggregation = expressions.stream().filter(r -> r instanceof GroupByExpression).map(r -> (GroupByExpression) r).noneMatch(g -> Values.deconstructRecord(g.getAggregateValue()).size() > 1);
        Assert.thatUnchecked(groupByContainsOneAggregation, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, found group by expression with more than one aggregation");

        // result values of each operation must be simple or arithmetic values.
        final var allRecordValues = expressions.stream().allMatch(r -> (r.getResultValue().getResultType().getTypeCode() == Type.TypeCode.RECORD));
        Assert.thatUnchecked(allRecordValues, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, some operators return non-record values");

        final var allSimpleValues = expressions.stream()
                .filter(r -> r.getResultType().getInnerType() instanceof Type.Record)
                .allMatch(r -> Values.deconstructRecord(r.getResultValue()).stream().allMatch(v -> v instanceof FieldValue || v instanceof VersionValue || v instanceof QuantifiedObjectValue || v instanceof AggregateValue || v instanceof ArithmeticValue || v instanceof LiteralValue));
        Assert.thatUnchecked(allSimpleValues, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, not all fields can be mapped to key expression in");
    }

    @Nullable
    private static QueryPredicate getTopLevelPredicate(@Nonnull List<? extends RelationalExpression> expressions) {
        if (expressions.isEmpty()) {
            return null;
        }
        int currentExpression = 0;
        if (expressions.get(currentExpression) instanceof LogicalSortExpression) {
            currentExpression++;
        }
        if (expressions.size() > currentExpression && expressions.get(currentExpression) instanceof SelectExpression) {
            if (expressions.size() > (currentExpression + 1) && expressions.get(currentExpression + 1) instanceof GroupByExpression) {
                // the above select-having must not contain any predicate.
                Assert.thatUnchecked(((SelectExpression) expressions.get(currentExpression)).getPredicates().isEmpty(), ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, found predicate in select-having");
                currentExpression++; // group-by expression.
                Assert.thatUnchecked(expressions.size() > currentExpression);
                currentExpression++; // select-where.
            }
        }
        // current expression is either top-level select, or select-where or top-level group by.
        // make sure any other select statement does not have any predicates defined.
        for (int i = currentExpression + 1; i < expressions.size(); i++) {
            if (expressions.get(i) instanceof SelectExpression) {
                final var innerSelect = (SelectExpression) expressions.get(i);
                Assert.thatUnchecked(innerSelect.getPredicates().isEmpty(), ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, found predicate in inner-select");
            }
        }
        final var predicates = ((SelectExpression) expressions.get(currentExpression)).getPredicates().stream().map(QueryPredicate::toResidualPredicate).collect(toList());
        // todo (yhatem) make sure we through if the generated DNF does not meet the deserialization requirements.
        if (predicates.isEmpty()) {
            return null;
        }
        final var conjunction = predicates.size() == 1 ? predicates.get(0) : AndPredicate.and(predicates);
        final var result = BooleanPredicateNormalizer.getDefaultInstanceForDnf().normalize(conjunction, false).orElse(conjunction);
        Assert.thatUnchecked(IndexPredicate.isSupported(result), ErrorCode.UNSUPPORTED_OPERATION, () -> String.format("Unsupported predicate '%s'", result))    ;
        if (IndexPredicateExpansion.dnfPredicateToRanges(result).isEmpty()) {
            return conjunction;
        }
        return result;
    }

    private static final class AnnotatedAccessor extends FieldValue.ResolvedAccessor {

        private final int marker;

        private AnnotatedAccessor(@Nonnull Type fieldType,
                                  @Nullable String fieldName,
                                  int fieldOrdinal,
                                  int marker) {
            super(fieldName, fieldOrdinal, fieldType);
            this.marker = marker;
        }

        @Nonnull
        public static AnnotatedAccessor of(@Nonnull FieldValue.ResolvedAccessor resolvedAccessor, int marker) {
            return new AnnotatedAccessor(resolvedAccessor.getType(), resolvedAccessor.getName(), resolvedAccessor.getOrdinal(), marker);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            AnnotatedAccessor that = (AnnotatedAccessor) o;
            return marker == that.marker;
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), marker);
        }
    }

    private void collectQuantifiers(@Nonnull RelationalExpression relationalExpression) {
        AtomicInteger counter = new AtomicInteger(0);
        collectQuantifiersInternal(relationalExpression, counter);
    }

    private void collectQuantifiersInternal(@Nonnull RelationalExpression relationalExpression, @Nonnull AtomicInteger explodeCounter) {
        for (final var qun : relationalExpression.getQuantifiers()) {
            if (qun.getRangesOver().get() instanceof ExplodeExpression) {
                explodeCounter.incrementAndGet();
                final var collectionValue = ((ExplodeExpression) qun.getRangesOver().get()).getCollectionValue();
                if (collectionValue instanceof FieldValue) {
                    final var field = (FieldValue) collectionValue;
                    final var fieldAccessors = new ArrayList<>(field.getFieldPath().getFieldAccessors());
                    fieldAccessors.set(fieldAccessors.size() - 1, AnnotatedAccessor.of(fieldAccessors.get(fieldAccessors.size() - 1), explodeCounter.get()));
                    correlatedKeyExpressions.put(qun.getAlias(), FieldValue.ofFields(((FieldValue) collectionValue).getChild(), new FieldValue.FieldPath(fieldAccessors)));
                } else {
                    correlatedKeyExpressions.put(qun.getAlias(), collectionValue);
                }
            } else {
                correlatedKeyExpressions.put(qun.getAlias(), qun.getRangesOver().get().getResultValue());
            }
            collectQuantifiersInternal(qun.getRangesOver().get(), explodeCounter);
        }
    }

    @Nonnull
    private Value dereference(@Nonnull Value value) {
        if (value instanceof RecordConstructorValue) {
            return RecordConstructorValue.ofColumns(
                    ((RecordConstructorValue) value).getColumns()
                            .stream()
                            .map(c -> Column.of(c.getField(), dereference(c.getValue())))
                            .collect(toList()));
        } else if (value instanceof CountValue) {
            final var children = StreamSupport.stream(value.getChildren().spliterator(), false).collect(toList());
            Verify.verify(children.size() <= 1);
            if (!children.isEmpty()) {
                return value.withChildren(Collections.singleton(dereference(children.get(0))));
            } else {
                return value;
            }
        } else if (value instanceof FieldValue || value instanceof IndexableAggregateValue) {
            final var valueWithChild = (ValueWithChild) value;
            return valueWithChild.withNewChild(dereference(valueWithChild.getChild()));
        } else if (value instanceof QuantifiedObjectValue) {
            return dereference(correlatedKeyExpressions.get(value.getCorrelatedTo().stream().findFirst().orElseThrow()));
        } else if (value instanceof ArithmeticValue) {
            final List<Value> newChildren = new ArrayList<>();
            for (Value v:value.getChildren()) {
                newChildren.add(dereference(v));
            }
            return ((ArithmeticValue) value).withChildren(newChildren);
        } else {
            return value;
        }
    }

    @Nonnull
    private KeyExpression toKeyExpression(@Nonnull List<Pair<String, Type>> fields) {
        return toKeyExpression(fields, 0);
    }

    @Nonnull
    private KeyExpression toKeyExpression(@Nonnull List<Pair<String, Type>> fields, int index) {
        Assert.thatUnchecked(!fields.isEmpty());
        final var field = fields.get(index);
        final var keyExpression = toKeyExpression(field.getLeft(), field.getRight());
        if (index + 1 < fields.size()) {
            return keyExpression.nest(toKeyExpression(fields, index + 1));
        }
        return keyExpression;
    }

    @Nonnull
    public String getRecordTypeName() {
        final var expressionRefs = relationalExpressions.stream()
                .filter(r -> r instanceof LogicalTypeFilterExpression)
                .map(r -> (LogicalTypeFilterExpression) r)
                .collect(toList());
        Assert.thatUnchecked(expressionRefs.size() == 1, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported query, expected to find exactly one type filter operator");
        final var recordTypes = expressionRefs.get(0).getRecordTypes();
        Assert.thatUnchecked(recordTypes.size() == 1, ErrorCode.UNSUPPORTED_OPERATION, () -> String.format("Unsupported query, expected to find exactly one record type in type filter operator, however found %s", recordTypes.isEmpty() ? "nothing" : String.join(",", recordTypes)));
        return recordTypes.stream().findFirst().orElseThrow();
    }

    @Nonnull
    private static FieldKeyExpression toKeyExpression(@Nonnull String name, @Nonnull Type type) {
        Assert.notNullUnchecked(name);
        final var fanType = type.getTypeCode() == Type.TypeCode.ARRAY ?
                KeyExpression.FanType.FanOut :
                KeyExpression.FanType.None;
        return field(name, fanType);
    }

    @Nonnull
    public static IndexGenerator from(@Nonnull RelationalExpression relationalExpression, boolean useLongBasedExtremumEver) {
        return new IndexGenerator(relationalExpression, useLongBasedExtremumEver);
    }
}
