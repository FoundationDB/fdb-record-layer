/*
 * IndexGenerator.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query.ddl;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.FunctionNames;
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
import com.apple.foundationdb.record.query.plan.cascades.typing.PseudoField;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.CardinalityValue;
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
import com.apple.foundationdb.record.query.plan.planning.BooleanPredicateNormalizer;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSyntheticTable;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerUnnestedSyntheticTable;
import com.apple.foundationdb.relational.recordlayer.query.FieldValueTrieNode;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.NullableArrayUtils;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
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
import java.util.LinkedHashMap;
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
import static com.apple.foundationdb.record.query.plan.cascades.properties.ReferencesAndDependenciesProperty.referencesAndDependencies;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * Generates a {@link KeyExpression} from a given query plan.
 */
@SuppressWarnings({"PMD.TooManyStaticImports", "OptionalUsedAsFieldOrParameterType"})
@API(API.Status.EXPERIMENTAL)
public final class MaterializedViewIndexGenerator {

    private static final String UNNESTED_TABLE_NAME_PREFIX = "__unnested_";
    private static final String BITMAP_BIT_POSITION = "bitmap_bit_position";
    private static final String BITMAP_BUCKET_OFFSET = "bitmap_bucket_offset";

    /**
     * Map from each correlation in the query plan to its list of results.
     */
    @Nonnull
    private final IdentityHashMap<CorrelationIdentifier, Value> correlatedKeyExpressions = new IdentityHashMap<>();

    /**
     * Unnestings discovered while collecting quantifiers, keyed by the {@link AnnotatedAccessor} marker. One marker
     * identifies exactly one explode, so this is a plain map; whether an unnesting becomes a constituent of the
     * unnested synthetic table or a fan-out is {@link UnnestingInfo#structArray()}.
     */
    @Nonnull
    private final Map<Integer, UnnestingInfo> unnestings = new LinkedHashMap<>();

    @Nonnull
    private final List<RelationalExpression> relationalExpressions;

    @Nonnull
    private final RelationalExpression relationalExpression;

    private final boolean useLegacyBasedExtremumEver;

    @Nonnull
    private Optional<RecordLayerSyntheticTable.Builder> syntheticTableBuilder = Optional.empty();

    private MaterializedViewIndexGenerator(@Nonnull RelationalExpression relationalExpression, boolean useLegacyBasedExtremumEver) {
        collectQuantifiers(relationalExpression);
        final var partialOrder = referencesAndDependencies().evaluate(Reference.initialOf(relationalExpression));
        relationalExpressions =
                TopologicalSort.anyTopologicalOrderPermutation(partialOrder)
                        .orElseThrow(() -> new RelationalException("graph has cycles", ErrorCode.UNSUPPORTED_OPERATION).toUncheckedWrappedException())
                        .stream()
                        .map(Reference::get)
                        .collect(toList());
        this.relationalExpression = relationalExpression;
        this.useLegacyBasedExtremumEver = useLegacyBasedExtremumEver;
    }

    /**
     * Generates the index definition, and for an unnesting over a struct array, the unnested synthetic table
     * to define it on. Callers must register the returned table, if present, otherwise the index would name a
     * type that does not exist.
     *
     * @param schemaTemplateBuilder the schema template being built
     * @param indexName the name of the index
     * @param isUnique whether the index is unique
     * @param containsNullableArray whether the schema contains any nullable array
     * @param generateKeyValueExpressionWithEmptyKey whether to generate a key-with-value expression
     *        even when there is no ordering
     * @return the index definition and, when the plan unnests a struct array, the unnested synthetic table
     */
    @Nonnull
    public IndexGenerationResult generate(@Nonnull RecordLayerSchemaTemplate.Builder schemaTemplateBuilder, @Nonnull String indexName,
                                     boolean isUnique, boolean containsNullableArray, boolean generateKeyValueExpressionWithEmptyKey) {
        final String recordTypeName = getRecordTypeName();
        // Have to use the storage name here because the index generator uses it
        final Type.Record tableType = schemaTemplateBuilder.findTableByStorageName(recordTypeName).getType();
        final var indexBuilder = RecordLayerIndex.newBuilder()
                .setName(indexName)
                .setUnique(isUnique);

        collectQuantifiers(relationalExpression);

        final var partialOrder = referencesAndDependencies().evaluate(Reference.initialOf(relationalExpression));
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
                () -> String.format(Locale.ROOT, "Unsupported aggregate index definition containing non-indexable aggregation (%s), consider using a value index on the aggregated column instead.", unsupportedAggregates.stream().map(Objects::toString).collect(joining(","))));

        Assert.thatUnchecked(simplifiedValues.stream().allMatch(sv -> sv instanceof FieldValue || sv instanceof IndexableAggregateValue || sv instanceof ArithmeticValue || sv instanceof CardinalityValue));
        final var aggregateValues = simplifiedValues.stream().filter(sv -> sv instanceof IndexableAggregateValue).collect(toList());
        final var fieldValues = simplifiedValues.stream().filter(sv -> !(sv instanceof IndexableAggregateValue)).collect(toList());
        final var versionValues = simplifiedValues.stream().filter(sv -> sv instanceof FieldValue && sv.getResultType().equals(PseudoField.ROW_VERSION.getType())).collect(toList());
        Assert.thatUnchecked(versionValues.size() <= 1, ErrorCode.UNSUPPORTED_OPERATION, "Cannot have index with more than one version column");
        final Map<Value, String> orderingFunctions = new IdentityHashMap<>();
        final var orderByValues = getOrderByValues(relationalExpression, orderingFunctions);
        if (aggregateValues.isEmpty()) {
            indexBuilder.setIndexType(versionValues.isEmpty() ? IndexTypes.VALUE : IndexTypes.VERSION);
            Assert.thatUnchecked(orderByValues.stream().allMatch(sv -> sv instanceof FieldValue || sv instanceof ArithmeticValue || sv instanceof CardinalityValue), ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, order by must be a subset of projection list");
            if (fieldValues.size() > 1) {
                Assert.thatUnchecked(!orderByValues.isEmpty(), ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, value indexes must have an order by clause at the top level");
            }
            final var reordered = reorderValues(fieldValues, orderByValues);
            var splitPoint = orderByValues.size();
            if (orderByValues.isEmpty() && !generateKeyValueExpressionWithEmptyKey) {
                splitPoint = -1;
            }
            validateScalarUnnestings(reordered);
            final boolean useUnnestedSyntheticTable = requiresUnnestedSyntheticTable(reordered);
            // A predicate would have to be evaluated against the synthetic record rather than the stored one,
            // which is not worked out yet. Rejected rather than falling back to a fan-out, which cannot express
            // these shapes and so would fail later with a less clear error.
            Assert.thatUnchecked(!useUnnestedSyntheticTable || predicate == null, ErrorCode.UNSUPPORTED_OPERATION,
                    "Unsupported index definition, a predicate is not supported on an index over an unnested synthetic table");
            KeyExpression keyExpression;
            if (useUnnestedSyntheticTable) {
                final String syntheticTableName = UNNESTED_TABLE_NAME_PREFIX + recordTypeName + "_" + indexName;
                indexBuilder
                        .setTableName(syntheticTableName)
                        .setTableStorageName(syntheticTableName);
                final String parentConstituentAlias = findParentConstituentAlias();
                syntheticTableBuilder = Optional.of(buildUnnestedSyntheticTable(
                        schemaTemplateBuilder, syntheticTableName, recordTypeName, parentConstituentAlias));
                // For unnested synthetic tables the key expression uses constituent-alias paths
                // (e.g. field("SQ").nest(field("a"))) rather than stored-table field paths.
                // Build it directly from the dereferenced FieldValues.
                final KeyExpression fullExpr = buildConstituentKeyExpression(
                        reordered, orderingFunctions, parentConstituentAlias);
                keyExpression = splitPoint != -1 && splitPoint < fieldValues.size()
                                ? keyWithValue(fullExpr, splitPoint)
                                : fullExpr;
            } else {
                indexBuilder.setTableType(tableType);
                final var expression = generate(reordered, orderingFunctions);
                final var unwrappedKeyExpression = splitPoint != -1 && splitPoint < fieldValues.size() ?
                                                   keyWithValue(expression, splitPoint) : expression;
                keyExpression = KeyExpression.fromProto(NullableArrayUtils.wrapArray(
                        unwrappedKeyExpression.toKeyExpression(), tableType, containsNullableArray));
            }
            indexBuilder.setKeyExpression(keyExpression);
        } else {
            Assert.thatUnchecked(aggregateValues.size() == 1, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, multiple group by aggregations found");
            indexBuilder.setTableType(tableType);
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
            if (IndexTypes.PERMUTED_MIN.equals(indexType) || IndexTypes.PERMUTED_MAX.equals(indexType)) {
                int permutedSize = aggregateOrderIndex < 0 ? 0 : (fieldValues.size() - aggregateOrderIndex);
                indexBuilder.setOption(IndexOptions.PERMUTED_SIZE_OPTION, permutedSize);
            } else if (aggregateOrderIndex > 0) {
                Assert.failUnchecked(ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition. Cannot order " + indexType + " index by aggregate value");
            }
        }
        return new IndexGenerationResult(indexBuilder, this.syntheticTableBuilder);
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
                    final var simplifiedGroupingValues =
                            Values.deconstructRecord(groupingValues).stream().map(this::dereference)
                                    .map(v -> v.simplify(EvaluationContext.empty(), AliasMap.emptyMap(),
                                            Set.of()));
                    return Stream.concat(adjustResultValues.stream(), simplifiedGroupingValues).collect(toList());
                }
            } else {
                // Make sure the grouping values and the result values are consistent
                if (groupingValues == null) {
                    // This shouldn't happen unless there's more than one indexable aggregate value
                    Assert.failUnchecked(ErrorCode.UNSUPPORTED_OPERATION, "Grouping values absent from aggregate result value");
                }
                final var simplifiedGroupingValues =
                        Values.deconstructRecord(groupingValues).stream()
                                .map(this::dereference)
                                .map(v -> v.simplify(EvaluationContext.empty(), AliasMap.emptyMap(),
                                        Set.of())).iterator();
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
                .map(v -> v.simplify(EvaluationContext.empty(), AliasMap.emptyMap(), Set.of()))
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
                                .simplify(EvaluationContext.empty(), AliasMap.emptyMap(), Set.of());
                        values.add(rebased);
                        if (orderingFunction != null) {
                            orderingFunctions.put(rebased, orderingFunction);
                        }
                    }
                } else {
                    final Value rebased = dereference(orderingPart.getValue().rebase(reverseAliasMap))
                            .simplify(EvaluationContext.empty(), AliasMap.emptyMap(), Set.of());
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
        if (aggregateValue instanceof CountValue && IndexTypes.COUNT.equals(indexTypeName)) {
            if (maybeGroupingExpression.isPresent()) {
                keyExpression = new GroupingKeyExpression(maybeGroupingExpression.get(), 0);
            } else {
                keyExpression = new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0);
            }
        } else if (aggregateValue instanceof NumericAggregationValue.BitmapConstructAgg && IndexTypes.BITMAP_VALUE.equals(indexTypeName)) {
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
        if (IndexTypes.MAX_EVER.equals(indexTypeName)) {
            if (useLegacyBasedExtremumEver) {
                final var indexValue = Iterables.getOnlyElement(indexableAggregateValue.getChildren());
                Verify.verify(indexValue.getResultType().isNumeric(), "only numeric types allowed in " + IndexTypes.MAX_EVER_LONG + " aggregation operation");
                indexTypeName = IndexTypes.MAX_EVER_LONG;
            } else {
                indexTypeName = IndexTypes.MAX_EVER_TUPLE;
            }
        } else if (IndexTypes.MIN_EVER.equals(indexTypeName)) {
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

    /**
     * Build the key expression representing the arguments of a {@link FunctionKeyExpression}.
     */
    @Nonnull
    private static KeyExpression buildArgumentKeyExpression(List<KeyExpression> argumentList) {
        if (argumentList.isEmpty()) {
            return empty();
        } else if (argumentList.size() == 1) {
            return argumentList.get(0);
        } else {
            return concat(argumentList);
        }
    }

    @Nonnull
    private KeyExpression toKeyExpression(@Nonnull Value value) {
        if (value instanceof FieldValue) {
            final FieldValue fieldValue = (FieldValue) value;
            return toKeyExpression(fieldValue.getFieldPath().getFieldAccessors().iterator(), KeyExpression.FanType.FanOut);
        } else if (value instanceof CardinalityValue) {
            // CARDINALITY() consumes an array value. Currently, it can only be applied to a `field` directly. We make
            // sure here that the field gets accessed with fan-out type `Concatenate` instead of `FanOut` so that it
            // produces the materialized array.
            final var it = value.getChildren().iterator();
            Assert.thatUnchecked(it.hasNext(), "Invalid children list for `CardinalityValue`");
            final Value childValue = it.next();
            Assert.thatUnchecked(!it.hasNext(), "Invalid children list for `CardinalityValue`");
            Assert.thatUnchecked(childValue instanceof FieldValue, "CARDINALITY() must be applied to a `field()` in an index key expression.");
            final var fieldValue = (FieldValue)childValue;
            final KeyExpression childKeyExpression = toKeyExpression(fieldValue.getFieldPath().getFieldAccessors().iterator(), KeyExpression.FanType.Concatenate);
            return function(FunctionNames.CARDINALITY, childKeyExpression);
        } else if (value instanceof ArithmeticValue) {
            var children = value.getChildren();
            var builder = ImmutableList.<KeyExpression>builder();
            for (Value child : children) {
                builder.add(toKeyExpression(child));
            }
            KeyExpression argumentExpr = buildArgumentKeyExpression(builder.build());
            final String name = ((ArithmeticValue)value).getLogicalOperator().name().toLowerCase(Locale.ROOT);
            return function(name, argumentExpr);
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
        final var exprConstituents = childrenMap.entrySet().stream().map(nodeEntry -> {
            final FieldValue.ResolvedAccessor accessor = nodeEntry.getKey();
            final FieldValueTrieNode node = nodeEntry.getValue();
            final KeyExpression expr = toFieldKeyExpression(accessor, KeyExpression.FanType.FanOut);
            if (node.getChildrenMap() != null) {
                final FieldKeyExpression fieldExpr = Assert.castUnchecked(expr, FieldKeyExpression.class);
                return fieldExpr.nest(toKeyExpression(node, orderingFunctions));
            } else if (orderingFunctions.containsKey(node.getValue())) {
                return function(orderingFunctions.get(node.getValue()), expr);
            } else {
                return expr;
            }
        }).collect(toList());
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

        // Result values of each operation must be record-typed. `ExplodeExpression` is excluded because
        // unnesting a scalar array (e.g., a STRING ARRAY) produces scalar elements, not records.
        final var allRecordValues = expressions.stream()
                .filter(r -> !(r instanceof ExplodeExpression))
                .allMatch(r -> (r.getResultValue().getResultType().getTypeCode() == Type.TypeCode.RECORD));
        Assert.thatUnchecked(allRecordValues, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, some operators return non-record values");

        // Fields of result values of each record-typed operation must be simple or arithmetic values.
        final var allSimpleValues = expressions.stream()
                .filter(r -> r.getResultType().getInnerType() instanceof Type.Record)
                .allMatch(r -> Values.deconstructRecord(r.getResultValue()).stream().allMatch(v -> v instanceof FieldValue || v instanceof QuantifiedObjectValue || v instanceof AggregateValue || v instanceof ArithmeticValue || v instanceof LiteralValue || v instanceof CardinalityValue));
        Assert.thatUnchecked(allSimpleValues, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported index definition, not all fields can be mapped to key expression in");
    }

    @Nullable
    public static QueryPredicate getTopLevelPredicate(@Nonnull List<? extends RelationalExpression> expressions) {
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
        final var expr = expressions.get(currentExpression);
        if (!(expr instanceof SelectExpression)) {
            return null;
        }
        final var predicates = ((SelectExpression) expr).getPredicates().stream().map(QueryPredicate::toResidualPredicate).collect(toList());
        // todo (yhatem) make sure we through if the generated DNF does not meet the deserialization requirements.
        if (predicates.isEmpty()) {
            return null;
        }
        final var conjunction = predicates.size() == 1 ? predicates.get(0) : AndPredicate.and(predicates);
        final var result = BooleanPredicateNormalizer.getDefaultInstanceForDnf().normalize(conjunction, false).orElse(conjunction);
        Assert.thatUnchecked(IndexPredicate.isSupported(result), ErrorCode.UNSUPPORTED_OPERATION, () -> String.format(Locale.ROOT, "Unsupported predicate '%s'", result))    ;
        if (IndexPredicateExpansion.dnfPredicateToRanges(result).isEmpty()) {
            return conjunction;
        }
        return result;
    }

    private static final class AnnotatedAccessor extends FieldValue.ResolvedAccessor {

        private final int marker;

        private AnnotatedAccessor(@Nonnull Type.Record.Field field,
                                  int ordinal,
                                  int marker) {
            super(field, ordinal);
            this.marker = marker;
        }

        int getMarker() {
            return marker;
        }

        @Nonnull
        public static AnnotatedAccessor of(@Nonnull FieldValue.ResolvedAccessor resolvedAccessor, int marker) {
            return new AnnotatedAccessor(resolvedAccessor.getField(), resolvedAccessor.getOrdinal(), marker);
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
                if (collectionValue instanceof final FieldValue field) {
                    final var fieldAccessors = new ArrayList<>(field.getFieldPath().getFieldAccessors());
                    fieldAccessors.set(fieldAccessors.size() - 1, AnnotatedAccessor.of(fieldAccessors.get(fieldAccessors.size() - 1), explodeCounter.get()));
                    correlatedKeyExpressions.put(qun.getAlias(), FieldValue.ofFields(field.getChild(), new FieldValue.FieldPath(fieldAccessors)));
                    final var accessors = field.getFieldPath().getFieldAccessors();
                    final String arrayFieldStorageName =
                            accessors.get(accessors.size() - 1).getField().getFieldStorageName();
                    final String owningAlias = resolveOwningAlias(field);
                    final var arrayType = (Type.Array) field.getFieldPath().getLastFieldType();
                    unnestings.put(explodeCounter.get(),
                            new UnnestingInfo(qun.getAlias().toString(), owningAlias, arrayFieldStorageName,
                                    arrayType.isNullable(), arrayType.getElementType() instanceof Type.Record));
                } else {
                    correlatedKeyExpressions.put(qun.getAlias(), collectionValue);
                }
            } else {
                correlatedKeyExpressions.put(qun.getAlias(), qun.getRangesOver().get().getResultValue());
            }
            collectQuantifiersInternal(qun.getRangesOver().get(), explodeCounter);
        }
    }

    /**
     * Returns the alias of the constituent that owns the array an explode reads from. Dereferences the collection
     * value and takes the innermost enclosing unnesting, identified by an {@link AnnotatedAccessor} in the resulting
     * path; when there is none, the array hangs off the stored record and the value's own correlation is returned.
     *
     * @param collectionValue the array a newly seen explode ranges over
     * @return the alias of the owning constituent
     */
    @Nonnull
    private String resolveOwningAlias(@Nonnull final FieldValue collectionValue) {
        final var markers = unnestingMarkers(dereference(collectionValue));
        for (int i = markers.size() - 1; i >= 0; i--) {
            final UnnestingInfo enclosing = unnestings.get(markers.get(i));
            if (enclosing != null && enclosing.structArray()) {
                return enclosing.alias();
            }
        }
        return Iterables.getOnlyElement(collectionValue.getCorrelatedTo()).toString();
    }

    /**
     * One unnesting, as discovered from an {@link ExplodeExpression} during {@link #collectQuantifiers}.
     *
     * <p>A struct array becomes a constituent of the unnested synthetic table, navigated by {@link #arrayElements()} from
     * {@code owningAlias}. A scalar array cannot be a constituent, since its elements have no fields to reference,
     * so the same expression is instead emitted as a fan-out inside the owning constituent.
     */
    private record UnnestingInfo(@Nonnull String alias,
                                 @Nonnull String owningAlias,
                                 @Nonnull String arrayFieldStorageName,
                                 boolean nullableArray,
                                 boolean structArray) {

        @Nonnull
        KeyExpression arrayElements() {
            return NullableArrayUtils.arrayElements(arrayFieldStorageName, nullableArray);
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
    private KeyExpression toKeyExpression(@Nonnull Iterator<FieldValue.ResolvedAccessor> resolvedAccessors, KeyExpression.FanType fanTypeForArray) {
        Assert.thatUnchecked(resolvedAccessors.hasNext(), "cannot resolve empty list");
        final FieldValue.ResolvedAccessor accessor = resolvedAccessors.next();
        final KeyExpression expression = toFieldKeyExpression(accessor, fanTypeForArray);
        if (resolvedAccessors.hasNext()) {
            KeyExpression childExpression = toKeyExpression(resolvedAccessors, fanTypeForArray);
            final FieldKeyExpression fieldExpression = Assert.castUnchecked(expression, FieldKeyExpression.class);
            return fieldExpression.nest(childExpression);
        } else {
            return expression;
        }
    }

    @Nonnull
    private String getRecordTypeName() {
        final var expressionRefs = relationalExpressions.stream()
                .filter(r -> r instanceof LogicalTypeFilterExpression)
                .map(r -> (LogicalTypeFilterExpression) r)
                .collect(toList());
        Assert.thatUnchecked(expressionRefs.size() == 1, ErrorCode.UNSUPPORTED_OPERATION, "Unsupported query, expected to find exactly one type filter operator");
        final var recordTypes = expressionRefs.get(0).getRecordTypes();
        Assert.thatUnchecked(recordTypes.size() == 1, ErrorCode.UNSUPPORTED_OPERATION, () -> String.format(Locale.ROOT, "Unsupported query, expected to find exactly one record type in type filter operator, however found %s", recordTypes.isEmpty() ? "nothing" : String.join(",", recordTypes)));
        return recordTypes.stream().findFirst().orElseThrow();
    }

    /**
     * Return a {@link FieldKeyExpression} or {@link VersionKeyExpression} for the given field type, as appropriate.
     *
     * @param fanTypeForArray The fan-out type to use for the {@code field} key expression in case the field is an
     * ARRAY. This should be either {@code FanOut} or {@code Concatenate}.
     */
    @Nonnull
    private static KeyExpression toFieldKeyExpression(@Nonnull FieldValue.ResolvedAccessor accessor, KeyExpression.FanType fanTypeForArray) {
        final Type.Record.Field fieldType = accessor.getField();
        Assert.notNullUnchecked(fieldType.getFieldStorageName());
        Assert.thatUnchecked(fanTypeForArray == KeyExpression.FanType.FanOut || fanTypeForArray == KeyExpression.FanType.Concatenate);
        Assert.thatUnchecked(!fieldType.getFieldType().isArray()
                        || (accessor instanceof AnnotatedAccessor)
                        || (fanTypeForArray == KeyExpression.FanType.Concatenate),
                ErrorCode.UNSUPPORTED_OPERATION,
                "Unsupported index definition, cannot create index on array field '" +
                fieldType.getFieldName() + "' without unnesting");
        final Type type = fieldType.getFieldType();
        if (PseudoField.ROW_VERSION.getType().equals(type) && PseudoField.ROW_VERSION.getFieldName().equals(fieldType.getFieldName())) {
            return VersionKeyExpression.VERSION;
        }
        final var fanType = type.isArray() ? fanTypeForArray : KeyExpression.FanType.None;
        // Here we need to use the storage field name, as that will be the name referenced in Protobuf storage.
        return field(fieldType.getFieldStorageName(), fanType);
    }

    @Nonnull
    public static MaterializedViewIndexGenerator from(@Nonnull RelationalExpression relationalExpression, boolean useLongBasedExtremumEver) {
        return new MaterializedViewIndexGenerator(relationalExpression, useLongBasedExtremumEver);
    }

    /**
     * Finds the correlation alias of the parent (stored record) constituent of an unnested index.
     * The parent is the quantifier that ranges over the stored record scan, which in an index
     * definition plan is always wrapped in a {@link LogicalTypeFilterExpression}. All other
     * quantifiers of the select range over {@link ExplodeExpression}s (the unnested constituents).
     */
    @Nonnull
    private String findParentConstituentAlias() {
        return relationalExpressions.stream()
                .filter(expression -> expression instanceof SelectExpression)
                .flatMap(expression -> expression.getQuantifiers().stream())
                .filter(quantifier -> quantifier.getRangesOver().get() instanceof LogicalTypeFilterExpression)
                .map(quantifier -> quantifier.getAlias().toString())
                .findFirst()
                .orElseThrow(() -> Assert.failUnchecked(ErrorCode.UNSUPPORTED_OPERATION,
                        "Could not determine parent constituent alias in unnested index definition"));
    }

    @Nonnull
    private RecordLayerUnnestedSyntheticTable.Builder buildUnnestedSyntheticTable(
            @Nonnull RecordLayerSchemaTemplate.Builder schemaTemplateBuilder,
            @Nonnull String syntheticTableName,
            @Nonnull String recordTypeName,
            @Nonnull String parentAlias) {
        final RecordLayerUnnestedSyntheticTable.Builder builder = RecordLayerUnnestedSyntheticTable.newBuilder()
                .setName(syntheticTableName)
                .setAlias(parentAlias)
                .setParentTableType(schemaTemplateBuilder.findTableByStorageName(recordTypeName).getType());
        unnestings.values().stream().filter(UnnestingInfo::structArray).forEach(info ->
                builder.addConstituent(new RecordLayerUnnestedSyntheticTable.NestedConstituent(
                        info.alias(), info.owningAlias(), info.arrayElements())));
        return builder;
    }

    /**
     * Builds the index key for an unnested synthetic table, rewriting the {@link AnnotatedAccessor}-marked paths of
     * the dereferenced values into constituent-alias paths: {@code field("SQ").nest(field("a"))} for unnested
     * fields, {@code field("row").nest(...)} for parent fields. Values are emitted positionally, preserving the
     * order of {@code reordered}; a constituent is navigated with {@link KeyExpression.FanType#None}, so it may be
     * referenced repeatedly and non-adjacently without duplicating entries.
     */
    @Nonnull
    private KeyExpression buildConstituentKeyExpression(
            @Nonnull List<Value> reordered,
            @Nonnull Map<Value, String> orderingFunctions,
            @Nonnull String parentAlias) {
        final List<KeyExpression> parts = new ArrayList<>(reordered.size());
        for (final Value value : reordered) {
            final KeyExpression base = toKeyExpressionOnNestedConstituent(value, parentAlias);
            final String orderingFunctionName = orderingFunctions.get(value);
            parts.add(orderingFunctionName != null ? function(orderingFunctionName, base) : base);
        }
        return parts.size() == 1 ? parts.get(0) : concat(parts);
    }

    /**
     * Returns the markers of every unnestings in a value transitively.
     * For {@code FROM A AS a, (SELECT * FROM a.p) AS b, (SELECT * FROM b.q) AS c} the value {@code c.y} dereferences
     * to {@code [ann(P), ann(Q), Y]} and so traverses both unnestings, while {@code b.x} dereferences to
     * {@code [ann(P), X]} and traverses only the outer one.
     *
     * @param value the dereferenced value
     * @return the markers of the unnestings traversed, outermost first, empty if there are none
     */
    @Nonnull
    private static List<Integer> unnestingMarkers(@Nonnull final Value value) {
        final var markers = ImmutableList.<Integer>builder();
        if (value instanceof FieldValue fieldValue) {
            for (final FieldValue.ResolvedAccessor accessor : fieldValue.getFieldPath().getFieldAccessors()) {
                if (accessor instanceof AnnotatedAccessor annotatedAccessor) {
                    markers.add(annotatedAccessor.getMarker());
                }
            }
        }
        for (final Value child : value.getChildren()) {
            markers.addAll(unnestingMarkers(child));
        }
        return markers.build();
    }

    /**
     * Rejects an index whose key reads through the same scalar unnesting at more than one position. A scalar array
     * cannot be a constituent of an unnested synthetic table, so each reference is emitted as its own fan-out over
     * the array;
     * two of them would range over it independently and yield a cross-product of one view column against itself.
     * The stored-table path already rejects this shape, so the check applies to both representations.
     *
     * @param keyValues the index key columns, in key order
     */
    private void validateScalarUnnestings(@Nonnull final List<Value> keyValues) {
        final Map<Integer, Integer> scalarPositionCounts = new LinkedHashMap<>();
        for (final Value keyValue : keyValues) {
            for (final Integer marker : ImmutableSet.copyOf(unnestingMarkers(keyValue))) {
                final UnnestingInfo info = unnestings.get(marker);
                if (info == null || !info.structArray()) {
                    scalarPositionCounts.merge(marker, 1, Integer::sum);
                }
            }
        }
        Assert.thatUnchecked(scalarPositionCounts.values().stream().allMatch(count -> count == 1),
                ErrorCode.UNSUPPORTED_OPERATION,
                "Unsupported index definition, a scalar array cannot be referenced at more than one index key position");
    }

    /**
     * Returns whether this index has to be defined on an {@link RecordLayerUnnestedSyntheticTable} rather than on the
     * stored table with a fan-out key expression. A fan-out suffices while every column read through one unnesting
     * sits in a contiguous run of the index key; two or more columns reached through the same unnesting at
     * non-adjacent positions require an unnested synthetic table, whose constituents are navigated with
     * {@link KeyExpression.FanType#None} and so may be referenced at any number of key positions.
     *
     * <p>Every unnesting a column is read through counts, not only the innermost, so chained unnesting can require
     * one even when no innermost unnesting is itself split. Only struct arrays are considered, since a scalar array
     * cannot be a constituent.
     *
     * @param keyValues the index key columns, in key order
     * @return whether an unnested synthetic table is required
     */
    private boolean requiresUnnestedSyntheticTable(@Nonnull final List<Value> keyValues) {
        final Map<Integer, Integer> firstPositions = new LinkedHashMap<>();
        final Map<Integer, Integer> lastPositions = new LinkedHashMap<>();
        final Map<Integer, Integer> counts = new LinkedHashMap<>();
        for (int i = 0; i < keyValues.size(); i++) {
            // Distinct markers per position: the counts below must be a number of key positions, and one value
            // can read through the same unnesting more than once (e.g. `M.x + M.y`).
            for (final Integer marker : ImmutableSet.copyOf(unnestingMarkers(keyValues.get(i)))) {
                // Skip scalar arrays, which cannot be constituents.
                final UnnestingInfo info = unnestings.get(marker);
                if (info == null || !info.structArray()) {
                    continue;
                }
                firstPositions.putIfAbsent(marker, i);
                lastPositions.put(marker, i);
                counts.merge(marker, 1, Integer::sum);
            }
        }
        return counts.entrySet().stream().anyMatch(entry ->
                lastPositions.get(entry.getKey()) - firstPositions.get(entry.getKey()) + 1 != entry.getValue());
    }

    /**
     * Translates a single dereferenced {@link FieldValue} into a constituent-alias key expression.
     * {@link AnnotatedAccessor}s in the path mark unnesting boundaries.
     *
     * <p>The <em>last</em> such accessor is the one that matters: it identifies the innermost
     * unnesting, and therefore the constituent the value actually lives in. Any outer unnestings in
     * the path are already accounted for by that constituent's parent chain, so re-emitting them
     * here would fan out a second time. For {@code FROM T AS r, r.a AS x, x.b AS y} the value
     * {@code y.c} dereferences to {@code [ann(A), ann(B), C]} and must become
     * {@code field("y").nest(field("C"))}, not {@code field("x").nest(field("B", FanOut).nest(field("C")))}.
     */
    @Nonnull
    private KeyExpression toKeyExpressionOnNestedConstituent(@Nonnull Value value, @Nonnull String parentAlias) {
        if (!(value instanceof FieldValue fieldValue)) {
            throw Assert.failUnchecked(ErrorCode.UNSUPPORTED_OPERATION,
                    "Unsupported index definition, an index over an unnested synthetic table supports only plain column references");
        }
        final var accessors = fieldValue.getFieldPath().getFieldAccessors();
        if (accessors.isEmpty()) {
            return field(parentAlias, KeyExpression.FanType.None);
        }
        // for `FROM T AS r, r.a AS x, x.b AS y`, the value y.c dereferences to [ann(A), ann(B), C],
        // ann(B) that says where c lives.
        int innermostUnnestingIdx = -1;
        for (int i = accessors.size() - 1; i >= 0; i--) {
            if (accessors.get(i) instanceof AnnotatedAccessor) {
                innermostUnnestingIdx = i;
                break;
            }
        }
        if (innermostUnnestingIdx < 0) {
            return field(parentAlias, KeyExpression.FanType.None)
                    .nest(toKeyExpression(accessors.iterator(), KeyExpression.FanType.FanOut));
        }
        final int marker = ((AnnotatedAccessor) accessors.get(innermostUnnestingIdx)).getMarker();
        final var remaining = accessors.subList(innermostUnnestingIdx + 1, accessors.size());
        final UnnestingInfo info = unnestings.get(marker);
        Assert.notNullUnchecked(info, "unknown unnesting in index definition");
        if (info.structArray()) {
            final FieldKeyExpression constituent = field(info.alias(), KeyExpression.FanType.None);
            return remaining.isEmpty()
                   ? constituent
                   : constituent.nest(toKeyExpression(remaining.iterator(), KeyExpression.FanType.FanOut));
        }
        // Scalar array, not a constituent. Fan out over the array field within the
        // constituent that owns it. Scalar elements have no sub-fields, so nothing remains.
        Assert.thatUnchecked(remaining.isEmpty(), ErrorCode.UNSUPPORTED_OPERATION,
                "Unsupported index definition, cannot dereference a field of a scalar array element");
        return field(info.owningAlias(), KeyExpression.FanType.None)
                .nest(info.arrayElements());
    }
}
