/*
 * ValueToKeyExpressionVisitor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FunctionKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import com.apple.foundationdb.record.query.plan.cascades.typing.PseudoField;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.CardinalityValue;
import com.apple.foundationdb.record.query.plan.cascades.values.CountValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.IndexOnlyAggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.IndexableAggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NumericAggregationValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.SimpleValueVisitor;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.Values;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.query.FieldValueTrieNode;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.empty;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;

/**
 * Translates the result value of an index-defining select into the {@link KeyExpression} the index key is built from, and
 * reports the type of index that can store it.
 * <p>
 * A supported kind of value gets a visitation method; {@link #evaluateAtValue} rejects the rest. The grouping of an
 * aggregate index is everything in the projection other than the single aggregate. Sort direction comes from the caller,
 * as an ordering function per column.
 * </p>
 */
final class ValueToKeyExpressionVisitor implements SimpleValueVisitor<KeyExpression> {

    private static final String BITMAP_BIT_POSITION = "bitmap_bit_position";
    private static final String BITMAP_BUCKET_OFFSET = "bitmap_bucket_offset";

    /**
     * The ordering function each column is wrapped in, keyed by identity on the values being translated.
     */
    @Nonnull
    private final Map<Value, String> orderingFunctions;

    /**
     * Which form {@code MIN_EVER} and {@code MAX_EVER} are stored in.
     */
    @Nonnull
    private final ExtremumEverStorage extremumEverStorage;

    /**
     * The translated grouping columns, or {@code null} when the projection holds none. Set by
     * {@link #visitRecordConstructorValue} and read by the aggregate it descends into.
     */
    @Nullable
    private KeyExpression groupingExpression;

    /**
     * The index type the projection calls for: a value index, unless an aggregate or the version pseudo-column says
     * otherwise.
     */
    @Nonnull
    private String indexType = IndexTypes.VALUE;

    private ValueToKeyExpressionVisitor(@Nonnull final Map<Value, String> orderingFunctions,
                                        @Nonnull final ExtremumEverStorage extremumEverStorage) {
        this.orderingFunctions = orderingFunctions;
        this.extremumEverStorage = extremumEverStorage;
    }

    //
    // Entry point.
    //

    /**
     * Translates the result value of an index-defining select to the corresponding key expression and index type.
     *
     * @param value the result value of the select
     * @param orderingFunctions the ordering function per column, keyed by identity on the columns of {@code value}
     * @param extremumEverStorage which form an extremum-ever aggregate is stored in
     *
     * @return the key expression and the index type
     */
    @Nonnull
    public static Result translate(@Nonnull final Value value,
                                   @Nonnull final Map<Value, String> orderingFunctions,
                                   @Nonnull final ExtremumEverStorage extremumEverStorage) {
        final var visitor = new ValueToKeyExpressionVisitor(orderingFunctions, extremumEverStorage);
        return new Result(Objects.requireNonNull(value.acceptVisitor(visitor)), visitor.indexType);
    }

    /**
     * What a translation produced.
     *
     * @param keyExpression the key the index is built from
     * @param indexType the type of index that can store it
     */
    public record Result(@Nonnull KeyExpression keyExpression, @Nonnull String indexType) {
    }

    //
    // Visitation. A record is the projection; every other kind of value is one column of the key.
    //

    @Nonnull
    @Override
    public KeyExpression visitRecordConstructorValue(@Nonnull final RecordConstructorValue element) {
        final var columns = Values.deconstructRecord(element);
        final var aggregates = columns.stream()
                .filter(IndexableAggregateValue.class::isInstance)
                .collect(ImmutableList.toImmutableList());
        Assert.thatUnchecked(aggregates.size() <= 1, ErrorCode.UNSUPPORTED_OPERATION,
                "Unsupported index definition, multiple group by aggregations found");
        if (aggregates.isEmpty()) {
            return combine(columns);
        }
        final var groupingColumns = columns.stream()
                .filter(column -> !(column instanceof IndexableAggregateValue))
                .collect(ImmutableList.toImmutableList());
        groupingExpression = groupingColumns.isEmpty() ? null : combine(groupingColumns);
        return visit(aggregates.get(0));
    }

    @Nonnull
    @Override
    public KeyExpression visitFieldValue(@Nonnull final FieldValue fieldValue) {
        return fieldPathToKeyExpression(fieldValue, KeyExpression.FanType.FanOut);
    }

    @Nonnull
    @Override
    public KeyExpression visitCardinalityValue(@Nonnull final CardinalityValue element) {
        // CARDINALITY() applies to an array field, accessed with fan type Concatenate to materialize it
        final var child = Iterables.getOnlyElement(element.getChildren());
        Assert.thatUnchecked(child instanceof FieldValue,
                "CARDINALITY() must be applied to a `field()` in an index key expression.");
        return function(FunctionNames.CARDINALITY,
                fieldPathToKeyExpression((FieldValue)child, KeyExpression.FanType.Concatenate));
    }

    @Nonnull
    @Override
    public KeyExpression visitArithmeticValue(@Nonnull final ArithmeticValue element) {
        return function(element.getLogicalOperator().name().toLowerCase(Locale.ROOT),
                arguments(visitChildren(element)));
    }

    @Nonnull
    @Override
    public KeyExpression visitLiteralValue(@Nonnull final LiteralValue<?> element) {
        return Key.Expressions.value(element.getLiteralValue());
    }

    // An aggregate yields a GroupingKeyExpression and records the index type it implies.

    @Nonnull
    @Override
    public KeyExpression visitCountValue(@Nonnull final CountValue element) {
        if (!IndexTypes.COUNT.equals(element.getIndexTypeName())) {
            return groupedAggregate(element);
        }
        // COUNT(*) counts records rather than a column, so there is nothing to group
        indexType = indexTypeOf(element);
        return new GroupingKeyExpression(groupingExpression == null ? EmptyKeyExpression.EMPTY : groupingExpression, 0);
    }

    @Nonnull
    @Override
    public KeyExpression visitSum(@Nonnull final NumericAggregationValue.Sum element) {
        return groupedAggregate(element);
    }

    @Nonnull
    @Override
    public KeyExpression visitMin(@Nonnull final NumericAggregationValue.Min element) {
        return groupedAggregate(element);
    }

    @Nonnull
    @Override
    public KeyExpression visitMax(@Nonnull final NumericAggregationValue.Max element) {
        return groupedAggregate(element);
    }

    @Nonnull
    @Override
    public KeyExpression visitMinEverValue(@Nonnull final IndexOnlyAggregateValue.MinEverValue element) {
        return groupedAggregate(element);
    }

    @Nonnull
    @Override
    public KeyExpression visitMaxEverValue(@Nonnull final IndexOnlyAggregateValue.MaxEverValue element) {
        return groupedAggregate(element);
    }

    @Nonnull
    @Override
    public KeyExpression visitBitmapConstructAgg(@Nonnull final NumericAggregationValue.BitmapConstructAgg element) {
        if (!IndexTypes.BITMAP_VALUE.equals(element.getIndexTypeName())) {
            return groupedAggregate(element);
        }
        indexType = indexTypeOf(element);
        final var child = Iterables.getOnlyElement(element.getChildren());
        Assert.thatUnchecked(child instanceof FieldValue || child instanceof ArithmeticValue,
                "Unsupported index definition, expecting a column argument in aggregation function");
        final var groupedValue = visit(child);
        // only bitmap_construct_agg(bitmap_bit_position(column)) is supported
        Assert.thatUnchecked(groupedValue instanceof FunctionKeyExpression
                        && BITMAP_BIT_POSITION.equals(((FunctionKeyExpression)groupedValue).getName()),
                "Unsupported index definition, expecting a bitmap_bit_position function in bitmap_construct_agg function");
        if (groupingExpression == null) {
            throw Assert.failUnchecked("Unsupported index definition, unexpected grouping expression " + groupedValue);
        }
        // a bitmap index implies the bucket offset, so it is not part of the grouping
        final var grouping = removeBitmapBucketOffset(groupingExpression);
        final var arguments = (ThenKeyExpression)((FunctionKeyExpression)groupedValue).getArguments();
        final var groupedColumn = (FieldKeyExpression)arguments.getChildren().get(0);
        return grouping == null ? groupedColumn.ungrouped() : groupedColumn.groupBy(grouping);
    }

    @Nonnull
    @Override
    public KeyExpression evaluateAtValue(@Nonnull final Value value, @Nonnull final List<KeyExpression> childResults) {
        // any kind of value with no visitation method of its own
        throw new RelationalException("unable to construct expression", ErrorCode.UNSUPPORTED_OPERATION).toUncheckedWrappedException();
    }

    //
    // Combining columns.
    //

    /**
     * Combines sibling columns into one key expression. Adjacent {@link FieldValue}s nest under their shared prefix:
     * {@code r.s.a, r.s.b} becomes {@code field("R").nest(concat(A, B))}.
     */
    @Nonnull
    private KeyExpression combine(@Nonnull final List<Value> values) {
        if (values.isEmpty()) {
            return EmptyKeyExpression.EMPTY;
        }
        if (values.size() == 1) {
            return ordered(values.get(0));
        }
        // a run of adjacent field values forms one component; any other value forms its own
        final List<FieldValueTrieNode> tries = new ArrayList<>(values.size());
        final List<KeyExpression> components = new ArrayList<>(values.size());
        final PeekingIterator<Value> valueIterator = Iterators.peekingIterator(values.iterator());
        while (valueIterator.hasNext()) {
            components.add(valueIterator.peek() instanceof FieldValue
                           ? nextFieldPaths(valueIterator, tries)
                           : ordered(valueIterator.next()));
        }
        return concatOf(components);
    }

    /**
     * Takes the run of adjacent field values the iterator is positioned on and nests them under their shared prefixes.
     *
     * @param tries the tries of the runs before this one; the new trie is validated against them and joins them
     */
    @Nonnull
    private KeyExpression nextFieldPaths(@Nonnull final PeekingIterator<Value> valueIterator,
                                         @Nonnull final List<FieldValueTrieNode> tries) {
        final var trie = FieldValueTrieNode.computeTrieForValues(FieldValue.FieldPath.empty(), valueIterator);
        trie.validateNoOverlaps(tries);
        tries.add(trie);
        return trieToKeyExpression(trie);
    }

    @Nonnull
    private KeyExpression trieToKeyExpression(@Nonnull final FieldValueTrieNode trie) {
        final var childrenMap = Assert.notNullUnchecked(trie.getChildrenMap());
        Assert.thatUnchecked(!childrenMap.isEmpty());
        final var components = childrenMap.entrySet().stream().map(entry -> {
            final var node = entry.getValue();
            final var expression = fieldAccessorToKeyExpression(entry.getKey(), KeyExpression.FanType.FanOut);
            if (node.getChildrenMap() != null) {
                return Assert.castUnchecked(expression, FieldKeyExpression.class).nest(trieToKeyExpression(node));
            }
            return withOrderingFunction(node.getValue(), expression);
        }).collect(ImmutableList.toImmutableList());
        return concatOf(components);
    }

    @Nonnull
    private static KeyExpression concatOf(@Nonnull final List<KeyExpression> components) {
        return components.size() == 1 ? components.get(0) : concat(components);
    }

    @Nonnull
    private KeyExpression ordered(@Nonnull final Value column) {
        return withOrderingFunction(column, Objects.requireNonNull(visit(column)));
    }

    @Nonnull
    private KeyExpression withOrderingFunction(@Nonnull final Value column, @Nonnull final KeyExpression expression) {
        final var orderingFunction = orderingFunctions.get(column);
        return orderingFunction == null ? expression : function(orderingFunction, expression);
    }

    //
    // Field paths.
    //

    @Nonnull
    private KeyExpression fieldPathToKeyExpression(@Nonnull final FieldValue fieldValue,
                                                   @Nonnull final KeyExpression.FanType fanTypeForArray) {
        return fieldPathToKeyExpression(fieldValue.getFieldPath().getFieldAccessors().iterator(), fanTypeForArray);
    }

    @Nonnull
    private KeyExpression fieldPathToKeyExpression(@Nonnull final Iterator<FieldValue.ResolvedAccessor> accessors,
                                                   @Nonnull final KeyExpression.FanType fanTypeForArray) {
        Assert.thatUnchecked(accessors.hasNext(), "cannot resolve empty list");
        final var expression = fieldAccessorToKeyExpression(accessors.next(), fanTypeForArray);
        if (!accessors.hasNext()) {
            return expression;
        }
        return Assert.castUnchecked(expression, FieldKeyExpression.class)
                .nest(fieldPathToKeyExpression(accessors, fanTypeForArray));
    }

    /**
     * One step of a field path, as a {@link FieldKeyExpression} or, for the version pseudo-column, a
     * {@link VersionKeyExpression}. Storing the version makes the index a version index.
     *
     * @param fanTypeForArray the fan type to use if the field is an ARRAY, either {@code FanOut} or {@code Concatenate}
     */
    @Nonnull
    private KeyExpression fieldAccessorToKeyExpression(@Nonnull final FieldValue.ResolvedAccessor accessor,
                                                       @Nonnull final KeyExpression.FanType fanTypeForArray) {
        Assert.thatUnchecked(fanTypeForArray == KeyExpression.FanType.FanOut
                             || fanTypeForArray == KeyExpression.FanType.Concatenate);
        final var recordField = accessor.getField();
        if (isRowVersion(recordField)) {
            indexType = IndexTypes.VERSION;
            return VersionKeyExpression.VERSION;
        }
        // Protobuf storage references the storage name
        final var storageName = Assert.notNullUnchecked(recordField.getFieldStorageName());
        if (!recordField.getFieldType().isArray()) {
            return field(storageName, KeyExpression.FanType.None);
        }
        // an array is indexable only through an unnest, which tags its accessor, or materialized whole
        Assert.thatUnchecked(accessor instanceof QuantifierValues.AnnotatedAccessor
                        || fanTypeForArray == KeyExpression.FanType.Concatenate,
                ErrorCode.UNSUPPORTED_OPERATION,
                "Unsupported index definition, cannot create index on array field '"
                        + recordField.getFieldName() + "' without unnesting");
        return field(storageName, fanTypeForArray);
    }

    private static boolean isRowVersion(@Nonnull final Type.Record.Field recordField) {
        return PseudoField.ROW_VERSION.getType().equals(recordField.getFieldType())
                && PseudoField.ROW_VERSION.getFieldName().equals(recordField.getFieldName());
    }

    @Nonnull
    private static KeyExpression arguments(@Nonnull final List<KeyExpression> argumentList) {
        return argumentList.isEmpty() ? empty() : concatOf(argumentList);
    }

    //
    // Aggregates.
    //

    /**
     * The shape every aggregate but {@code COUNT(*)} and {@code BITMAP_CONSTRUCT_AGG} takes: the aggregated column,
     * grouped by the grouping columns, or ungrouped if there are none.
     */
    @Nonnull
    private KeyExpression groupedAggregate(@Nonnull final IndexableAggregateValue aggregateValue) {
        indexType = indexTypeOf(aggregateValue);
        final var child = Iterables.getOnlyElement(aggregateValue.getChildren());
        Assert.thatUnchecked(child instanceof FieldValue,
                "Unsupported index definition, expecting a column argument in aggregation function");
        final var groupedValue = visit(child);
        Assert.thatUnchecked(groupedValue instanceof FieldKeyExpression || groupedValue instanceof ThenKeyExpression);
        if (groupedValue instanceof final FieldKeyExpression field) {
            return groupingExpression == null ? field.ungrouped() : field.groupBy(groupingExpression);
        }
        final var then = (ThenKeyExpression)groupedValue;
        return groupingExpression == null ? then.ungrouped() : then.groupBy(groupingExpression);
    }

    /**
     * The type of index that can maintain an aggregate: its own, except for {@code MIN_EVER} and {@code MAX_EVER}, whose
     * storage form the caller chooses.
     */
    @Nonnull
    @SuppressWarnings("deprecation")
    private String indexTypeOf(@Nonnull final IndexableAggregateValue aggregateValue) {
        final var indexTypeName = aggregateValue.getIndexTypeName();
        if (IndexTypes.MIN_EVER.equals(indexTypeName)) {
            return extremumEverIndexType(extremumEverStorage.minEverIndexType(), aggregateValue);
        }
        if (IndexTypes.MAX_EVER.equals(indexTypeName)) {
            return extremumEverIndexType(extremumEverStorage.maxEverIndexType(), aggregateValue);
        }
        return indexTypeName;
    }

    /**
     * The extremum-ever index type, checked against the aggregated column.
     */
    @Nonnull
    private String extremumEverIndexType(@Nonnull final String indexType,
                                         @Nonnull final IndexableAggregateValue aggregateValue) {
        Verify.verify(!extremumEverStorage.isNumericOnly()
                        || Iterables.getOnlyElement(aggregateValue.getChildren()).getResultType().isNumeric(),
                "only numeric types allowed in " + indexType + " aggregation operation");
        return indexType;
    }

    /**
     * Removes {@code bitmap_bucket_offset(col)} from the grouping expression, which looks like
     * {@code [*, bitmap_bucket_offset(C)]} and so is either a Then or a Function.
     *
     * @return {@code null} if the grouping expression contained nothing else
     */
    @Nullable
    private static KeyExpression removeBitmapBucketOffset(@Nonnull final KeyExpression groupingExpression) {
        Assert.thatUnchecked(groupingExpression instanceof ThenKeyExpression
                        || groupingExpression instanceof FunctionKeyExpression,
                "Unsupported index definition, expecting column or function arguments in group by");
        if (groupingExpression instanceof FunctionKeyExpression) {
            return BITMAP_BUCKET_OFFSET.equals(((FunctionKeyExpression)groupingExpression).getName())
                   ? null : groupingExpression;
        }
        final List<KeyExpression> children = ((ThenKeyExpression)groupingExpression).getChildren();
        final var last = children.get(children.size() - 1);
        Assert.thatUnchecked(last instanceof FunctionKeyExpression
                        && BITMAP_BUCKET_OFFSET.equals(((FunctionKeyExpression)last).getName()),
                "Unsupported index definition, expecting the last element in group by to be a bitmap_bucket_offset function");
        // a ThenKeyExpression has at least two children
        return children.size() >= 3 ? new ThenKeyExpression(children, 0, children.size() - 1) : children.get(0);
    }
}
