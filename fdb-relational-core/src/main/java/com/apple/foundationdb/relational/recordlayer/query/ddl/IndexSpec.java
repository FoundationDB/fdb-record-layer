/*
 * IndexSpec.java
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

import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.OrderingPart.RequestedSortOrder;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.SimpleExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.ExplodeExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.FullUnorderedScanExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalTypeFilterExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.SelectExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.PseudoField;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.CardinalityValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.IndexableAggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.StreamableAggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.Values;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * Everything an index is made of, as read off the plan of the query that defines it: the record type, the predicate, the
 * group by, and the projection and ordering, both resolved down to the base record. {@link #collect} gathers it in one
 * bottom-up pass, stating each rule at the node it concerns; {@link #checkValidity} rejects the plans that cannot become
 * an index.
 */
record IndexSpec(int scanCount, @Nullable String recordTypeName, @Nullable QueryPredicate predicate,
                        @Nullable GroupByExpression groupBy, @Nullable OrderBy orderBy,
                        @Nullable Projection projection) {

    /**
     * Collects what the plan of an index-defining query is making.
     *
     * @param expression the root of the plan
     * @param quantifierValues what the plan's quantifiers stand for, from the preceding pass
     *
     * @return what the index is made of
     */
    @Nonnull
    public static IndexSpec collect(@Nonnull final RelationalExpression expression,
                                    @Nonnull final QuantifierValues quantifierValues) {
        final var visitor = new Visitor(quantifierValues);
        final var indexSpec = Assert.notNullUnchecked(visitor.visit(expression));
        // the projection belongs to the root, which the bottom-up traversal cannot single out
        return indexSpec.withProjection(new ProjectionResolver(quantifierValues)
                .resolve(expression.getResultValue(), indexSpec.groupBy()));
    }

    @Override
    @Nonnull
    public String recordTypeName() {
        return Assert.notNullUnchecked(recordTypeName, ErrorCode.UNSUPPORTED_OPERATION,
                "Unsupported query, expected to find exactly one type filter operator");
    }

    /**
     * The columns the index is ordered by, resolved down to the base record, empty when the definition had no
     * {@code ORDER BY}.
     */
    @Nonnull
    public List<Value> getOrderByValues() {
        return orderBy == null ? ImmutableList.of() : orderBy.values();
    }

    /**
     * The ordering function each order-by column is wrapped in, keyed by identity on {@link #getOrderByValues()}.
     */
    @Nonnull
    public Map<Value, String> getOrderingFunctions() {
        return orderBy == null ? ImmutableMap.of() : orderBy.orderingFunctions();
    }

    /**
     * The projection the index is defined over, resolved down to the base record.
     */
    @Override
    @Nonnull
    public Projection projection() {
        return Assert.notNullUnchecked(projection);
    }

    @Nonnull
    private IndexSpec withProjection(@Nonnull final Projection newProjection) {
        return new IndexSpec(scanCount, recordTypeName, predicate, groupBy, orderBy, newProjection);
    }

    @Nonnull
    private IndexSpec withOrderBy(@Nonnull final OrderBy newOrderBy) {
        Assert.thatUnchecked(orderBy == null, ErrorCode.UNSUPPORTED_OPERATION,
                "Unsupported index definition, more than one sort expression found");
        return new IndexSpec(scanCount, recordTypeName, predicate, groupBy, newOrderBy, projection);
    }

    @Nonnull
    private IndexSpec withScan() {
        return new IndexSpec(scanCount + 1, recordTypeName, predicate, groupBy, orderBy, projection);
    }

    @Nonnull
    private IndexSpec withRecordTypeName(@Nonnull final String newRecordTypeName) {
        Assert.thatUnchecked(recordTypeName == null, ErrorCode.UNSUPPORTED_OPERATION,
                "Unsupported query, expected to find exactly one type filter operator");
        return new IndexSpec(scanCount, newRecordTypeName, predicate, groupBy, orderBy, projection);
    }

    @Nonnull
    private IndexSpec withPredicate(@Nonnull final QueryPredicate newPredicate) {
        return new IndexSpec(scanCount, recordTypeName, newPredicate, groupBy, orderBy, projection);
    }

    @Nonnull
    private IndexSpec withGroupBy(@Nonnull final GroupByExpression newGroupBy) {
        Assert.thatUnchecked(groupBy == null, ErrorCode.UNSUPPORTED_OPERATION,
                "Unsupported index definition, multiple group by expressions found");
        return new IndexSpec(scanCount, recordTypeName, predicate, newGroupBy, orderBy, projection);
    }

    /**
     * Rejects every definition the generator cannot turn into an index, apart from two: the predicate, checked as it is
     * collected, and ordering by the aggregate, checked once the index type is known.
     */
    public void checkValidity() {
        // the traversal rejects a second scan as a join, leaving none to reject here
        Assert.thatUnchecked(scanCount == 1, ErrorCode.UNSUPPORTED_OPERATION,
                "Unsupported index definition, no iteration generator found");
        // throws unless exactly one type filter was found
        recordTypeName();

        final var projection = projection();
        reject(projection.values().stream()
                        .filter(value -> value instanceof StreamableAggregateValue && !(value instanceof IndexableAggregateValue))
                        .collect(toList()),
                "Unsupported aggregate index definition containing non-indexable aggregation (%s), consider using a value index on the aggregated column instead.");
        reject(projection.values().stream()
                        .filter(value -> !isTranslatableColumn(value))
                        .collect(toList()),
                "Unsupported index definition, cannot map %s to a key expression");
        Assert.thatUnchecked(projection.versionValues().size() <= 1, ErrorCode.UNSUPPORTED_OPERATION,
                "Cannot have index with more than one version column");

        if (projection.aggregate() == null) {
            Assert.thatUnchecked(getOrderByValues().stream().allMatch(value -> isTranslatableColumn(value)
                            && !(value instanceof IndexableAggregateValue)),
                    ErrorCode.UNSUPPORTED_OPERATION,
                    "Unsupported index definition, order by must be a subset of projection list");
            if (projection.fieldValues().size() > 1) {
                Assert.thatUnchecked(!getOrderByValues().isEmpty(), ErrorCode.UNSUPPORTED_OPERATION,
                        "Unsupported index definition, value indexes must have an order by clause at the top level");
            }
        } else {
            // rejects a covering aggregate index
            aggregateOrderIndex();
        }
    }

    /**
     * Fails with {@code message}, naming the offending columns, unless there are none.
     */
    private static void reject(@Nonnull final List<Value> offendingColumns, @Nonnull final String message) {
        Assert.thatUnchecked(offendingColumns.isEmpty(), ErrorCode.UNSUPPORTED_OPERATION,
                () -> String.format(Locale.ROOT, message,
                        offendingColumns.stream().map(Object::toString).collect(joining(","))));
    }

    private static boolean isTranslatableColumn(@Nonnull final Value value) {
        return value instanceof FieldValue
                || value instanceof IndexableAggregateValue
                || value instanceof ArithmeticValue
                || value instanceof CardinalityValue;
    }

    /**
     * Where the aggregate appears in the ordering. Walking the ordering also establishes that the rest of it is the
     * grouping columns in their key order; anything else is a covering aggregate index, which cannot be stored.
     *
     * @return the position of the aggregate among the order-by columns, or {@code -1} if it does not appear
     */
    public int aggregateOrderIndex() {
        final var orderByValues = getOrderByValues();
        if (orderByValues.isEmpty()) {
            return -1;
        }
        final var aggregate = projection().aggregate();
        final var fieldIterator = projection().fieldValues().iterator();
        var aggregateOrderIndex = -1;
        var inOrder = true;
        for (int i = 0; i < orderByValues.size(); i++) {
            final var value = orderByValues.get(i);
            if (value.equals(aggregate)) {
                Assert.thatUnchecked(aggregateOrderIndex < 0, ErrorCode.UNSUPPORTED_OPERATION,
                        "Unsupported index definition, aggregate can appear only once in ordering clause");
                aggregateOrderIndex = i;
            } else if (fieldIterator.hasNext()) {
                if (!value.equals(fieldIterator.next())) {
                    inOrder = false;
                    break;
                }
            } else {
                inOrder = false;
                break;
            }
        }
        Assert.thatUnchecked(inOrder && !fieldIterator.hasNext(), ErrorCode.UNSUPPORTED_OPERATION,
                "Unsupported index definition, attempt to create a covering aggregate index");
        return aggregateOrderIndex;
    }

    /**
     * Combines what the children of one expression found. At most one child may contribute a record type, a predicate, a
     * group by or a scan; a second scan is a join.
     */
    @Nonnull
    private static IndexSpec merge(@Nonnull final List<IndexSpec> childSpecs) {
        var merged = new IndexSpec(0, null, null, null, null, null);
        for (final var childSpec : childSpecs) {
            // the record type comes first: a join trips this before the scan below, which is the message callers see
            final var recordTypeName = pickOneRecordTypeName(merged.recordTypeName, childSpec.recordTypeName);
            Assert.thatUnchecked(merged.scanCount == 0 || childSpec.scanCount == 0,
                    ErrorCode.UNSUPPORTED_OPERATION,
                    "Unsupported index definition, join indexes are not supported");
            merged = new IndexSpec(merged.scanCount + childSpec.scanCount,
                    recordTypeName,
                    pickOne(merged.predicate, childSpec.predicate, "predicate"),
                    pickOne(merged.groupBy, childSpec.groupBy, "group by expression"),
                    pickOne(merged.orderBy, childSpec.orderBy, "sort expression"), null);
        }
        return merged;
    }

    @Nullable
    private static String pickOneRecordTypeName(@Nullable final String left, @Nullable final String right) {
        Assert.thatUnchecked(left == null || right == null, ErrorCode.UNSUPPORTED_OPERATION,
                "Unsupported query, expected to find exactly one type filter operator");
        return left == null ? right : left;
    }

    @Nullable
    private static <T> T pickOne(@Nullable final T left, @Nullable final T right, @Nonnull final String what) {
        Assert.thatUnchecked(left == null || right == null, ErrorCode.UNSUPPORTED_OPERATION,
                () -> String.format(Locale.ROOT, "Unsupported index definition, more than one %s found", what));
        return left == null ? right : left;
    }

    /**
     * The columns the index is ordered by, in order and resolved down to the base record.
     *
     * @param values the columns, in the order they are sorted by
     * @param orderingFunctions the ordering function per column, keyed by identity so that a column appearing twice keeps
     * a direction per occurrence
     */
    public record OrderBy(@Nonnull List<Value> values, @Nonnull Map<Value, String> orderingFunctions) {
    }

    /**
     * The columns of the projection, resolved down to the base record. The aggregate and the rest are views over the one
     * list rather than fields of their own.
     *
     * @param values the columns, in the order the definition projects them
     */
    public record Projection(@Nonnull List<Value> values) {

        /**
         * The aggregate the index is over, or {@code null} if it is not an aggregate index. There is at most one.
         */
        @Nullable
        public IndexableAggregateValue aggregate() {
            return values.stream()
                    .filter(IndexableAggregateValue.class::isInstance)
                    .map(IndexableAggregateValue.class::cast)
                    .findFirst().orElse(null);
        }

        /**
         * Everything that is not the aggregate: the columns of a value index key, or the grouping columns.
         */
        @Nonnull
        public List<Value> fieldValues() {
            return values.stream()
                    .filter(value -> !(value instanceof IndexableAggregateValue))
                    .collect(ImmutableList.toImmutableList());
        }

        @Nonnull
        private List<Value> versionValues() {
            return values.stream()
                    .filter(value -> value instanceof FieldValue
                            && value.getResultType().equals(PseudoField.ROW_VERSION.getType()))
                    .collect(ImmutableList.toImmutableList());
        }
    }

    /**
     * The traversal. Each override visits its children, then applies what the node contributes; the checks every node
     * shares live in {@link #evaluateAtExpression}.
     */
    private record Visitor(@Nonnull QuantifierValues quantifierValues) implements SimpleExpressionVisitor<IndexSpec> {

        @Nonnull
        @Override
        public IndexSpec evaluateAtExpression(@Nonnull final RelationalExpression expression,
                                              @Nonnull final List<IndexSpec> childResults) {
            checkResultValue(expression);
            return IndexSpec.merge(childResults);
        }

        @Nonnull
        @Override
        public IndexSpec evaluateAtRef(@Nonnull final Reference ref, @Nonnull final List<IndexSpec> memberResults) {
            return IndexSpec.merge(memberResults);
        }

        @Nonnull
        @Override
        public IndexSpec visitFullUnorderedScanExpression(@Nonnull final FullUnorderedScanExpression expression) {
            return evaluateAtExpression(expression, visitQuantifiers(expression)).withScan();
        }

        @Nonnull
        @Override
        public IndexSpec visitLogicalTypeFilterExpression(@Nonnull final LogicalTypeFilterExpression expression) {
            final var recordTypes = expression.getRecordTypes();
            Assert.thatUnchecked(recordTypes.size() == 1, ErrorCode.UNSUPPORTED_OPERATION,
                    () -> String.format(Locale.ROOT,
                            "Unsupported query, expected to find exactly one record type in type filter operator, however found %s",
                            recordTypes.isEmpty() ? "nothing" : String.join(",", recordTypes)));
            return evaluateAtExpression(expression, visitQuantifiers(expression))
                    .withRecordTypeName(recordTypes.stream().findFirst().orElseThrow());
        }

        @Nonnull
        @Override
        public IndexSpec visitGroupByExpression(@Nonnull final GroupByExpression expression) {
            Assert.thatUnchecked(Values.deconstructRecord(expression.getAggregateValue()).size() <= 1,
                    ErrorCode.UNSUPPORTED_OPERATION,
                    "Unsupported index definition, found group by expression with more than one aggregation");
            return evaluateAtExpression(expression, visitQuantifiers(expression)).withGroupBy(expression);
        }

        /**
         * The innermost select owns the predicate; any select above a group by or above a select that already owns one
         * has to be predicate-free.
         */
        @Nonnull
        @Override
        public IndexSpec visitSelectExpression(@Nonnull final SelectExpression expression) {
            final var spec = evaluateAtExpression(expression, visitQuantifiers(expression));
            final var predicates = ImmutableList.copyOf(expression.getPredicates());
            if (spec.groupBy() != null || spec.predicate() != null) {
                Assert.thatUnchecked(predicates.isEmpty(), ErrorCode.UNSUPPORTED_OPERATION,
                        spec.groupBy() != null
                        ? "Unsupported index definition, found predicate in select-having"
                        : "Unsupported index definition, found predicate in inner-select");
                return spec;
            }
            if (predicates.isEmpty()) {
                return spec;
            }
            return spec.withPredicate(IndexPredicates.normalize(predicates));
        }

        @Nonnull
        @Override
        public IndexSpec visitLogicalSortExpression(@Nonnull final LogicalSortExpression expression) {
            return evaluateAtExpression(expression, visitQuantifiers(expression)).withOrderBy(orderByOf(expression));
        }

        /**
         * An explode yields elements rather than records, so it skips the record-typed result check.
         */
        @Nonnull
        @Override
        public IndexSpec visitExplodeExpression(@Nonnull final ExplodeExpression expression) {
            return IndexSpec.merge(visitQuantifiers(expression));
        }

        private static void checkResultValue(@Nonnull final RelationalExpression expression) {
            Assert.thatUnchecked(expression.getResultValue().getResultType().getTypeCode() == Type.TypeCode.RECORD,
                    ErrorCode.UNSUPPORTED_OPERATION,
                    () -> String.format(Locale.ROOT,
                            "Unsupported index definition, operator %s returns a non-record value",
                            expression.getClass().getSimpleName()));
            if (expression.getResultType().getInnerType() instanceof Type.Record) {
                Assert.thatUnchecked(Values.deconstructRecord(expression.getResultValue())
                                .stream()
                                .allMatch(Visitor::isSupportedResultValue),
                        ErrorCode.UNSUPPORTED_OPERATION,
                        () -> String.format(Locale.ROOT,
                                "Unsupported index definition, not all fields can be mapped to key expression in %s",
                                expression.getClass().getSimpleName()));
            }
        }

        private static boolean isSupportedResultValue(@Nonnull final Value value) {
            return value instanceof FieldValue
                    || value instanceof QuantifiedObjectValue
                    || value instanceof AggregateValue
                    || value instanceof ArithmeticValue
                    || value instanceof LiteralValue
                    || value instanceof CardinalityValue;
        }

        /**
         * Flattens the ordering into one column per key component, each resolved down to the base record. An ordering part
         * over a record contributes each of its fields, all with that part's direction.
         */
        @Nonnull
        private OrderBy orderByOf(@Nonnull final LogicalSortExpression expression) {
            final var aliasMap = AliasMap.ofAliases(Quantifier.current(), expression.getQuantifiers().get(0).getAlias());
            final ImmutableList.Builder<Value> values = ImmutableList.builder();
            final Map<Value, String> orderingFunctions = new IdentityHashMap<>();
            for (final var orderingPart : expression.getOrdering().getOrderingParts()) {
                final var orderingFunction = orderingFunctionOf(orderingPart.getSortOrder());
                final var partValue = orderingPart.getValue().rebase(aliasMap);
                final List<? extends Value> columns =
                        partValue.getResultType().getTypeCode() == Type.TypeCode.RECORD
                        ? Values.deconstructRecord(partValue)
                        : ImmutableList.of(partValue);
                for (final var column : columns) {
                    final var value = quantifierValues.resolve(column);
                    values.add(value);
                    if (orderingFunction != null) {
                        orderingFunctions.put(value, orderingFunction);
                    }
                }
            }
            return new OrderBy(values.build(), orderingFunctions);
        }

        /**
         * The ordering function a column is wrapped in to express its sort order, or {@code null} for plain ascending.
         * Every sort order is named, so a new one is a compile error here.
         */
        @Nullable
        private static String orderingFunctionOf(@Nonnull final RequestedSortOrder sortOrder) {
            return switch (sortOrder) {
                case ASCENDING -> null;
                case DESCENDING -> "order_desc_nulls_last";
                case ASCENDING_NULLS_LAST -> "order_asc_nulls_last";
                case DESCENDING_NULLS_FIRST -> "order_desc_nulls_first";
                case ANY -> throw Assert.failUnchecked(ErrorCode.UNSUPPORTED_OPERATION,
                        "Unsupported index definition, an index key needs a definite sort order");
            };
        }
    }
}
