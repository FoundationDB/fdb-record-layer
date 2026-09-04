/*
 * ProjectionResolver.java
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

import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.GroupByExpression;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.IndexableAggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.Values;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

/**
 * Turns the result value of an index-defining select into the columns of the index, resolved down to the base record. A
 * definition projecting nothing but its aggregate has the grouping columns added; one that projects them is checked to
 * project exactly them, in order.
 */
final class ProjectionResolver {

    @Nonnull
    private final QuantifierValues quantifierValues;

    ProjectionResolver(@Nonnull final QuantifierValues quantifierValues) {
        this.quantifierValues = quantifierValues;
    }

    @Nonnull
    IndexSpec.Projection resolve(@Nonnull final Value resultValue, @Nullable final GroupByExpression groupBy) {
        final var resultValues = resolveEach(resultValue).collect(toList());
        if (groupBy == null) {
            return new IndexSpec.Projection(resultValues);
        }
        final var groupingValue = groupBy.getGroupingValue();
        final var adjusted = adjustGroupByFieldPaths(resultValues, groupBy);
        if (resultValues.size() != 1 || !(resultValues.get(0) instanceof IndexableAggregateValue)) {
            checkGroupingColumnsProjected(resultValues, groupingValue);
            return new IndexSpec.Projection(adjusted);
        }
        if (groupingValue == null) {
            return new IndexSpec.Projection(adjusted);
        }
        return new IndexSpec.Projection(Stream.concat(adjusted.stream(), resolveEach(groupingValue)).collect(toList()));
    }

    /**
     * Checks that the columns beside the aggregate are exactly the grouping columns, in the order they are grouped by.
     */
    private void checkGroupingColumnsProjected(@Nonnull final List<Value> resultValues,
                                               @Nullable final Value groupingValue) {
        final var grouping = Assert.notNullUnchecked(groupingValue, ErrorCode.UNSUPPORTED_OPERATION,
                "Grouping values absent from aggregate result value");
        final var groupingColumns = resolveEach(grouping).iterator();
        for (final var resultValue : resultValues) {
            if (resultValue instanceof IndexableAggregateValue) {
                continue;
            }
            Assert.thatUnchecked(groupingColumns.hasNext(), ErrorCode.UNSUPPORTED_OPERATION,
                    "Aggregate result value contains values missing from the grouping expression");
            Assert.thatUnchecked(resultValue.equals(groupingColumns.next()), ErrorCode.UNSUPPORTED_OPERATION,
                    "Aggregate result value does not align with grouping value");
        }
        Assert.thatUnchecked(!groupingColumns.hasNext(), ErrorCode.UNSUPPORTED_OPERATION,
                "Grouping value absent from aggregate result value");
    }

    /**
     * Strips the root of the field path from every column referencing the underlying select-where, leaving a field path of
     * the base record.
     */
    @Nonnull
    private static List<Value> adjustGroupByFieldPaths(@Nonnull final List<Value> resultValues,
                                                       @Nonnull final GroupByExpression groupBy) {
        final var selectWhereAlias = groupBy.getQuantifiers().get(0).getAlias();
        return resultValues.stream()
                .map(resultValue -> resultValue.replace(value -> stripSelectWhereRoot(value, selectWhereAlias)))
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Drops the leading accessor of a field path that starts at the select-where; leaves anything else alone.
     */
    @Nonnull
    private static Value stripSelectWhereRoot(@Nonnull final Value value,
                                              @Nonnull final CorrelationIdentifier selectWhereAlias) {
        if (value instanceof final FieldValue fieldValue
                && fieldValue.getChild() instanceof final QuantifiedObjectValue root
                && root.getAlias().equals(selectWhereAlias)) {
            final var accessors = fieldValue.getFieldPath().getFieldAccessors();
            return FieldValue.ofFields(root, new FieldValue.FieldPath(accessors.subList(1, accessors.size())));
        }
        return value;
    }

    @Nonnull
    private Stream<Value> resolveEach(@Nonnull final Value record) {
        return Values.deconstructRecord(record).stream().map(quantifierValues::resolve);
    }
}
