/*
 * Query.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.expressions;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.StoreRecordFunction;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.QueryableKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.record.provider.foundationdb.leaderboard.TimeWindowForFunction;
import com.apple.foundationdb.record.provider.foundationdb.leaderboard.TimeWindowRecordFunction;
import com.apple.foundationdb.tuple.Tuple;

import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Holder class for creating querying expressions.
 */
@API(API.Status.UNSTABLE)
public class Query {

    /**
     * Creates a new Field context. This has a variety of methods for asserting about the value of the associated field.
     * Usable for any type field.
     * @param name the name of the field
     * @return a new Field ready for matching
     */
    public static Field field(String name) {
        return new Field(name);
    }

    /**
     * Check that a set of components all evaluate to true for a given record.
     * @param first the first assertion
     * @param second the second assertion
     * @param operands any other assertions
     * @return a new component that will return the record if all the children match
     */
    public static QueryComponent and(QueryComponent first, QueryComponent second,
                                     QueryComponent... operands) {
        return AndComponent.from(toList(first, second, operands));
    }

    /**
     * Check that a set of components all evaluate to true for a given record.
     * @param operands assertions
     * @return a new component that will return the record if all the children match
     */
    public static QueryComponent and(List<? extends QueryComponent> operands) {
        return AndComponent.from(operands);
    }


    /**
     * Check that any of a set of components evaluate to true for a given record.
     * @param first the first assertion
     * @param second the second assertion
     * @param operands any other assertions
     * @return a new component that will return the record if any of the children match
     */
    public static QueryComponent or(QueryComponent first, QueryComponent second, QueryComponent... operands) {
        return OrComponent.from(toList(first, second, operands));
    }

    /**
     * Check that a set of components all evaluate to true for a given record.
     * @param operands assertions
     * @return a new component that will return the record if any of the children match
     */
    public static QueryComponent or(List<QueryComponent> operands) {
        return OrComponent.from(operands);
    }

    /**
     * Negate a component test.
     * @param operand assertion to be negated
     * @return a new component that will return the record if the child does not match
     */
    public static QueryComponent not(QueryComponent operand) {
        return new NotComponent(operand);
    }

    /**
     * A record function that can be used to determine or compare the rank value for a record.
     * @param operand the argument to rank
     * @return a record function that evaluates the rank of the operand
     */
    public static QueryRecordFunction<Long> rank(GroupingKeyExpression operand) {
        return new QueryRecordFunction<>(new IndexRecordFunction<>(FunctionNames.RANK, operand, null));
    }

    /**
     * A record function that can be used to determine or compare the rank value for a record.
     * @param fieldName the argument to rank
     * @return a record function that evaluates the rank of the operand
     */
    public static QueryRecordFunction<Long> rank(String fieldName) {
        return rank(Key.Expressions.field(fieldName).ungrouped());
    }

    /**
     * A record function that can be used to determine or compare the rank value for a record for a time window.
     * @param timeWindow the time window for which to get the rank
     * @param operand the argument to rank
     * @return a record function that evaluates to the rank of the operand
     */
    public static QueryRecordFunction<Long> timeWindowRank(TimeWindowForFunction timeWindow, GroupingKeyExpression operand) {
        return new QueryRecordFunction<>(new TimeWindowRecordFunction<>(FunctionNames.TIME_WINDOW_RANK, operand, null, timeWindow));
    }

    /**
     * A record function that can be used to determine or compare the rank value for a record for a time window.
     * @param type the type of time window
     * @param timestamp the target timestamp
     * @param typeParameter the name of the time window type parameter if it is a parameter
     * @param timestampParameter the name of the timestamp parameter if it is a parameter
     * @param operand the argument to rank
     * @return a record function that evaluates to the rank of the operand
     */
    public static QueryRecordFunction<Long> timeWindowRank(int type, long timestamp, @Nullable String typeParameter, @Nullable String timestampParameter, GroupingKeyExpression operand) {
        return timeWindowRank(new TimeWindowForFunction(type, timestamp, typeParameter, timestampParameter), operand);
    }

    /**
     * A record function that can be used to determine or compare the rank value for a record for a time window.
     * @param type the type of time window
     * @param timestamp the target timestamp
     * @param operand the argument to rank
     * @return a record function that evaluates to the rank of the operand
     */
    public static QueryRecordFunction<Long> timeWindowRank(int type, long timestamp, GroupingKeyExpression operand) {
        return timeWindowRank(type, timestamp, null, null, operand);
    }

    /**
     * A record function that can be used to determine or compare the rank value for a record for a time window.
     * @param typeParameter the name of the time window type parameter
     * @param timestampParameter the name of the timestamp parameter
     * @param operand the argument to rank
     * @return a record function that evaluates to the rank of the operand
     */
    public static QueryRecordFunction<Long> timeWindowRank(String typeParameter, String timestampParameter, GroupingKeyExpression operand) {
        return timeWindowRank(0, 0, typeParameter, timestampParameter, operand);
    }

    /**
     * A record function that can be used to determine or compare the rank value for a record for a time window and the score entry that determined that rank.
     *
     * The result of evaluation will be {@code null} if the record has no entry within the specified time window or else a {@code Tuple}
     * of rank, value, timestamp, and any other values in the specified operand's items.
     * @param timeWindow the time window for which to get the rank
     * @param operand the argument to rank
     * @return a record function that evaluates to a {@code Tuple} of the rank and the corresponding entry
     */
    public static QueryRecordFunction<Tuple> timeWindowRankAndEntry(TimeWindowForFunction timeWindow, GroupingKeyExpression operand) {
        return new QueryRecordFunction<>(new TimeWindowRecordFunction<>(FunctionNames.TIME_WINDOW_RANK_AND_ENTRY, operand, null, timeWindow));
    }

    /**
     * A record function that can be used to determine or compare the rank value for a record for a time window and the score entry that determined that rank.
     *
     * The result of evaluation will be {@code null} if the record has no entry within the specified time window or else a {@code Tuple}
     * of rank, value, timestamp, and any other values in the specified operand's items.
     * @param type the type of time window
     * @param timestamp the target timestamp
     * @param typeParameter the name of the time window type parameter if it is a parameter
     * @param timestampParameter the name of the timestamp parameter if it is a parameter
     * @param operand the argument to rank
     * @return a record function that evaluates to the rank of the operand
     */
    public static QueryRecordFunction<Tuple> timeWindowRankAndEntry(int type, long timestamp, @Nullable String typeParameter, @Nullable String timestampParameter, GroupingKeyExpression operand) {
        return timeWindowRankAndEntry(new TimeWindowForFunction(type, timestamp, typeParameter, timestampParameter), operand);
    }

    /**
     * A record function that can be used to determine or compare the rank value for a record for a time window and the score entry that determined that rank.
     *
     * The result of evaluation will be {@code null} if the record has no entry within the specified time window or else a {@code Tuple}
     * of rank, value, timestamp, and any other values in the specified operand's items.
     * @param type the type of time window
     * @param timestamp the target timestamp
     * @param operand the argument to rank
     * @return a record function that evaluates to the rank of the operand
     */
    public static QueryRecordFunction<Tuple> timeWindowRankAndEntry(int type, long timestamp, GroupingKeyExpression operand) {
        return timeWindowRankAndEntry(type, timestamp, null, null, operand);
    }

    /**
     * A record function that can be used to determine or compare the rank value for a record for a time window and the score entry that determined that rank.
     *
     * The result of evaluation will be {@code null} if the record has no entry within the specified time window or else a {@code Tuple}
     * of rank, value, timestamp, and any other values in the specified operand's items.
     * @param typeParameter the name of the time window type parameter
     * @param timestampParameter the name of the timestamp parameter
     * @param operand the argument to rank
     * @return a record function that evaluates to the rank of the operand
     */
    public static QueryRecordFunction<Tuple> timeWindowRankAndEntry(String typeParameter, String timestampParameter, GroupingKeyExpression operand) {
        return timeWindowRankAndEntry(0, 0, typeParameter, timestampParameter, operand);
    }

    /**
     * A record function that can be used to determine the version of a record.
     * @return a record function that evaluates the version of the record
     */
    public static QueryRecordFunction<FDBRecordVersion> version() {
        return new QueryRecordFunction<>(new StoreRecordFunction<>(FunctionNames.VERSION));
    }

    /**
     * Build query components using a key expression.
     * @param keyExpression the key expression to compare with values
     * @return a {@code QueryKeyExpression} for matching
     */
    @API(API.Status.EXPERIMENTAL)
    public static QueryKeyExpression keyExpression(KeyExpression keyExpression) {
        if (!(keyExpression instanceof QueryableKeyExpression)) {
            throw new MetaDataException("query key expression must be queryable");
        }
        return new QueryKeyExpression((QueryableKeyExpression)keyExpression);
    }

    /**
     * Exception thrown when a query expression is not valid in some context.
     */
    @SuppressWarnings("serial")
    public static class InvalidExpressionException extends IllegalStateException {
        public InvalidExpressionException(String message) {
            super(message);
        }
    }

    private static List<QueryComponent> toList(QueryComponent first, QueryComponent second,
                                               QueryComponent... operands) {
        List<QueryComponent> children = new ArrayList<>(operands.length + 2);
        children.add(first);
        children.add(second);
        Collections.addAll(children, operands);
        return children;
    }

    public static boolean isSingleFieldComparison(QueryComponent component) {
        return component instanceof FieldWithComparison ||
               (component instanceof NestedField &&
                ((NestedField)component).getChild() instanceof FieldWithComparison);
    }

    private Query() {
    }
}
