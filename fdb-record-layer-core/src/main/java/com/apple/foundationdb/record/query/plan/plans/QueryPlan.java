/*
 * QueryPlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.plans;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.query.plan.cascades.FinalMemoizer;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

/**
 * An executable query plan. A query plan can be executed against a record store to get a stream of items.
 * Unlike a {@link RecordQueryPlan} (which extends this interface), implementations are not required to
 * return records but might instead return items of an arbitrary type. Note that
 * {@link com.apple.foundationdb.record.query.plan.QueryPlanner QueryPlanner}s always return {@code RecordQueryPlan}s,
 * but the query plan tree might contain intermediate plans that return something other than records.
 *
 * @param <T> the type of element produced by executing this plan
 */
@API(API.Status.EXPERIMENTAL)
public interface QueryPlan<T> extends PlanHashable, RelationalExpression {

    /**
     * The result of {@link #maxCardinality} is not known.
     */
    int UNKNOWN_MAX_CARDINALITY = Integer.MAX_VALUE;

    /**
     * Execute this query plan.
     * @param store record store from which to fetch items
     * @param context evaluation context containing parameter bindings
     * @param continuation continuation from a previous execution of this same plan
     * @param executeProperties limits on execution
     * @return a cursor of items that match the query criteria
     */
    @Nonnull
    RecordCursor<T> execute(@Nonnull FDBRecordStore store, @Nonnull EvaluationContext context,
                            @Nullable byte[] continuation, @Nonnull ExecuteProperties executeProperties);

    /**
     * Execute this query plan.
     * @param store record store from which to fetch items
     * @return a cursor of items that match the query criteria
     */
    @Nonnull
    default RecordCursor<T> execute(@Nonnull FDBRecordStore store) {
        return execute(store, EvaluationContext.EMPTY);
    }

    /**
     * Execute this query plan.
     * @param store record store to access
     * @param context evaluation context containing parameter bindings
     * @return a cursor of items that match the query criteria
     */
    @Nonnull
    default RecordCursor<T> execute(@Nonnull FDBRecordStore store, @Nonnull EvaluationContext context) {
        return execute(store, context, null, ExecuteProperties.SERIAL_EXECUTE);
    }

    /**
     * Indicates whether this plan will return values in "reverse" order from the
     * natural order of results of this plan. The return value is <code>true</code> if the plan
     * returns elements in descending order and <code>false</code> if the elements are
     * returned in ascending order.
     * @return <code>true</code> if this plan returns elements in reverse order
     */
    boolean isReverse();

    /**
     * Indicates whether this plan (or one of its components) scans at least a subset of the record range directly
     * rather than going through a secondary index.
     * A plan may only scan over a subset of all records. Someone might not use any secondary indexes explicitly, but
     * they might make use of the primary key index. For example, if they had a compound primary key, and they issued
     * a query for all records that had some value for the first element of their primary key, the planner will produce
     * a plan which scans over a subset of all records.
     * @return <code>true</code> if this plan (or one of its components) scans at least a subset of the records directly
     */
    boolean hasRecordScan();

    /**
     * Indicates whether this plan (or one of its components) must perform a scan over all records in the store directly
     * rather than going through a secondary index.
     * See {@link #hasRecordScan hasRecordScan} for the comparison between two methods.
     * @return <code>true</code> if this plan (or one of its components) must perform a scan over all records in the
     * store directly
     */
    boolean hasFullRecordScan();

    /**
     * Indicates whether this plan scans the given index.
     * @param indexName the name of the index to check for
     * @return <code>true</code> if this plan (or one of its children) scans the given index
     */
    boolean hasIndexScan(@Nonnull String indexName);

    /**
     * Returns a set of names of the indexes used by this plan (and its sub-plans).
     * @return a set of indexes used by this plan
     */
    @Nonnull
    Set<String> getUsedIndexes();

    /**
     * Indicates whether this plan (or one of its components) loads records by their primary key.
     * @return <code>true</code> if this plan (or one of its components) loads records by their primary key
     */
    boolean hasLoadBykeys();

    /**
     * Indicates how many records this plan could possibly return.
     * @param metaData meta-data to use to determine things like index uniqueness
     * @return the maximum number of records or {@link #UNKNOWN_MAX_CARDINALITY} if not known
     */
    default int maxCardinality(@Nonnull RecordMetaData metaData) {
        return UNKNOWN_MAX_CARDINALITY;
    }

    /**
     * Indicates whether the ordering of records from this plan is fully accounted for by its requested sort.
     * That is, each new record will have a different value for that sort key.
     * @return {@code true} if this plan is fully sorted according to the query definition
     */
    default boolean isStrictlySorted() {
        return false;
    }

    /**
     * Return a copy of this plan that has the {@link #isStrictlySorted} property.
     * @param memoizer a memoizer that is used to memoize/re-reference new expressions references
     * @return a copy of this plan
     */
    default QueryPlan<T> strictlySorted(@Nonnull final FinalMemoizer memoizer) {
        return this;
    }

    /**
     * Adds one to an appropriate {@link StoreTimer} counter for each plan and subplan of this plan, allowing tracking
     * of which plans are being chosen (e.g. index scan vs. full scan).
     * @param timer the counters to increment
     */
    void logPlanStructure(StoreTimer timer);

    /**
     * Returns an integer representing the "complexity" of the generated plan.
     * Currently, this should simply be the number of plans in the plan tree with this plan as the root (i.e. the
     * number of descendants of this plan, including itself).
     * @return the complexity of this plan
     */
    int getComplexity();

    /**
     * Returns the (zero or more) child {@code QueryPlan}s of this plan. These children may or may not
     * return elements of the same type as their parent plan.
     *
     * <p>
     * <b>Warning</b>: This part of the API is currently undergoing active development. At some point in
     * the future, this will be renamed {@code getChildren()}. This cannot be done at current, however,
     * as it would require an incompatible change to {@link RecordQueryPlan#getChildren()}. That method
     * has been marked {@link API.Status#UNSTABLE} as of version 2.5.
     * </p>
     *
     * @return the child plans of this plan
     */
    @SuppressWarnings("squid:S1452") // wildcards in return type
    List<? extends QueryPlan<?>> getQueryPlanChildren();

    default Iterable<? extends QueryPlan<?>> collectDescendantPlans() {
        Iterable<? extends QueryPlan<?>> result = ImmutableList.of();
        for (final QueryPlan<?> queryPlanChild : getQueryPlanChildren()) {
            result = Iterables.concat(result, queryPlanChild.collectDescendantPlans(), ImmutableList.of(queryPlanChild));
        }
        return result;
    }
}
