/*
 * FDBEvaluationContext.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.API;
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.EvaluationContextBuilder;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordFunction;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * An {@link EvaluationContext} associated with a particular {@link FDBRecordStore}.
 * @param <M> type used to represent stored records
 */
@API(API.Status.MAINTAINED)
public abstract class FDBEvaluationContext<M extends Message> extends EvaluationContext {
    protected FDBEvaluationContext(@Nonnull Bindings bindings) {
        super(bindings);
    }

    /**
     * Get the record store associated with this context.
     * @return the record store for this context
     */
    public abstract FDBRecordStoreBase<M> getStore();

    @Nonnull
    @Override
    public Executor getExecutor() {
        return getStore().getExecutor();
    }

    /**
     * Get the timer used to instrument events within this context.
     * This function will return <code>null</code> if no timer has
     * been specified.
     *
     * @return the timer used when instrumenting events or <code>null</code>
     */
    @Nullable
    public FDBStoreTimer getTimer() {
        return getStore().getTimer();
    }

    /**
     * Evaluate a {@link RecordFunction} against a record.
     * @param function the function to evaluate
     * @param record the record to evaluate against
     * @param <T> the type of the result
     * @param <N> the type of the record
     * @return a future that will complete with the result of evaluating the function against the record
     */
    @Nonnull
    public <T, N extends M> CompletableFuture<T> evaluateRecordFunction(@Nonnull RecordFunction<T> function,
                                                                        @Nonnull FDBRecord<N> record) {
        return getStore().evaluateRecordFunction(this, function, record);
    }

    /**
     * Evaluate an {@link IndexAggregateFunction} against a range of the store.
     * @param recordTypeNames record types for which to find a matching index
     * @param aggregateFunction the function to evaluate
     * @param range the range of records (group) for which to evaluate
     * @param isolationLevel whether to use snapshot reads
     * @return a future that will complete with the result of evaluating the aggregate
     */
    @Nonnull
    public CompletableFuture<Tuple> evaluateAggregateFunction(@Nonnull List<String> recordTypeNames,
                                                              @Nonnull IndexAggregateFunction aggregateFunction,
                                                              @Nonnull TupleRange range,
                                                              @Nonnull IsolationLevel isolationLevel) {
        return getStore().evaluateAggregateFunction(recordTypeNames, aggregateFunction,
                aggregateFunction.adjustRange(this, range), isolationLevel);
    }

    /**
     * Evaluate an {@link IndexAggregateFunction} against a range of the store.
     * @param recordTypeNames record types for which to find a matching index
     * @param aggregateFunction the function to evaluate
     * @param range the range of records (group) for which to evaluate
     * @return a future that will complete with the result of evaluating the aggregate
     */
    @Nonnull
    public CompletableFuture<Tuple> evaluateAggregateFunction(@Nonnull List<String> recordTypeNames,
                                                              @Nonnull IndexAggregateFunction aggregateFunction,
                                                              @Nonnull TupleRange range) {
        return evaluateAggregateFunction(recordTypeNames, aggregateFunction, range, IsolationLevel.SERIALIZABLE);
    }

    /**
     * Plan and execute a query.
     * @param query the query to plan and execute
     * @return a cursor for query results
     * @see RecordQueryPlan#execute
     */
    @Nonnull
    public RecordCursor<FDBQueriedRecord<M>> executeQuery(@Nonnull RecordQuery query) {
        return executeQuery(getStore().planQuery(query));
    }

    /**
     * Execute a query.
     * @param query the query to execute
     * @return a cursor for query results
     * @see RecordQueryPlan#execute
     */
    @Nonnull
    public RecordCursor<FDBQueriedRecord<M>> executeQuery(@Nonnull RecordQueryPlan query) {
        return query.execute(this);
    }

    @Override
    @Nonnull
    public FDBEvaluationContext<M> withBinding(@Nonnull String bindingName, @Nullable Object value) {
        return childBuilder().setBinding(bindingName, value).build();
    }

    @Override
    @Nonnull
    public Builder<M> childBuilder() {
        return new Builder<>(this);
    }

    // This class exists primarily for overriding the return type to
    // explicitly return an FDBEvaluationContext rather than just an EvaluationContext.
    /**
     * A Builder for {@link FDBEvaluationContext}.
     * @param <M> type used to represent stored records
     */
    public static class Builder<M extends Message> extends EvaluationContextBuilder {

        protected Builder(@Nonnull FDBEvaluationContext<M> original) {
            super(original);
        }

        @Override
        @Nonnull
        public Builder<M> setBinding(@Nonnull String name, @Nullable Object value) {
            super.setBinding(name, value);
            return this;
        }

        @SuppressWarnings("unchecked")
        @Override
        @Nonnull
        public FDBEvaluationContext<M> build() {
            return (FDBEvaluationContext<M>) super.build();
        }
    }

}
