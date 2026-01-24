/*
 * Plan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metrics.MetricCollector;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;

import javax.annotation.Nonnull;

public abstract class Plan<T> {

    @Nonnull
    protected final String query;

    protected Plan(@Nonnull final String query) {
        this.query = query;
    }

    public static class ExecutionContext {
        @Nonnull
        final Transaction transaction;
        @Nonnull
        final Options options;
        @Nonnull
        final RelationalConnection connection;
        @Nonnull
        final MetricCollector metricCollector;

        ExecutionContext(@Nonnull Transaction transaction,
                         @Nonnull Options options,
                         @Nonnull RelationalConnection connection,
                         @Nonnull MetricCollector metricCollector) {
            this.transaction = transaction;
            this.options = options;
            this.connection = connection;
            this.metricCollector = metricCollector;
        }

        @Nonnull
        public Options getOptions() {
            return options;
        }

        @Nonnull
        public static ExecutionContext of(@Nonnull Transaction transaction,
                                          @Nonnull Options options,
                                          @Nonnull RelationalConnection connection,
                                          @Nonnull MetricCollector metricCollector) {
            return new ExecutionContext(transaction, options, connection, metricCollector);
        }
    }

    /**
     * Determine if the plan is an update plan, an update plan must not return a valuable result set, i.e.
     * {@code INSERT .... RETURN id} is not an update plan, even though it updates data.
     *
     * @return {@code true} if the plan is an insert, update, or delete plan, {@code false} otherwise.
     */
    public abstract boolean isUpdatePlan();

    public abstract Plan<T> optimize(@Nonnull CascadesPlanner planner, @Nonnull PlanContext planContext,
                                     @Nonnull PlanHashable.PlanHashMode currentPlanHashMode) throws RelationalException;

    /**
     * Executes a particular type of Plan. If the plan "can" be executed, it should be timed and registered as
     * {@link com.apple.foundationdb.record.provider.common.StoreTimer.Event} in the {@link MetricCollector} to be
     * added to the observability of the system.
     *
     * @param c The execution context.
     * @return The result of the query execution, if there.
     * @throws RelationalException if something goes wrong.
     */
    public final T execute(@Nonnull final ExecutionContext c) throws RelationalException {
        return c.metricCollector.clock(RelationalMetric.RelationalEvent.TOTAL_EXECUTE_QUERY, () -> executeInternal(c));
    }

    protected abstract T executeInternal(@Nonnull ExecutionContext c) throws RelationalException;

    @Nonnull
    public abstract QueryPlanConstraint getConstraint();

    @Nonnull
    public abstract Plan<T> withExecutionContext(@Nonnull QueryExecutionContext queryExecutionContext);

    @Nonnull
    public abstract String explain();

    @Nonnull
    public String getQuery() {
        return query;
    }
}
