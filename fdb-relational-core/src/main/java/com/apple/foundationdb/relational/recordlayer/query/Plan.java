/*
 * Plan.java
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
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metrics.MetricCollector;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.util.Assert;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.VerifyException;

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
     * Determine if the plan is an update plan.
     *
     * @return {@code true} if the plan is an insert, update, or delete plan, {@code false} otherwise.
     */
    public abstract boolean isUpdatePlan();

    public abstract Plan<T> optimize(@Nonnull final CascadesPlanner planner, @Nonnull final PlannerConfiguration configuration) throws RelationalException;

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

    protected abstract T executeInternal(@Nonnull final ExecutionContext c) throws RelationalException;

    @Nonnull
    public abstract QueryPlanConstraint getConstraint();

    @Nonnull
    public abstract Plan<T> withQueryExecutionParameters(@Nonnull final QueryExecutionParameters parameters);

    @Nonnull
    public abstract String explain();

    @Nonnull
    public String getQuery() {
        return query;
    }

    /**
     * Parses a query and generates an equivalent logical plan.
     *
     * @param query         The query string, required for logging.
     * @param planContext   The plan context.
     * @param caseSensitive use database-object text representation as-is if true, uppercase non-quoted ones otherwise
     * @return The logical plan of the query.
     * @throws RelationalException if something goes wrong.
     */
    @Nonnull
    @VisibleForTesting
    public static Plan<?> generate(@Nonnull final String query, @Nonnull PlanContext planContext, final boolean caseSensitive) throws RelationalException {
        final var context = PlanGenerationContext.newBuilder()
                .setMetadataFactory(planContext.getConstantActionFactory())
                .setPreparedStatementParameters(planContext.getPreparedStatementParameters())
                .build();
        context.pushDqlContext(RecordLayerSchemaTemplate.fromRecordMetadata(planContext.getMetaData(), "foo", 1));
        final var ast = QueryParser.parse(query);
        final var astWalker = new AstVisitor(context, planContext.getDdlQueryFactory(), planContext.getDbUri(), query, caseSensitive);
        long start = System.nanoTime();
        try {

            final Object maybePlan = astWalker.visit(ast);
            Assert.that(maybePlan instanceof Plan, String.format("Could not generate a logical plan for query '%s'", query));

            Plan<?> plan = (Plan<?>) maybePlan;

            //log the plan time
            long planTime = System.nanoTime() - start;
            QueryLogger.instance().logPlan(plan, query, planTime);
            return plan;
        } catch (UncheckedRelationalException uve) {
            QueryLogger.instance().logPlanError(query, uve);
            throw uve.unwrap();
        } catch (VerifyException | RecordCoreException re) {
            // we need a better way to pass-thru / translate errors codes between record layer and Relational as SQL exceptions
            RelationalException ve = ExceptionUtil.toRelationalException(re);
            QueryLogger.instance().logPlanError(query, ve);
            throw ve;
        }
    }
}
