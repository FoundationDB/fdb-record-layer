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
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.util.Assert;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.VerifyException;

import javax.annotation.Nonnull;

public interface Plan<T> {

    class ExecutionContext {
        @Nonnull
        final Transaction transaction;
        @Nonnull
        final Options options;
        @Nonnull
        final RelationalConnection connection;

        ExecutionContext(@Nonnull Transaction transaction,
                         @Nonnull Options options,
                         @Nonnull RelationalConnection connection) {
            this.transaction = transaction;
            this.options = options;
            this.connection = connection;
        }

        @Nonnull
        public static ExecutionContext of(@Nonnull Transaction transaction,
                                          @Nonnull Options options,
                                          @Nonnull RelationalConnection connection) {
            return new ExecutionContext(transaction, options, connection);
        }
    }

    Plan<T> optimize(@Nonnull final CascadesPlanner planner, @Nonnull final PlannerConfiguration configuration);

    T execute(@Nonnull final ExecutionContext c) throws RelationalException;

    @Nonnull
    QueryPlanConstraint getConstraint();

    @Nonnull
    Plan<T> withQueryExecutionParameters(@Nonnull final QueryExecutionParameters parameters);

    @Nonnull
    String explain();

    /**
     * Parses a query and generates an equivalent logical plan.
     *
     * @param query       The query string, required for logging.
     * @param planContext The plan context.
     * @return The logical plan of the query.
     * @throws RelationalException if something goes wrong.
     */
    @Nonnull
    @VisibleForTesting
    static Plan<?> generate(@Nonnull final String query, @Nonnull PlanContext planContext) throws RelationalException {
        final var context = PlanGenerationContext.newBuilder()
                .setMetadataFactory(planContext.getConstantActionFactory())
                .setPreparedStatementParameters(planContext.getPreparedStatementParameters())
                .build();
        context.pushDqlContext(RecordLayerSchemaTemplate.fromRecordMetadata(planContext.getMetaData(), "foo", 1));
        final var ast = AstVisitor.parseQuery(query);
        final var astWalker = new AstVisitor(context, planContext.getDdlQueryFactory(), planContext.getDbUri());
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
