/*
 * QueryPlan.java
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.QueryPlanInfoKeys;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.expressions.LogicalSortExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.properties.UsedTypesProperty;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.SqlTypeSupport;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.ddl.DdlQuery;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.ContinuationImpl;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.MessageTuple;
import com.apple.foundationdb.relational.recordlayer.RecordLayerIterator;
import com.apple.foundationdb.relational.recordlayer.RecordLayerResultSet;
import com.apple.foundationdb.relational.recordlayer.RecordLayerSchema;
import com.apple.foundationdb.relational.recordlayer.ResumableIterator;
import com.apple.foundationdb.relational.recordlayer.util.Assert;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public interface QueryPlan extends Plan<RelationalResultSet>, Typed {

    class PhysicalQueryPlan implements QueryPlan {

        @Nonnull
        private final RecordQueryPlan recordQueryPlan;

        @Nonnull
        private final QueryPlanConstraint constraint;

        @Nonnull
        private final QueryExecutionParameters queryExecutionParameters;

        public PhysicalQueryPlan(@Nonnull final RecordQueryPlan recordQueryPlan,
                                 @Nonnull final QueryPlanConstraint constraint,
                                 @Nonnull final QueryExecutionParameters queryExecutionParameters) {
            this.recordQueryPlan = recordQueryPlan;
            this.constraint = constraint;
            this.queryExecutionParameters = queryExecutionParameters;
        }

        @Nonnull
        @Override
        public Type getResultType() {
            return Assert.notNullUnchecked(recordQueryPlan.getResultType().getInnerType());
        }

        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        @Override
        @Nonnull
        public PhysicalQueryPlan withQueryExecutionParameters(@Nonnull final QueryExecutionParameters parameters) {
            if (parameters == this.queryExecutionParameters) {
                return this;
            }
            return new PhysicalQueryPlan(recordQueryPlan, constraint, parameters);
        }

        @Override
        @Nonnull
        public QueryPlanConstraint getConstraint() {
            return constraint;
        }

        @Override
        public Plan<RelationalResultSet> optimize(@Nonnull CascadesPlanner planner) {
            return this;
        }

        @Override
        public RelationalResultSet execute(@Nonnull final ExecutionContext executionContext) throws RelationalException {
            if (!(executionContext.connection instanceof EmbeddedRelationalConnection)) {
                //this is required until TODO is resolved
                throw new RelationalException("Cannot execute a QueryPlan without an EmbeddedRelationalConnection", ErrorCode.INTERNAL_ERROR);
            }

            final EmbeddedRelationalConnection conn = (EmbeddedRelationalConnection) executionContext.connection;
            final String schemaName = conn.getSchema();
            try (RecordLayerSchema recordLayerSchema = conn.getRecordLayerDatabase().loadSchema(schemaName)) {
                final TypeRepository.Builder builder = TypeRepository.newBuilder();
                final Set<Type> usedTypes = UsedTypesProperty.evaluate(recordQueryPlan);
                usedTypes.forEach(builder::addTypeIfNeeded);
                final var evaluationContext = queryExecutionParameters.getEvaluationContext();
                final var typedEvaluationContext = EvaluationContext.forBindingsAndTypeRepository(evaluationContext.getBindings(), builder.build());
                final var  executionProperties = queryExecutionParameters.getExecutionProperties(typedEvaluationContext);
                if (queryExecutionParameters.isForExplain()) {
                    return Plan.explainPhysicalPlan(recordQueryPlan, executionProperties);
                } else {
                    return executePhysicalPlan(recordQueryPlan,
                            recordLayerSchema,
                            typedEvaluationContext,
                            executionProperties,
                            queryExecutionParameters,
                            conn);
                }
            }
        }

        @Nonnull
        private static RelationalResultSet executePhysicalPlan(@Nonnull final RecordQueryPlan physicalPlan,
                                                             @Nonnull final RecordLayerSchema recordLayerSchema,
                                                             @Nonnull final EvaluationContext evaluationContext,
                                                             @Nonnull final ExecuteProperties executeProperties,
                                                             @Nonnull final QueryExecutionParameters executionParameters,
                                                             @Nullable final EmbeddedRelationalConnection connection) throws RelationalException {
            Type type = physicalPlan.getResultType().getInnerType();
            Assert.notNull(type);
            Assert.that(type instanceof Type.Record, String.format("unexpected plan returning top-level result of type %s", type.getTypeCode()));
            StructMetaData metaData = SqlTypeSupport.typeToMetaData(type);
            final FDBRecordStoreBase<Message> fdbRecordStore = recordLayerSchema.loadStore().unwrap(FDBRecordStoreBase.class);
            final RecordCursor<QueryResult> cursor;
            try {
                cursor = physicalPlan.executePlan(fdbRecordStore, evaluationContext, ContinuationImpl.parseContinuation(executionParameters.getContinuation()).getUnderlyingBytes(), executeProperties);
            } catch (InvalidProtocolBufferException ipbe) {
                throw ExceptionUtil.toRelationalException(ipbe);
            }
            final ResumableIterator<Row> iterator = RecordLayerIterator.create(cursor, messageFDBQueriedRecord -> new MessageTuple(messageFDBQueriedRecord.getMessage()));
            return new RecordLayerResultSet(metaData, iterator, connection, executionParameters.getPreparedStatementParameters(),
                    physicalPlan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
        }
    }

    /**
     * This represents a logical query plan that can be executed to produce a {@link java.sql.ResultSet}.
     */
    class LogicalQueryPlan implements QueryPlan {

        @Nonnull
        private final RelationalExpression relationalExpression;

        @Nonnull
        private final PlanGenerationContext context;

        @Nonnull
        private Optional<PhysicalQueryPlan> optimizedPlan;

        private LogicalQueryPlan(@Nonnull final RelationalExpression relationalExpression,
                                 @Nonnull final PlanGenerationContext context) {
            this.relationalExpression = relationalExpression;
            this.context = context;
            this.optimizedPlan = Optional.empty();
        }

        @Override
        @Nonnull
        public PhysicalQueryPlan optimize(@Nonnull final CascadesPlanner planner) {
            if (optimizedPlan.isPresent()) {
                return optimizedPlan.get();
            }
            final TypeRepository.Builder builder = TypeRepository.newBuilder();
            final Set<Type> usedTypes = UsedTypesProperty.evaluate(relationalExpression);
            usedTypes.forEach(builder::addTypeIfNeeded);
            final var evaluationContext = context.getEvaluationContext();
            final var typedEvaluationContext = EvaluationContext.forBindingsAndTypeRepository(evaluationContext.getBindings(), builder.build());
            final var planResult = planner.planGraph(
                    () -> GroupExpressionRef.of(relationalExpression),
                    Optional.empty(),
                    IndexQueryabilityFilter.TRUE,
                    ((LogicalSortExpression) relationalExpression).isReverse(), typedEvaluationContext);
            optimizedPlan = Optional.of(new PhysicalQueryPlan(planResult.getPlan(), QueryPlanConstraint.compose(List.of(Objects.requireNonNull(planResult.getPlanInfo().get(QueryPlanInfoKeys.CONSTRAINTS)), getConstraint())), context));
            return optimizedPlan.get();
        }

        @Nonnull
        @Override
        public QueryPlanConstraint getConstraint() {
            return context.getLiteralReferencesConstraint();
        }

        @Nonnull
        @Override
        public Plan<RelationalResultSet> withQueryExecutionParameters(@Nonnull QueryExecutionParameters parameters) {
            return this;
        }

        @Override
        public RelationalResultSet execute(@Nonnull final ExecutionContext executionContext) throws RelationalException {
            return this.optimizedPlan.get().execute(executionContext);
        }

        @Nonnull
        @Override
        public Type getResultType() {
            return relationalExpression.getResultType();
        }

        @Nonnull
        public RelationalExpression getRelationalExpression() {
            return relationalExpression;
        }

        @Nonnull
        public static LogicalQueryPlan of(@Nonnull final RelationalExpression relationalExpression,
                                          @Nonnull final PlanGenerationContext context) {
            return new LogicalQueryPlan(relationalExpression, context);
        }
    }

    class MetadataQueryPlan implements QueryPlan {

        @Nonnull
        private final CheckedFunctional<Transaction, RelationalResultSet> query;

        @Nonnull
        private final Type rowType;

        private interface CheckedFunctional<T, R> {
            R apply(T t) throws RelationalException;
        }

        private MetadataQueryPlan(@Nonnull final CheckedFunctional<Transaction, RelationalResultSet> query, @Nonnull final Type rowType) {
            this.query = query;
            this.rowType = rowType;
        }

        @Override
        public Plan<RelationalResultSet> optimize(@Nonnull CascadesPlanner planner) {
            return this;
        }

        @Override
        public RelationalResultSet execute(@Nonnull final ExecutionContext context) throws RelationalException {
            return query.apply(context.transaction);
        }

        @Nonnull
        @Override
        public QueryPlanConstraint getConstraint() {
            return QueryPlanConstraint.tautology();
        }

        @Nonnull
        @Override
        public Plan<RelationalResultSet> withQueryExecutionParameters(@Nonnull QueryExecutionParameters parameters) {
            return this;
        }

        @Nonnull
        @Override
        public Type getResultType() {
            return rowType;
        }

        @Nonnull
        public static MetadataQueryPlan of(DdlQuery ddlQuery) {
            return new MetadataQueryPlan(ddlQuery::executeAction, ddlQuery.getResultSetMetadata());
        }
    }
}
