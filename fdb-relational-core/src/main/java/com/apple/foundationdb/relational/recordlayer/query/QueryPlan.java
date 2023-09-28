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

import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.QueryPlanInfoKeys;
import com.apple.foundationdb.record.query.plan.QueryPlanResult;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.GroupExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.properties.UsedTypesProperty;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryDeletePlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInsertPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUpdatePlan;
import com.apple.foundationdb.relational.api.FieldDescription;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.SqlTypeSupport;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.api.ddl.DdlQuery;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;
import com.apple.foundationdb.relational.recordlayer.ContinuationImpl;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.IteratorResultSet;
import com.apple.foundationdb.relational.recordlayer.MessageTuple;
import com.apple.foundationdb.relational.recordlayer.RecordLayerIterator;
import com.apple.foundationdb.relational.recordlayer.RecordLayerResultSet;
import com.apple.foundationdb.relational.recordlayer.RecordLayerSchema;
import com.apple.foundationdb.relational.recordlayer.ResumableIterator;
import com.apple.foundationdb.relational.recordlayer.ValueTuple;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;
import com.apple.foundationdb.relational.util.Assert;

import com.google.common.base.Suppliers;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

public abstract class QueryPlan extends Plan<RelationalResultSet> implements Typed {

    protected QueryPlan(@Nonnull final String query) {
        super(query);
    }

    public static class PhysicalQueryPlan extends QueryPlan {

        @Nonnull
        private final RecordQueryPlan recordQueryPlan;

        private final Supplier<Integer> recordQueryPlanHash = Suppliers.memoize(() -> getRecordQueryPlan().planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));

        @Nonnull
        private final TypeRepository typeRepository;

        @Nonnull
        private final QueryPlanConstraint constraint;

        @Nonnull
        private final QueryExecutionParameters queryExecutionParameters;

        public PhysicalQueryPlan(@Nonnull final RecordQueryPlan recordQueryPlan,
                                 @Nonnull final TypeRepository typeRepository,
                                 @Nonnull final QueryPlanConstraint constraint,
                                 @Nonnull final QueryExecutionParameters queryExecutionParameters,
                                 @Nonnull final String query) {
            super(query);
            this.recordQueryPlan = recordQueryPlan;
            this.typeRepository = typeRepository;
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
            return new PhysicalQueryPlan(recordQueryPlan, typeRepository, constraint, parameters, query);
        }

        @Nonnull
        @Override
        public String explain() {
            final var executeProperties = queryExecutionParameters.getExecutionPropertiesBuilder();
            List<String> explainComponents = new ArrayList<>();
            explainComponents.add(recordQueryPlan.toString());
            if (executeProperties.getReturnedRowLimit() != ReadTransaction.ROW_LIMIT_UNLIMITED) {
                explainComponents.add(String.format("(limit=%d)", executeProperties.getReturnedRowLimit()));
            }
            if (executeProperties.getSkip() != 0) {
                explainComponents.add(String.format("(offset=%d)", executeProperties.getSkip()));
            }
            return String.join(" ", explainComponents);
        }

        @Override
        @Nonnull
        public QueryPlanConstraint getConstraint() {
            return constraint;
        }

        @Override
        public boolean isUpdatePlan() {
            if (this.queryExecutionParameters.isForExplain()) {
                return false;
            } else {
                //TODO(bfines) there may be a better way to do this, but I couldn't find it easily
                return recordQueryPlan instanceof RecordQueryInsertPlan ||
                        recordQueryPlan instanceof RecordQueryUpdatePlan ||
                        recordQueryPlan instanceof RecordQueryDeletePlan;
            }
        }

        @Override
        public Plan<RelationalResultSet> optimize(@Nonnull CascadesPlanner planner, @Nonnull PlannerConfiguration configuration) {
            return this;
        }

        @Override
        public RelationalResultSet executeInternal(@Nonnull final ExecutionContext executionContext) throws RelationalException {
            if (!(executionContext.connection instanceof EmbeddedRelationalConnection)) {
                //this is required until TODO is resolved
                throw new RelationalException("Cannot execute a QueryPlan without an EmbeddedRelationalConnection", ErrorCode.INTERNAL_ERROR);
            }

            final EmbeddedRelationalConnection conn = (EmbeddedRelationalConnection) executionContext.connection;
            final String schemaName = conn.getSchema();
            try (RecordLayerSchema recordLayerSchema = conn.getRecordLayerDatabase().loadSchema(schemaName)) {
                final var evaluationContext = queryExecutionParameters.getEvaluationContext();
                final var typedEvaluationContext = EvaluationContext.forBindingsAndTypeRepository(evaluationContext.getBindings(), typeRepository);
                if (queryExecutionParameters.isForExplain()) {
                    Row printablePlan = new ValueTuple(explain());
                    StructMetaData metaData = new RelationalStructMetaData(
                            FieldDescription.primitive("PLAN", Types.VARCHAR, DatabaseMetaData.columnNoNulls)
                    );
                    return new IteratorResultSet(metaData, Collections.singleton(printablePlan).iterator(), 0);
                } else {
                    PlanValidator.validate(recordQueryPlan, queryExecutionParameters);
                    return executePhysicalPlan(recordQueryPlan,
                            recordLayerSchema,
                            typedEvaluationContext,
                            queryExecutionParameters,
                            executionContext);
                }
            }
        }

        @Nonnull
        public RecordQueryPlan getRecordQueryPlan() {
            return recordQueryPlan;
        }

        @Nonnull
        private static RelationalResultSet executePhysicalPlan(@Nonnull final RecordQueryPlan physicalPlan,
                                                             @Nonnull final RecordLayerSchema recordLayerSchema,
                                                             @Nonnull final EvaluationContext evaluationContext,
                                                             @Nonnull final QueryExecutionParameters executionParameters,
                                                             @Nonnull final ExecutionContext executionContext) throws RelationalException {
            final var connection = (EmbeddedRelationalConnection) executionContext.connection;
            Type type = physicalPlan.getResultType().getInnerType();
            Assert.notNull(type);
            Assert.that(type instanceof Type.Record, String.format("unexpected plan returning top-level result of type %s", type.getTypeCode()));
            StructMetaData metaData = SqlTypeSupport.typeToMetaData(type);
            final FDBRecordStoreBase<Message> fdbRecordStore = recordLayerSchema.loadStore().unwrap(FDBRecordStoreBase.class);
            final RecordCursor<QueryResult> cursor;
            final var executeProperties = connection.getExecuteProperties().toBuilder()
                    .setSkip(executionParameters.getExecutionPropertiesBuilder().getSkip())
                    .setReturnedRowLimit(executionParameters.getExecutionPropertiesBuilder().getReturnedRowLimit())
                    .setDryRun(executionContext.getOptions().getOption(Options.Name.DRY_RUN))
                    .build();
            cursor = executionContext.metricCollector.clock(RelationalMetric.RelationalEvent.EXECUTE_RECORD_QUERY_PLAN, () -> {
                try {
                    return physicalPlan.executePlan(fdbRecordStore, evaluationContext,
                            ContinuationImpl.parseContinuation(executionParameters.getContinuation()).getUnderlyingBytes(),
                            executeProperties);
                } catch (InvalidProtocolBufferException ipbe) {
                    throw ExceptionUtil.toRelationalException(ipbe);
                }
            });
            return executionContext.metricCollector.clock(RelationalMetric.RelationalEvent.CREATE_RESULT_SET_ITERATOR, () -> {
                final ResumableIterator<Row> iterator = RecordLayerIterator.create(cursor, messageFDBQueriedRecord -> new MessageTuple(messageFDBQueriedRecord.getMessage()));
                return new RecordLayerResultSet(metaData, iterator, connection, executionParameters.getParameterHash(),
                        physicalPlan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            });
        }

        public int planHash() {
            return recordQueryPlanHash.get();
        }
    }

    /**
     * This represents a logical query plan that can be executed to produce a {@link java.sql.ResultSet}.
     */
    public static class LogicalQueryPlan extends QueryPlan {

        @Nonnull
        private final RelationalExpression relationalExpression;

        @Nonnull
        private final PlanGenerationContext context;

        @Nonnull
        private final String query;

        @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
        @Nonnull
        private Optional<PhysicalQueryPlan> optimizedPlan;

        private LogicalQueryPlan(@Nonnull final RelationalExpression relationalExpression,
                                 @Nonnull final PlanGenerationContext context,
                                 @Nonnull final String query) {
            super(query);
            this.relationalExpression = relationalExpression;
            this.context = context;
            this.optimizedPlan = Optional.empty();
            this.query = query;
        }

        @Override
        public boolean isUpdatePlan() {
            //TODO(bfines) determine this from the relational expression
            return false;
        }

        @Override
        @Nonnull
        public PhysicalQueryPlan optimize(@Nonnull final CascadesPlanner planner, @Nonnull PlannerConfiguration configuration) throws RelationalException {
            if (optimizedPlan.isPresent()) {
                return optimizedPlan.get();
            }
            return context.getMetricsCollector().clock(RelationalMetric.RelationalEvent.OPTIMIZE_PLAN, () -> {
                final TypeRepository.Builder builder = TypeRepository.newBuilder();
                final Set<Type> usedTypes = UsedTypesProperty.evaluate(relationalExpression);
                usedTypes.forEach(builder::addTypeIfNeeded);
                final var evaluationContext = context.getEvaluationContext();
                final var typedEvaluationContext = EvaluationContext.forBindingsAndTypeRepository(evaluationContext.getBindings(), builder.build());
                final QueryPlanResult planResult;
                try {
                    planResult = planner.planGraph(() ->
                                    GroupExpressionRef.of(relationalExpression),
                            configuration.getReadableIndexes().map(s -> s),
                            IndexQueryabilityFilter.TRUE,
                            typedEvaluationContext);
                } catch (RecordCoreException ex) {
                    throw ExceptionUtil.toRelationalException(ex);
                }

                // The plan itself can introduce new types. Collect those and include them in the type repository stored with the PhysicalQueryPlan
                final RecordQueryPlan recordQueryPlan = planResult.getPlan();
                Set<Type> planTypes = UsedTypesProperty.evaluate(recordQueryPlan);
                planTypes.forEach(builder::addTypeIfNeeded);
                optimizedPlan = Optional.of(new PhysicalQueryPlan(planResult.getPlan(), builder.build(), QueryPlanConstraint.compose(List.of(Objects.requireNonNull(planResult.getPlanInfo().get(QueryPlanInfoKeys.CONSTRAINTS)), getConstraint())), context, query));
                return optimizedPlan.get();
            });
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

        @Nonnull
        @Override
        public String explain() {
            // TODO: We should return something meaningful if `optimize` wasn't called
            //  TODO (Revisit LogicalQueryPlan.explain)
            return optimizedPlan.map(PhysicalQueryPlan::explain).orElse("Logical Query Plan");
        }

        @Override
        public RelationalResultSet executeInternal(@Nonnull final ExecutionContext executionContext) throws RelationalException {
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

        public PlanGenerationContext getGenerationContext() {
            return context;
        }

        @Nonnull
        public static LogicalQueryPlan of(@Nonnull final RelationalExpression relationalExpression,
                                          @Nonnull final PlanGenerationContext context,
                                          @Nonnull final String query) {
            return new LogicalQueryPlan(relationalExpression, context, query);
        }
    }

    public static class MetadataQueryPlan extends QueryPlan {

        @Nonnull
        private final CheckedFunctional<Transaction, RelationalResultSet> query;

        @Nonnull
        private final Type rowType;

        private interface CheckedFunctional<T, R> {
            R apply(T t) throws RelationalException;
        }

        private MetadataQueryPlan(@Nonnull final CheckedFunctional<Transaction, RelationalResultSet> query, @Nonnull final Type rowType) {
            // TODO: TODO (Implement MetadataQueryPlan.explain) (should cover toString as well).
            super("MetadataQueryPlan");
            this.query = query;
            this.rowType = rowType;
        }

        @Override
        public boolean isUpdatePlan() {
            return false;
        }

        @Override
        public Plan<RelationalResultSet> optimize(@Nonnull CascadesPlanner planner, @Nonnull PlannerConfiguration configuration) {
            return this;
        }

        @Override
        public RelationalResultSet executeInternal(@Nonnull final ExecutionContext context) throws RelationalException {
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
        public String explain() {
            // TODO: TODO (Implement MetadataQueryPlan.explain)
            return "MetadataQueryPlan";
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
