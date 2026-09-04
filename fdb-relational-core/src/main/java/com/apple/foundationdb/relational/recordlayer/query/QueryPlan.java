/*
 * QueryPlan.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.PlanHashable.PlanHashMode;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.QueryPlanInfoKeys;
import com.apple.foundationdb.record.query.plan.QueryPlanResult;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.PlannerPhase;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.events.ExecutingTaskPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.InsertIntoMemoPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.events.PlannerEvent.Location;
import com.apple.foundationdb.record.query.plan.cascades.events.PlannerEventStats;
import com.apple.foundationdb.record.query.plan.cascades.events.PlannerEventStatsMaps;
import com.apple.foundationdb.record.query.plan.cascades.events.TransformRuleCallPlannerEvent;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.CompatibleTypeEvolutionPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.DatabaseObjectDependenciesPredicate;
import com.apple.foundationdb.record.query.plan.cascades.explain.ExplainPlanVisitor;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.typing.Typed;
import com.apple.foundationdb.record.query.plan.plans.QueryResult;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryDeletePlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInsertPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUpdatePlan;
import com.apple.foundationdb.record.query.plan.serialization.DefaultPlanSerializationRegistry;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.ImmutableRowStruct;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.ddl.DdlQuery;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;
import com.apple.foundationdb.relational.continuation.CompiledStatement;
import com.apple.foundationdb.relational.recordlayer.ArrayRow;
import com.apple.foundationdb.relational.recordlayer.ContinuationImpl;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.IteratorResultSet;
import com.apple.foundationdb.relational.recordlayer.MessageTuple;
import com.apple.foundationdb.relational.recordlayer.RecordLayerIterator;
import com.apple.foundationdb.relational.recordlayer.RecordLayerResultSet;
import com.apple.foundationdb.relational.recordlayer.RecordLayerSchema;
import com.apple.foundationdb.relational.recordlayer.ResumableIterator;
import com.apple.foundationdb.relational.recordlayer.metadata.DataTypeUtils;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;
import com.apple.foundationdb.relational.util.Assert;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.jspecify.annotations.Nullable;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Collections;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.apple.foundationdb.record.query.plan.cascades.properties.UsedTypesProperty.usedTypes;

public abstract class QueryPlan extends Plan<RelationalResultSet> implements Typed {

    protected QueryPlan(final String query) {
        super(query);
    }

    public static class PhysicalQueryPlan extends QueryPlan {

        private final RecordQueryPlan recordQueryPlan;

        @Nullable
        private final PlannerEventStatsMaps plannerEventStatsMaps;

        private final PlanHashMode currentPlanHashMode;

        private final Supplier<Integer> planHashSupplier;

        private final TypeRepository typeRepository;

        private final QueryPlanConstraint constraint;

        private final QueryPlanConstraint continuationConstraint;

        private final QueryExecutionContext queryExecutionContext;

        /**
         * Semantic type structure captured during semantic analysis.
         * Complete StructType with field names and nested struct type names preserved.
         */
        private final DataType.StructType semanticStructType;

        public PhysicalQueryPlan(final RecordQueryPlan recordQueryPlan,
                                 @Nullable final PlannerEventStatsMaps plannerEventStatsMaps,
                                 final TypeRepository typeRepository,
                                 final QueryPlanConstraint constraint,
                                 final QueryPlanConstraint continuationConstraint,
                                 final QueryExecutionContext queryExecutionContext,
                                 final String query,
                                 final PlanHashMode currentPlanHashMode,
                                 final DataType.StructType semanticStructType) {
            super(query);
            this.recordQueryPlan = recordQueryPlan;
            this.plannerEventStatsMaps = plannerEventStatsMaps;
            this.typeRepository = typeRepository;
            this.constraint = constraint;
            this.continuationConstraint = continuationConstraint;
            this.queryExecutionContext = queryExecutionContext;
            this.currentPlanHashMode = currentPlanHashMode;
            this.planHashSupplier = Suppliers.memoize(() -> recordQueryPlan.planHash(currentPlanHashMode));
            this.semanticStructType = semanticStructType;
        }

        public RecordQueryPlan getRecordQueryPlan() {
            return recordQueryPlan;
        }

        public TypeRepository getTypeRepository() {
            return typeRepository;
        }

        @Override
        public QueryPlanConstraint getConstraint() {
            return constraint;
        }

        public QueryPlanConstraint getContinuationConstraint() {
            return continuationConstraint;
        }

        @Override
        public Type getResultType() {
            // getInnerType() is @Nullable in general, but recordQueryPlan's result type here always has an
            // inner type; Assert.notNullUnchecked can't narrow that since Assert lives in the not-yet-migrated
            // fdb-relational-api module.
            @SuppressWarnings("NullAway")
            final Type innerType = Assert.notNullUnchecked(recordQueryPlan.getResultType().getInnerType());
            return innerType;
        }

        public QueryExecutionContext getQueryExecutionContext() {
            return queryExecutionContext;
        }

        public PlanHashMode getCurrentPlanHashMode() {
            return currentPlanHashMode;
        }

        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        @Override
        public PhysicalQueryPlan withExecutionContext(final QueryExecutionContext queryExecutionContext) {
            if (queryExecutionContext == this.queryExecutionContext) {
                return this;
            }
            return new PhysicalQueryPlan(recordQueryPlan, plannerEventStatsMaps, typeRepository, constraint,
                    continuationConstraint, queryExecutionContext, query, queryExecutionContext.getPlanHashMode(),
                    semanticStructType);
        }

        @Override
        public String explain() {
            final var executeProperties = queryExecutionContext.getExecutionPropertiesBuilder();
            List<String> explainComponents = new ArrayList<>();
            explainComponents.add(ExplainPlanVisitor.toStringForExternalExplain(recordQueryPlan));
            if (executeProperties.getReturnedRowLimit() != ReadTransaction.ROW_LIMIT_UNLIMITED) {
                explainComponents.add("(limit=" + executeProperties.getReturnedRowLimit() + ")");
            }
            if (executeProperties.getSkip() != 0) {
                explainComponents.add("(offset=" + executeProperties.getSkip() + ")");
            }
            return String.join(" ", explainComponents);
        }

        @Override
        public boolean isUpdatePlan() {
            if (this.queryExecutionContext.isForExplain()) {
                return false;
            } else {
                //TODO(bfines) there may be a better way to do this, but I couldn't find it easily
                return recordQueryPlan instanceof RecordQueryInsertPlan ||
                        recordQueryPlan instanceof RecordQueryUpdatePlan ||
                        recordQueryPlan instanceof RecordQueryDeletePlan;
            }
        }

        @Override
        public Plan<RelationalResultSet> optimize(CascadesPlanner planner,
                                                PlanContext planContext,
                                                PlanHashMode currentPlanHashMode) {
            return this;
        }

        @Override
        @SuppressWarnings("PMD.CloseResource") // Connection not owned by this method
        public RelationalResultSet executeInternal(final ExecutionContext executionContext) throws RelationalException {
            if (!(executionContext.connection instanceof EmbeddedRelationalConnection)) {
                //this is required until TODO is resolved
                throw new RelationalException("Cannot execute a QueryPlan without an EmbeddedRelationalConnection", ErrorCode.INTERNAL_ERROR);
            }

            final EmbeddedRelationalConnection conn = (EmbeddedRelationalConnection) executionContext.connection;
            try {
                // Executing a query plan requires a schema to already be selected on the connection.
                final String schemaName = Objects.requireNonNull(conn.getSchema(), "No schema selected on connection");
                try (RecordLayerSchema recordLayerSchema = conn.getRecordLayerDatabase().loadSchema(schemaName)) {
                    final var evaluationContext = queryExecutionContext.getEvaluationContext();
                    final var typedEvaluationContext = EvaluationContext.forBindingsAndTypeRepository(evaluationContext.getBindings(), typeRepository);
                    final ContinuationImpl parsedContinuation;
                    try {
                        parsedContinuation = ContinuationImpl.parseContinuation(queryExecutionContext.getContinuation());
                    } catch (final InvalidProtocolBufferException ipbe) {
                        executionContext.metricCollector.increment(RelationalMetric.RelationalCount.CONTINUATION_REJECTED);
                        throw ExceptionUtil.toRelationalException(ipbe);
                    }
                    if (queryExecutionContext.isForExplain()) {
                        return executeExplain(parsedContinuation, executionContext);
                    } else {
                        return executePhysicalPlan(recordLayerSchema, typedEvaluationContext, executionContext, parsedContinuation);
                    }
                }
            } catch (SQLException sqle) {
                throw new RelationalException(sqle);
            }
        }

        protected <M extends Message> void validatePlanAgainstEnvironment(final ContinuationImpl parsedContinuation,
                                                                          final FDBRecordStoreBase<M> fdbRecordStore,
                                                                          final ExecutionContext executionContext,
                                                                          final Set<PlanHashMode> validPlanHashModes) throws RelationalException {
            PlanValidator.validateHashes(parsedContinuation, executionContext.metricCollector,
                    recordQueryPlan, queryExecutionContext, currentPlanHashMode, validPlanHashModes);

            if (!parsedContinuation.atBeginning()) {
                // if we are here it means that the current execution is from a regular planned plan, i.e. not
                // an EXECUTE CONTINUATION statement but it uses a continuation that is not at the beginning.
                // this can only happen if the query was started without the use of serialized plans, and we are now
                // asked to start issuing them -- push a metric
                executionContext.metricCollector.increment(RelationalMetric.RelationalCount.CONTINUATION_DOWN_LEVEL);
            }
        }

        protected Optional<RecordQueryPlan> getSerializedPlanFromContinuation(ContinuationImpl parsedContinuation,
                                                                              ExecutionContext executionContext) throws RelationalException {
            final var compiledStatement = parsedContinuation.getCompiledStatement();
            if (compiledStatement == null || !compiledStatement.hasPlan() || !compiledStatement.hasPlanSerializationMode()) {
                return Optional.empty();
            }

            try {
                final var planSerializationMode = PlanValidator.validateSerializedPlanSerializationMode(
                        compiledStatement,
                        OptionsUtils.getValidPlanHashModes(executionContext.getOptions())
                );
                final var serializationContext = new PlanSerializationContext(
                        DefaultPlanSerializationRegistry.INSTANCE, planSerializationMode
                );
                return Optional.of(
                        RecordQueryPlan.fromRecordQueryPlanProto(serializationContext, compiledStatement.getPlan()));
            } catch (RelationalException ex) {
                return Optional.empty();
            }
        }

        private RelationalResultSet executeExplain(ContinuationImpl parsedContinuation,
                                                   ExecutionContext executionContext) throws RelationalException {
            final var continuationStructType = DataType.StructType.from(
                    "PLAN_CONTINUATION", List.of(
                            DataType.StructType.Field.from("EXECUTION_STATE", DataType.Primitives.BYTES.type(), 0),
                            DataType.StructType.Field.from("VERSION", DataType.Primitives.INTEGER.type(), 1),
                            DataType.StructType.Field.from("PLAN_HASH_MODE", DataType.Primitives.STRING.type(), 2),
                            DataType.StructType.Field.from("PLAN_HASH", DataType.Primitives.INTEGER.type(), 3),
                            DataType.StructType.Field.from("SERIALIZED_PLAN_COMPLEXITY", DataType.Primitives.INTEGER.type(), 4)),
                    true);
            final var plannerMetricsStructType = DataType.StructType.from(
                    "PLANNER_METRICS", List.of(
                            DataType.StructType.Field.from("TASK_COUNT", DataType.Primitives.LONG.type(), 0),
                            DataType.StructType.Field.from("TASK_TOTAL_TIME_NS", DataType.Primitives.LONG.type(), 1),
                            DataType.StructType.Field.from("TRANSFORM_COUNT", DataType.Primitives.LONG.type(), 2),
                            DataType.StructType.Field.from("TRANSFORM_TIME_NS", DataType.Primitives.LONG.type(), 3),
                            DataType.StructType.Field.from("TRANSFORM_YIELD_COUNT", DataType.Primitives.LONG.type(), 4),
                            DataType.StructType.Field.from("INSERT_TIME_NS", DataType.Primitives.LONG.type(), 5),
                            DataType.StructType.Field.from("INSERT_NEW_COUNT", DataType.Primitives.LONG.type(), 6),
                            DataType.StructType.Field.from("INSERT_REUSED_COUNT", DataType.Primitives.LONG.type(), 7),
                            DataType.StructType.Field.from("TRANSFORM_DISCARDED_INTERSECTION_COMBINATIONS_COUNT", DataType.Primitives.LONG.type(), 8),
                            DataType.StructType.Field.from("TRANSFORM_ALL_INTERSECTION_COMBINATIONS_COUNT", DataType.Primitives.LONG.type(), 9),
                            DataType.StructType.Field.from("REWRITING_PHASE_TASK_COUNT", DataType.Primitives.LONG.type(), 10),
                            DataType.StructType.Field.from("PLANNING_PHASE_TASK_COUNT", DataType.Primitives.LONG.type(), 11),
                            DataType.StructType.Field.from("REWRITING_PHASE_TASKS_TOTAL_TIME_NS", DataType.Primitives.LONG.type(), 12),
                            DataType.StructType.Field.from("PLANNING_PHASE_TASKS_TOTAL_TIME_NS", DataType.Primitives.LONG.type(), 13)),
                    true);
            final var explainStructType = DataType.StructType.from(
                    "EXPLAIN", List.of(
                            DataType.StructType.Field.from("PLAN", DataType.Primitives.STRING.type(), 0),
                            DataType.StructType.Field.from("PLAN_HASH", DataType.Primitives.INTEGER.type(), 1),
                            DataType.StructType.Field.from("PLAN_DOT", DataType.Primitives.STRING.type(), 2),
                            DataType.StructType.Field.from("PLAN_GML", DataType.Primitives.STRING.type(), 3),
                            DataType.StructType.Field.from("PLAN_CONTINUATION", continuationStructType, 4),
                            DataType.StructType.Field.from("PLANNER_METRICS", plannerMetricsStructType, 5)),
                    true);

            // ArrayRow's vararg parameter type isn't @Nullable (NullAway/JSpecify doesn't reliably track
            // element nullability for array/vararg-typed parameters); PLAN_SERIALIZATION_MODE and the plan
            // complexity below are genuinely null in some cases.
            final var compiledStatement = parsedContinuation.getCompiledStatement();
            final Object planSerializationMode = compiledStatement == null ? null : compiledStatement.getPlanSerializationMode();
            @SuppressWarnings("NullAway")
            final ArrayRow continuationRow = new ArrayRow(
                            parsedContinuation.getExecutionState(),
                            parsedContinuation.getVersion(),
                            planSerializationMode,
                            parsedContinuation.getPlanHash(),
                            getSerializedPlanFromContinuation(parsedContinuation, executionContext).map(RecordQueryPlan::getComplexity).orElse(null)
                    );
            final Struct continuationInfo = ContinuationImpl.BEGIN.equals(parsedContinuation) ? null :
                                            new ImmutableRowStruct(continuationRow, RelationalStructMetaData.of(continuationStructType));

            final Struct plannerMetrics;
            if (plannerEventStatsMaps == null) {
                plannerMetrics = null;
            } else {
                final var plannerEventClassStatsMap = plannerEventStatsMaps.getEventClassStatsMap();

                final var aggregateExecutingTasksStats =
                        Optional.ofNullable(plannerEventClassStatsMap.get(ExecutingTaskPlannerEvent.class));
                final var aggregateTransformRuleCallStats =
                        Optional.ofNullable(plannerEventClassStatsMap.get(TransformRuleCallPlannerEvent.class));
                final var aggregateInsertIntoMemoStats =
                        Optional.ofNullable(plannerEventClassStatsMap.get(InsertIntoMemoPlannerEvent.class));

                final var executingTasksStatsForRewritingPhase =
                        plannerEventStatsMaps.getEventWithStateClassStatsMapByPlannerPhase(PlannerPhase.REWRITING)
                                .map(m -> m.get(ExecutingTaskPlannerEvent.class));
                final var executingTasksStatsForPlanningPhase =
                        plannerEventStatsMaps.getEventWithStateClassStatsMapByPlannerPhase(PlannerPhase.PLANNING)
                                .map(m -> m.get(ExecutingTaskPlannerEvent.class));

                plannerMetrics =
                        new ImmutableRowStruct(new ArrayRow(
                                aggregateExecutingTasksStats.map(s -> s.getCount(Location.BEGIN)).orElse(0L),
                                aggregateExecutingTasksStats.map(PlannerEventStats::getTotalTimeInNs).orElse(0L),
                                aggregateTransformRuleCallStats.map(s -> s.getCount(Location.BEGIN)).orElse(0L),
                                aggregateTransformRuleCallStats.map(PlannerEventStats::getOwnTimeInNs).orElse(0L),
                                aggregateTransformRuleCallStats.map(s -> s.getCount(Location.YIELD)).orElse(0L),
                                aggregateInsertIntoMemoStats.map(PlannerEventStats::getOwnTimeInNs).orElse(0L),
                                aggregateInsertIntoMemoStats.map(s -> s.getCount(Location.NEW)).orElse(0L),
                                aggregateInsertIntoMemoStats.map(s -> s.getCount(Location.REUSED)).orElse(0L),
                                aggregateTransformRuleCallStats.map(s -> s.getCount(
                                        Location.DISCARDED_INTERSECTION_COMBINATIONS)).orElse(0L),
                                aggregateTransformRuleCallStats.map(s -> s.getCount(
                                        Location.ALL_INTERSECTION_COMBINATIONS)).orElse(0L),
                                executingTasksStatsForRewritingPhase.map(s -> s.getCount(Location.BEGIN)).orElse(0L),
                                executingTasksStatsForPlanningPhase.map(s -> s.getCount(Location.BEGIN)).orElse(0L),
                                executingTasksStatsForRewritingPhase.map(PlannerEventStats::getTotalTimeInNs).orElse(0L),
                                executingTasksStatsForPlanningPhase.map(PlannerEventStats::getTotalTimeInNs).orElse(0L)
                        ), RelationalStructMetaData.of(plannerMetricsStructType));
            }

            final var plannerGraph = Objects.requireNonNull(recordQueryPlan.acceptVisitor(PlannerGraphVisitor.forExplain()));
            // ArrayRow's vararg parameter type isn't @Nullable (NullAway/JSpecify doesn't reliably track
            // element nullability for array/vararg-typed parameters); continuationInfo and plannerMetrics
            // are genuinely null in some cases (see their assignments above).
            @SuppressWarnings("NullAway")
            final ArrayRow explainRow = new ArrayRow(
                    explain(),
                    planHashSupplier.get(),
                    PlannerGraphVisitor.exportToDot(plannerGraph),
                    PlannerGraphVisitor.exportToGml(plannerGraph, Map.of()),
                    continuationInfo,
                    plannerMetrics);
            return new IteratorResultSet(RelationalStructMetaData.of(explainStructType), Collections.singleton(explainRow).iterator(), 0);
        }

        @SuppressWarnings("PMD.CloseResource") // cursor returned inside the ResultSet, Connection now owned by this method
        private RelationalResultSet executePhysicalPlan(final RecordLayerSchema recordLayerSchema,
                                                        final EvaluationContext evaluationContext,
                                                        final ExecutionContext executionContext,
                                                        final ContinuationImpl parsedContinuation) throws RelationalException {
            final var connection = (EmbeddedRelationalConnection) executionContext.connection;
            // Reassign through notNull (rather than discarding its result) so the non-null check actually
            // narrows the value used below; Assert.notNull can't narrow it on its own since Assert lives in
            // the not-yet-migrated fdb-relational-api module.
            @SuppressWarnings("NullAway")
            final Type type = Assert.notNull(recordQueryPlan.getResultType().getInnerType());
            Assert.that(type instanceof Type.Record, ErrorCode.INTERNAL_ERROR, "unexpected plan returning top-level result of type %s", type.getTypeCode());
            final FDBRecordStoreBase<?> fdbRecordStore = recordLayerSchema.loadStore().unwrap(FDBRecordStoreBase.class);

            final var options = executionContext.getOptions();

            validatePlanAgainstEnvironment(parsedContinuation, fdbRecordStore, executionContext, OptionsUtils.getValidPlanHashModes(options));

            final RecordCursor<QueryResult> cursor;
            final var executeProperties = connection.getExecuteProperties().toBuilder()
                    .setReturnedRowLimit(options.getOption(Options.Name.MAX_ROWS))
                    .setDryRun(options.getOption(Options.Name.DRY_RUN))
                    .build();
            cursor = executionContext.metricCollector.clock(
                    RelationalMetric.RelationalEvent.EXECUTE_RECORD_QUERY_PLAN, () -> recordQueryPlan.executePlan(fdbRecordStore, evaluationContext,
                            parsedContinuation.getExecutionState(),
                            executeProperties));
            final var currentPlanHashMode = OptionsUtils.getCurrentPlanHashMode(options);

            return executionContext.metricCollector.clock(RelationalMetric.RelationalEvent.CREATE_RESULT_SET_ITERATOR, () -> {
                final ResumableIterator<Row> iterator = RecordLayerIterator.create(cursor, messageFDBQueriedRecord -> new MessageTuple(messageFDBQueriedRecord.getMessage()));
                return new RecordLayerResultSet(RelationalStructMetaData.of(semanticStructType), iterator, connection,
                        (continuation, reason) -> enrichContinuation(continuation,
                                currentPlanHashMode, reason));
            });
        }

        private Continuation enrichContinuation(final Continuation continuation,
                                                final PlanHashMode currentPlanHashMode,
                                                final Continuation.Reason reason) throws RelationalException {
            final var continuationBuilder =  ContinuationImpl.copyOf(continuation).asBuilder()
                    .withBindingHash(queryExecutionContext.getParameterHash())
                    .withPlanHash(planHashSupplier.get())
                    .withReason(reason);
            // Do not send the serialized plan unless we can continue with this continuation.
            if (!continuation.atEnd()) {
                //
                // Note that serialization and deserialization of the constituent elements have to done in the same order
                // in order for the dictionary compression for type serialization to work properly. The order is
                // 1. plan
                // 2. arguments -- in the order of appearance in the ordinal table
                // 3. query constraints
                //

                final var serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE, currentPlanHashMode);

                final var literals = queryExecutionContext.getLiterals();
                final var compiledStatementBuilder = CompiledStatement.newBuilder()
                        .setPlanSerializationMode(queryExecutionContext.getPlanHashMode().name());

                compiledStatementBuilder.setPlan(recordQueryPlan.toRecordQueryPlanProto(serializationContext));

                int i = 0;
                for (final var orderedLiteral : literals.getOrderedLiterals()) {
                    final var orderedLiteralProto = orderedLiteral.toProto(serializationContext, i);
                    if (orderedLiteral.isQueryLiteral()) {
                        compiledStatementBuilder.addExtractedLiterals(orderedLiteralProto);
                    } else {
                        compiledStatementBuilder.addArguments(orderedLiteralProto);
                    }
                    i++;
                }

                compiledStatementBuilder.setPlanConstraint(getContinuationConstraint().toProto(serializationContext))
                        .setQueryMetadata(DataTypeUtils.toRecordLayerType(semanticStructType).toTypeProto(serializationContext));
                continuationBuilder.withCompiledStatement(compiledStatementBuilder.build());
            }
            return continuationBuilder.build();
        }

        public int planHash(final PlanHashMode currentPlanHashMode) {
            return recordQueryPlan.planHash(currentPlanHashMode);
        }

    }

    public static class ContinuedPhysicalQueryPlan extends PhysicalQueryPlan {
        private final PlanHashMode serializedPlanHashMode;

        private final Supplier<Integer> serializedPlanHashSupplier;

        public ContinuedPhysicalQueryPlan(final RecordQueryPlan recordQueryPlan,
                                          final TypeRepository typeRepository,
                                          final QueryPlanConstraint continuationConstraint,
                                          final QueryExecutionContext queryExecutionParameters,
                                          final String query,
                                          final PlanHashMode currentPlanHashMode,
                                          final PlanHashMode serializedPlanHashMode,
                                          final DataType.StructType semanticStructType) {
            super(recordQueryPlan, null, typeRepository, QueryPlanConstraint.noConstraint(),
                    continuationConstraint, queryExecutionParameters, query, currentPlanHashMode, semanticStructType);
            this.serializedPlanHashMode = serializedPlanHashMode;
            this.serializedPlanHashSupplier = Suppliers.memoize(() -> recordQueryPlan.planHash(serializedPlanHashMode));
        }

        public PlanHashMode getSerializedPlanHashMode() {
            return serializedPlanHashMode;
        }

        /**
         * Returns a plan with updated execution context.
         *
         * <p>Note: This method is never called in production because ContinuedPhysicalQueryPlan instances
         * are not cached - each EXECUTE CONTINUATION deserializes the plan fresh from the continuation blob.
         * However, we must override to satisfy the Plan interface contract.
         *
         * <p>TODO: Refactor the class hierarchy to eliminate this dead code. Potential approaches:
         * <ul>
         *   <li>Collapse ContinuedPhysicalQueryPlan into PhysicalQueryPlan with a flag</li>
         *   <li>Use composition instead of inheritance</li>
         *   <li>Make Plan.withExecutionContext() optional with default implementation</li>
         * </ul>
         *
         * @param queryExecutionContext The new execution context (ignored - never called)
         * @return This instance (since continuation plans are never cached)
         */
        @Override
        public PhysicalQueryPlan withExecutionContext(final QueryExecutionContext queryExecutionContext) {
            // This method is never called in production - continuation plans bypass the cache.
            // Return this to avoid maintaining dead code.
            return this;
        }

        @Override
        protected <M extends Message> void validatePlanAgainstEnvironment(final ContinuationImpl parsedContinuation,
                                                                          final FDBRecordStoreBase<M> fdbRecordStore,
                                                                          final ExecutionContext executionContext,
                                                                          final Set<PlanHashMode> validPlanHashModes) throws RelationalException {
            // We cannot be at the beginning
            Verify.verify(!parsedContinuation.atBeginning());

            final var metricCollector = executionContext.metricCollector;
            try {
                PlanValidator.validateContinuationConstraint(fdbRecordStore, getContinuationConstraint());
            } catch (final PlanValidator.PlanValidationException pVE) {
                metricCollector.increment(RelationalMetric.RelationalCount.CONTINUATION_REJECTED);
            }

            if (serializedPlanHashMode != getCurrentPlanHashMode()) {
                // we already checked that serializedPlanHashMode is among the valid plan hash modes
                metricCollector.increment(RelationalMetric.RelationalCount.CONTINUATION_DOWN_LEVEL);
            }

            metricCollector.increment(RelationalMetric.RelationalCount.CONTINUATION_ACCEPTED);
        }

        @Override
        protected Optional<RecordQueryPlan> getSerializedPlanFromContinuation(ContinuationImpl parsedContinuation,
                                                                              ExecutionContext executionContext) throws RelationalException {
            Assert.that(
                    Objects.requireNonNull(parsedContinuation.getPlanHash()).equals(serializedPlanHashSupplier.get()),
                    ErrorCode.INTERNAL_ERROR,
                    "unexpected mismatch between deserialized plan hash and continuation plan hash");
            // No need to deserialize the plan from the continuation again
            return Optional.of(getRecordQueryPlan());
        }
    }

    /**
     * This represents a logical query plan that can be executed to produce a {@link java.sql.ResultSet}.
     */
    public static class LogicalQueryPlan extends QueryPlan {

        private final RelationalExpression relationalExpression;

        private final MutablePlanGenerationContext context;

        private final String query;

        /**
         * Semantic type structure captured during semantic analysis.
         * Preserves struct type names - will be merged with planner field names after planning.
         */
        private final DataType.StructType semanticStructType;

        @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
        private Optional<PhysicalQueryPlan> optimizedPlan;

        private LogicalQueryPlan(final RelationalExpression relationalExpression,
                                 final MutablePlanGenerationContext context,
                                 final String query,
                                 final DataType.StructType semanticStructType) {
            super(query);
            this.relationalExpression = relationalExpression;
            this.context = context;
            this.optimizedPlan = Optional.empty();
            this.query = query;
            this.semanticStructType = semanticStructType;
        }

        @Override
        public boolean isUpdatePlan() {
            //TODO(bfines) determine this from the relational expression
            return false;
        }

        @Override
        public PhysicalQueryPlan optimize(CascadesPlanner planner, PlanContext planContext,
                                          PlanHashMode currentPlanHashMode) throws RelationalException {
            if (optimizedPlan.isPresent()) {
                return optimizedPlan.get();
            }
            return planContext.getMetricsCollector().clock(RelationalMetric.RelationalEvent.OPTIMIZE_PLAN, () -> {
                final TypeRepository.Builder builder = TypeRepository.newBuilder();
                final Set<Type> usedTypes = usedTypes().evaluate(relationalExpression);
                usedTypes.forEach(builder::addTypeIfNeeded);
                final var evaluationContext = context.getEvaluationContext();
                final var typedEvaluationContext = EvaluationContext.forBindingsAndTypeRepository(evaluationContext.getBindings(), builder.build());
                final QueryPlanResult planResult;
                try {
                    planResult = planner.planGraph(() ->
                                    Reference.initialOf(relationalExpression),
                            planContext.getReadableIndexes().map(s -> s),
                            IndexQueryabilityFilter.TRUE,
                            typedEvaluationContext);
                } catch (RecordCoreException ex) {
                    throw ExceptionUtil.toRelationalException(ex);
                }

                final var statsMaps = planResult.getPlanInfo().get(QueryPlanInfoKeys.STATS_MAPS);

                // The plan itself can introduce new types. Collect those and include them in the type repository stored with the PhysicalQueryPlan
                final RecordQueryPlan plannedPlan = planResult.getPlan();
                final RecordQueryPlan minimizedPlan = plannedPlan.minimize();

                Set<Type> planTypes = usedTypes().evaluate(minimizedPlan);
                planTypes.forEach(builder::addTypeIfNeeded);
                final var constraint = QueryPlanConstraint.composeConstraints(
                        List.of(Objects.requireNonNull(planResult.getPlanInfo().get(QueryPlanInfoKeys.CONSTRAINTS)),
                                getConstraint()));
                final QueryPlanConstraint continuationConstraint =
                        computeContinuationPlanConstraint(planContext.getMetaData(), plannedPlan);

                optimizedPlan = Optional.of(
                        new PhysicalQueryPlan(minimizedPlan, statsMaps, builder.build(),
                                constraint, continuationConstraint, context, query, currentPlanHashMode,
                                semanticStructType));
                return optimizedPlan.get();
            });
        }

        @Override
        public QueryPlanConstraint getConstraint() {
            return context.getPlanConstraintsForLiteralReferences();
        }

        @Override
        public Plan<RelationalResultSet> withExecutionContext(final QueryExecutionContext queryExecutionContext) {
            return this;
        }

        @Override
        public String explain() {
            // TODO: We should return something meaningful if `optimize` wasn't called
            //  TODO (Revisit LogicalQueryPlan.explain)
            return optimizedPlan.map(PhysicalQueryPlan::explain).orElse("Logical Query Plan");
        }

        @Override
        public RelationalResultSet executeInternal(final ExecutionContext executionContext) throws RelationalException {
            return this.optimizedPlan.get().execute(executionContext);
        }

        @Override
        public Type getResultType() {
            return relationalExpression.getResultType();
        }

        public RelationalExpression getRelationalExpression() {
            return relationalExpression;
        }

        public MutablePlanGenerationContext getGenerationContext() {
            return context;
        }

        public static LogicalQueryPlan of(final RelationalExpression relationalExpression,
                                          final MutablePlanGenerationContext context,
                                          final String query,
                                          final DataType.StructType semanticStructType) {
            return new LogicalQueryPlan(relationalExpression, context, query, semanticStructType);
        }

        private static QueryPlanConstraint computeContinuationPlanConstraint(final RecordMetaData recordMetaData,
                                                                             final RecordQueryPlan plannedPlan) {
            // this plan was planned -- we need to compute the plan constraints
            final var compatibleTypeEvolutionPredicate =
                    CompatibleTypeEvolutionPredicate.fromPlan(plannedPlan);
            final var databaseObjectDependenciesPredicate =
                    DatabaseObjectDependenciesPredicate.fromPlan(recordMetaData, plannedPlan);
            return QueryPlanConstraint.ofPredicates(ImmutableList.of(compatibleTypeEvolutionPredicate,
                    databaseObjectDependenciesPredicate));
        }
    }

    public static class MetadataQueryPlan extends QueryPlan {

        private final CheckedFunctional<Transaction, RelationalResultSet> query;

        private final Type rowType;

        private interface CheckedFunctional<T, R> {
            R apply(T t) throws RelationalException;
        }

        private MetadataQueryPlan(final CheckedFunctional<Transaction, RelationalResultSet> query, final Type rowType) {
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
        public Plan<RelationalResultSet> optimize(CascadesPlanner planner,
                                                PlanContext planContext,
                                                PlanHashMode currentPlanHashMode) {
            return this;
        }

        @Override
        public RelationalResultSet executeInternal(final ExecutionContext context) throws RelationalException {
            return query.apply(context.transaction);
        }

        @Override
        public QueryPlanConstraint getConstraint() {
            return QueryPlanConstraint.noConstraint();
        }

        @Override
        public Plan<RelationalResultSet> withExecutionContext(QueryExecutionContext queryExecutionContext) {
            return this;
        }

        @Override
        public String explain() {
            // TODO: TODO (Implement MetadataQueryPlan.explain)
            return "MetadataQueryPlan";
        }

        @Override
        public Type getResultType() {
            return rowType;
        }

        public static MetadataQueryPlan of(DdlQuery ddlQuery) {
            return new MetadataQueryPlan(ddlQuery::executeAction, ddlQuery.getResultSetMetadata());
        }
    }
}
