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
import com.apple.foundationdb.record.PlanHashable.PlanHashMode;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordQueryPlanProto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.IndexQueryabilityFilter;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.QueryPlanInfoKeys;
import com.apple.foundationdb.record.query.plan.QueryPlanResult;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphProperty;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.predicates.CompatibleTypeEvolutionPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.DatabaseObjectDependenciesPredicate;
import com.apple.foundationdb.record.query.plan.cascades.properties.UsedTypesProperty;
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
import com.apple.foundationdb.relational.api.FieldDescription;
import com.apple.foundationdb.relational.api.ImmutableRowStruct;
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
import com.apple.foundationdb.relational.continuation.CompiledStatement;
import com.apple.foundationdb.relational.continuation.TypedQueryArgument;
import com.apple.foundationdb.relational.recordlayer.ArrayRow;
import com.apple.foundationdb.relational.recordlayer.ContinuationImpl;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.IteratorResultSet;
import com.apple.foundationdb.relational.recordlayer.MessageTuple;
import com.apple.foundationdb.relational.recordlayer.RecordLayerIterator;
import com.apple.foundationdb.relational.recordlayer.RecordLayerResultSet;
import com.apple.foundationdb.relational.recordlayer.RecordLayerSchema;
import com.apple.foundationdb.relational.recordlayer.ResumableIterator;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;
import com.apple.foundationdb.relational.util.Assert;

import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import java.sql.DatabaseMetaData;
import java.sql.Struct;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
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

        @Nonnull
        private final PlanHashMode currentPlanHashMode;

        private final Supplier<Integer> planHashSupplier;

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
                                 @Nonnull final String query,
                                 @Nonnull final PlanHashMode currentPlanHashMode) {
            super(query);
            this.recordQueryPlan = recordQueryPlan;
            this.typeRepository = typeRepository;
            this.constraint = constraint;
            this.queryExecutionParameters = queryExecutionParameters;
            this.currentPlanHashMode = currentPlanHashMode;
            this.planHashSupplier = Suppliers.memoize(() -> recordQueryPlan.planHash(currentPlanHashMode));
        }

        @Nonnull
        public RecordQueryPlan getRecordQueryPlan() {
            return recordQueryPlan;
        }

        @Nonnull
        public TypeRepository getTypeRepository() {
            return typeRepository;
        }

        @Override
        @Nonnull
        public QueryPlanConstraint getConstraint() {
            return constraint;
        }

        @Nonnull
        @Override
        public Type getResultType() {
            return Assert.notNullUnchecked(recordQueryPlan.getResultType().getInnerType());
        }

        @Nonnull
        public QueryExecutionParameters getQueryExecutionParameters() {
            return queryExecutionParameters;
        }

        @Nonnull
        public PlanHashMode getCurrentPlanHashMode() {
            return currentPlanHashMode;
        }

        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        @Override
        @Nonnull
        public PhysicalQueryPlan withQueryExecutionParameters(@Nonnull final QueryExecutionParameters parameters) {
            if (parameters == this.queryExecutionParameters) {
                return this;
            }
            return new PhysicalQueryPlan(recordQueryPlan, typeRepository, constraint, parameters, query, parameters.getPlanHashMode());
        }

        @Nonnull
        @Override
        public String explain() {
            final var executeProperties = queryExecutionParameters.getExecutionPropertiesBuilder();
            List<String> explainComponents = new ArrayList<>();
            explainComponents.add(recordQueryPlan.toString());
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
        public Plan<RelationalResultSet> optimize(@Nonnull final CascadesPlanner planner,
                                                @Nonnull final PlannerConfiguration configuration,
                                                @Nonnull final PlanHashMode currentPlanHashMode) {
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
                final ContinuationImpl parsedContinuation;
                try {
                    parsedContinuation = ContinuationImpl.parseContinuation(queryExecutionParameters.getContinuation());
                } catch (final InvalidProtocolBufferException ipbe) {
                    executionContext.metricCollector.increment(RelationalMetric.RelationalCount.CONTINUATION_REJECTED);
                    throw ExceptionUtil.toRelationalException(ipbe);
                }
                if (queryExecutionParameters.isForExplain()) {
                    return executeExplain(parsedContinuation);
                } else {
                    return executePhysicalPlan(recordLayerSchema, typedEvaluationContext, executionContext, parsedContinuation);
                }
            }
        }

        protected <M extends Message> void validatePlanAgainstEnvironment(@Nonnull final ContinuationImpl parsedContinuation,
                                                                          @Nonnull final FDBRecordStoreBase<M> fdbRecordStore,
                                                                          @Nonnull final ExecutionContext executionContext,
                                                                          @Nonnull final Set<PlanHashMode> validPlanHashModes) throws RelationalException {
            PlanValidator.validateHashes(parsedContinuation, executionContext.metricCollector,
                    recordQueryPlan, queryExecutionParameters, currentPlanHashMode, validPlanHashModes);

            final var options = executionContext.getOptions();
            final var continuationsContainCompiledStatements = getContinuationsContainsCompiledStatements(options);
            if (continuationsContainCompiledStatements && !parsedContinuation.atBeginning()) {
                // if we are here it means that the current execution is from a regular planned plan, i.e. not
                // an EXECUTE CONTINUATION statement but it uses a continuation that is not at the beginning.
                // this can only happen if the query was started without the use of serialized plans, and we are now
                // asked to start issuing them -- push a metric
                executionContext.metricCollector.increment(RelationalMetric.RelationalCount.CONTINUATION_DOWN_LEVEL);
            }
        }

        @Nonnull
        private RelationalResultSet executeExplain(@Nonnull ContinuationImpl parsedContinuation) {
            StructMetaData continuationMetadata = new RelationalStructMetaData(
                    FieldDescription.primitive("EXECUTION_STATE", Types.BINARY, DatabaseMetaData.columnNoNulls),
                    FieldDescription.primitive("VERSION", Types.INTEGER, DatabaseMetaData.columnNoNulls),
                    FieldDescription.primitive("PLAN_HASH_MODE", Types.VARCHAR, DatabaseMetaData.columnNoNulls)
            );
            StructMetaData metaData = new RelationalStructMetaData(
                    FieldDescription.primitive("PLAN", Types.VARCHAR, DatabaseMetaData.columnNoNulls),
                    FieldDescription.primitive("PLAN_HASH", Types.INTEGER, DatabaseMetaData.columnNoNulls),
                    FieldDescription.primitive("PLAN_DOT", Types.VARCHAR, DatabaseMetaData.columnNoNulls),
                    FieldDescription.struct("PLAN_CONTINUATION", DatabaseMetaData.columnNoNulls, continuationMetadata)
            );
            final Struct continuationInfo = parsedContinuation == ContinuationImpl.BEGIN ? null :
                    new ImmutableRowStruct(new ArrayRow(
                            parsedContinuation.getExecutionState(),
                            parsedContinuation.getVersion(),
                            parsedContinuation.getCompiledStatement() == null ? null : parsedContinuation.getCompiledStatement().getPlanSerializationMode()
                    ), continuationMetadata);

            return new IteratorResultSet(metaData, Collections.singleton(new ArrayRow(
                    explain(),
                    planHashSupplier.get(),
                    PlannerGraphProperty.exportToDot(Objects.requireNonNull(recordQueryPlan.acceptPropertyVisitor(PlannerGraphProperty.forExplain()))),
                    continuationInfo)).iterator(), 0);
        }

        @Nonnull
        private RelationalResultSet executePhysicalPlan(@Nonnull final RecordLayerSchema recordLayerSchema,
                                                      @Nonnull final EvaluationContext evaluationContext,
                                                      @Nonnull final ExecutionContext executionContext,
                                                      @Nonnull final ContinuationImpl parsedContinuation) throws RelationalException {
            final var connection = (EmbeddedRelationalConnection) executionContext.connection;
            Type type = recordQueryPlan.getResultType().getInnerType();
            Assert.notNull(type);
            Assert.that(type instanceof Type.Record, ErrorCode.INTERNAL_ERROR, "unexpected plan returning top-level result of type %s", type.getTypeCode());
            final FDBRecordStoreBase<Message> fdbRecordStore = recordLayerSchema.loadStore().unwrap(FDBRecordStoreBase.class);

            final var options = executionContext.getOptions();

            validatePlanAgainstEnvironment(parsedContinuation, fdbRecordStore, executionContext, getValidPlanHashModes(options));

            final RecordCursor<QueryResult> cursor;
            final var executeProperties = connection.getExecuteProperties().toBuilder()
                    .setSkip(queryExecutionParameters.getExecutionPropertiesBuilder().getSkip())
                    .setReturnedRowLimit(queryExecutionParameters.getExecutionPropertiesBuilder().getReturnedRowLimit())
                    .setDryRun(options.getOption(Options.Name.DRY_RUN))
                    .build();
            cursor = executionContext.metricCollector.clock(
                    RelationalMetric.RelationalEvent.EXECUTE_RECORD_QUERY_PLAN, () -> recordQueryPlan.executePlan(fdbRecordStore, evaluationContext,
                            parsedContinuation.getExecutionState(),
                            executeProperties));
            final var currentPlanHashMode = getCurrentPlanHashMode(options);
            final var metaData = SqlTypeSupport.typeToMetaData(type);
            return executionContext.metricCollector.clock(RelationalMetric.RelationalEvent.CREATE_RESULT_SET_ITERATOR, () -> {
                final ResumableIterator<Row> iterator = RecordLayerIterator.create(cursor, messageFDBQueriedRecord -> new MessageTuple(messageFDBQueriedRecord.getMessage()));
                return new RecordLayerResultSet(metaData, iterator, connection,
                        continuation -> enrichContinuation(continuation, fdbRecordStore.getRecordMetaData(),
                                currentPlanHashMode, getContinuationsContainsCompiledStatements(options)));
            });
        }

        @Nonnull
        private RecordQueryPlanProto.PRecordQueryPlan computePlanProto(@Nonnull final PlanHashMode currentPlanHashMode) {
            final PlanSerializationContext serializationContext =
                    new PlanSerializationContext(new DefaultPlanSerializationRegistry(), currentPlanHashMode);
            return recordQueryPlan.toRecordQueryPlanProto(serializationContext);
        }

        @Nonnull
        private Continuation enrichContinuation(@Nonnull final Continuation continuation,
                                                @Nonnull final RecordMetaData recordMetaData,
                                                @Nonnull final PlanHashMode currentPlanHashMode,
                                                final boolean serializeCompiledStatement) throws RelationalException {
            final var continuationBuilder =  ContinuationImpl.copyOf(continuation).asBuilder()
                    .withBindingHash(queryExecutionParameters.getParameterHash())
                    .withPlanHash(planHashSupplier.get());
            // Do not send the serialized plan unless we can continue with this continuation.
            if (serializeCompiledStatement && !continuation.atEnd()) {
                final var serializationContext = new PlanSerializationContext(new DefaultPlanSerializationRegistry(), currentPlanHashMode);
                final var literals = queryExecutionParameters.getLiterals();
                final var compiledStatementBuilder = CompiledStatement.newBuilder()
                        .setPlanSerializationMode(queryExecutionParameters.getPlanHashMode().name());
                int i = 0;
                for (final var orderedLiteral : literals.getOrderedLiterals()) {
                    final var type = orderedLiteral.getType();
                    final var argumentBuilder = TypedQueryArgument.newBuilder()
                            .setType(type.toTypeProto(serializationContext))
                            .setLiteralsTableIndex(i)
                            .setTokenIndex(orderedLiteral.getTokenIndex());
                    argumentBuilder.setObject(LiteralsUtils.objectToliteralObjectProto(type, orderedLiteral.getLiteralObject()));
                    if (orderedLiteral.isQueryLiteral()) {
                        // literal
                        compiledStatementBuilder.addExtractedLiterals(argumentBuilder.build());
                    } else {
                        // actual parameter
                        Verify.verify(orderedLiteral.isNamedParameter() || orderedLiteral.isUnnamedParameter());
                        if (orderedLiteral.isNamedParameter()) {
                            argumentBuilder.setParameterName(Objects.requireNonNull(orderedLiteral.getParameterName()));
                        } else {
                            argumentBuilder.setUnnamedParameterIndex(Objects.requireNonNull(orderedLiteral.getUnnamedParameterIndex()));
                        }
                        compiledStatementBuilder.addArguments(argumentBuilder.build());
                    }
                    i++;
                }
                compiledStatementBuilder.setPlan(computePlanProto(currentPlanHashMode));

                compiledStatementBuilder.setPlanConstraint(
                        getContinuationPlanConstraint(recordMetaData).toProto(serializationContext));

                continuationBuilder.withCompiledStatement(compiledStatementBuilder.build());
            }
            return continuationBuilder.build();
        }

        @Nonnull
        protected QueryPlanConstraint getContinuationPlanConstraint(@Nonnull RecordMetaData recordMetaData) {
            // this plan was planned -- we need to compute the plan constraints
            final var compatibleTypeEvolutionPredicate =
                    CompatibleTypeEvolutionPredicate.fromPlan(recordQueryPlan);
            final var databaseObjectDependenciesPredicate =
                    DatabaseObjectDependenciesPredicate.fromPlan(recordMetaData, recordQueryPlan);
            return QueryPlanConstraint.ofPredicates(ImmutableList.of(compatibleTypeEvolutionPredicate,
                    databaseObjectDependenciesPredicate));
        }

        public int planHash(@Nonnull final PlanHashMode currentPlanHashMode) {
            return recordQueryPlan.planHash(currentPlanHashMode);
        }

        @Nonnull
        public static PlanHashMode getCurrentPlanHashMode(@Nonnull final Options options) {
            final String planHashModeAsString = options.getOption(Options.Name.CURRENT_PLAN_HASH_MODE);
            return planHashModeAsString == null ?
                    PlanHashable.CURRENT_FOR_CONTINUATION :
                    PlanHashMode.valueOf(planHashModeAsString);
        }

        @Nonnull
        public static Set<PlanHashMode> getValidPlanHashModes(@Nonnull final Options options) {
            final String planHashModesAsString = options.getOption(Options.Name.VALID_PLAN_HASH_MODES);
            if (planHashModesAsString == null) {
                return ImmutableSet.of(PlanHashable.CURRENT_FOR_CONTINUATION);
            }

            return Arrays.stream(planHashModesAsString.split(","))
                    .map(planHashModeAsString -> PlanHashMode.valueOf(planHashModeAsString.trim()))
                    .collect(ImmutableSet.toImmutableSet());
        }

        public static boolean getContinuationsContainsCompiledStatements(@Nonnull final Options options) {
            return Objects.requireNonNull(options.getOption(Options.Name.CONTINUATIONS_CONTAIN_COMPILED_STATEMENTS));
        }
    }

    public static class ContinuedPhysicalQueryPlan extends PhysicalQueryPlan {
        @Nonnull
        private final PlanHashMode serializedPlanHashMode;

        public ContinuedPhysicalQueryPlan(@Nonnull final RecordQueryPlan recordQueryPlan,
                                          @Nonnull final TypeRepository typeRepository,
                                          @Nonnull final QueryPlanConstraint constraint,
                                          @Nonnull final QueryExecutionParameters queryExecutionParameters,
                                          @Nonnull final String query,
                                          @Nonnull final PlanHashMode currentPlanHashMode,
                                          @Nonnull final PlanHashMode serializedPlanHashMode) {
            super(recordQueryPlan, typeRepository, constraint, queryExecutionParameters, query, currentPlanHashMode);
            this.serializedPlanHashMode = serializedPlanHashMode;
        }

        @Nonnull
        public PlanHashMode getSerializedPlanHashMode() {
            return serializedPlanHashMode;
        }

        @SuppressWarnings("PMD.CompareObjectsWithEquals")
        @Override
        @Nonnull
        public PhysicalQueryPlan withQueryExecutionParameters(@Nonnull final QueryExecutionParameters parameters) {
            if (parameters == this.getQueryExecutionParameters()) {
                return this;
            }
            return new ContinuedPhysicalQueryPlan(getRecordQueryPlan(), getTypeRepository(), getConstraint(),
                    parameters, query, parameters.getPlanHashMode(), getSerializedPlanHashMode());
        }

        @Override
        protected <M extends Message> void validatePlanAgainstEnvironment(@Nonnull final ContinuationImpl parsedContinuation,
                                                                          @Nonnull final FDBRecordStoreBase<M> fdbRecordStore,
                                                                          @Nonnull final ExecutionContext executionContext,
                                                                          @Nonnull final Set<PlanHashMode> validPlanHashModes) throws RelationalException {
            // We cannot be at the beginning
            Verify.verify(!parsedContinuation.atBeginning());

            final var metricCollector = executionContext.metricCollector;
            try {
                PlanValidator.validateContinuationConstraint(fdbRecordStore, getConstraint());
            } catch (final PlanValidator.PlanValidationException pVE) {
                metricCollector.increment(RelationalMetric.RelationalCount.CONTINUATION_REJECTED);
            }

            if (serializedPlanHashMode != getCurrentPlanHashMode()) {
                // we already checked that serializedPlanHashMode is among the valid plan hash modes
                metricCollector.increment(RelationalMetric.RelationalCount.CONTINUATION_DOWN_LEVEL);
            }

            metricCollector.increment(RelationalMetric.RelationalCount.CONTINUATION_ACCEPTED);
        }

        @Nonnull
        @Override
        protected QueryPlanConstraint getContinuationPlanConstraint(@Nonnull final RecordMetaData recordMetaData) {
            return getConstraint();
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
        public PhysicalQueryPlan optimize(@Nonnull final CascadesPlanner planner, @Nonnull PlannerConfiguration configuration,
                                          @Nonnull final PlanHashMode currentPlanHashMode) throws RelationalException {
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
                                    Reference.of(relationalExpression),
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
                optimizedPlan = Optional.of(
                        new PhysicalQueryPlan(planResult.getPlan(), builder.build(),
                                QueryPlanConstraint.compose(List.of(Objects.requireNonNull(planResult.getPlanInfo().get(QueryPlanInfoKeys.CONSTRAINTS)), getConstraint())),
                                context, query, currentPlanHashMode));
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
        public Plan<RelationalResultSet> withQueryExecutionParameters(@Nonnull final QueryExecutionParameters parameters) {
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
        public Plan<RelationalResultSet> optimize(@Nonnull final CascadesPlanner planner,
                                                @Nonnull final PlannerConfiguration configuration,
                                                @Nonnull final PlanHashMode currentPlanHashMode) {
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
