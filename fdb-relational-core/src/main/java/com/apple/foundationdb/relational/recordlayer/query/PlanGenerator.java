/*
 * PlanGenerator.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.IndexMatchCandidateRegistry;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.StableSelectorCostModel;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.serialization.DefaultPlanSerializationRegistry;
import com.apple.foundationdb.record.util.ProtoUtils;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;
import com.apple.foundationdb.relational.continuation.CompiledStatement;
import com.apple.foundationdb.relational.continuation.TypedQueryArgument;
import com.apple.foundationdb.relational.recordlayer.ContinuationImpl;
import com.apple.foundationdb.relational.recordlayer.metadata.DataTypeUtils;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.query.cache.PhysicalPlanEquivalence;
import com.apple.foundationdb.relational.recordlayer.query.cache.RelationalPlanCache;
import com.apple.foundationdb.relational.recordlayer.query.visitors.BaseVisitor;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.RelationalLoggingUtil;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.apple.foundationdb.record.query.plan.cascades.properties.UsedTypesProperty.usedTypes;

/**
 * The plan generator, responsible for creating execution plans from SQL statements.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
@API(API.Status.EXPERIMENTAL)
public final class PlanGenerator {
    private static final Logger logger = LogManager.getLogger(PlanGenerator.class);

    /**
     * An optional plan cache used to improve performance by storing execution plans.
     * This allows the system to reuse plans for identical queries, avoiding redundant planning.
     */
    @Nonnull
    private final Optional<RelationalPlanCache> cache;

    @Nonnull
    private final CascadesPlanner planner;

    @Nonnull
    private final PlanContext planContext;

    @Nonnull
    private Options options;

    private long beginTime = System.nanoTime();

    private long currentTime = beginTime;

    private PlanGenerator(@Nonnull final Optional<RelationalPlanCache> cache,
                          @Nonnull final PlanContext planContext,
                          @Nonnull final CascadesPlanner planner,
                          @Nonnull final Options options) {
        this.cache = cache;
        this.planContext = planContext;
        this.planner = planner;
        this.options = options;
    }

    /**
     * Returns a {@link Plan} for the given SQL query, representing the execution plan.
     * If a plan cache is enabled, this method first attempts to retrieve a pre-existing plan from the cache.
     * If a plan is found in the cache, it is returned directly, avoiding plan generation.
     * If no plan is found in the cache, a new plan is generated and stored in the cache for future use.
     *
     * @param query The SQL query to generator the plan for.
     * @return a corresponding {@link Plan}.
     * @throws RelationalException If planning was unsuccessful.
     */
    @Nonnull
    public Plan<?> getPlan(@Nonnull final String query) throws RelationalException {
        resetTimer();
        KeyValueLogMessage message = KeyValueLogMessage.build("PlanGenerator");
        Plan<?> plan = null;
        RelationalException exception = null;
        try {
            plan = planContext.getMetricsCollector().clock(RelationalMetric.RelationalEvent.TOTAL_GET_PLAN_QUERY, () -> getPlanInternal(query, message));
        } catch (RelationalException e) {
            exception = e;
            throw e;
        } finally {
            RelationalLoggingUtil.publishPlanGenerationLogs(logger, message, plan, exception, totalTimeMicros(), options);
        }
        return plan;
    }

    private boolean isCaseSensitive() {
        return options.getOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS);
    }

    @Nonnull
    private Plan<?> getPlanInternal(@Nonnull String query, @Nonnull KeyValueLogMessage message) throws RelationalException {
        try {
            // parse query, generate AST, extract literals from AST, hash it w.r.t. prepared parameters, and identify query caching behavior flags
            final Set<PlanHashable.PlanHashMode> validPlanHashModes = OptionsUtils.getValidPlanHashModes(options);
            final PlanHashable.PlanHashMode currentPlanHashMode = OptionsUtils.getCurrentPlanHashMode(options);
            final var astHashResult = AstNormalizer.normalizeQuery(planContext, query, isCaseSensitive(), currentPlanHashMode);
            RelationalLoggingUtil.publishNormalizeQueryLogs(message, stepTimeMicros(), astHashResult.getQueryCacheKey().getHash(),
                    astHashResult.getQueryCacheKey().getCanonicalQueryString());
            options = options.withChild(astHashResult.getQueryOptions());

            // shortcut plan cache if the query is determined not-cacheable or the cache is not set (disabled).
            if (shouldNotCache(astHashResult.getQueryCachingFlags()) || cache.isEmpty()) {
                Plan<?> plan = generatePhysicalPlan(astHashResult, validPlanHashModes, currentPlanHashMode);
                RelationalLoggingUtil.publishPlanCacheLogs(message, RelationalLoggingUtil.PlanCacheEvent.SKIP, stepTimeMicros(), 0);
                return plan;
            }

            // Default is to cache hit. This is modified later if we cache miss
            RelationalLoggingUtil.publishPlanCacheLogs(message, RelationalLoggingUtil.PlanCacheEvent.HIT, -1, cache.get().getStats().numEntries());

            // otherwise, lookup the query in the cache
            final var planEquivalence = PhysicalPlanEquivalence.of(astHashResult.getQueryExecutionContext().getEvaluationContext());
            return planContext.getMetricsCollector().clock(RelationalMetric.RelationalEvent.CACHE_LOOKUP, () ->
                    cache.get().reduce(
                            astHashResult.getSchemaTemplateName(),
                            astHashResult.getQueryCacheKey(),
                            planEquivalence,
                            () -> {
                                final Plan<?> physicalPlan;
                                try {
                                    physicalPlan = generatePhysicalPlan(astHashResult, validPlanHashModes, currentPlanHashMode);
                                } catch (final RelationalException vE) {
                                    throw vE.toUncheckedWrappedException();
                                }
                                RelationalLoggingUtil.publishPlanCacheLogs(message, RelationalLoggingUtil.PlanCacheEvent.MISS, stepTimeMicros(), cache.get().getStats().numEntries());
                                return NonnullPair.of(planEquivalence.withConstraint(physicalPlan.getConstraint()), physicalPlan);
                            },
                            value -> value.withExecutionContext(astHashResult.getQueryExecutionContext()),
                            plans -> plans.reduce(null, (acc, candidate) -> {
                                if (candidate instanceof QueryPlan.PhysicalQueryPlan) {
                                    final var candidatePhysicalPlan = Assert.castUnchecked(candidate, QueryPlan.PhysicalQueryPlan.class);
                                    final var candidateQueryPlan = candidatePhysicalPlan.getRecordQueryPlan();
                                    final var bestQueryPlanSoFar = acc == null ? null : Assert.castUnchecked(acc, QueryPlan.PhysicalQueryPlan.class).getRecordQueryPlan();
                                    if (bestQueryPlanSoFar == null || new StableSelectorCostModel().compare(candidateQueryPlan, bestQueryPlanSoFar) < 0) {
                                        return candidate;
                                    } else {
                                        return acc;
                                    }
                                } else {
                                    return candidate;
                                }
                            }),
                            e -> planContext.getMetricsCollector().increment(e)
                    )
            );
        } catch (UncheckedRelationalException uve) {
            throw uve.unwrap();
        } catch (MetaDataException mde) {
            // we need a better way for translating error codes between record layer and Relational SQL error codes
            throw new RelationalException(mde.getMessage(), ErrorCode.SYNTAX_OR_ACCESS_VIOLATION, mde);
        } catch (VerifyException | SemanticException ve) {
            throw new RelationalException(ve.getMessage(), ErrorCode.INTERNAL_ERROR, ve);
        } catch (SQLException e) {
            throw ExceptionUtil.toRelationalException(e);
        }
    }

    @Nonnull
    private Plan<?> generatePhysicalPlan(@Nonnull AstNormalizer.NormalizationResult ast,
                                         @Nonnull Set<PlanHashable.PlanHashMode> validPlanHashModes,
                                         @Nonnull PlanHashable.PlanHashMode currentPlanHashMode) throws RelationalException {
        if (ast.getQueryCachingFlags().contains(AstNormalizer.NormalizationResult.QueryCachingFlags.IS_EXECUTE_CONTINUATION_STATEMENT)) {
            return planContext.getMetricsCollector().clock(RelationalMetric.RelationalEvent.GENERATE_CONTINUED_PLAN, () ->
                    generatePhysicalPlanForExecuteContinuation(ast, validPlanHashModes, currentPlanHashMode));
        } else {
            return generatePhysicalPlanForCompilableStatement(ast, isCaseSensitive(), currentPlanHashMode);
        }
    }

    @Nonnull
    public PlannerConfiguration getPlannerConfigurations() {
        return planContext.getPlannerConfiguration();
    }

    @Nonnull
    private Plan<?> generatePhysicalPlanForCompilableStatement(@Nonnull AstNormalizer.NormalizationResult ast,
                                                               boolean caseSensitive,
                                                               @Nonnull PlanHashable.PlanHashMode currentPlanHashMode) {
        // The hash value used accounts for the values that identify the query and not part of the execution context (e.g.
        // literal and parameter values without LIMIT and CONTINUATION)
        final var parameterHash = ast.getQueryExecutionContext().getParameterHash();

        final var planGenerationContext = new MutablePlanGenerationContext(planContext.getPreparedStatementParameters(),
                currentPlanHashMode, ast.getQuery(), ast.getQueryCacheKey().getCanonicalQueryString(), parameterHash);
        final var metadata = Assert.castUnchecked(planContext.getSchemaTemplate(), RecordLayerSchemaTemplate.class);
        try {
            final var maybePlan = planContext.getMetricsCollector().clock(RelationalMetric.RelationalEvent.GENERATE_LOGICAL_PLAN, () ->
                    new BaseVisitor(planGenerationContext, metadata, planContext.getDdlQueryFactory(),
                            planContext.getConstantActionFactory(), planContext.getDbUri(), caseSensitive)
                            .generateLogicalPlan(ast.getParseTree()));
            return maybePlan.optimize(planner, planContext, currentPlanHashMode);
        } catch (ProtoUtils.InvalidNameException ine) {
            throw new RelationalException(ine.getMessage(), ErrorCode.INVALID_NAME, ine).toUncheckedWrappedException();
        } catch (MetaDataException mde) {
            // we need a better way for translating error codes between record layer and Relational SQL error codes
            throw new RelationalException(mde.getMessage(), ErrorCode.SYNTAX_OR_ACCESS_VIOLATION, mde).toUncheckedWrappedException();
        } catch (VerifyException | SemanticException ve) {
            throw ExceptionUtil.toRelationalException(ve).toUncheckedWrappedException();
        } catch (RelationalException e) {
            throw e.toUncheckedWrappedException();
        }
    }

    @Nonnull
    private QueryPlan.PhysicalQueryPlan generatePhysicalPlanForExecuteContinuation(@Nonnull AstNormalizer.NormalizationResult ast,
                                                                                   @Nonnull Set<PlanHashable.PlanHashMode> validPlanHashModes,
                                                                                   @Nonnull PlanHashable.PlanHashMode currentPlanHashMode)
            throws RelationalException {
        final var queryHasherContext = ast.getQueryExecutionContext();
        final var continuationProto = queryHasherContext.getContinuation();
        final ContinuationImpl continuation;
        try {
            continuation = ContinuationImpl.parseContinuation(continuationProto);
        } catch (final InvalidProtocolBufferException e) {
            throw new RelationalException("unable to parse continuation",
                    ErrorCode.INTERNAL_ERROR, e);
        }

        final var compiledStatement = Assert.notNullUnchecked(continuation.getCompiledStatement());
        final var serializedPlanHashMode =
                PlanValidator.validateSerializedPlanSerializationMode(compiledStatement, validPlanHashModes);

        //
        // Note that serialization and deserialization of the constituent elements have to done in the same order
        // in order for the dictionary compression for type serialization to work properly. The order is
        // 1. plan
        // 2. arguments -- in the order of appearance in the ordinal table
        // 3. query constraints
        //

        final var serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE,
                PlanHashable.PlanHashMode.valueOf(Assert.notNullUnchecked(compiledStatement.getPlanSerializationMode())));
        final var recordQueryPlan =
                RecordQueryPlan.fromRecordQueryPlanProto(serializationContext, Assert.notNullUnchecked(compiledStatement.getPlan()));

        //
        // We know that the continuation must have a plan hash set and that the current runtime supports the request
        // to validate the plan hash using the mode given by the continuation. They must match!
        if (!Objects.requireNonNull(continuation.getPlanHash()).equals(recordQueryPlan.planHash(serializedPlanHashMode))) {
            throw new PlanValidator.PlanValidationException("cannot continue query due to mismatch between serialized and actual plan hash");
        }

        final var planTypes = usedTypes().evaluate(recordQueryPlan);
        final var typeRepositoryBuilder = TypeRepository.newBuilder();
        planTypes.forEach(typeRepositoryBuilder::addTypeIfNeeded);
        final var typeRepository = typeRepositoryBuilder.build();

        final OrderedLiteral[] orderedLiterals =
                new OrderedLiteral[compiledStatement.getArgumentsCount() + compiledStatement.getExtractedLiteralsCount()];
        final TypedQueryArgument[] typedQueryArguments =
                new TypedQueryArgument[compiledStatement.getArgumentsCount() + compiledStatement.getExtractedLiteralsCount()];
        for (int i = 0; i < compiledStatement.getArgumentsCount(); i++) {
            final var argument = compiledStatement.getArguments(i);
            typedQueryArguments[argument.getLiteralsTableIndex()] = argument;
        }
        for (int i = 0; i < compiledStatement.getExtractedLiteralsCount(); i++) {
            final var extractedLiteral = compiledStatement.getExtractedLiterals(i);
            typedQueryArguments[extractedLiteral.getLiteralsTableIndex()] = extractedLiteral;
        }

        for (int i = 0; i < typedQueryArguments.length; i++) {
            final TypedQueryArgument typedQueryArgument = typedQueryArguments[i];
            orderedLiterals[i] = OrderedLiteral.fromProto(serializationContext, typeRepository, typedQueryArgument);
        }

        final var preparedStatementParameters =
                deserializeArgumentsForParameters(compiledStatement, orderedLiterals);

        final var planGenerationContext = new MutablePlanGenerationContext(preparedStatementParameters,
                currentPlanHashMode,
                ast.getQuery(),
                ast.getQueryCacheKey().getCanonicalQueryString(), Objects.requireNonNull(continuation.getBindingHash()));
        planGenerationContext.setForExplain(ast.getQueryExecutionContext().isForExplain());
        Arrays.stream(orderedLiterals).forEach(literal -> planGenerationContext.getLiteralsBuilder().addLiteral(literal));
        planGenerationContext.setContinuation(continuationProto);
        final var continuationPlanConstraint =
                QueryPlanConstraint.fromProto(serializationContext, compiledStatement.getPlanConstraint());

        final DataType.StructType semanticStructType;
        if (compiledStatement.hasQueryMetadata()) {
            semanticStructType = Assert.castUnchecked(DataTypeUtils.toRelationalType(Type.fromTypeProto(serializationContext, compiledStatement.getQueryMetadata())),
                    DataType.StructType.class);
        } else {
            final Type resultType = recordQueryPlan.getResultType().getInnerType();
            if (resultType instanceof Type.Record) {
                final Type.Record recordType = (Type.Record)resultType;
                final List<DataType.StructType.Field> fields = recordType.getFields().stream()
                        .map(field -> DataType.StructType.Field.from(
                                field.getFieldName(),
                                DataTypeUtils.toRelationalType(field.getFieldType()),
                                field.getFieldIndex()))
                        .collect(java.util.stream.Collectors.toList());
                semanticStructType = DataType.StructType.from("QUERY_RESULT", fields, true);
            } else {
                // Fallback for non-record types (shouldn't happen for SELECT results)
                semanticStructType = DataType.StructType.from("QUERY_RESULT", ImmutableList.of(), true);
            }
        }

        return new QueryPlan.ContinuedPhysicalQueryPlan(recordQueryPlan, typeRepository,
                continuationPlanConstraint,
                planGenerationContext,
                "EXECUTE CONTINUATION " + ast.getQueryCacheKey().getCanonicalQueryString(),
                currentPlanHashMode,
                serializedPlanHashMode,
                semanticStructType);
    }

    private void resetTimer() {
        currentTime = System.nanoTime();
        beginTime = currentTime;
    }

    private long stepTimeMicros() {
        final long time = System.nanoTime();
        final long result = TimeUnit.NANOSECONDS.toMicros(time - currentTime);
        currentTime = time;
        return result;
    }

    private long totalTimeMicros() {
        return TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - beginTime);
    }

    @Nonnull
    private static PreparedParams deserializeArgumentsForParameters(@Nonnull final CompiledStatement compiledStatement,
                                                                    @Nonnull final OrderedLiteral[] orderedLiteralsTable) {
        final var unnamedParameterMap = Maps.<Integer, Object>newHashMap();
        final var namedParameterMap = Maps.<String, Object>newHashMap();

        for (int i = 0; i < compiledStatement.getArgumentsCount(); i++) {
            final var argument = compiledStatement.getArguments(i);
            final var literal = orderedLiteralsTable[argument.getLiteralsTableIndex()];
            if (argument.hasUnnamedParameterIndex()) {
                unnamedParameterMap.put(argument.getUnnamedParameterIndex(), literal.getLiteralObject());
            } else {
                Assert.thatUnchecked(argument.hasParameterName());
                namedParameterMap.put(argument.getParameterName(), literal.getLiteralObject());
            }
        }
        return PreparedParams.of(unnamedParameterMap, namedParameterMap);
    }

    @Nonnull
    public Options getOptions() {
        return options;
    }

    /**
     * Determines whether the query should be looked up and, if not found, cached in the plan cache. Currently, we take
     * this decision is taken statically, in the future we should combine with other combine it with environmental
     * conditions such as plan-cache-specific session options.
     *
     * @param queryCachingFlags A bitset of query caching flags.
     *
     * @return {@code true} if the query should interact with the plan cache, otherwise {@code false}.
     */
    private static boolean shouldNotCache(@Nonnull final Set<AstNormalizer.NormalizationResult.QueryCachingFlags> queryCachingFlags) {
        return queryCachingFlags.contains(AstNormalizer.NormalizationResult.QueryCachingFlags.WITH_NO_CACHE_OPTION) ||
                queryCachingFlags.contains(AstNormalizer.NormalizationResult.QueryCachingFlags.IS_DDL_STATEMENT) ||
                // avoid caching INSERT statements since they could result in extremely large plans leading to potential
                // OOM when too many of them are stored in the plan cache.
                queryCachingFlags.contains(AstNormalizer.NormalizationResult.QueryCachingFlags.IS_INSERT_STATEMENT);

    }

    /**
     * Creates a new instance of the plan generator.
     *
     * @param cache An optional instance of the query plan cache.
     * @param metaData The record store metadata
     * @param planContext The context related for planning the query and looking it in the cache.
     * @param recordStoreState The record store state
     * @param options a set of planner options.
     * @return a new instance of the plan generator.
     * @throws RelationalException if creation of the plan generator fails.
     */
    @Nonnull
    public static PlanGenerator create(@Nonnull final Optional<RelationalPlanCache> cache,
                                       @Nonnull final PlanContext planContext,
                                       @Nonnull final RecordMetaData metaData,
                                       @Nonnull final RecordStoreState recordStoreState,
                                       @Nonnull final IndexMatchCandidateRegistry matchCandidateRegistry,
                                       @Nonnull final Options options) throws RelationalException {
        final var planner = new CascadesPlanner(metaData, recordStoreState, matchCandidateRegistry);
        planner.setConfiguration(planContext.getRecordQueryPlannerConfiguration());
        return new PlanGenerator(cache, planContext, planner, options);
    }

    /**
     * Create a new instance of the plan generator for a given store.
     *
     * @param cache An optional instance of the query plan cache
     * @param planContext The context related for planning the query and looking it in the cache
     * @param store The record store to generate the planner for
     * @param options a set of planner options
     * @return a new instance of the plan generator
     * @throws RelationalException if creation of the plan generator fails
     */
    @Nonnull
    public static PlanGenerator create(@Nonnull final Optional<RelationalPlanCache> cache,
                                       @Nonnull final PlanContext planContext,
                                       @Nonnull FDBRecordStoreBase<?> store,
                                       @Nonnull final Options options) throws RelationalException {
        return create(cache, planContext, store.getRecordMetaData(), store.getRecordStoreState(), store.getIndexMaintainerRegistry(), options);
    }
}
