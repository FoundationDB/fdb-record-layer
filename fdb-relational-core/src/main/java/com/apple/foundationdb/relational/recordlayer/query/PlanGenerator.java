/*
 * PlanGenerator.java
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

import com.apple.foundationdb.record.IndexFetchMethod;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.CascadesCostModel;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.properties.UsedTypesProperty;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.serialization.DefaultPlanSerializationRegistry;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.ddl.DdlQueryFactory;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;
import com.apple.foundationdb.relational.continuation.CompiledStatement;
import com.apple.foundationdb.relational.continuation.TypedQueryArgument;
import com.apple.foundationdb.relational.recordlayer.ContinuationImpl;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.query.QueryExecutionParameters.OrderedLiteral;
import com.apple.foundationdb.relational.recordlayer.query.cache.PhysicalPlanEquivalence;
import com.apple.foundationdb.relational.recordlayer.query.cache.RelationalPlanCache;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.RelationalLoggingUtil;

import com.google.common.base.VerifyException;
import com.google.common.collect.Maps;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public final class PlanGenerator {
    private static final Logger logger = LogManager.getLogger(PlanGenerator.class);

    /**
     * An optional plan cache used for improving performance by caching plans, so we can avoid planning them
     * unnecessarily.
     */
    @Nonnull
    private final Optional<RelationalPlanCache> cache;

    // todo (yhatem) TODO (Interaction between planner configurations and query cache)
    @Nonnull
    private final CascadesPlanner planner;

    private long beginTime = System.nanoTime();
    private long currentTime = beginTime;

    @Nonnull
    private Options options;

    private PlanGenerator(@Nonnull final Optional<RelationalPlanCache> cache,
                          @Nonnull final CascadesPlanner planner,
                          @Nonnull final Options options) {
        this.cache = cache;
        this.planner = planner;
        this.options = options;
    }

    /**
     * Returns for a given SQL query, a corresponding {@link Plan} that maybe executed to produce results. If a plan
     * cache is available, it looks up the SQL query in the cache, and if a {@link Plan} is found it returns that
     * plan directly from the cache without actually generating it. If no plan is found, it will generate it and store
     * it in the cache.
     *
     * @param query The SQL query.
     * @param context The context related for planning the query and looking it in the cache.
     * @return a corresponding {@link Plan}.
     * @throws RelationalException If planning was unsuccessful.
     */
    @Nonnull
    public Plan<?> getPlan(@Nonnull final String query,
                           @Nonnull final PlanContext context) throws RelationalException {
        resetTimer();
        KeyValueLogMessage message = KeyValueLogMessage.build("PlanGenerator");
        Plan<?> plan = null;
        RelationalException exception = null;
        try {
            plan = context.getMetricsCollector().clock(RelationalMetric.RelationalEvent.TOTAL_GET_PLAN_QUERY, () ->
                    getPlanInternal(query, context, message));
        } catch (RelationalException e) {
            exception = e;
            throw e;
        } finally {
            RelationalLoggingUtil.publishPlanGenerationLogs(logger, message, plan, exception, totalTimeMicros(), options);
        }
        return plan;
    }

    @Nonnull
    public Options getOptions() {
        return options;
    }

    @Nonnull
    private Plan<?> getPlanInternal(@Nonnull final String query, @Nonnull final PlanContext context,
                                     @Nonnull KeyValueLogMessage message) throws RelationalException {
        try {
            // parse query, generate AST, extract literals from AST, hash it w.r.t. prepared parameters, and identify query caching behavior flags
            final boolean caseSensitive = options.getOption(Options.Name.CASE_SENSITIVE_IDENTIFIERS);
            final Set<PlanHashable.PlanHashMode> validPlanHashModes =
                    QueryPlan.PhysicalQueryPlan.getValidPlanHashModes(options);
            final PlanHashable.PlanHashMode currentPlanHashMode = QueryPlan.PhysicalQueryPlan.getCurrentPlanHashMode(options);
            final var astHashResult = AstNormalizer.normalizeQuery(context, query, caseSensitive, currentPlanHashMode);
            RelationalLoggingUtil.publishNormalizeQueryLogs(message, stepTimeMicros(), astHashResult.getQueryCacheKey().getHash(),
                    astHashResult.getQueryCacheKey().getCanonicalQueryString());
            options = Options.combine(astHashResult.getQueryOptions(), options);

            // shortcut plan cache if the query is determined not-cacheable or the cache is not set (disabled).
            if (shouldNotCache(astHashResult.getQueryCachingFlags()) || cache.isEmpty()) {
                Plan<?> plan = generatePhysicalPlan(astHashResult, context, planner, caseSensitive, validPlanHashModes,
                        currentPlanHashMode);
                RelationalLoggingUtil.publishPlanCacheLogs(message, RelationalLoggingUtil.PlanCacheEvent.SKIP, stepTimeMicros(), 0);
                return plan;
            }

            // Default is to cache hit. This is modified later if we cache miss
            RelationalLoggingUtil.publishPlanCacheLogs(message, RelationalLoggingUtil.PlanCacheEvent.HIT, -1, cache.get().getStats().numEntries());

            // otherwise, lookup the query in the cache
            final var planEquivalence = PhysicalPlanEquivalence.of(astHashResult.getQueryExecutionParameters().getEvaluationContext());
            return context.getMetricsCollector().clock(RelationalMetric.RelationalEvent.CACHE_LOOKUP, () ->
                    cache.get().reduce(
                            astHashResult.getSchemaTemplateName(),
                            astHashResult.getQueryCacheKey(),
                            planEquivalence,
                            () -> {
                                final Plan<?> physicalPlan;
                                try {
                                    physicalPlan = generatePhysicalPlan(astHashResult, context, planner, caseSensitive,
                                            validPlanHashModes, currentPlanHashMode);
                                } catch (final RelationalException vE) {
                                    throw vE.toUncheckedWrappedException();
                                }
                                RelationalLoggingUtil.publishPlanCacheLogs(message, RelationalLoggingUtil.PlanCacheEvent.MISS, stepTimeMicros(), cache.get().getStats().numEntries());
                                return Pair.of(planEquivalence.withConstraint(physicalPlan.getConstraint()), physicalPlan);
                            },
                            value -> value.withQueryExecutionParameters(astHashResult.getQueryExecutionParameters()),
                            plans -> plans.reduce(null, (acc, candidate) -> {
                                if (candidate instanceof QueryPlan.PhysicalQueryPlan) {
                                    final var result = (QueryPlan.PhysicalQueryPlan) candidate;
                                    final var candidateQueryPlan = result.getRecordQueryPlan();
                                    var bestQueryPlan = acc == null ? null : ((QueryPlan.PhysicalQueryPlan) acc).getRecordQueryPlan();
                                    if (bestQueryPlan == null || new CascadesCostModel(planner.getConfiguration()).compare(candidateQueryPlan, bestQueryPlan) < 0) {
                                        return candidate;
                                    } else {
                                        return acc;
                                    }
                                } else {
                                    return candidate;
                                }
                            }),
                            e -> context.getMetricsCollector().increment(e)
                    )
            );
        } catch (UncheckedRelationalException uve) {
            throw uve.unwrap();
        } catch (MetaDataException mde) {
            // we need a better way to pass-thru / translate errors codes between record layer and Relational as SQL exceptions
            throw new RelationalException(mde.getMessage(), ErrorCode.SYNTAX_OR_ACCESS_VIOLATION, mde);
        } catch (VerifyException | SemanticException ve) {
            throw new RelationalException(ve.getMessage(), ErrorCode.INTERNAL_ERROR, ve);
        } catch (SQLException e) {
            throw ExceptionUtil.toRelationalException(e);
        }
    }

    /**
     * Creates a new instance of the plan generator.
     * @param cache An optional plan cache.
     * @param metaData The record store metadata
     * @param recordStoreState The record store state
     * @param options a set of planner options.
     * @return a plan generator
     * @throws RelationalException if creation of the plan generator fails.
     */
    @Nonnull
    public static PlanGenerator of(@Nonnull final Optional<RelationalPlanCache> cache,
                                   @Nonnull final RecordMetaData metaData,
                                   @Nonnull final RecordStoreState recordStoreState,
                                   @Nonnull final Options options) throws RelationalException {
        return new PlanGenerator(cache, createPlanner(metaData, recordStoreState, options), options);
    }

    @Nonnull
    private static Plan<?> generatePhysicalPlan(@Nonnull final AstNormalizer.Result ast,
                                                @Nonnull final PlanContext planContext,
                                                @Nonnull final CascadesPlanner planner,
                                                final boolean caseSensitive,
                                                @Nonnull final Set<PlanHashable.PlanHashMode> validPlanHashModes,
                                                @Nonnull final PlanHashable.PlanHashMode currentPlanHashMode) throws RelationalException {
        if (ast.getQueryCachingFlags().contains(AstNormalizer.Result.QueryCachingFlags.IS_EXECUTE_CONTINUATION_STATEMENT)) {
            return generatePhysicalPlanForExecuteContinuation(ast, planContext, validPlanHashModes, currentPlanHashMode);
        } else {
            return generatePhysicalPlanForCompilableStatement(ast, planContext, planner, caseSensitive, currentPlanHashMode);
        }
    }

    @Nonnull
    private static Plan<?> generatePhysicalPlanForCompilableStatement(@Nonnull final AstNormalizer.Result ast,
                                                                      @Nonnull final PlanContext planContext,
                                                                      @Nonnull final CascadesPlanner planner,
                                                                      final boolean caseSensitive,
                                                                      @Nonnull final PlanHashable.PlanHashMode currentPlanHashMode) {
        // todo (yhatem) rewrite this
        final var context =
                PlanGenerationContext.newBuilder()
                        .setMetadataFactory(planContext.getConstantActionFactory())
                        .setPreparedStatementParameters(planContext.getPreparedStatementParameters())
                        .setMetricsCollector(planContext.getMetricsCollector())
                        .setPlanHashMode(currentPlanHashMode)
                        .build();
        // (yhatem) why is this needed? looks hacky...
        // TODO This is needed so the execute can pick up the limit and offset which are stored by the grammar
        //      actions in that dql context.
        context.pushDqlContext(RecordLayerSchemaTemplate.fromRecordMetadata(planContext.getMetaData(), "foo", 1));
        // The hash value used accounts for the values that identify the query and not part of the execution context (e.g.
        // literal and parameter values without LIMIT and CONTINUATION)
        context.setParameterHash(ast.getQueryExecutionParameters().getParameterHash());
        try {
            final var maybePlan = generateLogicalPlan(context, ast, planContext.getDdlQueryFactory(), planContext.getDbUri(), caseSensitive);
            Assert.thatUnchecked(maybePlan instanceof Plan, ErrorCode.INTERNAL_ERROR, "Could not generate a logical plan for query '%s'", ast.getQueryCacheKey().getCanonicalQueryString());
            final Plan<?> logicalPlan = (Plan<?>) maybePlan;
            return logicalPlan.optimize(planner, planContext.getPlannerConfiguration(), currentPlanHashMode);
        } catch (MetaDataException mde) {
            // we need a better way to pass-thru / translate errors codes between record layer and Relational as SQL exceptions
            throw new RelationalException(mde.getMessage(), ErrorCode.SYNTAX_OR_ACCESS_VIOLATION, mde).toUncheckedWrappedException();
        } catch (VerifyException | SemanticException ve) {
            throw ExceptionUtil.toRelationalException(ve).toUncheckedWrappedException();
        } catch (RelationalException e) {
            throw e.toUncheckedWrappedException();
        }
    }

    @Nonnull
    private static QueryPlan.PhysicalQueryPlan generatePhysicalPlanForExecuteContinuation(@Nonnull final AstNormalizer.Result ast,
                                                                                          @Nonnull final PlanContext planContext,
                                                                                          @Nonnull final Set<PlanHashable.PlanHashMode> validPlanHashModes,
                                                                                          @Nonnull final PlanHashable.PlanHashMode currentPlanHashMode) throws RelationalException {
        final var queryHasherContext = (QueryHasherContext) ast.getQueryExecutionParameters();
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

        final var serializationContext = new PlanSerializationContext(DefaultPlanSerializationRegistry.INSTANCE,
                PlanHashable.PlanHashMode.valueOf(Assert.notNullUnchecked(compiledStatement.getPlanSerializationMode())));
        final var recordQueryPlan =
                RecordQueryPlan.fromRecordQueryPlanProto(serializationContext, Assert.notNullUnchecked(compiledStatement.getPlan()));

        //
        // We know that the continuation must have a plan hash set and that the current runtime supports the request
        // to validate the plan hash using the mode given by the continuation. They must match!
        if (Objects.requireNonNull(continuation.getPlanHash()) != recordQueryPlan.planHash(serializedPlanHashMode)) {
            throw new PlanValidator.PlanValidationException("cannot continue query due to mismatch between serialized and actual plan hash");
        }

        final var planTypes = UsedTypesProperty.evaluate(recordQueryPlan);
        final var typeRepositoryBuilder = TypeRepository.newBuilder();
        planTypes.forEach(typeRepositoryBuilder::addTypeIfNeeded);
        final var typeRepository = typeRepositoryBuilder.build();

        final OrderedLiteral[] orderedLiteralsTable =
                new OrderedLiteral[compiledStatement.getArgumentsCount() + compiledStatement.getExtractedLiteralsCount()];
        for (int i = 0; i < compiledStatement.getArgumentsCount(); i++) {
            deserializeTypedQueryArgument(serializationContext, typeRepository, orderedLiteralsTable,
                    compiledStatement.getArguments(i));
        }
        for (int i = 0; i < compiledStatement.getExtractedLiteralsCount(); i++) {
            deserializeTypedQueryArgument(serializationContext, typeRepository, orderedLiteralsTable,
                    compiledStatement.getExtractedLiterals(i));
        }

        final var preparedStatementParameters =
                deserializeArgumentsForParameters(compiledStatement, orderedLiteralsTable);

        final var context =
                PlanGenerationContext.newBuilder()
                        .setMetadataFactory(planContext.getConstantActionFactory())
                        .setPreparedStatementParameters(preparedStatementParameters)
                        .setMetricsCollector(planContext.getMetricsCollector())
                        .setPlanHashMode(currentPlanHashMode)
                        .build();
        context.setParameterHash(Objects.requireNonNull(continuation.getBindingHash()));

        // TODO This is needed so the execute code path can pick up the limit and offset which are stored by the grammar
        //      actions in that dql context.
        final var dqlContext =
                context.pushDqlContext(RecordLayerSchemaTemplate.fromRecordMetadata(planContext.getMetaData(),
                        "foo", 1));
        dqlContext.setLimit(queryHasherContext.getLimit());
        dqlContext.setOffset(queryHasherContext.getOffset());

        Arrays.stream(orderedLiteralsTable).forEach(context::addStrippedLiteralOrParameter);
        context.setContinuation(continuationProto);

        final var continuationPlanConstraint =
                QueryPlanConstraint.fromProto(serializationContext, compiledStatement.getPlanConstraint());

        return new QueryPlan.ContinuedPhysicalQueryPlan(recordQueryPlan, typeRepository,
                continuationPlanConstraint,
                context,
                "EXECUTE CONTINUATION " + ast.getQueryCacheKey().getCanonicalQueryString(),
                currentPlanHashMode,
                serializedPlanHashMode);
    }

    private static void deserializeTypedQueryArgument(@Nonnull final PlanSerializationContext serializationContext,
                                                      @Nonnull final TypeRepository typeRepository,
                                                      @Nonnull final OrderedLiteral[] orderedLiteralsTable,
                                                      @Nonnull final TypedQueryArgument argumentProto) {
        final var argumentType = Type.fromTypeProto(serializationContext, argumentProto.getType());
        if (argumentProto.hasUnnamedParameterIndex()) {
            orderedLiteralsTable[argumentProto.getLiteralsTableIndex()] =
                    OrderedLiteral.forUnnamedParameter(argumentType,
                            LiteralsUtils.objectFromLiteralObjectProto(typeRepository, argumentType, argumentProto.getObject()),
                            argumentProto.getUnnamedParameterIndex(), argumentProto.getTokenIndex());
        } else if (argumentProto.hasParameterName()) {
            orderedLiteralsTable[argumentProto.getLiteralsTableIndex()] =
                    OrderedLiteral.forNamedParameter(argumentType,
                            LiteralsUtils.objectFromLiteralObjectProto(typeRepository, argumentType, argumentProto.getObject()),
                            argumentProto.getParameterName(), argumentProto.getTokenIndex());
        } else {
            orderedLiteralsTable[argumentProto.getLiteralsTableIndex()] =
                    OrderedLiteral.forQueryLiteral(argumentType,
                            LiteralsUtils.objectFromLiteralObjectProto(typeRepository, argumentType, argumentProto.getObject()),
                            argumentProto.getTokenIndex());
        }
    }

    @Nonnull
    private static PreparedStatementParameters deserializeArgumentsForParameters(@Nonnull final CompiledStatement compiledStatement,
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
        return PreparedStatementParameters.of(unnamedParameterMap, namedParameterMap);
    }

    private static Object generateLogicalPlan(@Nonnull PlanGenerationContext planGenerationContext,
                                              @Nonnull AstNormalizer.Result ast,
                                              @Nonnull DdlQueryFactory ddlQueryFactory,
                                              @Nonnull URI dbUri,
                                              final boolean caseSensitive) throws RelationalException {
        return planGenerationContext.getMetricsCollector().clock(RelationalMetric.RelationalEvent.GENERATE_LOGICAL_PLAN, () ->
                new AstVisitor(planGenerationContext, ddlQueryFactory, dbUri, ast.getQueryCacheKey().getCanonicalQueryString(), caseSensitive)
                        .visit(ast.getParseTree()));
    }

    @Nonnull
    private static CascadesPlanner createPlanner(@Nonnull final RecordMetaData metaData,
                                                 @Nonnull final RecordStoreState recordStoreState,
                                                 @Nonnull final Options options) throws RelationalException {
        // todo (yhatem) TODO (Interaction between planner configurations and query cache)
        Options.IndexFetchMethod indexFetchMethod = options.getOption(Options.Name.INDEX_FETCH_METHOD);
        CascadesPlanner planner = new CascadesPlanner(metaData, recordStoreState);
        // TODO: TODO (Expose planner configuration parameters like index scan preference)
        RecordQueryPlannerConfiguration configuration = RecordQueryPlannerConfiguration.builder()
                .setIndexScanPreference(QueryPlanner.IndexScanPreference.PREFER_INDEX)
                .setIndexFetchMethod(toRecLayerIndexFetchMethod(indexFetchMethod))
                .build();
        planner.setConfiguration(configuration);
        return planner;
    }

    @Nonnull
    private static IndexFetchMethod toRecLayerIndexFetchMethod(Options.IndexFetchMethod method) throws RelationalException {
        if (method == null) {
            return IndexFetchMethod.USE_REMOTE_FETCH_WITH_FALLBACK;
        }
        switch (method) {
            case SCAN_AND_FETCH:
                return IndexFetchMethod.SCAN_AND_FETCH;
            case USE_REMOTE_FETCH:
                return IndexFetchMethod.USE_REMOTE_FETCH;
            case USE_REMOTE_FETCH_WITH_FALLBACK:
                return IndexFetchMethod.USE_REMOTE_FETCH_WITH_FALLBACK;
            default:
                throw new RelationalException("Index Fetch Method mismatch when converting the option from Relational to the Record Layer",
                        ErrorCode.INTERNAL_ERROR);
        }
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
    private static boolean shouldNotCache(@Nonnull final Set<AstNormalizer.Result.QueryCachingFlags> queryCachingFlags) {
        return queryCachingFlags.contains(AstNormalizer.Result.QueryCachingFlags.WITH_NO_CACHE_OPTION) ||
                queryCachingFlags.contains(AstNormalizer.Result.QueryCachingFlags.IS_DDL_STATEMENT);
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
}
