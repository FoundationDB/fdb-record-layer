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
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.RecordQueryPlannerConfiguration;
import com.apple.foundationdb.record.query.plan.cascades.CascadesCostModel;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.UncheckedRelationalException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.query.cache.PhysicalPlanEquivalence;
import com.apple.foundationdb.relational.recordlayer.query.cache.RelationalPlanCache;
import com.apple.foundationdb.relational.recordlayer.util.Assert;

import com.google.common.base.VerifyException;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.Set;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public final class PlanGenerator {

    /**
     * An optional plan cache used for improving performance by caching plans, so we can avoid planning them
     * unnecessarily.
     */
    @Nonnull
    private final Optional<RelationalPlanCache> cache;

    // todo (yhatem) TODO (Interaction between planner configurations and query cache)
    @Nonnull
    private final CascadesPlanner planner;

    private PlanGenerator(@Nonnull final Optional<RelationalPlanCache> cache,
                         @Nonnull final CascadesPlanner planner) {
        this.cache = cache;
        this.planner = planner;
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
        try {
            // parse query, generate AST, extract literals from AST, hash it w.r.t. prepared parameters, and identify query caching behavior flags
            final var astHashResult = AstNormalizer.normalizeQuery(context.getSchemaTemplate(), query, PreparedStatementParameters.of(context.getPreparedStatementParameters()),
                    context.getUserVersion(),
                    context.getSchemaTemplate().getIndexEntriesAsBitset(context.getPlannerConfiguration().getReadableIndexes()));

            // shortcut plan cache if the query is determined not-cacheable or the cache is not set (disabled).
            if (shouldNotCache(astHashResult.getQueryCachingFlags()) || cache.isEmpty()) {
                return generatePhysicalPlan(query, astHashResult, context, planner);
            }

            // otherwise, lookup the query in the cache
            final var planEquivalence = PhysicalPlanEquivalence.of(astHashResult.getQueryExecutionParameters().getEvaluationContext());
            return cache.get()
                    .reduce(astHashResult.getQueryCacheKey(),
                            planEquivalence,
                            () -> {
                                final var plan = generatePhysicalPlan(query, astHashResult, context, planner);
                                return Pair.of(planEquivalence.withConstraint(plan.getConstraint()), plan);
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
                                        return null;
                                    }
                                } else {
                                    return candidate;
                                }
                            }));
        } catch (UncheckedRelationalException uve) {
            throw uve.unwrap();
        } catch (MetaDataException mde) {
            // we need a better way to pass-thru / translate errors codes between record layer and Relational as SQL exceptions
            throw new RelationalException(mde.getMessage(), ErrorCode.SYNTAX_OR_ACCESS_VIOLATION, mde);
        } catch (VerifyException | SemanticException ve) {
            throw new RelationalException(ve.getMessage(), ErrorCode.INTERNAL_ERROR, ve);
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
        return new PlanGenerator(cache, createPlanner(metaData, recordStoreState, options));
    }

    private static Plan<?> generatePhysicalPlan(@Nonnull final String query,
                                                @Nonnull final AstNormalizer.Result ast,
                                                @Nonnull final PlanContext planContext,
                                                @Nonnull final CascadesPlanner planner) {
        // todo (yhatem) rewrite this.
        final var context = PlanGenerationContext.newBuilder()
                .setMetadataFactory(planContext.getConstantActionFactory())
                .setPreparedStatementParameters(planContext.getPreparedStatementParameters())
                .build();
        // (yhatem) why is this needed? looks hacky...
        context.pushDqlContext(RecordLayerSchemaTemplate.fromRecordMetadata(planContext.getMetaData(), "foo", 1));
        final var astWalker = new AstVisitor(context, planContext.getDdlQueryFactory(), planContext.getDbUri());
        try {

            final Object maybePlan = astWalker.visit(ast.getParseTree());
            Assert.thatUnchecked(maybePlan instanceof Plan, String.format("Could not generate a logical plan for query '%s'", query));
            final Plan<?> logicalPlan = (Plan<?>) maybePlan;
            return logicalPlan.optimize(planner, planContext.getPlannerConfiguration());
        } catch (MetaDataException mde) {
            // we need a better way to pass-thru / translate errors codes between record layer and Relational as SQL exceptions
            throw new RelationalException(mde.getMessage(), ErrorCode.SYNTAX_OR_ACCESS_VIOLATION, mde).toUncheckedWrappedException();
        } catch (VerifyException | SemanticException ve) {
            throw new RelationalException(ve.getMessage(), ErrorCode.INTERNAL_ERROR, ve).toUncheckedWrappedException();
        }
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
}
