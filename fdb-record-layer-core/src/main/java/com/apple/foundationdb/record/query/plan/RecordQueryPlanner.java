/*
 * RecordQueryPlanner.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexFetchMethod;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.DimensionsKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression.FanType;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.OrderFunctionKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.RecordTypeKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanParameters;
import com.apple.foundationdb.record.provider.foundationdb.MultidimensionalIndexScanComparisons;
import com.apple.foundationdb.record.provider.foundationdb.indexes.MultidimensionalIndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.leaderboard.TimeWindowRecordFunction;
import com.apple.foundationdb.record.provider.foundationdb.leaderboard.TimeWindowScanComparisons;
import com.apple.foundationdb.record.query.ParameterRelationshipGraph;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.AndComponent;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.ComponentWithComparison;
import com.apple.foundationdb.record.query.expressions.FieldWithComparison;
import com.apple.foundationdb.record.query.expressions.NestedField;
import com.apple.foundationdb.record.query.expressions.OneOfThemWithComparison;
import com.apple.foundationdb.record.query.expressions.OneOfThemWithComponent;
import com.apple.foundationdb.record.query.expressions.OrComponent;
import com.apple.foundationdb.record.query.expressions.OrderQueryKeyExpression;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.expressions.QueryKeyExpressionWithComparison;
import com.apple.foundationdb.record.query.expressions.QueryKeyExpressionWithOneOfComparison;
import com.apple.foundationdb.record.query.expressions.QueryRecordFunctionWithComparison;
import com.apple.foundationdb.record.query.expressions.RecordTypeKeyComparison;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRange;
import com.apple.foundationdb.record.query.plan.cascades.ComparisonRanges;
import com.apple.foundationdb.record.query.plan.cascades.explain.PlannerGraphProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.ComparisonsProperty;
import com.apple.foundationdb.record.query.plan.cascades.properties.FieldWithComparisonCountProperty;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.planning.BooleanNormalizer;
import com.apple.foundationdb.record.query.plan.planning.FilterSatisfiedMask;
import com.apple.foundationdb.record.query.plan.planning.InExtractor;
import com.apple.foundationdb.record.query.plan.planning.RankComparisons;
import com.apple.foundationdb.record.query.plan.planning.TextScanPlanner;
import com.apple.foundationdb.record.query.plan.plans.InSource;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryInUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithChild;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTextIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedDistinctPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import com.apple.foundationdb.record.query.plan.sorting.RecordQueryPlannerSortConfiguration;
import com.apple.foundationdb.record.query.plan.sorting.RecordQuerySortPlan;
import com.apple.foundationdb.record.query.plan.visitor.FilterVisitor;
import com.apple.foundationdb.record.query.plan.visitor.RecordQueryPlannerSubstitutionVisitor;
import com.apple.foundationdb.record.query.plan.visitor.UnorderedPrimaryKeyDistinctVisitor;
import com.apple.foundationdb.record.util.pair.Pair;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.ImmutableIntArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * The query planner.
 *
 * Query planning means converting a {@link RecordQuery} to a {@link RecordQueryPlan}.
 * The plan can use secondary indexes defined in a {@link RecordMetaData} to execute the query efficiently.
 */
@API(API.Status.STABLE)
public class RecordQueryPlanner implements QueryPlanner {
    @Nonnull
    private static final Logger logger = LoggerFactory.getLogger(RecordQueryPlanner.class);

    /**
     * Default limit on the complexity of the plans generated by the planner.
     * @see RecordQueryPlannerConfiguration#getComplexityThreshold
     */
    public static final int DEFAULT_COMPLEXITY_THRESHOLD = 3000;

    @Nonnull
    private final RecordMetaData metaData;
    @Nonnull
    private final RecordStoreState recordStoreState;
    @Nullable
    private final StoreTimer timer;
    @Nonnull
    private final PlannableIndexTypes indexTypes;

    private boolean primaryKeyHasRecordTypePrefix;
    @Nonnull
    private RecordQueryPlannerConfiguration configuration;

    public RecordQueryPlanner(@Nonnull RecordMetaData metaData, @Nonnull RecordStoreState recordStoreState) {
        this(metaData, recordStoreState, null);
    }

    public RecordQueryPlanner(@Nonnull RecordMetaData metaData, @Nonnull RecordStoreState recordStoreState,
                              @Nullable StoreTimer timer) {
        this(metaData, recordStoreState, PlannableIndexTypes.DEFAULT, timer, DEFAULT_COMPLEXITY_THRESHOLD);
    }

    public RecordQueryPlanner(@Nonnull RecordMetaData metaData, @Nonnull RecordStoreState recordStoreState,
                              @Nonnull PlannableIndexTypes indexTypes, @Nullable StoreTimer timer) {
        this(metaData, recordStoreState, indexTypes, timer, DEFAULT_COMPLEXITY_THRESHOLD);
    }

    public RecordQueryPlanner(@Nonnull RecordMetaData metaData, @Nonnull RecordStoreState recordStoreState,
                              @Nullable StoreTimer timer, int complexityThreshold) {
        this(metaData, recordStoreState, PlannableIndexTypes.DEFAULT, timer, complexityThreshold);
    }

    public RecordQueryPlanner(@Nonnull RecordMetaData metaData, @Nonnull RecordStoreState recordStoreState,
                              @Nonnull PlannableIndexTypes indexTypes, @Nullable StoreTimer timer, int complexityThreshold) {
        this.metaData = metaData;
        this.recordStoreState = recordStoreState;
        this.indexTypes = indexTypes;
        this.timer = timer;

        primaryKeyHasRecordTypePrefix = metaData.primaryKeyHasRecordTypePrefix();
        configuration = RecordQueryPlannerConfiguration.builder()
                // If we are going to need type filters on Scan, index is safer without knowing any cardinalities.
                .setIndexScanPreference(metaData.getRecordTypes().size() > 1 && !primaryKeyHasRecordTypePrefix ?
                              IndexScanPreference.PREFER_INDEX : IndexScanPreference.PREFER_SCAN)
                .setAttemptFailedInJoinAsOr(true)
                .setComplexityThreshold(complexityThreshold)
                .build();
    }

    /**
     * Get whether {@link RecordQueryIndexPlan} is preferred over {@link RecordQueryScanPlan} even when it does not
     * satisfy any additional conditions.
     * @return whether to prefer index scan over record scan
     */
    @Nonnull
    public IndexScanPreference getIndexScanPreference() {
        return configuration.getIndexScanPreference();
    }

    /**
     * Set whether {@link RecordQueryIndexPlan} is preferred over {@link RecordQueryScanPlan} even when it does not
     * satisfy any additional conditions.
     * Scanning without an index is more efficient, but will have to skip over unrelated record types.
     * For that reason, it is safer to use an index, except when there is only one record type.
     * If the meta-data has more than one record type but the record store does not, this can be overridden.
     * If a {@link RecordQueryPlannerConfiguration} is already set using
     * {@link #setConfiguration(RecordQueryPlannerConfiguration)} (RecordQueryPlannerConfiguration)} it will be retained,
     * but the {@code IndexScanPreference} for the configuration will be replaced with the given preference.
     * @param indexScanPreference whether to prefer index scan over record scan
     */
    @Override
    public void setIndexScanPreference(@Nonnull IndexScanPreference indexScanPreference) {
        configuration = this.configuration.asBuilder()
                .setIndexScanPreference(indexScanPreference)
                .build();
    }

    @Nonnull
    @Override
    public RecordQueryPlannerConfiguration getConfiguration() {
        return configuration;
    }
    
    @Override
    public void setConfiguration(@Nonnull RecordQueryPlannerConfiguration configuration) {
        this.configuration = configuration;
    }

    /**
     * Get the {@link RecordMetaData} for this planner.
     * @return the meta-data
     */
    @Nonnull
    @Override
    public RecordMetaData getRecordMetaData() {
        return metaData;
    }

    /**
     * Get the {@link RecordStoreState} for this planner.
     * @return the record store state
     */
    @Nonnull
    @Override
    public RecordStoreState getRecordStoreState() {
        return recordStoreState;
    }

    /**
     * Create a plan to get the results of the provided query.
     * This method returns a {@link QueryPlanResult} that contains the same plan ass returned by {@link #plan(RecordQuery)}
     * with additional information provided in the {@link QueryPlanInfo}
     *
     * @param query a query for records on this planner's metadata
     * @param parameterRelationshipGraph a set of bindings the planner can use that may constrain requirements of the plan
     *        but also lead to better plans
     * @return a {@link QueryPlanResult} that contains the plan for the query with additional information
     * @throws com.apple.foundationdb.record.RecordCoreException if the planner cannot plan the query
     */
    @Nonnull
    @Override
    public QueryPlanResult planQuery(@Nonnull final RecordQuery query, @Nonnull ParameterRelationshipGraph parameterRelationshipGraph) {
        return new QueryPlanResult(plan(query, parameterRelationshipGraph));
    }

    /**
     * Create a plan to get the results of the provided query.
     *
     * @param query a query for records on this planner's metadata
     * @param parameterRelationshipGraph a set of bindings and their relationships that provide additional information
     *        to the planner that may improve plan quality but may also tighten requirements imposed on the parameter
     *        bindings that are used to execute the query
     * @return a plan that will return the results of the provided query when executed
     * @throws com.apple.foundationdb.record.RecordCoreException if there is no index that matches the sort in the provided query
     */
    @Nonnull
    @Override
    public RecordQueryPlan plan(@Nonnull RecordQuery query, @Nonnull ParameterRelationshipGraph parameterRelationshipGraph) {
        query.validate(metaData);

        final PlanContext planContext = getPlanContext(query);

        final BooleanNormalizer normalizer = BooleanNormalizer.forConfiguration(configuration);
        final QueryComponent queryFilter = query.getFilter();
        final QueryComponent filter =
                normalizer.normalizeIfPossible(queryFilter == null
                                               ? null : queryFilter.withParameterRelationshipMap(parameterRelationshipGraph));
        final KeyExpression sort = query.getSort();
        final boolean sortReverse = query.isSortReverse();

        RecordQueryPlan plan = plan(planContext, filter, sort, sortReverse);
        if (plan == null) {
            if (sort == null) {
                throw new RecordCoreException("Unexpected failure to plan without sort");
            }
            final RecordQueryPlannerSortConfiguration sortConfiguration = configuration.getSortConfiguration();
            if (sortConfiguration != null && sortConfiguration.shouldAllowNonIndexSort()) {
                final PlanContext withoutSort = new PlanContext(query.toBuilder().setSort(null).build(),
                        planContext.indexes, planContext.commonPrimaryKey);
                plan = plan(withoutSort, filter, null, false);
                if (plan == null) {
                    throw new RecordCoreException("Unexpected failure to plan without sort");
                }
                plan = new RecordQuerySortPlan(plan, sortConfiguration.getSortKey(sort, sortReverse));
            } else {
                throw new RecordCoreException("Cannot sort without appropriate index: " + sort);
            }
        }

        if (query.getRequiredResults() != null) {
            plan = tryToConvertToCoveringPlan(planContext, plan);
        }

        if (timer != null) {
            plan.logPlanStructure(timer);
        }

        if (plan.getComplexity() > configuration.getComplexityThreshold()) {
            throw new RecordQueryPlanComplexityException(plan);
        }

        if (logger.isTraceEnabled()) {
            logger.trace(KeyValueLogMessage.of("explain of plan",
                    "explain", PlannerGraphProperty.explain(plan)));
        }

        return plan;
    }

    @Nullable
    private RecordQueryPlan plan(PlanContext planContext, QueryComponent filter, KeyExpression sort, boolean sortReverse) {
        RecordQueryPlan plan = null;
        if (filter == null) {
            plan = planNoFilter(planContext, sort, sortReverse);
        } else {
            if (configuration.shouldPlanOtherAttemptWholeFilter()) {
                for (Index index : planContext.indexes) {
                    if (!indexTypes.getValueTypes().contains(index.getType()) &&
                            !indexTypes.getRankTypes().contains(index.getType()) &&
                            !indexTypes.getTextTypes().contains(index.getType())) {
                        final QueryComponent originalFilter = planContext.query.getFilter();
                        final CandidateScan candidateScan = new CandidateScan(planContext, index, sortReverse);
                        ScoredPlan wholePlan = planOther(candidateScan, index, originalFilter, sort, sortReverse, planContext.commonPrimaryKey);
                        if (wholePlan != null && wholePlan.unsatisfiedFilters.isEmpty()) {
                            return wholePlan.getPlan();
                        }
                    }
                }
            }
            ScoredPlan bestPlan = planFilter(planContext, filter);
            if (bestPlan != null) {
                plan = bestPlan.getPlan();
            }
        }
        if (plan == null) {
            if (sort == null) {
                plan = valueScan(new CandidateScan(planContext, null, false), null, false);
                if (filter != null) {
                    plan = new RecordQueryFilterPlan(plan, filter);
                }
            } else {
                return null;
            }
        }
        if (configuration.shouldDeferFetchAfterUnionAndIntersection() || configuration.shouldDeferFetchAfterInJoinAndInUnion()) {
            plan = RecordQueryPlannerSubstitutionVisitor.applyRegularVisitors(configuration, plan, metaData, indexTypes, planContext.commonPrimaryKey);
        } else {
            // Always do filter pushdown
            plan = plan.accept(new FilterVisitor(metaData, indexTypes, planContext.commonPrimaryKey));
        }
        return plan;
    }

    @Nullable
    private RecordQueryPlan planNoFilter(PlanContext planContext, KeyExpression sort, boolean sortReverse) {
        ScoredPlan bestPlan = null;
        Index bestIndex = null;
        if (sort == null) {
            bestPlan = planNoFilterNoSort(planContext, null);
        } else if (planContext.commonPrimaryKey != null) {
            bestPlan = planSortOnly(new CandidateScan(planContext, null, sortReverse), planContext.commonPrimaryKey, sort);
        }
        for (Index index : planContext.indexes) {
            ScoredPlan p;
            if (sort == null) {
                p = planNoFilterNoSort(planContext, index);
            } else {
                p = planSortOnly(new CandidateScan(planContext, index, sortReverse), indexKeyExpressionForPlan(planContext.commonPrimaryKey, index), sort);
            }
            if (p != null) {
                p = computePlanProperties(planContext, p);

                if (bestPlan == null || p.score > bestPlan.score ||
                        (p.score == bestPlan.score && compareIndexes(planContext, index, p.flowsAllRequiredFields,
                                bestIndex, bestPlan.flowsAllRequiredFields) > 0)) {
                    bestPlan = p;
                    bestIndex = index;
                }
            }
        }
        if (bestPlan != null) {
            bestPlan = planRemoveDuplicates(planContext, bestPlan);
            if (bestPlan == null) {
                throw new RecordCoreException("A common primary key is required to remove duplicates");
            }
            return bestPlan.getPlan();
        }
        return null;
    }

    @Nullable
    private ScoredPlan planNoFilterNoSort(PlanContext planContext, @Nullable Index index) {
        if (index != null && (!indexTypes.getValueTypes().contains(index.getType()) || index.getRootExpression().createsDuplicates())) {
            return null;
        }
        ScanComparisons scanComparisons = null;
        if (index == null &&
                planContext.query.getRecordTypes().size() == 1 &&
                planContext.commonPrimaryKey != null &&
                Key.Expressions.hasRecordTypePrefix(planContext.commonPrimaryKey)) {
            // Can scan just the one requested record type.
            final RecordTypeKeyComparison recordTypeKeyComparison = new RecordTypeKeyComparison(planContext.query.getRecordTypes().iterator().next());
            scanComparisons = new ScanComparisons.Builder().addEqualityComparison(recordTypeKeyComparison.getComparison()).build();
        }
        return new ScoredPlan(0, valueScan(new CandidateScan(planContext, index, false), scanComparisons, false));
    }

    private int compareIndexes(@Nonnull final PlanContext planContext, @Nullable final Index index1, final boolean flowsAllRequiredResults1,
                               @Nullable final Index index2, final boolean flowsAllRequiredResults2) {
        if (index1 == null) {
            if (index2 == null) {
                return 0;
            } else {
                // index2 is an index scan
                if (flowsAllRequiredResults2) {
                    // index2 should not incur a fetch
                    return -1;
                }
                return preferIndexToScan(planContext, index2) ? -1 : +1;
            }
        } else if (index2 == null) {
            // index1 is an index scan
            if (flowsAllRequiredResults1) {
                // index1 should not incur a fetch
                return 1;
            }
            return preferIndexToScan(planContext, index1) ? +1 : -1;
        } else {
            if (flowsAllRequiredResults1 == flowsAllRequiredResults2) {
                // Better for fewer stored columns.
                return Integer.compare(indexSizeOverhead(planContext, index2), indexSizeOverhead(planContext, index1));
            }
            return flowsAllRequiredResults1 ? +1 : -1;
        }
    }

    // Compatible behavior with older code: prefer an index on *just* the primary key.
    private boolean preferIndexToScan(PlanContext planContext, @Nonnull Index index) {
        IndexScanPreference indexScanPreference = getIndexScanPreference();
        switch (indexScanPreference) {
            case PREFER_INDEX:
                return true;
            case PREFER_SCAN:
                return false;
            case PREFER_PRIMARY_KEY_INDEX:
                return index.getRootExpression().equals(planContext.commonPrimaryKey);
            default:
                throw new RecordCoreException("Unknown indexScanPreference: " + indexScanPreference);
        }
    }

    private static int indexSizeOverhead(PlanContext planContext, @Nonnull Index index) {
        if (planContext.commonPrimaryKey == null) {
            return index.getColumnSize();
        } else {
            return index.getEntrySize(planContext.commonPrimaryKey);
        }
    }

    @Nullable
    private ScoredPlan planFilter(@Nonnull PlanContext planContext, @Nonnull QueryComponent filter) {
        if (filter instanceof AndComponent) {
            QueryComponent normalized = normalizeAndOr((AndComponent) filter);
            if (normalized instanceof OrComponent) {
                // The best we could do with the And version is index for the first part
                // and checking the Or with a filter for the second part. If we can do a
                // union instead, that would be superior. If not, don't miss the chance.
                ScoredPlan asOr = planOr(planContext, (OrComponent) normalized);
                if (asOr != null) {
                    return asOr;
                }
            }
        }
        if (filter instanceof OrComponent) {
            ScoredPlan orPlan = planOr(planContext, (OrComponent) filter);
            if (orPlan != null) {
                return orPlan;
            }
        }
        return planFilter(planContext, filter, false);
    }

    /**
     * Plan the given filter, which can be the whole query or a branch of an {@code OR}.
     * @param planContext the plan context for the query
     * @param filter the filter to plan
     * @param needOrdering whether to populate {@link ScoredPlan#planOrderingKey} to facilitate combining sub-plans
     * @return the best plan or {@code null} if no suitable index exists
     */
    @Nullable
    private ScoredPlan planFilter(@Nonnull PlanContext planContext, @Nonnull QueryComponent filter, boolean needOrdering) {
        final InExtractor inExtractor = InExtractor.fromFilter(filter, (componentWithComparison, inBinding) -> true);
        ScoredPlan withInAsOrUnion = null;
        if (planContext.query.getSort() != null) {
            final InExtractor savedExtractor = new InExtractor(inExtractor);
            boolean canSort = inExtractor.setSort(planContext.query.getSort(), planContext.query.isSortReverse());
            if (!canSort) {
                if (getConfiguration().shouldAttemptFailedInJoinAsUnion()) {
                    withInAsOrUnion = planFilterWithInUnion(planContext, savedExtractor);
                } else if (getConfiguration().shouldAttemptFailedInJoinAsOr()) {
                    // Can't implement as an IN join because of the sort order. Try as an OR instead.
                    QueryComponent asOr = normalizeAndOrForInAsOr(inExtractor.asOr());
                    if (!filter.equals(asOr)) {
                        withInAsOrUnion = planFilter(planContext, asOr);
                    }
                }
            }
        } else if (needOrdering) {
            inExtractor.sortByClauses();
        }
        final ScoredPlan withInJoin = planFilterWithInJoin(planContext, inExtractor, needOrdering);
        if (withInAsOrUnion != null) {
            if (withInJoin == null || withInAsOrUnion.score > withInJoin.score ||
                    FieldWithComparisonCountProperty.evaluate(withInAsOrUnion.getPlan()) < FieldWithComparisonCountProperty.evaluate(withInJoin.getPlan())) {
                return withInAsOrUnion;
            }
        }
        return withInJoin;
    }

    @Nullable
    private ScoredPlan planFilterWithInJoin(@Nonnull PlanContext planContext, @Nonnull InExtractor inExtractor, boolean needOrdering) {
        int maxNumReplans = Math.max(getConfiguration().getMaxNumReplansForInToJoin(), 0);
        boolean allowNonSargedInBindings = getConfiguration().getMaxNumReplansForInToJoin() < 0;
        int numReplan = 0;
        boolean progress = true;
        ScoredPlan bestPlan = null;
        while (numReplan <= maxNumReplans) {
            bestPlan = planFilterForInJoin(planContext, inExtractor.subFilter(), needOrdering);
            if (bestPlan == null) {
                return null;
            }
            
            final Set<String> inBindings = inExtractor.getInBindings();
            final Set<String> sargedInBindings = bestPlan.getSargedInBindings();
            if (allowNonSargedInBindings || sargedInBindings.containsAll(inBindings)) {
                break;
            }

            // create a new in extractor that only uses the in-clauses we were actually able to use
            inExtractor = inExtractor.filter((componentWithComparison, inBinding) -> {
                if (sargedInBindings.contains(inBinding)) {
                    return true;
                }
                // we cannot filter out INs that are defined using more complicated things like "rank(...) IN (...)"
                return isRankInComparison(planContext, componentWithComparison, inBinding);
            });

            if (inBindings.size() == inExtractor.getInBindings().size()) {
                // we were unable to make progress
                progress = false;
                break;
            }
                
            // Continue to re-plan with fewer in-clauses or exit the loop.
            numReplan ++;
        }  
        
        if (!progress || numReplan > maxNumReplans) {
            //
            // We exhausted all attempts to replan with fewer number of in clauses. Replan one last time with
            // 0 in-clauses.
            inExtractor = inExtractor.filter((componentWithComparison, inBinding) -> isRankInComparison(planContext, componentWithComparison, inBinding));
            bestPlan = planFilterForInJoin(planContext, inExtractor.subFilter(), needOrdering);
            if (bestPlan == null) {
                // This is borderline impossible.
                return null;
            }
        }

        Verify.verifyNotNull(bestPlan);
        
        final RecordQueryPlan wrapped = inExtractor.wrap(planContext.rankComparisons.wrap(bestPlan.getPlan(), bestPlan.includedRankComparisons, metaData));
        final ScoredPlan scoredPlan = new ScoredPlan(bestPlan.score, wrapped);
        if (needOrdering) {
            scoredPlan.planOrderingKey = inExtractor.adjustOrdering(bestPlan.planOrderingKey, false);
        }
        return scoredPlan;
    }

    private boolean isRankInComparison(@Nonnull PlanContext planContext, @Nonnull ComponentWithComparison comparison, @Nonnull String bindingName) {
        if (!(comparison instanceof QueryRecordFunctionWithComparison)) {
            return false;
        }
        QueryRecordFunctionWithComparison asEquals = (QueryRecordFunctionWithComparison)
                comparison.withOtherComparison(new Comparisons.ParameterComparison(Comparisons.Type.EQUALS, bindingName, Bindings.Internal.IN));
        return planContext.rankComparisons.getPlanComparison(asEquals) != null;
    }

    @Nullable
    private ScoredPlan planFilterWithInUnion(@Nonnull PlanContext planContext, @Nonnull InExtractor inExtractor) {
        final ScoredPlan scoredPlan = planFilterForInJoin(planContext, inExtractor.subFilter(), false);
        if (scoredPlan != null) {
            RecordQueryPlan inner = scoredPlan.getPlan();
            boolean distinct = false;
            if (inner instanceof RecordQueryUnorderedPrimaryKeyDistinctPlan ||
                    inner instanceof RecordQueryUnorderedDistinctPlan) {
                inner = ((RecordQueryPlanWithChild)inner).getChild();
                distinct = true;
            }
            // Compute this _after_ taking off any Distinct.
            // While these distinct plans are just like filters in that they do not affect the ordering of results, changing
            // forPlan to handle them that way exposes problems elsewhere in attempting to do Union / Intersection with FanOut
            // comparison keys, which is only valid against an index entry, not an actual record.
            scoredPlan.planOrderingKey = PlanOrderingKey.forPlan(metaData, inner, planContext.commonPrimaryKey);
            scoredPlan.planOrderingKey = inExtractor.adjustOrdering(scoredPlan.planOrderingKey, true);
            if (scoredPlan.planOrderingKey == null) {
                return null;
            }
            @Nullable final KeyExpression candidateKey;
            boolean candidateOnly;
            if (getConfiguration().shouldOmitPrimaryKeyInOrderingKeyForInUnion()) {
                candidateKey = planContext.query.getSort();
                candidateOnly = false;
            } else {
                candidateKey = getKeyForMerge(planContext.query.getSort(), planContext.commonPrimaryKey);
                candidateOnly = true;
            }
            final KeyExpression comparisonKey = PlanOrderingKey.mergedComparisonKey(Collections.singletonList(scoredPlan), candidateKey, candidateOnly);
            if (comparisonKey == null) {
                return null;
            }
            final List<InSource> valuesSources = inExtractor.unionSources();
            final RecordQueryPlan union = RecordQueryInUnionPlan.from(inner, valuesSources, comparisonKey, planContext.query.isSortReverse(), getConfiguration().getAttemptFailedInJoinAsUnionMaxSize(), Bindings.Internal.IN);
            if (distinct) {
                // Put back in the Distinct with the same comparison key.
                RecordQueryPlan distinctPlan = scoredPlan.getPlan() instanceof RecordQueryUnorderedPrimaryKeyDistinctPlan ?
                                               new RecordQueryUnorderedPrimaryKeyDistinctPlan(union) :
                                               new RecordQueryUnorderedDistinctPlan(union, ((RecordQueryUnorderedDistinctPlan)scoredPlan.getPlan()).getComparisonKey());
                return new ScoredPlan(scoredPlan.score, distinctPlan);
            } else {
                return new ScoredPlan(scoredPlan.score, union);
            }
        }
        return null;
    }

    private ScoredPlan planFilterForInJoin(@Nonnull PlanContext planContext, @Nonnull QueryComponent filter, boolean needOrdering) {
        planContext.rankComparisons = new RankComparisons(filter, planContext.indexes);
        List<ScoredPlan> intersectionCandidates = new ArrayList<>();
        ScoredPlan bestPlan = null;
        Index bestIndex = null;
        if (planContext.commonPrimaryKey != null && !avoidScanPlan(planContext)) {
            bestPlan = planIndex(planContext, filter, null, planContext.commonPrimaryKey, intersectionCandidates);
        }
        for (Index index : planContext.indexes) {
            KeyExpression indexKeyExpression = indexKeyExpressionForPlan(planContext.commonPrimaryKey, index);
            ScoredPlan p = planIndex(planContext, filter, index, indexKeyExpression, intersectionCandidates);
            if (p != null) {
                // TODO: Consider more organized score / cost:
                //   * predicates handled / unhandled.
                //   * size of row.
                //   * need for type filtering if row scan with multiple types.
                if (isBetterThanOther(planContext, p, index, bestPlan, bestIndex)) {
                    bestPlan = p;
                    bestIndex = index;
                }
            }
        }
        if (bestPlan != null) {
            if (bestPlan.getNumNonSargables() > 0) {
                bestPlan = handleNonSargables(bestPlan, intersectionCandidates, planContext);
            }
            if (needOrdering) {
                bestPlan.planOrderingKey = PlanOrderingKey.forPlan(metaData, bestPlan.getPlan(), planContext.commonPrimaryKey);
            }
        }
        return bestPlan;
    }

    // Get the key expression for the index entries of the given index, which includes primary key fields for normal indexes.
    private KeyExpression indexKeyExpressionForPlan(@Nullable KeyExpression commonPrimaryKey, @Nonnull Index index) {
        KeyExpression indexKeyExpression = index.getRootExpression();
        if (indexKeyExpression instanceof KeyWithValueExpression) {
            indexKeyExpression = ((KeyWithValueExpression) indexKeyExpression).getKeyExpression();
        }
        if (indexKeyExpression instanceof DimensionsKeyExpression) {
            // prefix + dimensions are key
            indexKeyExpression = ((DimensionsKeyExpression) indexKeyExpression).getWholeKey();
        }
        if (commonPrimaryKey != null && indexTypes.getValueTypes().contains(index.getType()) && configuration.shouldUseFullKeyForValueIndex()) {
            final List<KeyExpression> keys = new ArrayList<>(commonPrimaryKey.normalizeKeyForPositions());
            index.trimPrimaryKey(keys);
            if (!keys.isEmpty()) {
                keys.add(0, indexKeyExpression);
                indexKeyExpression = Key.Expressions.concat(keys);
            }
        }
        return indexKeyExpression;
    }

    public boolean isBetterThanOther(@Nonnull final PlanContext planContext,
                                     @Nonnull final ScoredPlan plan,
                                     @Nullable final Index index,
                                     @Nullable final ScoredPlan otherPlan,
                                     @Nullable final Index otherIndex) {
        if (otherPlan == null) {
            return true;
        }

        // better if higher score (for indexes the number of sargables)
        if (plan.score > otherPlan.score) {
            return true;
        }

        // better if lower number of non-sargables (residuals + index filters)
        if (plan.getNumNonSargables() < otherPlan.getNumNonSargables()) {
            return true;
        }

        // if same score
        if (plan.score == otherPlan.score) {
            // if same non-sargables
            if (plan.getNumNonSargables() == otherPlan.getNumNonSargables()) {
                if (plan.getNumIndexFilters() == otherPlan.getNumIndexFilters()) {
                    if (compareIndexes(planContext, index, plan.flowsAllRequiredFields, otherIndex, otherPlan.flowsAllRequiredFields) > 0) {
                        return true;
                    }
                }
                // better if a higher number of index filters --> fewer fetches
                return plan.getNumIndexFilters() > otherPlan.getNumIndexFilters();
            }
        }

        return false;
    }

    @Nullable
    private ScoredPlan planIndex(@Nonnull PlanContext planContext, @Nonnull QueryComponent filter,
                                 @Nullable Index index, @Nonnull KeyExpression indexExpr,
                                 @Nonnull List<ScoredPlan> intersectionCandidates) {
        final KeyExpression sort = planContext.query.getSort();
        final boolean sortReverse = planContext.query.isSortReverse();
        final CandidateScan candidateScan = new CandidateScan(planContext, index, sortReverse);
        ScoredPlan p = null;
        if (index != null) {
            if (indexTypes.getRankTypes().contains(index.getType())) {
                GroupingKeyExpression grouping = (GroupingKeyExpression) indexExpr;
                p = planRank(candidateScan, index, grouping, filter);
                indexExpr = grouping.getWholeKey(); // Plan as just value index.
            } else if (!indexTypes.getValueTypes().contains(index.getType())) {
                p = planOther(candidateScan, index, filter, sort, sortReverse, planContext.commonPrimaryKey);
                if (p != null) {
                    p = planRemoveDuplicates(planContext, p);
                }
                if (p != null) {
                    p = computePlanProperties(planContext, p);
                }
                if (p != null && p.getNumNonSargables() > 0) {
                    PlanOrderingKey planOrderingKey = PlanOrderingKey.forPlan(metaData, p.getPlan(), planContext.commonPrimaryKey);
                    if (planOrderingKey != null && sort != null) {
                        p.planOrderingKey = planOrderingKey;
                        intersectionCandidates.add(p);
                    }
                }
                return p;
            }
        }
        if (p == null) {
            p = matchToPlan(candidateScan, matchCandidateScan(candidateScan, indexExpr, filter, sort));
        }
        if (p == null) {
            // we can't match the filter, but maybe the sort
            p = planSortOnly(candidateScan, indexExpr, sort);
            if (p != null) {
                final List<QueryComponent> unsatisfiedFilters = filter instanceof AndComponent ?
                                                                ((AndComponent) filter).getChildren() :
                                                                Collections.singletonList(filter);
                p = new ScoredPlan(0, p.getPlan(), unsatisfiedFilters, p.createsDuplicates, p.isStrictlySorted);
            }
        }

        if (p != null) {
            if (getConfiguration().shouldOptimizeForIndexFilters()) {
                // partition index filters
                if (index == null) {
                    // If we scan without an index all filters become index filters as we don't need a fetch
                    // to evaluate these filters. Also, this plan flows all possible fields of the record type(s);
                    // therefore it also flows all required fields.
                    p = p.withResidualFilterAndSargedComparisons(p.combineNonSargables(), computeSargedComparisons(p.getPlan()), true);
                } else {
                    p = computePlanProperties(planContext, p);
                }
            } else {
                p = p.withSargedComparisons(computeSargedComparisons(p.getPlan()));
            }
        }

        if (p != null) {
            p = planRemoveDuplicates(planContext, p);
            if (p != null && p.getNumNonSargables() > 0) {
                PlanOrderingKey planOrderingKey = PlanOrderingKey.forPlan(metaData, p.getPlan(), planContext.commonPrimaryKey);
                if (planOrderingKey != null && (sort != null || planOrderingKey.isPrimaryKeyOrdered())) {
                    // If there is a sort, all chosen plans should be ordered by it and so compatible.
                    // Otherwise, by requiring pkey order, we miss out on the possible intersection of
                    // X < 10 AND X < 5, which should have been handled already. We gain simplicity
                    // in not trying X < 10 AND Y = 5 AND Z = 'foo', where we would need to throw
                    // some out as we fail to align them all.
                    p.planOrderingKey = planOrderingKey;
                    intersectionCandidates.add(p);
                }
            }
        }

        return p;
    }

    private ScoredPlan computePlanProperties(@Nonnull PlanContext planContext, @Nonnull final ScoredPlan plan) {
        final Set<Comparisons.Comparison> sargedComparisons = computeSargedComparisons(plan.getPlan());

        List<QueryComponent> indexFilters = plan.indexFilters;
        List<QueryComponent> residualFilters = plan.unsatisfiedFilters;
        boolean flowsAllRequiredFields = plan.flowsAllRequiredFields;
        if (plan.getPlan() instanceof RecordQueryPlanWithIndex) {
            final RecordQueryPlanWithIndex planWithIndex = (RecordQueryPlanWithIndex) plan.getPlan();
            final Index index = metaData.getIndex(planWithIndex.getIndexName());
            final Collection<RecordType> recordTypes = metaData.recordTypesForIndex(index);
            if (recordTypes.size() == 1) {
                final RecordType recordType = Iterables.getOnlyElement(recordTypes);
                final List<QueryComponent> unsatisfiedFilters = new ArrayList<>(plan.unsatisfiedFilters);
                final AvailableFields availableFieldsFromIndex =
                        AvailableFields.fromIndex(recordType, index, indexTypes, planContext.commonPrimaryKey, planWithIndex);

                final List<KeyExpression> requiredResults = planContext.query.getRequiredResults();
                flowsAllRequiredFields = configuration.shouldOptimizeForRequiredResults() &&
                                         requiredResults != null &&
                                         availableFieldsFromIndex.containsAll(requiredResults);
                final List<QueryComponent> indexFiltersFromPartitioning =
                        Lists.newArrayListWithCapacity(unsatisfiedFilters.size());
                final List<QueryComponent> residualFiltersFromPartitioning = Lists.newArrayListWithCapacity(unsatisfiedFilters.size());
                FilterVisitor.partitionFilters(unsatisfiedFilters,
                        availableFieldsFromIndex,
                        indexFiltersFromPartitioning,
                        residualFiltersFromPartitioning,
                        null);

                if (!indexFiltersFromPartitioning.isEmpty()) {
                    indexFilters = indexFiltersFromPartitioning;
                    residualFilters = residualFiltersFromPartitioning;
                }
            }
        }

        return plan.withFiltersAndSargedComparisons(residualFilters, indexFilters, sargedComparisons, flowsAllRequiredFields);
    }

    protected Set<Comparisons.Comparison> computeSargedComparisons(@Nonnull final RecordQueryPlan plan) {
        return ComparisonsProperty.evaluate(plan);
    }

    @Nullable
    private ScoredMatch matchCandidateScan(@Nonnull CandidateScan candidateScan,
                                           @Nonnull KeyExpression indexExpr,
                                           @Nonnull QueryComponent filter, @Nullable KeyExpression sort) {
        filter = candidateScan.planContext.rankComparisons.planComparisonSubstitute(filter);
        if (filter instanceof FieldWithComparison) {
            return planFieldWithComparison(candidateScan, indexExpr, (FieldWithComparison) filter, sort, true);
        } else if (filter instanceof OneOfThemWithComparison) {
            return planOneOfThemWithComparison(candidateScan, indexExpr, (OneOfThemWithComparison) filter, sort);
        } else if (filter instanceof AndComponent) {
            return planAnd(candidateScan, indexExpr, (AndComponent) filter, sort);
        } else if (filter instanceof NestedField) {
            return planNestedField(candidateScan, indexExpr, (NestedField) filter, sort);
        } else if (filter instanceof OneOfThemWithComponent) {
            return planOneOfThemWithComponent(candidateScan, indexExpr, (OneOfThemWithComponent) filter, sort);
        } else if (filter instanceof QueryRecordFunctionWithComparison) {
            if (FunctionNames.VERSION.equals(((QueryRecordFunctionWithComparison) filter).getFunction().getName())) {
                return planVersion(candidateScan, indexExpr, (QueryRecordFunctionWithComparison) filter, sort);
            }
        } else if (filter instanceof QueryKeyExpressionWithComparison) {
            return planQueryKeyExpressionWithComparison(candidateScan, indexExpr, (QueryKeyExpressionWithComparison) filter, sort);
        } else if (filter instanceof QueryKeyExpressionWithOneOfComparison) {
            return planQueryKeyExpressionWithOneOfComparison(candidateScan, indexExpr, (QueryKeyExpressionWithOneOfComparison) filter, sort);
        }
        return null;
    }

    @Nullable
    private ScoredPlan matchToPlan(@Nonnull final CandidateScan candidateScan,
                                   @Nullable final ScoredMatch scoredMatch) {
        if (scoredMatch == null) {
            return null;
        }
        final Index index = candidateScan.getIndex();
        if (index != null && index.getType().equals(IndexTypes.MULTIDIMENSIONAL)) {
            return matchToMultidimensionalIndexScan(candidateScan, scoredMatch, index);
        }

        return scoredMatch.asScoredPlan(valueScan(candidateScan, scoredMatch.getComparisonRanges().toScanComparisons(),
                scoredMatch.isStrictlySorted));
    }

    @Nullable
    private ScoredPlan matchToMultidimensionalIndexScan(final @Nonnull CandidateScan candidateScan,
                                                        final @Nonnull ScoredMatch scoredMatch, final Index index) {
        final ComparisonRanges comparisonRanges = scoredMatch.getComparisonRanges();

        final DimensionsKeyExpression dimensionsKeyExpression =
                MultidimensionalIndexMaintainer.getDimensionsKeyExpression(index.getRootExpression());
        final KeyExpression commonPrimaryKey = candidateScan.getPlanContext().commonPrimaryKey;
        final KeyExpression indexKeyExpression = indexKeyExpressionForPlan(commonPrimaryKey, index);
        Verify.verify(comparisonRanges.size() == indexKeyExpression.getColumnSize());

        final int prefixCount = dimensionsKeyExpression.getPrefixSize();
        final int dimensionsCount = dimensionsKeyExpression.getDimensionsSize();
        final ComparisonRanges prefixComparisonRanges = new ComparisonRanges(comparisonRanges.subRanges(0, prefixCount));
        if (!prefixComparisonRanges.getRanges()
                .stream()
                .allMatch(ComparisonRange::isEquality)) {
            return null;
        }

        final List<ScanComparisons> dimensionsScanComparisons =
                comparisonRanges.subRanges(prefixCount, prefixCount + dimensionsCount)
                        .stream()
                        .map(ComparisonRange::toScanComparisons)
                        .collect(ImmutableList.toImmutableList());

        // TODO For now we will not plan a multidimensional index scan if one or more of the dimensions is not at least
        //      matched by an inequality.
        if (dimensionsScanComparisons.stream().anyMatch(ScanComparisons::isEmpty)) {
            return null;
        }

        final int suffixCount = comparisonRanges.size() - prefixCount - dimensionsCount;
        final ComparisonRanges suffixComparisonRanges =
                new ComparisonRanges(comparisonRanges.subRanges(prefixCount + dimensionsCount,
                        comparisonRanges.size()));
        final ScanComparisons suffixScanComparisons = suffixComparisonRanges.toScanComparisons();
        final List<QueryComponent> compensationComponentsForSuffix;
        if (suffixScanComparisons.size() < suffixCount) {
            // we need to compensate
            final List<KeyExpression> suffixKeyExpressions =
                    indexKeyExpression.normalizeKeyForPositions().subList(prefixCount + dimensionsCount, comparisonRanges.size());
            compensationComponentsForSuffix =
                    suffixComparisonRanges.compensateForScanComparisons(suffixKeyExpressions);
            if (compensationComponentsForSuffix == null) {
                return null;
            }
        } else {
            compensationComponentsForSuffix = Lists.newArrayList();
        }

        final IndexScanParameters indexScanParameters =
                MultidimensionalIndexScanComparisons.byValue(prefixComparisonRanges.toScanComparisons(),
                        dimensionsScanComparisons, suffixComparisonRanges.toScanComparisons());
        final RecordQueryPlan plan = planScan(candidateScan, indexScanParameters, scoredMatch.isStrictlySorted);
        final ScoredPlan scoredPlan = scoredMatch.asScoredPlan(plan);
        if (!compensationComponentsForSuffix.isEmpty()) {
            return scoredPlan.withAdditionalIndexFilters(compensationComponentsForSuffix);
        }
        return scoredPlan;
    }

    @Nonnull
    private List<Index> readableOf(@Nonnull List<Index> indexes) {
        if (recordStoreState.allIndexesReadable()) {
            return indexes;
        } else {
            return indexes.stream().filter(recordStoreState::isReadable).collect(Collectors.toList());
        }
    }

    @Nonnull
    private PlanContext getPlanContext(@Nonnull RecordQuery query) {
        final List<Index> indexes = new ArrayList<>();
        @Nullable final KeyExpression commonPrimaryKey;

        recordStoreState.beginRead();
        try {
            if (query.getRecordTypes().isEmpty()) { // ALL_TYPES
                commonPrimaryKey = RecordMetaData.commonPrimaryKey(metaData.getRecordTypes().values());
            } else {
                final List<RecordType> recordTypes = query.getRecordTypes().stream().map(metaData::getQueryableRecordType).collect(Collectors.toList());
                if (recordTypes.size() == 1) {
                    final RecordType recordType = recordTypes.get(0);
                    indexes.addAll(readableOf(recordType.getIndexes()));
                    indexes.addAll(readableOf(recordType.getMultiTypeIndexes()));
                    commonPrimaryKey = recordType.getPrimaryKey();
                } else {
                    boolean first = true;
                    for (RecordType recordType : recordTypes) {
                        if (first) {
                            indexes.addAll(readableOf(recordType.getMultiTypeIndexes()));
                            first = false;
                        } else {
                            indexes.retainAll(readableOf(recordType.getMultiTypeIndexes()));
                        }
                    }
                    commonPrimaryKey = RecordMetaData.commonPrimaryKey(recordTypes);
                }
            }

            indexes.addAll(readableOf(metaData.getUniversalIndexes()));
        } finally {
            recordStoreState.endRead();
        }

        indexes.removeIf(query.hasAllowedIndexes() ?
                index -> !query.getAllowedIndexes().contains(index.getName()) :
                index -> !query.getIndexQueryabilityFilter().isQueryable(index));

        return new PlanContext(query, indexes, commonPrimaryKey);
    }

    @Nullable
    private ScoredPlan planRemoveDuplicates(@Nonnull PlanContext planContext, @Nonnull ScoredPlan plan) {
        if (plan.createsDuplicates && planContext.query.removesDuplicates()) {
            if (planContext.commonPrimaryKey == null) {
                return null;
            }
            return new ScoredPlan(new RecordQueryUnorderedPrimaryKeyDistinctPlan(plan.getPlan()),
                    plan.unsatisfiedFilters, plan.indexFilters, plan.sargedComparisons, plan.score,
                    false, plan.isStrictlySorted, plan.flowsAllRequiredFields, plan.includedRankComparisons);
        } else {
            return plan;
        }
    }

    @Nonnull
    private ScoredPlan handleNonSargables(@Nonnull ScoredPlan bestPlan,
                                          @Nonnull List<ScoredPlan> intersectionCandidates,
                                          @Nonnull PlanContext planContext) {
        if (planContext.commonPrimaryKey != null && !intersectionCandidates.isEmpty()) {
            KeyExpression comparisonKey = planContext.commonPrimaryKey;
            final KeyExpression sort = planContext.query.getSort();
            comparisonKey = getKeyForMerge(sort, comparisonKey);
            ScoredPlan intersectionPlan = planIntersection(intersectionCandidates, comparisonKey);
            if (intersectionPlan != null) {
                if (intersectionPlan.unsatisfiedFilters.isEmpty()) {
                    return intersectionPlan;
                } else if (bestPlan.getNumNonSargables() > intersectionPlan.getNumNonSargables()) {
                    bestPlan = intersectionPlan;
                }
            }
        }

        if (bestPlan.getNumNonSargables() > 0) {
            final RecordQueryPlan filtered = new RecordQueryFilterPlan(bestPlan.getPlan(),
                    planContext.rankComparisons.planComparisonSubstitutes(bestPlan.combineNonSargables()));
            // TODO: further optimization requires knowing which filters are satisfied
            return new ScoredPlan(filtered, Collections.emptyList(), Collections.emptyList(),
                    bestPlan.sargedComparisons, bestPlan.score, bestPlan.createsDuplicates, bestPlan.isStrictlySorted,
                    bestPlan.flowsAllRequiredFields, bestPlan.includedRankComparisons);
        } else {
            return bestPlan;
        }
    }

    @Nullable
    private ScoredPlan planIntersection(@Nonnull List<ScoredPlan> intersectionCandidates,
                                        @Nonnull KeyExpression comparisonKey) {
        // Prefer plans that handle more filters (leave fewer unhandled), more index filters
        intersectionCandidates.sort(
                Comparator.comparingInt(ScoredPlan::getNumNonSargables)
                        .thenComparing(Comparator.comparingInt(ScoredPlan::getNumIndexFilters).reversed())
                        .thenComparing(Comparator.<ScoredPlan>comparingInt(p -> p.flowsAllRequiredFields ? 1 : 0).reversed()));
        // Since we limited to isPrimaryKeyOrdered(), comparisonKey will always work.
        ScoredPlan plan1 = intersectionCandidates.get(0);
        List<QueryComponent> nonSargables = new ArrayList<>(plan1.combineNonSargables());
        Set<RankComparisons.RankComparison> includedRankComparisons =
                mergeRankComparisons(null, plan1.includedRankComparisons);
        RecordQueryPlan plan = plan1.getPlan();
        List<RecordQueryPlan> includedPlans = new ArrayList<>(intersectionCandidates.size());
        includedPlans.add(plan);
        // TODO optimize so that we don't do excessive intersections
        for (int i = 1; i < intersectionCandidates.size(); i++) {
            ScoredPlan nextPlan = intersectionCandidates.get(i);
            List<QueryComponent> nextNonSargables = new ArrayList<>(nextPlan.combineNonSargables());
            int oldCount = nonSargables.size();
            nonSargables.retainAll(nextNonSargables);
            if (nonSargables.size() < oldCount) {
                if (plan.isReverse() != nextPlan.getPlan().isReverse()) {
                    // Cannot intersect plans with incompatible reverse settings.
                    return null;
                }
                includedPlans.add(nextPlan.getPlan());
            }
            includedRankComparisons = mergeRankComparisons(includedRankComparisons, nextPlan.includedRankComparisons);
        }
        if (includedPlans.size() > 1) {
            // Calculating the new score would require more state, not doing, because we currently ignore the score
            // after this call.
            final RecordQueryPlan intersectionPlan = RecordQueryIntersectionPlan.from(includedPlans, comparisonKey);
            if (intersectionPlan.getComplexity() > configuration.getComplexityThreshold()) {
                throw new RecordQueryPlanComplexityException(intersectionPlan);
            }
            return new ScoredPlan(intersectionPlan, nonSargables, Collections.emptyList(),
                    computeSargedComparisons(intersectionPlan), plan1.score, plan1.createsDuplicates,
                    plan1.isStrictlySorted, false, includedRankComparisons);
        } else {
            return null;
        }
    }

    @Nullable
    private ScoredMatch planOneOfThemWithComponent(@Nonnull CandidateScan candidateScan,
                                                   @Nonnull KeyExpression indexExpr,
                                                   @Nonnull OneOfThemWithComponent filter,
                                                   @Nullable KeyExpression sort) {
        if (indexExpr instanceof FieldKeyExpression) {
            return null;
        } else if (indexExpr instanceof ThenKeyExpression) {
            ThenKeyExpression then = (ThenKeyExpression) indexExpr;
            return planOneOfThemWithComponent(candidateScan, then.getChildren().get(0), filter, sort);
        } else if (indexExpr instanceof NestingKeyExpression) {
            NestingKeyExpression indexNesting = (NestingKeyExpression) indexExpr;
            ScoredMatch match = null;
            if (sort == null) {
                match = planNesting(candidateScan, indexNesting, filter, null);
            } else if (sort instanceof FieldKeyExpression) {
                match = null;
            } else if (sort instanceof ThenKeyExpression) {
                match = null;
            } else if (sort instanceof NestingKeyExpression) {
                NestingKeyExpression sortNesting = (NestingKeyExpression) sort;
                match = planNesting(candidateScan, indexNesting, filter, sortNesting);
            }
            if (match != null) {
                List<QueryComponent> unsatisfied;
                if (!match.unsatisfiedFilters.isEmpty()) {
                    unsatisfied = Collections.singletonList(filter);
                } else {
                    unsatisfied = Collections.emptyList();
                }
                // Right now it marks the whole nesting as unsatisfied, in theory there could be plans that handle that
                match = new ScoredMatch(match.score, match.getComparisonRanges(), unsatisfied, true, match.isStrictlySorted);
            }
            return match;
        }
        return null;
    }

    @Nullable
    private ScoredMatch planNesting(@Nonnull CandidateScan candidateScan,
                                    @Nonnull NestingKeyExpression indexExpr,
                                    @Nonnull OneOfThemWithComponent filter, @Nullable NestingKeyExpression sort) {
        if (sort == null || Objects.equals(indexExpr.getParent().getFieldName(), sort.getParent().getFieldName())) {
            // great, sort aligns
            if (Objects.equals(indexExpr.getParent().getFieldName(), filter.getFieldName())) {
                return matchCandidateScan(candidateScan, indexExpr.getChild(), filter.getChild(),
                        sort == null ? null : sort.getChild());
            }
        }
        return null;
    }

    @Nullable
    @SpotBugsSuppressWarnings("NP_LOAD_OF_KNOWN_NULL_VALUE")
    private ScoredMatch planNestedField(@Nonnull CandidateScan candidateScan,
                                        @Nonnull KeyExpression indexExpr,
                                        @Nonnull NestedField filter,
                                        @Nullable KeyExpression sort) {
        if (indexExpr instanceof FieldKeyExpression) {
            return null;
        } else if (indexExpr instanceof ThenKeyExpression) {
            return planThenNestedField(candidateScan, (ThenKeyExpression)indexExpr, filter, sort);
        } else if (indexExpr instanceof NestingKeyExpression) {
            return planNestingNestedField(candidateScan, (NestingKeyExpression)indexExpr, filter, sort);
        }
        return null;
    }

    private ScoredMatch planThenNestedField(@Nonnull CandidateScan candidateScan, @Nonnull ThenKeyExpression then,
                                            @Nonnull NestedField filter, @Nullable KeyExpression sort) {
        if (sort instanceof ThenKeyExpression || then.createsDuplicates()) {
            // Too complicated for the simple checks below.
            return planAndWithThen(candidateScan, then, Collections.singletonList(filter), sort);
        }
        ScoredMatch match = planNestedField(candidateScan, then.getChildren().get(0), filter, sort);
        if (match == null && sort != null && sort.equals(then.getChildren().get(1))) {
            final ScoredMatch sortlessMatch = planNestedField(candidateScan, then.getChildren().get(0), filter, null);
            if (sortlessMatch != null) {
                ComparisonRanges sortlessComparisons = sortlessMatch.getComparisonRanges();
                if (sortlessComparisons.isEqualities()) {
                    // A scan for an equality filter will be sorted by the next index key.
                    return sortlessMatch;
                }
            }
        }
        return match;
    }

    private ScoredMatch planNestingNestedField(@Nonnull CandidateScan candidateScan, @Nonnull NestingKeyExpression nesting,
                                               @Nonnull NestedField filter, @Nullable KeyExpression sort) {
        if (Objects.equals(nesting.getParent().getFieldName(), filter.getFieldName())) {
            ScoredMatch childMatch = null;
            if (sort == null) {
                childMatch = matchCandidateScan(candidateScan, nesting.getChild(), filter.getChild(), null);
            } else if (sort instanceof NestingKeyExpression) {
                NestingKeyExpression sortNesting = (NestingKeyExpression)sort;
                if (Objects.equals(sortNesting.getParent().getFieldName(), nesting.getParent().getFieldName())) {
                    childMatch = matchCandidateScan(candidateScan, nesting.getChild(), filter.getChild(), sortNesting.getChild());
                }
            }

            if (childMatch != null && !childMatch.unsatisfiedFilters.isEmpty()) {
                // Add the parent to the unsatisfied filters of this ScoredPlan if non-zero.
                QueryComponent unsatisfiedFilter;
                if (childMatch.unsatisfiedFilters.size() > 1) {
                    unsatisfiedFilter = Query.field(filter.getFieldName()).matches(Query.and(childMatch.unsatisfiedFilters));
                } else {
                    unsatisfiedFilter = Query.field(filter.getFieldName()).matches(childMatch.unsatisfiedFilters.get(0));
                }
                return childMatch.withUnsatisfiedFilters(Collections.singletonList(unsatisfiedFilter));
            } else {
                return childMatch;
            }
        }
        return null;
    }

    @Nullable
    private ComparisonRanges getPlanComparisonRanges(@Nonnull final RecordQueryPlan plan) {
        if (plan instanceof RecordQueryTypeFilterPlan) {
            return getPlanComparisonRanges(((RecordQueryTypeFilterPlan) plan).getInnerPlan());
        }
        if (plan instanceof RecordQueryPlanWithComparisons) {
            final var planWithComparisons = (RecordQueryPlanWithComparisons)plan;
            if (planWithComparisons.hasComparisonRanges()) {
                return planWithComparisons.getComparisonRanges();
            }
        }
        return null;
    }

    @Nullable
    private ScoredMatch planOneOfThemWithComparison(@Nonnull CandidateScan candidateScan,
                                                    @Nonnull KeyExpression indexExpr,
                                                    @Nonnull OneOfThemWithComparison oneOfThemWithComparison,
                                                    @Nullable KeyExpression sort) {
        final Comparisons.Comparison comparison = oneOfThemWithComparison.getComparison();
        final ComparisonRanges comparisonRanges = ComparisonRanges.tryFrom(comparison);
        if (comparisonRanges == null) {
            final ScoredPlan sortOnlyPlan = planSortOnly(candidateScan, indexExpr, sort);
            final ComparisonRanges planComparisonRanges =
                    sortOnlyPlan == null ? null : getPlanComparisonRanges(sortOnlyPlan.getPlan());
            if (planComparisonRanges != null) {
                return new ScoredMatch(0, planComparisonRanges,
                        Collections.singletonList(oneOfThemWithComparison),
                        sortOnlyPlan.createsDuplicates, sortOnlyPlan.isStrictlySorted);
            } else {
                return null;
            }
        }
        if (indexExpr instanceof FieldKeyExpression) {
            FieldKeyExpression field = (FieldKeyExpression) indexExpr;
            if (Objects.equals(oneOfThemWithComparison.getFieldName(), field.getFieldName())
                    && field.getFanType() == FanType.FanOut) {
                if (sort != null) {
                    if (sort instanceof FieldKeyExpression) {
                        FieldKeyExpression sortField = (FieldKeyExpression) sort;
                        if (Objects.equals(sortField.getFieldName(), field.getFieldName())) {
                            // everything matches, yay!! Hopefully that comparison can be for tuples
                            return new ScoredMatch(1, comparisonRanges,
                                    Collections.emptyList(), true, true);
                        }
                    }
                } else {
                    return new ScoredMatch(1, comparisonRanges,
                            Collections.<QueryComponent>emptyList(), true, false);
                }
            }
            return null;
        } else if (indexExpr instanceof ThenKeyExpression) {
            // May need second column to do sort, so handle like And, which does such cases.
            ThenKeyExpression then = (ThenKeyExpression) indexExpr;
            return planAndWithThen(candidateScan, then, Collections.singletonList(oneOfThemWithComparison), sort);
        } else if (indexExpr instanceof NestingKeyExpression) {
            return null;
        }
        return null;
    }

    @Nullable
    private ScoredMatch planAnd(@Nonnull CandidateScan candidateScan,
                                @Nonnull KeyExpression indexExpr,
                                @Nonnull AndComponent filter,
                                @Nullable KeyExpression sort) {
        if (indexExpr instanceof NestingKeyExpression) {
            return planAndWithNesting(candidateScan, (NestingKeyExpression)indexExpr, filter, sort);
        } else if (indexExpr instanceof ThenKeyExpression) {
            return planAndWithThen(candidateScan, (ThenKeyExpression)indexExpr, filter.getChildren(), sort);
        } else {
            return planAndWithThen(candidateScan, null, Collections.singletonList(indexExpr), filter.getChildren(), sort);
        }
    }

    @SpotBugsSuppressWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", justification = "maybe https://github.com/spotbugs/spotbugs/issues/616?")
    private ScoredMatch planAndWithThen(@Nonnull CandidateScan candidateScan,
                                       @Nonnull ThenKeyExpression indexExpr,
                                       @Nonnull List<QueryComponent> filters,
                                       @Nullable KeyExpression sort) {
        return planAndWithThen(candidateScan, indexExpr, indexExpr.getChildren(), filters, sort);
    }

    private ScoredMatch planAndWithThen(@Nonnull CandidateScan candidateScan,
                                        @Nullable ThenKeyExpression indexExpr,
                                        @Nonnull List<KeyExpression> indexChildren,
                                        @Nonnull List<QueryComponent> filters,
                                        @Nullable KeyExpression sort) {
        final AbstractAndWithThenPlanner andWithThenPlanner;
        if (candidateScan.index != null && candidateScan.index.getType().equals(IndexTypes.MULTIDIMENSIONAL)) {
            andWithThenPlanner = new MultidimensionalAndWithThenPlanner(candidateScan, indexExpr, indexChildren, filters, sort);
        } else {
            andWithThenPlanner = new AndWithThenPlanner(candidateScan, indexExpr, indexChildren, filters, sort);
        }

        return andWithThenPlanner.plan();
    }

    @Nullable
    private ScoredMatch planAndWithNesting(@Nonnull CandidateScan candidateScan,
                                           @Nonnull NestingKeyExpression indexExpr,
                                           @Nonnull AndComponent filter,
                                           @Nullable KeyExpression sort) {
        final FieldKeyExpression parent = indexExpr.getParent();
        if (parent.getFanType() == FanType.None) {
            // For non-spread case, we can do a better job trying to match more than one of the filter children if
            // they have the same nesting.
            final List<QueryComponent> nestedFilters = new ArrayList<>();
            final List<QueryComponent> remainingFilters = new ArrayList<>();
            for (QueryComponent filterChild : filter.getChildren()) {
                QueryComponent filterComponent = candidateScan.planContext.rankComparisons.planComparisonSubstitute(filterChild);
                if (filterComponent instanceof NestedField) {
                    final NestedField nestedField = (NestedField) filterComponent;
                    if (parent.getFieldName().equals(nestedField.getFieldName())) {
                        nestedFilters.add(nestedField.getChild());
                        continue;
                    }
                }
                remainingFilters.add(filterChild);
            }
            if (nestedFilters.size() > 1) {
                final NestedField nestedAnd = new NestedField(parent.getFieldName(), Query.and(nestedFilters));
                final ScoredMatch match = planNestedField(candidateScan, indexExpr, nestedAnd, sort);
                if (match != null) {
                    if (remainingFilters.isEmpty()) {
                        return match;
                    } else {
                        return match.withUnsatisfiedFilters(remainingFilters);
                    }
                } else {
                    return null;
                }
            }
        }
        List<QueryComponent> unsatisfiedFilters = new ArrayList<>(filter.getChildren());
        for (QueryComponent filterChild : filter.getChildren()) {
            QueryComponent filterComponent = candidateScan.planContext.rankComparisons.planComparisonSubstitute(filterChild);
            if (filterComponent instanceof NestedField) {
                NestedField nestedField = (NestedField) filterComponent;
                final ScoredMatch match = planNestedField(candidateScan, indexExpr, nestedField, sort);
                if (match != null) {
                    unsatisfiedFilters.remove(filterChild);
                    return match.withUnsatisfiedFilters(unsatisfiedFilters);
                }
            }
        }
        return null;
    }

    @Nullable
    private ScoredMatch planFieldWithComparison(@Nonnull CandidateScan candidateScan,
                                                @Nonnull KeyExpression indexExpr,
                                                @Nonnull FieldWithComparison singleField,
                                                @Nullable KeyExpression sort,
                                                boolean fullKey) {
        final Comparisons.Comparison comparison = singleField.getComparison();
        final ComparisonRanges comparisonRanges = ComparisonRanges.tryFrom(comparison);
        if (comparisonRanges == null) {
            // This comparison cannot be accomplished with a single scan.
            // It is still possible that the sort can be accomplished with
            // this index, but this should be handled elsewhere by the planner.
            return null;
        }
        if (indexExpr instanceof FieldKeyExpression) {
            FieldKeyExpression field = (FieldKeyExpression) indexExpr;
            if (Objects.equals(singleField.getFieldName(), field.getFieldName())) {
                if (sort != null) {
                    if (sort instanceof FieldKeyExpression) {
                        FieldKeyExpression sortField = (FieldKeyExpression) sort;
                        if (Objects.equals(sortField.getFieldName(), field.getFieldName())) {
                            // everything matches, yay!! Hopefully that comparison can be for tuples
                            return new ScoredMatch(1, comparisonRanges, Collections.emptyList(),
                                    false, fullKey);
                        }
                    }
                } else {
                    return new ScoredMatch(1, comparisonRanges, Collections.emptyList(),
                            false, false);
                }
            }
            return null;
        } else if (indexExpr instanceof ThenKeyExpression) {
            ThenKeyExpression then = (ThenKeyExpression) indexExpr;
            if ((sort == null || sort.equals(then.getChildren().get(0))) &&
                    !then.createsDuplicates() &&
                    !(then.getChildren().get(0) instanceof RecordTypeKeyExpression)) {
                // First column will do it all or not.
                return planFieldWithComparison(candidateScan, then.getChildren().get(0), singleField, sort, false);
            } else {
                // May need second column to do sort, so handle like And, which does such cases.
                return planAndWithThen(candidateScan, then, Collections.singletonList(singleField), sort);
            }
        }
        return null;
    }

    @Nullable
    private ScoredMatch planQueryKeyExpressionWithComparison(@Nonnull CandidateScan candidateScan,
                                                            @Nonnull KeyExpression indexExpr,
                                                            @Nonnull QueryKeyExpressionWithComparison queryKeyExpressionWithComparison,
                                                            @Nullable KeyExpression sort) {
        if (indexExpr.equals(queryKeyExpressionWithComparison.getKeyExpression()) && (sort == null || sort.equals(indexExpr))) {
            final Comparisons.Comparison comparison = queryKeyExpressionWithComparison.getComparison();
            final ComparisonRanges comparisonRanges = ComparisonRanges.tryFrom(comparison);
            if (comparisonRanges == null) {
                return null;
            }
            final boolean strictlySorted = sort != null; // Must be equal.
            return new ScoredMatch(1, comparisonRanges, Collections.emptyList(),
                    false, strictlySorted);
        } else if (indexExpr instanceof ThenKeyExpression) {
            return planAndWithThen(candidateScan, (ThenKeyExpression) indexExpr, Collections.singletonList(queryKeyExpressionWithComparison), sort);
        }
        return null;
    }

    @Nullable
    private ScoredMatch planQueryKeyExpressionWithOneOfComparison(@Nonnull CandidateScan candidateScan,
                                                                  @Nonnull KeyExpression indexExpr,
                                                                  @Nonnull QueryKeyExpressionWithOneOfComparison queryKeyExpressionWithOneOfComparison,
                                                                  @Nullable KeyExpression sort) {
        if (indexExpr.equals(queryKeyExpressionWithOneOfComparison.getKeyExpression()) && (sort == null || sort.equals(indexExpr))) {
            final Comparisons.Comparison comparison = queryKeyExpressionWithOneOfComparison.getComparison();
            final ComparisonRanges comparisonRanges = ComparisonRanges.tryFrom(comparison);
            if (comparisonRanges == null) {
                return null;
            }
            final boolean strictlySorted = sort != null; // Must be equal.
            return new ScoredMatch(1, comparisonRanges, Collections.emptyList(),
                    false, strictlySorted);
        } else if (indexExpr instanceof ThenKeyExpression) {
            return planAndWithThen(candidateScan, (ThenKeyExpression) indexExpr, Collections.singletonList(queryKeyExpressionWithOneOfComparison), sort);
        }
        return null;
    }

    @Nullable
    private ScoredPlan planSortOnly(@Nonnull CandidateScan candidateScan,
                                    @Nonnull KeyExpression indexExpr,
                                    @Nullable KeyExpression sort) {
        if (sort == null) {
            return null;
        }
        // Better error than no index found for impossible sorts.
        if (sort instanceof FieldKeyExpression) {
            FieldKeyExpression sortField = (FieldKeyExpression) sort;
            if (sortField.getFanType() == FanType.Concatenate) {
                throw new KeyExpression.InvalidExpressionException("Sorting by concatenate not supported");
            }
        }

        if (sort.isPrefixKey(indexExpr)) {
            final boolean strictlySorted =
                    sort.equals(indexExpr) ||
                    (candidateScan.index != null && candidateScan.index.isUnique() && sort.getColumnSize() >= candidateScan.index.getColumnSize());
            return new ScoredPlan(0,
                    valueScan(candidateScan, null, strictlySorted), Collections.emptyList(), indexExpr.createsDuplicates(),
                    strictlySorted);
        } else {
            return null;
        }
    }

    @Nonnull
    protected Set<String> getPossibleTypes(@Nonnull Index index) {
        final Collection<RecordType> recordTypes = metaData.recordTypesForIndex(index);
        if (recordTypes.size() == 1) {
            final RecordType singleRecordType = recordTypes.iterator().next();
            return Collections.singleton(singleRecordType.getName());
        } else {
            return recordTypes.stream().map(RecordType::getName).collect(Collectors.toSet());
        }
    }

    @Nonnull
    protected RecordQueryPlan addTypeFilterIfNeeded(@Nonnull CandidateScan candidateScan, @Nonnull RecordQueryPlan plan,
                                                    @Nonnull Set<String> possibleTypes) {
        Collection<String> allowedTypes = candidateScan.planContext.query.getRecordTypes();
        if (!allowedTypes.isEmpty() && !allowedTypes.containsAll(possibleTypes)) {
            return new RecordQueryTypeFilterPlan(plan, allowedTypes);
        } else {
            return plan;
        }
    }

    @Nullable
    private ScoredMatch planVersion(@Nonnull CandidateScan candidateScan,
                                    @Nonnull KeyExpression indexExpr,
                                    @Nonnull QueryRecordFunctionWithComparison filter,
                                    @Nullable KeyExpression sort) {
        if (indexExpr instanceof VersionKeyExpression) {
            if (sort == null || sort.equals(VersionKeyExpression.VERSION)) {
                final Comparisons.Comparison comparison = filter.getComparison();
                final ComparisonRanges comparisonRanges = ComparisonRanges.tryFrom(comparison);
                return new ScoredMatch(1,
                        comparisonRanges == null ? new ComparisonRanges() : comparisonRanges,
                        Collections.emptyList());
            }
        } else if (indexExpr instanceof ThenKeyExpression) {
            ThenKeyExpression then = (ThenKeyExpression) indexExpr;
            if (sort == null) { //&& !then.createsDuplicates()) {
                return planVersion(candidateScan, then.getChildren().get(0), filter, null);
            } else {
                return planAndWithThen(candidateScan, then, Collections.singletonList(filter), sort);
            }
        }
        return null;
    }

    @Nullable
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private ScoredPlan planRank(@Nonnull CandidateScan candidateScan,
                                @Nonnull Index index, @Nonnull GroupingKeyExpression indexExpr,
                                @Nonnull QueryComponent filter) {
        if (filter instanceof QueryRecordFunctionWithComparison) {
            final QueryRecordFunctionWithComparison filterComparison = (QueryRecordFunctionWithComparison) filter;
            final RankComparisons.RankComparison rankComparison = candidateScan.planContext.rankComparisons.getPlanComparison(filterComparison);
            if (rankComparison != null && rankComparison.getIndex() == index &&
                    RankComparisons.matchesSort(indexExpr, candidateScan.planContext.query.getSort())) {
                final ScanComparisons scanComparisons = rankComparison.getScanComparisons();
                final RecordQueryPlan scan = rankScan(candidateScan, filterComparison, scanComparisons);
                final boolean createsDuplicates = RankComparisons.createsDuplicates(index, indexExpr);
                return new ScoredPlan(scan, Collections.emptyList(), Collections.emptyList(),
                        computeSargedComparisons(scan), 1, createsDuplicates, scan.isStrictlySorted(),
                        false, Collections.singleton(rankComparison));
            }
        } else if (filter instanceof AndComponent) {
            return planRankWithAnd(candidateScan, index, indexExpr, (AndComponent) filter);
        }
        return null;
    }

    @Nullable
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private ScoredPlan planRankWithAnd(@Nonnull CandidateScan candidateScan,
                                       @Nonnull Index index, @Nonnull GroupingKeyExpression indexExpr,
                                       @Nonnull AndComponent and) {
        final List<QueryComponent> filters = and.getChildren();
        for (QueryComponent filter : filters) {
            if (filter instanceof QueryRecordFunctionWithComparison) {
                final QueryRecordFunctionWithComparison filterComparison = (QueryRecordFunctionWithComparison) filter;
                final RankComparisons.RankComparison rankComparison = candidateScan.planContext.rankComparisons.getPlanComparison(filterComparison);
                if (rankComparison != null && rankComparison.getIndex() == index &&
                        RankComparisons.matchesSort(indexExpr, candidateScan.planContext.query.getSort())) {
                    ScanComparisons scanComparisons = rankComparison.getScanComparisons();
                    final Set<RankComparisons.RankComparison> includedRankComparisons = new HashSet<>();
                    includedRankComparisons.add(rankComparison);
                    final List<QueryComponent> unsatisfiedFilters = new ArrayList<>(filters);
                    unsatisfiedFilters.remove(filter);
                    unsatisfiedFilters.removeAll(rankComparison.getGroupFilters());
                    int i = 0;
                    while (i < unsatisfiedFilters.size()) {
                        final QueryComponent otherFilter = unsatisfiedFilters.get(i);
                        if (otherFilter instanceof QueryRecordFunctionWithComparison) {
                            final QueryRecordFunctionWithComparison otherComparison = (QueryRecordFunctionWithComparison) otherFilter;
                            final RankComparisons.RankComparison otherRank = candidateScan.planContext.rankComparisons.getPlanComparison(otherComparison);
                            if (otherRank != null) {
                                ScanComparisons mergedScanComparisons = scanComparisons.merge(otherRank.getScanComparisons());
                                if (mergedScanComparisons != null) {
                                    scanComparisons = mergedScanComparisons;
                                    includedRankComparisons.add(otherRank);
                                    unsatisfiedFilters.remove(i--);
                                }
                            }
                        }
                        i++;
                    }
                    final RecordQueryPlan scan = rankScan(candidateScan, filterComparison, scanComparisons);
                    final boolean createsDuplicates = RankComparisons.createsDuplicates(index, indexExpr);
                    return new ScoredPlan(scan, unsatisfiedFilters, Collections.emptyList(), computeSargedComparisons(scan),
                            indexExpr.getColumnSize(), createsDuplicates, scan.isStrictlySorted(), false, includedRankComparisons);
                }
            }
        }
        return null;
    }

    @Nullable
    protected ScoredPlan planOther(@Nonnull CandidateScan candidateScan,
                                   @Nonnull Index index, @Nonnull QueryComponent filter,
                                   @Nullable KeyExpression sort, boolean sortReverse,
                                   @Nullable KeyExpression commonPrimaryKey) {
        if (indexTypes.getTextTypes().contains(index.getType())) {
            return planText(candidateScan, index, filter, sort, sortReverse);
        } else {
            return null;
        }
    }

    @Nullable
    @SuppressWarnings("PMD.UnusedFormalParameter")
    private ScoredPlan planText(@Nonnull CandidateScan candidateScan,
                                @Nonnull Index index, @Nonnull QueryComponent filter,
                                @Nullable KeyExpression sort, boolean sortReverse) {
        if (sort != null) {
            // TODO: Full Text: Sorts are not supported with full text queries (https://github.com/FoundationDB/fdb-record-layer/issues/55)
            return null;
        }
        FilterSatisfiedMask filterMask = FilterSatisfiedMask.of(filter);
        final TextScan scan = TextScanPlanner.getScanForQuery(index, filter, false, filterMask);
        if (scan == null) {
            return null;
        }
        // TODO: Check the rest of the fields of the text index expression to see if the sort and unsatisfied filters can be helped.
        RecordQueryPlan plan = new RecordQueryTextIndexPlan(index.getName(), scan, candidateScan.reverse);
        // Add a type filter if the index is over more types than those the query specifies
        Set<String> possibleTypes = getPossibleTypes(index);
        plan = addTypeFilterIfNeeded(candidateScan, plan, possibleTypes);
        // The scan produced by a "contains all prefixes" predicate might return false positives, so if the comparison
        // is "strict", it must be surrounded be a filter plan.
        if (scan.getTextComparison() instanceof Comparisons.TextContainsAllPrefixesComparison) {
            Comparisons.TextContainsAllPrefixesComparison textComparison = (Comparisons.TextContainsAllPrefixesComparison) scan.getTextComparison();
            if (textComparison.isStrict()) {
                plan = new RecordQueryFilterPlan(plan, filter);
                filterMask.setSatisfied(true);
            }
        }
        // This weight is fairly arbitrary, but it is supposed to be higher than for most indexes because
        // most of the time, the full text scan is believed to be more selective (and expensive to run as a post-filter)
        // than other indexes.
        return new ScoredPlan(plan, filterMask.getUnsatisfiedFilters(), Collections.emptyList(), computeSargedComparisons(plan),
                10, scan.createsDuplicates(), plan.isStrictlySorted(), false, null);
    }

    @Nonnull
    private RecordQueryPlan planScan(@Nonnull CandidateScan candidateScan,
                                     @Nonnull IndexScanParameters indexScanParameters,
                                     boolean strictlySorted) {
        RecordQueryPlan plan;
        Set<String> possibleTypes;
        if (candidateScan.index == null) {
            Verify.verify(indexScanParameters instanceof IndexScanComparisons);
            final ScanComparisons scanComparisons = ((IndexScanComparisons)indexScanParameters).getComparisons();
            if (primaryKeyHasRecordTypePrefix && RecordTypeKeyComparison.hasRecordTypeKeyComparison(scanComparisons)) {
                possibleTypes = RecordTypeKeyComparison.recordTypeKeyComparisonTypes(scanComparisons);
            } else {
                possibleTypes = metaData.getRecordTypes().keySet();
            }
            
            if (avoidScanPlan(candidateScan.planContext)) {
                throw new RecordCoreException("cannot create scan plan for a synthetic record type");
            }

            plan = new RecordQueryScanPlan(possibleTypes, new Type.Any(), candidateScan.planContext.commonPrimaryKey, scanComparisons, candidateScan.reverse, strictlySorted);
        } else {
            final FetchIndexRecords fetchIndexRecords = resolveFetchIndexRecords(candidateScan.getPlanContext());
            // If this is a regular fetch using the primary key, we can opt to use the configured fetch method,
            // if, however, this fetch fetches synthetic constituents, we must do it at the client (at this point).
            final IndexFetchMethod indexFetchMethod =
                    fetchIndexRecords == FetchIndexRecords.PRIMARY_KEY
                    ? getConfiguration().getIndexFetchMethod()
                    : IndexFetchMethod.SCAN_AND_FETCH;
            plan = new RecordQueryIndexPlan(candidateScan.index.getName(), candidateScan.planContext.commonPrimaryKey, indexScanParameters, indexFetchMethod, fetchIndexRecords, candidateScan.reverse, strictlySorted);
            possibleTypes = getPossibleTypes(candidateScan.index);
        }
        // Add a type filter if the query plan might return records of more types than the query specified
        plan = addTypeFilterIfNeeded(candidateScan, plan, possibleTypes);
        return plan;
    }

    private boolean avoidScanPlan(@Nonnull PlanContext planContext) {
        final var queriedRecordTypes = planContext.query.getRecordTypes();
        final var syntheticRecordTypes = metaData.getSyntheticRecordTypes().keySet();

        return !queriedRecordTypes.isEmpty() && // ok if user queried all record types which excludes synthetic ones
               queriedRecordTypes.stream().anyMatch(syntheticRecordTypes::contains);
    }

    /**
     * Method to statically resolve the method of how records are fetched given an index key. In particular,
     * {@link com.apple.foundationdb.record.metadata.SyntheticRecordType}s have constituent parts which need to
     * be separately fetched, while index scans over regular record types just need to fetch the base record
     * using the entire primary key in the index.
     * <br>
     * Note that the method on how to fetch the base record(s) has to be statically resolved during planning time as
     * the index entry itself does not contain any information to help us resolve that question on a per-record
     * basis.
     * 
     * @param planContext the current plan context
     * @return an enum of type {@link FetchIndexRecords} which determines how records are fetched given an index key
     */
    @Nonnull
    protected FetchIndexRecords resolveFetchIndexRecords(@Nonnull PlanContext planContext) {
        final var queriedRecordTypes = planContext.query.getRecordTypes();
        final var syntheticRecordTypes = metaData.getSyntheticRecordTypes().keySet();
        final var regularRecordTypes = metaData.getRecordTypes().keySet();

        if (!syntheticRecordTypes.isEmpty()) {
            if (queriedRecordTypes.isEmpty()) {
                // all record types are queried
                return FetchIndexRecords.PRIMARY_KEY;
            } else {
                if (syntheticRecordTypes.containsAll(queriedRecordTypes)) {
                    return FetchIndexRecords.SYNTHETIC_CONSTITUENTS;
                }
                if (regularRecordTypes.containsAll(queriedRecordTypes)) {
                    return FetchIndexRecords.PRIMARY_KEY;
                }
                throw new RecordCoreException("cannot mix regular and synthetic record types in query");
            }
        }
        return FetchIndexRecords.PRIMARY_KEY;
    }

    @Nonnull
    private RecordQueryPlan valueScan(@Nonnull CandidateScan candidateScan,
                                      @Nullable ScanComparisons scanComparisons,
                                      boolean strictlySorted) {
        IndexScanType scanType = candidateScan.index != null && this.configuration.valueIndexOverScanNeeded(candidateScan.index.getName())
                                 ? IndexScanType.BY_VALUE_OVER_SCAN
                                 : IndexScanType.BY_VALUE;
        return planScan(candidateScan, IndexScanComparisons.byValue(scanComparisons, scanType), strictlySorted);
    }

    @Nonnull
    private RecordQueryPlan rankScan(@Nonnull CandidateScan candidateScan,
                                     @Nonnull QueryRecordFunctionWithComparison rank,
                                     @Nonnull ScanComparisons scanComparisons) {
        IndexScanComparisons scanParameters;
        if (FunctionNames.TIME_WINDOW_RANK.equals(rank.getFunction().getName())) {
            scanParameters = new TimeWindowScanComparisons(((TimeWindowRecordFunction<?>) rank.getFunction()).getTimeWindow(), scanComparisons);
        } else {
            scanParameters = new IndexScanComparisons(IndexScanType.BY_RANK, scanComparisons);
        }
        return planScan(candidateScan, scanParameters, false);
    }

    @Nullable
    private ScoredPlan planOr(@Nonnull PlanContext planContext, @Nonnull OrComponent filter) {
        if (filter.getChildren().isEmpty()) {
            return null;
        }
        List<ScoredPlan> subplans = new ArrayList<>(filter.getChildren().size());
        boolean allHaveOrderingKey = true;
        RecordQueryPlan commonFilteredBasePlan = null;
        boolean allHaveSameBasePlan = true;
        for (QueryComponent subfilter : filter.getChildren()) {
            ScoredPlan subplan = planFilter(planContext, subfilter, true);
            if (subplan == null) {
                return null;
            }
            if (subplan.planOrderingKey == null) {
                allHaveOrderingKey = false;
            }
            RecordQueryPlan filteredBasePlan;
            if (subplan.getPlan() instanceof RecordQueryFilterPlan) {
                filteredBasePlan = ((RecordQueryFilterPlan)subplan.getPlan()).getInnerPlan();
            } else {
                filteredBasePlan = null;
            }
            if (subplans.isEmpty()) {
                commonFilteredBasePlan = filteredBasePlan;
                allHaveSameBasePlan = filteredBasePlan != null;
            } else if (allHaveSameBasePlan && !Objects.equals(filteredBasePlan, commonFilteredBasePlan)) {
                allHaveSameBasePlan = false;
            }
            subplans.add(subplan);
        }
        // If the child plans only differ in their filters, then there is no point in repeating the base
        // scan only to evaluate each of the filters. Just evaluate the scan with an OR filter.
        // Note that this also improves the _second-best_ plan for planFilterWithInJoin, but an IN filter wins
        // out there over the equivalent OR(EQUALS) filters.
        if (allHaveSameBasePlan) {
            final RecordQueryPlan combinedOrFilter = new RecordQueryFilterPlan(commonFilteredBasePlan,
                    new OrComponent(subplans.stream()
                            .map(subplan -> ((RecordQueryFilterPlan)subplan.getPlan()).getConjunctedFilter())
                            .collect(Collectors.toList())));
            ScoredPlan firstSubPlan = subplans.get(0);
            return new ScoredPlan(combinedOrFilter, Collections.emptyList(), Collections.emptyList(), Collections.emptySet(),
                    firstSubPlan.score, firstSubPlan.createsDuplicates, firstSubPlan.isStrictlySorted,
                    false, firstSubPlan.includedRankComparisons);
        }
        // If the child plans are compatibly ordered, return a union plan that removes duplicates from the
        // children as they come. If the child plans aren't ordered that way, then try and plan a union that
        // neither removes duplicates nor requires the children be in order.
        if (allHaveOrderingKey) {
            final ScoredPlan orderedUnionPlan = planOrderedUnion(planContext, subplans);
            if (orderedUnionPlan != null) {
                return orderedUnionPlan;
            }
        }
        final ScoredPlan unorderedUnionPlan = planUnorderedUnion(planContext, subplans);
        if (unorderedUnionPlan != null) {
            return planRemoveDuplicates(planContext, unorderedUnionPlan);
        }
        return null;
    }

    @Nullable
    private ScoredPlan planOrderedUnion(@Nonnull PlanContext planContext, @Nonnull List<ScoredPlan> subplans) {
        @Nullable final KeyExpression sort = planContext.query.getSort();
        @Nullable KeyExpression candidateKey;
        boolean candidateOnly = false;
        if (configuration.shouldOmitPrimaryKeyInUnionOrderingKey() || planContext.commonPrimaryKey == null) {
            candidateKey = sort;
        } else if (sort == null) {
            candidateKey = PlanOrderingKey.candidateContainingPrimaryKey(subplans, planContext.commonPrimaryKey);
        } else {
            candidateKey = getKeyForMerge(sort, planContext.commonPrimaryKey);
            candidateOnly = true;
        }
        KeyExpression comparisonKey = PlanOrderingKey.mergedComparisonKey(subplans, candidateKey, candidateOnly);
        if (comparisonKey == null) {
            return null;
        }
        boolean reverse = subplans.get(0).getPlan().isReverse();
        boolean anyDuplicates = false;
        Set<RankComparisons.RankComparison> includedRankComparisons = null;
        List<RecordQueryPlan> childPlans = new ArrayList<>(subplans.size());
        for (ScoredPlan subplan : subplans) {
            if (subplan.getPlan().isReverse() != reverse) {
                // Cannot mix plans that go opposite directions with the common ordering key.
                return null;
            }
            childPlans.add(subplan.getPlan());
            anyDuplicates |= subplan.createsDuplicates;
            includedRankComparisons = mergeRankComparisons(includedRankComparisons, subplan.includedRankComparisons);
        }
        boolean showComparisonKey = !comparisonKey.equals(planContext.commonPrimaryKey);
        final RecordQueryPlan unionPlan = RecordQueryUnionPlan.from(childPlans, comparisonKey, showComparisonKey);
        if (unionPlan.getComplexity() > configuration.getComplexityThreshold()) {
            throw new RecordQueryPlanComplexityException(unionPlan);
        }

        // If we don't change this when shouldAttemptFailedInJoinAsOr() is true, then we _always_ pick the union plan,
        // rather than the in join plan.
        int score = getConfiguration().shouldAttemptFailedInJoinAsOr() ? 0 : 1;

        return new ScoredPlan(unionPlan, Collections.emptyList(), Collections.emptyList(), Collections.emptySet(),
                score, anyDuplicates, false, false, includedRankComparisons);
    }

    @Nullable
    private ScoredPlan planUnorderedUnion(@Nonnull PlanContext planContext, @Nonnull List<ScoredPlan> subplans) {
        final KeyExpression sort = planContext.query.getSort();
        if (sort != null) {
            return null;
        }
        List<RecordQueryPlan> childPlans = new ArrayList<>(subplans.size());
        Set<RankComparisons.RankComparison> includedRankComparisons = null;
        for (ScoredPlan subplan : subplans) {
            childPlans.add(subplan.getPlan());
            includedRankComparisons = mergeRankComparisons(includedRankComparisons, subplan.includedRankComparisons);
        }
        final RecordQueryUnorderedUnionPlan unionPlan = RecordQueryUnorderedUnionPlan.from(childPlans);
        if (unionPlan.getComplexity() > configuration.getComplexityThreshold()) {
            throw new RecordQueryPlanComplexityException(unionPlan);
        }
        return new ScoredPlan(unionPlan, Collections.emptyList(), Collections.emptyList(), Collections.emptySet(),
                1, true, false, false, includedRankComparisons);
    }

    @Nullable
    private Set<RankComparisons.RankComparison> mergeRankComparisons(@Nullable Set<RankComparisons.RankComparison> into,
                                                                     @Nullable Set<RankComparisons.RankComparison> additional) {
        if (additional != null) {
            if (into == null) {
                return new HashSet<>(additional);
            } else {
                into.addAll(additional);
                return into;
            }
        } else {
            return into;
        }
    }

    /**
     * Generate a key for a merge operation, logically consisting of a sort key for the merge comparison and a primary
     * key for uniqueness. If the sort is a prefix of the primary key, then the primary key suffices.
     */
    @Nonnull
    private KeyExpression getKeyForMerge(@Nullable KeyExpression sort, @Nonnull KeyExpression candidateKey) {
        if (sort == null || sort.isPrefixKey(candidateKey)) {
            return candidateKey;
        } else if (candidateKey.isPrefixKey(sort)) {
            return sort;
        } else {
            return concatWithoutDuplicates(sort, candidateKey);
        }
    }

    // TODO: Perhaps this should be a public method on Key.Expressions.
    private ThenKeyExpression concatWithoutDuplicates(@Nullable KeyExpression expr1, @Nonnull KeyExpression expr2) {
        final List<KeyExpression> children = new ArrayList<>(2);
        if (expr1 instanceof ThenKeyExpression) {
            children.addAll(((ThenKeyExpression)expr1).getChildren());
        } else {
            children.add(expr1);
        }
        if (expr2 instanceof ThenKeyExpression) {
            for (KeyExpression child : ((ThenKeyExpression)expr2).getChildren()) {
                if (!children.contains(child)) {
                    children.add(child);
                }
            }
        } else if (!children.contains(expr2)) {
            children.add(expr2);
        }
        return new ThenKeyExpression(children);
    }

    @Nonnull
    // This is sufficient to handle the very common case of a single prefix comparison.
    // Distribute it across a disjunction so that we can union complex index lookups.
    private QueryComponent normalizeAndOr(AndComponent and) {
        if (and.getChildren().size() == 2) {
            QueryComponent child1 = and.getChildren().get(0);
            QueryComponent child2 = and.getChildren().get(1);
            if (child1 instanceof OrComponent && Query.isSingleFieldComparison(child2)) {
                return OrComponent.from(distributeAnd(Collections.singletonList(child2), ((OrComponent)child1).getChildren()));
            }
            if (child2 instanceof OrComponent && Query.isSingleFieldComparison(child1)) {
                return OrComponent.from(distributeAnd(Collections.singletonList(child1), ((OrComponent)child2).getChildren()));
            }
        }
        return and;
    }

    private QueryComponent normalizeAndOrForInAsOr(@Nonnull QueryComponent component) {
        if (!(component instanceof AndComponent)) {
            return component;
        }
        final AndComponent and = (AndComponent) component;
        OrComponent singleOrChild = null;
        final List<QueryComponent> otherChildren = new ArrayList<>();

        for (QueryComponent child : and.getChildren()) {
            if (child instanceof OrComponent) {
                if (singleOrChild == null) {
                    singleOrChild = (OrComponent) child;
                } else {
                    return and;
                }
            } else if (Query.isSingleFieldComparison(child)) {
                otherChildren.add(child);
            } else {
                return and;
            }
        }
        if (singleOrChild == null) {
            return and;
        }

        // We have exactly one OR child and the others are single field comparisons
        return OrComponent.from(distributeAnd(otherChildren, singleOrChild.getChildren()));
    }

    private List<QueryComponent> distributeAnd(List<QueryComponent> predicatesToDistribute, List<QueryComponent> children) {
        List<QueryComponent> distributed = new ArrayList<>();
        for (QueryComponent child : children) {
            List<QueryComponent> conjuncts = new ArrayList<>(2);
            conjuncts.addAll(predicatesToDistribute);
            if (child instanceof AndComponent) {
                conjuncts.addAll(((AndComponent)child).getChildren());
            } else {
                conjuncts.add(child);
            }
            QueryComponent cchild = AndComponent.from(conjuncts);
            distributed.add(cchild);
        }
        return distributed;
    }

    @Nonnull
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    private RecordQueryPlan tryToConvertToCoveringPlan(@Nonnull PlanContext planContext, @Nonnull RecordQueryPlan chosenPlan) {
        if (planContext.query.getRequiredResults() == null) {
            // This should already be true when calling, but as a safety precaution, check here anyway.
            return chosenPlan;
        }

        final Set<KeyExpression> resultFields = new HashSet<>(planContext.query.getRequiredResults().size());
        for (KeyExpression resultField : planContext.query.getRequiredResults()) {
            resultFields.addAll(resultField.normalizeKeyForPositions());
        }

        chosenPlan = chosenPlan.accept(new UnorderedPrimaryKeyDistinctVisitor(metaData, indexTypes, planContext.commonPrimaryKey));
        @Nullable RecordQueryPlan withoutFetch = RecordQueryPlannerSubstitutionVisitor.removeIndexFetch(
                metaData, indexTypes, planContext.commonPrimaryKey, chosenPlan, resultFields);
        return withoutFetch == null ? chosenPlan : withoutFetch;
    }

    @Nullable
    public RecordQueryCoveringIndexPlan planCoveringAggregateIndex(@Nonnull RecordQuery query, @Nonnull String indexName) {
        final Index index = metaData.getIndex(indexName);
        KeyExpression indexExpr = index.getRootExpression();
        if (indexExpr instanceof GroupingKeyExpression) {
            indexExpr = ((GroupingKeyExpression)indexExpr).getGroupingSubKey();
            if (IndexTypes.PERMUTED_MAX.equals(index.getType()) || IndexTypes.PERMUTED_MIN.equals(index.getType())) {
                @Nullable String permutedSizeOption = index.getOption(IndexOptions.PERMUTED_SIZE_OPTION);
                if (permutedSizeOption != null) {
                    int permutedSize = Integer.parseInt(permutedSizeOption);
                    indexExpr = Key.Expressions.concat(
                            indexExpr.getSubKey(0, indexExpr.getColumnSize() - permutedSize),
                            ((GroupingKeyExpression)index.getRootExpression()).getGroupedSubKey(),
                            indexExpr.getSubKey(indexExpr.getColumnSize() - permutedSize, indexExpr.getColumnSize())
                    );
                }
            }
        } else {
            indexExpr = EmptyKeyExpression.EMPTY;
        }
        return planCoveringAggregateIndex(query, index, indexExpr);
    }

    @Nullable
    public RecordQueryCoveringIndexPlan planCoveringAggregateIndex(@Nonnull RecordQuery query, @Nonnull Index index, @Nonnull KeyExpression indexExpr) {
        final Collection<RecordType> recordTypes = metaData.recordTypesForIndex(index);
        if (recordTypes.size() != 1) {
            // Unfortunately, since we materialize partial records, we need a unique type for them.
            return null;
        }
        final RecordType recordType = recordTypes.iterator().next();
        final PlanContext planContext = getPlanContext(query);
        planContext.rankComparisons = new RankComparisons(query.getFilter(), planContext.indexes);
        // Repeated fields will be scanned one at a time by covering aggregate, so there is no issue with fan out.
        planContext.allowDuplicates = true;
        final CandidateScan candidateScan = new CandidateScan(planContext, index, query.isSortReverse());
        final ScoredPlan scoredPlan = matchToPlan(candidateScan, matchCandidateScan(candidateScan, indexExpr,
                BooleanNormalizer.forConfiguration(configuration).normalizeIfPossible(query.getFilter()), query.getSort()));
        // It would be possible to handle unsatisfiedFilters if they, too, only involved group key (covering) fields.
        if (scoredPlan == null || !scoredPlan.unsatisfiedFilters.isEmpty() || !(scoredPlan.getPlan() instanceof RecordQueryIndexPlan)) {
            return null;
        }

        final IndexKeyValueToPartialRecord.Builder builder = IndexKeyValueToPartialRecord.newBuilder(recordType);
        final List<KeyExpression> keyFields = indexExpr.normalizeKeyForPositions();
        final List<KeyExpression> valueFields = Collections.emptyList();
        for (KeyExpression resultField : query.getRequiredResults()) {
            if (!addCoveringField(resultField, builder, keyFields, valueFields)) {
                return null;
            }
        }
        builder.addRequiredMessageFields();
        if (!builder.isValid(true)) {
            return null;
        }

        RecordQueryIndexPlan plan = (RecordQueryIndexPlan)scoredPlan.getPlan();
        IndexScanParameters scanParameters = new IndexScanComparisons(IndexScanType.BY_GROUP, plan.getScanComparisons());
        plan = new RecordQueryIndexPlan(plan.getIndexName(), scanParameters, plan.isReverse());
        return new RecordQueryCoveringIndexPlan(plan, recordType.getName(), AvailableFields.NO_FIELDS, builder.build());
    }

    @SuppressWarnings("UnstableApiUsage")
    private static boolean addCoveringField(@Nonnull KeyExpression requiredExpr,
                                            @Nonnull IndexKeyValueToPartialRecord.Builder builder,
                                            @Nonnull List<KeyExpression> keyFields,
                                            @Nonnull List<KeyExpression> valueFields) {
        final IndexKeyValueToPartialRecord.TupleSource source;
        final int index;

        int i = keyFieldPosition(requiredExpr, keyFields);
        if (i >= 0) {
            source = IndexKeyValueToPartialRecord.TupleSource.KEY;
            index = i;
        } else {
            i = valueFields.indexOf(requiredExpr);
            if (i >= 0) {
                source = IndexKeyValueToPartialRecord.TupleSource.VALUE;
                index = i;
            } else {
                return false;
            }
        }
        return AvailableFields.addCoveringField(requiredExpr, AvailableFields.FieldData.ofUnconditional(source, ImmutableIntArray.of(index)), builder);
    }

    private static int keyFieldPosition(final @Nonnull KeyExpression requiredExpr, final @Nonnull List<KeyExpression> keyFields) {
        int position = 0;
        for (KeyExpression keyField : keyFields) {
            if (keyField.equals(requiredExpr)) {
                return position;
            }
            position += keyField.getColumnSize();
        }
        return -1;
    }

    private static class PlanContext {
        @Nonnull
        final RecordQuery query;
        @Nonnull
        final List<Index> indexes;
        @Nullable
        final KeyExpression commonPrimaryKey;
        RankComparisons rankComparisons;
        boolean allowDuplicates;

        public PlanContext(@Nonnull RecordQuery query, @Nonnull List<Index> indexes,
                           @Nullable KeyExpression commonPrimaryKey) {
            this.query = query;
            this.indexes = indexes;
            this.commonPrimaryKey = commonPrimaryKey;
        }
    }

    protected static class CandidateScan {
        @Nonnull
        final PlanContext planContext;
        @Nullable
        final Index index;
        final boolean reverse;

        public CandidateScan(@Nonnull PlanContext planContext, @Nullable Index index, boolean reverse) {
            this.planContext = planContext;
            this.index = index;
            this.reverse = reverse;
        }

        @Nonnull
        public PlanContext getPlanContext() {
            return planContext;
        }

        @Nullable
        public Index getIndex() {
            return index;
        }

        public boolean isReverse() {
            return reverse;
        }
    }

    protected abstract static class ScoredInfo<T, S extends ScoredInfo<T, S>> {
        final int score;
        @Nonnull
        final T info;

        /**
         * A list of unsatisfied filters. If the set of filters expands beyond
         * /And|(Field|OneOfThem)(WithComparison|WithComponent)/ then doing a simple list here might stop being
         * sufficient. Remember to carry things up when dealing with children (i.e. a OneOfThemWithComponent that has
         * a partially satisfied And for its child, will be completely unsatisfied)
         */
        @Nonnull
        final List<QueryComponent> unsatisfiedFilters;
        @Nonnull
        final List<QueryComponent> indexFilters;

        @Nonnull
        final Set<Comparisons.Comparison> sargedComparisons;

        @Nonnull
        private final Supplier<Set<String>> sargedInBindingsSupplier;

        final boolean createsDuplicates;
        final boolean isStrictlySorted;
        final boolean flowsAllRequiredFields;
        @Nullable
        final Set<RankComparisons.RankComparison> includedRankComparisons;

        @Nullable
        PlanOrderingKey planOrderingKey;

        public ScoredInfo(@Nonnull T info,
                          @Nonnull List<QueryComponent> unsatisfiedFilters, @Nonnull final List<QueryComponent> indexFilters,
                          @Nonnull final Set<Comparisons.Comparison> sargedComparisons, int score, boolean createsDuplicates,
                          boolean isStrictlySorted, boolean flowsAllRequiredFields,
                          @Nullable Set<RankComparisons.RankComparison> includedRankComparisons) {
            this.score = score;
            this.info = info;
            this.unsatisfiedFilters = unsatisfiedFilters;
            this.indexFilters = indexFilters;
            this.sargedComparisons = sargedComparisons;
            this.sargedInBindingsSupplier = Suppliers.memoize(this::computeSargedInBindings);
            this.flowsAllRequiredFields = flowsAllRequiredFields;
            this.createsDuplicates = createsDuplicates;
            this.includedRankComparisons = includedRankComparisons;
            this.isStrictlySorted = isStrictlySorted;
        }

        public int getNumResiduals() {
            return unsatisfiedFilters.size();
        }

        public int getNumIndexFilters() {
            return indexFilters.size();
        }

        public int getNumNonSargables() {
            return getNumResiduals() + indexFilters.size();
        }

        public List<QueryComponent> combineNonSargables() {
            return ImmutableList.<QueryComponent>builder()
                    .addAll(unsatisfiedFilters)
                    .addAll(indexFilters)
                    .build();
        }

        protected abstract S getThis();

        protected abstract S with(@Nonnull T plan, @Nonnull List<QueryComponent> unsatisfiedFilters,
                                  @Nonnull List<QueryComponent> indexFilters,
                                  @Nonnull Set<Comparisons.Comparison> sargedComparisons,
                                  int score, boolean createsDuplicates, boolean isStrictlySorted,
                                  boolean flowsAllRequiredFields,
                                  @Nullable Set<RankComparisons.RankComparison> includedRankComparisons);

        @Nonnull
        public S withInfo(@Nonnull T newInfo) {
            return with(newInfo, unsatisfiedFilters, indexFilters, sargedComparisons, score, createsDuplicates,
                    isStrictlySorted, flowsAllRequiredFields, includedRankComparisons);
        }

        @Nonnull
        public S withScore(int newScore) {
            if (newScore == score) {
                return getThis();
            } else {
                return with(info, unsatisfiedFilters, indexFilters, sargedComparisons, newScore, createsDuplicates,
                        isStrictlySorted, flowsAllRequiredFields, includedRankComparisons);
            }
        }

        public Set<String> getSargedInBindings() {
            return sargedInBindingsSupplier.get();
        }

        private Set<String> computeSargedInBindings() {
            return sargedComparisons.stream()
                    .filter(comparison -> comparison.getType() == Comparisons.Type.EQUALS &&
                                          comparison instanceof Comparisons.ParameterComparison)
                    .map(comparison -> (Comparisons.ParameterComparison)comparison)
                    .filter(parameterComparison -> Bindings.Internal.IN.isOfType(parameterComparison.getParameter()))
                    .map(Comparisons.ParameterComparison::getParameter)
                    .collect(ImmutableSet.toImmutableSet());
        }

        @Nonnull
        public S withUnsatisfiedFilters(@Nonnull List<QueryComponent> newFilters) {
            return with(info, newFilters, indexFilters, sargedComparisons, score, createsDuplicates, isStrictlySorted,
                    flowsAllRequiredFields, includedRankComparisons);
        }

        @Nonnull
        public S withIndexFilters(@Nonnull List<QueryComponent> newIndexFilters) {
            return with(info, unsatisfiedFilters, newIndexFilters, sargedComparisons, score,
                    createsDuplicates, isStrictlySorted, flowsAllRequiredFields, includedRankComparisons);
        }

        @Nonnull
        public S withAdditionalIndexFilters(@Nonnull List<QueryComponent> additionalIndexFilters) {
            final List<QueryComponent> newIndexFilters = Lists.newArrayList();
            newIndexFilters.addAll(indexFilters);
            newIndexFilters.addAll(additionalIndexFilters);
            return with(info, unsatisfiedFilters, newIndexFilters, sargedComparisons, score, createsDuplicates,
                    isStrictlySorted, flowsAllRequiredFields, includedRankComparisons);
        }

        @Nonnull
        public S withResidualFilterAndSargedComparisons(@Nonnull List<QueryComponent> newUnsatisfiedFilters,  @Nonnull final Set<Comparisons.Comparison> sargedComparisons, boolean flowsAllRequiredFields) {
            return withFiltersAndSargedComparisons(newUnsatisfiedFilters, Collections.emptyList(), sargedComparisons, flowsAllRequiredFields);
        }

        @Nonnull
        public S withFiltersAndSargedComparisons(@Nonnull List<QueryComponent> newUnsatisfiedFilters, @Nonnull List<QueryComponent> newIndexFilters, @Nonnull final Set<Comparisons.Comparison> sargedComparisons, boolean flowsAllRequiredFields) {
            return with(info, newUnsatisfiedFilters, newIndexFilters, sargedComparisons, score, createsDuplicates,
                    isStrictlySorted, flowsAllRequiredFields, includedRankComparisons);
        }

        public S withSargedComparisons(@Nonnull Set<Comparisons.Comparison> sargedComparisons) {
            return with(info, unsatisfiedFilters, indexFilters, sargedComparisons, score, createsDuplicates,
                    isStrictlySorted, flowsAllRequiredFields, includedRankComparisons);
        }

        @Nonnull
        public S withCreatesDuplicates(boolean newCreatesDuplicates) {
            if (createsDuplicates == newCreatesDuplicates) {
                return getThis();
            } else {
                return with(info, unsatisfiedFilters, indexFilters, sargedComparisons, score, newCreatesDuplicates,
                        isStrictlySorted, flowsAllRequiredFields, includedRankComparisons);
            }
        }
    }

    protected static class ScoredPlan extends ScoredInfo<RecordQueryPlan, ScoredPlan> {
        public ScoredPlan(int score, @Nonnull RecordQueryPlan plan) {
            this(score, plan, Collections.emptyList());
        }

        public ScoredPlan(int score, @Nonnull RecordQueryPlan plan,
                          @Nonnull List<QueryComponent> unsatisfiedFilters) {
            this(score, plan, unsatisfiedFilters, false, false);
        }

        public ScoredPlan(int score, @Nonnull RecordQueryPlan plan, @Nonnull List<QueryComponent> unsatisfiedFilters,
                          boolean createsDuplicates, boolean isStrictlySorted) {
            this(plan, unsatisfiedFilters, Collections.emptyList(), Collections.emptySet(), score,
                    createsDuplicates, isStrictlySorted, false, null);
        }

        public ScoredPlan(@Nonnull RecordQueryPlan plan, @Nonnull List<QueryComponent> unsatisfiedFilters,
                          @Nonnull final List<QueryComponent> indexFilters,
                          @Nonnull final Set<Comparisons.Comparison> sargedComparisons, int score, boolean createsDuplicates,
                          boolean isStrictlySorted, boolean flowsAllRequiredFields,
                          @Nullable Set<RankComparisons.RankComparison> includedRankComparisons) {
            super(plan, unsatisfiedFilters, indexFilters, sargedComparisons, score, createsDuplicates, isStrictlySorted,
                    flowsAllRequiredFields, includedRankComparisons);
        }

        @Override
        protected ScoredPlan getThis() {
            return this;
        }

        /**
         * Make callers get the plan in a more readable way.
         * @return the {@link RecordQueryPlan}
         */
        @Nonnull
        public RecordQueryPlan getPlan() {
            return info;
        }

        @Override
        protected ScoredPlan with(@Nonnull final RecordQueryPlan plan,
                                  @Nonnull final List<QueryComponent> unsatisfiedFilters,
                                  @Nonnull final List<QueryComponent> indexFilters,
                                  @Nonnull final Set<Comparisons.Comparison> sargedComparisons,
                                  final int score, final boolean createsDuplicates, final boolean isStrictlySorted,
                                  final boolean flowsAllRequiredFields,
                                  @Nullable final Set<RankComparisons.RankComparison> includedRankComparisons) {
            return new ScoredPlan(plan, unsatisfiedFilters, indexFilters, sargedComparisons, score, createsDuplicates,
                    isStrictlySorted, flowsAllRequiredFields, includedRankComparisons);
        }
    }

    protected static class ScoredMatch extends ScoredInfo<ComparisonRanges, ScoredMatch> {
        public ScoredMatch(int score, @Nonnull ComparisonRanges comparisonRanges) {
            this(score, comparisonRanges, Collections.emptyList());
        }

        public ScoredMatch(int score, @Nonnull ComparisonRanges comparisonRanges,
                           @Nonnull List<QueryComponent> unsatisfiedFilters) {
            this(score, comparisonRanges, unsatisfiedFilters, false, false);
        }

        public ScoredMatch(int score, @Nonnull ComparisonRanges comparisonRanges,
                           @Nonnull List<QueryComponent> unsatisfiedFilters, boolean createsDuplicates, boolean isStrictlySorted) {
            this(comparisonRanges, unsatisfiedFilters, Collections.emptyList(), Collections.emptySet(), score,
                    createsDuplicates, isStrictlySorted, false, null);
        }

        public ScoredMatch(@Nonnull ComparisonRanges comparisonRanges,
                           @Nonnull List<QueryComponent> unsatisfiedFilters, @Nonnull final List<QueryComponent> indexFilters,
                           @Nonnull final Set<Comparisons.Comparison> sargedComparisons, int score, boolean createsDuplicates,
                           boolean isStrictlySorted, boolean flowsAllRequiredFields,
                           @Nullable Set<RankComparisons.RankComparison> includedRankComparisons) {
            super(comparisonRanges, unsatisfiedFilters, indexFilters, sargedComparisons, score, createsDuplicates,
                    isStrictlySorted, flowsAllRequiredFields, includedRankComparisons);
        }

        @Override
        protected ScoredMatch getThis() {
            return this;
        }

        /**
         * Make callers get the info in a more readable way.
         * @return the {@link ComparisonRanges}
         */
        @Nonnull
        public ComparisonRanges getComparisonRanges() {
            return info;
        }

        @Override
        protected ScoredMatch with(@Nonnull final ComparisonRanges comparisonRanges,
                                   @Nonnull final List<QueryComponent> unsatisfiedFilters,
                                   @Nonnull final List<QueryComponent> indexFilters,
                                   @Nonnull final Set<Comparisons.Comparison> sargedComparisons,
                                   final int score, final boolean createsDuplicates, final boolean isStrictlySorted,
                                   final boolean flowsAllRequiredFields,
                                   @Nullable final Set<RankComparisons.RankComparison> includedRankComparisons) {
            return new ScoredMatch(comparisonRanges, unsatisfiedFilters, indexFilters, sargedComparisons, score,
                    createsDuplicates, isStrictlySorted, flowsAllRequiredFields, includedRankComparisons);
        }

        @Nonnull
        public ScoredPlan asScoredPlan(@Nonnull final RecordQueryPlan recordQueryPlan) {
            return new ScoredPlan(recordQueryPlan, unsatisfiedFilters, indexFilters, sargedComparisons, score,
                    createsDuplicates, isStrictlySorted, flowsAllRequiredFields, includedRankComparisons);
        }
    }

    /**
     * Mini-planner for handling the way that queries with multiple filters ("ands") on indexes with multiple components
     * ("thens"). This handles things like matching comparisons to the different columns of the index and then combining
     * them into a single scan, as well as validating that the sort is matched correctly.
     *
     * <p>
     * In addition to handling cases where there really are multiple filters on compound indexes, this also handles cases
     * like (1) a single filter on a compound index and (2) multiple filters on a single index. This is because those
     * cases end up having more-or-less the same logic as the multi-field cases.
     * </p>
     */
    private abstract class AbstractAndWithThenPlanner {
        /**
         * The original root expression on the index or {@code null} if the index actually has only a single column.
         */
        @Nullable
        protected final ThenKeyExpression indexExpr;
        /**
         * The children of the root expression or a single key expression if the index actually has only a single column.
         */
        @Nonnull
        protected final List<KeyExpression> indexChildren;
        /**
         * The children of the {@link AndComponent} or a single filter if the query is actually on a single component.
         */
        @Nonnull
        protected final List<QueryComponent> filters;
        @Nullable
        protected final KeyExpression sort;
        @Nonnull
        protected final CandidateScan candidateScan;
        /**
         * The filters in the {@link AndComponent} that have not been satisfied (yet).
         */
        @Nonnull
        protected final List<QueryComponent> unsatisfiedFilters;
        /**
         * The set of sort keys that have not been satisfied (yet).
         */
        @Nonnull
        protected final List<KeyExpression> unsatisfiedSorts;
        /**
         * True if the current child of the index {@link ThenKeyExpression Then} clause has a corresponding equality comparison in the filter.
         */
        protected boolean foundComparison;
        /**
         * True if {@code foundComparison} completely accounted for the child.
         */
        protected boolean foundCompleteComparison;

        @SpotBugsSuppressWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", justification = "maybe https://github.com/spotbugs/spotbugs/issues/616?")
        protected AbstractAndWithThenPlanner(@Nonnull CandidateScan candidateScan,
                                             @Nullable ThenKeyExpression indexExpr,
                                             @Nonnull List<KeyExpression> indexChildren,
                                             @Nonnull List<QueryComponent> filters,
                                             @Nullable KeyExpression sort) {
            this.indexExpr = indexExpr;
            this.indexChildren = indexChildren;
            this.filters = filters;
            this.sort = sort;
            this.candidateScan = candidateScan;
            unsatisfiedFilters = new ArrayList<>();
            unsatisfiedSorts = new ArrayList<>();
        }

        @Nullable
        public abstract ScoredMatch plan();

        protected void setupPlanState() {
            unsatisfiedFilters.clear();
            unsatisfiedFilters.addAll(filters);
            unsatisfiedSorts.clear();

            if (sort != null) {
                KeyExpression sortKey = sort;
                if (sortKey instanceof GroupingKeyExpression) {
                    sortKey = ((GroupingKeyExpression) sortKey).getWholeKey();
                }
                if (sortKey instanceof ThenKeyExpression) {
                    ThenKeyExpression sortThen = (ThenKeyExpression) sortKey;
                    unsatisfiedSorts.addAll(sortThen.getChildren());
                } else {
                    unsatisfiedSorts.add(sortKey);
                }
            }
        }

        protected void planChild(@Nonnull KeyExpression child) {
            foundCompleteComparison = foundComparison = false;
            if (child instanceof RecordTypeKeyExpression) {
                if (candidateScan.planContext.query.getRecordTypes().size() == 1) {
                    // Can scan just the one requested record type.
                    final RecordTypeKeyComparison recordTypeKeyComparison = new RecordTypeKeyComparison(candidateScan.planContext.query.getRecordTypes().iterator().next());
                    addToComparisons(recordTypeKeyComparison.getComparison());
                    foundCompleteComparison = true;
                }
                return;
            }
            // It may be possible to match a nested then to multiple filters, but the filters need to be adjusted a bit.
            // Cf. planAndWithNesting
            if (child instanceof NestingKeyExpression &&
                    filters.size() > 1 &&
                    child.getColumnSize() > 1) {
                final NestingKeyExpression nestingKey = (NestingKeyExpression)child;
                final FieldKeyExpression parent = nestingKey.getParent();
                if (parent.getFanType() == FanType.None) {
                    final List<QueryComponent> nestedFilters = new ArrayList<>();
                    final List<QueryComponent> nestedChildren = new ArrayList<>();
                    for (QueryComponent filterChild : filters) {
                        if (filterChild instanceof NestedField) {
                            final NestedField nestedField = (NestedField) filterChild;
                            if (parent.getFieldName().equals(nestedField.getFieldName())) {
                                nestedFilters.add(nestedField);
                                nestedChildren.add(nestedField.getChild());
                            }
                        }
                    }
                    if (nestedFilters.size() > 1) {
                        final NestedField nestedAnd = new NestedField(parent.getFieldName(), Query.and(nestedChildren));
                        final List<QueryComponent> saveUnsatisfiedFilters = new ArrayList<>(unsatisfiedFilters);
                        unsatisfiedFilters.removeAll(nestedFilters);
                        unsatisfiedFilters.add(nestedAnd);
                        if (planNestedFieldChild(child, nestedAnd, nestedAnd)) {
                            return;
                        }
                        unsatisfiedFilters.clear();
                        unsatisfiedFilters.addAll(saveUnsatisfiedFilters);
                    }
                }
            }
            for (QueryComponent filterChild : filters) {
                QueryComponent filterComponent = candidateScan.planContext.rankComparisons.planComparisonSubstitute(filterChild);
                if (filterComponent instanceof FieldWithComparison) {
                    planWithComparisonChild(child, (FieldWithComparison) filterComponent, filterChild);
                } else if (filterComponent instanceof NestedField) {
                    planNestedFieldChild(child, (NestedField) filterComponent, filterChild);
                } else if (filterComponent instanceof OneOfThemWithComponent) {
                    planOneOfThemWithComponentChild(child, (OneOfThemWithComponent) filterComponent, filterChild);
                } else if (filterComponent instanceof OneOfThemWithComparison) {
                    planOneOfThemWithComparisonChild(child, (OneOfThemWithComparison) filterComponent, filterChild);
                } else if (filterComponent instanceof QueryRecordFunctionWithComparison
                           && FunctionNames.VERSION.equals(((QueryRecordFunctionWithComparison) filterComponent).getFunction().getName())) {
                    planWithVersionComparisonChild(child, (QueryRecordFunctionWithComparison) filterComponent, filterChild);
                } else if (filterComponent instanceof QueryKeyExpressionWithComparison) {
                    planWithComparisonChild(child, (QueryKeyExpressionWithComparison) filterComponent, filterChild);
                } else if (filterComponent instanceof QueryKeyExpressionWithOneOfComparison) {
                    planOneOfThemWithComparisonChild(child, (QueryKeyExpressionWithOneOfComparison) filterComponent, filterChild);
                }
                if (foundComparison) {
                    break;
                }
            }
        }

        private boolean planNestedFieldChild(@Nonnull KeyExpression child, @Nonnull NestedField filterField, @Nonnull QueryComponent filterChild) {
            return planNestedFieldOrComponentChild(child, filterChild,
                    (maybeSort) -> planNestedField(candidateScan, child, filterField, maybeSort));
        }

        private boolean planOneOfThemWithComponentChild(@Nonnull KeyExpression child, @Nonnull OneOfThemWithComponent oneOfThemWithComponent, @Nonnull QueryComponent filterChild) {
            return planNestedFieldOrComponentChild(child, filterChild,
                    (maybeSort) -> planOneOfThemWithComponent(candidateScan, child, oneOfThemWithComponent, maybeSort));
        }

        protected abstract boolean planNestedFieldOrComponentChild(@Nonnull KeyExpression child,
                                                                   @Nonnull QueryComponent filterChild,
                                                                   @Nonnull Function<KeyExpression, ScoredMatch> maybeSortedMatch);

        protected abstract int getEqualitySize();

        private void planWithComparisonChild(@Nonnull KeyExpression child, @Nonnull FieldWithComparison field, @Nonnull QueryComponent filterChild) {
            if (child instanceof FieldKeyExpression) {
                FieldKeyExpression indexField = (FieldKeyExpression) child;
                if (Objects.equals(field.getFieldName(), indexField.getFieldName())) {
                    if (addToComparisons(field.getComparison())) {
                        addedComparison(child, filterChild);
                    }
                }
            } else if (child instanceof OrderFunctionKeyExpression) {
                OrderFunctionKeyExpression indexOrderedField = (OrderFunctionKeyExpression)child;
                if (indexOrderedField.getArguments() instanceof FieldKeyExpression) {
                    FieldKeyExpression indexField = (FieldKeyExpression) indexOrderedField.getArguments();
                    if (Objects.equals(field.getFieldName(), indexField.getFieldName())) {
                        final OrderQueryKeyExpression orderedExpression = new OrderQueryKeyExpression(indexOrderedField);
                        final Pair<Comparisons.Comparison, Comparisons.Comparison> adjustedComparisons = orderedExpression.adjustComparison(field.getComparison());
                        if (adjustedComparisons != null && addToComparisons(adjustedComparisons.getLeft())) {
                            if (adjustedComparisons.getRight() != null) {
                                addToComparisons(adjustedComparisons.getRight());
                            }
                            addedComparison(child, filterChild);
                        }
                    }
                }
            }
        }

        private void planWithComparisonChild(@Nonnull KeyExpression child, @Nonnull QueryKeyExpressionWithComparison queryKeyExpression, @Nonnull QueryComponent filterChild) {
            if (child.equals(queryKeyExpression.getKeyExpression())) {
                if (addToComparisons(queryKeyExpression.getComparison())) {
                    addedComparison(child, filterChild);
                }
            }
        }

        private void planOneOfThemWithComparisonChild(@Nonnull KeyExpression child, @Nonnull OneOfThemWithComparison oneOfThem, @Nonnull QueryComponent filterChild) {
            if (child instanceof FieldKeyExpression) {
                FieldKeyExpression indexField = (FieldKeyExpression) child;
                if (Objects.equals(oneOfThem.getFieldName(), indexField.getFieldName()) && indexField.getFanType() == FanType.FanOut) {
                    if (addToComparisons(oneOfThem.getComparison())) {
                        addedComparison(child, filterChild);
                    }
                }
            }
        }

        private void planOneOfThemWithComparisonChild(@Nonnull KeyExpression child, @Nonnull QueryKeyExpressionWithOneOfComparison queryKeyExpression, @Nonnull QueryComponent filterChild) {
            if (child.equals(queryKeyExpression.getKeyExpression())) {
                if (addToComparisons(queryKeyExpression.getComparison())) {
                    addedComparison(child, filterChild);
                }
            }
        }

        private void planWithVersionComparisonChild(@Nonnull KeyExpression child, @Nonnull QueryRecordFunctionWithComparison filter, @Nonnull QueryComponent filterChild) {
            if (child instanceof VersionKeyExpression) {
                if (addToComparisons(filter.getComparison())) {
                    addedComparison(child, filterChild);
                }
            }
        }

        protected abstract boolean addToComparisons(@Nonnull Comparisons.Comparison comparison);

        protected abstract void addedComparison(@Nonnull KeyExpression child, @Nonnull QueryComponent filterChild);
    }

    /**
     * Mini-planner for handling the way that queries with multiple filters ("ands") on indexes with multiple components
     * ("thens"). This handles things like matching comparisons to the different columns of the index and then combining
     * them into a single scan, as well as validating that the sort is matched correctly.
     *
     * <p>
     * In addition to handling cases where there really are multiple filters on compound indexes, this also handles cases
     * like (1) a single filter on a compound index and (2) multiple filters on a single index. This is because those
     * cases end up having more-or-less the same logic as the multi-field cases.
     * </p>
     */
    private class AndWithThenPlanner extends AbstractAndWithThenPlanner {
        /**
         * Accumulate matching comparisons here.
         */
        @Nonnull
        private final ScanComparisons.Builder comparisons;

        @SpotBugsSuppressWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", justification = "maybe https://github.com/spotbugs/spotbugs/issues/616?")
        public AndWithThenPlanner(@Nonnull CandidateScan candidateScan,
                                  @Nonnull ThenKeyExpression indexExpr,
                                  @Nonnull AndComponent filter,
                                  @Nullable KeyExpression sort) {
            this(candidateScan, indexExpr, filter.getChildren(), sort);
        }

        @SpotBugsSuppressWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", justification = "maybe https://github.com/spotbugs/spotbugs/issues/616?")
        public AndWithThenPlanner(@Nonnull CandidateScan candidateScan,
                                  @Nonnull ThenKeyExpression indexExpr,
                                  @Nonnull List<QueryComponent> filters,
                                  @Nullable KeyExpression sort) {
            this (candidateScan, indexExpr, indexExpr.getChildren(), filters, sort);
        }

        @SpotBugsSuppressWarnings(value = {"NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", "NP_NONNULL_PARAM_VIOLATION"}, justification = "maybe https://github.com/spotbugs/spotbugs/issues/616?")
        public AndWithThenPlanner(@Nonnull CandidateScan candidateScan,
                                  @Nonnull List<KeyExpression> indexChildren,
                                  @Nonnull AndComponent filter,
                                  @Nullable KeyExpression sort) {
            this(candidateScan, null, indexChildren, filter.getChildren(), sort);
        }

        @SpotBugsSuppressWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", justification = "maybe https://github.com/spotbugs/spotbugs/issues/616?")
        private AndWithThenPlanner(@Nonnull CandidateScan candidateScan,
                                   @Nullable ThenKeyExpression indexExpr,
                                   @Nonnull List<KeyExpression> indexChildren,
                                   @Nonnull List<QueryComponent> filters,
                                   @Nullable KeyExpression sort) {
            super(candidateScan, indexExpr, indexChildren, filters, sort);
            this.comparisons = new ScanComparisons.Builder();
        }

        @Nullable
        @Override
        public ScoredMatch plan() {
            setupPlanState();
            boolean doneComparing = false;
            boolean strictlySorted = true;
            int childColumns = 0;
            for (KeyExpression child : indexChildren) {
                if (!doneComparing) {
                    planChild(child);
                    if (!comparisons.isEquality() || !foundCompleteComparison) {
                        // Didn't add another equality or only did part of child; done matching filters to index.
                        doneComparing = true;
                    }
                }
                if (doneComparing) {
                    if (unsatisfiedSorts.isEmpty()) {
                        if (!(candidateScan.index != null && candidateScan.index.isUnique() && childColumns >= candidateScan.index.getColumnSize())) {
                            // More index children than sorts, except for unique index sorted up far enough.
                            strictlySorted = false;
                        }
                        break;
                    }
                    // With inequalities or no filters, index ordering must match sort ordering.
                    if (!nextSortSatisfied(child, childColumns)) {
                        break;
                    }
                }
                childColumns += child.getColumnSize();
            }
            if (!unsatisfiedSorts.isEmpty()) {
                return null;
            }
            if (comparisons.isEmpty()) {
                return null;
            }
            boolean createsDuplicates = false;
            if (candidateScan.index != null) {
                if (!candidateScan.planContext.allowDuplicates) {
                    createsDuplicates = candidateScan.index.getRootExpression().createsDuplicates();
                }
                if (createsDuplicates && indexExpr != null && indexExpr.createsDuplicatesAfter(comparisons.size())) {
                    // If fields after we stopped comparing create duplicates, they might be empty, so that a record
                    // that otherwise matches the comparisons would be absent from the index entirely.
                    return null;
                }
            }
            final ComparisonRanges comparisonRanges = ComparisonRanges.from(comparisons.build());
            return new ScoredMatch(comparisons.totalSize(), comparisonRanges, unsatisfiedFilters, createsDuplicates, strictlySorted);
        }

        @Override
        protected void setupPlanState() {
            super.setupPlanState();
            comparisons.clear();
        }

        @Override
        protected int getEqualitySize() {
            return comparisons.getEqualitySize();
        }

        @Override
        protected boolean planNestedFieldOrComponentChild(@Nonnull KeyExpression child,
                                                          @Nonnull QueryComponent filterChild,
                                                          @Nonnull Function<KeyExpression, ScoredMatch> maybeSortedMatch) {
            ScoredMatch scoredMatch = maybeSortedMatch.apply(null);
            if (scoredMatch != null) {
                ScanComparisons nextComparisons = scoredMatch.getComparisonRanges().toScanComparisons();
                if (!comparisons.isEquality() && nextComparisons.getEqualitySize() > 0) {
                    throw new Query.InvalidExpressionException(
                            "Two nested fields in the same and clause, combine them into one");
                } else {
                    if (!unsatisfiedSorts.isEmpty() && !nextComparisons.isEquality()) {
                        // Didn't plan to equality, need to try with sorting.
                        scoredMatch = maybeSortedMatch.apply(unsatisfiedSorts.get(0));
                        nextComparisons = scoredMatch == null ? null : scoredMatch.getComparisonRanges().toScanComparisons();
                    }
                    if (scoredMatch != null) {
                        unsatisfiedFilters.remove(filterChild);
                        unsatisfiedFilters.addAll(scoredMatch.unsatisfiedFilters);
                        comparisons.addAll(nextComparisons);
                        if (nextComparisons.isEquality()) {
                            foundComparison = true;
                            foundCompleteComparison = nextComparisons.getEqualitySize() == child.getColumnSize();
                            satisfyEqualitySort(child);
                        }
                        return true;
                    }
                }
            }
            return false;
        }

        // A sort key corresponding to an equality comparison is (trivially) satisfied throughout.
        @SuppressWarnings({"PMD.EmptyWhileStmt", "StatementWithEmptyBody"})
        private void satisfyEqualitySort(@Nonnull KeyExpression child) {
            while (unsatisfiedSorts.remove(child)) {
                // Keep removing all occurrences.
            }
        }

        // Does this sort key from an inequality comparison or in the index after filters match what's pending?
        private boolean nextSortSatisfied(@Nonnull KeyExpression child, int childColumns) {
            if (unsatisfiedSorts.isEmpty()) {
                return false;
            }
            if (child.equals(unsatisfiedSorts.get(0))) {
                unsatisfiedSorts.remove(0);
                return true;
            }
            int childSize = child.getColumnSize();
            if (childSize > 1) {
                List<KeyExpression> flattenedChildren = child.normalizeKeyForPositions();
                int childEqualityOffset = comparisons.getEqualitySize() - childColumns;
                int remainingChildren = flattenedChildren.size() - childEqualityOffset;
                if (remainingChildren > 0) {
                    for (int i = 0; i < remainingChildren; i++) {
                        if (flattenedChildren.get(childEqualityOffset + i).equals(unsatisfiedSorts.get(0))) {
                            unsatisfiedSorts.remove(0);
                        } else {
                            return false;
                        }
                    }
                    return true;
                }
            }
            return false;
        }

        @Override
        protected boolean addToComparisons(@Nonnull Comparisons.Comparison comparison) {
            switch (ScanComparisons.getComparisonType(comparison)) {
                case EQUALITY:
                    // TODO: If there is an equality on the same field as inequalities, it
                    //  would have been better to get it earlier and potentially match more of
                    //  the index. Which may require two passes over filter children.
                    if (comparisons.isEquality()) {
                        comparisons.addEqualityComparison(comparison);
                        foundComparison = true;
                        return true;
                    }
                    break;
                case INEQUALITY:
                    comparisons.addInequalityComparison(comparison);
                    return true;
                default:
                    break;
            }
            return false;
        }

        @Override
        protected void addedComparison(@Nonnull KeyExpression child, @Nonnull QueryComponent filterChild) {
            unsatisfiedFilters.remove(filterChild);
            if (foundComparison) {
                foundCompleteComparison = true;
                satisfyEqualitySort(child);
            }
        }
    }

    /**
     * Mini-planner for handling the way that queries with multiple filters ("ands") on multidimensional indexes with
     * multiple components ("thens"). This handles things like matching comparisons to the different columns of the
     * index and then combining them into a single scan.
     * <p>
     * In particular, this specific mini planner matches filters to sub expressions in the index not considering
     * matches of preceding index sub expressions, i.e. this planner matches a filter to a sub expression independently
     * of any other match between a filter and a sub expression of the index. This allows for free permutation of
     * dimension columns for multidimensional indexes.
     * </p>
     * <p>
     * If an index scan over a multidimensional index can be planned, we will still just plan a regular by-value
     * prefix scan of that index here but leave all necessary bread crumbs to create a multi-dimensional index scan
     * plan in {@link #matchToPlan(CandidateScan, ScoredMatch)}.
     * </p>
     */
    private class MultidimensionalAndWithThenPlanner extends AbstractAndWithThenPlanner {
        /**
         * Accumulate matching comparisons here.
         */
        @Nonnull
        private final ComparisonRanges comparisons;

        @SpotBugsSuppressWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", justification = "maybe https://github.com/spotbugs/spotbugs/issues/616?")
        private MultidimensionalAndWithThenPlanner(@Nonnull CandidateScan candidateScan,
                                                   @Nullable ThenKeyExpression indexExpr,
                                                   @Nonnull List<KeyExpression> indexChildren,
                                                   @Nonnull List<QueryComponent> filters,
                                                   @Nullable KeyExpression sort) {
            super(candidateScan, indexExpr, indexChildren, filters, sort);
            comparisons = new ComparisonRanges();
        }

        @Nullable
        @Override
        public ScoredMatch plan() {
            setupPlanState();
            if (!unsatisfiedSorts.isEmpty()) {
                return null;
            }

            // non-equalities comprises inequalities and no matches (empty matches)
            boolean matchedNonEqualities = false;
            boolean strictlySorted = true;
            int childColumns = 0;
            for (KeyExpression child : indexChildren) {
                final int childColumnSize = child.getColumnSize();
                planChild(child);
                final int uncommittedComparisonRangesSize = comparisons.uncommittedComparisonRangesSize();
                if (uncommittedComparisonRangesSize < childColumnSize) {
                    // nothing added in this iteration
                    comparisons.addEmptyRanges(childColumnSize - uncommittedComparisonRangesSize);
                }
                if (!comparisons.isEqualities() || !foundCompleteComparison) {
                    // Didn't add another equality or only did part of child; done matching filters to index.
                    matchedNonEqualities = true;
                }
                if (matchedNonEqualities) {
                    if (strictlySorted &&
                            !(candidateScan.index != null &&
                              candidateScan.index.isUnique() &&
                              childColumns >= candidateScan.index.getColumnSize())) {
                        // More index children than sorts, except for unique index sorted up far enough.
                        strictlySorted = false;
                    }
                }
                childColumns += childColumnSize;

                comparisons.commitAndAdvance();
            }
            if (comparisons.isEmpty()) {
                return null;
            }
            boolean createsDuplicates = false;
            if (candidateScan.index != null) {
                if (!candidateScan.planContext.allowDuplicates) {
                    createsDuplicates = candidateScan.index.getRootExpression().createsDuplicates();
                }
                if (createsDuplicates && indexExpr != null && indexExpr.createsDuplicatesAfter(comparisons.size())) {
                    // If fields after we stopped comparing create duplicates, they might be empty, so that a record
                    // that otherwise matches the comparisons would be absent from the index entirely.
                    return null;
                }
            }
            return new ScoredMatch(comparisons.totalSize(), comparisons, unsatisfiedFilters, createsDuplicates,
                    strictlySorted);
        }

        @Override
        protected void setupPlanState() {
            super.setupPlanState();
            comparisons.clear();
        }

        @Override
        protected int getEqualitySize() {
            return comparisons.getEqualitiesSize();
        }

        @Override
        protected boolean planNestedFieldOrComponentChild(@Nonnull KeyExpression child,
                                                          @Nonnull QueryComponent filterChild,
                                                          @Nonnull Function<KeyExpression, ScoredMatch> maybeSortedMatch) {
            @Nullable ScoredMatch scoredMatch = maybeSortedMatch.apply(null);
            if (scoredMatch != null) {
                ComparisonRanges nextComparisonRanges = scoredMatch.getComparisonRanges();
                if (!comparisons.isUncommitedComparisonRangesEqualities() && nextComparisonRanges.getEqualitiesSize() > 0) {
                    throw new Query.InvalidExpressionException("Two nested fields in the same and clause, combine them into one");
                } else {
                    // nextComparisonRanges should not be null at this point
                    Objects.requireNonNull(nextComparisonRanges);
                    unsatisfiedFilters.remove(filterChild);
                    unsatisfiedFilters.addAll(scoredMatch.unsatisfiedFilters);
                    comparisons.addAll(nextComparisonRanges);
                    if (nextComparisonRanges.isEqualities()) {
                        foundComparison = true;
                        foundCompleteComparison = nextComparisonRanges.getEqualitiesSize() == child.getColumnSize();
                    }
                    return true;
                }
            }
            return false;
        }

        @Override
        protected boolean addToComparisons(@Nonnull Comparisons.Comparison comparison) {
            switch (ScanComparisons.getComparisonType(comparison)) {
                case EQUALITY:
                    comparisons.addEqualityComparison(comparison);
                    foundComparison = true;
                    return true;
                case INEQUALITY:
                    comparisons.addInequalityComparison(comparison);
                    return true;
                default:
                    break;
            }
            return false;
        }

        @Override
        protected void addedComparison(@Nonnull KeyExpression child, @Nonnull QueryComponent filterChild) {
            unsatisfiedFilters.remove(filterChild);
            if (foundComparison) {
                foundCompleteComparison = true;
            }
        }
    }
}
