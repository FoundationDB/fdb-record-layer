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
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression.FanType;
import com.apple.foundationdb.record.metadata.expressions.KeyWithValueExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.RecordTypeKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.leaderboard.TimeWindowRecordFunction;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.AndComponent;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.FieldWithComparison;
import com.apple.foundationdb.record.query.expressions.LuceneQueryComponent;
import com.apple.foundationdb.record.query.expressions.NestedField;
import com.apple.foundationdb.record.query.expressions.OneOfThemWithComparison;
import com.apple.foundationdb.record.query.expressions.OneOfThemWithComponent;
import com.apple.foundationdb.record.query.expressions.OrComponent;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.expressions.QueryKeyExpressionWithComparison;
import com.apple.foundationdb.record.query.expressions.QueryRecordFunctionWithComparison;
import com.apple.foundationdb.record.query.expressions.RecordTypeKeyComparison;
import com.apple.foundationdb.record.query.plan.planning.BooleanNormalizer;
import com.apple.foundationdb.record.query.plan.planning.FilterSatisfiedMask;
import com.apple.foundationdb.record.query.plan.planning.InExtractor;
import com.apple.foundationdb.record.query.plan.planning.RankComparisons;
import com.apple.foundationdb.record.query.plan.planning.TextScanPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithIndex;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScanPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTextIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryTypeFilterPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import com.apple.foundationdb.record.query.plan.temp.PlanContext;
import com.apple.foundationdb.record.query.plan.temp.explain.PlannerGraphProperty;
import com.apple.foundationdb.record.query.plan.temp.properties.FieldWithComparisonCountProperty;
import com.apple.foundationdb.record.query.plan.visitor.FilterVisitor;
import com.apple.foundationdb.record.query.plan.visitor.RecordQueryPlannerSubstitutionVisitor;
import com.apple.foundationdb.record.query.plan.visitor.UnorderedPrimaryKeyDistinctVisitor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
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
    @VisibleForTesting
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

    /**
     * Set the {@link RecordQueryPlannerConfiguration} for this planner.
     * If an {@link com.apple.foundationdb.record.query.plan.QueryPlanner.IndexScanPreference} is already set using
     * {@link #setIndexScanPreference(IndexScanPreference)} then it will be ignored.
     * @param configuration a configuration object for this planner
     */
    public void setConfiguration(@Nonnull RecordQueryPlannerConfiguration configuration) {
        this.configuration = configuration;
    }

    @Nonnull
    public RecordQueryPlannerConfiguration getConfiguration() {
        return configuration;
    }

    /**
     * Get the {@link RecordMetaData} for this planner.
     * @return the meta-data
     */
    @Nonnull
    public RecordMetaData getRecordMetaData() {
        return metaData;
    }

    /**
     * Get the {@link RecordStoreState} for this planner.
     * @return the record store state
     */
    @Nonnull
    public RecordStoreState getRecordStoreState() {
        return recordStoreState;
    }

    /**
     * Create a plan to get the results of the provided query.
     *
     * @param query a query for records on this planner's metadata
     * @return a plan that will return the results of the provided query when executed
     * @throws com.apple.foundationdb.record.RecordCoreException if there is no index that matches the sort in the provided query
     */
    @Nonnull
    @Override
    public RecordQueryPlan plan(@Nonnull RecordQuery query) {
        query.validate(metaData);

        final PlanContext planContext = getPlanContext(query);

        final BooleanNormalizer normalizer = BooleanNormalizer.forConfiguration(configuration);
        final QueryComponent filter = normalizer.normalizeIfPossible(query.getFilter());
        final KeyExpression sort = query.getSort();
        final boolean sortReverse = query.isSortReverse();

        RecordQueryPlan plan = null;
        if (filter == null) {
            plan = planNoFilter(planContext, sort, sortReverse);
        } else {
            ScoredPlan bestPlan = planFilter(planContext, filter);
            if (bestPlan != null) {
                plan = bestPlan.plan;
            }
        }
        if (plan == null) {
            if (sort == null) {
                plan = planScan(new CandidateScan(planContext, null, false));
                if (filter != null) {
                    plan = new RecordQueryFilterPlan(plan, filter);
                }
            } else {
                throw new RecordCoreException("Cannot sort without appropriate index: " + sort);
            }
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
        if (configuration.shouldDeferFetchAfterUnionAndIntersection()) {
            plan = RecordQueryPlannerSubstitutionVisitor.applyVisitors(plan, metaData, indexTypes, planContext.commonPrimaryKey);
        } else {
            // Always do filter pushdown
            plan = plan.accept(new FilterVisitor(metaData, indexTypes, planContext.commonPrimaryKey));
        }
        if (query.getRequiredResults() != null) {
            plan = tryToConvertToCoveringPlan(planContext, plan);
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
                p = planSortOnly(new CandidateScan(planContext, index, sortReverse), index.getRootExpression(), sort);
            }
            if (p != null) {
                if (bestPlan == null || p.score > bestPlan.score ||
                        (p.score == bestPlan.score && compareIndexes(planContext, index, bestIndex) > 0)) {
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
            return bestPlan.plan;
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
        return new ScoredPlan(0, planScan(new CandidateScan(planContext, index, false), scanComparisons));
    }

    private int compareIndexes(PlanContext planContext, @Nullable Index index1, @Nullable Index index2) {
        if (index1 == null) {
            if (index2 == null) {
                return 0;
            } else {
                return preferIndexToScan(planContext, index2) ? -1 : +1;
            }
        } else if (index2 == null) {
            return preferIndexToScan(planContext, index1) ? +1 : -1;
        } else {
            // Better for fewer stored columns.
            return Integer.compare(indexSizeOverhead(planContext, index2), indexSizeOverhead(planContext, index1));
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

    @Nullable
    private ScoredPlan planFilter(@Nonnull PlanContext planContext, @Nonnull QueryComponent filter, boolean needOrdering) {
        final InExtractor inExtractor = new InExtractor(filter);
        ScoredPlan withInAsOr = null;
        if (planContext.query.getSort() != null) {
            boolean canSort = inExtractor.setSort(planContext.query.getSort(), planContext.query.isSortReverse());
            if (!canSort && getConfiguration().shouldAttemptFailedInJoinAsOr()) {
                // Can't implement as an in join because of the sort order. Try as an OR instead.
                withInAsOr = planFilter(planContext, normalizeAndOrForInAsOr(inExtractor.asOr()));
            }
        } else if (needOrdering) {
            inExtractor.sortByClauses();
        }
        final ScoredPlan withInJoin = planFilterWithInJoin(planContext, inExtractor, needOrdering);
        if (withInAsOr != null) {
            if (withInJoin == null || withInAsOr.score > withInJoin.score ||
                    FieldWithComparisonCountProperty.evaluate(withInAsOr.plan) < FieldWithComparisonCountProperty.evaluate(withInJoin.plan)) {
                return withInAsOr;
            }
        }
        return withInJoin;
    }

    private ScoredPlan planFilterWithInJoin(@Nonnull PlanContext planContext, @Nonnull InExtractor inExtractor, boolean needOrdering) {
        final QueryComponent filter = inExtractor.subFilter();
        planContext.rankComparisons = new RankComparisons(filter, planContext.indexes);
        List<ScoredPlan> intersectionCandidates = new ArrayList<>();
        ScoredPlan bestPlan = null;
        Index bestIndex = null;
        if (planContext.commonPrimaryKey != null) {
            bestPlan = planIndex(planContext, filter, null, planContext.commonPrimaryKey, intersectionCandidates);
        }
        for (Index index : planContext.indexes) {
            KeyExpression indexKeyExpression = index.getRootExpression();
            if (indexKeyExpression instanceof KeyWithValueExpression) {
                indexKeyExpression = ((KeyWithValueExpression) indexKeyExpression).getKeyExpression();
            }

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
            final RecordQueryPlan wrapped = inExtractor.wrap(planContext.rankComparisons.wrap(bestPlan.plan, bestPlan.includedRankComparisons, metaData));
            ScoredPlan scoredPlan = new ScoredPlan(bestPlan.score, wrapped);
            if (needOrdering) {
                PlanOrderingKey planOrderingKey = PlanOrderingKey.forPlan(metaData, bestPlan.plan, planContext.commonPrimaryKey);
                planOrderingKey = inExtractor.adjustOrdering(planOrderingKey);
                scoredPlan.planOrderingKey = planOrderingKey;
            }
            return scoredPlan;
        }
        return null;
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
                    if (compareIndexes(planContext, index, otherIndex) > 0) {
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
            } else if (indexTypes.getTextTypes().contains(index.getType())) {
                p = planText(candidateScan, index, filter, sort);
                if (p != null) {
                    p = planRemoveDuplicates(planContext, p);
                }
                if (p != null) {
                    p = computeIndexFilters(planContext, p);
                }
                return p;
            } else if (indexTypes.getLuceneTypes().contains(index.getType())) {
                p = planLucene(candidateScan, index, filter, sort);
                if (p != null) {
                    p = planRemoveDuplicates(planContext, p);
                }
                if (p != null) {
                    p = computeIndexFilters(planContext, p);
                }
                return p;
            } else if (!indexTypes.getValueTypes().contains(index.getType())) {
                return null;
            }
        }
        if (p == null) {
            p = planCandidateScan(candidateScan, indexExpr, filter, sort);
        }
        if (p == null) {
            // we can't match the filter, but maybe the sort
            p = planSortOnly(candidateScan, indexExpr, sort);
            if (p != null) {
                final List<QueryComponent> unsatisfiedFilters = filter instanceof AndComponent ?
                                                                ((AndComponent) filter).getChildren() :
                                                                Collections.singletonList(filter);
                p = new ScoredPlan(0, p.plan, unsatisfiedFilters, p.createsDuplicates);
            }
        }

        if (p != null) {
            if (getConfiguration().shouldOptimizeForIndexFilters()) {
                // partition index filters
                if (index == null) {
                    // if we scan without an index all filters become index filters as we don't need a fetch
                    // to evaluate these filters
                    p = p.withFilters(Collections.emptyList(), p.combineNonSargables());
                } else {
                    p = computeIndexFilters(planContext, p);
                }
            }
        }

        if (p != null) {
            p = planRemoveDuplicates(planContext, p);
            if (p != null && p.getNumNonSargables() > 0) {
                PlanOrderingKey planOrderingKey = PlanOrderingKey.forPlan(metaData, p.plan, planContext.commonPrimaryKey);
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

    private ScoredPlan computeIndexFilters(@Nonnull PlanContext planContext, @Nonnull final ScoredPlan plan) {
        if (plan.plan instanceof RecordQueryPlanWithIndex) {
            final RecordQueryPlanWithIndex indexPlan = (RecordQueryPlanWithIndex) plan.plan;
            final Index index = metaData.getIndex(indexPlan.getIndexName());
            final Collection<RecordType> recordTypes = metaData.recordTypesForIndex(index);
            if (recordTypes.size() != 1) {
                return plan;
            }
            final RecordType recordType = Iterables.getOnlyElement(recordTypes);
            final List<QueryComponent> unsatisfiedFilters = new ArrayList<>(plan.unsatisfiedFilters);
            final AvailableFields availableFieldsFromIndex =
                    AvailableFields.fromIndex(recordType, index, indexTypes, planContext.commonPrimaryKey);

            final List<QueryComponent> indexFilters = Lists.newArrayListWithCapacity(unsatisfiedFilters.size());
            final List<QueryComponent> residualFilters = Lists.newArrayListWithCapacity(unsatisfiedFilters.size());
            FilterVisitor.partitionFilters(unsatisfiedFilters,
                    availableFieldsFromIndex,
                    indexFilters,
                    residualFilters,
                    null);

            if (!indexFilters.isEmpty()) {
                return plan.withFilters(residualFilters, indexFilters);
            }
        }
        return plan;
    }

    @Nullable
    private ScoredPlan planCandidateScan(@Nonnull CandidateScan candidateScan,
                                         @Nonnull KeyExpression index,
                                         @Nonnull QueryComponent filter, @Nullable KeyExpression sort) {
        filter = candidateScan.planContext.rankComparisons.planComparisonSubstitute(filter);
        if (filter instanceof FieldWithComparison) {
            return planFieldWithComparison(candidateScan, index, (FieldWithComparison) filter, sort);
        } else if (filter instanceof OneOfThemWithComparison) {
            return planOneOfThemWithComparison(candidateScan, index, (OneOfThemWithComparison) filter, sort);
        } else if (filter instanceof AndComponent) {
            return planAnd(candidateScan, index, (AndComponent) filter, sort);
        } else if (filter instanceof NestedField) {
            return planNestedField(candidateScan, index, (NestedField) filter, sort);
        } else if (filter instanceof OneOfThemWithComponent) {
            return planOneOfThemWithComponent(candidateScan, index, (OneOfThemWithComponent) filter, sort);
        } else if (filter instanceof QueryRecordFunctionWithComparison) {
            if (((QueryRecordFunctionWithComparison) filter).getFunction().getName().equals(FunctionNames.VERSION)) {
                return planVersion(candidateScan, index, (QueryRecordFunctionWithComparison) filter, sort);
            }
        } else if (filter instanceof QueryKeyExpressionWithComparison) {
            return planQueryKeyExpressionWithComparison(candidateScan, index, (QueryKeyExpressionWithComparison) filter, sort);
        }
        return null;
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
                final List<RecordType> recordTypes = query.getRecordTypes().stream().map(metaData::getRecordType).collect(Collectors.toList());
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
            return new ScoredPlan(new RecordQueryUnorderedPrimaryKeyDistinctPlan(plan.plan), plan.unsatisfiedFilters, plan.indexFilters, plan.score,
                    false, plan.includedRankComparisons);
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
            final RecordQueryPlan filtered = new RecordQueryFilterPlan(bestPlan.plan,
                    planContext.rankComparisons.planComparisonSubstitutes(bestPlan.combineNonSargables()));
            // TODO: further optimization requires knowing which filters are satisfied
            return new ScoredPlan(filtered, Collections.emptyList(), Collections.emptyList(), bestPlan.score,
                    bestPlan.createsDuplicates, bestPlan.includedRankComparisons);
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
                        .thenComparing(Comparator.comparingInt(ScoredPlan::getNumIndexFilters).reversed()));
        // Since we limited to isPrimaryKeyOrdered(), comparisonKey will always work.
        ScoredPlan plan1 = intersectionCandidates.get(0);
        List<QueryComponent> nonSargables = new ArrayList<>(plan1.combineNonSargables());
        Set<RankComparisons.RankComparison> includedRankComparisons =
                mergeRankComparisons(null, plan1.includedRankComparisons);
        RecordQueryPlan plan = plan1.plan;
        List<RecordQueryPlan> includedPlans = new ArrayList<>(intersectionCandidates.size());
        includedPlans.add(plan);
        // TODO optimize so that we don't do excessive intersections
        for (int i = 1; i < intersectionCandidates.size(); i++) {
            ScoredPlan nextPlan = intersectionCandidates.get(i);
            List<QueryComponent> nextNonSargables = new ArrayList<>(nextPlan.combineNonSargables());
            int oldCount = nonSargables.size();
            nonSargables.retainAll(nextNonSargables);
            if (nonSargables.size() < oldCount) {
                if (plan.isReverse() != nextPlan.plan.isReverse()) {
                    // Cannot intersect plans with incompatible reverse settings.
                    return null;
                }
                includedPlans.add(nextPlan.plan);
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
            return new ScoredPlan(intersectionPlan, nonSargables, Collections.emptyList(), plan1.score, plan1.createsDuplicates, includedRankComparisons);
        } else {
            return null;
        }
    }

    @Nullable
    private ScoredPlan planOneOfThemWithComponent(@Nonnull CandidateScan candidateScan,
                                                  @Nonnull KeyExpression index,
                                                  @Nonnull OneOfThemWithComponent filter,
                                                  @Nullable KeyExpression sort) {
        if (index instanceof FieldKeyExpression) {
            return null;
        } else if (index instanceof ThenKeyExpression) {
            ThenKeyExpression then = (ThenKeyExpression) index;
            return planOneOfThemWithComponent(candidateScan, then.getChildren().get(0), filter, sort);
        } else if (index instanceof NestingKeyExpression) {
            NestingKeyExpression indexNesting = (NestingKeyExpression) index;
            ScoredPlan plan = null;
            if (sort == null) {
                plan = planNesting(candidateScan, indexNesting, filter, null);
            } else if (sort instanceof FieldKeyExpression) {
                plan = null;
            } else if (sort instanceof ThenKeyExpression) {
                plan = null;
            } else if (sort instanceof NestingKeyExpression) {
                NestingKeyExpression sortNesting = (NestingKeyExpression) sort;
                plan = planNesting(candidateScan, indexNesting, filter, sortNesting);
            }
            if (plan != null) {
                List<QueryComponent> unsatisfied;
                if (!plan.unsatisfiedFilters.isEmpty()) {
                    unsatisfied = Collections.singletonList(filter);
                } else {
                    unsatisfied = Collections.emptyList();
                }
                // Right now it marks the whole nesting as unsatisfied, in theory there could be plans that handle that
                plan = new ScoredPlan(plan.score, plan.plan, unsatisfied, true);
            }
            return plan;
        }
        return null;
    }

    @Nullable
    private ScoredPlan planNesting(@Nonnull CandidateScan candidateScan,
                                   @Nonnull NestingKeyExpression index,
                                   @Nonnull OneOfThemWithComponent filter, @Nullable NestingKeyExpression sort) {
        if (sort == null || Objects.equals(index.getParent().getFieldName(), sort.getParent().getFieldName())) {
            // great, sort aligns
            if (Objects.equals(index.getParent().getFieldName(), filter.getFieldName())) {
                return planCandidateScan(candidateScan, index.getChild(), filter.getChild(),
                        sort == null ? null : sort.getChild());
            }
        }
        return null;
    }

    @Nullable
    @SpotBugsSuppressWarnings("NP_LOAD_OF_KNOWN_NULL_VALUE")
    private ScoredPlan planNestedField(@Nonnull CandidateScan candidateScan,
                                       @Nonnull KeyExpression index,
                                       @Nonnull NestedField filter,
                                       @Nullable KeyExpression sort) {
        if (index instanceof FieldKeyExpression) {
            return null;
        } else if (index instanceof ThenKeyExpression) {
            return planThenNestedField(candidateScan, (ThenKeyExpression)index, filter, sort);
        } else if (index instanceof NestingKeyExpression) {
            return planNestingNestedField(candidateScan, (NestingKeyExpression)index, filter, sort);
        }
        return null;
    }

    private ScoredPlan planThenNestedField(@Nonnull CandidateScan candidateScan, @Nonnull ThenKeyExpression then,
                                           @Nonnull NestedField filter, @Nullable KeyExpression sort) {
        if (sort instanceof ThenKeyExpression || then.createsDuplicates()) {
            // Too complicated for the simple checks below.
            return new AndWithThenPlanner(candidateScan, then, Collections.singletonList(filter), sort).plan();
        }
        ScoredPlan plan = planNestedField(candidateScan, then.getChildren().get(0), filter, sort);
        if (plan == null && sort != null && sort.equals(then.getChildren().get(1))) {
            ScoredPlan sortlessPlan = planNestedField(candidateScan, then.getChildren().get(0), filter, null);
            ScanComparisons sortlessComparisons = getPlanComparisons(sortlessPlan);
            if (sortlessComparisons != null && sortlessComparisons.isEquality()) {
                // A scan for an equality filter will be sorted by the next index key.
                plan = sortlessPlan;
            }
        }
        return plan;
    }

    private ScoredPlan planNestingNestedField(@Nonnull CandidateScan candidateScan, @Nonnull NestingKeyExpression nesting,
                                              @Nonnull NestedField filter, @Nullable KeyExpression sort) {
        if (Objects.equals(nesting.getParent().getFieldName(), filter.getFieldName())) {
            ScoredPlan childPlan = null;
            if (sort == null) {
                childPlan = planCandidateScan(candidateScan, nesting.getChild(), filter.getChild(), null);
            } else if (sort instanceof NestingKeyExpression) {
                NestingKeyExpression sortNesting = (NestingKeyExpression)sort;
                if (Objects.equals(sortNesting.getParent().getFieldName(), nesting.getParent().getFieldName())) {
                    childPlan = planCandidateScan(candidateScan, nesting.getChild(), filter.getChild(), sortNesting.getChild());
                }
            }

            if (childPlan != null && !childPlan.unsatisfiedFilters.isEmpty()) {
                // Add the parent to the unsatisfied filters of this ScoredPlan if non-zero.
                QueryComponent unsatisfiedFilter;
                if (childPlan.unsatisfiedFilters.size() > 1) {
                    unsatisfiedFilter = Query.field(filter.getFieldName()).matches(Query.and(childPlan.unsatisfiedFilters));
                } else {
                    unsatisfiedFilter = Query.field(filter.getFieldName()).matches(childPlan.unsatisfiedFilters.get(0));
                }
                return childPlan.withUnsatisfiedFilters(Collections.singletonList(unsatisfiedFilter));
            } else {
                return childPlan;
            }
        }
        return null;
    }

    @Nullable
    private ScanComparisons getPlanComparisons(@Nullable ScoredPlan scoredPlan) {
        return scoredPlan == null ? null : getPlanComparisons(scoredPlan.plan);
    }

    @Nullable
    private ScanComparisons getPlanComparisons(@Nonnull RecordQueryPlan plan) {
        if (plan instanceof RecordQueryIndexPlan) {
            return ((RecordQueryIndexPlan) plan).getComparisons();
        }
        if (plan instanceof RecordQueryScanPlan) {
            return ((RecordQueryScanPlan) plan).getComparisons();
        }
        if (plan instanceof RecordQueryTypeFilterPlan) {
            return getPlanComparisons(((RecordQueryTypeFilterPlan) plan).getInnerPlan());
        }
        return null;
    }

    @Nullable
    private ScoredPlan planOneOfThemWithComparison(@Nonnull CandidateScan candidateScan,
                                                   @Nonnull KeyExpression index,
                                                   @Nonnull OneOfThemWithComparison oneOfThemWithComparison,
                                                   @Nullable KeyExpression sort) {
        final Comparisons.Comparison comparison = oneOfThemWithComparison.getComparison();
        final ScanComparisons scanComparisons = ScanComparisons.from(comparison);
        if (scanComparisons == null) {
            final ScoredPlan sortOnlyPlan = planSortOnly(candidateScan, index, sort);
            if (sortOnlyPlan != null) {
                return new ScoredPlan(0, sortOnlyPlan.plan,
                        Collections.<QueryComponent>singletonList(oneOfThemWithComparison),
                        sortOnlyPlan.createsDuplicates);
            } else {
                return null;
            }
        }
        if (index instanceof FieldKeyExpression) {
            FieldKeyExpression field = (FieldKeyExpression) index;
            if (Objects.equals(oneOfThemWithComparison.getFieldName(), field.getFieldName())
                    && field.getFanType() == FanType.FanOut) {
                if (sort != null) {
                    if (sort instanceof FieldKeyExpression) {
                        FieldKeyExpression sortField = (FieldKeyExpression) sort;
                        if (Objects.equals(sortField.getFieldName(), field.getFieldName())) {
                            // everything matches, yay!! Hopefully that comparison can be for tuples
                            return new ScoredPlan(1, planScan(candidateScan, scanComparisons),
                                    Collections.<QueryComponent>emptyList(), true);
                        }
                    }
                } else {
                    return new ScoredPlan(1, planScan(candidateScan, scanComparisons),
                            Collections.<QueryComponent>emptyList(), true);
                }
            }
            return null;
        } else if (index instanceof ThenKeyExpression) {
            // May need second column to do sort, so handle like And, which does such cases.
            ThenKeyExpression then = (ThenKeyExpression) index;
            return new AndWithThenPlanner(candidateScan, then, Collections.singletonList(oneOfThemWithComparison), sort).plan();
        } else if (index instanceof NestingKeyExpression) {
            return null;
        }
        return null;
    }

    @Nullable
    private ScoredPlan planAnd(@Nonnull CandidateScan candidateScan,
                               @Nonnull KeyExpression index,
                               @Nonnull AndComponent filter,
                               @Nullable KeyExpression sort) {
        if (index instanceof NestingKeyExpression) {
            return planAndWithNesting(candidateScan, (NestingKeyExpression)index, filter, sort);
        } else if (index instanceof ThenKeyExpression) {
            return new AndWithThenPlanner(candidateScan, (ThenKeyExpression)index, filter, sort).plan();
        } else {
            return new AndWithThenPlanner(candidateScan, Collections.singletonList(index), filter, sort).plan();
        }
    }

    @Nullable
    private ScoredPlan planAndWithNesting(@Nonnull CandidateScan candidateScan,
                                          @Nonnull NestingKeyExpression index,
                                          @Nonnull AndComponent filter,
                                          @Nullable KeyExpression sort) {
        final FieldKeyExpression parent = index.getParent();
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
                final ScoredPlan plan = planNestedField(candidateScan, index, nestedAnd, sort);
                if (plan != null) {
                    if (remainingFilters.isEmpty()) {
                        return plan;
                    } else {
                        return plan.withUnsatisfiedFilters(remainingFilters);
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
                final ScoredPlan plan = planNestedField(candidateScan, index, nestedField, sort);
                if (plan != null) {
                    unsatisfiedFilters.remove(filterChild);
                    return plan.withUnsatisfiedFilters(unsatisfiedFilters);
                }
            }
        }
        return null;
    }

    @Nullable
    private ScoredPlan planFieldWithComparison(@Nonnull CandidateScan candidateScan,
                                               @Nonnull KeyExpression index,
                                               @Nonnull FieldWithComparison singleField,
                                               @Nullable KeyExpression sort) {
        final Comparisons.Comparison comparison = singleField.getComparison();
        final ScanComparisons scanComparisons = ScanComparisons.from(comparison);
        if (scanComparisons == null) {
            // This comparison cannot be accomplished with a single scan.
            // It is still possible that the sort can be accomplished with
            // this index, but this should be handled elsewhere by the planner.
            return null;
        }
        if (index instanceof FieldKeyExpression) {
            FieldKeyExpression field = (FieldKeyExpression) index;
            if (Objects.equals(singleField.getFieldName(), field.getFieldName())) {
                if (sort != null) {
                    if (sort instanceof FieldKeyExpression) {
                        FieldKeyExpression sortField = (FieldKeyExpression) sort;
                        if (Objects.equals(sortField.getFieldName(), field.getFieldName())) {
                            // everything matches, yay!! Hopefully that comparison can be for tuples
                            return new ScoredPlan(1, planScan(candidateScan, scanComparisons));
                        }
                    }
                } else {
                    return new ScoredPlan(1, planScan(candidateScan, scanComparisons));
                }
            }
            return null;
        } else if (index instanceof ThenKeyExpression) {
            ThenKeyExpression then = (ThenKeyExpression) index;
            if ((sort == null || sort.equals(then.getChildren().get(0))) &&
                    !then.createsDuplicates() &&
                    !(then.getChildren().get(0) instanceof RecordTypeKeyExpression)) {
                // First column will do it all or not.
                return planFieldWithComparison(candidateScan, then.getChildren().get(0), singleField, sort);
            } else {
                // May need second column to do sort, so handle like And, which does such cases.
                return new AndWithThenPlanner(candidateScan, then, Collections.singletonList(singleField), sort).plan();
            }
        }
        return null;
    }

    @Nullable
    private ScoredPlan planQueryKeyExpressionWithComparison(@Nonnull CandidateScan candidateScan,
                                                            @Nonnull KeyExpression index,
                                                            @Nonnull QueryKeyExpressionWithComparison queryKeyExpressionWithComparison,
                                                            @Nullable KeyExpression sort) {
        if (index.equals(queryKeyExpressionWithComparison.getKeyExpression()) && (sort == null || sort.equals(index))) {
            final Comparisons.Comparison comparison = queryKeyExpressionWithComparison.getComparison();
            final ScanComparisons scanComparisons = ScanComparisons.from(comparison);
            if (scanComparisons == null) {
                return null;
            }
            return new ScoredPlan(1, planScan(candidateScan, scanComparisons));
        } else if (index instanceof ThenKeyExpression) {
            return new AndWithThenPlanner(candidateScan, (ThenKeyExpression) index, Collections.singletonList(queryKeyExpressionWithComparison), sort).plan();
        }
        return null;
    }

    @Nullable
    private ScoredPlan planSortOnly(@Nonnull CandidateScan candidateScan,
                                    @Nonnull KeyExpression index,
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

        if (sort.isPrefixKey(index)) {
            return new ScoredPlan(0, planScan(candidateScan), Collections.emptyList(), index.createsDuplicates());
        } else {
            return null;
        }
    }

    @Nonnull
    private Set<String> getPossibleTypes(@Nonnull Index index) {
        final Collection<RecordType> recordTypes = metaData.recordTypesForIndex(index);
        if (recordTypes.size() == 1) {
            final RecordType singleRecordType = recordTypes.iterator().next();
            return Collections.singleton(singleRecordType.getName());
        } else {
            return recordTypes.stream().map(RecordType::getName).collect(Collectors.toSet());
        }
    }

    @Nonnull
    private RecordQueryPlan addTypeFilterIfNeeded(@Nonnull CandidateScan candidateScan, @Nonnull RecordQueryPlan plan,
                                                  @Nonnull Set<String> possibleTypes) {
        Collection<String> allowedTypes = candidateScan.planContext.query.getRecordTypes();
        if (!allowedTypes.isEmpty() && !allowedTypes.containsAll(possibleTypes)) {
            return new RecordQueryTypeFilterPlan(plan, allowedTypes);
        } else {
            return plan;
        }
    }

    @Nullable
    private ScoredPlan planVersion(@Nonnull CandidateScan candidateScan,
                                   @Nonnull KeyExpression index,
                                   @Nonnull QueryRecordFunctionWithComparison filter,
                                   @Nullable KeyExpression sort) {
        if (index instanceof VersionKeyExpression) {
            final Comparisons.Comparison comparison = filter.getComparison();
            final ScanComparisons comparisons = ScanComparisons.from(comparison);
            if (sort == null || sort.equals(VersionKeyExpression.VERSION)) {
                RecordQueryPlan plan = new RecordQueryIndexPlan(candidateScan.index.getName(), IndexScanType.BY_VALUE, comparisons, candidateScan.reverse);
                return new ScoredPlan(1, plan, Collections.emptyList(), false);
            }
        } else if (index instanceof ThenKeyExpression) {
            ThenKeyExpression then = (ThenKeyExpression) index;
            if (sort == null) { //&& !then.createsDuplicates()) {
                return planVersion(candidateScan, then.getChildren().get(0), filter, null);
            } else {
                return new AndWithThenPlanner(candidateScan, then, Collections.singletonList(filter), sort).plan();
            }
        }
        return null;
    }

    @Nullable
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
                return new ScoredPlan(scan, Collections.emptyList(), Collections.emptyList(), 1, createsDuplicates, Collections.singleton(rankComparison));
            }
        } else if (filter instanceof AndComponent) {
            return planRankWithAnd(candidateScan, index, indexExpr, (AndComponent) filter);
        }
        return null;
    }

    @Nullable
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
                    for (int i = 0; i < unsatisfiedFilters.size(); i++) {
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
                    }
                    final RecordQueryPlan scan = rankScan(candidateScan, filterComparison, scanComparisons);
                    final boolean createsDuplicates = RankComparisons.createsDuplicates(index, indexExpr);
                    return new ScoredPlan(scan, unsatisfiedFilters, Collections.emptyList(), indexExpr.getColumnSize(), createsDuplicates, includedRankComparisons);
                }
            }
        }
        return null;
    }

    @Nullable
    private ScoredPlan planLucene(@Nonnull CandidateScan candidateScan,
                                @Nonnull Index index, @Nonnull QueryComponent filter,
                                @Nullable KeyExpression sort) {
        if (sort != null) {
            // TODO: Full Text: Sorts are not supported with full text queries (https://github.com/FoundationDB/fdb-record-layer/issues/55)
            return null;
        }
        FilterSatisfiedMask filterMask = FilterSatisfiedMask.of(filter);
//        final TextScan scan = TextScanPlanner.getScanForQuery(index, filter, false, filterMask);
//        if (scan == null) {
//            return null;
//        }
        if (filter instanceof LuceneQueryComponent) {
            RecordQueryPlan plan = new RecordQueryIndexPlan(index.getName(), IndexScanType.BY_LUCENE,
                    ScanComparisons.from( ((LuceneQueryComponent) filter).getComparison()), false);
            // TODO: Check the rest of the fields of the text index expression to see if the sort and unsatisfied filters can be helped.
            // Add a type filter if the index is over more types than those the query specifies
            Set<String> possibleTypes = getPossibleTypes(index);
            plan = addTypeFilterIfNeeded(candidateScan, plan, possibleTypes);
            return new ScoredPlan(plan, filterMask.getUnsatisfiedFilters(), Collections.emptyList(), 10, false, null);
        } else {
            return null;
        }
    }

    @Nullable
    private ScoredPlan planText(@Nonnull CandidateScan candidateScan,
                                @Nonnull Index index, @Nonnull QueryComponent filter,
                                @Nullable KeyExpression sort) {
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
        return new ScoredPlan(plan, filterMask.getUnsatisfiedFilters(), Collections.emptyList(), 10, scan.createsDuplicates(), null);
    }

    @Nonnull
    private RecordQueryPlan planScan(@Nonnull CandidateScan candidateScan) {
        return planScan(candidateScan, null, null);
    }

    @Nonnull
    private RecordQueryPlan planScan(@Nonnull CandidateScan candidateScan,
                                     @Nullable ScanComparisons scanComparisons) {
        return planScan(candidateScan, null, scanComparisons);
    }

    @Nonnull
    private RecordQueryPlan planScan(@Nonnull CandidateScan candidateScan,
                                     @Nullable IndexScanType scanType,
                                     @Nullable ScanComparisons scanComparisons) {
        if (scanComparisons == null) {
            scanComparisons = ScanComparisons.EMPTY;
        }
        RecordQueryPlan plan;
        Set<String> possibleTypes;
        if (candidateScan.index == null) {
            if (primaryKeyHasRecordTypePrefix && RecordTypeKeyComparison.hasRecordTypeKeyComparison(scanComparisons)) {
                possibleTypes = RecordTypeKeyComparison.recordTypeKeyComparisonTypes(scanComparisons);
            } else {
                possibleTypes = metaData.getRecordTypes().keySet();
            }
            plan = new RecordQueryScanPlan(possibleTypes, scanComparisons, candidateScan.reverse);
        } else {
            if (scanType == null) {
                scanType = IndexScanType.BY_VALUE;
            }
            plan = new RecordQueryIndexPlan(candidateScan.index.getName(), scanType, scanComparisons, candidateScan.reverse);
            possibleTypes = getPossibleTypes(candidateScan.index);
        }
        // Add a type filter if the query plan might return records of more types than the query specified
        plan = addTypeFilterIfNeeded(candidateScan, plan, possibleTypes);
        return plan;
    }

    @Nonnull
    private RecordQueryPlan rankScan(@Nonnull CandidateScan candidateScan,
                                     @Nonnull QueryRecordFunctionWithComparison rank,
                                     @Nullable ScanComparisons scanComparisons) {
        if (rank.getFunction().getName().equals(FunctionNames.TIME_WINDOW_RANK)) {
            return planScan(candidateScan, IndexScanType.BY_TIME_WINDOW,
                    ((TimeWindowRecordFunction<?>) rank.getFunction()).getTimeWindow().prependLeaderboardKeys(scanComparisons));
        } else {
            return planScan(candidateScan, IndexScanType.BY_RANK, scanComparisons);
        }
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
            if (subplan.plan instanceof RecordQueryFilterPlan) {
                filteredBasePlan = ((RecordQueryFilterPlan)subplan.plan).getInnerPlan();
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
                            .map(subplan -> ((RecordQueryFilterPlan)subplan.plan).getFilter())
                            .collect(Collectors.toList())));
            ScoredPlan firstSubPlan = subplans.get(0);
            return new ScoredPlan(combinedOrFilter, Collections.emptyList(), Collections.emptyList(), firstSubPlan.score,
                    firstSubPlan.createsDuplicates, firstSubPlan.includedRankComparisons);
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
        final KeyExpression sort = planContext.query.getSort();
        KeyExpression candidateKey = planContext.commonPrimaryKey;
        boolean candidateOnly = false;
        if (sort != null) {
            candidateKey = getKeyForMerge(sort, candidateKey);
            candidateOnly = true;
        }
        KeyExpression comparisonKey = PlanOrderingKey.mergedComparisonKey(subplans, candidateKey, candidateOnly);
        if (comparisonKey == null) {
            return null;
        }
        boolean reverse = subplans.get(0).plan.isReverse();
        boolean anyDuplicates = false;
        Set<RankComparisons.RankComparison> includedRankComparisons = null;
        List<RecordQueryPlan> childPlans = new ArrayList<>(subplans.size());
        for (ScoredPlan subplan : subplans) {
            if (subplan.plan.isReverse() != reverse) {
                // Cannot mix plans that go opposite directions with the common ordering key.
                return null;
            }
            childPlans.add(subplan.plan);
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

        return new ScoredPlan(unionPlan, Collections.emptyList(), Collections.emptyList(), score, anyDuplicates, includedRankComparisons);
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
            childPlans.add(subplan.plan);
            includedRankComparisons = mergeRankComparisons(includedRankComparisons, subplan.includedRankComparisons);
        }
        final RecordQueryUnorderedUnionPlan unionPlan = RecordQueryUnorderedUnionPlan.from(childPlans);
        if (unionPlan.getComplexity() > configuration.getComplexityThreshold()) {
            throw new RecordQueryPlanComplexityException(unionPlan);
        }
        return new ScoredPlan(unionPlan, Collections.emptyList(), Collections.emptyList(), 1, true, includedRankComparisons);
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
        } else {
            return Key.Expressions.concat(sort, candidateKey);
        }
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
            child = AndComponent.from(conjuncts);
            distributed.add(child);
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
        } else {
            indexExpr = EmptyKeyExpression.EMPTY;
        }
        return planCoveringAggregateIndex(query, index, indexExpr);
    }

    @Nullable
    public RecordQueryCoveringIndexPlan planCoveringAggregateIndex(@Nonnull RecordQuery query, @Nonnull Index index, @Nonnull KeyExpression indexExpr) {
        return planCoveringAggregateIndex(query, index, indexExpr, false);
    }

    @Nullable
    public RecordQueryCoveringIndexPlan planCoveringAggregateIndex(@Nonnull RecordQuery query, @Nonnull Index index,
                                                                   @Nonnull KeyExpression indexExpr, boolean allowRepeated) {
        final Collection<RecordType> recordTypes = metaData.recordTypesForIndex(index);
        if (recordTypes.size() != 1) {
            // Unfortunately, since we materialize partial records, we need a unique type for them.
            return null;
        }
        final RecordType recordType = recordTypes.iterator().next();
        final PlanContext planContext = getPlanContext(query);
        planContext.rankComparisons = new RankComparisons(query.getFilter(), planContext.indexes);
        final CandidateScan candidateScan = new CandidateScan(planContext, index, query.isSortReverse());
        final ScoredPlan scoredPlan = planCandidateScan(candidateScan, indexExpr,
                BooleanNormalizer.forConfiguration(configuration).normalizeIfPossible(query.getFilter()), query.getSort());
        // It would be possible to handle unsatisfiedFilters if they, too, only involved group key (covering) fields.
        if (scoredPlan == null || !scoredPlan.unsatisfiedFilters.isEmpty() || !(scoredPlan.plan instanceof RecordQueryIndexPlan)) {
            return null;
        }

        final IndexKeyValueToPartialRecord.Builder builder = IndexKeyValueToPartialRecord.newBuilder(recordType);
        final List<KeyExpression> keyFields = index.getRootExpression().normalizeKeyForPositions();
        final List<KeyExpression> valueFields = Collections.emptyList();
        for (KeyExpression resultField : query.getRequiredResults()) {
            if (!addCoveringField(resultField, builder, keyFields, valueFields)) {
                return null;
            }
        }
        builder.addRequiredMessageFields();
        if (!builder.isValid(allowRepeated)) {
            return null;
        }

        RecordQueryIndexPlan plan = (RecordQueryIndexPlan)scoredPlan.plan;
        plan = new RecordQueryIndexPlan(plan.getIndexName(), IndexScanType.BY_GROUP, plan.getComparisons(), plan.isReverse());
        return new RecordQueryCoveringIndexPlan(plan, recordType.getName(), AvailableFields.NO_FIELDS, builder.build());
    }

    private static boolean addCoveringField(@Nonnull KeyExpression requiredExpr,
                                            @Nonnull IndexKeyValueToPartialRecord.Builder builder,
                                            @Nonnull List<KeyExpression> keyFields,
                                            @Nonnull List<KeyExpression> valueFields) {
        final IndexKeyValueToPartialRecord.TupleSource source;
        final int index;

        int i = keyFields.indexOf(requiredExpr);
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
        return AvailableFields.addCoveringField(requiredExpr, AvailableFields.FieldData.of(source, index), builder);
    }

    private static class PlanContext {
        @Nonnull
        final RecordQuery query;
        @Nonnull
        final List<Index> indexes;
        @Nullable
        final KeyExpression commonPrimaryKey;
        RankComparisons rankComparisons;

        public PlanContext(@Nonnull RecordQuery query, @Nonnull List<Index> indexes,
                           @Nullable KeyExpression commonPrimaryKey) {
            this.query = query;
            this.indexes = indexes;
            this.commonPrimaryKey = commonPrimaryKey;
        }
    }

    private static class CandidateScan {
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
    }

    protected static class ScoredPlan {
        final int score;
        @Nonnull
        final RecordQueryPlan plan;
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
        final boolean createsDuplicates;
        @Nullable
        final Set<RankComparisons.RankComparison> includedRankComparisons;

        @Nullable
        PlanOrderingKey planOrderingKey;

        public ScoredPlan(int score, @Nonnull RecordQueryPlan plan) {
            this(score, plan, Collections.<QueryComponent>emptyList());
        }

        public ScoredPlan(int score, @Nonnull RecordQueryPlan plan,
                          @Nonnull List<QueryComponent> unsatisfiedFilters) {
            this(score, plan, unsatisfiedFilters, false);
        }

        public ScoredPlan(int score, @Nonnull RecordQueryPlan plan,
                          @Nonnull List<QueryComponent> unsatisfiedFilters, boolean createsDuplicates) {
            this(plan, unsatisfiedFilters, Collections.emptyList(), score, createsDuplicates, null);
        }

        public ScoredPlan(@Nonnull RecordQueryPlan plan, @Nonnull List<QueryComponent> unsatisfiedFilters,
                          @Nonnull final List<QueryComponent> indexFilters, int score, boolean createsDuplicates,
                          @Nullable Set<RankComparisons.RankComparison> includedRankComparisons) {
            this.score = score;
            this.plan = plan;
            this.unsatisfiedFilters = unsatisfiedFilters;
            this.indexFilters = indexFilters;
            this.createsDuplicates = createsDuplicates;
            this.includedRankComparisons = includedRankComparisons;
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

        @Nonnull
        public ScoredPlan withPlan(@Nonnull RecordQueryPlan newPlan) {
            return new ScoredPlan(newPlan, unsatisfiedFilters, indexFilters, score, createsDuplicates, includedRankComparisons);
        }

        @Nonnull
        public ScoredPlan withScore(int newScore) {
            if (newScore == score) {
                return this;
            } else {
                return new ScoredPlan(plan, unsatisfiedFilters, indexFilters, newScore, createsDuplicates, includedRankComparisons);
            }
        }

        @Nonnull
        public ScoredPlan withUnsatisfiedFilters(@Nonnull List<QueryComponent> newFilters) {
            return new ScoredPlan(plan, newFilters, indexFilters, score, createsDuplicates, includedRankComparisons);
        }

        @Nonnull
        public ScoredPlan withFilters(@Nonnull List<QueryComponent> newUnsatisfiedFilters, @Nonnull List<QueryComponent> newIndexFilters) {
            return new ScoredPlan(plan, newUnsatisfiedFilters, newIndexFilters, score, createsDuplicates, includedRankComparisons);
        }

        @Nonnull
        public ScoredPlan withCreatesDuplicates(boolean newCreatesDuplicates) {
            if (createsDuplicates == newCreatesDuplicates) {
                return this;
            } else {
                return new ScoredPlan(plan, unsatisfiedFilters, indexFilters, score, newCreatesDuplicates, includedRankComparisons);
            }
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
    private class AndWithThenPlanner {
        /**
         * The original root expression on the index or {@code null} if the index actually has only a single column.
         */
        @Nullable
        private final ThenKeyExpression index;
        /**
         * The children of the root expression or a single key expression if the index actually has only a single column.
         */
        @Nonnull
        private final List<KeyExpression> indexChildren;
        /**
         * The children of the {@link AndComponent} or a single filter if the query is actually on a single component.
         */
        @Nonnull
        private final List<QueryComponent> filters;
        @Nullable
        private KeyExpression sort;
        @Nonnull
        private final CandidateScan candidateScan;
        /**
         * The set of filters in the and that have not been satisfied (yet).
         */
        @Nonnull
        private List<QueryComponent> unsatisfiedFilters;
        /**
         * If the sort is also a then, this iterates over its children.
         */
        @Nullable
        private Iterator<KeyExpression> sortIterator;
        /**
         * The current sort child, or the sort itself, or {@code null} if the sort has been satisfied.
         */
        @Nullable
        private KeyExpression currentSort;
        /**
         * True if the current child of the index {@link ThenKeyExpression Then} clause has a corresponding equality comparison in the filter.
         */
        private boolean foundComparison;
        /**
         * Accumulate matching comparisons here.
         */
        @Nonnull
        private ScanComparisons.Builder comparisons;

        @SpotBugsSuppressWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", justification = "maybe https://github.com/spotbugs/spotbugs/issues/616?")
        public AndWithThenPlanner(@Nonnull CandidateScan candidateScan,
                                  @Nonnull ThenKeyExpression index,
                                  @Nonnull AndComponent filter,
                                  @Nullable KeyExpression sort) {
            this(candidateScan, index, filter.getChildren(), sort);
        }

        @SpotBugsSuppressWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", justification = "maybe https://github.com/spotbugs/spotbugs/issues/616?")
        public AndWithThenPlanner(@Nonnull CandidateScan candidateScan,
                                  @Nonnull ThenKeyExpression index,
                                  @Nonnull List<QueryComponent> filters,
                                  @Nullable KeyExpression sort) {
            this (candidateScan, index, index.getChildren(), filters, sort);
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
                                   @Nullable ThenKeyExpression index,
                                   @Nonnull List<KeyExpression> indexChildren,
                                   @Nonnull List<QueryComponent> filters,
                                   @Nullable KeyExpression sort) {
            this.index = index;
            this.indexChildren = indexChildren;
            this.filters = filters;
            this.sort = sort;
            this.candidateScan = candidateScan;

            unsatisfiedFilters = new ArrayList<>();
            comparisons = new ScanComparisons.Builder();
        }

        public ScoredPlan plan() {
            setupPlanState();
            boolean doneComparing = false;
            for (KeyExpression child : indexChildren) {
                if (!doneComparing) {
                    planChild(child);
                    if (!comparisons.isEquality() || !foundComparison) {
                        // Didn't add another equality; done matching filters to index.
                        doneComparing = true;
                    }
                }
                if (doneComparing) {
                    if (currentSort == null) {
                        break;
                    }
                    // With inequalities or no filters, index ordering must match sort ordering.
                    if (currentSortMatches(child)) {
                        advanceCurrentSort();
                    } else {
                        break;
                    }
                }
            }
            if (currentSort != null) {
                return null;
            }
            if (comparisons.isEmpty()) {
                return null;
            }
            boolean createsDuplicates = false;
            if (candidateScan.index != null) {
                createsDuplicates = candidateScan.index.getRootExpression().createsDuplicates();
                if (createsDuplicates && index != null && index.createsDuplicatesAfter(comparisons.size())) {
                    // If fields after we stopped comparing create duplicates, they might be empty, so that a record
                    // that otherwise matches the comparisons would be absent from the index entirely.
                    return null;
                }
            }
            return new ScoredPlan(comparisons.totalSize(), planScan(candidateScan, comparisons.build()), unsatisfiedFilters, createsDuplicates);
        }

        private void setupPlanState() {
            unsatisfiedFilters = new ArrayList<>(filters);
            comparisons = new ScanComparisons.Builder();
            KeyExpression sortKey = sort;
            if (sortKey instanceof GroupingKeyExpression) {
                sortKey = ((GroupingKeyExpression) sortKey).getWholeKey();
            }
            if (sortKey instanceof ThenKeyExpression) {
                ThenKeyExpression sortThen = (ThenKeyExpression) sortKey;
                sortIterator = sortThen.getChildren().iterator();
                currentSort = sortIterator.next();
            } else {
                currentSort = sortKey;
                sortIterator = null;
            }
        }

        private void planChild(@Nonnull KeyExpression child) {
            foundComparison = false;
            if (child instanceof RecordTypeKeyExpression) {
                if (candidateScan.planContext.query.getRecordTypes().size() == 1) {
                    // Can scan just the one requested record type.
                    final RecordTypeKeyComparison recordTypeKeyComparison = new RecordTypeKeyComparison(candidateScan.planContext.query.getRecordTypes().iterator().next());
                    addToComparisons(recordTypeKeyComparison.getComparison());
                }
                return;
            }
            for (QueryComponent filterChild : filters) {
                QueryComponent filterComponent = candidateScan.planContext.rankComparisons.planComparisonSubstitute(filterChild);
                if (filterComponent instanceof FieldWithComparison) {
                    planWithComparisonChild(child, (FieldWithComparison) filterComponent, filterChild);
                } else if (filterComponent instanceof NestedField) {
                    planNestedFieldChild(child, (NestedField) filterComponent, filterChild);
                } else if (filterComponent instanceof OneOfThemWithComparison) {
                    planOneOfThemWithComparisonChild(child, (OneOfThemWithComparison) filterComponent, filterChild);
                } else if (filterComponent instanceof QueryRecordFunctionWithComparison
                           && ((QueryRecordFunctionWithComparison) filterComponent).getFunction().getName().equals(FunctionNames.VERSION)) {
                    planWithVersionComparisonChild(child, (QueryRecordFunctionWithComparison) filterComponent, filterChild);
                } else if (filterComponent instanceof QueryKeyExpressionWithComparison) {
                    planWithComparisonChild(child, (QueryKeyExpressionWithComparison) filterComponent, filterChild);
                }
                if (foundComparison) {
                    break;
                }
            }
        }

        private void planNestedFieldChild(@Nonnull KeyExpression child, @Nonnull NestedField filterField, @Nonnull QueryComponent filterChild) {
            ScoredPlan scoredPlan = planNestedField(candidateScan, child, filterField, null);
            ScanComparisons nextComparisons = getPlanComparisons(scoredPlan);
            if (nextComparisons != null) {
                if (!comparisons.isEquality() && nextComparisons.getEqualitySize() > 0) {
                    throw new Query.InvalidExpressionException(
                            "Two nested fields in the same and clause, combine them into one");
                } else {
                    if (nextComparisons.isEquality()) {
                        // Equality comparisons might match required sort.
                        if (currentSortMatches(child)) {
                            advanceCurrentSort();
                        }
                    } else if (currentSort != null) {
                        // Didn't plan to equality, need to try with sorting.
                        scoredPlan = planNestedField(candidateScan, child, filterField, currentSort);
                    }
                    if (scoredPlan != null) {
                        unsatisfiedFilters.remove(filterChild);
                        unsatisfiedFilters.addAll(scoredPlan.unsatisfiedFilters);
                        comparisons.addAll(nextComparisons);
                        if (nextComparisons.isEquality()) {
                            foundComparison = true;
                        }
                    }
                }
            }
        }

        private boolean currentSortMatches(@Nonnull KeyExpression child) {
            if (currentSort != null) {
                if (currentSort.equals(child)) {
                    return true;
                }
            }
            return false;
        }

        private void advanceCurrentSort() {
            if (sortIterator != null && sortIterator.hasNext()) {
                currentSort = sortIterator.next();
            } else {
                currentSort = null;
            }
        }

        private void planWithComparisonChild(@Nonnull KeyExpression child, @Nonnull FieldWithComparison field, @Nonnull QueryComponent filterChild) {
            if (child instanceof FieldKeyExpression) {
                FieldKeyExpression indexField = (FieldKeyExpression) child;
                if (Objects.equals(field.getFieldName(), indexField.getFieldName())) {
                    if (addToComparisons(field.getComparison())) {
                        unsatisfiedFilters.remove(filterChild);
                        if (foundComparison && currentSortMatches(child)) {
                            advanceCurrentSort();
                        }
                    }
                }
            }
        }

        private void planWithComparisonChild(@Nonnull KeyExpression child, @Nonnull QueryKeyExpressionWithComparison queryKeyExpression, @Nonnull QueryComponent filterChild) {
            if (child.equals(queryKeyExpression.getKeyExpression())) {
                if (addToComparisons(queryKeyExpression.getComparison())) {
                    unsatisfiedFilters.remove(filterChild);
                    if (foundComparison && currentSortMatches(child)) {
                        advanceCurrentSort();
                    }
                }
            }
        }

        private void planOneOfThemWithComparisonChild(@Nonnull KeyExpression child, @Nonnull OneOfThemWithComparison oneOfThem, @Nonnull QueryComponent filterChild) {
            if (child instanceof FieldKeyExpression) {
                FieldKeyExpression indexField = (FieldKeyExpression) child;
                if (Objects.equals(oneOfThem.getFieldName(), indexField.getFieldName()) && indexField.getFanType() == FanType.FanOut) {
                    if (addToComparisons(oneOfThem.getComparison())) {
                        unsatisfiedFilters.remove(filterChild);
                        if (foundComparison && currentSortMatches(child)) {
                            advanceCurrentSort();
                        }
                    }
                }
            }
        }

        private void planWithVersionComparisonChild(@Nonnull KeyExpression child, @Nonnull QueryRecordFunctionWithComparison filter, @Nonnull QueryComponent filterChild) {
            if (child instanceof VersionKeyExpression) {
                if (addToComparisons(filter.getComparison())) {
                    unsatisfiedFilters.remove(filterChild);
                    if (foundComparison && currentSortMatches(child)) {
                        advanceCurrentSort();
                    }
                }
            }
        }

        private boolean addToComparisons(@Nonnull Comparisons.Comparison comparison) {
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

    }

}
