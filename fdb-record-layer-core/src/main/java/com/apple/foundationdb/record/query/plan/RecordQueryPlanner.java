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
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
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
import com.google.common.annotations.VisibleForTesting;

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
    /**
     * A limit on the complexity of the plans generated by the planner.
     * If the planner generates a query plan that exceeds this complexity, an exception will be thrown.
     * See <code>RecordQueryPlan.getComplexity()</code> for a description of plan complexity.
     */
    @VisibleForTesting
    public static final int DEFAULT_COMPLEXITY_THRESHOLD = 3000;
    private final int complexityThreshold;

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
    private IndexScanPreference indexScanPreference;

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
        this.complexityThreshold = complexityThreshold;

        primaryKeyHasRecordTypePrefix = metaData.primaryKeyHasRecordTypePrefix();
        // If we are going to need type filters on Scan, index is safer without knowing any cardinalities.
        indexScanPreference = metaData.getRecordTypes().size() > 1 && !primaryKeyHasRecordTypePrefix ?
                              IndexScanPreference.PREFER_INDEX : IndexScanPreference.PREFER_SCAN;
    }

    /**
     * Get whether {@link RecordQueryIndexPlan} is preferred over {@link RecordQueryScanPlan} even when it does not
     * satisfy any additional conditions.
     * @return whether to prefer index scan over record scan
     */
    @Nonnull
    public IndexScanPreference getIndexScanPreference() {
        return indexScanPreference;
    }

    /**
     * Set whether {@link RecordQueryIndexPlan} is preferred over {@link RecordQueryScanPlan} even when it does not
     * satisfy any additional conditions.
     * Scanning without an index is more efficient, but will have to skip over unrelated record types.
     * For that reason, it is safer to use an index, except when there is only one record type.
     * If the meta-data has more than one record type but the record store does not, this can be overridden.
     * @param indexScanPreference whether to prefer index scan over record scan
     */
    @Override
    public void setIndexScanPreference(@Nonnull IndexScanPreference indexScanPreference) {
        this.indexScanPreference = indexScanPreference;
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

        final BooleanNormalizer normalizer = BooleanNormalizer.withLimit(complexityThreshold);
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
        if (query.getRequiredResults() != null) {
            plan = tryToConvertToCoveringPlan(planContext, plan);
        }

        if (timer != null) {
            plan.logPlanStructure(timer);
        }

        if (plan.getComplexity() > complexityThreshold) {
            throw new RecordQueryPlanComplexityException(plan);
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
        switch (getIndexScanPreference()) {
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

    private int indexSizeOverhead(PlanContext planContext, @Nonnull Index index) {
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
        if (planContext.query.getSort() != null) {
            inExtractor.setSort(planContext.query.getSort(), planContext.query.isSortReverse());
        } else if (needOrdering) {
            inExtractor.sortByClauses();
        }
        filter = inExtractor.subFilter();
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
                if (bestPlan == null || p.score > bestPlan.score ||
                        (p.score == bestPlan.score && compareIndexes(planContext, index, bestIndex) > 0)) {
                    bestPlan = p;
                    bestIndex = index;
                }
            }
        }
        if (bestPlan != null) {
            if (!bestPlan.unsatisfiedFilters.isEmpty()) {
                bestPlan = handleUnsatisfiedFilters(bestPlan, intersectionCandidates, planContext);
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
            p = planRemoveDuplicates(planContext, p);
            if (p != null && !p.unsatisfiedFilters.isEmpty()) {
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
                commonPrimaryKey = commonPrimaryKey(metaData.getRecordTypes().values());
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
                    commonPrimaryKey = commonPrimaryKey(recordTypes);
                }
            }

            indexes.addAll(readableOf(metaData.getUniversalIndexes()));
        } finally {
            recordStoreState.endRead();
        }

        indexes.removeIf(query.hasAllowedIndexes() ?
                index -> !query.getAllowedIndexes().contains(index.getName()) :
                index -> !index.getBooleanOption(IndexOptions.ALLOWED_FOR_QUERY_OPTION, true));

        return new PlanContext(query, indexes, commonPrimaryKey);
    }

    @Nullable
    private static KeyExpression commonPrimaryKey(@Nonnull Collection<RecordType> recordTypes) {
        KeyExpression common = null;
        boolean first = true;
        for (RecordType recordType : recordTypes) {
            if (first) {
                common = recordType.getPrimaryKey();
                first = false;
            } else if (!common.equals(recordType.getPrimaryKey())) {
                return null;
            }
        }
        return common;
    }

    @Nullable
    private ScoredPlan planRemoveDuplicates(@Nonnull PlanContext planContext, @Nonnull ScoredPlan plan) {
        if (plan.createsDuplicates && planContext.query.removesDuplicates()) {
            if (planContext.commonPrimaryKey == null) {
                return null;
            }
            return new ScoredPlan(plan.score, new RecordQueryUnorderedPrimaryKeyDistinctPlan(plan.plan),
                    plan.unsatisfiedFilters, false, plan.includedRankComparisons);
        } else {
            return plan;
        }
    }

    @Nonnull
    private ScoredPlan handleUnsatisfiedFilters(@Nonnull ScoredPlan bestPlan,
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
                } else if (bestPlan.unsatisfiedFilters.size() > intersectionPlan.unsatisfiedFilters.size()) {
                    bestPlan = intersectionPlan;
                }
            }
        }
        final RecordQueryPlan filtered = new RecordQueryFilterPlan(bestPlan.plan,
                planContext.rankComparisons.planComparisonSubsitutes(bestPlan.unsatisfiedFilters));
        // TODO: further optimization requires knowing which filters are satisfied
        return new ScoredPlan(bestPlan.score, filtered, Collections.emptyList(),
                bestPlan.createsDuplicates, bestPlan.includedRankComparisons);
    }

    @Nullable
    private ScoredPlan planIntersection(@Nonnull List<ScoredPlan> intersectionCandidates,
                                        @Nonnull KeyExpression comparisonKey) {
        // Prefer plans that handle more filters (leave fewer unhandled).
        intersectionCandidates.sort(Comparator.comparingInt(p -> p.unsatisfiedFilters.size()));
        // Since we limited to isPrimaryKeyOrdered(), comparisonKey will always work.
        ScoredPlan plan1 = intersectionCandidates.get(0);
        List<QueryComponent> unsatisfiedFilters = new ArrayList<>(plan1.unsatisfiedFilters);
        Set<RankComparisons.RankComparison> includedRankComparisons =
                mergeRankComparisons(null, plan1.includedRankComparisons);
        RecordQueryPlan plan = plan1.plan;
        List<RecordQueryPlan> includedPlans = new ArrayList<>(intersectionCandidates.size());
        includedPlans.add(plan);
        // TODO optimize so that we don't do excessive intersections
        for (int i = 1; i < intersectionCandidates.size(); i++) {
            ScoredPlan nextPlan = intersectionCandidates.get(i);
            List<QueryComponent> nextUnsatisfiedFilters = new ArrayList<>(nextPlan.unsatisfiedFilters);
            int oldCount = unsatisfiedFilters.size();
            unsatisfiedFilters.retainAll(nextUnsatisfiedFilters);
            if (unsatisfiedFilters.size() < oldCount) {
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
            final RecordQueryPlan intersectionPlan = new RecordQueryIntersectionPlan(includedPlans, comparisonKey, plan.isReverse());
            if (intersectionPlan.getComplexity() > complexityThreshold) {
                throw new RecordQueryPlanComplexityException(intersectionPlan);
            }
            return new ScoredPlan(plan1.score, intersectionPlan, unsatisfiedFilters, plan1.createsDuplicates, includedRankComparisons);
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
            return getPlanComparisons(((RecordQueryTypeFilterPlan) plan).getInner());
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
        } else if (!(index instanceof FieldKeyExpression || index instanceof VersionKeyExpression)) {
            return null;
        }

        List<QueryComponent> unsatisfiedFilters = new ArrayList<>(filter.getChildren());
        for (QueryComponent filterChild : filter.getChildren()) {
            QueryComponent filterComponent = candidateScan.planContext.rankComparisons.planComparisonSubstitute(filterChild);
            ScoredPlan plan = null;
            if (index instanceof FieldKeyExpression && filterComponent instanceof FieldWithComparison) {
                FieldWithComparison fieldWithComparison = (FieldWithComparison)filterComponent;
                plan = planFieldWithComparison(candidateScan, index,
                        fieldWithComparison, sort);
                if (plan != null) {
                    unsatisfiedFilters.remove(filterChild);
                    return plan.withUnsatisfiedFilters(unsatisfiedFilters);
                }
            } else if (index instanceof VersionKeyExpression &&
                       filterComponent instanceof QueryRecordFunctionWithComparison
                       && ((QueryRecordFunctionWithComparison)filterComponent).getFunction().getName().equals(FunctionNames.VERSION)) {
                QueryRecordFunctionWithComparison functionComparison = (QueryRecordFunctionWithComparison)filterComponent;
                plan = planVersion(candidateScan, index, functionComparison, sort);
            } else if (filterComponent instanceof QueryKeyExpressionWithComparison) {
                plan = planQueryKeyExpressionWithComparison(candidateScan, index, (QueryKeyExpressionWithComparison)filterComponent, sort);
                if (plan != null) {
                    unsatisfiedFilters.remove(filterChild);
                    return plan.withUnsatisfiedFilters(unsatisfiedFilters);
                }
            }

            if (plan != null) {
                unsatisfiedFilters.remove(filterChild);
                return plan.withUnsatisfiedFilters(unsatisfiedFilters);
            }
        }
        return null;
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
                return new ScoredPlan(1, scan, Collections.emptyList(), createsDuplicates, Collections.singleton(rankComparison));
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
                    return new ScoredPlan(indexExpr.getColumnSize(), scan, unsatisfiedFilters, createsDuplicates, includedRankComparisons);
                }
            }
        }
        return null;
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
        return new ScoredPlan(10, plan, filterMask.getUnsatisfiedFilters(), scan.createsDuplicates(), null);
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
            plan = new RecordQueryScanPlan(scanComparisons, candidateScan.reverse);
            if (primaryKeyHasRecordTypePrefix && RecordTypeKeyComparison.hasRecordTypeKeyComparison(scanComparisons)) {
                possibleTypes = RecordTypeKeyComparison.recordTypeKeyComparisonTypes(scanComparisons);
            } else {
                possibleTypes = metaData.getRecordTypes().keySet();
            }
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
        for (QueryComponent subfilter : filter.getChildren()) {
            ScoredPlan subplan = planFilter(planContext, subfilter, true);
            if (subplan == null) {
                return null;
            }
            if (subplan.planOrderingKey == null) {
                allHaveOrderingKey = false;
            }
            subplans.add(subplan);
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
        final RecordQueryPlan unionPlan = new RecordQueryUnionPlan(childPlans, comparisonKey, reverse, showComparisonKey);
        if (unionPlan.getComplexity() > complexityThreshold) {
            throw new RecordQueryPlanComplexityException(unionPlan);
        }
        return new ScoredPlan(1, unionPlan, Collections.emptyList(), anyDuplicates, includedRankComparisons);
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
        final RecordQueryUnorderedUnionPlan unionPlan = new RecordQueryUnorderedUnionPlan(childPlans, subplans.get(0).plan.isReverse());
        if (unionPlan.getComplexity() > complexityThreshold) {
            throw new RecordQueryPlanComplexityException(unionPlan);
        }
        return new ScoredPlan(1, unionPlan, Collections.emptyList(), true, includedRankComparisons);
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
                return OrComponent.from(distributeAnd(child2, ((OrComponent)child1).getChildren()));
            }
            if (child2 instanceof OrComponent && Query.isSingleFieldComparison(child1)) {
                return OrComponent.from(distributeAnd(child1, ((OrComponent)child2).getChildren()));
            }
        }
        return and;
    }

    private List<QueryComponent> distributeAnd(QueryComponent component, List<QueryComponent> children) {
        List<QueryComponent> distributed = new ArrayList<>();
        for (QueryComponent child : children) {
            List<QueryComponent> conjuncts = new ArrayList<>(2);
            conjuncts.add(component);
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
    private RecordQueryPlan tryToConvertToCoveringPlan(@Nonnull PlanContext planContext, @Nonnull RecordQueryPlan chosenPlan) {
        if (chosenPlan instanceof RecordQueryPlanWithIndex) {
            // Check if the index scan covers, then convert it to a covering plan.
            return tryToConvertToCoveringPlan(planContext, (RecordQueryPlanWithIndex) chosenPlan);
        } else if (chosenPlan instanceof RecordQueryUnorderedPrimaryKeyDistinctPlan) {
            // If possible, push down the covering index transformation so that
            // it happens before checking for distinct primary keys
            final RecordQueryUnorderedPrimaryKeyDistinctPlan distinctPlan = (RecordQueryUnorderedPrimaryKeyDistinctPlan) chosenPlan;
            if (distinctPlan.getChild() instanceof RecordQueryPlanWithIndex) {
                final RecordQueryPlan newChildPlan = tryToConvertToCoveringPlan(planContext, (RecordQueryPlanWithIndex) distinctPlan.getChild());
                if (newChildPlan != distinctPlan.getChild()) {
                    return new RecordQueryUnorderedPrimaryKeyDistinctPlan(newChildPlan);
                }
            }
        }
        // No valid transformations could be applied. Just return the original plan.
        return chosenPlan;
    }

    @Nonnull
    private RecordQueryPlan tryToConvertToCoveringPlan(@Nonnull PlanContext context, @Nonnull RecordQueryPlanWithIndex chosenPlan) {
        if (context.query.getRequiredResults() == null) {
            // This should already be true when calling, but as a safety precaution, check here anyway.
            return chosenPlan;
        }
        final Index index = metaData.getIndex(chosenPlan.getIndexName());
        Collection<RecordType> recordTypes = metaData.recordTypesForIndex(index);
        if (recordTypes.size() != 1) {
            return chosenPlan;
        }
        final RecordType recordType = recordTypes.iterator().next();
        final List<KeyExpression> resultFields = new ArrayList<>(context.query.getRequiredResults().size());
        for (KeyExpression resultField : context.query.getRequiredResults()) {
            resultFields.addAll(resultField.normalizeKeyForPositions());
        }
        final KeyExpression rootExpression = index.getRootExpression();
        final List<KeyExpression> normalizedKeys = rootExpression.normalizeKeyForPositions();
        final List<KeyExpression> keyFields;
        final List<KeyExpression> valueFields;
        if (rootExpression instanceof KeyWithValueExpression) {
            final KeyWithValueExpression keyWithValue = (KeyWithValueExpression) rootExpression;
            keyFields = new ArrayList<>(normalizedKeys.subList(0, keyWithValue.getSplitPoint()));
            valueFields = new ArrayList<>(normalizedKeys.subList(keyWithValue.getSplitPoint(), normalizedKeys.size()));
        } else {
            keyFields = new ArrayList<>(normalizedKeys);
            valueFields = Collections.singletonList(EmptyKeyExpression.EMPTY);
        }

        // Like FDBRecordStoreBase.indexEntryKey(), but with key expressions instead of actual values.
        final List<KeyExpression> primaryKeys = context.commonPrimaryKey == null
                ? Collections.emptyList()
                : context.commonPrimaryKey.normalizeKeyForPositions();
        index.trimPrimaryKey(primaryKeys);
        keyFields.addAll(primaryKeys);

        final IndexKeyValueToPartialRecord.Builder builder = IndexKeyValueToPartialRecord.newBuilder(recordType.getDescriptor());

        for (KeyExpression resultField : resultFields) {
            if (!addCoveringField(resultField, builder, keyFields, valueFields)) {
                return chosenPlan;
            }
        }

        if (context.commonPrimaryKey != null) {
            for (KeyExpression primaryKeyField : context.commonPrimaryKey.normalizeKeyForPositions()) {
                // Need the primary key, even if it wasn't one of the explicit result fields.
                if (!resultFields.contains(primaryKeyField)) {
                    addCoveringField(primaryKeyField, builder, keyFields, valueFields);
                }
            }
        }

        if (!builder.isValid()) {
            return chosenPlan;
        }

        return new RecordQueryCoveringIndexPlan(chosenPlan, recordType.getName(), builder.build());
    }

    @Nullable
    private boolean addCoveringField(@Nonnull KeyExpression requiredExpr,
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

        while (requiredExpr instanceof NestingKeyExpression) {
            NestingKeyExpression nesting = (NestingKeyExpression)requiredExpr;
            String fieldName = nesting.getParent().getFieldName();
            requiredExpr = nesting.getChild();
            builder = builder.getFieldBuilder(fieldName);
        }
        if (requiredExpr instanceof FieldKeyExpression) {
            String fieldName = ((FieldKeyExpression)requiredExpr).getFieldName();
            builder.addField(fieldName, source, index);
            return true;
        } else {
            return false;
        }
    }
    
    @Nullable
    public RecordQueryPlan planCoveringAggregateIndex(@Nonnull RecordQuery query, @Nonnull String indexName) {
        final Index index = metaData.getIndex(indexName);
        final Collection<RecordType> recordTypes = metaData.recordTypesForIndex(index);
        if (recordTypes.size() != 1) {
            // Unfortunately, since we materialize partial records, we need a unique type for them.
            return null;
        }
        final RecordType recordType = recordTypes.iterator().next();
        final PlanContext planContext = getPlanContext(query);
        planContext.rankComparisons = new RankComparisons(query.getFilter(), planContext.indexes);
        final CandidateScan candidateScan = new CandidateScan(planContext, index, query.isSortReverse());
        KeyExpression indexExpr = index.getRootExpression();
        if (indexExpr instanceof GroupingKeyExpression) {
            indexExpr = ((GroupingKeyExpression)indexExpr).getGroupingSubKey();
        } else {
            indexExpr = EmptyKeyExpression.EMPTY;
        }
        final ScoredPlan scoredPlan = planCandidateScan(candidateScan, indexExpr,
                BooleanNormalizer.withLimit(complexityThreshold).normalizeIfPossible(query.getFilter()), query.getSort());
        // It would be possible to handle unsatisfiedFilters if they, too, only involved group key (covering) fields.
        if (scoredPlan == null || !scoredPlan.unsatisfiedFilters.isEmpty() || !(scoredPlan.plan instanceof RecordQueryIndexPlan)) {
            return null;
        }

        final IndexKeyValueToPartialRecord.Builder builder = IndexKeyValueToPartialRecord.newBuilder(recordType.getDescriptor());
        final List<KeyExpression> keyFields = index.getRootExpression().normalizeKeyForPositions();
        final List<KeyExpression> valueFields = Collections.emptyList();
        for (KeyExpression resultField : query.getRequiredResults()) {
            if (!addCoveringField(resultField, builder, keyFields, valueFields)) {
                return null;
            }
        }
        builder.addRequiredMessageFields();
        if (!builder.isValid()) {
            return null;
        }

        RecordQueryIndexPlan plan = (RecordQueryIndexPlan)scoredPlan.plan;
        plan = new RecordQueryIndexPlan(plan.getIndexName(), IndexScanType.BY_GROUP, plan.getComparisons(), plan.isReverse());
        return new RecordQueryCoveringIndexPlan(plan, recordType.getName(), builder.build());
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
            this(score, plan, unsatisfiedFilters, createsDuplicates, null);
        }

        public ScoredPlan(int score, @Nonnull RecordQueryPlan plan,
                          @Nonnull List<QueryComponent> unsatisfiedFilters, boolean createsDuplicates,
                          @Nullable Set<RankComparisons.RankComparison> includedRankComparisons) {
            this.score = score;
            this.plan = plan;
            this.unsatisfiedFilters = unsatisfiedFilters;
            this.createsDuplicates = createsDuplicates;
            this.includedRankComparisons = includedRankComparisons;
        }

        @Nonnull
        public ScoredPlan withPlan(@Nonnull RecordQueryPlan newPlan) {
            return new ScoredPlan(score, newPlan, unsatisfiedFilters, createsDuplicates, includedRankComparisons);
        }

        @Nonnull
        public ScoredPlan withScore(int newScore) {
            if (newScore == score) {
                return this;
            } else {
                return new ScoredPlan(newScore, plan, unsatisfiedFilters, createsDuplicates, includedRankComparisons);
            }
        }

        @Nonnull
        public ScoredPlan withUnsatisfiedFilters(@Nonnull List<QueryComponent> newFilters) {
            return new ScoredPlan(score, plan, newFilters, createsDuplicates, includedRankComparisons);
        }

        @Nonnull
        public ScoredPlan withCreatesDuplicates(boolean newCreatesDuplicates) {
            if (createsDuplicates == newCreatesDuplicates) {
                return this;
            } else {
                return new ScoredPlan(score, plan, unsatisfiedFilters, newCreatesDuplicates, includedRankComparisons);
            }
        }
    }

    private class AndWithThenPlanner {
        @Nonnull
        private final ThenKeyExpression index;
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
            this.index = index;
            this.filters = filters;
            this.sort = sort;
            this.candidateScan = candidateScan;

            unsatisfiedFilters = new ArrayList<>();
            comparisons = new ScanComparisons.Builder();
        }

        public ScoredPlan plan() {
            setupPlanState();
            boolean doneComparing = false;
            for (KeyExpression child : index.getChildren()) {
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
                if (createsDuplicates && index.createsDuplicatesAfter(comparisons.size())) {
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
