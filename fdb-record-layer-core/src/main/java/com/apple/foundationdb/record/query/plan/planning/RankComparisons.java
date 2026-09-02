/*
 * RankComparisons.java
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

package com.apple.foundationdb.record.query.plan.planning;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.Bindings;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordFunction;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.FieldKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.query.expressions.AndComponent;
import com.apple.foundationdb.record.query.expressions.AndOrComponent;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.ComponentWithChildren;
import com.apple.foundationdb.record.query.expressions.FieldWithComparison;
import com.apple.foundationdb.record.query.expressions.NestedField;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.expressions.QueryRecordFunctionWithComparison;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryScoreForRankPlan;

import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Track comparisons involving rank-type functions.
 */
@API(API.Status.INTERNAL)
public class RankComparisons {
    private final Map<QueryRecordFunctionWithComparison, RankComparison> comparisons = new HashMap<>();

    public RankComparisons(@Nullable QueryComponent filter, List<Index> indexes) {
        final List<QueryComponent> groupingFilters;
        if (filter instanceof AndComponent) {
            groupingFilters = ((AndComponent)filter).getChildren();
        } else {
            groupingFilters = Collections.emptyList();
        }
        findComparisons(filter, indexes, groupingFilters, new AtomicInteger());
    }

    @Nullable
    public RankComparison getPlanComparison(QueryRecordFunctionWithComparison comparison) {
        return comparisons.get(comparison);
    }

    public QueryComponent planComparisonSubstitute(QueryComponent component) {
        if (component instanceof QueryRecordFunctionWithComparison) {
            final RankComparison rankComparison = getPlanComparison((QueryRecordFunctionWithComparison)component);
            if (rankComparison != null) {
                final QueryComponent substitute = rankComparison.getSubstitute();
                if (substitute != null) {
                    return substitute;
                }
            }
        }
        return component;
    }

    @Nullable
    public List<QueryComponent> planComparisonSubstitutes(@Nullable List<QueryComponent> components) {
        if (components == null || comparisons.isEmpty()) {
            return components;
        }
        return components.stream().map(this::planComparisonSubstitute).collect(Collectors.toList());
    }

    public RecordQueryPlan wrap(RecordQueryPlan plan, @Nullable Set<RankComparison> includedRankComparisons,
                                RecordMetaData metaData) {
        if (comparisons.isEmpty()) {
            return plan;
        }
        List<RecordQueryScoreForRankPlan.ScoreForRank> ranks = new ArrayList<>(comparisons.size());
        for (RankComparison rankComparison : comparisons.values()) {
            if (includedRankComparisons != null && includedRankComparisons.contains(rankComparison)) {
                continue;
            }
            ranks.add(rankComparison.getScoreForRank(metaData));
        }
        if (!ranks.isEmpty()) {
            // Make exact plan deterministic.
            Collections.sort(ranks, Comparator.comparing(RecordQueryScoreForRankPlan.ScoreForRank::getBindingName));
            plan = new RecordQueryScoreForRankPlan(plan, ranks);
        }
        return plan;
    }

    public static String scoreForRankFunction(QueryRecordFunctionWithComparison comparison) {
        final String rankFunction = comparison.getFunction().getName();
        final Comparisons.Type comparisonType = comparison.getComparison().getType();
        final boolean isRightRange = comparisonType == Comparisons.Type.LESS_THAN || comparisonType == Comparisons.Type.LESS_THAN_OR_EQUALS;
        if (FunctionNames.RANK.equals(rankFunction)) {
            return isRightRange ? FunctionNames.SCORE_FOR_RANK_ELSE_SKIP : FunctionNames.SCORE_FOR_RANK;
        } else if (FunctionNames.TIME_WINDOW_RANK.equals(rankFunction)) {
            return isRightRange ? FunctionNames.SCORE_FOR_TIME_WINDOW_RANK_ELSE_SKIP : FunctionNames.SCORE_FOR_TIME_WINDOW_RANK;
        } else {
            throw new RecordCoreException("Unknown rank function: " + rankFunction);
        }
    }

    public static boolean matchesSort(GroupingKeyExpression indexExpr, @Nullable KeyExpression sort) {
        if (sort == null) {
            return true;
        }
        final KeyExpression grouped = indexExpr.getGroupedSubKey();
        return sort.equals(grouped);
    }

    public static boolean createsDuplicates(Index index, GroupingKeyExpression indexExpr) {
        // A time window leaderboard index takes the best score for the record among repeated fields.
        return !IndexTypes.TIME_WINDOW_LEADERBOARD.equals(index.getType()) && indexExpr.createsDuplicates();
    }

    private void findComparisons(@Nullable QueryComponent filter,
                                 List<Index> indexes, List<QueryComponent> groupingFilters,
                                 AtomicInteger counter) {
        if (filter instanceof AndOrComponent) {
            for (QueryComponent child : ((ComponentWithChildren)filter).getChildren()) {
                findComparisons(child, indexes, groupingFilters, counter);
            }
        } else if (filter instanceof QueryRecordFunctionWithComparison) {
            findComparison((QueryRecordFunctionWithComparison)filter, indexes, groupingFilters, counter);
        }
    }

    private void findComparison(QueryRecordFunctionWithComparison comparison,
                                List<Index> indexes, List<QueryComponent> potentialGroupFilters,
                                AtomicInteger counter) {
        RecordFunction<?> recordFunction = comparison.getFunction();
        // TODO: Should share with indexMaintainerForAggregateFunction
        // TODO: Move index-specific query planning behavior outside of planner (https://github.com/FoundationDB/fdb-record-layer/issues/17)
        List<String> requiredIndexTypes;
        if (FunctionNames.RANK.equals(recordFunction.getName())) {
            requiredIndexTypes = Arrays.asList(IndexTypes.RANK, IndexTypes.TIME_WINDOW_LEADERBOARD);
        } else if (FunctionNames.TIME_WINDOW_RANK.equals(recordFunction.getName())) {
            requiredIndexTypes = Collections.singletonList(IndexTypes.TIME_WINDOW_LEADERBOARD);
        } else {
            requiredIndexTypes = null;
        }
        if (requiredIndexTypes != null) {
            final GroupingKeyExpression operand = ((IndexRecordFunction) recordFunction).getOperand();
            Optional<Index> matchingIndex = indexes.stream()
                    .filter(index -> requiredIndexTypes.contains(index.getType()) && index.getRootExpression().equals(operand))
                    .min(Comparator.comparing(Index::getColumnSize));
            if (matchingIndex.isPresent()) {
                final KeyExpression groupBy = operand.getGroupingSubKey();
                final List<QueryComponent> groupFilters = new ArrayList<>();
                final List<Comparisons.Comparison> groupComparisons = new ArrayList<>();
                if (!GroupingValidator.findGroupKeyFilters(potentialGroupFilters, groupBy, groupFilters, groupComparisons)) {
                    return;
                }
                QueryComponent substitute = null;
                String bindingName = null;
                final Comparisons.Type comparisonType = comparison.getComparison().getType();
                if (!operand.createsDuplicates() && !comparisonType.isUnary()) {
                    bindingName = Bindings.Internal.RANK.bindingName(Integer.toString(counter.getAndIncrement()));
                    Comparisons.Comparison substituteComparison = new Comparisons.ParameterComparison(comparisonType, bindingName, Bindings.Internal.RANK);
                    final KeyExpression grouped = operand.getGroupedSubKey();
                    if (grouped instanceof FieldKeyExpression) {
                        substitute = new FieldWithComparison(((FieldKeyExpression)grouped).getFieldName(), substituteComparison);
                    } else if (grouped instanceof NestingKeyExpression) {
                        NestingKeyExpression nesting = (NestingKeyExpression)grouped ;
                        if (nesting.getChild() instanceof FieldKeyExpression) {
                            substitute = new NestedField(nesting.getParent().getFieldName(),
                                    new FieldWithComparison(((FieldKeyExpression)nesting.getChild()).getFieldName(), substituteComparison));
                        }
                    }
                    if (substitute == null) {
                        bindingName = null;
                    }
                }
                comparisons.put(comparison, new RankComparison(comparison, matchingIndex.get(),
                        groupFilters, groupComparisons, substitute, bindingName));
            }
        }
    }

    /**
     * A single rank function comparison.
     */
    public static class RankComparison {
        private final QueryRecordFunctionWithComparison comparison;

        private final Index index;
        private final List<QueryComponent> groupFilters;
        private final List<Comparisons.Comparison> groupComparisons;
        @Nullable
        private final QueryComponent substitute;
        @Nullable
        private final String bindingName;

        protected RankComparison(QueryRecordFunctionWithComparison comparison,
                                 Index index,
                                 List<QueryComponent> groupFilters,
                                 List<Comparisons.Comparison> groupComparisons,
                                 @Nullable QueryComponent substitute,
                                 @Nullable String bindingName) {
            this.comparison = comparison;
            this.index = index;
            this.groupFilters = groupFilters;
            this.groupComparisons = groupComparisons;
            this.substitute = substitute;
            this.bindingName = bindingName;
        }

        public Index getIndex() {
            return index;
        }

        public List<QueryComponent> getGroupFilters() {
            return groupFilters;
        }

        public ScanComparisons getScanComparisons() {
            // A rank comparison's underlying comparison is always EQUALITY or INEQUALITY (see
            // ScanComparisons.ComparisonType), so ScanComparisons.from never actually returns null here.
            final ScanComparisons rankComparison = Objects.requireNonNull(ScanComparisons.from(comparison.getComparison()));
            if (groupComparisons.isEmpty()) {
                return rankComparison;
            } else {
                // The ScanComparisons built here has no inequality comparisons, so isEquality() is always
                // true and append() (which only returns null for a non-equality receiver) never does here.
                return Objects.requireNonNull(new ScanComparisons(groupComparisons, Collections.emptySet()).append(rankComparison));
            }
        }

        @Nullable
        public QueryComponent getSubstitute() {
            return substitute;
        }

        public RecordQueryScoreForRankPlan.ScoreForRank getScoreForRank(RecordMetaData metaData) {
            final String functionName = scoreForRankFunction(comparison);
            final IndexAggregateFunction function = new IndexAggregateFunction(functionName,
                    ((IndexRecordFunction<?>)comparison.getFunction()).getOperand(), index.getName());
            final List<Comparisons.Comparison> comparisons = new ArrayList<>(groupComparisons);
            comparisons.add(comparison.getComparison());
            // substitute/bindingName are set together (both null or both non-null) at construction time; a
            // RankComparison without a substitute isn't expected to reach getScoreForRank().
            final BindingFunction bindingFunction = BindingFunction.comparisonBindingFunction(Objects.requireNonNull(substitute), index, metaData);
            return new RecordQueryScoreForRankPlan.ScoreForRank(Objects.requireNonNull(bindingName), bindingFunction, function, comparisons);
        }
    }
}
