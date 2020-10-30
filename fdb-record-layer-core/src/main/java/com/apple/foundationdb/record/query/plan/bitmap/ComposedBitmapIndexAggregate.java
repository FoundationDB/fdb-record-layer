/*
 * ComposedBitmapIndex.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.bitmap;

import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.ThenKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.IndexAggregateGroupKeys;
import com.apple.foundationdb.record.provider.foundationdb.IndexFunctionHelper;
import com.apple.foundationdb.record.provider.foundationdb.indexes.BitmapValueIndexMaintainer;
import com.apple.foundationdb.record.query.QueryToKeyMatcher;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.AndOrComponent;
import com.apple.foundationdb.record.query.expressions.ComponentWithComparison;
import com.apple.foundationdb.record.query.expressions.FieldWithComparison;
import com.apple.foundationdb.record.query.expressions.NotComponent;
import com.apple.foundationdb.record.query.expressions.OrComponent;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.expressions.QueryKeyExpressionWithComparison;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.planning.FilterSatisfiedMask;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Transform a tree of Boolean expressions into a tree of bitwise operations on streams of bitmaps.
 *
 * So, {@code AND} turns into {@code BITAND} and {@code OR} into {@code BITOR}, with the leaves of the streams
 * being scans of a {@code BITMAP_VALUE} index keyed by the leaf condition.
 *
 * Optional additional grouping predicates for all indexes are also preserved.
 */
public class ComposedBitmapIndexAggregate {
    @Nonnull
    private final Node root;

    ComposedBitmapIndexAggregate(@Nonnull Node root) {
        this.root = root;
    }

    /**
     * Try to build a composed bitmap plan for the given query and aggregate function.
     * @param planner a query planner to use to construct the plans
     * @param query a query providing target record type, filter, and required fields
     * @param indexAggregateFunction the function giving the desired position and grouping
     * @return an {@code Optional} query plan or {@code Optional.empty} if planning is not possible
     */
    @Nonnull
    public static Optional<ComposedBitmapIndexQueryPlan> tryPlan(@Nonnull RecordQueryPlanner planner,
                                                                 @Nonnull RecordQuery query,
                                                                 @Nonnull IndexAggregateFunction indexAggregateFunction) {
        if (query.getFilter() == null || query.getSort() != null) {
            return Optional.empty();
        }
        return tryBuild(planner, query.getRecordTypes(), indexAggregateFunction, query.getFilter())
                .flatMap(p -> p.tryPlan(planner, query.toBuilder()));
    }

    /**
     * Try to turn this composed bitmap into an executable plan.
     * @param planner a query planner to use to construct the plans
     * @param queryBuilder a prototype query providing target record types and required fields
     * @return an {@code Optional} query plan or {@code Optional.empty} if planning is not possible
     */
    @Nonnull
    public Optional<ComposedBitmapIndexQueryPlan> tryPlan(@Nonnull RecordQueryPlanner planner,
                                                          @Nonnull RecordQuery.Builder queryBuilder) {
        final List<RecordQueryCoveringIndexPlan> indexScans = new ArrayList<>();
        final Map<IndexNode, ComposedBitmapIndexQueryPlan.IndexComposer> indexComposers = new IdentityHashMap<>();
        return Optional.ofNullable(plan(root, queryBuilder, planner, indexScans, indexComposers))
                .map(composer -> new ComposedBitmapIndexQueryPlan(indexScans, composer));
    }

    /**
     * Try to build a composed bitmap for the given aggregate function and filters.
     * <p>
     * The function should use a supported aggregate function (currently {@value BitmapValueIndexMaintainer#AGGREGATE_FUNCTION_NAME})
     * and the bitmap-indexed position field grouped by any common fields.
     * The filter should supply equality conditions to the common fields.
     * The filter can include additional equality conditions on various other fields for which there are appropriate bitmap indexes, in a Boolean
     * expression that will be transformed into a set the corresponding bit operations on the bitmaps.
     * The filter can also include range conditions on the position field.
     * </p>
     * @param planner a query planner to use to construct the plans
     * @param recordTypeNames the record types on which the indexes are defined
     * @param indexAggregateFunction the function giving the desired position and grouping
     * @param filter conditions on the groups and position
     * @return an {@code Optional} composed bitmap or {@code Optional.empty} if there conditions could not be satisfied
     */
    @Nonnull
    public static Optional<ComposedBitmapIndexAggregate> tryBuild(@Nonnull RecordQueryPlanner planner,
                                                                  @Nonnull Collection<String> recordTypeNames,
                                                                  @Nonnull IndexAggregateFunction indexAggregateFunction,
                                                                  @Nonnull QueryComponent filter) {
        // The filters that are common to all the composed index queries.
        // They can be equality conditions on the common group prefix (as specified by indexAggregateFunction)
        // or inequalities on the position.
        List<QueryComponent> commonFilters = new ArrayList<>();
        // The filters that are specific to a single index. At present, each comparison must be accomplished by a single
        // index. It would be possible, though, to have indexes with multiple grouping fields after the common prefix
        // and to pick any complete covering of indexFilters.
        List<QueryComponent> indexFilters = new ArrayList<>();
        if (!separateGroupFilters(filter, indexAggregateFunction, commonFilters, indexFilters) || indexFilters.isEmpty()) {
            return Optional.empty();
        }
        Builder builder = new Builder(planner, recordTypeNames, commonFilters, indexAggregateFunction);
        return builder.tryBuild(indexFilters.size() > 1 ? Query.and(indexFilters) : indexFilters.get(0))
            .map(ComposedBitmapIndexAggregate::new);
    }

    private static boolean separateGroupFilters(@Nonnull QueryComponent filter,
                                                @Nonnull IndexAggregateFunction indexAggregateFunction,
                                                @Nonnull List<QueryComponent> commonFilters,
                                                @Nonnull List<QueryComponent> indexFilters) {
        QueryToKeyMatcher matcher = new QueryToKeyMatcher(filter);
        FilterSatisfiedMask filterMask = FilterSatisfiedMask.of(filter);
        QueryToKeyMatcher.Match match = matcher.matchesCoveringKey(((GroupingKeyExpression)indexAggregateFunction.getOperand()).getGroupingSubKey(), filterMask);
        if (match.getType() != QueryToKeyMatcher.MatchType.EQUALITY) {
            return false;   // Did not manage to fully restrict the grouping key.
        }
        // The position key(s) can also be constrained with inequalities and those go among the group filters.
        matcher.matchesCoveringKey(((GroupingKeyExpression)indexAggregateFunction.getOperand()).getGroupedSubKey(), filterMask);
        if (filterMask.allSatisfied()) {
            return false;   // Not enough conditions left over.
        }
        for (FilterSatisfiedMask child : filterMask.getChildren()) {
            // A child filter will be satisfied if it matches one of the subkeys given to matchesCoveringKey,
            // either the common group prefix for equality on the first pass,
            // or the position on the second pass.
            // Any left-over filter not matching either of those must match some per-index key.
            if (child.allSatisfied()) {
                commonFilters.add(child.getFilter());
            } else {
                indexFilters.add(child.getFilter());
            }
        }
        return true;
    }

    @Nullable
    private ComposedBitmapIndexQueryPlan.ComposerBase plan(@Nonnull Node node, @Nonnull RecordQuery.Builder queryBuilder,
                                                           @Nonnull RecordQueryPlanner planner,
                                                           @Nonnull List<RecordQueryCoveringIndexPlan> indexScans,
                                                           @Nonnull Map<IndexNode, ComposedBitmapIndexQueryPlan.IndexComposer> indexComposers) {
        if (node instanceof OperatorNode) {
            final OperatorNode operatorNode = (OperatorNode) node;
            final List<ComposedBitmapIndexQueryPlan.ComposerBase> children = new ArrayList<>();
            for (Node n : operatorNode.operands) {
                ComposedBitmapIndexQueryPlan.ComposerBase plan = plan(n, queryBuilder, planner, indexScans, indexComposers);
                if (plan == null) {
                    return null;
                }
                children.add(plan);
            }
            switch (operatorNode.operator) {
                case AND:
                    return new ComposedBitmapIndexQueryPlan.AndComposer(children);
                case OR:
                    return new ComposedBitmapIndexQueryPlan.OrComposer(children);
                case NOT:
                    return new ComposedBitmapIndexQueryPlan.NotComposer(children.get(0));
                default:
                    throw new IllegalArgumentException("Unknown operator node: " + node);
            }
        } else if (node instanceof IndexNode) {
            return indexComposers.computeIfAbsent((IndexNode)node, indexNode -> {
                // We change the filter of the supplied builder and then immediately build it.
                queryBuilder.setFilter(indexNode.filter);
                final Index index = planner.getRecordMetaData().getIndex(indexNode.indexName);
                final KeyExpression wholeKey = ((GroupingKeyExpression)index.getRootExpression()).getWholeKey();
                final RecordQueryCoveringIndexPlan indexScan = planner.planCoveringAggregateIndex(queryBuilder.build(), index,
                        wholeKey, true); // Partial view of repeated field is okay for composition.
                if (indexScan == null) {
                    return null;
                }
                final int position = indexScans.size();
                indexScans.add(indexScan);
                return new ComposedBitmapIndexQueryPlan.IndexComposer(position);
            });
        } else {
            throw new IllegalArgumentException("Unknown node type: " + node);
        }
    }

    static class Node {
    }

    static class OperatorNode extends Node {
        enum Operator { AND, OR, NOT }

        @Nonnull
        private final Operator operator;
        @Nonnull
        private final List<Node> operands;

        OperatorNode(@Nonnull Operator operator, @Nonnull List<Node> operands) {
            this.operator = operator;
            this.operands = operands;
        }
    }

    // Note that the same IndexNode can occur multiple times in the tree, if the same condition subexpression appears
    // multiple times in the filter.
    static class IndexNode extends Node {
        @Nonnull
        private final QueryComponent filter;
        @Nonnull
        private final IndexAggregateGroupKeys groupKeys;
        @Nonnull
        private final String indexName;

        IndexNode(@Nonnull QueryComponent filter, @Nonnull IndexAggregateGroupKeys groupKeys, @Nonnull String indexName) {
            this.filter = filter;
            this.groupKeys = groupKeys;
            this.indexName = indexName;
        }
    }

    static class Builder {
        @Nonnull
        private final RecordQueryPlanner planner;
        @Nonnull
        private final Collection<String> recordTypeNames;
        @Nonnull
        private final List<QueryComponent> groupFilters;
        @Nonnull
        private final IndexAggregateFunction indexAggregateFunction;
        @Nullable
        private Map<KeyExpression, Index> bitmapIndexes;
        @Nullable
        private Map<QueryComponent, IndexNode> indexNodes;

        Builder(@Nonnull final RecordQueryPlanner planner, @Nonnull Collection<String> recordTypeNames,
                @Nonnull List<QueryComponent> groupFilters, @Nonnull IndexAggregateFunction indexAggregateFunction) {
            this.planner = planner;
            this.recordTypeNames = recordTypeNames;
            this.groupFilters = groupFilters;
            this.indexAggregateFunction = indexAggregateFunction;
        }

        @Nonnull
        Optional<Node> tryBuild(@Nonnull QueryComponent indexFilter) {
            if (indexFilter instanceof ComponentWithComparison) {
                return indexScan(indexFilter);
            }
            if (indexFilter instanceof AndOrComponent) {
                final AndOrComponent andOrComponent = (AndOrComponent) indexFilter;
                List<Node> childNodes = new ArrayList<>(andOrComponent.getChildren().size());
                for (QueryComponent child : andOrComponent.getChildren()) {
                    Optional<Node> childNode = tryBuild(child);
                    if (!childNode.isPresent()) {
                        return Optional.empty();
                    }
                    childNodes.add(childNode.get());
                }
                final OperatorNode.Operator operator = indexFilter instanceof OrComponent ? OperatorNode.Operator.OR : OperatorNode.Operator.AND;
                return Optional.of(new OperatorNode(operator, childNodes));
            }
            if (indexFilter instanceof NotComponent) {
                return tryBuild(((NotComponent) indexFilter).getChild())
                        .map(childNode -> new OperatorNode(OperatorNode.Operator.NOT, Collections.singletonList(childNode)));
            }
            return Optional.empty();
        }

        @Nonnull
        Optional<Node> indexScan(@Nonnull QueryComponent indexFilter) {
            if (bitmapIndexes == null) {
                bitmapIndexes = findBitmapIndexes(indexAggregateFunction.getName());
                if (bitmapIndexes.isEmpty()) {
                    return Optional.empty();
                }
                indexNodes = new HashMap<>();
            }
            IndexNode existing = indexNodes.get(indexFilter);
            if (existing != null) {
                return Optional.of(existing);
            }
            final KeyExpression indexKey;
            if (indexFilter instanceof FieldWithComparison) {
                indexKey = Key.Expressions.field(((FieldWithComparison) indexFilter).getFieldName());
            } else if (indexFilter instanceof QueryKeyExpressionWithComparison) {
                indexKey = ((QueryKeyExpressionWithComparison) indexFilter).getKeyExpression();
            } else {
                return Optional.empty();
            }
            // Splice the index's key between the common grouping key and the position.
            // The simplest place is directly before the position.
            // But if part of the group is a nested concat, breaking that up would need support in QueryToKeyMatcher.
            // Moreover, the caller needs to have arranged for a compatible index to exist, which requires new support to define.
            // (https://github.com/FoundationDB/fdb-record-layer/issues/1056)
            final GroupingKeyExpression groupKey = (GroupingKeyExpression)indexAggregateFunction.getOperand();
            final int groupedCount = groupKey.getGroupedCount();
            final int wholeCount = groupKey.getColumnSize();
            int afterSpliceCount = groupedCount;
            if (groupKey.getWholeKey() instanceof ThenKeyExpression) {
                final List<KeyExpression> thenChildren = ((ThenKeyExpression)groupKey.getWholeKey()).getChildren();
                int childPosition = thenChildren.size();
                // Compute the minimum number that includes all grouped fields and keeps involved children intact.
                afterSpliceCount = 0;
                while (afterSpliceCount < groupedCount) {
                    afterSpliceCount += thenChildren.get(--childPosition).getColumnSize();
                }
            }
            final ThenKeyExpression splicedKey;
            if (afterSpliceCount == groupedCount) {
                // Preferred position at end of grouping keys.
                splicedKey = Key.Expressions.concat(groupKey.getGroupingSubKey(), indexKey, groupKey.getGroupedSubKey());
            } else {
                final KeyExpression wholeKey = groupKey.getWholeKey();
                final int splicePoint = wholeCount - afterSpliceCount;
                if (splicePoint == 0) {
                    splicedKey = Key.Expressions.concat(indexKey, wholeKey);
                } else {
                    splicedKey = Key.Expressions.concat(wholeKey.getSubKey(0, splicePoint), indexKey, wholeKey.getSubKey(splicePoint, wholeCount));
                }
            }
            GroupingKeyExpression fullKey = splicedKey.group(groupedCount);
            Index index = bitmapIndexes.get(fullKey);
            if (index == null) {
                return Optional.empty();
            }
            final QueryComponent fullFilter;
            if (groupFilters.isEmpty()) {
                fullFilter = indexFilter;
            } else {
                List<QueryComponent> allFilters = new ArrayList<>(groupFilters.size() + 1);
                allFilters.addAll(groupFilters);
                allFilters.add(indexFilter);
                fullFilter = Query.and(allFilters);
            }
            // Allow conditions on the position field as well.
            final KeyExpression fullOperand = new GroupingKeyExpression(fullKey.getWholeKey(), 0);
            return IndexAggregateGroupKeys.conditionsToGroupKeys(fullOperand, fullFilter)
                    .map(groupKeys -> {
                        final IndexNode indexNode = new IndexNode(fullFilter, groupKeys, index.getName());
                        indexNodes.put(indexFilter, indexNode);
                        return indexNode;
                    });
        }

        @Nonnull
        Map<KeyExpression, Index> findBitmapIndexes(@Nonnull String aggregateFunction) {
            final String indexType;
            if (BitmapValueIndexMaintainer.AGGREGATE_FUNCTION_NAME.equals(aggregateFunction)) {
                indexType = IndexTypes.BITMAP_VALUE;
            } else {
                return Collections.emptyMap();
            }
            final RecordStoreState recordStoreState = planner.getRecordStoreState();
            return IndexFunctionHelper.indexesForRecordTypes(planner.getRecordMetaData(), recordTypeNames)
                    .filter(index -> index.getType().equals(indexType))
                    .filter(recordStoreState::isReadable)
                    .collect(Collectors.toMap(Index::getRootExpression, Function.identity()));
        }

    }
}
