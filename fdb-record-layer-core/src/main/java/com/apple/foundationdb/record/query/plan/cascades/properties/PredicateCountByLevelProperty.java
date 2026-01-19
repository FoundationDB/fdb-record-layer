/*
 * PredicateCountByLevelProperty.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.plan.cascades.properties;

import com.apple.foundationdb.record.query.plan.cascades.ExpressionProperty;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.SimpleExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionWithPredicates;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedPrimaryKeyDistinctPlan;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.stream.Collectors;

/**
 * <p>
 * This property traverses a {@link RelationalExpression} to find the total number of
 * {@link com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate}s associated with
 * {@link RelationalExpressionWithPredicates} implementations at each level of the expression tree.
 * </p>
 * <p>
 * Information about the number of predicates at each level of the tree is encoded in instances of
 * {@link PredicateCountByLevelInfo}, which can be compared using the method {@link PredicateCountByLevelInfo#compare}
 * to determine which expression trees contain more predicates at a deeper level.
 * </p>
 * <p>
 * See also {@link com.apple.foundationdb.record.query.plan.cascades.rules.PredicatePushDownRule}.
 * </p>
 */
public class PredicateCountByLevelProperty implements ExpressionProperty<PredicateCountByLevelProperty.PredicateCountByLevelInfo> {
    @Nonnull
    private static final PredicateCountByLevelProperty PREDICATE_COUNT_BY_LEVEL = new PredicateCountByLevelProperty();

    private PredicateCountByLevelProperty() {
        // prevent outside instantiation
    }

    /**
     * Returns the singleton instance of {@link PredicateCountByLevelProperty}.
     *
     * @return the singleton instance of {@link PredicateCountByLevelProperty}
     */
    @Nonnull
    public static PredicateCountByLevelProperty predicateCountByLevel() {
        return PREDICATE_COUNT_BY_LEVEL;
    }

    /**
     * Creates a {@link SimpleExpressionVisitor} that can traverse a {@link RelationalExpression}
     * to find the total number of {@link com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate}
     * associated with {@link RelationalExpressionWithPredicates} implementations at each level of the expression tree.
     *
     * @return a {@link SimpleExpressionVisitor} that produces {@link PredicateCountByLevelInfo} results.
     */
    @Nonnull
    @Override
    public SimpleExpressionVisitor<PredicateCountByLevelInfo> createVisitor() {
        return PredicateCountByLevelVisitor.VISITOR;
    }

    /**
     * Evaluate this property for over the given {@link RelationalExpression}.
     *
     * @param expression The root of the expression tree to traverse.
     * @return a {@link PredicateCountByLevelInfo} containing the predicate count at each level
     *         of the expression tree.
     */
    @Nonnull
    public PredicateCountByLevelInfo evaluate(RelationalExpression expression) {
        return Objects.requireNonNull(expression.acceptVisitor(createVisitor()));
    }

    /**
     * <p>
     * An object that contains information about the number of query predicates at each level
     * of a {@link RelationalExpression} tree. Level numbers in instances of this class
     * start from 0 for leaf nodes and increase towards the root, with the root node having the highest
     * level number, which can be retrieved via {@link #getHighestLevel()}.
     * </p>
     * <p>
     * Instances of this class are can be created for a {@link RelationalExpression} using the method
     * {@link PredicateCountByLevelProperty#evaluate(RelationalExpression)}.
     * </p>
     */
    public static final class PredicateCountByLevelInfo {
        private final ImmutableSortedMap<Integer, Integer> levelToPredicateCount;

        public PredicateCountByLevelInfo(Map<Integer, Integer> levelToPredicateCount) {
            this.levelToPredicateCount = ImmutableSortedMap.copyOf(levelToPredicateCount);
        }

        /**
         * Combine a list of {@link PredicateCountByLevelInfo} instances by summing the number of query predicates
         * at the same level across all instances.
         *
         * @param heightInfos a collection of {@link PredicateCountByLevelInfo} instances.
         * @return a new instance of {@link PredicateCountByLevelInfo} with the combined count.
         */
        @Nonnull
        public static PredicateCountByLevelInfo combine(Collection<PredicateCountByLevelInfo> heightInfos) {
            return new PredicateCountByLevelInfo(heightInfos
                    .stream()
                    .flatMap(heightInfo -> heightInfo.getLevelToPredicateCount().entrySet().stream())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Integer::sum)));
        }

        /**
         * <p>
         * Get a view of the query predicate count at each level as a {@link SortedMap}.
         * </p>
         * <p>
         * Level heights, which are the keys of the returned {@link SortedMap}, start from 0 for leaf nodes in the
         * {@link RelationalExpression} used to create this instance, and increase towards the root, with the root node
         * having the highest level number.
         * </p>
         *
         * @return a {@link SortedMap} of level heights to the count of query predicates at that level.
         */
        @Nonnull
        public SortedMap<Integer, Integer> getLevelToPredicateCount() {
            return levelToPredicateCount;
        }

        /**
         * Retrieves the highest level height in the {@link RelationalExpression} tree used to create
         * used to create this instance, which corresponds to the level of the root node of the expression tree.
         *
         * @return an integer the height of the highest level number in the tree, or -1 if no levels have been recorded.
         */
        public int getHighestLevel() {
            return levelToPredicateCount.isEmpty() ? -1 : levelToPredicateCount.lastKey();
        }

        /**
         * <p>
         * Compares two {@link PredicateCountByLevelInfo} instances level by level.
         * </p>
         * <p>
         * This comparison is done by comparing the number of
         * {@link com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate}s at each level recorded
         * within the provided {@link PredicateCountByLevelInfo} instances, starting from the deepest level (representing
         * the leaf nodes of the {@link RelationalExpression} tree used to create those instances) to the highest level
         * (representing the root node) and returns the integer comparison between the first non-equal query predicate
         * counts. If the number of query predicates is equal at each level, this returns zero indicating that the
         * two plans should be considered equivalent.
         * </p>
         * @param a the first {@link PredicateCountByLevelInfo} to compare
         * @param b the second {@link PredicateCountByLevelInfo} to compare
         *
         * @return the value {@code 0} if {@code a} have the same number of predicates at each level as {@code b};
         *         a value less than {@code 0} if {@code a} has fewer predicates than {@code b} at a
         *         deeper level; and a value greater than {@code 0} if {@code a} has more predicates than
         *         {@code b} at a deeper level
         */
        public static int compare(final PredicateCountByLevelInfo a, final PredicateCountByLevelInfo b) {
            final SortedMap<Integer, Integer> aLevelToPredicateCount = a.getLevelToPredicateCount();
            int aHighestLevel = a.getHighestLevel();
            final SortedMap<Integer, Integer> bLevelToPredicateCount = b.getLevelToPredicateCount();
            int bHighestLevel = b.getHighestLevel();
            for (int level = 0; level < Math.max(aHighestLevel, bHighestLevel); level++) {
                final int aPredicateCountAtLevel = aLevelToPredicateCount.getOrDefault(level, 0);
                final int bPredicateCountAtLevel = bLevelToPredicateCount.getOrDefault(level, 0);
                if (aPredicateCountAtLevel != bPredicateCountAtLevel) {
                    return Integer.compare(aPredicateCountAtLevel, bPredicateCountAtLevel);
                }
            }
            return 0;
        }
    }

    private static final class PredicateCountByLevelVisitor implements SimpleExpressionVisitor<PredicateCountByLevelInfo> {
        @Nonnull
        private static final PredicateCountByLevelVisitor VISITOR = new PredicateCountByLevelVisitor();

        private PredicateCountByLevelVisitor() {
            // prevent outside instantiation
        }

        @Nonnull
        @Override
        public PredicateCountByLevelInfo evaluateAtExpression(@Nonnull final RelationalExpression expression,
                                                              @Nonnull final List<PredicateCountByLevelInfo> childResults) {
            final var newLevelToPredicateCountMap = ImmutableMap.<Integer, Integer>builder()
                    .putAll(PredicateCountByLevelInfo.combine(childResults).getLevelToPredicateCount());
            if (expression instanceof RecordQueryUnorderedPrimaryKeyDistinctPlan) {
                // For the purposes of computing the predicate levels, ignore distinct plans. This is a bit hacky,
                // but we don't want them to participate in comparisons on predicate levels. This is because
                // we will generally want to prefer plans with deeper predicates, except we want to push distinct
                // operators below filters (see: PushDistinctThroughFilterRule).
                return new PredicateCountByLevelInfo(newLevelToPredicateCountMap.build());
            }
            final var currentLevel = childResults
                    .stream().mapToInt(PredicateCountByLevelInfo::getHighestLevel).max().orElse(-1) + 1;
            final int currentLevelPredicates;
            if (expression instanceof RelationalExpressionWithPredicates) {
                currentLevelPredicates = ((RelationalExpressionWithPredicates)expression).getPredicates().size();
            } else {
                currentLevelPredicates = 0;
            }
            return new PredicateCountByLevelInfo(newLevelToPredicateCountMap.put(currentLevel, currentLevelPredicates).build());
        }

        @Nonnull
        @Override
        public PredicateCountByLevelInfo evaluateAtRef(@Nonnull final Reference reference, @Nonnull final List<PredicateCountByLevelInfo> memberResults) {
            Verify.verify(memberResults.size() == 1);
            return Iterables.getOnlyElement(memberResults);
        }
    }
}
