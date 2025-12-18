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
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitorWithDefaults;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionWithPredicates;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class PredicateCountByLevelProperty implements ExpressionProperty<PredicateCountByLevelProperty.PredicateCountByLevelInfo> {
    @Nonnull
    private static final PredicateCountByLevelProperty PREDICATE_COUNT_BY_LEVEL = new PredicateCountByLevelProperty();

    private PredicateCountByLevelProperty() {
        // prevent outside instantiation
    }

    @Nonnull
    @Override
    public RelationalExpressionVisitor<PredicateCountByLevelInfo> createVisitor() {
        return PredicateCountByLevelVisitor.VISITOR;
    }

    public PredicateCountByLevelInfo evaluate(RelationalExpression expression) {
        return Objects.requireNonNull(createVisitor().visit(expression));
    }

    @Nonnull
    public static PredicateCountByLevelProperty predicateCountByLevel() {
        return PREDICATE_COUNT_BY_LEVEL;
    }

    public static final class PredicateCountByLevelInfo {
        private final Map<Integer, Integer> levelToPredicateCount;
        private final int highestLevel;

        public PredicateCountByLevelInfo(Map<Integer, Integer> levelToPredicateCount, int highestLevel) {
            this.levelToPredicateCount = Collections.unmodifiableMap(levelToPredicateCount);
            this.highestLevel = highestLevel;
        }

        /**
         * Combine a list of PredicateCountByLevelInfo instances by adding the number of predicates at the same level
         * across all instances.
         *
         * @param heightInfos a collection of {@code PredicateCountByLevelInfo} instances
         * @return an instance of {@code PredicateCountByLevelInfo} with predicate counts added at each level.
         */
        public static PredicateCountByLevelInfo combine(Collection<PredicateCountByLevelInfo> heightInfos) {
            final int highestLevel = heightInfos.stream().map(PredicateCountByLevelInfo::getHighestLevel).max(Integer::compare).orElse(0);
            final var newLevelToComplexityMap = heightInfos
                    .stream()
                    .flatMap(heightInfo -> heightInfo.getLevelToPredicateCount().entrySet().stream())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Integer::sum));
            return new PredicateCountByLevelInfo(newLevelToComplexityMap, highestLevel);
        }

        /**
         * Get a view of the predicate count at each level as a map.
         *
         * @return a map of level heights to the count of predicates at that level.
         */
        public Map<Integer, Integer> getLevelToPredicateCount() {
            return Collections.unmodifiableMap(levelToPredicateCount);
        }

        /**
         * Retrieves the highest level (i.e. height of root node) in this instance
         *
         * @return integer representing the highest level.
         */
        public int getHighestLevel() {
            return highestLevel;
        }

        /**
         * Compares two {@code PredicateCountByLevelInfo} instances level by level.
         * This comparison is done by comparing the number of predicates at each level recorded within the two
         * {@code PredicateCountByLevelInfo} instances, starting from the deepest level to the highest level,
         * and returns the integer comparison between the first non-equal predicate counts.
         *
         * @param  a the first {@code PredicateCountByLevelInfo} to compare
         * @param  b the second {@code PredicateCountByLevelInfo} to compare
         * @return the value {@code 0} if {@code a} have the same number of predicates at each level as {@code b};
         *         a value less than {@code 0} if a {@code a} has fewer predicates than {@code b} at a
         *         deeper level or if {@code a} has a lower number of levels with predicates; and
         *         a value greater than {@code 0} if {@code a} has more predicates than {@code b} at a
         *         deeper level or if {@code a} has a higher number of levels with predicates.
         */
        public static int compare(final PredicateCountByLevelInfo a, final PredicateCountByLevelInfo b) {
            final int highestLevel = Integer.max(a.getHighestLevel(), b.getHighestLevel());
            for (int currentLevel = 0; currentLevel <= highestLevel; currentLevel++) {
                final int aLevelPredicateCount = a.getLevelToPredicateCount().getOrDefault(currentLevel, 0);
                final int bLevelPredicateCount = b.getLevelToPredicateCount().getOrDefault(currentLevel, 0);
                int countAtLevelComparison = Integer.compare(aLevelPredicateCount, bLevelPredicateCount);
                if (countAtLevelComparison != 0) {
                    return countAtLevelComparison;
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
            final var currentLevel = childResults
                    .stream().mapToInt(PredicateCountByLevelInfo::getHighestLevel).max().orElse(0) + 1;
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
