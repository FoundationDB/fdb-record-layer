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
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionWithPredicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public class PredicateCountByLevelProperty implements ExpressionProperty<PredicateCountByLevelProperty.PredicateCountByLevelInfo> {
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
        return createVisitor().visit(expression);
    }

    @Nonnull
    public static PredicateCountByLevelProperty predicateCountByLevel() {
        return PREDICATE_COUNT_BY_LEVEL;
    }

    public static final class PredicateCountByLevelInfo {
        private final Map<Integer, Integer> levelToPredicateCount;
        private final int highestLevel;

        private PredicateCountByLevelInfo(Map<Integer, Integer> levelToPredicateCount, int highestLevel) {
            this.levelToPredicateCount = levelToPredicateCount;
            this.highestLevel = highestLevel;
        }

        public static PredicateCountByLevelInfo combine(Collection<? extends PredicateCountByLevelInfo> heightInfos, BiFunction<Integer, Integer, Integer> combineFunc) {
            final Map<Integer, Integer> newLevelToComplexityMap = new HashMap<>();
            int heightLevel = 0;
            for (PredicateCountByLevelInfo heightInfo : heightInfos) {
                for (var entry : heightInfo.getLevelToPredicateCount().entrySet()) {
                    newLevelToComplexityMap.put(entry.getKey(), combineFunc.apply(newLevelToComplexityMap.getOrDefault(entry.getKey(), 0), entry.getValue()));
                }
                heightLevel = Integer.max(heightLevel, heightInfo.getHighestLevel());
            }
            return new PredicateCountByLevelInfo(newLevelToComplexityMap, heightLevel);
        }

        public Map<Integer, Integer> getLevelToPredicateCount() {
            return Collections.unmodifiableMap(levelToPredicateCount);
        }

        public int getHighestLevel() {
            return highestLevel;
        }

        /**
         * Compares two {@code PredicateHeightInfo} instances level by level.
         *
         * @param  a the first {@code PredicateHeightInfo} to compare
         * @param  b the second {@code PredicateHeightInfo} to compare
         * @return the value {@code 0} if {@code a} have the same number of predicates at each level as {@code b};
         *         a value less than {@code 0} if a {@code a} has fewer predicates than {@code b} at a
         *         lower level or if {@code a} has a lower number of levels with predicates; and
         *         a value greater than {@code 0} if {@code b} has fewer predicates than {@code a} at a
         *         lower level or if {@code b} has a lower number of levels with predicates.
         * @since 1.7
         */
        public static int compare(PredicateCountByLevelInfo a, PredicateCountByLevelInfo b) {
            for (int currentLevel = 0; currentLevel < Integer.max(a.getHighestLevel(), b.getHighestLevel()); currentLevel++) {
                var aLevelComplexity = a.getLevelToPredicateCount().getOrDefault(currentLevel, 0);
                var bLevelComplexity = b.getLevelToPredicateCount().getOrDefault(currentLevel, 0);
                var levelComparison = Integer.compare(aLevelComplexity, bLevelComplexity);
                if (levelComparison != 0) {
                    return levelComparison;
                }
            }
            return 0;
        }
    }

    private static final class PredicateCountByLevelVisitor implements SimpleExpressionVisitor<PredicateCountByLevelInfo> {
        private static final PredicateCountByLevelVisitor VISITOR = new PredicateCountByLevelVisitor();

        @Nonnull
        @Override
        public PredicateCountByLevelInfo evaluateAtExpression(@Nonnull final RelationalExpression expression, @Nonnull final List<PredicateCountByLevelInfo> childResults) {
            var lowerLevels = PredicateCountByLevelInfo.combine(childResults, Integer::sum);
            var currentLevel = childResults.stream().mapToInt(PredicateCountByLevelInfo::getHighestLevel).max().orElse(0) + 1;
            var currentLevelPredicates = 0;
            if (expression instanceof RelationalExpressionWithPredicates) {
                currentLevelPredicates = ((RelationalExpressionWithPredicates)expression).getPredicates().size();
            }
            return new PredicateCountByLevelInfo(
                    ImmutableMap.<Integer, Integer>builder()
                            .putAll(lowerLevels.getLevelToPredicateCount())
                            .put(currentLevel, currentLevelPredicates)
                            .build(),
                    currentLevel
            );
        }

        @Nonnull
        @Override
        public PredicateCountByLevelInfo evaluateAtRef(@Nonnull final Reference ref, @Nonnull final List<PredicateCountByLevelInfo> memberResults) {
            if (memberResults.size() == 1) {
                return Iterables.getOnlyElement(memberResults);
            }
            return PredicateCountByLevelInfo.combine(memberResults, Integer::max);
        }
    }
}
