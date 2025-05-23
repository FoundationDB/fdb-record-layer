/*
 * PredicateHeightProperty.java
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
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;

public class PredicateHeightProperty implements ExpressionProperty<PredicateHeightProperty.PredicateHeightInfo> {
    private static final PredicateHeightProperty PREDICATE_HEIGHT = new PredicateHeightProperty();

    @Nonnull
    @Override
    public RelationalExpressionVisitor<PredicateHeightInfo> createVisitor() {
        return PredicateHeightVisitor.VISITOR;
    }

    public int evaluate(RelationalExpression expression) {
        return createVisitor().visit(expression).getPredicateHeight();
    }

    @Nonnull
    public static PredicateHeightProperty predicateHeight() {
        return PREDICATE_HEIGHT;
    }

    public static final class PredicateHeightInfo {
        private final int height;
        private final int predicateHeight;

        private PredicateHeightInfo(int height, int predicateHeight) {
            this.height = height;
            this.predicateHeight = predicateHeight;
        }

        public static PredicateHeightInfo combine(Collection<? extends PredicateHeightInfo> heightInfos) {
            int newHeight = 0;
            int newPredicateHeight = 0;
            for (PredicateHeightInfo heightInfo : heightInfos) {
                newHeight = Math.max(newHeight, heightInfo.height);
                newPredicateHeight = Math.max(newPredicateHeight, heightInfo.predicateHeight);
            }
            return new PredicateHeightInfo(newHeight, newPredicateHeight);
        }

        public int getHeight() {
            return height;
        }

        public int getPredicateHeight() {
            return predicateHeight;
        }
    }

    private static final class PredicateHeightVisitor implements SimpleExpressionVisitor<PredicateHeightInfo> {
        private static final PredicateHeightVisitor VISITOR = new PredicateHeightVisitor();

        @Nonnull
        @Override
        public PredicateHeightInfo evaluateAtExpression(@Nonnull final RelationalExpression expression, @Nonnull final List<PredicateHeightInfo> childResults) {
            int newHeight = childResults.stream().mapToInt(PredicateHeightInfo::getHeight).max().orElse(0) + 1;
            if (expression instanceof RelationalExpressionWithPredicates) {
                var predicateExpression = (RelationalExpressionWithPredicates) expression;
                if (!predicateExpression.getPredicates().isEmpty()) {
                    return new PredicateHeightInfo(newHeight, newHeight);
                }
            }
            int newPredicateHeight = childResults.stream().mapToInt(PredicateHeightInfo::getPredicateHeight).max().orElse(0);
            return new PredicateHeightInfo(newHeight, newPredicateHeight);
        }

        @Nonnull
        @Override
        public PredicateHeightInfo evaluateAtRef(@Nonnull final Reference ref, @Nonnull final List<PredicateHeightInfo> memberResults) {
            if (memberResults.size() == 1) {
                return Iterables.getOnlyElement(memberResults);
            }
            return PredicateHeightInfo.combine(memberResults);
        }
    }
}
