/*
 * PredicatesUnderFetchProperty.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;

public final class PredicatesUnderFetchProperty implements ExpressionProperty<PredicatesUnderFetchProperty.PredicatesPotentiallyUnderFetch> {
    @Nonnull
    private static final PredicatesUnderFetchProperty PREDICATES_UNDER_FETCH = new PredicatesUnderFetchProperty();

    public static PredicatesUnderFetchProperty predicatesUnderFetch() {
        return PREDICATES_UNDER_FETCH;
    }

    private PredicatesUnderFetchProperty() {
        // Singleton
    }

    @Nonnull
    @Override
    public RelationalExpressionVisitor<PredicatesPotentiallyUnderFetch> createVisitor() {
        return new PredicatesUnderFetchVisitor();
    }

    public int evaluate(@Nonnull RelationalExpression expression) {
        return createVisitor().visit(expression).getPredicatesUnderFetch();
    }

    public static class PredicatesPotentiallyUnderFetch implements Comparable<PredicatesPotentiallyUnderFetch> {
        private final int totalPredicates;
        private final int predicatesUnderFetch;

        private PredicatesPotentiallyUnderFetch(int totalPredicates, int predicatesUnderFetch) {
            this.totalPredicates = totalPredicates;
            this.predicatesUnderFetch = predicatesUnderFetch;
        }

        public int getPredicatesUnderFetch() {
            return predicatesUnderFetch;
        }

        public int getTotalPredicates() {
            return totalPredicates;
        }

        @Override
        public int compareTo(final PredicatesPotentiallyUnderFetch o) {
            if (predicatesUnderFetch != o.getPredicatesUnderFetch()) {
                return Integer.compare(predicatesUnderFetch, o.getPredicatesUnderFetch());
            } else {
                return Integer.compare(totalPredicates, o.getTotalPredicates());
            }
        }
    }

    public static class PredicatesUnderFetchVisitor implements SimpleExpressionVisitor<PredicatesPotentiallyUnderFetch> {
        @Nonnull
        @Override
        public PredicatesPotentiallyUnderFetch evaluateAtExpression(@Nonnull final RelationalExpression expression, @Nonnull final List<PredicatesPotentiallyUnderFetch> childResults) {
            int newTotal = childResults.stream().mapToInt(PredicatesPotentiallyUnderFetch::getTotalPredicates).sum();
            if (expression instanceof RecordQueryFetchFromPartialRecordPlan) {
                return new PredicatesPotentiallyUnderFetch(newTotal, newTotal);
            } else {
                int newUnderFetch = childResults.stream().mapToInt(PredicatesPotentiallyUnderFetch::getPredicatesUnderFetch).sum();
                if (expression instanceof RelationalExpressionWithPredicates) {
                    newTotal += ((RelationalExpressionWithPredicates)expression).getPredicates().size();
                }
                return new PredicatesPotentiallyUnderFetch(newTotal, newUnderFetch);
            }
        }

        @Nonnull
        @Override
        public PredicatesPotentiallyUnderFetch evaluateAtRef(@Nonnull final Reference ref, @Nonnull final List<PredicatesPotentiallyUnderFetch> memberResults) {
            return Collections.max(memberResults);
        }
    }
}
