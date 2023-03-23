/*
 * QueryPlanConstraint.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanVisitorWithDefaults;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithConstraint;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;

/**
 * Represents a query plan constraint.
 */
@FunctionalInterface
public interface QueryPlanConstraint {
    @Nonnull
    QueryPlanConstraint TAUTOLOGY = ignored -> true;

    boolean satisfiesConstraint(@Nonnull final EvaluationContext context);

    @Nonnull
    static QueryPlanConstraint tautology() {
        return TAUTOLOGY;
    }

    @Nonnull
    static QueryPlanConstraint collectConstraints(@Nonnull final RecordQueryPlan plan) {
        final var collector = new QueryPlanConstraintsCollector();
        return collector.getConstraint(plan);
    }

    /**
     * Visits a plan and collects all the {@link QueryPlanConstraint}s from it.
     */
    class QueryPlanConstraintsCollector implements RecordQueryPlanVisitorWithDefaults<Void> {

        @Nonnull
        private final ImmutableList.Builder<QueryPlanConstraint> builder = ImmutableList.builder();

        @Nonnull
        @Override
        public Void visitCoveringIndexPlan(@Nonnull final RecordQueryCoveringIndexPlan element) {
            visitDefault(element.getIndexPlan());
            return null;
        }

        @Nonnull
        @Override
        public Void visitDefault(@Nonnull final RecordQueryPlan element) {
            if (element instanceof RecordQueryPlanWithConstraint) {
                builder.add(((RecordQueryPlanWithConstraint)element).getConstraint());
            }
            for (final var child : element.getChildren()) {
                visit(child);
            }
            return null;
        }

        public QueryPlanConstraint getConstraint(@Nonnull final RecordQueryPlan plan) {
            visit(plan);
            final var constraints = builder.build();
            if (constraints.isEmpty()) {
                return tautology();
            } else if (constraints.size() == 1) {
                return constraints.get(0);
            } else {
                return evaluationContext -> constraints.stream().allMatch(constraint -> constraint.satisfiesConstraint(evaluationContext));
            }
        }
    }
}
