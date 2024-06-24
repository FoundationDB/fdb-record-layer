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

import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordQueryPlanProto.PQueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanVisitorWithDefaults;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithConstraint;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Represents a query plan constraint.
 */
public class QueryPlanConstraint implements PlanHashable, PlanSerializable {
    @Nonnull
    private static final QueryPlanConstraint TAUTOLOGY = new QueryPlanConstraint(ConstantPredicate.TRUE);

    @Nonnull
    private final QueryPredicate predicate;

    private QueryPlanConstraint(@Nonnull final QueryPredicate predicate) {
        this.predicate = predicate;
    }

    public boolean compileTimeEval(@Nonnull final EvaluationContext context) {
        return Boolean.TRUE.equals(predicate.compileTimeEval(context));
    }

    @Nonnull
    public QueryPredicate getPredicate() {
        return predicate;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final QueryPlanConstraint that = (QueryPlanConstraint)o;
        return Objects.equals(predicate, that.predicate);
    }

    @Override
    public int hashCode() {
        return Objects.hash(predicate);
    }

    @Override
    public String toString() {
        return predicate.toString();
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode hashMode) {
        return PlanHashable.objectPlanHash(hashMode, predicate);
    }

    @Nonnull
    public QueryPlanConstraint compose(@Nonnull final QueryPlanConstraint otherQueryPlanConstraint) {
        return compose(ImmutableList.of(this, otherQueryPlanConstraint));
    }

    @Nonnull
    @Override
    public PQueryPlanConstraint toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PQueryPlanConstraint.newBuilder().setPredicate(predicate.toQueryPredicateProto(serializationContext)).build();
    }

    @Nonnull
    public static QueryPlanConstraint fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                                @Nonnull final PQueryPlanConstraint queryPlanConstraintProto) {
        return new QueryPlanConstraint(QueryPredicate.fromQueryPredicateProto(serializationContext,
                Objects.requireNonNull(queryPlanConstraintProto.getPredicate())));
    }

    @Nonnull
    public static QueryPlanConstraint compose(@Nonnull final Collection<QueryPlanConstraint> constraints) {
        return new QueryPlanConstraint(AndPredicate.andOrTrue(constraints.stream().map(QueryPlanConstraint::getPredicate).collect(Collectors.toList())));
    }

    @Nonnull
    public static QueryPlanConstraint ofPredicate(@Nonnull final QueryPredicate predicate) {
        return new QueryPlanConstraint(predicate);
    }

    @Nonnull
    public static QueryPlanConstraint ofPredicates(@Nonnull final Collection<QueryPredicate> predicates) {
        return new QueryPlanConstraint(AndPredicate.andOrTrue(predicates));
    }

    @Nonnull
    public static QueryPlanConstraint tautology() {
        return TAUTOLOGY;
    }

    @Nonnull
    public static QueryPlanConstraint collectConstraints(@Nonnull final RecordQueryPlan plan) {
        final var collector = new QueryPlanConstraintsCollector();
        return collector.getConstraint(plan);
    }

    /**
     * Visits a plan and collects all the {@link QueryPlanConstraint}s from it.
     */
    @SpotBugsSuppressWarnings({"NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", "NP_NONNULL_PARAM_VIOLATION", "NP_METHOD_RETURN_RELAXING_ANNOTATION"})
    static class QueryPlanConstraintsCollector implements RecordQueryPlanVisitorWithDefaults<Void> {

        @Nonnull
        private final ImmutableList.Builder<QueryPlanConstraint> builder = ImmutableList.builder();

        @Nullable
        @Override
        public Void visitCoveringIndexPlan(@Nonnull final RecordQueryCoveringIndexPlan element) {
            visitDefault(element.getIndexPlan());
            return null;
        }

        @Nullable
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

        @Nonnull
        public QueryPlanConstraint getConstraint(@Nonnull final RecordQueryPlan plan) {
            visit(plan);
            final var constraints = builder.build();
            return QueryPlanConstraint.ofPredicate(AndPredicate.andOrTrue(constraints.stream().map(QueryPlanConstraint::getPredicate).collect(Collectors.toList())));
        }
    }
}
