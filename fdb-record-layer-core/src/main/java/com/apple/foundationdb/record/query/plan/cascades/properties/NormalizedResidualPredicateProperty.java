/*
 * NormalizedResidualPredicateProperty.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.ExpressionProperty;
import com.apple.foundationdb.record.query.plan.cascades.Reference;
import com.apple.foundationdb.record.query.plan.cascades.SimpleExpressionVisitor;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionWithPredicates;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.planning.BooleanPredicateNormalizer;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionOnValuesPlan;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;

/**
 * A property that summarizes all <em>residual</em> filtering work in an expression tree as a single synthetic
 * {@link QueryPredicate}.
 *
 * <p>A <em>residual predicate</em> is a predicate that the planner cannot push down into an index range and thus
 * survives in the plan as a row-by-row filter (for example, a {@code FILTER} node somewhere above a {@code SCAN}). The
 * <em>accumulated</em> residual predicate is the recursive boolean combination of every such filter in the tree.
 * Filters at ordinary nodes are combined with {@code AND}; filters at union-like plans are combined with {@code OR}.
 * Tautologies and constant predicates are excluded, since they incur no meaningful work at runtime.</p>
 *
 * <p>The returned predicate is purely analytical; it cannot be evaluated against a stream of data. Its only purpose is
 * to be reasoned about as a boolean formula. The main consumer is {@link #countNormalizedConjuncts}, which serves
 * as a cost-model tie-breaker (lower means less residual filtering, hence preferred).</p>
 */
@API(API.Status.EXPERIMENTAL)
public final class NormalizedResidualPredicateProperty implements ExpressionProperty<QueryPredicate> {
    @Nonnull
    private static final NormalizedResidualPredicateProperty INSTANCE = new NormalizedResidualPredicateProperty();

    private NormalizedResidualPredicateProperty() {
    }

    @Nonnull
    @Override
    public NormalizedResidualPredicateVisitor createVisitor() {
        return new NormalizedResidualPredicateVisitor();
    }

    @Nonnull
    public QueryPredicate evaluate(@Nonnull final Reference reference) {
        return Objects.requireNonNull(reference.acceptVisitor(createVisitor()));
    }

    @Nonnull
    public QueryPredicate evaluate(@Nonnull final RelationalExpression expression) {
        return Objects.requireNonNull(expression.acceptVisitor(createVisitor()));
    }

    @Nonnull
    public static NormalizedResidualPredicateProperty normalizedResidualPredicate() {
        return INSTANCE;
    }

    /**
     * Counts the conjuncts in the CNF of the residual predicate accumulated across {@code expression}. A return value
     * of {@code 0} means the residual is a tautology and the expression has no residual filtering work at runtime.
     *
     * @param expression the expression to inspect
     * @return the CNF conjunct count of the accumulated residual predicate, or {@code 0} if it is a tautology
     */
    public static long countNormalizedConjuncts(@Nonnull final RelationalExpression expression) {
        final QueryPredicate residual = normalizedResidualPredicate().evaluate(expression);
        return residual.isTautology()
               ? 0
               : BooleanPredicateNormalizer
                 .getDefaultInstanceForCnf()
                 .getMetrics(residual)
                 .getNormalFormFullSize();
    }

    public static final class NormalizedResidualPredicateVisitor implements SimpleExpressionVisitor<QueryPredicate> {
        @Nonnull
        @Override
        public QueryPredicate visitRecordQueryUnionOnValuesPlan(@Nonnull final RecordQueryUnionOnValuesPlan unionPlan) {
            // For UNION-like plans, the predicates from the quantifiers must be combined using OR.
            final var predicatesFromQuantifiers = visitQuantifiers(unionPlan).stream()
                    .filter(Objects::nonNull)
                    .filter(NormalizedResidualPredicateVisitor::isResidual)
                    .collect(ImmutableList.toImmutableList());
            return OrPredicate.orOrTrue(predicatesFromQuantifiers);
        }

        @Nonnull
        @Override
        public QueryPredicate evaluateAtExpression(@Nonnull final RelationalExpression expression,
                                                   @Nonnull final List<QueryPredicate> childResults) {
            final var builder = ImmutableList.<QueryPredicate>builder();

            childResults.stream()
                    .filter(Objects::nonNull)
                    .filter(NormalizedResidualPredicateVisitor::isResidual)
                    .forEach(builder::add);

            if (expression instanceof RelationalExpressionWithPredicates withPredicates) {
                withPredicates.getPredicates()
                        .stream()
                        .filter(NormalizedResidualPredicateVisitor::isResidual)
                        .forEach(builder::add);
            }
            return AndPredicate.and(builder.build());
        }

        /**
         * A predicate is a runtime residual only if it actually has to be evaluated against rows. Tautologies and
         * {@link ConstantPredicate} objects (including {@code FALSE} and {@code NULL}) reduce to fixed plan-time values
         * and therefore do not contribute to the residual conjunct count used by the cost model.
         */
        private static boolean isResidual(@Nonnull final QueryPredicate predicate) {
            return !(predicate instanceof ConstantPredicate) && !predicate.isTautology();
        }

        @Nonnull
        @Override
        public QueryPredicate evaluateAtRef(@Nonnull final Reference ref,
                                            @Nonnull final List<QueryPredicate> memberResults) {
            Verify.verify(memberResults.size() == 1);
            return memberResults.get(0);
        }
    }
}
