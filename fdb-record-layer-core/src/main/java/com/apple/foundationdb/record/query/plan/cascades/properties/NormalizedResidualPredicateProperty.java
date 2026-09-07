/*
 * NormalizedResidualPredicateProperty.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.planning.BooleanPredicateNormalizer;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionOnValuesPlan;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Objects;

/**
 * This property collects a {@link QueryPredicate} that represents the entirety of all accumulated residual predicates.
 * This predicate cannot be applied in any meaningful way to a stream of data, but it can be reasoned over using boolean
 * algebra. In addition, this approach also allows us to unify set operations such a UNION and INTERSECTION with their
 * boolean counterparts OR and AND.
 * One particular use of the collected residual predicate is to derive the number of effective boolean factors of its
 * CNF which is a direct measure of the number of residual filters that have to be applied and therefore its static
 * filter factor. Note that such a number can always be computed without actually materializing the CNF.
 */
@API(API.Status.EXPERIMENTAL)
public class NormalizedResidualPredicateProperty implements ExpressionProperty<QueryPredicate> {
    private static final NormalizedResidualPredicateProperty NORMALIZED_RESIDUAL_PREDICATE =
            new NormalizedResidualPredicateProperty();

    private NormalizedResidualPredicateProperty() {
        // prevent outside instantiation
    }

    @Override
    public NormalizedResidualPredicateVisitor createVisitor() {
        return new NormalizedResidualPredicateVisitor();
    }

    public QueryPredicate evaluate(final Reference reference) {
        return Objects.requireNonNull(reference.acceptVisitor(createVisitor()));
    }

    public QueryPredicate evaluate(final RelationalExpression expression) {
        return Objects.requireNonNull(expression.acceptVisitor(createVisitor()));
    }

    public static NormalizedResidualPredicateProperty normalizedResidualPredicate() {
        return NORMALIZED_RESIDUAL_PREDICATE;
    }

    public static long countNormalizedConjuncts(RelationalExpression expression) {
        final var magicPredicate = normalizedResidualPredicate().evaluate(expression);
        return magicPredicate.isTautology()
               ? 0
               : BooleanPredicateNormalizer
                       .getDefaultInstanceForCnf()
                       .getMetrics(magicPredicate)
                       .getNormalFormFullSize();
    }

    public static class NormalizedResidualPredicateVisitor implements SimpleExpressionVisitor<QueryPredicate> {
        @Override
        public QueryPredicate visitRecordQueryUnionOnValuesPlan(final RecordQueryUnionOnValuesPlan unionPlan) {
            final var predicatesFromQuantifiers = visitQuantifiers(unionPlan).stream()
                    .filter(Objects::nonNull)
                    .filter(predicate -> !predicate.isTautology())
                    .collect(ImmutableList.toImmutableList());
            return OrPredicate.orOrTrue(predicatesFromQuantifiers);
        }

        @Override
        public QueryPredicate evaluateAtExpression(final RelationalExpression expression,
                                                   final List<QueryPredicate> childResults) {
            final var resultPredicatesBuilder = ImmutableList.<QueryPredicate>builder();

            childResults.stream()
                    .filter(Objects::nonNull)
                    .filter(predicate -> !predicate.isTautology())
                    .forEach(resultPredicatesBuilder::add);

            if (expression instanceof RelationalExpressionWithPredicates) {
                ((RelationalExpressionWithPredicates)expression).getPredicates()
                        .stream()
                        .filter(predicate -> !predicate.isTautology())
                        .forEach(resultPredicatesBuilder::add);
            }
            return AndPredicate.and(resultPredicatesBuilder.build());
        }

        @Override
        public QueryPredicate evaluateAtRef(final Reference ref,
                                            final List<QueryPredicate> memberResults) {
            Verify.verify(memberResults.size() == 1);
            return Iterables.getOnlyElement(memberResults);
        }
    }
}
