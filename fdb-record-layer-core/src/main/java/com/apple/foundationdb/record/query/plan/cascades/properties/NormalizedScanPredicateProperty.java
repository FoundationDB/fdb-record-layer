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
import com.apple.foundationdb.record.query.plan.cascades.ExpressionRef;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitorWithDefaults;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.planning.BooleanPredicateNormalizer;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryCoveringIndexPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIntersectionOnValuesPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlanWithComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQuerySetPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionOnValuesPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnorderedUnionPlan;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
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
public class NormalizedScanPredicateProperty implements ExpressionProperty<QueryPredicate>, RelationalExpressionVisitorWithDefaults<QueryPredicate> {
    @Nonnull
    private static final NormalizedScanPredicateProperty INSTANCE = new NormalizedScanPredicateProperty();

    @Nonnull
    @Override
    public QueryPredicate visitRecordQueryUnionOnValuesPlan(@Nonnull final RecordQueryUnionOnValuesPlan unionPlan) {
        return visitSetPlan(unionPlan);
    }

    @Nonnull
    @Override
    public QueryPredicate visitRecordQueryUnorderedUnionPlan(@Nonnull final RecordQueryUnorderedUnionPlan unionPlan) {
        return visitSetPlan(unionPlan);
    }

    @Nonnull
    @Override
    public QueryPredicate visitRecordQueryIntersectionOnValuesPlan(@Nonnull final RecordQueryIntersectionOnValuesPlan intersectionPlan) {
        return visitSetPlan(intersectionPlan);
    }

    @Nonnull
    private QueryPredicate visitSetPlan(@Nonnull final RecordQuerySetPlan setPlan) {
        final var predicatesFromQuantifiers = visitQuantifiers(setPlan);

        QueryPredicate minScanPredicate = null;
        long minConjuncts = Long.MAX_VALUE;
        for (final QueryPredicate predicate : predicatesFromQuantifiers) {
            final long numConjuncts = countNormalizedConjuncts(predicate);

            if (minScanPredicate == null || numConjuncts < minConjuncts) {
                minScanPredicate = predicate;
                minConjuncts = numConjuncts;
            }
        }

        return minScanPredicate == null ? ConstantPredicate.TRUE : minScanPredicate;
    }

    @Nonnull
    @Override
    public QueryPredicate evaluateAtExpression(@Nonnull RelationalExpression expression, @Nonnull List<QueryPredicate> childResults) {
        final var resultPredicatesBuilder = ImmutableList.<QueryPredicate>builder();

        if (expression instanceof RecordQueryCoveringIndexPlan) {
            expression = ((RecordQueryCoveringIndexPlan)expression).getIndexPlan();
        }

        if (expression instanceof RecordQueryPlanWithComparisons) {
            final var planWithComparisons = (RecordQueryPlanWithComparisons)expression;
            if (planWithComparisons.hasComparisons()) {
                planWithComparisons.getComparisons()
                        .stream()
                        .map(comparison ->
                                new ValuePredicate(QuantifiedObjectValue.of(Quantifier.uniqueID(), Type.any()), comparison))
                        .filter(predicate -> !predicate.isTautology())
                        .forEach(resultPredicatesBuilder::add);
            }
        } else {
            childResults.stream()
                    .filter(Objects::nonNull)
                    .filter(predicate -> !predicate.isTautology())
                    .forEach(resultPredicatesBuilder::add);
        }

        return AndPredicate.andOrTrue(resultPredicatesBuilder.build());
    }

    @Nonnull
    @Override
    public QueryPredicate evaluateAtRef(@Nonnull ExpressionRef<? extends RelationalExpression> ref, @Nonnull List<QueryPredicate> memberResults) {
        Verify.verify(memberResults.size() == 1);
        return Iterables.getOnlyElement(memberResults);
    }

    @Nonnull
    public static QueryPredicate evaluate(ExpressionRef<? extends RelationalExpression> ref) {
        return Verify.verifyNotNull(ref.acceptPropertyVisitor(INSTANCE));
    }

    @Nonnull
    public static QueryPredicate evaluate(@Nonnull RelationalExpression expression) {
        return Verify.verifyNotNull(expression.acceptPropertyVisitor(INSTANCE));
    }

    public static long countNormalizedConjuncts(@Nonnull final RelationalExpression expression) {
        return countNormalizedConjuncts(evaluate(expression));
    }

    protected static long countNormalizedConjuncts(@Nonnull final QueryPredicate predicate) {
        return predicate.isTautology()
               ? 0
               : BooleanPredicateNormalizer
                       .getDefaultInstanceForCnf()
                       .getMetrics(predicate)
                       .getNormalFormFullSize();
    }
}
