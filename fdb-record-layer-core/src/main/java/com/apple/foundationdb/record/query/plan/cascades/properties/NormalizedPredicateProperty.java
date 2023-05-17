/*
 * NormalizedPredicateProperty.java
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
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionVisitorWithDefaults;
import com.apple.foundationdb.record.query.plan.cascades.expressions.RelationalExpressionWithPredicates;
import com.apple.foundationdb.record.query.plan.cascades.predicates.AndPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.OrPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.QueryPredicate;
import com.apple.foundationdb.record.query.plan.planning.BooleanPredicateNormalizer;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryUnionOnValuesPlan;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;

/**
 * // TODO.
 */
@API(API.Status.EXPERIMENTAL)
public class NormalizedPredicateProperty implements ExpressionProperty<QueryPredicate>, RelationalExpressionVisitorWithDefaults<QueryPredicate> {
    private static final NormalizedPredicateProperty INSTANCE = new NormalizedPredicateProperty();

    @Nonnull
    @Override
    public QueryPredicate visitRecordQueryUnionOnValuesPlan(@Nonnull final RecordQueryUnionOnValuesPlan unionPlan) {
        final var predicatesFromQuantifiers = visitQuantifiers(unionPlan).stream()
                .filter(Objects::nonNull)
                .filter(predicate -> !predicate.isTautology())
                .collect(ImmutableList.toImmutableList());
        return OrPredicate.orOrTrue(predicatesFromQuantifiers);
    }

    @Nonnull
    @Override
    public QueryPredicate evaluateAtExpression(@Nonnull RelationalExpression expression, @Nonnull List<QueryPredicate> childResults) {
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

    public static long countNormalizedConjuncts(@Nonnull RelationalExpression expression) {
        final var magicPredicate = evaluate(expression);
        return magicPredicate.isTautology()
               ? 0
               : BooleanPredicateNormalizer
                       .getDefaultInstanceForCnf()
                       .getMetrics(magicPredicate)
                       .getNormalFormFullSize();
    }
}
