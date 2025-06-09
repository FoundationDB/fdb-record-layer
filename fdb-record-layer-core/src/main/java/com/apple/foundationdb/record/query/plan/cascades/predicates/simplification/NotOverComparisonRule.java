/*
 * NotOverComparisonRule.java
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

package com.apple.foundationdb.record.query.plan.cascades.predicates.simplification;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.QueryPlanConstraint;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher;
import com.apple.foundationdb.record.query.plan.cascades.predicates.NotPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.anyComparison;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.notPredicate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.valuePredicate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.anyValue;

/**
 * A rule that matches a {@link NotPredicate} (with the argument values) and attempts to push it into a boolean variable
 * underneath (which can be a comparison).
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class NotOverComparisonRule extends QueryPredicateComputationRule<EvaluationContext, List<QueryPlanConstraint>, NotPredicate> {
    @Nonnull
    private static final BindingMatcher<Value> anyValueMatcher = anyValue();
    @Nonnull
    private static final BindingMatcher<Comparisons.Comparison> anyComparisonMatcher = anyComparison();
    @Nonnull
    private static final BindingMatcher<ValuePredicate> anyValuePredicateMatcher = valuePredicate(anyValueMatcher, anyComparisonMatcher);
    @Nonnull
    private static final BindingMatcher<NotPredicate> rootMatcher = notPredicate(ListMatcher.exactly(anyValuePredicateMatcher));

    public NotOverComparisonRule() {
        super(rootMatcher);
    }

    @Nonnull
    @Override
    public Optional<Class<?>> getRootOperator() {
        return Optional.of(NotPredicate.class);
    }

    @Override
    public void onMatch(@Nonnull final QueryPredicateComputationRuleCall<EvaluationContext, List<QueryPlanConstraint>> call) {
        final var bindings = call.getBindings();
        final var value = bindings.get(anyValueMatcher);
        final var comparison = bindings.get(anyComparisonMatcher);

        final var invertedComparisonType = Comparisons.invertComparisonType(comparison.getType());
        if (invertedComparisonType == null) {
            return;
        }

        call.yieldPredicate(new ValuePredicate(value, comparison.withType(invertedComparisonType)), ImmutableList.of(QueryPlanConstraint.noConstraint()));
    }
}
