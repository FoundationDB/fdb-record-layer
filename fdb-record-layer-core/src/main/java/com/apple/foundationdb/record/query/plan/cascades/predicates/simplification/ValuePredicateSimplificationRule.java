/*
 * ValuePredicateSimplificationRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.Set;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.predicateWithValue;

/**
 * Simplifies {@link Value} instances within a {@link PredicateWithValue}.
 * The actual value simplification is delegated to the {@link Value#simplify(EvaluationContext, AliasMap, Set)} method.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class ValuePredicateSimplificationRule extends QueryPredicateSimplificationRule<PredicateWithValue> {

    @Nonnull
    private static final BindingMatcher<PredicateWithValue> rootMatcher = predicateWithValue();

    public ValuePredicateSimplificationRule() {
        super(rootMatcher);
    }

    @Nonnull
    @Override
    public Optional<Class<?>> getRootOperator() {
        return Optional.empty();
    }

    @Override
    public void onMatch(@Nonnull final QueryPredicateSimplificationRuleCall call) {
        final var predicateWithValue = call.getBindings().get(rootMatcher);
        final var simplifiedPredicateMaybe = predicateWithValue.translateValueAndComparisonsMaybe(
                value -> Optional.of(value.simplify(call.getEvaluationContext(), call.getEquivalenceMap(), call.getConstantAliases())),
                comparison -> {
                    if (comparison instanceof Comparisons.ValueComparison) {
                        final var comparisonType = comparison.getType();
                        if (!comparisonType.isUnary()) {
                            final var simplifiedOperand = comparison.getValue().simplify(call.getEvaluationContext(),
                                    call.getEquivalenceMap(), call.getConstantAliases());
                            return Optional.of(comparison.withValue(simplifiedOperand));
                        }
                    }
                    return Optional.of(comparison);
                }
        );
        if (simplifiedPredicateMaybe.isPresent() && simplifiedPredicateMaybe.get() != predicateWithValue) {
            call.yieldResult(simplifiedPredicateMaybe.get());
        }
    }
}
