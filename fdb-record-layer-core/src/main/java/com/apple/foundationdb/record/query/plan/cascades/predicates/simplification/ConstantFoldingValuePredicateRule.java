/*
 * ConstantFoldingBooleanLiteralsRule.java
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

package com.apple.foundationdb.record.query.plan.cascades.predicates.simplification;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.anyComparison;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.valuePredicate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.anyValue;

/**
 * Simplifies a specific form of constant {@link ValuePredicate}. The following simplifications are performed:
 *
 * <ul>
 *     <li>{@code NULL = NULL  → NULL}</li>
 *     <li>{@code NULL = TRUE  → NULL}</li>
 *     <li>{@code NULL = FALSE → NULL}</li>
 *     <li>{@code TRUE = NULL  → NULL}</li>
 *     <li>{@code TRUE = TRUE  → TRUE}</li>
 *     <li>{@code TRUE = FALSE → FALSE}</li>
 *     <li>{@code FALSE = NULL → NULL}</li>
 *     <li>{@code FALSE = TRUE  → FALSE}</li>
 *     <li>{@code FALSE = FALSE → TRUE}</li>
 *     <li>{@code NULL ≠ NULL  → NULL}</li>
 *     <li>{@code NULL ≠ TRUE  → NULL}</li>
 *     <li>{@code NULL ≠ FALSE → NULL}</li>
 *     <li>{@code TRUE ≠ NULL  → NULL}</li>
 *     <li>{@code TRUE ≠ TRUE  → FALSE}</li>
 *     <li>{@code TRUE ≠ FALSE → TRUE}</li>
 *     <li>{@code FALSE ≠ NULL → NULL}</li>
 *     <li>{@code FALSE ≠ TRUE  → TRUE}</li>
 *     <li>{@code FALSE ≠ FALSE → FALSE}</li>
 *     <li>{@code NOT_NULL IS NULL  → FALSE}</li>
 *     <li>{@code TRUE IS NULL → FALSE}</li>
 *     <li>{@code FALSE IS NULL → FALSE}</li>
 *     <li>{@code NULL IS NULL → TRUE}</li>
 *     <li>{@code TRUE IS NOT NULL → TRUE}</li>
 *     <li>{@code FALSE IS NOT NULL → TRUE}</li>
 *     <li>{@code NULL IS NOT NULL → FALSE}</li>
 * </ul>
 */
@API(API.Status.EXPERIMENTAL)
public class ConstantFoldingValuePredicateRule extends QueryPredicateSimplificationRule<ValuePredicate> implements ConstantPredicateFoldingTrait {

    @Nonnull
    private static final BindingMatcher<Comparisons.Comparison> comparisonMatcher = anyComparison();

    @Nonnull
    private static final BindingMatcher<Value> comparandMatcher = anyValue();

    @Nonnull
    private static final BindingMatcher<ValuePredicate> rootMatcher = valuePredicate(comparandMatcher, comparisonMatcher);

    public ConstantFoldingValuePredicateRule() {
        super(rootMatcher);
    }

    @Override
    public void onMatch(@Nonnull final QueryPredicateSimplificationRuleCall call) {
        final var root = call.getBindings().get(rootMatcher);
        final var comparison = call.getBindings().get(comparisonMatcher);
        final var lhsValue = root.getValue();
        foldComparisonMaybe(lhsValue, comparison).ifPresent(call::yieldResult);
    }
}
