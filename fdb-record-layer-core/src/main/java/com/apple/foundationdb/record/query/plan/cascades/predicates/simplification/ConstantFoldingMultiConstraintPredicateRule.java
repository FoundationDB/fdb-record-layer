/*
 * ConstantFoldingBooleanPredicateWithRangesRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.PredicateWithValueAndRanges;
import com.apple.foundationdb.record.query.plan.cascades.predicates.RangeConstraints;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.Optional;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ListMatcher.exactly;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.atLeastTwo;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.anyComparison;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.predicateWithValueAndRanges;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.rangeConstraint;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.anyValue;

/**
 * This rule examines {@link PredicateWithValueAndRanges} comprising a single {@link RangeConstraints} with multiple
 * constraints and attempts to fold to a simple {@link ConstantPredicate} if it is feasible to do so, i.e. if the
 * range constraints correspond to certain types of constant comparisons.
 * The following cases are handled:
 * <ul>
 *     <li>{@code FALSE, [NULL, NULL] (AND [NULL, NULL])+  → NULL}</li>
 *     <li>{@code TRUE, [NULL, NULL] (AND [NULL, NULL])+  → NULL}</li>
 *     <li>{@code TRUE = [TRUE, TRUE] (AND [TRUE, TRUE])+  → TRUE}</li>
 *     <li>{@code TRUE = [FALSE, FALSE] (AND [FALSE, FALSE])+ → FALSE}</li>
 *     <li>{@code FALSE = [TRUE, TRUE] (AND [TRUE, TRUE])+ → FALSE}</li>
 *     <li>{@code FALSE = [FALSE, FALSE] (AND [FALSE, FALSE])+ → TRUE}</li>
 *     <li>{@code NULL = <ANYTHING> → NULL}</li>
 *     <li>{@code <ANYTHING> = NULL → NULL}</li>
 *     <li>{@code NULL ≠ [NULL, NULL] (AND [NULL, NULL])+ → NULL}</li>
 *     <li>{@code NULL ≠ [TRUE, TRUE] (AND [TRUE, TRUE])+ → NULL}</li>
 *     <li>{@code NULL ≠ [FALSE, FALSE] (AND [FALSE, FALSE])+ → NULL}</li>
 *     <li>{@code TRUE ≠ [NULL, NULL] (AND [NULL, NULL])+ → NULL}</li>
 *     <li>{@code TRUE ≠ [TRUE, TRUE] (AND [TRUE, TRUE])+ → FALSE}</li>
 *     <li>{@code TRUE ≠ [FALSE, FALSE] (AND [FALSE, FALSE])+ → TRUE}</li>
 *     <li>{@code FALSE ≠ [NULL, NULL] (AND [NULL, NULL])+ → NULL}</li>
 *     <li>{@code FALSE ≠ [TRUE, TRUE] (AND [TRUE, TRUE])+ → TRUE}</li>
 *     <li>{@code FALSE ≠ [FALSE, FALSE] (AND [FALSE, FALSE])+ → FALSE}</li>
 *     <li>{@code NULL, IS_NULL (AND IS_NULL)+ → TRUE}</li>
 *     <li>{@code TRUE, IS_NULL (AND IS_NULL)+ → FALSE}</li>
 *     <li>{@code FALSE, IS_NULL (AND IS_NULL)+ → FALSE}</li>
 *     <li>{@code NOT_NULL, IS_NULL (AND IS_NULL)+  → FALSE}</li>
 *     <li>{@code TRUE IS_NOT_NULL (AND IS_NOT_NULL)+ → TRUE}</li>
 *     <li>{@code FALSE IS_NOT_NULL (AND IS_NOT_NULL)+ → TRUE}</li>
 *     <li>{@code NULL IS_NOT_NULL (AND IS_NOT_NULL)+ → FALSE}</li>
 *     <li>{@code FALSE = ... AND [TRUE, TRUE] AND ... → FALSE} (implied and annulment) </li>
 *     <li>{@code TRUE = ... AND [TRUE, TRUE] AND [FALSE, FALSE] ... → FALSE} and other contradictions leading to disjoint ranges</li>
 *     <li>{@code FALSE = ... AND [NULL, NULL] AND [FALSE, FALSE] ... → NULL} assuming all constraints are either {@code [FALSE, FALSE] } or {@code [NULL, NULL]}</li>
 * </ul>
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class ConstantFoldingMultiConstraintPredicateRule extends QueryPredicateSimplificationRule<PredicateWithValueAndRanges> {

    @Nonnull
    private static final BindingMatcher<RangeConstraints> rangeMatcher = rangeConstraint(atLeastTwo(anyComparison()));

    @Nonnull
    private static final BindingMatcher<Value> comparandMatcher = anyValue();

    @Nonnull
    private static final BindingMatcher<PredicateWithValueAndRanges> rootMatcher = predicateWithValueAndRanges(comparandMatcher, exactly(rangeMatcher));

    public ConstantFoldingMultiConstraintPredicateRule() {
        super(rootMatcher);
    }

    @Override
    public void onMatch(@Nonnull final QueryPredicateSimplificationRuleCall call) {
        final var multiConstraintRange = call.getBindings().get(rangeMatcher);
        final var lhs = call.getBindings().get(comparandMatcher);
        foldConjunctionRangesMaybe(lhs, multiConstraintRange).ifPresent(call::yieldResult);
    }

    @Nonnull
    private Optional<ConstantPredicate> foldConjunctionRangesMaybe(final Value lhs, @Nonnull final RangeConstraints rangeConstraints) {
        // degenerate case, give up.
        if (rangeConstraints.getComparisons().isEmpty()) {
            return Optional.empty();
        }

        final var predicateCategoriesBuilder = ImmutableSet.<ConstantPredicateFoldingUtil.PredicateCategory>builder();
        for (Comparisons.Comparison comparison : rangeConstraints.getComparisons()) {
            predicateCategoriesBuilder.add(ConstantPredicateFoldingUtil.foldComparisonMaybe(lhs, comparison));

        }
        final var predicateCategories = predicateCategoriesBuilder.build();

        // if we have only one distinct range, then that's the one we go with
        if (predicateCategories.size() == 1) {
            return Iterables.getOnlyElement(predicateCategories).getPredicateMaybe();
        }

        // if at least one range is FALSE, the returned result must be FALSE
        if (predicateCategories.contains(ConstantPredicateFoldingUtil.PredicateCategory.SINGLETON_FALSE)) {
            return Optional.of(ConstantPredicate.FALSE);
        }

        // if at least one range is UNKNOWN, give up.
        if (predicateCategories.contains(ConstantPredicateFoldingUtil.PredicateCategory.UNKNOWN)) {
            return Optional.empty();
        }

        // we have multiple disjoint singleton ranges then it is impossible to find a single value satisfying all
        // of them ...

        // ... unless we have a combination of TRUE and NULL then the result is NULL
        if (predicateCategories.stream().noneMatch(p -> p == ConstantPredicateFoldingUtil.PredicateCategory.SINGLETON_FALSE)) {
            // we must have TRUE and NULL
            Verify.verify(predicateCategories.contains(ConstantPredicateFoldingUtil.PredicateCategory.SINGLETON_TRUE));
            Verify.verify(predicateCategories.contains(ConstantPredicateFoldingUtil.PredicateCategory.SINGLETON_NULL));
            return Optional.of(ConstantPredicate.NULL);
        }

        // ... ok range is impossible, bailout.
        return Optional.of(ConstantPredicate.FALSE);
    }
}
