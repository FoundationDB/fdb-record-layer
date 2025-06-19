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
import com.apple.foundationdb.record.query.plan.cascades.predicates.simplification.ConstantPredicateFoldingUtil.EffectiveConstant;
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
        final var rangeCategoriesBuilder = ImmutableSet.<RangeCategory>builder();
        final var lhsOperand = EffectiveConstant.from(lhs);

        // degenerate case, give up.
        if (rangeConstraints.getComparisons().isEmpty()) {
            return Optional.empty();
        }

        for (final var comparison : rangeConstraints.getComparisons()) {
            if (comparison.getType().isUnary()) {
                if (comparison.getType() == Comparisons.Type.IS_NULL) {
                    switch (lhsOperand) {
                        case NULL:
                            rangeCategoriesBuilder.add(RangeCategory.SingletonTrue);
                            break;
                        case TRUE: // fallthrough
                        case FALSE: // fallthrough
                        case NOT_NULL:
                            rangeCategoriesBuilder.add(RangeCategory.SingletonFalse);
                            break;
                        default:
                            break;
                    }
                } else {
                    rangeCategoriesBuilder.add(RangeCategory.UNKNOWN);
                }
            } else {
                if (!(comparison instanceof Comparisons.ValueComparison)) {
                    rangeCategoriesBuilder.add(RangeCategory.UNKNOWN);
                    continue;
                }
                final var valueComparison = (Comparisons.ValueComparison)comparison;
                final var rhsOperand = EffectiveConstant.from(valueComparison.getValue());
                switch (comparison.getType()) {
                    case EQUALS:
                        if (lhsOperand == EffectiveConstant.NULL || rhsOperand == EffectiveConstant.NULL) {
                            rangeCategoriesBuilder.add(RangeCategory.SingletonNull);
                            break;
                        } else if (lhsOperand.equals(rhsOperand)) {
                            rangeCategoriesBuilder.add(RangeCategory.SingletonTrue);
                            break;
                        } else {
                            rangeCategoriesBuilder.add(RangeCategory.SingletonFalse);
                            break;
                        }
                    case NOT_EQUALS:
                        if (lhsOperand == EffectiveConstant.NULL || rhsOperand == EffectiveConstant.NULL) {
                            rangeCategoriesBuilder.add(RangeCategory.SingletonNull);
                            break;
                        } else if (!lhsOperand.equals(rhsOperand)) {
                            rangeCategoriesBuilder.add(RangeCategory.SingletonTrue);
                            break;
                        } else {
                            rangeCategoriesBuilder.add(RangeCategory.SingletonFalse);
                            break;
                        }
                    default:
                        rangeCategoriesBuilder.add(RangeCategory.UNKNOWN);
                }
            }
        }
        // if we have more than one distinct range then it is impossible to find a single value that satisfies all of them
        final var rangeCategories = rangeCategoriesBuilder.build();

        // if at least one range is FALSE, the returned result must be FALSE
        if (rangeCategories.contains(RangeCategory.SingletonFalse)) {
            return Optional.of(ConstantPredicate.FALSE);
        }

        // if at least one range is UNKNOWN, give up.
        if (rangeCategories.contains(RangeCategory.UNKNOWN)) {
            return Optional.empty();
        }

        if (rangeCategories.size() == 1) {
            final var singleRangeCategory = Iterables.getOnlyElement(rangeCategories);
            switch (singleRangeCategory) {
                case SingletonTrue:
                    return Optional.of(ConstantPredicate.TRUE);
                case SingletonFalse:
                    return Optional.of(ConstantPredicate.FALSE);
                case SingletonNull:
                    return Optional.of(ConstantPredicate.NULL);
                default:
                    return Optional.empty();
            }
        }

        // we have multiple disjoint singleton ranges then it is impossible to find a single value satisfying all
        // of them ...

        // ... unless we have a combination of TRUE and NULL then the result is NULL
        if (rangeCategories.stream().noneMatch(p -> p == RangeCategory.SingletonFalse)) {
            // we must have TRUE and NULL
            Verify.verify(rangeCategories.contains(RangeCategory.SingletonTrue));
            Verify.verify(rangeCategories.contains(RangeCategory.SingletonNull));
            return Optional.of(ConstantPredicate.NULL);
        }

        // ... ok range is impossible, bailout.
        return Optional.of(ConstantPredicate.FALSE);
    }

    private enum RangeCategory {
        SingletonNull,
        SingletonTrue,
        SingletonFalse,
        UNKNOWN
    }
}
