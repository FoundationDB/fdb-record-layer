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

import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ConstantPredicate;
import com.apple.foundationdb.record.query.plan.cascades.predicates.ValuePredicate;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.anyComparison;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.QueryPredicateMatchers.valuePredicate;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.anyValue;

public class ConstantFoldingBooleanLiteralsRule extends QueryPredicateSimplificationRule<ValuePredicate> {

    @Nonnull
    private static final BindingMatcher<Comparisons.Comparison> comparisonMatcher = anyComparison();

    @Nonnull
    private static final BindingMatcher<Value> comparandMatcher = anyValue();

    @Nonnull
    private static final BindingMatcher<ValuePredicate> rootMatcher = valuePredicate(comparandMatcher, comparisonMatcher);

    public ConstantFoldingBooleanLiteralsRule() {
        super(rootMatcher);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onMatch(@Nonnull final QueryPredicateSimplificationRuleCall call) {
        final var root = call.getBindings().get(rootMatcher);
        final var comparison = call.getBindings().get(comparisonMatcher);
        final var comparisonType = comparison.getType();
        final var lhsValue = root.getValue();

        final var lhsOperand = lhsValue.evalWithoutStore(call.getEvaluationContext());

        if (comparisonType.isUnary()) {
            switch (comparisonType) {
                case IS_NULL:
                    if (lhsOperand == null) {
                        call.yieldResult(ConstantPredicate.TRUE);
                    } else {
                        call.yieldResult(ConstantPredicate.FALSE);
                    }
                    break;
                case NOT_NULL:
                    if (lhsOperand == null) {
                        call.yieldResult(ConstantPredicate.FALSE);
                    } else {
                        call.yieldResult(ConstantPredicate.TRUE);
                    }
                    break;
                default:
                    return;
            }
        }

        if (!(comparison instanceof Comparisons.ValueComparison)) {
            return;
        }

        final var valueComparison = (Comparisons.ValueComparison)comparison;
        final var rhsValue = valueComparison.getValue();

        if (rhsValue instanceof NullValue ||
                rhsValue.getResultType().getTypeCode() == Type.TypeCode.BOOLEAN && rhsValue instanceof LiteralValue<?>) {

            final Object rhsOperand;
            if (rhsValue instanceof NullValue) {
                rhsOperand = null;
            } else {
                rhsOperand = ((LiteralValue<Boolean>)rhsValue).getLiteralValue();
            }

            switch (comparisonType) {
                case EQUALS:
                    if (lhsOperand == null || rhsOperand == null) {
                        call.yieldResult(ConstantPredicate.NULL);
                    } else if (lhsOperand == rhsOperand) {
                        call.yieldResult(ConstantPredicate.TRUE);
                    } else {
                        call.yieldResult(ConstantPredicate.FALSE);
                    }
                    break;
                case NOT_EQUALS:
                    if (lhsOperand == null || rhsOperand == null) {
                        call.yieldResult(ConstantPredicate.NULL);
                    } else if (lhsOperand != rhsOperand) {
                        call.yieldResult(ConstantPredicate.TRUE);
                    } else {
                        call.yieldResult(ConstantPredicate.FALSE);
                    }
                    break;
                default:
                    break;
            }
        }
    }
}
