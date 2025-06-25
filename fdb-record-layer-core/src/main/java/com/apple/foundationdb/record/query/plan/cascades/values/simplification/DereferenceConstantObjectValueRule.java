/*
 * DereferenceConstantObjectValueRule.java
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

package com.apple.foundationdb.record.query.plan.cascades.values.simplification;

import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.anyConstantObjectValue;

/**
 * This rule replaces specific instances of {@link ConstantObjectValue} with their equivalent {@link LiteralValue}
 * representations.  The following cases are currently handled:
 * <ul>
 *    <li>Boolean constant object values are represented as {@link LiteralValue}s.</li>
 *    <li>{@code Null} constant object values are represented as null values.</li>
 *    <li>Non-null constant object values may be represented as a new {@link ConstantObjectValue} instance with a
 * non-nullable type.</li>
 * </ul>
 */
public class DereferenceConstantObjectValueRule extends ValueSimplificationRule<ConstantObjectValue> {

    @Nonnull
    private static final BindingMatcher<ConstantObjectValue> rootMatcher = anyConstantObjectValue();

    public DereferenceConstantObjectValueRule() {
        super(rootMatcher);
    }

    @Override
    public void onMatch(@Nonnull final ValueSimplificationRuleCall call) {
        final var constantObjectValue = call.getBindings().get(rootMatcher);
        final var evaluationContext = call.getEvaluationContext();
        if (!evaluationContext.containsConstantBinding(constantObjectValue.getAlias(), constantObjectValue.getConstantId())) {
            //
            // We don't have access to the constant. This can happen when trying to plan a statement
            // that has not been fully prepared (that is, one is trying to generate a plan that is independent
            // of particular constant values). In this case, we just leave the value alone
            //
            return;
        }
        final var plainValue = constantObjectValue.evalWithoutStore(evaluationContext);

        if (plainValue == null) {
            Verify.verify(constantObjectValue.getResultType().isNullable());
            call.yieldResult(new NullValue(constantObjectValue.getResultType()));
            return;
        }
        final var nonNullType = constantObjectValue.getResultType().withNullability(false);
        if (constantObjectValue.getResultType().getTypeCode() == Type.TypeCode.BOOLEAN) {
            Verify.verify(plainValue instanceof Boolean);
            final var result = LiteralValue.ofScalar(plainValue).with(nonNullType);
            call.yieldResult(result);
            return;
        }

        constantObjectValue.overrideTypeMaybe(nonNullType).ifPresent(call::yieldResult);
    }
}
