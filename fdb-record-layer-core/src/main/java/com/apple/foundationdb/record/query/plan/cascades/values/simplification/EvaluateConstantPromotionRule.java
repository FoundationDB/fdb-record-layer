/*
 * EvaluateConstantPromotionRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.PromoteValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.anyPromoteValue;

/**
 * A rule that evaluates constant {@link PromoteValue} immediately. Specifically:
 *
 * <ul>
 *     <li>{@code Promote(NullValue, TypeXYZ) -> NullValue of type TypeXYZ}</li>
 *     <li>{@code Promote('[] untyped empty array, ArrayType<T>) -> '[] LightArrayConstructorValue of element type T}</li>
 *     <li>{@code Promote('True, T | T is BooleanType) -> 'True of T}</li>
 *     <li>{@code Promote('False, T | T is BooleanType) -> 'False of T}</li>
 *     <li>{@code Promote('Null, T | T is BooleanType) -> 'Null of T}</li>
 * </ul>
 */
public class EvaluateConstantPromotionRule extends ValueSimplificationRule<PromoteValue> {

    @Nonnull
    private static final BindingMatcher<PromoteValue> rootMatcher = anyPromoteValue();

    public EvaluateConstantPromotionRule() {
        super(rootMatcher);
    }

    @Override
    public void onMatch(@Nonnull final ValueSimplificationRuleCall call) {
        final PromoteValue promoteValue = call.getBindings().get(rootMatcher);
        final Type promoteType = promoteValue.getResultType();
        final Value value = promoteValue.getChild();
        final Type type = value.getResultType();

        // Case 1: NULL value
        if (value instanceof NullValue) {
            call.yieldResult(value.with(promoteType));
            return;
        }

        // Case 2: Untyped empty array constructor []
        if (type.isNone()) {
            // Yield a typed empty array of the desired element type.
            Verify.verify(promoteType.isArray());
            call.yieldResult(value.with(promoteType));
            return;
        }

        // Case 3: A value of the desired type that differs only in nullability.
        if (type.nullable().equals(promoteType.nullable())) {
            // The types must differ in nullability; otherwise the promotion would not be needed in the first place.
            Verify.verify(type.isNullable() != promoteType.isNullable());

            // Ignore a promotion from not-nullable to nullable. This is to facilitate subsequent simplifications.
            // For example, `IsNull(Promote('42L, NullableLong))` could subsequently be simplified to 'False.
            if (!type.isNullable()) {
                call.yieldResult(value);
                return;
            }

            if (value.canResultInType(promoteType)) {
                call.yieldResult(value.with(promoteType));
                return;
            }
        }
    }
}
