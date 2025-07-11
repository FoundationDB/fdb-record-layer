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
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.PromoteValue;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.anyPromoteValue;

/**
 * A rule that evaluates constant {@link PromoteValue} immediately. Specifically:
 *
 * <ul>
 *     <li>{@code Promote(NullValue, TypeXYZ) -> NullValue of type TypeXYZ}</li>
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
        final var promoteValue = call.getBindings().get(rootMatcher);
        final var childValue = promoteValue.getChild();

        if (childValue instanceof NullValue) {
            call.yieldResult(childValue.with(promoteValue.getResultType()));
            return;
        }

        if (childValue.getResultType().nullable().equals(promoteValue.getResultType().nullable())) {

            // both types are the same, either type (but not both) must be not nullable, otherwise the promotion would
            // not be necessary.
            Verify.verify(childValue.getResultType().isNullable() != promoteValue.getResultType().isNullable());

            // Returns the child value with its original non-nullable type, overriding any nullable type requested by promotion.
            // This intentional restriction is acceptable because it allows for subsequent simplifications and optimizations.
            // For example, IsNull(Promote('42L, NullableLong)) can be simplified to 'False' due to this type restriction.
            if (childValue.getResultType().isNotNullable()) {
                call.yieldResult(childValue);
            } else {
                childValue.overrideTypeMaybe(promoteValue.getResultType()).ifPresent(call::yieldResult);
            }
        }
    }
}
