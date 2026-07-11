/*
 * ComposeFieldValueOverFieldValueRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.PlannerBindings;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.anyValue;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.fieldValue;

/**
 * A rule that composes a field access and an underlying field access to a concatenated field access.
 *
 * <p>{@code (_.a).b} or more precisely {@code FieldValue(FieldValue(_, "a"), "b")} is transformed to {@code _.a.b} or
 * {@code FieldValue(_, ["a", "b"])}.
 *
 * <p>Note that this rule is the conceptual opposite of {@link ExpandFusedFieldValueRule}. These rules should not be placed
 * into the same rule set as the effect of it is undefined and may cause a stack overflow.
 */
@API(API.Status.EXPERIMENTAL)
public class ComposeFieldValueOverFieldValueRule extends ValueSimplificationRule<FieldValue> {
    @Nonnull
    private static final BindingMatcher<Value> innerChildMatcher = anyValue();
    @Nonnull
    private static final BindingMatcher<FieldValue> innerFieldValueMatcher = fieldValue(innerChildMatcher);
    @Nonnull
    private static final BindingMatcher<FieldValue> rootMatcher = fieldValue(innerFieldValueMatcher);

    public ComposeFieldValueOverFieldValueRule() {
        super(rootMatcher);
    }

    @Override
    public void onMatch(@Nonnull final ValueSimplificationRuleCall call) {
        final PlannerBindings bindings = call.getBindings();
        final FieldValue outer = bindings.get(rootMatcher);
        final FieldValue inner = bindings.get(innerFieldValueMatcher);
        final Value innerChild = bindings.get(innerChildMatcher);
        Verify.verify(!outer.getFieldPath().isEmpty());
        Verify.verify(!inner.getFieldPath().isEmpty());
        call.yieldResultBuilder()
                .addConstraintsFrom(outer, inner)
                .yieldResult(FieldValue.ofFields(innerChild,
                        inner.getFieldPath().withSuffix(outer.getFieldPath())));
    }
}
