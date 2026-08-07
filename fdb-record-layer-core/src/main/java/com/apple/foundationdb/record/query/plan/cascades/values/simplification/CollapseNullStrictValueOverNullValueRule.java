/*
 * CollapseNullStrictValueOverNullValueRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcherWithPredicate.typedMatcherWithPredicate;

/**
 * A rule that collapses a {@link Value} into a {@link NullValue} if it is strictly null-propagating and has at least
 * one child that is a {@link NullValue}. A {@code Value} class is <em>strictly null-propagating</em> if it produces a
 * null result when any child evaluates to null. For example, arithmetic operators ({@link ArithmeticValue}) yield null
 * when either operand is null.
 *
 * <p>The rule is parameterized over the concrete, strictly null-propagating {@link Value} subclass. This way it can use
 * a typed matcher for the specific class and benefit from the rule index in the rule set.
 *
 * @param <V> the {@link Value} subclass that this rule instance matches
 */
@API(API.Status.EXPERIMENTAL)
public final class CollapseNullStrictValueOverNullValueRule<V extends Value> extends ValueSimplificationRule<V> {

    @Nonnull
    private final BindingMatcher<V> rootMatcher;

    public CollapseNullStrictValueOverNullValueRule(@Nonnull final Class<V> valueClass) {
        this(typedMatcherWithPredicate(valueClass, CollapseNullStrictValueOverNullValueRule::hasNullValueChild));
    }

    private CollapseNullStrictValueOverNullValueRule(@Nonnull final BindingMatcher<V> rootMatcher) {
        super(rootMatcher);
        this.rootMatcher = rootMatcher;
    }

    @Override
    public void onMatch(@Nonnull final ValueSimplificationRuleCall call) {
        final V value = call.getBindings().get(rootMatcher);
        // Note that the `NullValue` will always have a nullable result type even if the value’s result type is not.
        call.yieldResult(new NullValue(value.getResultType()));
    }

    private static boolean hasNullValueChild(@Nonnull final Value value) {
        for (final Value child : value.getChildren()) {
            if (child instanceof NullValue) {
                return true;
            }
        }
        return false;
    }
}
