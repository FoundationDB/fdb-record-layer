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
import com.apple.foundationdb.record.query.plan.cascades.values.CastValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NotValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.PromoteValue;
import com.apple.foundationdb.record.query.plan.cascades.values.SubscriptValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.Optional;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.TypedMatcherWithPredicate.typedMatcherWithPredicate;

/**
 * A rule that collapses a {@link Value} into a {@link NullValue} if it is strictly null-propagating and has at least
 * one child that is a {@link NullValue}. A {@code Value} class is <em>strictly null-propagating</em> if it produces a
 * null result when any child evaluates to null. For example, arithmetic operators ({@link ArithmeticValue}) yield null
 * when either operand is null. For the particular classes that are considered by this rule, see {@link #VALUE_CLASSES}.
 */
@API(API.Status.EXPERIMENTAL)
public class CollapseNullStrictValueOverNullValueRule extends ValueSimplificationRule<Value> {

    /**
     * {@link Value} subclasses that are considered to be strictly null-propagating by this rule.
     */
    @Nonnull
    private static final ImmutableSet<Class<? extends Value>> VALUE_CLASSES = ImmutableSet.of(
            ArithmeticValue.class,
            CastValue.class,
            FieldValue.class,
            NotValue.class,
            PromoteValue.class,
            SubscriptValue.class);

    @Nonnull
    private static final BindingMatcher<Value> rootMatcher = typedMatcherWithPredicate(Value.class,
            v -> VALUE_CLASSES.contains(v.getClass()) && hasNullValueChild(v));

    public CollapseNullStrictValueOverNullValueRule() {
        super(rootMatcher);
    }

    @Nonnull
    @Override
    public Optional<Class<?>> getRootOperator() {
        return Optional.empty();
    }

    @Override
    public void onMatch(@Nonnull final ValueSimplificationRuleCall call) {
        // Note that the `NullValue` will always have a nullable result type even if the value’s result type is not.
        call.yieldResult(new NullValue(call.getBindings().get(rootMatcher).getResultType()));
    }

    private static boolean hasNullValueChild(@Nonnull final Value value) {
        return Streams.stream(value.getChildren()).anyMatch(NullValue.class::isInstance);
    }
}
