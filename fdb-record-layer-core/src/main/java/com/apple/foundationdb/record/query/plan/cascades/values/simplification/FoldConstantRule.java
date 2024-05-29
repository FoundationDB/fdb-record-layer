/*
 * FoldConstantRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.values.ConstantValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.Optional;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.anyValue;

/**
 * A rule that detects a value that evaluates to one constant value given the a set of aliases that are considered
 * being bound to constant objects.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class FoldConstantRule extends ValueSimplificationRule<Value> {
    @Nonnull
    private static final BindingMatcher<Value> rootMatcher = anyValue();

    public FoldConstantRule() {
        super(rootMatcher);
    }

    @Nonnull
    @Override
    public Optional<Class<?>> getRootOperator() {
        return Optional.empty(); // needs to go into the alwaysRules
    }

    @Override
    public void onMatch(@Nonnull final ValueSimplificationRuleCall call) {
        final var bindings = call.getBindings();
        final var root = bindings.get(rootMatcher);

        if (root instanceof ConstantValue) {
            return;
        }
        
        final var constantAliases = call.getConstantAliases();

        if (constantAliases.containsAll(root.getCorrelatedTo())) {
            // loop through the children of root to eliminate constant values in order to lift the constant value
            final var newChildren = ImmutableList.<Value>builder();
            for (final var child : root.getChildren()) {
                if (child instanceof ConstantValue) {
                    newChildren.add(((ConstantValue)child).getValue());
                } else {
                    newChildren.add(child);
                }
            }

            call.yieldResult(new ConstantValue(root.withChildren(newChildren.build())));
        }
    }
}
