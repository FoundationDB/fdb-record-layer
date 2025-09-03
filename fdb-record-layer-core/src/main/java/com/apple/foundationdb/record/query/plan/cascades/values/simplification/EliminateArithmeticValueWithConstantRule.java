/*
 * EliminateArithmeticValueWithConstantRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.cascades.values.ArithmeticValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.anyValue;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.arithmeticValue;

/**
 * A rule that eliminates an arithmetic operation. One of the following conditions have to be met:
 * <ul>
 * <li> this is an addition and one of the operands is constant. </li>
 * <li> TODO this is a multiplication and one of the operands is constant and the constant operand is greater than 0
 * </ul>
 *
 * Note that this simplification rule was implemented to simplify {@link Value}s used for modelling ordering. The
 * simplification employs the idea that {@code ORDER BY x + 5} is equivalent to {@code ORDER BY x}.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class EliminateArithmeticValueWithConstantRule extends ValueSimplificationRule<ArithmeticValue> {
    @Nonnull
    private static final CollectionMatcher<Value> childrenMatcher = all(anyValue());

    @Nonnull
    private static final BindingMatcher<ArithmeticValue> rootMatcher =
            arithmeticValue(childrenMatcher);

    public EliminateArithmeticValueWithConstantRule() {
        super(rootMatcher);
    }

    @Override
    public void onMatch(@Nonnull final ValueSimplificationRuleCall call) {
        final var bindings = call.getBindings();
        final var arithmeticValue = bindings.get(rootMatcher);

        //
        // We can only do this if the arithmetic value is the root
        //
        if (!call.isRoot()) {
            return;
        }
        
        switch (arithmeticValue.getLogicalOperator()) {
            case ADD:
            case SUB:
                break;
            default:
                // TODO MUL/DIV
                return;
        }

        final var childrenCollection = bindings.get(childrenMatcher);
        if (childrenCollection.size() != 2) {
            return;
        }

        final var children = ImmutableList.copyOf(childrenCollection);

        final var constantAliases = call.getConstantAliases();
        if (constantAliases.containsAll(arithmeticValue.getCorrelatedTo())) {
            return;
        }

        final var yieldBuilder = call.yieldResultBuilder();
        if (constantAliases.containsAll(children.get(0).getCorrelatedTo())) {
            // this first child is the constant one, the second child is not
            yieldBuilder.addConstraintsFrom(arithmeticValue, children.get(0))
                    .yieldResult(children.get(1));
        } else if (constantAliases.containsAll(children.get(1).getCorrelatedTo())) {
            // this second child is the constant one, the first child is not
            yieldBuilder.addConstraintsFrom(arithmeticValue, children.get(1))
                    .yieldResult(children.get(0));
        } // else they are both not constant
    }
}
