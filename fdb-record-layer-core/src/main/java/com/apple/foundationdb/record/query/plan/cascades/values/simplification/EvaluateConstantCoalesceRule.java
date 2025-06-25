/*
 * EvaluateConstantCoalesceRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.values.LiteralValue;
import com.apple.foundationdb.record.query.plan.cascades.values.NullValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.VariadicFunctionValue;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.coalesceFunction;

/**
 * A rule that evaluates {@code Coalesce} with literal {@code 'True}, {@code 'False}, or {@code 'Null} to their
 * effective coalesced value literal.
 *
 * <ul>
 *     <li>{@code Coalesce(X1, X2, ... Xn, Y, Z1, Z2, .... Zm | Xi ∈ {'Null}, Y ∈ {'True, 'False} Zj ∈ {<ANY VALUE>}) -> Y}</li>
 *     <li>{@code Coalesce(X1, X2, ... Xn, Y1, Y2, ... Ym | Xi ∈ {<ANY VALUE EXCEPT 'null>}, Y ∈ {<ANY VALUE>}) -> Coalesce(X1, X2, ... Xn, Y'1, Y'2, ... Y'k | Y' ∈ {<ANY VALUE EXCEPT 'null>, k < m}}</li>
 *     <li>{@code Coalesce(Y1, Y2, ... Yi, X, Yi+1, Yi+2, ... Yj | X ∈ {<ANY VALUE EXCEPT 'null>}, Y ∈ {'null}) -> X}</li>
 *     <li>{@code Coalesce(Y1, Y2, ... Ym | Y ∈ {'null}) -> 'null}</li>
 * </ul>
 */
public class EvaluateConstantCoalesceRule extends ValueSimplificationRule<VariadicFunctionValue> {

    @Nonnull
    private static final BindingMatcher<VariadicFunctionValue> rootMatcher = coalesceFunction();

    public EvaluateConstantCoalesceRule() {
        super(rootMatcher);
    }

    @Override
    @SuppressWarnings("PMD.AvoidBranchingStatementAsLastInLoop")
    public void onMatch(@Nonnull final ValueSimplificationRuleCall call) {
        final var variadicFunctionValue = call.getBindings().get(rootMatcher);

        final var newChildrenBuilder = ImmutableList.<Value>builder();
        boolean yieldsNewCoalesce = false;
        boolean removeRedundantNulls = false;
        boolean seenOnlyConstantsSoFar = true;
        boolean onlyNulls = true;
        for (final var child : variadicFunctionValue.getChildren()) {
            if (cannotFold(child)) {
                onlyNulls = false;
                removeRedundantNulls = true;
                seenOnlyConstantsSoFar = false;
            } else if (child instanceof NullValue) {
                if (removeRedundantNulls) {
                    yieldsNewCoalesce = true;
                    continue;
                }
            } else {
                onlyNulls = false;
                if (seenOnlyConstantsSoFar) {
                    call.yieldResult(child);
                    return;
                }
            }
            newChildrenBuilder.add(child);
        }

        if (onlyNulls) {
            // all values were null constants => return null.
            call.yieldResult(new NullValue(variadicFunctionValue.getResultType()));
            return;
        }

        if (!yieldsNewCoalesce) {
            return;
        }

        final var newChildren = newChildrenBuilder.build();
        Verify.verify(!newChildren.isEmpty());
        if (newChildren.size() == 1) {
            // degenerate case
            call.yieldResult(newChildren.get(0));
        } else {
            call.yieldResult(variadicFunctionValue.withChildren(newChildren));
        }
    }

    private static boolean cannotFold(@Nonnull final Value value) {
        return !(value instanceof NullValue)
                && !(value.getResultType().isNotNullable() && value instanceof LiteralValue<?>);
    }
}
