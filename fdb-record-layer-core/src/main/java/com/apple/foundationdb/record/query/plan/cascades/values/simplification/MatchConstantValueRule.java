/*
 * MatchConstantValueRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentityMap;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers;
import com.apple.foundationdb.record.query.plan.cascades.values.AggregateValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A rule that matches any {@link Value} (with the argument values) that is a constant expression.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class MatchConstantValueRule extends ValueComputationRule<Iterable<? extends Value>, Map<Value, PullUpCompensation>, Value> {
    @Nonnull
    private static final BindingMatcher<Value> rootMatcher =
            ValueMatchers.anyValue();

    public MatchConstantValueRule() {
        super(rootMatcher);
    }

    @Nonnull
    @Override
    public Optional<Class<?>> getRootOperator() {
        return Optional.empty(); // this is an always-rule
    }

    @Override
    public void onMatch(@Nonnull final ValueComputationRuleCall<Iterable<? extends Value>, Map<Value, PullUpCompensation>> call) {
        if (!call.isRoot()) {
            return;
        }

        final var bindings = call.getBindings();
        final var value = bindings.get(rootMatcher);
        final var toBePulledUpValues = Objects.requireNonNull(call.getArgument());
        final var newMatchedValuesMap = new LinkedIdentityMap<Value, PullUpCompensation>();
        final var resultPair = call.getResult(value);
        final var matchedValuesMap = resultPair == null ? null : resultPair.getRight();
        if (matchedValuesMap != null) {
            newMatchedValuesMap.putAll(matchedValuesMap);
        }

        final var constantAliases = call.getConstantAliases();
        for (final var toBePulledUpValue : toBePulledUpValues) {
            final var correlatedTo = toBePulledUpValue.getCorrelatedTo();

            if (toBePulledUpValue.preOrderStream()
                    .anyMatch(v -> v instanceof AggregateValue)) {
                continue;
            }

            if (constantAliases.containsAll(correlatedTo)) {
                newMatchedValuesMap.put(toBePulledUpValue, (ignored1, ignored2) -> toBePulledUpValue);
            }
        }
        call.yieldValue(value, newMatchedValuesMap);
    }
}
