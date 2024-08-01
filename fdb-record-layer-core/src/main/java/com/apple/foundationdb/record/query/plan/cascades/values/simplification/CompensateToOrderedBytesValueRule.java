/*
 * MatchValueRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.values.FromOrderedBytesValue;
import com.apple.foundationdb.record.query.plan.cascades.values.ToOrderedBytesValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Optional;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.anyValue;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.toOrderedBytesValue;

/**
 * A rule that matches a {@link Value} (with the argument values).
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class CompensateToOrderedBytesValueRule extends ValueComputationRule<Value, Map<Value, ValueCompensation>, ToOrderedBytesValue> {
    @Nonnull
    private static final BindingMatcher<Value> childFieldMatcher = anyValue();
    @Nonnull
    private static final BindingMatcher<ToOrderedBytesValue> rootMatcher = toOrderedBytesValue(childFieldMatcher);

    public CompensateToOrderedBytesValueRule() {
        super(rootMatcher);
    }

    @Nonnull
    @Override
    public Optional<Class<?>> getRootOperator() {
        return Optional.of(ToOrderedBytesValue.class);
    }

    @Override
    public void onMatch(@Nonnull final ValueComputationRuleCall<Value, Map<Value, ValueCompensation>> call) {
        final var bindings = call.getBindings();
        final var rootValue = bindings.get(rootMatcher);
        final var childValue = bindings.get(childFieldMatcher);

        final var resultPair = call.getResult(rootValue);
        final var matchedValuesMap = resultPair == null ? null : resultPair.getRight();
        if (matchedValuesMap != null && !matchedValuesMap.isEmpty()) {
            return;
        }
        final var childResultPair = call.getResult(childValue);
        final var childMatchedValuesMap = childResultPair == null ? null : childResultPair.getRight();
        if (childMatchedValuesMap == null || childMatchedValuesMap.size() != 1) {
            return;
        }
        final var childEntry = Iterables.getOnlyElement(childMatchedValuesMap.entrySet());

        final var newMatchedValuesMap = new LinkedIdentityMap<Value, ValueCompensation>();
        final var matchedValue = childEntry.getKey();
        final var matchedValueCompensation = childEntry.getValue();
        newMatchedValuesMap.put(matchedValue, value ->
                matchedValueCompensation.compensate(new FromOrderedBytesValue(value, rootValue.getDirection(), childValue.getResultType())));
        call.yieldValue(rootValue, newMatchedValuesMap);
    }
}
