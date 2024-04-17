/*
 * MatchOrCompensateFieldValueRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * A rule that matches a {@link QuantifiedObjectValue} (with the argument values). If the argument is a
 * {@link FieldValue} on top of a semantically equal {@link QuantifiedObjectValue}, we match and create a compensation
 * that if applied injects the field path of the {@link FieldValue}.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class MatchOrCompensateQuantifiedObjectValueRule extends ValueComputationRule<Iterable<? extends Value>, Map<Value, Function<Value, Value>>, QuantifiedObjectValue> {
    @Nonnull
    private static final BindingMatcher<QuantifiedObjectValue> rootMatcher =
            ValueMatchers.quantifiedObjectValue();

    public MatchOrCompensateQuantifiedObjectValueRule() {
        super(rootMatcher);
    }

    @Override
    public void onMatch(@Nonnull final ValueComputationRuleCall<Iterable<? extends Value>, Map<Value, Function<Value, Value>>> call) {
        final var bindings = call.getBindings();
        final var quantifiedObjectValue = bindings.get(rootMatcher);
        final var toBePulledUpValues = Objects.requireNonNull(call.getArgument());
        final var equivalenceMap = call.getEquivalenceMap();

        final var newMatchedValuesMap = new LinkedIdentityMap<Value, Function<Value, Value>>();

        for (final var toBePulledUpValue : toBePulledUpValues) {
            if (toBePulledUpValue instanceof FieldValue) {
                final var toBePulledUpFieldValue = (FieldValue)toBePulledUpValue;

                if (quantifiedObjectValue.semanticEquals(toBePulledUpFieldValue.getChild(), equivalenceMap)) {
                    newMatchedValuesMap.put(toBePulledUpValue, new FieldValueCompensation(toBePulledUpFieldValue.getFieldPath()));
                }
            }
        }
        call.yieldValue(quantifiedObjectValue, newMatchedValuesMap);
    }
}
