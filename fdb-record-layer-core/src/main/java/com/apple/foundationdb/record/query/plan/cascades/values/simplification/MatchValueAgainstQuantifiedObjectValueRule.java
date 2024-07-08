/*
 * MatchValueAgainstQuantifiedObjectValueRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentityMap;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * A rule that matches a {@link Value} against the current {@link QuantifiedObjectValue}. If the argument is a
 * {@link Value} that is semantically equal to the current {@link Value}, we match and create a compensation that
 * rebases the matched {@link Value}.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class MatchValueAgainstQuantifiedObjectValueRule extends ValueComputationRule<Iterable<? extends Value>, Map<Value, Function<Value, Value>>, QuantifiedObjectValue> {
    @Nonnull
    private static final BindingMatcher<QuantifiedObjectValue> rootMatcher =
            ValueMatchers.quantifiedObjectValue();

    public MatchValueAgainstQuantifiedObjectValueRule() {
        super(rootMatcher);
    }

    @Override
    public void onMatch(@Nonnull final ValueComputationRuleCall<Iterable<? extends Value>, Map<Value, Function<Value, Value>>> call) {
        if (!call.isRoot()) {
            return;
        }

        final var bindings = call.getBindings();
        final var quantifiedObjectValue = bindings.get(rootMatcher);
        final var toBePulledUpValues = Objects.requireNonNull(call.getArgument());
        final var resultPairFromChild = call.getResult(quantifiedObjectValue);
        final var matchedValuesMap =
                resultPairFromChild == null ? null : resultPairFromChild.getRight();

        final var newMatchedValuesMap = new LinkedIdentityMap<Value, Function<Value, Value>>();

        for (final var toBePulledUpValue : toBePulledUpValues) {
            if (toBePulledUpValue instanceof FieldValue ||
                    toBePulledUpValue instanceof QuantifiedObjectValue) {
                inheritMatchedMapEntry(matchedValuesMap, newMatchedValuesMap, toBePulledUpValue);
                continue;
            }

            final var correlatedTo = toBePulledUpValue.getCorrelatedTo();
            if (correlatedTo.isEmpty()) {
                // there is a rule for constants
                inheritMatchedMapEntry(matchedValuesMap, newMatchedValuesMap, toBePulledUpValue);
                continue;
            }

            if (correlatedTo.size() > 1) {
                continue;
            }

            final var alias = Iterables.getOnlyElement(correlatedTo);

            newMatchedValuesMap.put(toBePulledUpValue, value -> {
                Verify.verify(value instanceof QuantifiedValue);
                final var newUpperBaseAlias = ((QuantifiedValue)value).getAlias();
                return toBePulledUpValue.rebase(AliasMap.ofAliases(alias, newUpperBaseAlias));
            });
        }
        call.yieldValue(quantifiedObjectValue, newMatchedValuesMap);
    }

    private static void inheritMatchedMapEntry(@Nullable final Map<Value, Function<Value, Value>> matchedValuesMap,
                                               @Nonnull final Map<Value, Function<Value, Value>> newMatchedValuesMap,
                                               @Nonnull final Value toBePulledUpValue) {
        if (matchedValuesMap != null && matchedValuesMap.containsKey(toBePulledUpValue)) {
            newMatchedValuesMap.put(toBePulledUpValue, matchedValuesMap.get(toBePulledUpValue));
        }
    }
}
