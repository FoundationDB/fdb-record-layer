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
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentityMap;
import com.apple.foundationdb.record.query.plan.cascades.Quantifier;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.QuantifiedObjectValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.translation.TranslationMap;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;

import javax.annotation.Nonnull;
import java.util.Objects;

/**
 * A rule that matches a {@link Value} against the current {@link QuantifiedObjectValue}. If the argument is a
 * {@link Value} that is semantically equal to the current {@link Value}, we match and create a compensation that
 * re-bases the matched {@link Value}.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class MatchValueAgainstQuantifiedObjectValueRule extends ValueComputationRule<Iterable<? extends Value>, ListMultimap<Value, ValueCompensation>, QuantifiedObjectValue> {
    @Nonnull
    private static final BindingMatcher<QuantifiedObjectValue> rootMatcher =
            ValueMatchers.quantifiedObjectValue();

    public MatchValueAgainstQuantifiedObjectValueRule() {
        super(rootMatcher);
    }

    @Override
    public void onMatch(@Nonnull final ValueComputationRuleCall<Iterable<? extends Value>, ListMultimap<Value, ValueCompensation>> call) {
        final var bindings = call.getBindings();
        final var quantifiedObjectValue = bindings.get(rootMatcher);
        final var toBePulledUpValues = Objects.requireNonNull(call.getArgument());
        final var resultPairFromChild = call.getResult(quantifiedObjectValue);
        final var matchedValuesMap =
                resultPairFromChild == null
                ? ImmutableListMultimap.<Value, ValueCompensation>of()
                : resultPairFromChild.getRight();

        final var newMatchedValuesMap =
                Multimaps.<Value, ValueCompensation>newListMultimap(new LinkedIdentityMap<>(), Lists::newArrayList);

        for (final var toBePulledUpValue : toBePulledUpValues) {
            if (toBePulledUpValue instanceof FieldValue ||
                    toBePulledUpValue instanceof QuantifiedObjectValue) {
                // MatchValueRule or MatchOrCompensateFieldValueRule are responsible for this
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

            if (!alias.equals(Quantifier.current()) && !alias.equals(quantifiedObjectValue.getAlias())) {
                continue;
            }

            newMatchedValuesMap.put(toBePulledUpValue,
                    (value -> {
                        final var translationMapBuilder = TranslationMap.regularBuilder();
                        translationMapBuilder.when(alias).then(((sourceAlias, leafValue) -> value));
                        return toBePulledUpValue.translateCorrelations(translationMapBuilder.build());
                    }));
        }
        call.yieldValue(quantifiedObjectValue, newMatchedValuesMap);
    }

    private static void inheritMatchedMapEntry(@Nonnull final ListMultimap<Value, ValueCompensation> matchedValuesMap,
                                               @Nonnull final ListMultimap<Value, ValueCompensation> newMatchedValuesMap,
                                               @Nonnull final Value toBePulledUpValue) {
        if (matchedValuesMap.containsKey(toBePulledUpValue)) {
            newMatchedValuesMap.putAll(toBePulledUpValue, matchedValuesMap.get(toBePulledUpValue));
        }
    }
}
