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
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.LinkedIdentityMap;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PrimitiveMatchers.anyObject;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.anyValue;

/**
 * A rule that composes a field access and an underlying record construction.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class MatchOrCompensateFieldValueRule extends ValueComputationRule<List<Value>, Map<Value, Function<Value, Value>>, FieldValue> {
    @Nonnull
    private static final CollectionMatcher<Type.Record.Field> fieldPathMatcher = all(anyObject());

    @Nonnull
    private static final BindingMatcher<FieldValue> rootMatcher =
            ValueMatchers.fieldValueWithFieldPath(anyValue(), fieldPathMatcher);

    public MatchOrCompensateFieldValueRule() {
        super(rootMatcher);
    }

    @Override
    public void onMatch(@Nonnull final ValueComputationRuleCall<List<Value>, Map<Value, Function<Value, Value>>> call) {
        final var bindings = call.getBindings();
        final var fieldValue = bindings.get(rootMatcher);

        final var toBePulledUpValues = Objects.requireNonNull(call.getArgument());
        final var valueWithResultFromChild = call.getResult(fieldValue.getChild());
        final var matchedValueMap =
                valueWithResultFromChild == null ? null : valueWithResultFromChild.getResult();

        final var newMatchedValueMap = new LinkedIdentityMap<Value, Function<Value, Value>>();

        for (final var toBePulledUpValue : toBePulledUpValues) {
            if (toBePulledUpValue instanceof FieldValue) {
                if (matchedValueMap == null || !matchedValueMap.containsKey(toBePulledUpValue)) {
                    final var toBePulledUpFieldValue = (FieldValue)toBePulledUpValue;
                    //
                    // If the current field value uses a prefix of the field value we are trying to pull up
                    // (on an equal inValue), then we have found a match. For instance if we are tyring to pull up
                    // $a.x.y.z and the value we are pulling through is b = $a.x.y we can match those with a compensation of
                    // $b.z
                    //
                    final var commonCorrelatedTo = ImmutableSet.copyOf(Sets.union(toBePulledUpFieldValue.getCorrelatedTo(), fieldValue.getCorrelatedTo()));

                    if (toBePulledUpFieldValue.semanticEquals(fieldValue, AliasMap.identitiesFor(commonCorrelatedTo))) {
                        final var pathSuffixOptional = FieldValue.stripFieldPrefixMaybe(toBePulledUpFieldValue.getFieldPath(), fieldValue.getFieldPath());
                        pathSuffixOptional.ifPresent(pathSuffix -> newMatchedValueMap.put(toBePulledUpValue, new FieldValueCompensation(pathSuffix)));
                    }
                } else {
                    // there already is a matched field value
                    final var compensation = matchedValueMap.get(toBePulledUpValue);
                    if (compensation instanceof FieldValueCompensation) {
                        final var fieldValueCompensation = (FieldValueCompensation)compensation;
                        final var pathSuffixOptional = FieldValue.stripFieldPrefixMaybe(fieldValueCompensation.getFieldPath(), fieldValue.getFieldPath());
                        pathSuffixOptional.ifPresent(pathSuffix -> newMatchedValueMap.put(toBePulledUpValue, new FieldValueCompensation(pathSuffix)));
                    }
                }
            }
        }
        call.yield(fieldValue, newMatchedValueMap);
    }

    private static class FieldValueCompensation implements Function<Value, Value> {
        @Nonnull
        private final List<Type.Record.Field> fieldPath;

        public FieldValueCompensation(@Nonnull final List<Type.Record.Field> fieldPath) {
            this.fieldPath = ImmutableList.copyOf(fieldPath);
        }

        @Nonnull
        public List<Type.Record.Field> getFieldPath() {
            return fieldPath;
        }

        @Nonnull
        @Override
        public Value apply(final Value value) {
            return FieldValue.ofFieldsAndFuseIfPossible(value, fieldPath);
        }
    }
}
