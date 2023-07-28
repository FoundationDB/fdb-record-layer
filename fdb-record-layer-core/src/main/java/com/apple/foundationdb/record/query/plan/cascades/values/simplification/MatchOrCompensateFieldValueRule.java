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
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PrimitiveMatchers.anyObject;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.anyValue;

/**
 * A rule that matches a {@link FieldValue} (with the argument values).
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class MatchOrCompensateFieldValueRule extends ValueComputationRule<Iterable<? extends Value>, Map<Value, Function<Value, Value>>, FieldValue> {
    @Nonnull
    private static final CollectionMatcher<Integer> fieldPathOrdinalsMatcher = all(anyObject());

    @Nonnull
    private static final CollectionMatcher<Type> fieldPathTypesMatcher = all(anyObject());

    @Nonnull
    private static final BindingMatcher<FieldValue> rootMatcher =
            ValueMatchers.fieldValueWithFieldPath(anyValue(), fieldPathOrdinalsMatcher, fieldPathTypesMatcher);

    public MatchOrCompensateFieldValueRule() {
        super(rootMatcher);
    }

    @Override
    public void onMatch(@Nonnull final ValueComputationRuleCall<Iterable<? extends Value>, Map<Value, Function<Value, Value>>> call) {
        final var bindings = call.getBindings();
        final var fieldValue = bindings.get(rootMatcher);

        final var toBePulledUpValues = Objects.requireNonNull(call.getArgument());
        final var resultPairFromChild = call.getResult(fieldValue.getChild());
        final var matchedValuesMap =
                resultPairFromChild == null ? null : resultPairFromChild.getRight();

        final var newMatchedValuesMap = new LinkedIdentityMap<Value, Function<Value, Value>>();

        for (final var toBePulledUpValue : toBePulledUpValues) {
            if (toBePulledUpValue instanceof FieldValue) {
                if (matchedValuesMap == null || !matchedValuesMap.containsKey(toBePulledUpValue)) {
                    final var toBePulledUpFieldValue = (FieldValue)toBePulledUpValue;
                    //
                    // If the current field value uses a prefix of the field value we are trying to pull up
                    // (on an equal inValue), then we have found a match. For instance if we are tyring to pull up
                    // $a.x.y.z and the value we are pulling through is b = $a.x.y we can match those with a compensation of
                    // $b.z
                    //
                    if (fieldValue.getChild().semanticEquals(toBePulledUpFieldValue.getChild(), call.getEquivalenceMap())) {
                        final var pathSuffixOptional = FieldValue.stripFieldPrefixMaybe(toBePulledUpFieldValue.getFieldPath(), fieldValue.getFieldPath());
                        pathSuffixOptional.ifPresent(pathSuffix -> {
                            if (pathSuffix.isEmpty()) {
                                newMatchedValuesMap.put(toBePulledUpValue, Function.identity());
                            } else {
                                newMatchedValuesMap.put(toBePulledUpValue, new FieldValueCompensation(pathSuffix));
                            }
                        });
                    }
                } else {
                    // there already is a matched field value
                    final var compensation = matchedValuesMap.get(toBePulledUpValue);
                    if (compensation instanceof FieldValueCompensation) {
                        final var fieldValueCompensation = (FieldValueCompensation)compensation;
                        final var pathSuffixOptional = FieldValue.stripFieldPrefixMaybe(fieldValueCompensation.getFieldPath(), fieldValue.getFieldPath());
                        pathSuffixOptional.ifPresent(pathSuffix -> newMatchedValuesMap.put(toBePulledUpValue, fieldValueCompensation.withSuffix(pathSuffix)));
                    }
                }
            }
        }
        call.yield(fieldValue, newMatchedValuesMap);
    }

    /**
     * A compensation that utilizes a field access.
     */
    public static class FieldValueCompensation implements Function<Value, Value> {
        @Nonnull
        private final FieldValue.FieldPath fieldPath;

        @Nonnull
        private final Function<Value, Value> downstreamCompensation;

        public FieldValueCompensation(@Nonnull final FieldValue.FieldPath fieldPath) {
            this(fieldPath, Function.identity());
        }

        public FieldValueCompensation(@Nonnull final FieldValue.FieldPath fieldPath, @Nonnull final Function<Value, Value> downstreamCompensation) {
            this.fieldPath = fieldPath;
            this.downstreamCompensation = downstreamCompensation;
        }


        @Nonnull
        public FieldValue.FieldPath getFieldPath() {
            return fieldPath;
        }

        @Nonnull
        @Override
        public Value apply(final Value value) {
            return downstreamCompensation.apply(FieldValue.ofFieldsAndFuseIfPossible(value, fieldPath));
        }

        public Function<Value, Value> withSuffix(@Nonnull final FieldValue.FieldPath suffixFieldPath) {
            if (suffixFieldPath.isEmpty()) {
                return downstreamCompensation;
            }
            return new FieldValueCompensation(suffixFieldPath, downstreamCompensation);
        }
    }
}
