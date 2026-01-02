/*
 * MatchSimpleFieldValueRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.typing.PseudoField;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.fieldValue;

/**
 * A rule that matches any simple {@link FieldValue}, i.e. {@code fieldValue(qov(b))} where {@code qov(b)} is the base
 * value handed in through the arguments to the simplification call.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class MatchSimpleFieldValueRule extends ValueComputationRule<Value, Map<Value, ValueCompensation>, FieldValue> {
    @Nonnull
    private static final BindingMatcher<FieldValue> rootMatcher = fieldValue(ValueMatchers.quantifiedObjectValue());

    public MatchSimpleFieldValueRule() {
        super(rootMatcher);
    }

    @Nonnull
    @Override
    public Optional<Class<?>> getRootOperator() {
        return Optional.of(FieldValue.class);
    }

    @Override
    public void onMatch(@Nonnull final ValueComputationRuleCall<Value, Map<Value, ValueCompensation>> call) {
        final var bindings = call.getBindings();
        final var rootValue = bindings.get(rootMatcher);
        final var baseValue = Objects.requireNonNull(call.getArgument());

        final var resultPair = call.getResult(rootValue);
        final var matchedValuesMap = resultPair == null ? null : resultPair.getRight();
        Verify.verify(matchedValuesMap == null || matchedValuesMap.isEmpty() ||
                (matchedValuesMap.size() == 1 && matchedValuesMap.containsKey(rootValue)));
        if (!rootValue.isFunctionallyDependentOn(baseValue)) {
            return;
        }
        if (rootValue.getResultType().equals(PseudoField.ROW_VERSION.getType())) {
            // We cannot currently copy out versions, at least when this is used to construct
            // the IndexKeyValueToPartialRecord based on an index scan. The reason for this is that
            // we'd need to be able to (if the version is the record's version) copy the version
            // not into a field on the base record but into FDBQueriedRecord's "version" field.
            // If we ever replace the IndexKeyValueToPartialRecord on a covering index plan
            // with an RCV that constructs the record directly from the index entry, we could
            // start to loosen this
            return;
        }

        final var newMatchedValuesMap = new LinkedIdentityMap<Value, ValueCompensation>();
        newMatchedValuesMap.put(rootValue, ValueCompensation.noCompensation());
        call.yieldValue(rootValue, newMatchedValuesMap);
    }
}
