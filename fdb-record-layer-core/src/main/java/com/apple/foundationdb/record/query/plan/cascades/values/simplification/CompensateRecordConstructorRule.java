/*
 * CompensateRecordConstructorRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;

import javax.annotation.Nonnull;
import java.util.Map;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.anyValue;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.recordConstructorValue;

/**
 * A rule that computes compensation for a record constructor and all matched values for children of this record
 * constructor.
 * <br>
 * For instance, for a record constructor {@code (_.a as x, _.b)} where {@code _.a} is already matched using some
 * compensation, this rule composes the existing compensation with additional compensation that navigated through
 * the {@code x} accessor.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class CompensateRecordConstructorRule extends ValueComputationRule<Iterable<? extends Value>, Map<Value, PullUpCompensation>, RecordConstructorValue> {
    @Nonnull
    private static final BindingMatcher<RecordConstructorValue> rootMatcher =
            recordConstructorValue(all(anyValue()));

    public CompensateRecordConstructorRule() {
        super(rootMatcher);
    }

    @Override
    public void onMatch(@Nonnull final ValueComputationRuleCall<Iterable<? extends Value>, Map<Value, PullUpCompensation>> call) {
        final var bindings = call.getBindings();
        final var recordConstructorValue = bindings.get(rootMatcher);
        final var resultingMatchedValuesMap = new LinkedIdentityMap<Value, PullUpCompensation>();

        final var recordConstructorValueResult = call.getResult(recordConstructorValue);
        final var matchedCompensation = recordConstructorValueResult == null
                                        ? null : recordConstructorValueResult.getValue().get(recordConstructorValue);
        if (matchedCompensation != null) {
            resultingMatchedValuesMap.put(recordConstructorValue, matchedCompensation);
        } else {
            for (int i = 0; i < recordConstructorValue.getColumns().size(); ++i) {
                final var column = recordConstructorValue.getColumns().get(i);
                final var childResultPair = call.getResult(column.getValue());
                if (childResultPair == null) {
                    continue;
                }

                //
                // At this point we have a column and the result we computed for all columns that do have results
                // associated with them, i.e. the columns flowing results of values we care about.
                //
                for (final var childValueEntry : childResultPair.getRight().entrySet()) {
                    final var argumentValue = childValueEntry.getKey();
                    final var argumentValueCompensation = childValueEntry.getValue();
                    final var field = column.getField();
                    resultingMatchedValuesMap.putIfAbsent(argumentValue,
                            new FieldValueCompensation(FieldValue.FieldPath.ofSingle(field.getFieldNameOptional().orElse(null), field.getFieldType(), i), argumentValueCompensation));
                }
            }
        }

        call.yieldValue(recordConstructorValue, resultingMatchedValuesMap);
    }
}
