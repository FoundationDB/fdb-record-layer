/*
 * ComposeFieldValueOverRecordConstructorRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.MatchOrCompensateFieldValueRule.FieldValueCompensation;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.anyValue;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.recordConstructorValue;

/**
 * A rule that composes a field access and an underlying record construction.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class CompensateRecordConstructorRule extends ValueComputationRule<List<Value>, Map<Value, Function<Value, Value>>, RecordConstructorValue> {
    @Nonnull
    private static final BindingMatcher<RecordConstructorValue> rootMatcher =
            recordConstructorValue(all(anyValue()));

    public CompensateRecordConstructorRule() {
        super(rootMatcher);
    }

    @Override
    public void onMatch(@Nonnull final ValueComputationRuleCall<List<Value>, Map<Value, Function<Value, Value>>> call) {
        final var bindings = call.getBindings();
        final var recordConstructorValue = bindings.get(rootMatcher);

        final var mergedMatchedValuesMap =
                recordConstructorValue.getColumns()
                        .stream()
                        .flatMap(column -> {
                            final var childResult = call.getResult(column.getValue());
                            return childResult != null ? Stream.of(Pair.of(column, childResult.getResult())) : Stream.empty();
                        })
                        .map(columnWithResult -> {
                            final var column = columnWithResult.getLeft();
                            final var matchedValuesMap = columnWithResult.getRight();

                            //
                            // No we have a column and the result we computed for all columns that do have results associated with them,
                            // i.e. the columns flowing results of values we care about.
                            //
                            final var newMatchedValuesMap = new LinkedIdentityMap<Value, Function<Value, Value>>();

                            for (final var childValueEntry : matchedValuesMap.entrySet()) {
                                final var argumentValue = childValueEntry.getKey();
                                final var argumentValueCompensation = childValueEntry.getValue();
                                newMatchedValuesMap.put(argumentValue,
                                        new FieldValueCompensation(ImmutableList.of(column.getField()), argumentValueCompensation));
                            }
                            return newMatchedValuesMap;
                        })
                        .flatMap(map -> map.entrySet().stream())
                        .collect(Collectors.toMap(Map.Entry::getKey,
                                Map.Entry::getValue,
                                (l, r) -> l,
                                LinkedIdentityMap::new));

        call.yield(recordConstructorValue, mergedMatchedValuesMap);
    }
}
