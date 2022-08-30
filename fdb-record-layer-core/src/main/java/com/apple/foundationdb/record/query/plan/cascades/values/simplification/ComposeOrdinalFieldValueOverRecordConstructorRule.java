/*
 * ComposeOrdinalFieldValueOverRecordConstructorRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.values.OrdinalFieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PrimitiveMatchers.anyObject;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.anyValue;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.ordinalFieldValue;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.recordConstructorValue;

/**
 * A rule that composes a field access and an underlying record construction.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class ComposeOrdinalFieldValueOverRecordConstructorRule extends ValueSimplificationRule<OrdinalFieldValue> {
    @Nonnull
    private static final BindingMatcher<RecordConstructorValue> recordConstructorMatcher =
            recordConstructorValue(all(anyValue()));

    @Nonnull
    private static final BindingMatcher<Integer> ordinalPositionMatcher = anyObject();

    @Nonnull
    private static final BindingMatcher<OrdinalFieldValue> rootMatcher =
            ordinalFieldValue(recordConstructorMatcher, ordinalPositionMatcher);

    public ComposeOrdinalFieldValueOverRecordConstructorRule() {
        super(rootMatcher);
    }

    @Override
    public void onMatch(@Nonnull final ValueSimplificationRuleCall call) {
        final var bindings = call.getBindings();
        final int ordinalPosition = bindings.get(ordinalPositionMatcher);
        final var recordConstructor = bindings.get(recordConstructorMatcher);
        final var column = recordConstructor.getColumns().get(ordinalPosition);
        // just return the child
        call.yield(column.getValue());
    }
}
