/*
 * CollapseRecordConstructorOverFieldsToStarRule.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Values;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.anyFieldValue;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.recordConstructorValue;

/**
 * A rule that un-expands a star expansion.
 * <br>
 * {@code ((q.a as a, q.b as b)} is transformed to {@code q} if the type of {@code q} is just composed of the fields
 * {@code a}, and {@code b}.
 * <br>
 * Note that this rule is the conceptual opposite of {@link ExpandRecordRule}. These rules should not be placed into the
 * same rule set as the effect of it is undefined and may cause a stack overflow.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class CollapseRecordConstructorOverFieldsToStarRule extends ValueSimplificationRule<RecordConstructorValue> {
    private static final CollectionMatcher<FieldValue> fieldValuesMatcher = all(anyFieldValue());
    @Nonnull
    private static final BindingMatcher<RecordConstructorValue> rootMatcher =
            recordConstructorValue(fieldValuesMatcher);

    public CollapseRecordConstructorOverFieldsToStarRule() {
        super(rootMatcher);
    }

    @Override
    @SpotBugsSuppressWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    public void onMatch(@Nonnull final ValueSimplificationRuleCall call) {
        final var bindings = call.getBindings();
        final var recordConstructorValue = bindings.get(rootMatcher);
        final var fieldValues = bindings.get(fieldValuesMatcher);
        Values.collapseSimpleSelectMaybe(recordConstructorValue).ifPresent(
                commonChildValue ->
                        call.yieldResultBuilder()
                                .addConstraintsFrom(recordConstructorValue)
                                .addConstraintsFrom(fieldValues)
                                .yieldResult(commonChildValue));
    }
}
