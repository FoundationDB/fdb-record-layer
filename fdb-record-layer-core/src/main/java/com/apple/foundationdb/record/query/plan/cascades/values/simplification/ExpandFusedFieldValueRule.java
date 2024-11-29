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
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue.FieldPath;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.anyFieldValue;
import static com.apple.foundationdb.record.query.plan.cascades.values.FieldValue.ofFields;

/**
 * A rule that composes a field access and an underlying record construction.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class ExpandFusedFieldValueRule extends ValueSimplificationRule<FieldValue> {
    @Nonnull
    private static final BindingMatcher<FieldValue> rootMatcher = anyFieldValue();

    public ExpandFusedFieldValueRule() {
        super(rootMatcher);
    }

    @Override
    public void onMatch(@Nonnull final ValueSimplificationRuleCall call) {
        Verify.verify(call.isRoot());

        final var bindings = call.getBindings();
        final var fieldValue = bindings.get(rootMatcher);
        final var fieldPath = fieldValue.getFieldPath();
        if (fieldPath.size() < 2) {
            return;
        }

        final var lastAccessor = fieldPath.getLastFieldAccessor();
        final var expandedFieldValue =
                ofFields(ofFields(fieldValue.getChild(), fieldPath.getFieldPrefix()),
                        new FieldPath(ImmutableList.of(lastAccessor)));

        call.yieldAndReExplore(expandedFieldValue);
    }
}
