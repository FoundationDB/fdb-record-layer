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
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;

import javax.annotation.Nonnull;
import java.util.Objects;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.anyFieldValue;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.recordConstructorValue;

/**
 * A rule that un-expands a star expansion.
 * <br>
 * {@code ((q.a as a, q.b as b)} is transformed to {@code q} if the type of {@code q} is just composed of the fields
 * {@code a}, and {@code b}.
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

        if (fieldValues.isEmpty()) {
            return;
        }

        Value commonChildValue = null;
        int i = 0;
        for (var iterator = fieldValues.iterator(); iterator.hasNext(); i ++) {
            final var fieldValue = iterator.next();
            final var fieldPath = fieldValue.getFieldPath();
            final Value childValue;
            if (fieldPath.size() > 1) {
                childValue = FieldValue.ofFields(fieldValue.getChild(), fieldPath.getFieldPrefix());
            } else {
                Verify.verify(fieldPath.size() == 1);
                childValue = fieldValue.getChild();
            }

            if (fieldPath.getLastFieldAccessor().getOrdinal() != i) {
                return;
            }

            if (commonChildValue == null) {
                commonChildValue = childValue;

                if (!recordConstructorValue.getResultType().equals(commonChildValue.getResultType())) {
                    return;
                }
            } else {
                if (!commonChildValue.equals(childValue)) {
                    return;
                }
            }
        }

        call.yieldExpression(Objects.requireNonNull(commonChildValue));
    }
}
