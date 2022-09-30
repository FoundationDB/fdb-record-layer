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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type.Record.Field;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.Objects;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PrimitiveMatchers.anyObject;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.anyValue;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.recordConstructorValue;

/**
 * A rule that composes a field access and an underlying record construction.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class ComposeFieldValueOverRecordConstructorRule extends ValueSimplificationRule<FieldValue> {
    @Nonnull
    private static final BindingMatcher<RecordConstructorValue> recordConstructorMatcher =
            recordConstructorValue(all(anyValue()));

    @Nonnull
    private static final CollectionMatcher<Field> fieldPathMatcher = all(anyObject());

    @Nonnull
    private static final BindingMatcher<FieldValue> rootMatcher =
            ValueMatchers.fieldValueWithFieldPath(recordConstructorMatcher, fieldPathMatcher);

    public ComposeFieldValueOverRecordConstructorRule() {
        super(rootMatcher);
    }

    @Override
    @SpotBugsSuppressWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    public void onMatch(@Nonnull final ValueSimplificationRuleCall call) {
        final var bindings = call.getBindings();

        final var fieldPath = bindings.get(fieldPathMatcher);
        Verify.verify(!fieldPath.isEmpty());
        final var recordConstructor = bindings.get(recordConstructorMatcher);

        final var firstField = Objects.requireNonNull(Iterables.getFirst(fieldPath, null));
        final var column = findColumn(recordConstructor, firstField);
        if (fieldPath.size() == 1) {
            // just return the child
            call.yield(column.getValue());
        } else {
            call.yield(FieldValue.ofFields(column.getValue(),
                    fieldPath.stream()
                            .skip(1L)
                            .collect(ImmutableList.toImmutableList())));
        }
    }

    @Nonnull
    private static Column<? extends Value> findColumn(@Nonnull final RecordConstructorValue recordConstructorValue, @Nonnull final Field field) {
        for (final var column : recordConstructorValue.getColumns()) {
            if (field.getFieldIndex() == column.getField().getFieldIndex()) {
                Verify.verify(field.getFieldNameOptional().equals(column.getField().getFieldNameOptional()));
                return column;
            }
        }
        throw new RecordCoreException("should have found field by field name");
    }
}
