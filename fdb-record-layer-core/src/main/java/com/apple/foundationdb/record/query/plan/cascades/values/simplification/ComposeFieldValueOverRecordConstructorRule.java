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
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.CollectionMatcher;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;
import com.google.common.collect.Iterables;

import javax.annotation.Nonnull;
import java.util.Objects;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.PrimitiveMatchers.anyObject;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.anyValue;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.recordConstructorValue;

/**
 * A rule that composes a field access and an underlying record construction, for example
 * <br>
 * {@code (("Hello" as a, "World" as b).b} is transformed to {@code "World"}.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class ComposeFieldValueOverRecordConstructorRule extends ValueSimplificationRule<FieldValue> {
    @Nonnull
    private static final BindingMatcher<RecordConstructorValue> recordConstructorMatcher =
            recordConstructorValue(all(anyValue()));

    @Nonnull
    private static final CollectionMatcher<Integer> fieldPathOrdinalsMatcher = all(anyObject());

    @Nonnull
    private static final CollectionMatcher<Type> fieldPathTypesMatcher = all(anyObject());

    @Nonnull
    private static final BindingMatcher<FieldValue> rootMatcher =
            ValueMatchers.fieldValueWithFieldPath(recordConstructorMatcher, fieldPathOrdinalsMatcher, fieldPathTypesMatcher);

    public ComposeFieldValueOverRecordConstructorRule() {
        super(rootMatcher);
    }

    @Override
    @SpotBugsSuppressWarnings("NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE")
    public void onMatch(@Nonnull final ValueSimplificationRuleCall call) {
        final var bindings = call.getBindings();

        final var fieldPathOrdinals = bindings.get(fieldPathOrdinalsMatcher);
        Verify.verify(!fieldPathOrdinals.isEmpty());
        final var fieldPathTypes = bindings.get(fieldPathTypesMatcher);
        Verify.verify(!fieldPathTypes.isEmpty());
        final var recordConstructor = bindings.get(recordConstructorMatcher);

        final var firstFieldOrdinal = Objects.requireNonNull(Iterables.getFirst(fieldPathOrdinals, null));
        final var fieldFieldType = Objects.requireNonNull(Iterables.getFirst(fieldPathTypes, null));
        final var column = findColumn(recordConstructor, firstFieldOrdinal, fieldFieldType);

        final var root = bindings.get(rootMatcher);
        if (fieldPathOrdinals.size() == 1) {
            // just return the child
            call.yieldExpression(column.getValue());
        } else {
            call.yieldExpression(FieldValue.ofFields(column.getValue(), root.getFieldPath().subList(1)));
        }
    }

    @Nonnull
    private static Column<? extends Value> findColumn(@Nonnull final RecordConstructorValue recordConstructorValue, final int fieldOrdinal, @Nonnull Type fieldType) {
        final var result = recordConstructorValue.getColumns().get(fieldOrdinal);
        Verify.verify(result.getField().getFieldType().equals(fieldType));
        return result;
    }
}
