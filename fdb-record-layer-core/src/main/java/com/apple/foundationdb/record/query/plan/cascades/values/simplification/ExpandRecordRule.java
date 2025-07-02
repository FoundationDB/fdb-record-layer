/*
 * ExpandRecordRule.java
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
import com.apple.foundationdb.record.query.plan.cascades.Column;
import com.apple.foundationdb.record.query.plan.cascades.matching.structure.BindingMatcher;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.FieldValue;
import com.apple.foundationdb.record.query.plan.cascades.values.RecordConstructorValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.MultiMatcher.all;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.NotMatcher.not;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.anyValue;
import static com.apple.foundationdb.record.query.plan.cascades.matching.structure.ValueMatchers.recordConstructorValue;

/**
 * A rule that expands a {@link Value} of type {@link Type.Record} into a record constructor over its constituent parts
 * as follows:
 * <br>
 * <pre>
 * {@code
 *     some non rcv value v ==> rcv(FieldValue(v, f1), FieldValue(v, f2), ..., FieldValue(v, fn))) for all fields
 *                              f1, ..., fn in the type produced by v
 * }
 * </pre>
 * <br>
 * Note that this rule is the conceptual opposite of {@link CollapseRecordConstructorOverFieldsToStarRule}. These rules
 * should not be placed into the same rule set as the effect of it is undefined and may cause a stack overflow.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("PMD.TooManyStaticImports")
public class ExpandRecordRule extends ValueSimplificationRule<Value> {
    @Nonnull
    private static final BindingMatcher<Value> rootMatcher =
            anyValue().where(not(recordConstructorValue(all(anyValue()))));

    public ExpandRecordRule() {
        super(rootMatcher);
    }

    @Nonnull
    @Override
    public Optional<Class<?>> getRootOperator() {
        return Optional.empty();
    }

    @Override
    public void onMatch(@Nonnull final ValueSimplificationRuleCall call) {
        Verify.verify(call.isRoot());

        final var bindings = call.getBindings();
        final var value = bindings.get(rootMatcher);
        if (value instanceof FieldValue && ((FieldValue)value).getFieldPath().size() > 1) {
            return;
        }
        final var originalResultType = value.getResultType();
        if (!originalResultType.isRecord()) {
            return;
        }
        Verify.verify(originalResultType instanceof Type.Record);
        final Type.Record resultRecordType = (Type.Record)originalResultType;

        final List<Type.Record.Field> fields = Objects.requireNonNull(resultRecordType.getFields());
        final var resultBuilder = ImmutableList.<Column<? extends Value>>builder();
        for (int i = 0; i < fields.size(); i++) {
            final var field = fields.get(i);
            resultBuilder.add(Column.of(field, FieldValue.ofOrdinalNumberAndFuseIfPossible(value, i)));
        }
        final var resultValue =
                RecordConstructorValue.ofColumns(resultBuilder.build(), originalResultType.isNullable());
        Verify.verify(originalResultType.isNullable() == resultValue.getResultType().isNullable());
        call.yieldResultBuilder().addConstraintsFrom(value).yieldResultAndReExplore(resultValue);
    }
}
