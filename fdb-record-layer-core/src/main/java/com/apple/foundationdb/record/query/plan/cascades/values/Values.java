/*
 * Values.java
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

package com.apple.foundationdb.record.query.plan.cascades.values;

import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.DefaultValueSimplificationRuleSet;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Helper class for dealing with {@link Value}s.
 */
public class Values {
    /**
     * Expand a record value into its field values.
     * @param recordValue the record value
     * @return a list of field values
     */
    @Nonnull
    public static List<Value> deconstructRecord(@Nonnull Value recordValue) {
        Verify.verify(recordValue.getResultType().getTypeCode() == Type.TypeCode.RECORD);
        Verify.verify(recordValue.getResultType() instanceof Type.Record);
        final Type.Record resultType = (Type.Record)recordValue.getResultType();

        if (recordValue instanceof RecordConstructorValue) {
            final var recordConstructorValue = (RecordConstructorValue)recordValue;
            final List<Value> children = ImmutableList.copyOf(recordConstructorValue.getChildren());
            Verify.verify(Objects.requireNonNull(resultType.getFields()).size() == children.size());
            return children;
        }

        final List<Type.Record.Field> fields = Objects.requireNonNull(resultType.getFields());
        final ImmutableList.Builder<Value> resultBuilder = ImmutableList.builder();
        for (int i = 0; i < fields.size(); i++) {
            resultBuilder.add(FieldValue.ofOrdinalNumber(recordValue, i));
        }
        return resultBuilder.build();
    }

    @Nonnull
    public static Set<Value> orderingValuesFromType(@Nonnull final Type type,
                                                    @Nonnull final Supplier<Value> baseValueSupplier,
                                                    @Nonnull final Set<CorrelationIdentifier> constantAliases) {
        if (type.getTypeCode() != Type.TypeCode.RECORD) {
            return ImmutableSet.of(baseValueSupplier.get());
        }

        final var orderingValuesBuilder = ImmutableSet.<Value>builder();
        final var recordType = (Type.Record)type;
        final var fields = recordType.getFields();

        for (final var field : fields) {
            orderingValuesFromType(field.getFieldType(), () -> FieldValue.ofFieldsAndFuseIfPossible(baseValueSupplier.get(), ImmutableList.of(field)), constantAliases).stream()
                    .map(orderingValue -> orderingValue.simplify(DefaultValueSimplificationRuleSet.ofSimplificationRules(), AliasMap.emptyMap(), constantAliases))
                    .forEach(orderingValuesBuilder::add);
        }

        return orderingValuesBuilder.build();
    }
}
