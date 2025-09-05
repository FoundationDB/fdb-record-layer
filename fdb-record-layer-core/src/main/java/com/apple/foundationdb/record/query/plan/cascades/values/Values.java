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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.CorrelationIdentifier;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.AbstractValueRuleSet;
import com.apple.foundationdb.record.query.plan.cascades.values.simplification.ValueSimplificationRuleCall;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;

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
    public static List<Value> deconstructRecord(@Nonnull final Value recordValue) {
        final var resultType = recordValue.getResultType();
        Verify.verify(resultType.isRecord());
        Verify.verify(resultType instanceof Type.Record);
        final Type.Record resultRecordType = (Type.Record)resultType;

        if (recordValue instanceof RecordConstructorValue) {
            final var recordConstructorValue = (RecordConstructorValue)recordValue;
            final List<Value> children = ImmutableList.copyOf(recordConstructorValue.getChildren());
            Verify.verify(Objects.requireNonNull(resultRecordType.getFields()).size() == children.size());
            return children;
        }

        final List<Type.Record.Field> fields = Objects.requireNonNull(resultRecordType.getFields());
        final ImmutableList.Builder<Value> resultBuilder = ImmutableList.builder();
        for (int i = 0; i < fields.size(); i++) {
            resultBuilder.add(FieldValue.ofOrdinalNumberAndFuseIfPossible(recordValue, i));
        }
        return resultBuilder.build();
    }

    @Nonnull
    public static List<Value> simplify(@Nonnull final Iterable<Value> values,
                                       @Nonnull final AbstractValueRuleSet<Value, ValueSimplificationRuleCall> ruleSet,
                                       @Nonnull final EvaluationContext evaluationContext,
                                       @Nonnull final AliasMap aliasMap,
                                       @Nonnull final Set<CorrelationIdentifier> constantAliases) {
        return Streams.stream(values)
                .map(value -> value.simplify(ruleSet, evaluationContext, aliasMap, constantAliases))
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * Method to construct a set of {@link Value} that can be used to express orderings based on the type of a flowed
     * object.
     * <br>
     * For instance, a {@link QuantifiedObjectValue} of some alias flows records of the shape
     * {@code ((a as a, b as b) as x, c as y)} where {@code a}, {@code b}, and {@code c} are of primitive types. This
     * method uses this type information to construct a list of accessors that retrieve the primitive data elements of
     * that flowed record, i.e, this method would return {@code {_.x.a, _.x,b, _.y}}.
     * <br>
     * @param type the type used to construct accessors for
     * @param baseValueSupplier a supplier that creates a base value the accessors are expressed over
     * @return a list of {@link Value}s consisting of the accessors to primitive elements of the given type.
     */
    @Nonnull
    public static List<Value> primitiveAccessorsForType(@Nonnull final Type type,
                                                        @Nonnull final Supplier<Value> baseValueSupplier) {
        if (type.getTypeCode() != Type.TypeCode.RECORD) {
            SemanticException.check(type.isPrimitive() || type.isEnum(),
                    SemanticException.ErrorCode.ORDERING_IS_OF_INCOMPATIBLE_TYPE);
            return ImmutableList.of(baseValueSupplier.get());
        }

        final var orderingValuesBuilder = ImmutableList.<Value>builder();
        final var recordType = (Type.Record)type;
        final var fields = recordType.getFields();

        for (int i = 0; i < fields.size(); i++) {
            final var field = fields.get(i);
            final var singleStepPath =
                    FieldValue.FieldPath.ofSingle(FieldValue.ResolvedAccessor.of(
                            field.getFieldNameOptional().orElse(null), i, field.getFieldType()));
            primitiveAccessorsForType(field.getFieldType(),
                    () -> FieldValue.ofFieldsAndFuseIfPossible(baseValueSupplier.get(), singleStepPath))
                    .forEach(orderingValuesBuilder::add);
        }

        return orderingValuesBuilder.build();
    }
}
