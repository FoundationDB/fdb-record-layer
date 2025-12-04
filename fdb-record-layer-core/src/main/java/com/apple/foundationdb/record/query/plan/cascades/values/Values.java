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
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.StreamSupport;

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
                    FieldValue.FieldPath.ofSingle(FieldValue.ResolvedAccessor.of(field, i));
            primitiveAccessorsForType(field.getFieldType(),
                    () -> FieldValue.ofFieldsAndFuseIfPossible(baseValueSupplier.get(), singleStepPath))
                    .forEach(orderingValuesBuilder::add);
        }

        return orderingValuesBuilder.build();
    }

    /**
     * Attempts to collapse a {@link RecordConstructorValue} that represents a simple field selection back to its
     * underlying record value.
     * <br>
     * This method checks if a record constructor simply reconstructs all fields of a record in their original order
     * without any transformations. If so, it returns the original underlying record value instead of the redundant
     * record constructor.
     * <br>
     * For example, if we have a record constructor that takes a record {@code r} and creates
     * {@code (r.field0, r.field1, r.field2)} where the fields are accessed in order (0, 1, 2), this method
     * would return {@code r} directly instead of the constructor.
     * <br>
     * The method performs several validation checks:
     * <ul>
     * <li>All children must be {@link FieldValue} instances</li>
     * <li>Field access must be in sequential order (0, 1, 2, ...)</li>
     * <li>All field values must reference the same underlying record</li>
     * <li>The result type must match the underlying record's type</li>
     * </ul>
     * <br>
     * Note that this method will still return the underlying record value even if it contains more fields than the
     * record. This is considered to be fine as it does not influence the semantics of any {@code Value} operating on
     * top of the record, because any invisible fields to an upper {@code Value} remain invisible.
     *
     * @param recordConstructorValue the record constructor to potentially collapse
     * @return an {@link Optional} containing the underlying record value if collapse is possible,
     *         or {@link Optional#empty()} if the record constructor cannot be simplified
     */
    @Nonnull
    public static Optional<Value> collapseSimpleSelectMaybe(@Nonnull final RecordConstructorValue recordConstructorValue) {
        if (Iterables.isEmpty(recordConstructorValue.getChildren()) ||
                StreamSupport.stream(recordConstructorValue.getChildren().spliterator(), false).anyMatch(v -> !(v instanceof FieldValue))) {
            return Optional.empty();
        }

        final var fieldValues = recordConstructorValue.getChildren().iterator();

        Value commonChildValue = null;
        int i = 0;
        for (; fieldValues.hasNext(); i++) {
            final var fieldValue = (FieldValue)fieldValues.next();
            final var fieldPath = fieldValue.getFieldPath();

            if (fieldPath.getLastFieldAccessor().getOrdinal() != i) {
                return Optional.empty();
            }

            final Value childValue;
            if (fieldPath.size() > 1) {
                childValue = FieldValue.ofFields(fieldValue.getChild(), fieldPath.getFieldPrefix());
            } else {
                Verify.verify(fieldPath.size() == 1);
                childValue = fieldValue.getChild();
            }

            if (commonChildValue == null) {
                commonChildValue = childValue;

                if (!recordConstructorValue.getResultType().equals(commonChildValue.getResultType())) {
                    return Optional.empty();
                }
            } else {
                if (!commonChildValue.equals(childValue)) {
                    return Optional.empty();
                }
            }
        }
        return Optional.of(Objects.requireNonNull(commonChildValue));
    }
}
