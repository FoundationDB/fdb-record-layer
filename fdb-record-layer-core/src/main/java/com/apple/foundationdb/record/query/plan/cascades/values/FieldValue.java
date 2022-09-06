/*
 * FieldValue.java
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.annotation.SpotBugsSuppressWarnings;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ObjectPlanHash;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.NullableArrayTypeUtils;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type.Record.Field;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A value representing the contents of a (non-repeated, arbitrarily-nested) field of a quantifier.
 */
@API(API.Status.EXPERIMENTAL)
public class FieldValue implements ValueWithChild {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Field-Value");

    @Nonnull
    private final Value childValue;
    @Nonnull
    private final List<Field> fieldPath;

    private FieldValue(@Nonnull Value childValue, @Nonnull List<Field> fieldPath) {
        Preconditions.checkArgument(!fieldPath.isEmpty());
        this.childValue = childValue;
        this.fieldPath = ImmutableList.copyOf(fieldPath);
    }

    @Nonnull
    public List<Field> getFieldPath() {
        return fieldPath;
    }

    @Nonnull
    public List<String> getFieldPathNames() {
        return fieldPath.stream()
                .map(Field::getFieldName)
                .collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    public List<Field> getFieldPrefix() {
        return fieldPath.subList(0, fieldPath.size() - 1);
    }

    @Nonnull
    public Field getLastField() {
        return fieldPath.get(fieldPath.size() - 1);
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return getLastField().getFieldType();
    }

    @Nonnull
    @Override
    public Value getChild() {
        return childValue;
    }

    @Nonnull
    @Override
    public FieldValue withNewChild(@Nonnull final Value child) {
        return new FieldValue(child, fieldPath);
    }

    @Override
    public <M extends Message> Object eval(@Nonnull final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        final var childResult = childValue.eval(store, context);
        if (!(childResult instanceof Message)) {
            return null;
        }
        final var fieldValue = MessageValue.getFieldValueForFieldNames((Message)childResult, fieldPath);
        //
        // If the last step in the field path is an array that is also nullable, then we need to unwrap the value
        // wrapper.
        //
        return NullableArrayTypeUtils.unwrapIfArray(fieldValue, getResultType());
    }

    @Override
    public boolean equalsWithoutChildren(@Nonnull final Value other, @Nonnull final AliasMap equivalenceMap) {
        if (!ValueWithChild.super.equalsWithoutChildren(other, equivalenceMap)) {
            return false;
        }

        final var that = (FieldValue)other;
        return childValue.semanticEquals(that.childValue, equivalenceMap) &&
               fieldPath.equals(that.fieldPath);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH, fieldPath);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, fieldPath.stream().map(Field::getFieldName).collect(ImmutableList.toImmutableList()));
    }

    @Override
    public String toString() {
        final var fieldPathString = getFieldPathAsString();
        if (childValue instanceof QuantifiedValue) {
            return childValue + fieldPathString;
        } else {
            return "(" + childValue + ")" + fieldPathString;
        }
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return childValue.explain(formatter) + getFieldPathAsString();
    }

    @Nonnull
    private String getFieldPathAsString() {
        return fieldPath.stream()
                .map(field -> {
                    if (field.getFieldNameOptional().isPresent()) {
                        return "." + field.getFieldName();
                    } else if (field.getFieldIndexOptional().isPresent()) {
                        return "#" + field.getFieldIndex();
                    }
                    return "(null)";
                })
                .collect(Collectors.joining());
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.identitiesFor(childValue.getCorrelatedTo()));
    }

    @Nonnull
    private static List<Field> resolveFieldPath(@Nonnull final Type inputType, @Nonnull final List<Accessor> accessors) {
        final var accessorPathBuilder = ImmutableList.<Field>builder();
        var currentType = inputType;
        for (final var accessor : accessors) {
            final var fieldName = accessor.getFieldName();
            SemanticException.check(currentType.getTypeCode() == Type.TypeCode.RECORD,
                    String.format("field '%s' can only be resolved on records", fieldName == null ? "#" + accessor.getOrdinalFieldNumber() : fieldName));
            final var recordType = (Type.Record)currentType;
            final var fieldNameFieldMap = Objects.requireNonNull(recordType.getFieldNameFieldMap());
            final Field field;
            if (fieldName != null) {
                SemanticException.check(fieldNameFieldMap.containsKey(fieldName), "record does not contain specified field");
                field = fieldNameFieldMap.get(fieldName);
            } else {
                // field is not accessed by field but by ordinal number
                Verify.verify(accessor.getOrdinalFieldNumber() >= 0);
                field = recordType.getFields().get(accessor.getOrdinalFieldNumber());
            }
            accessorPathBuilder.add(field);
            currentType = field.getFieldType();
        }
        return accessorPathBuilder.build();
    }

    @Nonnull
    public static FieldValue ofFieldName(@Nonnull Value childValue, @Nonnull final String fieldName) {
        return new FieldValue(childValue, resolveFieldPath(childValue.getResultType(), ImmutableList.of(new Accessor(fieldName, -1))));
    }

    public static FieldValue ofFieldNames(@Nonnull Value childValue, @Nonnull final List<String> fieldNames) {
        return new FieldValue(childValue, resolveFieldPath(childValue.getResultType(), fieldNames.stream().map(fieldName -> new Accessor(fieldName, -1)).collect(ImmutableList.toImmutableList())));
    }

    public static FieldValue ofAccessors(@Nonnull Value childValue, @Nonnull final List<Accessor> accessors) {
        return new FieldValue(childValue, resolveFieldPath(childValue.getResultType(), accessors));
    }

    @Nonnull
    public static FieldValue ofOrdinalNumber(@Nonnull Value childValue, final int ordinalNumber) {
        return new FieldValue(childValue, resolveFieldPath(childValue.getResultType(), ImmutableList.of(new Accessor(null, ordinalNumber))));
    }

    /**
     * Helper class to hold information about a particular field access.
     */
    public static class Accessor {
        @Nullable
        final String fieldName;

        final int ordinalFieldNumber;

        public Accessor(@Nullable final String fieldName, final int ordinalFieldNumber) {
            this.fieldName = fieldName;
            this.ordinalFieldNumber = ordinalFieldNumber;
        }

        @Nullable
        public String getFieldName() {
            return fieldName;
        }

        public int getOrdinalFieldNumber() {
            return ordinalFieldNumber;
        }
    }
}
