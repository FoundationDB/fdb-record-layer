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
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.Comparators;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
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
    private final FieldPath fieldPath;

    @Nonnull
    private final Supplier<List<Field>> normalizedFieldsSupplier;

    private FieldValue(@Nonnull Value childValue, @Nonnull FieldPath fieldPath) {
        this.childValue = childValue;
        this.fieldPath = fieldPath;
        normalizedFieldsSupplier = Suppliers.memoize(() -> {
            return normalizeForStructuralEquality(childValue.getResultType(), fieldPath.fields);
        });
    }

    @Nonnull
    public FieldPath getFieldPath() {
        return fieldPath;
    }

    @Nonnull
    public List<Field> getFields() {
        return fieldPath.getFields();
    }

    @Nonnull
    public List<String> getFieldPathNames() {
        return getFields().stream()
                .map(Field::getFieldName)
                .collect(ImmutableList.toImmutableList());
    }

    @Nonnull
    public List<Field> getFieldPrefix() {
        return fieldPath.getFieldPrefix();
    }

    @Nonnull
    public Field getLastField() {
        return fieldPath.getLastField();
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
        final var fieldValue = MessageValue.getFieldValueForFields((Message)childResult, getFields());
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
               normalizedFieldsSupplier.get().equals(that.normalizedFieldsSupplier.get());
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH, fieldPath);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, getFields().stream().map(Field::getFieldName).collect(ImmutableList.toImmutableList()));
    }

    @Override
    public String toString() {
        final var fieldPathString = fieldPath.toString();
        if (childValue instanceof QuantifiedValue || childValue instanceof ObjectValue) {
            return childValue + fieldPathString;
        } else {
            return "(" + childValue + ")" + fieldPathString;
        }
    }

    @Nonnull
    @Override
    public String explain(@Nonnull final Formatter formatter) {
        return childValue.explain(formatter) + fieldPath;
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
    private static FieldPath resolveFieldPath(@Nonnull final Type inputType, @Nonnull final List<Accessor> accessors) {
        final var accessorPathBuilder = ImmutableList.<Field>builder();
        var currentType = inputType;
        for (final var accessor : accessors) {
            final var fieldName = accessor.getFieldName();
            SemanticException.check(currentType.getTypeCode() == Type.TypeCode.RECORD, SemanticException.ErrorCode.FIELD_ACCESS_INPUT_NON_RECORD_TYPE,
                    String.format("field '%s' can only be resolved on records", fieldName == null ? "#" + accessor.getOrdinalFieldNumber() : fieldName));
            final var recordType = (Type.Record)currentType;
            final var fieldNameFieldMap = Objects.requireNonNull(recordType.getFieldNameFieldMap());
            final Field field;
            if (fieldName != null) {
                SemanticException.check(fieldNameFieldMap.containsKey(fieldName), SemanticException.ErrorCode.RECORD_DOES_NOT_CONTAIN_FIELD);
                field = fieldNameFieldMap.get(fieldName);
            } else {
                // field is not accessed by field but by ordinal number
                Verify.verify(accessor.getOrdinalFieldNumber() >= 0);
                field = recordType.getFields().get(accessor.getOrdinalFieldNumber());
            }
            accessorPathBuilder.add(field);
            currentType = field.getFieldType();
        }
        return new FieldPath(accessorPathBuilder.build());
    }

    @Nonnull
    private static List<Field> normalizeForStructuralEquality(@Nonnull final Type inputType, @Nonnull final List<Field> fields) {
        final var normalizedFields = ImmutableList.<Field>builder();
        var currentType = inputType;
        for (final var field : fields) {
            SemanticException.check(currentType.getTypeCode() == Type.TypeCode.RECORD,
                    SemanticException.ErrorCode.UNKNOWN,
                    String.format("field '%s' can only be normalized on records", field.getFieldNameOptional().isEmpty() ? "#" + field.getFieldIndex() : field.getFieldName()));
            final var recordType = (Type.Record)currentType;
            if (field.getFieldIndexOptional().isPresent()) {
                if (field.getFieldNameOptional().isEmpty()) {
                    normalizedFields.add(field);
                } else {
                    normalizedFields.add(Field.of(field.getFieldType(), Optional.empty(), field.getFieldIndexOptional()));
                }
            } else {
                SemanticException.check(field.getFieldNameOptional().isPresent(), SemanticException.ErrorCode.UNKNOWN, "field does not have name or index");
                final var fieldNameFieldMap = Objects.requireNonNull(recordType.getFieldNameFieldMap());
                final var fieldName = field.getFieldName();
                SemanticException.check(fieldNameFieldMap.containsKey(fieldName), SemanticException.ErrorCode.UNKNOWN, "record does not contain specified field");
                final var recordField = fieldNameFieldMap.get(field.getFieldName());
                normalizedFields.add(Field.of(field.getFieldType(), Optional.empty(), recordField.getFieldIndexOptional()));
            }
            currentType = field.getFieldType();
        }
        return normalizedFields.build();
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

    public static FieldValue ofFields(@Nonnull Value childValue, @Nonnull final List<Field> fields) {
        return new FieldValue(childValue, new FieldPath(fields));
    }

    public static FieldValue ofFieldsAndFuseIfPossible(@Nonnull Value childValue, @Nonnull final List<Field> fields) {
        if (childValue instanceof FieldValue) {
            final var childFieldValue = (FieldValue)childValue;
            return FieldValue.ofFields(childFieldValue.getChild(),
                    ImmutableList.<Field>builder().addAll(childFieldValue.getFields()).addAll(fields).build());
        }
        return FieldValue.ofFields(childValue, fields);
    }

    @Nonnull
    public static FieldValue ofOrdinalNumber(@Nonnull Value childValue, final int ordinalNumber) {
        return new FieldValue(childValue, resolveFieldPath(childValue.getResultType(), ImmutableList.of(new Accessor(null, ordinalNumber))));
    }

    @Nonnull
    public static Optional<List<Field>> stripFieldPrefixMaybe(@Nonnull List<Field> fieldPath,
                                                              @Nonnull List<Field> potentialPrefixPath) {
        if (fieldPath.size() < potentialPrefixPath.size()) {
            return Optional.empty();
        }

        for (int i = 0; i < potentialPrefixPath.size(); i++) {
            if (!potentialPrefixPath.get(i).equals(fieldPath.get(i))) {
                return Optional.empty();
            }
        }

        return Optional.of(ImmutableList.copyOf(fieldPath.subList(potentialPrefixPath.size(), fieldPath.size())));
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

    /**
     * A list of fields forming a path.
     */
    @SuppressWarnings("UnstableApiUsage")
    public static class FieldPath {
        private static final FieldPath EMPTY = new FieldPath(ImmutableList.of());

        private static final Comparator<FieldPath> COMPARATOR =
                Comparator.comparing(FieldPath::getFields, Comparators.lexicographical(Comparator.<Field>naturalOrder()));

        @Nonnull
        private final List<Field> fields;

        public FieldPath(@Nonnull final List<Field> fields) {
            this.fields = ImmutableList.copyOf(fields);
        }

        @Nonnull
        public List<Field> getFields() {
            return fields;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof FieldPath)) {
                return false;
            }
            final FieldPath fieldPath = (FieldPath)o;
            return getFields().equals(fieldPath.getFields());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getFields());
        }

        @Override
        @Nonnull
        public String toString() {
            return fields.stream()
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

        @Nonnull
        public List<Field> getFieldPrefix() {
            return fields.subList(0, getFields().size() - 1);
        }

        @Nonnull
        public Field getLastField() {
            return fields.get(getFields().size() - 1);
        }

        public boolean isPrefixOf(@Nonnull final FieldPath otherFieldPath) {
            final var otherFields = otherFieldPath.getFields();
            for (int i = 0; i < fields.size(); i++) {
                final Field otherField = otherFields.get(i);
                if (!fields.get(i).equals(otherField)) {
                    return false;
                }
            }
            return true;
        }

        @Nonnull
        public static FieldPath empty() {
            return EMPTY;
        }

        @Nonnull
        public static Comparator<FieldPath> comparator() {
            return COMPARATOR;
        }
    }
}
