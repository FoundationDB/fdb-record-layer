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
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.Formatter;
import com.apple.foundationdb.record.query.plan.cascades.NullableArrayTypeUtils;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type.Record.Field;
import com.google.common.base.Preconditions;
import com.google.common.base.Verify;
import com.google.common.collect.Comparators;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import com.google.common.primitives.ImmutableIntArray;
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
@SuppressWarnings("UnstableApiUsage") // caused by usage of Guava's ImmutableIntArray.
@API(API.Status.EXPERIMENTAL)
public class FieldValue implements ValueWithChild {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Field-Value");

    @Nonnull
    private final Value childValue;
    @Nonnull
    private final FieldPath fieldPath;

    private FieldValue(@Nonnull Value childValue, @Nonnull FieldPath fieldPath) {
        this.childValue = childValue;
        this.fieldPath = fieldPath;
    }

    @Nonnull
    public FieldPath getFieldPath() {
        return fieldPath;
    }

    @Deprecated // should access fields by ordinals
    @Nonnull
    public List<String> getFieldPathNames() {
        return fieldPath.getFieldNames().stream().map(maybe -> maybe.orElseThrow(() -> new RecordCoreException("field name should have been set"))).collect(Collectors.toList());
    }

    @Nonnull
    public List<Type> getFieldPathTypes() {
        return fieldPath.getFieldTypes();
    }

    @Nonnull
    public ImmutableIntArray getFieldPathOrdinals() {
        return fieldPath.fieldOrdinals;
    }

    @Deprecated // should access fields by ordinals
    @Nonnull
    public List<Optional<String>> getFieldPathNamesMaybe() {
        return fieldPath.getFieldNames();
    }

    @Nonnull
    public FieldPath getFieldPrefix() {
        return fieldPath.getFieldPrefix();
    }

    @Nonnull
    public Optional<String> getLastFieldName() {
        return fieldPath.getLastFieldName();
    }

    @Nonnull
    @Override
    public Type getResultType() {
        return fieldPath.getLastFieldType();
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
        final var fieldValue = MessageValue.getFieldValueForFieldOrdinals((Message)childResult, fieldPath.getFieldOrdinals());
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
        return fieldPath.equals(that.fieldPath) &&
               childValue.semanticEquals(that.childValue, equivalenceMap);
    }

    @Override
    public boolean subsumedBy(@Nullable final Value other, @Nonnull final AliasMap equivalenceMap) {
        if (other == null) {
            return false;
        }

        if (!(other instanceof FieldValue)) {
            return false;
        }
        final var otherFieldValue = (FieldValue)other;
        return fieldPath.getFieldOrdinals().equals(otherFieldValue.getFieldPath().getFieldOrdinals()) &&
                childValue.subsumedBy(otherFieldValue.childValue, equivalenceMap);
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashKind.FOR_CONTINUATION, BASE_HASH, fieldPath);
    }
    
    @Override
    public int planHash(@Nonnull final PlanHashKind hashKind) {
        return PlanHashable.objectsPlanHash(hashKind, BASE_HASH, getFieldPathNames());
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
        final var fieldOrdinals = ImmutableIntArray.builder();
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
                final var fieldOrdinalsMap = Objects.requireNonNull(recordType.getFieldNameToOrdinalMap());
                SemanticException.check(fieldOrdinalsMap.containsKey(fieldName), SemanticException.ErrorCode.RECORD_DOES_NOT_CONTAIN_FIELD);
                fieldOrdinals.add(fieldOrdinalsMap.get(fieldName));
            } else {
                // field is not accessed by field but by ordinal number
                Verify.verify(accessor.getOrdinalFieldNumber() >= 0);
                field = recordType.getFields().get(accessor.getOrdinalFieldNumber());
                fieldOrdinals.add(accessor.getOrdinalFieldNumber());
            }
            accessorPathBuilder.add(field);
            currentType = field.getFieldType();
        }
        return new FieldPath(accessorPathBuilder.build(), fieldOrdinals.build());
    }

    @Nonnull
    public static FieldValue ofFieldName(@Nonnull Value childValue, @Nonnull final String fieldName) {
        final var resolved = resolveFieldPath(childValue.getResultType(), ImmutableList.of(new Accessor(fieldName, -1)));
        return new FieldValue(childValue, resolved);
    }

    public static FieldValue ofFieldNames(@Nonnull Value childValue, @Nonnull final List<String> fieldNames) {
        final var resolved = resolveFieldPath(childValue.getResultType(), fieldNames.stream().map(fieldName -> new Accessor(fieldName, -1)).collect(ImmutableList.toImmutableList()));
        return new FieldValue(childValue, resolved);
    }

    public static FieldValue ofFields(@Nonnull Value childValue, @Nonnull final FieldPath fieldPath) {
        return new FieldValue(childValue, fieldPath);
    }

    public static FieldValue ofFieldsAndFuseIfPossible(@Nonnull Value childValue, @Nonnull final FieldPath fields) {
        if (childValue instanceof FieldValue) {
            final var childFieldValue = (FieldValue)childValue;
            return FieldValue.ofFields(childFieldValue.getChild(), childFieldValue.fieldPath.withSuffix(fields));
        }
        return FieldValue.ofFields(childValue, fields);
    }

    @Nonnull
    public static FieldValue ofOrdinalNumber(@Nonnull Value childValue, final int ordinalNumber) {
        final var resolved = resolveFieldPath(childValue.getResultType(), ImmutableList.of(new Accessor(null, ordinalNumber)));
        return new FieldValue(childValue, resolved);
    }

    @Nonnull
    public static Optional<FieldPath> stripFieldPrefixMaybe(@Nonnull FieldPath fieldPath,
                                                              @Nonnull FieldPath potentialPrefixPath) {
        if (fieldPath.size() < potentialPrefixPath.size()) {
            return Optional.empty();
        }

        final var fieldPathOrdinals = fieldPath.getFieldOrdinals();
        final var fieldPathTypes = fieldPath.getFieldTypes();
        final var potentialPrefixFieldOrdinals = potentialPrefixPath.getFieldOrdinals();
        final var potentialPrefixFieldTypes = potentialPrefixPath.getFieldTypes();
        for (int i = 0; i < potentialPrefixPath.size(); i++) {
            if (fieldPathOrdinals.get(i) != potentialPrefixFieldOrdinals.get(i) || !fieldPathTypes.get(i).equals(potentialPrefixFieldTypes.get(i))) {
                return Optional.empty();
            }
        }
        return Optional.of(fieldPath.subList(potentialPrefixPath.size(), fieldPath.size()));
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
        private static final FieldPath EMPTY = new FieldPath(ImmutableList.of(), ImmutableIntArray.of());

        private static final Comparator<FieldPath> COMPARATOR =
                Comparator.comparing(f -> f.fieldOrdinals.asList(), Comparators.lexicographical(Comparator.<Integer>naturalOrder()));

        @Nonnull
        private final List<Optional<String>> fieldNames;

        @Nonnull
        private final List<Type> fieldTypes;

        /**
         * This encapsulates the ordinals of the field path encoded by this {@link FieldValue}. It serves two purposes:
         * <ul>
         *     <li>checking whether a {@link FieldValue} subsumes another {@link FieldValue}.</li>
         *     <li>evaluating a {@link Message} to get the corresponding field value object.</li>
         * </ul>
         */
        @Nonnull
        private final ImmutableIntArray fieldOrdinals;

        FieldPath(@Nonnull final List<Field> fields, @Nonnull final ImmutableIntArray fieldOrdinals) {
            Preconditions.checkArgument(fieldOrdinals.length() == fields.size());
            Preconditions.checkArgument(fieldOrdinals.stream().allMatch(f -> f >= 0));
            this.fieldNames = fields.stream().map(Field::getFieldNameOptional).collect(Collectors.toList());
            this.fieldTypes = fields.stream().map(Field::getFieldType).collect(Collectors.toList());
            this.fieldOrdinals = fieldOrdinals;
        }

        public FieldPath(@Nonnull final List<Optional<String>> fieldNames, @Nonnull final List<Type> fieldTypes, @Nonnull final ImmutableIntArray fieldOrdinals) {
            Preconditions.checkArgument(fieldNames.size() == fieldTypes.size());
            Preconditions.checkArgument(fieldTypes.size() == fieldOrdinals.length());
            Preconditions.checkArgument(fieldOrdinals.stream().allMatch(f -> f >= 0));
            this.fieldNames = ImmutableList.copyOf(fieldNames);
            this.fieldTypes = ImmutableList.copyOf(fieldTypes);
            this.fieldOrdinals = fieldOrdinals; // already immutable.
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof FieldPath)) {
                return false;
            }
            final FieldPath otherFieldPath = (FieldPath)o;
            return fieldNames.equals(otherFieldPath.fieldNames)
                    && fieldTypes.equals(otherFieldPath.fieldTypes)
                    && fieldOrdinals.equals(otherFieldPath.getFieldOrdinals());
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldNames, fieldOrdinals, fieldTypes);
        }

        @Override
        @Nonnull
        public String toString() {
            return Streams.zip(fieldNames.stream(), fieldOrdinals.stream().boxed(),
                    (maybeFieldName, fieldOrdinal) -> maybeFieldName.map(s -> "." + s).orElseGet(() -> "#" + fieldOrdinal))
                    .collect(Collectors.joining());
        }

        @Nonnull
        public List<Optional<String>> getFieldNames() {
            return fieldNames;
        }

        @Nonnull
        public FieldPath getFieldPrefix() {
            Preconditions.checkArgument(size() > 1);
            return subList(0, size() - 1);
        }

        @Nonnull
        public Optional<String> getLastFieldName() {
            Preconditions.checkArgument(!isEmpty());
            return fieldNames.get(size() - 1);
        }

        public int getLastFieldOrdinal() {
            Preconditions.checkArgument(!isEmpty());
            return fieldOrdinals.get(size() - 1);
        }

        @Nonnull
        public Type getLastFieldType() {
            Preconditions.checkArgument(!isEmpty());
            return fieldTypes.get(size() - 1);
        }

        @Nonnull
        public ImmutableIntArray getFieldOrdinals() {
            return fieldOrdinals;
        }

        public int size() {
            return fieldTypes.size();
        }

        public boolean isEmpty() {
            return fieldTypes.isEmpty();
        }

        @Nonnull
        public FieldPath subList(int fromIndex, int toIndex) {
            return new FieldPath(fieldNames.subList(fromIndex, toIndex),
                    fieldTypes.subList(fromIndex, toIndex),
                    fieldOrdinals.subArray(fromIndex, toIndex));
        }

        @Nonnull
        public FieldPath skip(int count) {
            Preconditions.checkArgument(count >= 0);
            Preconditions.checkArgument(count <= size());

            if (count == 0) {
                return this;
            } else if (count == size()) {
                return empty();
            }

            return subList(count, size());
        }

        @Nonnull
        public List<Type> getFieldTypes() {
            return fieldTypes;
        }

        public boolean isPrefixOf(@Nonnull final FieldPath otherFieldPath) {
            if (otherFieldPath.size() < size()) {
                return false;
            }
            for (int i = 0; i < size(); i++) {
                if (otherFieldPath.fieldOrdinals.get(i) != fieldOrdinals.get(i) || !otherFieldPath.fieldTypes.get(i).equals(fieldTypes.get(i))) {
                    return false;
                }
            }
            return true;
        }

        @Nonnull
        public FieldPath withSuffix(@Nonnull final FieldPath suffix) {
            if (suffix.isEmpty() && this.isEmpty()) {
                return empty();
            } else if (suffix.isEmpty()) {
                return this;
            } else if (this.isEmpty()) {
                return suffix;
            }
            return new FieldPath(ImmutableList.<Optional<String>>builder().addAll(fieldNames).addAll(suffix.fieldNames).build(),
                    ImmutableList.<Type>builder().addAll(fieldTypes).addAll(suffix.fieldTypes).build(),
                    ImmutableIntArray.builder().addAll(fieldOrdinals).addAll(suffix.fieldOrdinals).build());
        }

        @Nonnull
        public static FieldPath empty() {
            return EMPTY;
        }

        @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
        @Nonnull
        public static FieldPath flat(@Nonnull final Optional<String> fieldName, @Nonnull final Type fieldType, @Nonnull final Integer fieldOrdinal) {
            return new FieldPath(ImmutableList.of(fieldName), ImmutableList.of(fieldType), ImmutableIntArray.of(fieldOrdinal));
        }

        @Nonnull
        public static Comparator<FieldPath> comparator() {
            return COMPARATOR;
        }
    }
}
