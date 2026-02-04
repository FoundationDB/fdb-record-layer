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
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.PlanSerializable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.TupleFieldsProto;
import com.apple.foundationdb.record.metadata.expressions.TupleFieldsHelper;
import com.apple.foundationdb.record.planprotos.PFieldPath;
import com.apple.foundationdb.record.planprotos.PFieldPath.PResolvedAccessor;
import com.apple.foundationdb.record.planprotos.PFieldValue;
import com.apple.foundationdb.record.planprotos.PValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.record.query.plan.cascades.AliasMap;
import com.apple.foundationdb.record.query.plan.cascades.ConstrainedBoolean;
import com.apple.foundationdb.record.query.plan.cascades.NullableArrayTypeUtils;
import com.apple.foundationdb.record.query.plan.cascades.SemanticException;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type.Record.Field;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokensWithPrecedence.Precedence;
import com.apple.foundationdb.record.util.VectorUtils;
import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.base.VerifyException;
import com.google.common.collect.Comparators;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.primitives.ImmutableIntArray;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * A value representing the contents of a (non-repeated, arbitrarily-nested) field of a quantifier.
 */
@API(API.Status.EXPERIMENTAL)
public class FieldValue extends AbstractValue implements ValueWithChild {
    private static final ObjectPlanHash BASE_HASH = new ObjectPlanHash("Field-Value");

    @Nonnull
    private final Value childValue;
    @Nonnull
    private final FieldPath fieldPath;

    @Nonnull
    private final Supplier<List<String>> fieldNamesSupplier;
    @Nonnull
    private final Supplier<Type> resultTypeSupplier;

    private FieldValue(@Nonnull final Value childValue, @Nonnull final FieldPath fieldPath) {
        this.childValue = childValue;
        this.fieldPath = fieldPath;
        fieldNamesSupplier = Suppliers.memoize(() ->
                fieldPath.getOptionalFieldNames()
                        .stream()
                        .map(maybe -> maybe.orElseThrow(() -> new RecordCoreException("field name should have been set")))
                        .collect(Collectors.toList()));
        resultTypeSupplier = Suppliers.memoize(this::computeResultType);
    }

    @Nonnull
    public FieldPath getFieldPath() {
        return fieldPath;
    }

    @Nonnull
    public List<String> getFieldPathNames() {
        return fieldNamesSupplier.get();
    }

    @Nonnull
    public List<Type> getFieldPathTypes() {
        return fieldPath.getFieldTypes();
    }

    @Nonnull
    public ImmutableIntArray getFieldOrdinals() {
        return fieldPath.getFieldOrdinals();
    }

    @Nonnull
    public List<Optional<String>> getFieldPathNamesMaybe() {
        return fieldPath.getOptionalFieldNames();
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
        return resultTypeSupplier.get();
    }

    @Nonnull
    private Type computeResultType() {
        Type lastFieldType = fieldPath.getLastFieldType();
        boolean anyNullable = childValue.getResultType().isNullable() || fieldPath.areAnyFieldTypesNullable();
        return lastFieldType.overrideIfNullable(anyNullable);
    }

    @Nonnull
    @Override
    public Value getChild() {
        return childValue;
    }

    @Nonnull
    @Override
    public FieldValue withNewChild(@Nonnull final Value child) {
        return FieldValue.ofFieldsAndFuseIfPossible(child, fieldPath);
    }

    @Override
    public <M extends Message> Object eval(@Nullable final FDBRecordStoreBase<M> store, @Nonnull final EvaluationContext context) {
        final var childResult = childValue.eval(store, context);
        if (!(childResult instanceof Message)) {
            return null;
        }
        final var fieldValue = MessageHelpers.getFieldValueForFieldOrdinals((Message)childResult, fieldPath.getFieldOrdinals());
        //
        // If the last step in the field path is an array that is also nullable, then we need to unwrap the value
        // wrapper.
        //
        return unwrapPrimitive(getResultType(), NullableArrayTypeUtils.unwrapIfArray(fieldValue, getResultType()));
    }

    @Nullable
    private static Object unwrapPrimitive(@Nonnull Type type, @Nullable Object fieldValue) {
        if (fieldValue == null) {
            return null;
        }
        if (type instanceof Type.Array) {
            Verify.verify(fieldValue instanceof List<?>);
            List<?> list = (List<?>) fieldValue;
            Type elementType = Objects.requireNonNull(((Type.Array)type).getElementType());
            List<Object> returnList = new ArrayList<>(list.size());
            for (Object elem : list) {
                returnList.add(unwrapPrimitive(elementType, elem));
            }
            return returnList;
        } else if (type.getTypeCode() == Type.TypeCode.VERSION) {
            return FDBRecordVersion.fromBytes(((ByteString)fieldValue).toByteArray(), false);
        } else if (type.isUuid()) {
            if (fieldValue instanceof UUID) {
                return fieldValue;
            } else if (fieldValue instanceof Message) {
                Message fieldMessage = (Message) fieldValue;
                Verify.verify(fieldMessage.getDescriptorForType().equals(TupleFieldsProto.UUID.getDescriptor()));
                return TupleFieldsHelper.fromProto(fieldMessage, TupleFieldsProto.UUID.getDescriptor());
            }
            throw new VerifyException("Unable to unwrap UUID type " + fieldValue.getClass());
        } else if (type.isVector()) {
            Verify.verify(fieldValue instanceof ByteString);
            final var byteString = (ByteString) fieldValue;
            return VectorUtils.parseVector(byteString, (Type.Vector)type);
        } else {
            // This also may need to turn ByteString's into byte[] for Type.TypeCode.BYTES
            return fieldValue;
        }
    }

    @Nonnull
    @Override
    public ConstrainedBoolean equalsWithoutChildren(@Nonnull final Value other) {
        return ValueWithChild.super.equalsWithoutChildren(other)
                .filter(ignored -> fieldPath.equals(((FieldValue)other).getFieldPath()));
    }

    @Override
    public int hashCodeWithoutChildren() {
        return PlanHashable.objectsPlanHash(PlanHashable.CURRENT_FOR_CONTINUATION, BASE_HASH, fieldPath);
    }

    @Override
    public int planHash(@Nonnull final PlanHashMode mode) {
        return PlanHashable.objectsPlanHash(mode, BASE_HASH, fieldPath);
    }

    @Nonnull
    @Override
    public ExplainTokensWithPrecedence explain(@Nonnull final Iterable<Supplier<ExplainTokensWithPrecedence>> explainSuppliers) {
        final var explainSupplier = Iterables.getOnlyElement(explainSuppliers);
        final var childExplain =
                Precedence.DOT.parenthesizeChild(explainSupplier.get(), true);
        return ExplainTokensWithPrecedence.of(Precedence.DOT, new ExplainTokens().addNested(childExplain).addToString(fieldPath));
    }

    @Override
    public int hashCode() {
        return semanticHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @SpotBugsSuppressWarnings("EQ_UNUSUAL")
    @Override
    public boolean equals(final Object other) {
        return semanticEquals(other, AliasMap.emptyMap());
    }

    @Nonnull
    @Override
    public PFieldValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
        return PFieldValue.newBuilder()
                .setChildValue(childValue.toValueProto(serializationContext))
                .setFieldPath(fieldPath.toProto(serializationContext))
                .build();
    }

    @Nonnull
    @Override
    public PValue toValueProto(@Nonnull PlanSerializationContext serializationContext) {
        return PValue.newBuilder().setFieldValue(toProto(serializationContext)).build();
    }

    @Nonnull
    public static FieldValue fromProto(@Nonnull final PlanSerializationContext serializationContext, @Nonnull final PFieldValue fieldValueProto) {
        return new FieldValue(Value.fromValueProto(serializationContext, Objects.requireNonNull(fieldValueProto.getChildValue())),
                FieldPath.fromProto(serializationContext, Objects.requireNonNull(fieldValueProto.getFieldPath())));
    }

    @Nonnull
    @VisibleForTesting
    public static FieldPath resolveFieldPath(@Nonnull final Type inputType, @Nonnull final List<Accessor> accessors) {
        final var accessorPathBuilder = ImmutableList.<ResolvedAccessor>builder();
        var currentType = inputType;
        for (final var accessor : accessors) {
            final var fieldName = accessor.getName();
            SemanticException.check(currentType.isRecord(), SemanticException.ErrorCode.FIELD_ACCESS_INPUT_NON_RECORD_TYPE,
                    "field '" + (fieldName == null ? "#" + accessor.getOrdinal() : fieldName) + "' can only be resolved on records");
            final var recordType = (Type.Record)currentType;
            final var fieldNameFieldMap = Objects.requireNonNull(recordType.getFieldNameFieldMap());
            final Field field;
            final int ordinal;
            if (fieldName != null) {
                SemanticException.check(fieldNameFieldMap.containsKey(fieldName), SemanticException.ErrorCode.RECORD_DOES_NOT_CONTAIN_FIELD);
                field = fieldNameFieldMap.get(fieldName);
                final var fieldOrdinalsMap = Objects.requireNonNull(recordType.getFieldNameToOrdinalMap());
                SemanticException.check(fieldOrdinalsMap.containsKey(fieldName), SemanticException.ErrorCode.RECORD_DOES_NOT_CONTAIN_FIELD);
                ordinal = fieldOrdinalsMap.get(fieldName);
            } else {
                // field is not accessed by field but by ordinal number
                Verify.verify(accessor.getOrdinal() >= 0);
                field = recordType.getFields().get(accessor.getOrdinal());
                ordinal = accessor.getOrdinal();
            }
            currentType = field.getFieldType();
            accessorPathBuilder.add(ResolvedAccessor.of(field, ordinal));
        }
        return new FieldPath(accessorPathBuilder.build());
    }

    @Nonnull
    public static FieldValue ofFieldName(@Nonnull Value childValue, @Nonnull final String fieldName) {
        final var resolved = resolveFieldPath(childValue.getResultType(), ImmutableList.of(new Accessor(fieldName, -1)));
        return new FieldValue(childValue, resolved);
    }

    @Nonnull
    public static FieldValue ofFieldNameAndFuseIfPossible(@Nonnull Value childValue, @Nonnull final String fieldName) {
        final var resolved = resolveFieldPath(childValue.getResultType(), ImmutableList.of(new Accessor(fieldName, -1)));
        return ofFieldsAndFuseIfPossible(childValue, resolved);
    }

    @Nonnull
    public static FieldValue ofFieldNames(@Nonnull Value childValue, @Nonnull final List<String> fieldNames) {
        final var resolved = resolveFieldPath(childValue.getResultType(), fieldNames.stream().map(fieldName -> new Accessor(fieldName, -1)).collect(ImmutableList.toImmutableList()));
        return new FieldValue(childValue, resolved);
    }

    @Nonnull
    public static FieldValue ofFields(@Nonnull Value childValue, @Nonnull final FieldPath fieldPath) {
        return new FieldValue(childValue, fieldPath);
    }

    @Nonnull
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
    public static FieldValue ofOrdinalNumberAndFuseIfPossible(@Nonnull Value childValue, final int ordinalNumber) {
        final var resolved = resolveFieldPath(childValue.getResultType(), ImmutableList.of(new Accessor(null, ordinalNumber)));
        return ofFieldsAndFuseIfPossible(childValue, resolved);
    }

    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    @Nonnull
    public static Optional<FieldPath> stripFieldPrefixMaybe(@Nonnull FieldPath fieldPath,
                                                            @Nonnull FieldPath potentialPrefixPath) {
        if (fieldPath.size() < potentialPrefixPath.size()) {
            return Optional.empty();
        }

        final var fieldPathOrdinals = fieldPath.getFieldOrdinals();
        final var potentialPrefixFieldOrdinals = potentialPrefixPath.getFieldOrdinals();
        for (int i = 0; i < potentialPrefixPath.size(); i++) {
            if (fieldPathOrdinals.get(i) != potentialPrefixFieldOrdinals.get(i)) {
                return Optional.empty();
            }
        }
        return Optional.of(fieldPath.subList(potentialPrefixPath.size(), fieldPath.size()));
    }

    @Nonnull
    @Override
    protected Iterable<? extends Value> computeChildren() {
        return ImmutableList.of(getChild());
    }

    /**
     * A list of fields forming a path.
     */
    public static class FieldPath implements PlanSerializable {
        private static final FieldPath EMPTY = new FieldPath(ImmutableList.of());

        private static final Comparator<FieldPath> COMPARATOR =
                Comparator.comparing(f -> f.getFieldOrdinals().asList(), Comparators.lexicographical(Comparator.<Integer>naturalOrder()));

        @Nonnull
        private final List<ResolvedAccessor> fieldAccessors;

        @Nonnull
        private final Supplier<List<Optional<String>>> fieldNamesSupplier;

        /**
         * This encapsulates the ordinals of the field path encoded by this {@link FieldValue}. It serves two purposes:
         * <ul>
         *     <li>checking whether a {@link FieldValue} subsumes another {@link FieldValue}.</li>
         *     <li>evaluating a {@link Message} to get the corresponding field value object.</li>
         * </ul>
         */
        @Nonnull
        private final Supplier<ImmutableIntArray> fieldOrdinalsSupplier;

        @Nonnull
        private final Supplier<List<Type>> fieldTypesSupplier;

        public FieldPath(@Nonnull final List<ResolvedAccessor> fieldAccessors) {
            this.fieldAccessors = ImmutableList.copyOf(fieldAccessors);
            this.fieldNamesSupplier = Suppliers.memoize(() -> computeFieldNames(fieldAccessors));
            this.fieldOrdinalsSupplier = Suppliers.memoize(() -> computeOrdinals(fieldAccessors));
            this.fieldTypesSupplier = Suppliers.memoize(() -> computeFieldTypes(fieldAccessors));
        }

        @Nonnull
        public List<ResolvedAccessor> getFieldAccessors() {
            return fieldAccessors;
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
            return fieldAccessors.equals(otherFieldPath.fieldAccessors);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldAccessors);
        }

        @Override
        @Nonnull
        public String toString() {
            return fieldAccessors.stream()
                    .map(fieldAccessor -> fieldAccessor.getName() == null ? "#" + fieldAccessor.getOrdinal() : "." + fieldAccessor.getName())
                    .collect(Collectors.joining());
        }

        @Nonnull
        public List<Optional<String>> getOptionalFieldNames() {
            return fieldNamesSupplier.get();
        }

        @Nonnull
        public ImmutableIntArray getFieldOrdinals() {
            return fieldOrdinalsSupplier.get();
        }

        @Nonnull
        public List<Type> getFieldTypes() {
            return fieldTypesSupplier.get();
        }

        @Nonnull
        public FieldPath getFieldPrefix() {
            Preconditions.checkArgument(!isEmpty());
            return subList(0, size() - 1);
        }

        @Nonnull
        public ResolvedAccessor getLastFieldAccessor() {
            Preconditions.checkArgument(!isEmpty());
            return getFieldAccessors().get(size() - 1);
        }

        @Nonnull
        public Optional<String> getLastFieldName() {
            Preconditions.checkArgument(!isEmpty());
            return getOptionalFieldNames().get(size() - 1);
        }

        public int getLastFieldOrdinal() {
            Preconditions.checkArgument(!isEmpty());
            return getFieldOrdinals().get(size() - 1);
        }

        @Nonnull
        public Type getLastFieldType() {
            Preconditions.checkArgument(!isEmpty());
            return getFieldTypes().get(size() - 1);
        }

        public boolean areAnyFieldTypesNullable() {
            Preconditions.checkArgument(!isEmpty());
            return getFieldTypes().stream().anyMatch(Type::isNullable);
        }

        public int size() {
            return fieldAccessors.size();
        }

        public boolean isEmpty() {
            return fieldAccessors.isEmpty();
        }

        @Nonnull
        public FieldPath subList(int fromIndex, int toIndex) {
            return new FieldPath(fieldAccessors.subList(fromIndex, toIndex));
        }

        @Nonnull
        public FieldPath subList(int count) {
            Preconditions.checkArgument(count >= 0);
            Preconditions.checkArgument(count <= size());

            if (count == 0) {
                return this;
            } else if (count == size()) {
                return empty();
            }

            return subList(count, size());
        }

        @SuppressWarnings("BooleanMethodIsAlwaysInverted")
        public boolean isPrefixOf(@Nonnull final FieldPath otherFieldPath) {
            if (otherFieldPath.size() < size()) {
                return false;
            }
            for (int i = 0; i < size(); i++) {
                if (!fieldAccessors.get(i).equals(otherFieldPath.fieldAccessors.get(i))) {
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
            return new FieldPath(ImmutableList.<ResolvedAccessor>builder().addAll(fieldAccessors).addAll(suffix.fieldAccessors).build());
        }

        @Nonnull
        public static FieldPath empty() {
            return EMPTY;
        }

        @Nonnull
        private static List<Optional<String>> computeFieldNames(@Nonnull final List<ResolvedAccessor> fieldAccessors) {
            return fieldAccessors.stream()
                    .map(accessor -> accessor.getField().getFieldStorageNameOptional())
                    .collect(ImmutableList.toImmutableList());
        }

        @Nonnull
        private static ImmutableIntArray computeOrdinals(@Nonnull final List<ResolvedAccessor> fieldAccessors) {
            final var resultBuilder = ImmutableIntArray.builder();
            fieldAccessors.forEach(accessor -> resultBuilder.add(accessor.getOrdinal()));
            return resultBuilder.build();
        }

        @Nonnull
        private static List<Type> computeFieldTypes(@Nonnull final List<ResolvedAccessor> fieldAccessors) {
            return fieldAccessors.stream()
                    .map(ResolvedAccessor::getType)
                    .collect(ImmutableList.toImmutableList());
        }

        @Nonnull
        public static FieldPath ofSingle(@Nonnull Field field, @Nonnull final Integer fieldOrdinal) {
            return new FieldPath(ImmutableList.of(ResolvedAccessor.of(field, fieldOrdinal)));
        }

        @Nonnull
        public static FieldPath ofSingle(@Nonnull final ResolvedAccessor accessor) {
            return new FieldPath(ImmutableList.of(accessor));
        }

        @Nonnull
        public static Comparator<FieldPath> comparator() {
            return COMPARATOR;
        }

        @Nonnull
        @Override
        public PFieldPath toProto(@Nonnull final PlanSerializationContext serializationContext) {
            PFieldPath.Builder builder = PFieldPath.newBuilder();
            for (final ResolvedAccessor fieldAccessor : fieldAccessors) {
                builder.addFieldAccessors(fieldAccessor.toProto(serializationContext));
            }
            return builder.build();
        }

        @Nonnull
        public static FieldPath fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                          @Nonnull final PFieldPath fieldPathProto) {
            final ImmutableList.Builder<ResolvedAccessor> resolvedAccessorsBuilder = ImmutableList.builder();
            for (int i = 0; i < fieldPathProto.getFieldAccessorsCount(); i ++) {
                final PResolvedAccessor resolvedAccessorProto = fieldPathProto.getFieldAccessors(i);
                resolvedAccessorsBuilder.add(ResolvedAccessor.fromProto(serializationContext, resolvedAccessorProto));
            }
            final var resolvedAccessors = resolvedAccessorsBuilder.build();
            Verify.verify(!resolvedAccessors.isEmpty());
            return new FieldPath(resolvedAccessors);
        }
    }

    /**
     * Helper class to hold information about a particular field access.
     */
    public static class Accessor {
        @Nullable
        final String name;

        final int ordinal;

        public Accessor(@Nullable final String name, final int ordinal) {
            this.name = name;
            this.ordinal = ordinal;
        }

        @Nullable
        public String getName() {
            return name;
        }

        public int getOrdinal() {
            return ordinal;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Accessor)) {
                return false;
            }
            final Accessor accessor = (Accessor)o;
            return ordinal == accessor.ordinal && Objects.equals(name, accessor.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, ordinal);
        }
    }

    /**
     * A resolved {@link Accessor} that now also holds the resolved {@link Type}.
     */
    public static class ResolvedAccessor implements PlanSerializable {
        @Nonnull
        final Field field;
        final int ordinal;

        protected ResolvedAccessor(@Nonnull Field field, int ordinal) {
            Preconditions.checkArgument(ordinal >= 0);
            this.field = field;
            this.ordinal = ordinal;
        }

        @Nullable
        public String getName() {
            return field.getFieldNameOptional().orElse(null);
        }

        public int getOrdinal() {
            return ordinal;
        }

        @Nonnull
        public Field getField() {
            return field;
        }

        @Nonnull
        public Type getType() {
            return Objects.requireNonNull(field.getFieldType());
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ResolvedAccessor)) {
                return false;
            }
            final ResolvedAccessor that = (ResolvedAccessor)o;
            return getOrdinal() == that.getOrdinal();
        }

        @Override
        public int hashCode() {
            return Objects.hash(getOrdinal());
        }

        @Nonnull
        @Override
        public String toString() {
            return getName() + ';' + ordinal + ';' + getType();
        }

        @Nonnull
        @Override
        public PResolvedAccessor toProto(@Nonnull final PlanSerializationContext serializationContext) {
            PResolvedAccessor.Builder builder = PResolvedAccessor.newBuilder();
            // Older serialization: write out the name, ordinal, and type manually
            builder.setName(field.getFieldName());
            builder.setOrdinal(ordinal);
            if (field.getFieldType() != null) {
                builder.setType(getType().toTypeProto(serializationContext));
            }
            // Newer serialization: write that information in a nested field
            builder.setField(field.toProto(serializationContext));
            return builder.build();
        }

        @Nonnull
        public static ResolvedAccessor fromProto(@Nonnull PlanSerializationContext serializationContext,
                                                 @Nonnull final PResolvedAccessor resolvedAccessorProto) {
            final Type type;
            if (resolvedAccessorProto.hasType()) {
                type = Type.fromTypeProto(serializationContext, resolvedAccessorProto.getType());
            } else {
                type = null;
            }

            final Field field;
            if (resolvedAccessorProto.hasField()) {
                // Newer serialization: use a single nested field. If both are set, we need to deserialize
                // the field after reading the type information, as the type will be cached in the
                // serialization context
                field = Field.fromProto(serializationContext, resolvedAccessorProto.getField());
            } else {
                // Older serialization: get the name and type information from separate fields
                field = Field.of(Objects.requireNonNull(type), Optional.of(resolvedAccessorProto.getName()));
            }
            return new ResolvedAccessor(field, resolvedAccessorProto.getOrdinal());
        }

        @Nonnull
        public static ResolvedAccessor of(@Nonnull final Field field, final int ordinal) {
            return new ResolvedAccessor(field, ordinal);
        }

        @Nonnull
        public static ResolvedAccessor of(@Nullable final String fieldName, final int ordinalFieldNumber, @Nonnull final Type type) {
            final Field field = Field.of(type, Optional.ofNullable(fieldName));
            return new ResolvedAccessor(field, ordinalFieldNumber);
        }

        @Nonnull
        public static ResolvedAccessor of(@Nonnull final Type.Record recordType, @Nonnull final String fieldName, final int ordinalFieldNumber) {
            final Map<String, Field> fieldNameMap = recordType.getFieldNameFieldMap();
            Field field = fieldNameMap.get(fieldName);
            SemanticException.check(field != null, SemanticException.ErrorCode.RECORD_DOES_NOT_CONTAIN_FIELD);
            return new ResolvedAccessor(field, ordinalFieldNumber);
        }
    }

    /**
     * Deserializer.
     */
    @AutoService(PlanDeserializer.class)
    public static class Deserializer implements PlanDeserializer<PFieldValue, FieldValue> {
        @Nonnull
        @Override
        public Class<PFieldValue> getProtoMessageClass() {
            return PFieldValue.class;
        }

        @Nonnull
        @Override
        public FieldValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                    @Nonnull final PFieldValue fieldValueProto) {
            return FieldValue.fromProto(serializationContext, fieldValueProto);
        }
    }
}
