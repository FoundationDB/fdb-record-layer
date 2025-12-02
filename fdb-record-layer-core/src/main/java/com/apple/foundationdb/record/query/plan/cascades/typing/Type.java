/*
 * Type.java
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

package com.apple.foundationdb.record.query.plan.cascades.typing;

import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.record.PlanDeserializer;
import com.apple.foundationdb.record.PlanSerializable;
import com.apple.foundationdb.record.PlanSerializationContext;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordMetaDataOptionsProto;
import com.apple.foundationdb.record.TupleFieldsProto;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.planprotos.PType;
import com.apple.foundationdb.record.planprotos.PType.PAnyRecordType;
import com.apple.foundationdb.record.planprotos.PType.PAnyType;
import com.apple.foundationdb.record.planprotos.PType.PArrayType;
import com.apple.foundationdb.record.planprotos.PType.PEnumType;
import com.apple.foundationdb.record.planprotos.PType.PNoneType;
import com.apple.foundationdb.record.planprotos.PType.PNullType;
import com.apple.foundationdb.record.planprotos.PType.PPrimitiveType;
import com.apple.foundationdb.record.planprotos.PType.PRecordType;
import com.apple.foundationdb.record.planprotos.PType.PRelationType;
import com.apple.foundationdb.record.planprotos.PType.PTypeCode;
import com.apple.foundationdb.record.planprotos.PType.PUuidType;
import com.apple.foundationdb.record.planprotos.PType.PVectorType;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordVersion;
import com.apple.foundationdb.record.query.plan.cascades.Narrowable;
import com.apple.foundationdb.record.query.plan.cascades.NullableArrayTypeUtils;
import com.apple.foundationdb.record.query.plan.cascades.values.PromoteValue;
import com.apple.foundationdb.record.query.plan.cascades.values.Value;
import com.apple.foundationdb.record.query.plan.explain.DefaultExplainFormatter;
import com.apple.foundationdb.record.query.plan.explain.ExplainTokens;
import com.apple.foundationdb.record.query.plan.serialization.PlanSerialization;
import com.apple.foundationdb.record.util.ProtoUtils;
import com.apple.foundationdb.record.util.VectorUtils;
import com.apple.foundationdb.util.StringUtils;
import com.google.auto.service.AutoService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Provides type information about the output of an expression such as {@link Value} in a QGM.
 * <br>
 * Types bear a resemblance to protobuf types; they are either primitive such as <code>boolean</code>, <code>int</code>,
 * and <code>string</code> or structured such as {@link Record} and {@link Array}. Moreover, it is possible to switch
 * between a {@link Type} instance and an equivalent protobuf {@link Descriptors} in a lossless manner.
 * <br>
 * Finally, {@link Type}s are non-referential, so two structural types are considered equal iff their structures
 * are equal.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public interface Type extends Narrowable<Type>, PlanSerializable {
    @Nonnull
    Null NULL = new Null();

    @Nonnull
    None NONE = new None();

    @Nonnull
    Uuid UUID_NULL_INSTANCE = new Uuid(true);

    @Nonnull
    Uuid UUID_NON_NULL_INSTANCE = new Uuid(false);


    /**
     * A map from Java {@link Class} to corresponding {@link TypeCode}.
     */
    @Nonnull
    Supplier<BiMap<Class<?>, TypeCode>> CLASS_TO_TYPE_CODE_SUPPLIER = Suppliers.memoize(TypeCode::computeClassToTypeCodeMap);

    /**
     * Returns the {@link TypeCode} of the {@link Type} instance.
     *
     * @return The {@link TypeCode} of the {@link Type} instance.
     */
    TypeCode getTypeCode();

    /**
     * Returns the corresponding Java {@link Class} of the {@link Type} instance.
     *
     * @return the corresponding Java {@link Class} of the {@link Type} instance.
     */
    default Class<?> getJavaClass() {
        return getTypeCode().getJavaClass();
    }

    /**
     * Checks whether a {@link Type} is primitive or structured.
     *
     * @return <code>true</code> if the {@link Type} is primitive, otherwise <code>false</code>.
     */
    default boolean isPrimitive() {
        return getTypeCode().isPrimitive();
    }

    /**
     * Checks whether a {@link Type} is any type.
     *
     * @return <code>true</code> if the {@link Type} is any type, otherwise <code>false</code>.
     */
    default boolean isAny() {
        return getTypeCode().equals(TypeCode.ANY);
    }

    /**
     * Checks whether a {@link Type} is {@link Array}.
     *
     * @return <code>true</code> if the {@link Type} is {@link Array}, otherwise <code>false</code>.
     */
    default boolean isArray() {
        return getTypeCode().equals(TypeCode.ARRAY);
    }

    /**
     * Checks whether a {@link Type} is {@link Record}.
     *
     * @return <code>true</code> if the {@link Type} is {@link Record}, otherwise <code>false</code>.
     */
    default boolean isRecord() {
        return getTypeCode().equals(TypeCode.RECORD);
    }

    default boolean isVector() {
        return getTypeCode().equals(TypeCode.VECTOR);
    }

    /**
     * Checks whether a {@link Type} is {@link Relation}.
     *
     * @return <code>true</code> if the {@link Type} is {@link Relation}, otherwise <code>false</code>.
     */
    default boolean isRelation() {
        return getTypeCode().equals(TypeCode.RELATION);
    }

    /**
     * Checks whether a {@link Type} is {@link Enum}.
     *
     * @return <code>true</code> if the {@link Type} is {@link Enum}, otherwise <code>false</code>.
     */
    default boolean isEnum() {
        return getTypeCode().equals(TypeCode.ENUM);
    }

    /**
     * Checks whether a {@link Type} is {@link Uuid}.
     *
     * @return <code>true</code> if the {@link Type} is {@link Uuid}, otherwise <code>false</code>.
     */
    default boolean isUuid() {
        return false;
    }

    /**
     * Checks whether a {@link Type} is nullable.
     *
     * @return <code>true</code> if the {@link Type} is nullable, otherwise <code>false</code>.
     */
    boolean isNullable();

    default boolean isNotNullable() {
        return !isNullable();
    }

    default Type nullable() {
        return withNullability(true);
    }

    default Type notNullable() {
        return withNullability(false);
    }

    /**
     * Create a new type based on the current one that indicates nullability based on the {@code isNullable} parameter
     * passed in.
     * @param newIsNullable indicator whether the returned new type is nullable or not nullable
     * @return a new type that is the same type as the current type but reflecting the nullability as passed in to
     *         this method.
     */
    @Nonnull
    Type withNullability(boolean newIsNullable);

    @Nonnull
    default Type overrideIfNullable(boolean shouldBeNullable) {
        if (shouldBeNullable && !isNullable()) {
            return withNullability(true);
        } else {
            return this;
        }
    }

    /**
     * Safe-casts {@code this} into a {@link Array}.
     *
     * @return an {@code Optional} of {@code this} cast to array if {@code this} is an {@link Array}, otherwise an empty
     * {@link Optional}.
     */
    @Nonnull
    default Optional<Type.Array> narrowArrayMaybe() {
        if (isArray()) {
            return Optional.of((Type.Array)this);
        } else {
            return Optional.empty();
        }
    }

    /**
     * Safe-casts {@code this} into a {@link Record}.
     *
     * @return an {@code Optional} of {@code this} cast to array if {@code this} is an {@link Record}, otherwise an empty
     * {@link Optional}.
     */
    @Nonnull
    default Optional<Type.Record> narrowRecordMaybe() {
        if (isRecord()) {
            return Optional.of((Type.Record)this);
        } else {
            return Optional.empty();
        }
    }

    /**
     * Safe-casts {@code this} into a {@link Enum}.
     *
     * @return an {@code Optional} of {@code this} cast to array if {@code this} is an {@link Enum}, otherwise an empty
     * {@link Optional}.
     */
    @Nonnull
    default Optional<Type.Enum> narrowEnumMaybe() {
        if (isEnum()) {
            return Optional.of((Type.Enum)this);
        } else {
            return Optional.empty();
        }
    }

    /**
     * Checks whether a {@link Type} is numeric.
     * @return <code>true</code> if the {@link Type} is numeric, otherwise <code>false</code>.
     */
    default boolean isNumeric() {
        return getTypeCode().isNumeric();
    }

    default boolean isUnresolved() {
        final var typeCode = getTypeCode();
        return typeCode == TypeCode.UNKNOWN;
    }

    @Nonnull
    ExplainTokens describe();

    /**
     * Creates a synthetic protobuf descriptor that is equivalent to <code>this</code> {@link Type}.
     *
     * @param typeRepositoryBuilder The type repository builder.
     */
    default void defineProtoType(final TypeRepository.Builder typeRepositoryBuilder) {
        // by default we don't build anything here
    }

    /**
     * Creates a synthetic protobuf descriptor that is equivalent to the <code>this</code> {@link Type} within a given
     * protobuf descriptor.
     * @param typeRepositoryBuilder The type repository.
     * @param descriptorBuilder The parent descriptor into which the newly created descriptor will be created.
     * @param fieldNumber The field number of the descriptor.
     * @param fieldName The field name of the descriptor.
     * @param typeNameOptional The type name of the descriptor.
     * @param label The label of the descriptor.
     */
    void addProtoField(@Nonnull TypeRepository.Builder typeRepositoryBuilder,
                       @Nonnull DescriptorProto.Builder descriptorBuilder,
                       int fieldNumber,
                       @Nonnull String fieldName,
                       @Nonnull Optional<String> typeNameOptional,
                       @Nonnull FieldDescriptorProto.Label label);

    @Nullable
    default <T> T validateObject(@Nullable final T object) {
        if (object == null) {
            Verify.verify(isNullable());
        } else {
            Verify.verify(this.nullable().equals(fromObject(object).nullable()));
        }
        return object;
    }

    /**
     * Returns a map from Java {@link Class} to corresponding {@link TypeCode}.
     *
     * @return A map from Java {@link Class} to corresponding {@link TypeCode}.
     */
    @Nonnull
    static Map<Class<?>, TypeCode> getClassToTypeCodeMap() {
        return CLASS_TO_TYPE_CODE_SUPPLIER.get();
    }

    /**
     * Constructs a field name for a given field suffix.
     *
     * @param fieldSuffix The field suffix.
     * @return a field name generated using the field suffix.
     */
    static String fieldName(final Object fieldSuffix) {
        // do this in the style of Scala
        return "_" + fieldSuffix;
    }

    @Nonnull
    static Null nullType() {
        return Type.NULL;
    }

    @Nonnull
    static None noneType() {
        return Type.NONE;
    }

    @Nonnull
    static Uuid uuidType(boolean withNullability) {
        if (withNullability) {
            return UUID_NULL_INSTANCE;
        } else {
            return UUID_NON_NULL_INSTANCE;
        }
    }

    /**
     * For a given {@link TypeCode}, it returns a corresponding <i>nullable</i> {@link Type}.
     * <br>
     * pre-condition: The {@link TypeCode} is primitive.
     * <br>
     * @param typeCode The primitive type code.
     * @return the corresponding {@link Type}.
     */
    @Nonnull
    static Type primitiveType(@Nonnull final TypeCode typeCode) {
        return primitiveType(typeCode, true);
    }

    /**
     * For a given {@link TypeCode}, it returns a corresponding {@link Type}.
     * <br>
     * pre-condition: The {@link TypeCode} is primitive.
     * <br>
     * @param typeCode The primitive type code.
     * @param isNullable True, if the {@link Type} is supposed to be nullable, otherwise, false.
     * @return the corresponding {@link Type}.
     */
    @Nonnull
    @VisibleForTesting
    static Type primitiveType(@Nonnull final TypeCode typeCode, final boolean isNullable) {
        Verify.verify(typeCode.isPrimitive());
        return new Primitive(isNullable, typeCode);
    }

    /**
     * Maps a {@link List} of {@link Typed} instances to a {@link List} of their {@link Type}s.
     * @param typedList The list of {@link Typed} objects.
     * @return The list of {@link Type}s.
     */
    @Nonnull
    static List<Type> fromTyped(@Nonnull List<? extends Typed> typedList) {
        return typedList.stream()
                .map(Typed::getResultType)
                .collect(ImmutableList.toImmutableList());
    }

    /**
     * translates a protobuf {@link com.google.protobuf.Descriptors.Descriptor} to a {@link Type}.
     *
     * @param descriptor The protobuf descriptor.
     * @param protoType The protobuf descriptor type.
     * @param protoLabel The protobuf descriptor label.
     * @param isNullable <code>true</code> if the generated {@link Type} should be nullable, otherwise <code>false</code>.
     * @return A {@link Type} object that corresponds to the protobuf {@link com.google.protobuf.Descriptors.Descriptor}.
     */
    @Nonnull
    private static Type fromProtoType(@Nullable Descriptors.GenericDescriptor descriptor,
                                      @Nonnull Descriptors.FieldDescriptor.Type protoType,
                                      @Nonnull FieldDescriptorProto.Label protoLabel,
                                      @Nullable DescriptorProtos.FieldOptions fieldOptions,
                                      boolean isNullable,
                                      boolean preserveNames) {
        final var typeCode = TypeCode.fromProtobufFieldDescriptor(protoType, fieldOptions);
        if (protoLabel == FieldDescriptorProto.Label.LABEL_REPEATED) {
            // collection type
            return fromProtoTypeToArray(descriptor, protoType, typeCode, fieldOptions, false, preserveNames);
        } else if (typeCode.isPrimitive()) {
            final var fieldOptionMaybe = Optional.ofNullable(fieldOptions).map(f -> f.getExtension(RecordMetaDataOptionsProto.field));
            if (fieldOptionMaybe.isPresent() && fieldOptionMaybe.get().hasVectorOptions()) {
                final var vectorOptions = fieldOptionMaybe.get().getVectorOptions();
                return Type.Vector.of(isNullable, vectorOptions.getPrecision(), vectorOptions.getDimensions());
            }
            return primitiveType(typeCode, isNullable);
        } else if (typeCode == TypeCode.ENUM) {
            final var enumDescriptor = (Descriptors.EnumDescriptor)Objects.requireNonNull(descriptor);
            return preserveNames ? Enum.fromDescriptorPreservingNames(isNullable, enumDescriptor) : Enum.fromDescriptor(isNullable, enumDescriptor);
        } else if (typeCode == TypeCode.RECORD) {
            Objects.requireNonNull(descriptor);
            final var messageDescriptor = (Descriptors.Descriptor)descriptor;
            if (NullableArrayTypeUtils.describesWrappedArray(messageDescriptor)) {
                // find TypeCode of array elements
                final var elementField = messageDescriptor.findFieldByName(NullableArrayTypeUtils.getRepeatedFieldName());
                final var elementTypeCode = TypeCode.fromProtobufFieldDescriptor(elementField.getType(), elementField.getOptions());
                return fromProtoTypeToArray(descriptor, protoType, elementTypeCode, elementField.getOptions(), true, preserveNames);
            } else if (TupleFieldsProto.UUID.getDescriptor().equals(messageDescriptor)) {
                return Type.uuidType(isNullable);
            } else {
                final Type.Record recordType = preserveNames ? Record.fromDescriptorPreservingName(messageDescriptor) : Record.fromDescriptor(messageDescriptor);
                return recordType.withNullability(isNullable);
            }
        }

        throw new IllegalStateException("unable to translate protobuf descriptor to type");
    }

    /**
     * Translates a repeated field in a protobuf descriptor to a {@link Array}.
     * @param descriptor The protobuf descriptor.
     * @param protoType The protobuf descriptor type.
     * @return A {@link Array} object that corresponds to the protobuf {@link com.google.protobuf.Descriptors.Descriptor}.
     */
    @Nonnull
    private static Array fromProtoTypeToArray(@Nullable Descriptors.GenericDescriptor descriptor,
                                              @Nonnull Descriptors.FieldDescriptor.Type protoType,
                                              @Nonnull TypeCode typeCode,
                                              @Nullable DescriptorProtos.FieldOptions fieldOptions,
                                              boolean isNullable,
                                              boolean preserveNames) {
        if (typeCode.isPrimitive()) {
            final Type type;
            if (typeCode == TypeCode.VECTOR) {
                final var vectorOptions = Objects.requireNonNull(fieldOptions).getExtension(RecordMetaDataOptionsProto.field).getVectorOptions();
                type = Type.Vector.of(false, vectorOptions.getPrecision(), vectorOptions.getDimensions());
            } else {
                type = primitiveType(typeCode, false);
            }
            return new Array(isNullable, type);
        } else if (typeCode == TypeCode.ENUM) {
            final Descriptors.EnumDescriptor enumDescriptor;
            if (isNullable) {
                // Unwrap the nullable array
                enumDescriptor = ((Descriptors.Descriptor)Objects.requireNonNull(descriptor)).findFieldByName(NullableArrayTypeUtils.getRepeatedFieldName()).getEnumType();
            } else {
                enumDescriptor = (Descriptors.EnumDescriptor)Objects.requireNonNull(descriptor);
            }
            Objects.requireNonNull(enumDescriptor);
            final var enumType = preserveNames ? Enum.fromDescriptorPreservingNames(false, enumDescriptor) : Enum.fromDescriptor(false, enumDescriptor);
            return new Array(isNullable, enumType);
        } else {
            final Descriptors.Descriptor recordDescriptor;
            if (isNullable) {
                // Unwrap the nullable array
                recordDescriptor = ((Descriptors.Descriptor)Objects.requireNonNull(descriptor)).findFieldByName(NullableArrayTypeUtils.getRepeatedFieldName()).getMessageType();
                protoType = Descriptors.FieldDescriptor.Type.MESSAGE;
            } else {
                recordDescriptor = (Descriptors.Descriptor) descriptor;
            }
            Objects.requireNonNull(recordDescriptor);
            return new Array(isNullable, fromProtoType(recordDescriptor, protoType, FieldDescriptorProto.Label.LABEL_OPTIONAL, fieldOptions, false, preserveNames));
        }
    }

    /**
     * For a given {@link com.google.protobuf.Descriptors.FieldDescriptor} descriptor, returns the type-specific
     * descriptor if the field is a message or an enum, otherwise <code>null</code>.
     *
     * @param fieldDescriptor The descriptor.
     * @return the type-specific descriptor for the field, otherwise <code>null</code>.
     */
    @Nullable
    private static Descriptors.GenericDescriptor getTypeSpecificDescriptor(@Nonnull final Descriptors.FieldDescriptor fieldDescriptor) {
        switch (fieldDescriptor.getType()) {
            case MESSAGE:
                return fieldDescriptor.getMessageType();
            case ENUM:
                return fieldDescriptor.getEnumType();
            default:
                return null;
        }
    }

    /**
     * Find the maximum type of two types. The maximum type is the type that can describe all values adhering to both
     * sides passed in. Some combinations are not defined.
     * Primitive types are treated using the SQL-like promotion rules to form a promotion ladder meaning that
     * <pre>
     * {@code
     * INT --> LONG --> FLOAT --> DOUBLE
     * }
     * </pre>
     * can be promoted up (the values can be substituted without loss).
     * <pre>
     * Examples
     * {@code
     * int, int --> int
     * int, float --> float
     * int, string --> undefined
     * record(int as a, int as b), record(int as a, int as b) --> record(int as a, int as b)
     * record(int as a, int as b), record(int as c, int as d) --> record(int, int) (unnamed)
     * record(int, int), record(float, float) --> record(float, float)
     * record(int, float), record(float, int) --> record(float, float)
     * record(int, array(float)), record(int, array(double)) --> record(int, array(double))
     * record(int, string), record(float, int) --> undefined
     * record(int), record(int, int) --> undefined
     * }
     * </pre>
     *
     * @param t1 one type
     * @param t2 another type
     * @return the maximum type of {@code t1} and type {@code t2} or {@code null} if the maximum type is not defined
     */
    @Nullable
    @SuppressWarnings("PMD.CompareObjectsWithEquals")
    static Type maximumType(@Nonnull final Type t1, @Nonnull final Type t2) {
        if (t1.getTypeCode() == TypeCode.NULL && t2.getTypeCode() == TypeCode.NULL) {
            return Type.nullType();
        }

        if (t1.getTypeCode() == TypeCode.NULL && PromoteValue.isPromotable(t1, t2)) {
            return t2.withNullability(true);
        }
        if (t2.getTypeCode() == TypeCode.NULL && PromoteValue.isPromotable(t2, t1)) {
            return t1.withNullability(true);
        }

        Verify.verify(!t1.isUnresolved());
        Verify.verify(!t2.isUnresolved());

        boolean isResultNullable = t1.isNullable() || t2.isNullable();

        if (t1.isEnum() && t2.isEnum()) {
            final var t1Enum = (Enum)t1;
            final var t2Enum = (Enum)t2;
            final var t1EnumValues = t1Enum.enumValues;
            final var t2EnumValues = t2Enum.enumValues;
            if (t1EnumValues == null) {
                return t2EnumValues == null ? t1Enum.withNullability(isResultNullable) : null;
            }
            return t1EnumValues.equals(t2EnumValues) ? t1Enum.withNullability(isResultNullable) : null;
        } else if ((t1.isPrimitive() || t1.isEnum()) || t1.isUuid() && (t2.isPrimitive() || t2.isEnum() || t2.isUuid())) {
            if (t1.getTypeCode() == t2.getTypeCode()) {
                return t1.withNullability(isResultNullable);
            }
            if (PromoteValue.isPromotable(t1, t2)) {
                return t2.withNullability(isResultNullable);
            }
            if (PromoteValue.isPromotable(t2, t1)) {
                return t1.withNullability(isResultNullable);
            }
            // Type are primitive or enum but not compatible, no promotion possible.
            return null;
        }

        // neither of the types are null, primitives or enums
        if (t1.getTypeCode() != t2.getTypeCode()) {
            return null;
        }

        switch (t1.getTypeCode()) {
            case RECORD:
                final var t1Fields = ((Type.Record)t1).getFields();
                final var t2Fields = ((Type.Record)t2).getFields();

                if (t1Fields.size() != t2Fields.size()) {
                    return null;
                }

                final var resultFieldsBuilder = ImmutableList.<Type.Record.Field>builder();
                for (int i = 0; i < t1Fields.size(); i++) {
                    final var t1Field = t1Fields.get(i);
                    final var t2Field = t2Fields.get(i);
                    
                    final var resultFieldType = maximumType(t1Field.getFieldType(), t2Field.getFieldType());
                    if (resultFieldType == null) {
                        return null;
                    }

                    Optional<String> resultFieldNameOptional = Optional.empty();
                    if (t1Field.getFieldNameOptional().isEmpty()) {
                        resultFieldNameOptional = t2Field.getFieldNameOptional();
                    } else if (t2Field.getFieldNameOptional().isEmpty() || (t1Field.getFieldNameOptional().equals(t2Field.getFieldNameOptional()))) {
                        resultFieldNameOptional = t1Field.getFieldNameOptional();
                    }

                    resultFieldsBuilder.add(Record.Field.of(resultFieldType, resultFieldNameOptional));
                }
                return Type.Record.fromFields(isResultNullable, resultFieldsBuilder.build());

            case ARRAY:
                final var t1ElementType = Verify.verifyNotNull(((Type.Array)t1).getElementType());
                final var t2ElementType = Verify.verifyNotNull(((Type.Array)t2).getElementType());
                final var resultElementType = maximumType(t1ElementType, t2ElementType);
                if (resultElementType == null) {
                    return null;
                }
                return new Type.Array(isResultNullable, resultElementType);

            default:
                throw new RecordCoreException("do not know how to handle type code");
        }
    }

    /**
     * Returns an equivalent {@link Type} of a primitive object.
     *
     * @param o The object to determine the type of.
     * @return An equivalent {@link Type}.
     */
    @Nonnull
    static TypeCode typeCodeFromPrimitive(@Nullable final Object o) {
        if (o instanceof ByteString || o instanceof byte[]) {
            return TypeCode.BYTES;
        }
        return getClassToTypeCodeMap().getOrDefault(o == null ? null : o.getClass(), TypeCode.UNKNOWN);
    }

    /**
     * Returns an equivalent {@link Type} of a given Java object's type.
     * @param object The object whose Java type to be checked for an equivalent {@link Type}.
     * @return The equivalent {@link Type}.
     */
    @Nonnull
    static Type fromObject(@Nullable final Object object) {
        if (object instanceof Typed) {
            return ((Typed)object).getResultType();
        }
        if (object == null) {
            return Type.nullType();
        }
        if (object instanceof List) {
            if (((List<?>)object).isEmpty()) {
                return Type.noneType();
            }
            return new Type.Array(Type.fromListObject((List<?>)object));
        }
        if (object instanceof DynamicMessage) {
            return Record.fromDescriptor(((DynamicMessage) object).getDescriptorForType());
        }
        if (object instanceof RealVector) {
            final var vector = (RealVector)object;
            final var dimensions = vector.getNumDimensions();
            final var precision = VectorUtils.getVectorPrecision(vector);
            return Type.Vector.of(false, precision, dimensions);
        }
        final var typeCode = typeCodeFromPrimitive(object);
        if (typeCode == TypeCode.NULL) {
            return Type.nullType();
        }
        if (typeCode == TypeCode.UNKNOWN) {
            return Type.any();
        }
        if (typeCode.isPrimitive()) {
            return Type.primitiveType(typeCode, false);
        }
        if (typeCode == TypeCode.UUID) {
            return Type.uuidType(false);
        }
        throw new RecordCoreException("Unable to convert value to Type")
                .addLogInfo(LogMessageKeys.VALUE, object);
    }

    @Nonnull
    private static Type fromListObject(@Nullable final List<?> list) {
        if (list == null) {
            return Type.nullType();
        }
        if (list.isEmpty()) {
            return Type.any();
        }
        final var elementsTypes = list.stream().map(Type::fromObject).collect(Collectors.toList());
        final var nonNullElementType = elementsTypes.stream().distinct().filter(type -> type != Type.nullType()).collect(Collectors.toList());
        if (nonNullElementType.size() != 1) {
            return Type.any();
        } else {
            if (elementsTypes.stream().anyMatch(type -> type == Type.nullType())) {
                return nonNullElementType.get(0).withNullability(true);
            }
            return nonNullElementType.get(0);
        }
    }

    @Nonnull
    PType toTypeProto(@Nonnull PlanSerializationContext serializationContext);

    @Nonnull
    static Type fromTypeProto(@Nonnull final PlanSerializationContext serializationContext,
                              @Nonnull final PType typeProto) {
        return (Type)PlanSerialization.dispatchFromProtoContainer(serializationContext, typeProto);
    }

    /**
     * All supported {@link Type}s.
     */
    enum TypeCode {
        UNKNOWN(null, null, true, false),
        ANY(Object.class, null, false, false),
        NULL(Void.class, null, true, false),
        BOOLEAN(Boolean.class, FieldDescriptorProto.Type.TYPE_BOOL, true, false),
        BYTES(ByteString.class, FieldDescriptorProto.Type.TYPE_BYTES, true, false),
        DOUBLE(Double.class, FieldDescriptorProto.Type.TYPE_DOUBLE, true, true),
        FLOAT(Float.class, FieldDescriptorProto.Type.TYPE_FLOAT, true, true),
        INT(Integer.class, FieldDescriptorProto.Type.TYPE_INT32, true, true),
        LONG(Long.class, FieldDescriptorProto.Type.TYPE_INT64, true, true),
        STRING(String.class, FieldDescriptorProto.Type.TYPE_STRING, true, false),
        VECTOR(RealVector.class, FieldDescriptorProto.Type.TYPE_BYTES, true, false),
        VERSION(FDBRecordVersion.class, FieldDescriptorProto.Type.TYPE_BYTES, true, false),
        ENUM(Enum.class, FieldDescriptorProto.Type.TYPE_ENUM, false, false),
        RECORD(Message.class, null, false, false),
        UUID(java.util.UUID.class, null, false, false),
        ARRAY(List.class, null, false, false),
        RELATION(null, null, false, false),
        NONE(null, null, false, false);

        /**
         * Java {@link Class} that corresponds to the {@link TypeCode}.
         */
        @Nullable
        private final Class<?> javaClass;

        /**
         * Protobuf {@link com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type} descriptor that corresponds
         * to the {@link TypeCode}.
         */
        @Nullable
        private final FieldDescriptorProto.Type protoType;

        /**
         * flag to indicate whether a {@link TypeCode} is primitive or structured.
         */
        private final boolean isPrimitive;

        /**
         * flag to indicate whether a {@link TypeCode} is numeric or not.
         */
        private final boolean isNumeric;

        /**
         * Construct a new {@link TypeCode} instance.
         * @param javaClass Java {@link Class} that corresponds to the {@link TypeCode}.
         * @param protoType Protobuf {@link com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type} descriptor that corresponds
         * @param isPrimitive <code>true</code> if a {@link TypeCode} is primitive, otherwise <code>false</code>.
         * @param isNumeric <code>true</code> if a {@link TypeCode} is numeric, otherwise <code>false</code>.
         */
        TypeCode(@Nullable final Class<?> javaClass,
                 @Nullable final FieldDescriptorProto.Type protoType,
                 final boolean isPrimitive,
                 final boolean isNumeric) {
            this.javaClass = javaClass;
            this.protoType = protoType;
            this.isPrimitive = isPrimitive;
            this.isNumeric = isNumeric;
        }

        /**
         * Returns the corresponding Java {@link Class} of the {@link Type} instance.
         *
         * @return the corresponding Java {@link Class} of the {@link Type} instance.
         */
        @Nullable
        public Class<?> getJavaClass() {
            return javaClass;
        }

        /**
         * Returns the corresponding protobuf {@link com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type} of
         * the {@link Type} instance.
         *
         * @return the corresponding protobuf {@link com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type} of
         * the {@link Type} instance.
         */
        @Nullable
        public FieldDescriptorProto.Type getProtoType() {
            return protoType;
        }

        /**
         * Checks whether a {@link Type} is primitive or structured.
         *
         * @return <code>true</code> if the {@link Type} is primitive, otherwise <code>false</code>.
         */
        public boolean isPrimitive() {
            return isPrimitive;
        }

        /**
         * Checks whether a {@link Type} is numeric.
         *
         * @return <code>true</code> if the {@link Type} is numeric, otherwise <code>false</code>.
         */
        public boolean isNumeric() {
            return isNumeric;
        }

        /**
         * Computes a mapping from Java {@link Class} to corresponding {@link TypeCode} instance.
         * @return a mapping from Java {@link Class} to corresponding {@link TypeCode} instance.
         */
        @Nonnull
        private static BiMap<Class<?>, TypeCode> computeClassToTypeCodeMap() {
            final var builder = ImmutableBiMap.<Class<?>, TypeCode>builder();
            for (final TypeCode typeCode : TypeCode.values()) {
                if (typeCode.getJavaClass() != null) {
                    builder.put(typeCode.getJavaClass(), typeCode);
                }
            }
            return builder.build();
        }

        /**
         * Generates a {@link TypeCode} that corresponds to the given protobuf
         * {@link com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type}.
         * @param protobufType The protobuf descriptor of the type.
         * @return A corresponding {@link TypeCode} instance.
         */
        @Nonnull
        public static TypeCode fromProtobufFieldDescriptor(@Nonnull final Descriptors.FieldDescriptor.Type protobufType,
                                                           @Nullable final DescriptorProtos.FieldOptions fieldOptions) {
            switch (protobufType) {
                case DOUBLE:
                    return TypeCode.DOUBLE;
                case FLOAT:
                    return TypeCode.FLOAT;
                case INT64:
                case UINT64:
                case FIXED64:
                case SFIXED64:
                case SINT64:
                    return TypeCode.LONG;
                case INT32:
                case FIXED32:
                case UINT32:
                case SFIXED32:
                case SINT32:
                    return TypeCode.INT;
                case BOOL:
                    return TypeCode.BOOLEAN;
                case STRING:
                    return TypeCode.STRING;
                case GROUP:
                case ENUM:
                    return TypeCode.ENUM;
                case MESSAGE:
                    return TypeCode.RECORD;
                case BYTES:
                {
                    if (fieldOptions != null) {
                        final var recordTypeOptions = fieldOptions.getExtension(RecordMetaDataOptionsProto.field);
                        if (recordTypeOptions.hasVectorOptions()) {
                            return TypeCode.VECTOR;
                        }
                    }
                    return TypeCode.BYTES;
                }
                default:
                    throw new IllegalArgumentException("unknown protobuf type " + protobufType);
            }
        }

        @Nonnull
        @SuppressWarnings("unused")
        public PTypeCode toProto(@Nonnull final PlanSerializationContext serializationContext) {
            switch (this) {
                case UNKNOWN:
                    return PTypeCode.UNKNOWN;
                case ANY:
                    return PTypeCode.ANY;
                case NULL:
                    return PTypeCode.NULL;
                case BOOLEAN:
                    return PTypeCode.BOOLEAN;
                case BYTES:
                    return PTypeCode.BYTES;
                case DOUBLE:
                    return PTypeCode.DOUBLE;
                case FLOAT:
                    return PTypeCode.FLOAT;
                case INT:
                    return PTypeCode.INT;
                case LONG:
                    return PTypeCode.LONG;
                case STRING:
                    return PTypeCode.STRING;
                case VERSION:
                    return PTypeCode.VERSION;
                case ENUM:
                    return PTypeCode.ENUM;
                case RECORD:
                    return PTypeCode.RECORD;
                case ARRAY:
                    return PTypeCode.ARRAY;
                case RELATION:
                    return PTypeCode.RELATION;
                case NONE:
                    return PTypeCode.NONE;
                case UUID:
                    return PTypeCode.UUID;
                default:
                    throw new RecordCoreException("unable to find type code mapping. did you forgot to add it here?");
            }
        }

        @Nonnull
        @SuppressWarnings("unused")
        public static TypeCode fromProto(@Nonnull final PlanSerializationContext serializationContext, PTypeCode typeCodeProto) {
            switch (typeCodeProto) {
                case UNKNOWN:
                    return UNKNOWN;
                case ANY:
                    return ANY;
                case NULL:
                    return NULL;
                case BOOLEAN:
                    return BOOLEAN;
                case BYTES:
                    return BYTES;
                case DOUBLE:
                    return DOUBLE;
                case FLOAT:
                    return FLOAT;
                case INT:
                    return INT;
                case LONG:
                    return LONG;
                case STRING:
                    return STRING;
                case VERSION:
                    return VERSION;
                case ENUM:
                    return ENUM;
                case RECORD:
                    return RECORD;
                case ARRAY:
                    return ARRAY;
                case UUID:
                    return UUID;
                case RELATION:
                    return RELATION;
                case NONE:
                    return NONE;
                default:
                    throw new RecordCoreException("unable to find type code proto mapping");
            }
        }
    }

    /**
     * Interface for classes that can be erased, i.e. enums, records, arrays.
     */
    interface Erasable extends Type {
        boolean isErased();
    }

    /**
     * A primitive type.
     */
    class Primitive implements Type {
        private final boolean isNullable;
        @Nonnull
        private final TypeCode typeCode;

        @Nonnull
        private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

        private Primitive(final boolean isNullable, @Nonnull final TypeCode typeCode) {
            this.isNullable = isNullable;
            this.typeCode = typeCode;
        }

        @Override
        @Nonnull
        public TypeCode getTypeCode() {
            return typeCode;
        }

        @Override
        public boolean isNullable() {
            return isNullable;
        }

        @Nonnull
        @Override
        public Type withNullability(final boolean newIsNullable) {
            return newIsNullable == isNullable ? this : primitiveType(typeCode, newIsNullable);
        }

        @Override
        public void addProtoField(@Nonnull final TypeRepository.Builder typeRepositoryBuilder,
                                  @Nonnull final DescriptorProto.Builder descriptorBuilder,
                                  final int fieldNumber,
                                  @Nonnull final String fieldName,
                                  @Nonnull final Optional<String> ignored,
                                  @Nonnull final FieldDescriptorProto.Label label) {
            final var protoType = Objects.requireNonNull(getTypeCode().getProtoType());
            descriptorBuilder.addField(FieldDescriptorProto.newBuilder()
                    .setNumber(fieldNumber)
                    .setName(fieldName)
                    .setType(protoType)
                    .setLabel(label)
                    .build());
        }

        @Override
        public int hashCode() {
            return hashCodeSupplier.get();
        }

        private int computeHashCode() {
            return Objects.hash(typeCode.name().hashCode(), isNullable);
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == null) {
                return false;
            }

            if (obj == this) {
                return true;
            }

            if (getClass() != obj.getClass()) {
                return false;
            }

            final var otherType = (Type)obj;
            return getTypeCode() == otherType.getTypeCode() && isNullable() == otherType.isNullable();
        }

        @Nonnull
        @Override
        public String toString() {
            return describe().render(DefaultExplainFormatter.forDebugging()).toString();
        }

        @Nonnull
        @Override
        public ExplainTokens describe() {
            return new ExplainTokens().addKeyword(getTypeCode().toString());
        }

        @Nonnull
        @Override
        public PPrimitiveType toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PPrimitiveType.newBuilder()
                    .setIsNullable(isNullable)
                    .setTypeCode(typeCode.toProto(serializationContext))
                    .build();
        }

        @Nonnull
        @Override
        public PType toTypeProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PType.newBuilder().setPrimitiveType(toProto(serializationContext)).build();
        }

        @Nonnull
        public static Primitive fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                          @Nonnull final PPrimitiveType primitiveTypeProto) {
            Verify.verify(primitiveTypeProto.hasIsNullable());
            return new Primitive(primitiveTypeProto.getIsNullable(),
                    TypeCode.fromProto(serializationContext, Objects.requireNonNull(primitiveTypeProto.getTypeCode())));
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PPrimitiveType, Primitive> {
            @Nonnull
            @Override
            public Class<PPrimitiveType> getProtoMessageClass() {
                return PPrimitiveType.class;
            }

            @Nonnull
            @Override
            public Primitive fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                       @Nonnull final PPrimitiveType primitiveTypeProto) {
                return Primitive.fromProto(serializationContext, primitiveTypeProto);
            }
        }
    }

    /**
     * The null type is an unresolved type meaning that an entity returning a null type should resolve the
     * type to a regular type as the runtime does not support a null-typed data producer. Note that a type can be
     * nullable but that's not the same as to be null-typed. Only the constant {@code null} is actually of type null,
     * however, that type is changed to an actual type during type resolution that then just happens to be nullable.
     * It is correct to say that the null type (just as {@link None} type) are types that have no instances.
     * It is still useful use this type for modelling purposes. Just as in Scala, the null-type is implicitly, a
     * subtype of every other type in a sense that the substitution principle holds, e.g. {@code null} can be substituted
     * for any value of type {@code int}, or {@code string}, etc...
     */
    class Null implements Type {
        @Override
        public TypeCode getTypeCode() {
            return TypeCode.NULL;
        }

        @Override
        public boolean isNullable() {
            return true;
        }

        @Nonnull
        @Override
        public Type withNullability(final boolean newIsNullable) {
            Verify.verify(newIsNullable);
            return this;
        }

        @Override
        public boolean isUnresolved() {
            return true;
        }

        @Override
        public void addProtoField(@Nonnull final TypeRepository.Builder typeRepositoryBuilder, @Nonnull final DescriptorProto.Builder descriptorBuilder, final int fieldNumber, @Nonnull final String fieldName, @Nonnull final Optional<String> typeNameOptional, @Nonnull final FieldDescriptorProto.Label label) {
            throw new RecordCoreException("should not be called");
        }

        @Nonnull
        @Override
        public String toString() {
            return describe().render(DefaultExplainFormatter.forDebugging()).toString();
        }

        @Nonnull
        @Override
        public ExplainTokens describe() {
            return new ExplainTokens().addKeyword("NULL");
        }

        @Nonnull
        @Override
        public PNullType toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PNullType.newBuilder().build();
        }

        @Nonnull
        @Override
        public PType toTypeProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PType.newBuilder().setNullType(toProto(serializationContext)).build();
        }

        @SuppressWarnings("unused")
        @Nonnull
        public static Null fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                     @Nonnull final PNullType nullTypeProto) {
            return NULL;
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PNullType, Null> {
            @Nonnull
            @Override
            public Class<PNullType> getProtoMessageClass() {
                return PNullType.class;
            }

            @Nonnull
            @Override
            public Null fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                  @Nonnull final PNullType nullTypeProto) {
                return Null.fromProto(serializationContext, nullTypeProto);
            }
        }

        @Override
        public int hashCode() {
            return getTypeCode().name().hashCode();
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof Null;
        }
    }

    final class Vector implements Type {
        private final boolean isNullable;
        private final int precision;
        private final int dimensions;

        private Vector(final boolean isNullable, final int precision, final int dimensions) {
            this.isNullable = isNullable;
            this.precision = precision;
            this.dimensions = dimensions;
        }

        @Nonnull
        @SuppressWarnings("PMD.ReplaceVectorWithList")
        public static Vector of(final boolean isNullable, final int precision, final int dimensions) {
            return new Vector(isNullable, precision, dimensions);
        }

        @Override
        public TypeCode getTypeCode() {
            return TypeCode.VECTOR;
        }

        @Override
        public boolean isPrimitive() {
            return true;
        }

        @Override
        public boolean isNullable() {
            return isNullable;
        }

        @Nonnull
        @Override
        public Type withNullability(final boolean newIsNullable) {
            if (isNullable == newIsNullable) {
                return this;
            }
            return new Vector(newIsNullable, precision, dimensions);
        }

        public int getPrecision() {
            return precision;
        }

        public int getDimensions() {
            return dimensions;
        }

        @Nonnull
        @Override
        public ExplainTokens describe() {
            final var resultExplainTokens = new ExplainTokens();
            resultExplainTokens.addKeyword(getTypeCode().toString());
            return resultExplainTokens.addOptionalWhitespace().addOpeningParen().addOptionalWhitespace()
                    .addNested(new ExplainTokens().addToString(precision).addToString(", ").addToString(dimensions)).addOptionalWhitespace()
                    .addClosingParen();
        }

        @Nonnull
        @Override
        public String toString() {
            return describe().render(DefaultExplainFormatter.forDebugging()).toString();
        }

        @Override
        public void addProtoField(@Nonnull final TypeRepository.Builder typeRepositoryBuilder,
                                  @Nonnull final DescriptorProto.Builder descriptorBuilder, final int fieldNumber,
                                  @Nonnull final String fieldName, @Nonnull final Optional<String> typeNameOptional,
                                  @Nonnull final FieldDescriptorProto.Label label) {
            final var protoType = Objects.requireNonNull(getTypeCode().getProtoType());
            FieldDescriptorProto.Builder builder = FieldDescriptorProto.newBuilder()
                    .setNumber(fieldNumber)
                    .setName(fieldName)
                    .setType(protoType)
                    .setLabel(label);
            final var fieldOptions = RecordMetaDataOptionsProto.FieldOptions.newBuilder()
                    .setVectorOptions(
                            RecordMetaDataOptionsProto.FieldOptions.VectorOptions
                                    .newBuilder()
                                    .setPrecision(precision)
                                    .setDimensions(dimensions)
                                    .build())
                    .build();
            builder.getOptionsBuilder().setExtension(RecordMetaDataOptionsProto.field, fieldOptions);
            typeNameOptional.ifPresent(builder::setTypeName);
            descriptorBuilder.addField(builder);
        }

        @Nonnull
        @Override
        public PType toTypeProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PType.newBuilder().setVectorType(toProto(serializationContext)).build();
        }

        @Nonnull
        @Override
        public PVectorType toProto(@Nonnull final PlanSerializationContext serializationContext) {
            final PVectorType.Builder vectorTypeBuilder = PVectorType.newBuilder()
                    .setIsNullable(isNullable)
                    .setDimensions(dimensions)
                    .setPrecision(precision);
            return vectorTypeBuilder.build();
        }

        @Nonnull
        @SuppressWarnings("PMD.ReplaceVectorWithList")
        public static Vector fromProto(@Nonnull final PVectorType vectorTypeProto) {
            Verify.verify(vectorTypeProto.hasIsNullable());
            return new Vector(vectorTypeProto.getIsNullable(), vectorTypeProto.getPrecision(), vectorTypeProto.getDimensions());
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PVectorType, Vector> {
            @Nonnull
            @Override
            public Class<PVectorType> getProtoMessageClass() {
                return PVectorType.class;
            }

            @Nonnull
            @Override
            @SuppressWarnings("PMD.ReplaceVectorWithList")
            public Vector fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                    @Nonnull final PVectorType vectorTypeProto) {
                return Vector.fromProto(vectorTypeProto);
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(getTypeCode().name(), precision, dimensions);
        }

        @Override
        @SuppressWarnings("PMD.ReplaceVectorWithList")
        public boolean equals(final Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final Vector vector = (Vector)o;
            return isNullable == vector.isNullable
                    && precision == vector.precision
                    && dimensions == vector.dimensions;
        }
    }

    /**
     * The none type is an unresolved type meaning that an entity returning a none type should resolve the
     * type to a regular type as the runtime does not support a none-typed data producer. Only the empty array constant
     * is actually of type {@code none}, however, that type is changed to an actual type during type resolution (to an
     * array of some regular type).
     * It is correct to say that the none type (just as {@link Null} type) are types that have no instances.
     * It is still useful use this type for modelling purposes. Just as in Scala, the none-type is implicitly, a
     * subtype of every other type in a sense that the substitution principle holds, e.g. {@code none} can be substituted
     * for any value of an array type.
     */
    class None implements Type {
        @Override
        public TypeCode getTypeCode() {
            return TypeCode.NONE;
        }

        @Override
        public boolean isNullable() {
            return false;
        }

        @Nonnull
        @Override
        public Type withNullability(final boolean newIsNullable) {
            Verify.verify(!newIsNullable);
            return this;
        }

        @Override
        public boolean isUnresolved() {
            return true;
        }

        @Override
        public void addProtoField(@Nonnull final TypeRepository.Builder typeRepositoryBuilder, @Nonnull final DescriptorProto.Builder descriptorBuilder, final int fieldNumber, @Nonnull final String fieldName, @Nonnull final Optional<String> typeNameOptional, @Nonnull final FieldDescriptorProto.Label label) {
            throw new RecordCoreException("should not be called");
        }

        @Nonnull
        @Override
        public String toString() {
            return describe().render(DefaultExplainFormatter.forDebugging()).toString();
        }

        @Nonnull
        @Override
        public ExplainTokens describe() {
            return new ExplainTokens().addKeyword("NONE");
        }

        @Nonnull
        @Override
        public PNoneType toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PNoneType.newBuilder().build();
        }

        @Nonnull
        @Override
        public PType toTypeProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PType.newBuilder().setNoneType(toProto(serializationContext)).build();
        }

        @SuppressWarnings("unused")
        @Nonnull
        public static None fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                     @Nonnull final PNoneType noneTypeProto) {
            return NONE;
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PNoneType, None> {
            @Nonnull
            @Override
            public Class<PNoneType> getProtoMessageClass() {
                return PNoneType.class;
            }

            @Nonnull
            @Override
            public None fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                  @Nonnull final PNoneType noneTypeProto) {
                return None.fromProto(serializationContext, noneTypeProto);
            }
        }

        @Override
        public int hashCode() {
            return getTypeCode().name().hashCode();
        }

        @Override
        public boolean equals(final Object other) {
            return other instanceof None;
        }
    }

    /**
     * Special {@link Type} that is undefined.
     */
    class Any implements Type {
        /**
         * Memoized hash function.
         */
        @Nonnull
        private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

        @Nonnull
        private static final Any INSTANCE = new Any();

        private int computeHashCode() {
            return Objects.hash(getTypeCode().name().hashCode(), isNullable());
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public TypeCode getTypeCode() {
            return TypeCode.ANY;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isNullable() {
            return true;
        }

        @Nonnull
        @Override
        public Any withNullability(final boolean newIsNullable) {
            Verify.verify(newIsNullable);
            return this;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void addProtoField(@Nonnull final TypeRepository.Builder typeRepositoryBuilder,
                                  @Nonnull final DescriptorProto.Builder descriptorBuilder,
                                  final int fieldNumber,
                                  @Nonnull final String fieldName,
                                  @Nonnull final Optional<String> typeNameOptional,
                                  @Nonnull final FieldDescriptorProto.Label label) {
            throw new UnsupportedOperationException("type any cannot be represented in protobuf");
        }

        @Override
        public int hashCode() {
            return hashCodeSupplier.get();
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == null) {
                return false;
            }

            if (this == obj) {
                return true;
            }

            if (getClass() != obj.getClass()) {
                return false;
            }

            final var otherType = (Type)obj;
            return getTypeCode() == otherType.getTypeCode() && isNullable() == otherType.isNullable();
        }

        @Nonnull
        @Override
        public String toString() {
            return describe().render(DefaultExplainFormatter.forDebugging()).toString();
        }

        @Nonnull
        @Override
        public ExplainTokens describe() {
            return new ExplainTokens().addKeyword(getTypeCode().toString());
        }

        @Nonnull
        @Override
        public PAnyType toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PAnyType.newBuilder().build();
        }

        @Nonnull
        @Override
        public PType toTypeProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PType.newBuilder().setAnyType(toProto(serializationContext)).build();
        }

        @Nonnull
        @SuppressWarnings("unused")
        public static Any fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                    @Nonnull final PAnyType anyTypeProto) {
            return any();
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PAnyType, Any> {
            @Nonnull
            @Override
            public Class<PAnyType> getProtoMessageClass() {
                return PAnyType.class;
            }

            @Nonnull
            @Override
            public Any fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                 @Nonnull final PAnyType anyTypeProto) {
                return Any.fromProto(serializationContext, anyTypeProto);
            }
        }
    }

    @Nonnull
    static Any any() {
        return Any.INSTANCE;
    }

    /**
     * Special {@link Type.Record} that is undefined.
     */
    class AnyRecord implements Type, Erasable {
        private final boolean isNullable;

        @Nonnull
        private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

        public AnyRecord(final boolean isNullable) {
            this.isNullable = isNullable;
        }

        private int computeHashCode() {
            return Objects.hash(getTypeCode().name().hashCode(), isNullable());
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public TypeCode getTypeCode() {
            return TypeCode.RECORD;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isNullable() {
            return isNullable;
        }

        @Nonnull
        @Override
        public AnyRecord withNullability(final boolean newIsNullable) {
            if (newIsNullable == isNullable) {
                return this;
            } else {
                return new AnyRecord(newIsNullable);
            }
        }

        @Override
        public boolean isErased() {
            return true;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void addProtoField(@Nonnull final TypeRepository.Builder typeRepositoryBuilder,
                                  @Nonnull final DescriptorProto.Builder descriptorBuilder,
                                  final int fieldNumber,
                                  @Nonnull final String fieldName,
                                  @Nonnull final Optional<String> typeNameOptional,
                                  @Nonnull final FieldDescriptorProto.Label label) {
            throw new UnsupportedOperationException("type any cannot be represented in protobuf");
        }

        @Override
        public int hashCode() {
            return hashCodeSupplier.get();
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == null) {
                return false;
            }

            if (this == obj) {
                return true;
            }

            if (getClass() != obj.getClass()) {
                return false;
            }

            final var otherType = (Type)obj;
            return getTypeCode() == otherType.getTypeCode() && isNullable() == otherType.isNullable();
        }

        @Nonnull
        @Override
        public String toString() {
            return describe().render(DefaultExplainFormatter.forDebugging()).toString();
        }

        @Nonnull
        @Override
        public ExplainTokens describe() {
            return new ExplainTokens().addKeyword(getTypeCode().toString());
        }

        @Nonnull
        @Override
        public PAnyRecordType toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PAnyRecordType.newBuilder()
                    .setIsNullable(isNullable)
                    .build();
        }

        @Nonnull
        @Override
        public PType toTypeProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PType.newBuilder().setAnyRecordType(toProto(serializationContext)).build();
        }

        @Nonnull
        @SuppressWarnings("unused")
        public static AnyRecord fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                          @Nonnull final PAnyRecordType anyTypeProto) {
            Verify.verify(anyTypeProto.hasIsNullable());
            return new AnyRecord(anyTypeProto.getIsNullable());
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PAnyRecordType, AnyRecord> {
            @Nonnull
            @Override
            public Class<PAnyRecordType> getProtoMessageClass() {
                return PAnyRecordType.class;
            }

            @Nonnull
            @Override
            public AnyRecord fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                 @Nonnull final PAnyRecordType anyTypeProto) {
                return AnyRecord.fromProto(serializationContext, anyTypeProto);
            }
        }
    }

    /**
     * An enumeration type.
     */
    class Enum implements Type {
        final boolean isNullable;
        @Nullable
        final List<EnumValue> enumValues;
        @Nullable
        final String name;
        @Nullable
        final String storageName;

        /**
         * Memoized hash function.
         */
        @Nonnull
        private final Supplier<Integer> hashFunctionSupplier = Suppliers.memoize(this::computeHashCode);

        private Enum(final boolean isNullable,
                     @Nullable final List<EnumValue> enumValues) {
            this(isNullable, enumValues, null, null);
        }

        public Enum(final boolean isNullable,
                    @Nullable final List<EnumValue> enumValues,
                    @Nullable final String name,
                    @Nullable final String storageName) {
            this.isNullable = isNullable;
            this.enumValues = enumValues;
            this.name = name;
            this.storageName = storageName;
        }

        @Override
        public TypeCode getTypeCode() {
            return TypeCode.ENUM;
        }

        /**
         * Checks whether the {@link Record} type instance is erased or not.
         * @return <code>true</code> if the {@link Record} type is erased, other <code>false</code>.
         */
        boolean isErased() {
            return enumValues == null;
        }

        @Nonnull
        public List<EnumValue> getEnumValues() {
            return Objects.requireNonNull(enumValues);
        }

        @Override
        public boolean isNullable() {
            return isNullable;
        }

        @Nonnull
        @Override
        public Enum withNullability(final boolean newIsNullable) {
            if (newIsNullable == isNullable()) {
                return this;
            }
            return new Enum(newIsNullable, enumValues, name, storageName);
        }

        @Nullable
        public String getName() {
            return name;
        }

        @Nullable
        public String getStorageName() {
            return storageName;
        }

        @Override
        public void defineProtoType(@Nonnull final TypeRepository.Builder typeRepositoryBuilder) {
            Verify.verify(!isErased());
            final var typeName = storageName == null ? ProtoUtils.uniqueTypeName() : storageName;
            final var enumDescriptorProtoBuilder = DescriptorProtos.EnumDescriptorProto.newBuilder();
            enumDescriptorProtoBuilder.setName(typeName);

            for (final var enumValue : Objects.requireNonNull(enumValues)) {
                enumDescriptorProtoBuilder.addValue(DescriptorProtos.EnumValueDescriptorProto.newBuilder()
                        .setName(enumValue.getStorageName())
                        .setNumber(enumValue.getNumber()));
            }

            typeRepositoryBuilder.addEnumType(enumDescriptorProtoBuilder.build());
            typeRepositoryBuilder.registerTypeToTypeNameMapping(this, typeName);
        }

        @Override
        public void addProtoField(@Nonnull final TypeRepository.Builder typeRepositoryBuilder,
                                  @Nonnull final DescriptorProto.Builder descriptorBuilder,
                                  final int fieldNumber,
                                  @Nonnull final String fieldName,
                                  @Nonnull final Optional<String> typeNameOptional,
                                  @Nonnull final FieldDescriptorProto.Label label) {
            final var protoType = Objects.requireNonNull(getTypeCode().getProtoType());
            FieldDescriptorProto.Builder builder = FieldDescriptorProto.newBuilder()
                    .setNumber(fieldNumber)
                    .setName(fieldName)
                    .setType(protoType)
                    .setLabel(label);
            typeNameOptional.ifPresent(builder::setTypeName);
            descriptorBuilder.addField(builder);
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == null) {
                return false;
            }

            if (this == obj) {
                return true;
            }

            if (getClass() != obj.getClass()) {
                return false;
            }

            final var otherType = (Enum)obj;
            return getTypeCode() == otherType.getTypeCode() && isNullable() == otherType.isNullable()
                    && Objects.equals(enumValues, otherType.enumValues);
        }

        private int computeHashCode() {
            return Objects.hash(isNullable, enumValues);
        }

        @Override
        public int hashCode() {
            return hashFunctionSupplier.get();
        }

        @Nonnull
        @Override
        public String toString() {
            return describe().render(DefaultExplainFormatter.forDebugging()).toString();
        }

        @Nonnull
        @Override
        public ExplainTokens describe() {
            final var resultExplainTokens = new ExplainTokens();
            resultExplainTokens.addKeyword(getTypeCode().toString());
            if (isErased()) {
                return resultExplainTokens;
            }

            return resultExplainTokens
                    .addOpeningAngledBracket()
                    .addToStrings(Objects.requireNonNull(enumValues))
                    .addClosingAngledBracket();
        }

        @Nonnull
        @SuppressWarnings("PMD.UnnecessaryFullyQualifiedName") // false positive
        public static <T extends java.lang.Enum<T>> Enum forJavaEnum(@Nonnull final Class<T> enumClass) {
            final var enumValuesBuilder = ImmutableList.<EnumValue>builder();
            T[] enumConstants = enumClass.getEnumConstants();
            for (int i = 0; i < enumConstants.length; i++) {
                final var enumConstant = enumConstants[i];
                enumValuesBuilder.add(EnumValue.from(enumConstant.name(), i));
            }
            return new Enum(false, enumValuesBuilder.build(), null, null);
        }

        @Nonnull
        public static Enum fromDescriptor(boolean isNullable, @Nonnull Descriptors.EnumDescriptor enumDescriptor) {
            return Enum.fromValues(isNullable, enumValuesFromProto(enumDescriptor.getValues()));
        }

        @Nonnull
        public static Enum fromDescriptorPreservingNames(boolean isNullable, @Nonnull Descriptors.EnumDescriptor enumDescriptor) {
            return new Type.Enum(isNullable, enumValuesFromProto(enumDescriptor.getValues()), ProtoUtils.toUserIdentifier(enumDescriptor.getName()), enumDescriptor.getName());
        }

        @Nonnull
        public static List<EnumValue> enumValuesFromProto(@Nonnull final List<Descriptors.EnumValueDescriptor> enumValueDescriptors) {
            return enumValueDescriptors
                    .stream()
                    .map(enumValueDescriptor -> new EnumValue(ProtoUtils.toUserIdentifier(enumValueDescriptor.getName()), enumValueDescriptor.getName(), enumValueDescriptor.getNumber()))
                    .collect(ImmutableList.toImmutableList());
        }

        @Nonnull
        @Override
        public PEnumType toProto(@Nonnull final PlanSerializationContext serializationContext) {
            final PEnumType.Builder enumTypeProtoBuilder = PEnumType.newBuilder();
            enumTypeProtoBuilder.setIsNullable(isNullable);
            for (final EnumValue enumValue : Objects.requireNonNull(enumValues)) {
                enumTypeProtoBuilder.addEnumValues(enumValue.toProto(serializationContext));
            }
            if (name != null) {
                enumTypeProtoBuilder.setName(name);
            }
            if (storageName != null && !Objects.equals(storageName, name)) {
                enumTypeProtoBuilder.setStorageName(storageName);
            }
            return enumTypeProtoBuilder.build();
        }

        @Nonnull
        @Override
        public PType toTypeProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PType.newBuilder().setEnumType(toProto(serializationContext)).build();
        }

        @Nonnull
        public static Enum fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                     @Nonnull final PEnumType enumTypeProto) {
            Verify.verify(enumTypeProto.hasIsNullable());
            final ImmutableList.Builder<EnumValue> enumValuesBuilder = ImmutableList.builder();
            for (int i = 0; i < enumTypeProto.getEnumValuesCount(); i ++) {
                enumValuesBuilder.add(EnumValue.fromProto(serializationContext, enumTypeProto.getEnumValues(i)));
            }
            final ImmutableList<EnumValue> enumValues = enumValuesBuilder.build();
            Verify.verify(!enumValues.isEmpty());
            String name = PlanSerialization.getFieldOrNull(enumTypeProto, PEnumType::hasName, PEnumType::getName);
            String storageName = enumTypeProto.hasStorageName() ? enumTypeProto.getStorageName() : name;
            return new Enum(enumTypeProto.getIsNullable(), enumValues, name, storageName);
        }

        @Nonnull
        public static Type.Enum fromValues(boolean isNullable, @Nonnull List<EnumValue> enumValues) {
            return new Type.Enum(isNullable, enumValues);
        }

        @Nonnull
        public static Type.Enum fromValuesWithName(@Nonnull String name, boolean isNullable, @Nonnull List<EnumValue> enumValues) {
            return new Type.Enum(isNullable, enumValues, name, ProtoUtils.toProtoBufCompliantName(name));
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PEnumType, Enum> {
            @Nonnull
            @Override
            public Class<PEnumType> getProtoMessageClass() {
                return PEnumType.class;
            }

            @Nonnull
            @Override
            public Enum fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                  @Nonnull final PEnumType enumTypeProto) {
                return Enum.fromProto(serializationContext, enumTypeProto);
            }
        }

        /**
         * A member value of an enumeration.
         */
        public static class EnumValue implements PlanSerializable {
            @Nonnull
            final String name;
            @Nonnull
            final String storageName;
            final int number;

            EnumValue(@Nonnull final String name, @Nonnull String storageName, final int number) {
                this.name = name;
                this.storageName = storageName;
                this.number = number;
            }

            @Nonnull
            public String getName() {
                return name;
            }

            @Nonnull
            public String getStorageName() {
                return storageName;
            }

            public int getNumber() {
                return number;
            }

            @Override
            public boolean equals(final Object o) {
                if (this == o) {
                    return true;
                }
                if (o == null || getClass() != o.getClass()) {
                    return false;
                }
                final EnumValue enumValue = (EnumValue)o;
                return number == enumValue.number && name.equals(enumValue.name);
            }

            @Override
            public int hashCode() {
                return Objects.hash(name, number);
            }

            @Nonnull
            @Override
            public String toString() {
                return name + '(' + number + ')';
            }

            @Nonnull
            @Override
            public PEnumType.PEnumValue toProto(@Nonnull final PlanSerializationContext serializationContext) {
                PEnumType.PEnumValue.Builder enumValueBuilder = PEnumType.PEnumValue.newBuilder()
                        .setName(name)
                        .setNumber(number);
                if (!Objects.equals(storageName, name)) {
                    enumValueBuilder.setStorageName(storageName);
                }
                return enumValueBuilder.build();
            }

            @Nonnull
            @SuppressWarnings("unused")
            public static EnumValue fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                              @Nonnull final PEnumType.PEnumValue enumValueProto) {
                final String name = enumValueProto.getName();
                final String storageName = enumValueProto.hasStorageName() ? enumValueProto.getStorageName() : name;
                return new EnumValue(name, storageName, enumValueProto.getNumber());
            }

            @Nonnull
            public static EnumValue from(@Nonnull String name, int number) {
                return new EnumValue(name, ProtoUtils.toProtoBufCompliantName(name), number);
            }
        }
    }

    /**
     * A structured {@link Type} that contains a list of {@link Field} types.
     */
    class Record implements Type, Erasable {
        @Nullable
        private final String name;
        @Nullable
        private final String storageName;

        /**
         * indicates whether the {@link Record} type instance is nullable or not.
         */
        private final boolean isNullable;

        /**
         * list of {@link Field} types.
         */
        @Nullable
        private final List<Field> fields;

        /**
         * function that returns a mapping between field names and their {@link Field}s.
         */
        @Nonnull
        private final Supplier<Map<String, Field>> fieldNameFieldMapSupplier;

        @Nonnull
        private final Supplier<Map<String, Integer>> fieldNameToOrdinalSupplier;

        @Nonnull
        private final Supplier<Map<Integer, Integer>> fieldIndexToOrdinalSupplier;

        /**
         * function that returns a list of {@link Field} types.
         */
        @Nonnull
        private final Supplier<List<Type>> elementTypesSupplier;

        /**
         * Memoized hash function.
         */
        @Nonnull
        private final Supplier<Integer> hashFunctionSupplier = Suppliers.memoize(this::computeHashCode);

        private int computeHashCode() {
            return Objects.hash(getTypeCode().name().hashCode(), isNullable(), fields);
        }

        /**
         * Constructs a new {@link Record} using a list of {@link Field}s.
         * @param isNullable True if the record type is nullable, otherwise false.
         * @param normalizedFields The list of {@link Record} {@link Field}s.
         */
        protected Record(final boolean isNullable, @Nullable final List<Field> normalizedFields) {
            this(null, null, isNullable, normalizedFields);
        }

        /**
         * Constructs a new {@link Record} using a list of {@link Field}s and an explicit name.
         * @param name The name of the record.
         * @param isNullable True if the record type is nullable, otherwise false.
         * @param normalizedFields The list of {@link Record} {@link Field}s.
         */
        protected Record(@Nullable final String name, @Nullable final String storageName, final boolean isNullable, @Nullable final List<Field> normalizedFields) {
            this.name = name;
            this.storageName = storageName;
            this.isNullable = isNullable;
            this.fields = normalizedFields;
            this.fieldNameFieldMapSupplier = Suppliers.memoize(this::computeFieldNameFieldMap);
            this.fieldNameToOrdinalSupplier = Suppliers.memoize(this::computeFieldNameToOrdinal);
            this.fieldIndexToOrdinalSupplier = Suppliers.memoize(this::computeFieldIndexToOrdinal);
            this.elementTypesSupplier = Suppliers.memoize(this::computeElementTypes);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public TypeCode getTypeCode() {
            return TypeCode.RECORD;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isNullable() {
            return isNullable;
        }

        @Nonnull
        @Override
        public Record withNullability(final boolean newIsNullable) {
            if (isNullable == newIsNullable) {
                return this;
            }
            return new Record(name, storageName, newIsNullable, fields);
        }

        @Nonnull
        public Record withName(@Nonnull final String name) {
            return withNameAndStorageName(name, ProtoUtils.toProtoBufCompliantName(name));
        }

        @Nonnull
        public Record withNameAndStorageName(@Nonnull final String name, @Nonnull final String storageName) {
            return new Record(name, storageName, isNullable, fields);
        }

        @Nullable
        public String getName() {
            return name;
        }

        @Nullable
        public String getStorageName() {
            return storageName;
        }

        /**
         * Returns the list of {@link Record} {@link Field}s.
         * @return the list of {@link Record} {@link Field}s.
         */
        @Nonnull
        public List<Field> getFields() {
            return Objects.requireNonNull(fields);
        }

        @Nonnull
        public Field getField(int index) {
            return Objects.requireNonNull(getFields().get(index));
        }

        @Nonnull
        public Map<String, Integer> getFieldNameToOrdinalMap() {
            return fieldNameToOrdinalSupplier.get();
        }

        @Nonnull
        public Map<Integer, Integer> getFieldIndexToOrdinalMap() {
            return fieldIndexToOrdinalSupplier.get();
        }

        /**
         * Returns the list of {@link Field} {@link Type}s.
         * @return the list of {@link Field} {@link Type}s.
         */
        @Nullable
        public List<Type> getElementTypes() {
            return elementTypesSupplier.get();
        }

        /**
         * Computes the list of {@link Field} {@link Type}s.
         * @return the list of {@link Field} {@link Type}s.
         */
        private List<Type> computeElementTypes() {
            return Objects.requireNonNull(fields)
                    .stream()
                    .map(Field::getFieldType)
                    .collect(ImmutableList.toImmutableList());
        }

        /**
         * Returns a mapping from {@link Field} names to their {@link Type}s.
         * @return a mapping from {@link Field} names to their {@link Type}s.
         */
        @Nonnull
        public Map<String, Field> getFieldNameFieldMap() {
            return fieldNameFieldMapSupplier.get();
        }


        /**
         * Computes a mapping from {@link Field} names to their {@link Type}s.
         * @return a mapping from {@link Field} names to their {@link Type}s.
         */
        @SuppressWarnings("OptionalGetWithoutIsPresent")
        private Map<String, Field> computeFieldNameFieldMap() {
            return Objects.requireNonNull(fields)
                    .stream()
                    .collect(ImmutableMap.toImmutableMap(field -> field.getFieldNameOptional().get(), Function.identity()));
        }

        /**
         * Compute a mapping from {@link Field} to their ordinal positions in their {@link Type}.
         * @return a mapping from {@link Field} to their ordinal positions in their {@link Type}.
         */
        @Nonnull
        @SuppressWarnings("OptionalGetWithoutIsPresent")
        private Map<String, Integer> computeFieldNameToOrdinal() {
            return IntStream
                    .range(0, Objects.requireNonNull(fields).size())
                    .boxed()
                    .collect(ImmutableMap.toImmutableMap(id -> fields.get(id).getFieldNameOptional().get(), Function.identity()));
        }

        /**
         * Compute a mapping from {@link Field} to their ordinal positions in their {@link Type}.
         * @return a mapping from {@link Field} to their ordinal positions in their {@link Type}.
         */
        @Nonnull
        @SuppressWarnings("OptionalGetWithoutIsPresent")
        private Map<Integer, Integer> computeFieldIndexToOrdinal() {
            return IntStream
                    .range(0, Objects.requireNonNull(fields).size())
                    .boxed()
                    .collect(ImmutableMap.toImmutableMap(id -> fields.get(id).getFieldIndexOptional().get(), Function.identity()));
        }

        /**
         * Checks whether the {@link Record} type instance is erased or not.
         * @return <code>true</code> if the {@link Record} type is erased, other <code>false</code>.
         */
        @Override
        public boolean isErased() {
            return fields == null;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void defineProtoType(final TypeRepository.Builder typeRepositoryBuilder) {
            Objects.requireNonNull(fields);
            final var typeName = storageName == null ? ProtoUtils.uniqueTypeName() : storageName;
            final var recordMsgBuilder = DescriptorProto.newBuilder();
            recordMsgBuilder.setName(typeName);

            for (final var field : fields) {
                final var fieldType = field.getFieldType();
                final var fieldName = field.getFieldStorageName();
                fieldType.addProtoField(typeRepositoryBuilder, recordMsgBuilder,
                        field.getFieldIndex(),
                        fieldName,
                        typeRepositoryBuilder.defineAndResolveType(fieldType),
                        FieldDescriptorProto.Label.LABEL_OPTIONAL);
            }
            typeRepositoryBuilder.addMessageType(recordMsgBuilder.build());
            typeRepositoryBuilder.registerTypeToTypeNameMapping(this, typeName);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void addProtoField(@Nonnull final TypeRepository.Builder typeRepositoryBuilder,
                                  @Nonnull final DescriptorProto.Builder descriptorBuilder,
                                  final int fieldNumber,
                                  @Nonnull final String fieldName,
                                  @Nonnull final Optional<String> typeNameOptional,
                                  @Nonnull final FieldDescriptorProto.Label label) {
            final var fieldDescriptorProto = FieldDescriptorProto.newBuilder();
            fieldDescriptorProto
                    .setName(fieldName)
                    .setNumber(fieldNumber)
                    .setLabel(label);
            typeNameOptional.ifPresent(fieldDescriptorProto::setTypeName);
            descriptorBuilder.addField(fieldDescriptorProto.build());
        }

        @Override
        public int hashCode() {
            return hashFunctionSupplier.get();
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == null) {
                return false;
            }

            if (obj == this) {
                return true;
            }

            if (getClass() != obj.getClass()) {
                return false;
            }

            final var otherType = (Record)obj;
            return getTypeCode() == otherType.getTypeCode() && isNullable() == otherType.isNullable() &&
                   ((isErased() && otherType.isErased()) ||
                    (Objects.requireNonNull(fields).equals(otherType.fields)));
        }

        @Nonnull
        @Override
        public String toString() {
            return describe().render(DefaultExplainFormatter.forDebugging()).toString();
        }

        @Nonnull
        @Override
        public ExplainTokens describe() {
            final var resultExplainTokens = new ExplainTokens();
            if (isErased()) {
                return resultExplainTokens.addKeyword(getTypeCode().toString());
            }

            int i = 0;
            for (final var field : getFields()) {
                final Optional<String> fieldNameOptional = field.getFieldNameOptional();
                if (fieldNameOptional.isPresent()) {
                    resultExplainTokens.addNested(field.getFieldType().describe())
                            .addWhitespace()
                            .addKeyword("AS")
                            .addWhitespace()
                            .addIdentifier(fieldNameOptional.get());
                } else {
                    resultExplainTokens.addNested(field.getFieldType().describe());
                }

                if (i + 1 < getFields().size()) {
                    resultExplainTokens.addCommaAndWhiteSpace();
                }
                i ++;
            }
            return resultExplainTokens;
        }

        @Nonnull
        @Override
        public PRecordType toProto(@Nonnull final PlanSerializationContext serializationContext) {
            final Integer referenceId = serializationContext.lookupReferenceIdForRecordType(this);
            if (referenceId == null) {
                final PRecordType.Builder recordTypeProtoBuilder =
                        PRecordType.newBuilder()
                                .setReferenceId(serializationContext.registerReferenceIdForRecordType(this));
                if (name != null) {
                    recordTypeProtoBuilder.setName(name);
                }
                if (!Objects.equals(name, storageName) && storageName != null) {
                    recordTypeProtoBuilder.setStorageName(storageName);
                }
                recordTypeProtoBuilder.setIsNullable(isNullable);

                for (final Field field : Objects.requireNonNull(fields)) {
                    recordTypeProtoBuilder.addFields(field.toProto(serializationContext));
                }

                return recordTypeProtoBuilder.build();
            } else {
                return PRecordType.newBuilder().setReferenceId(referenceId).build();
            }
        }

        @Nonnull
        @Override
        public PType toTypeProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PType.newBuilder().setRecordType(toProto(serializationContext)).build();
        }

        @Nonnull
        public static Record fromProto(@Nonnull final PlanSerializationContext serializationContext, @Nonnull final PRecordType recordTypeProto) {
            Verify.verify(recordTypeProto.hasReferenceId());
            final int referenceId = recordTypeProto.getReferenceId();
            Type.Record type = serializationContext.lookupRecordTypeForReferenceId(referenceId);
            if (type != null) {
                return type;
            }

            Verify.verify(recordTypeProto.hasIsNullable());
            final ImmutableList.Builder<Field> fieldsBuilder = ImmutableList.builder();
            for (int i = 0; i < recordTypeProto.getFieldsCount(); i ++) {
                fieldsBuilder.add(Field.fromProto(serializationContext, recordTypeProto.getFields(i)));
            }
            final ImmutableList<Field> fields = fieldsBuilder.build();
            final String name = recordTypeProto.hasName() ? recordTypeProto.getName() : null;
            final String storageName = recordTypeProto.hasStorageName() ? recordTypeProto.getStorageName() : name;
            type = new Record(name, storageName, recordTypeProto.getIsNullable(), fields);
            serializationContext.registerReferenceIdForRecordType(type, referenceId);
            return type;
        }

        /**
         * Creates a new erased {@link Record} type instance and returns it.
         *
         * @return a new erased {@link Record} type instance.
         */
        @Nonnull
        public static Record erased() {
            return new Record(true, null);
        }

        /**
         * Creates a new <i>nullable</i> {@link Record} type instance using the given list of {@link Field}s.
         *
         * @param fields The list of {@link Field}s used to create the new {@link Record} type instance.
         * @return a new <i>nullable</i> {@link Record} type instance using the given list of {@link Field}s.
         */
        @Nonnull
        public static Record fromFields(@Nonnull final List<Field> fields) {
            return fromFields(true, fields);
        }

        /**
         * Creates a new {@link Record} type instance using the given list of {@link Field}s.
         *
         * @param isNullable True, if the {@link Record} type instance should be nullable, otherwise <code>false</code>.
         * @param fields The list of {@link Field}s used to create the new {@link Record} type instance.
         * @return a new {@link Record} type instance using the given list of {@link Field}s.
         */
        @Nonnull
        public static Record fromFields(final boolean isNullable, @Nonnull final List<Field> fields) {
            return new Record(isNullable, normalizeFields(fields));
        }

        @Nonnull
        public static Record fromFieldsWithName(@Nonnull String name, final boolean isNullable, @Nonnull final List<Field> fields) {
            return new Record(name, ProtoUtils.toProtoBufCompliantName(name), isNullable, normalizeFields(fields));
        }

        /**
         * Creates a new <i>nullable</i> {@link Record} type instance using the given map of field names to their protobuf
         * {@link com.google.protobuf.Descriptors.FieldDescriptor}s.
         *
         * @param fieldDescriptorMap A map of field names to their protobuf {@link com.google.protobuf.Descriptors.FieldDescriptor}s.
         *
         * @return a new <i>nullable</i> {@link Record} type instance using the given map of field names to their protobuf
         * {@link com.google.protobuf.Descriptors.FieldDescriptor}s.
         */
        @Nonnull
        public static Record fromFieldDescriptorsMap(@Nonnull final Map<String, Descriptors.FieldDescriptor> fieldDescriptorMap) {
            return fromFieldDescriptorsMap(false, fieldDescriptorMap);
        }

        /**
         * Creates a new {@link Record} type instance using the given map of field names to their protobuf
         * {@link com.google.protobuf.Descriptors.FieldDescriptor}s.
         *
         * @param isNullable True, if the {@link Record} type instance should be nullable, otherwise <code>false</code>.
         * @param fieldDescriptorMap A map of field names to their protobuf {@link com.google.protobuf.Descriptors.FieldDescriptor}s.
         * @return a new {@link Record} type instance using the given map of field names to their protobuf
         * {@link com.google.protobuf.Descriptors.FieldDescriptor}s.
         */
        @Nonnull
        public static Record fromFieldDescriptorsMap(final boolean isNullable, @Nonnull final Map<String, Descriptors.FieldDescriptor> fieldDescriptorMap) {
            return fromFields(isNullable, fieldsFromDescriptorMap(fieldDescriptorMap, false));
        }

        @Nonnull
        private static List<Field> fieldsFromDescriptorMap(@Nonnull final Map<String, Descriptors.FieldDescriptor> fieldDescriptorMap, boolean preserveNames) {
            final var fieldsBuilder = ImmutableList.<Field>builder();
            for (final var entry : Objects.requireNonNull(fieldDescriptorMap).entrySet()) {
                final var fieldDescriptor = entry.getValue();
                fieldsBuilder.add(Field.fromDescriptor(fieldDescriptor, preserveNames));
            }
            return fieldsBuilder.build();
        }

        /**
         * Translates a protobuf {@link com.google.protobuf.Descriptors.Descriptor} to a corresponding {@link Record} object.
         *
         * @param descriptor The protobuf {@link com.google.protobuf.Descriptors.Descriptor} to translate.
         * @return A {@link Record} object that corresponds to the protobuf {@link com.google.protobuf.Descriptors.Descriptor}.
         */
        @Nonnull
        public static Record fromDescriptor(final Descriptors.Descriptor descriptor) {
            return fromFieldDescriptorsMap(toFieldDescriptorMap(descriptor.getFields()));
        }

        @Nonnull
        public static Record fromDescriptorPreservingName(final Descriptors.Descriptor descriptor) {
            return new Record(ProtoUtils.toUserIdentifier(descriptor.getName()), descriptor.getName(), false,
                    fieldsFromDescriptorMap(toFieldDescriptorMap(descriptor.getFields()), true));
        }

        /**
         * Translates a list of {@link com.google.protobuf.Descriptors.FieldDescriptor}s to a mapping between field name
         * and the field itself.
         *
         * @param fieldDescriptors list of {@link com.google.protobuf.Descriptors.FieldDescriptor}s to map.
         * @return a mapping between field name and the field itself.
         */
        @Nonnull
        public static Map<String, Descriptors.FieldDescriptor> toFieldDescriptorMap(@Nonnull final List<Descriptors.FieldDescriptor> fieldDescriptors) {
            return fieldDescriptors
                    .stream()
                    .collect(ImmutableMap.toImmutableMap(Descriptors.FieldDescriptor::getName, fieldDescriptor -> fieldDescriptor));
        }

        /**
         * Normalizes a list of {@link Field}s such that their names and indices are consistent.
         *
         * @param fields The list of {@link Field}s to normalize.
         * @return a list of normalized {@link Field}s.
         */
        @Nullable
        private static List<Field> normalizeFields(@Nullable final List<Field> fields) {
            if (fields == null) {
                return null;
            }

            Set<Integer> fieldIndexesSeen = Sets.newHashSet();
            boolean override = false;
            for (final var field : fields) {
                if (field.getFieldNameOptional().isEmpty() || field.getFieldIndexOptional().isEmpty()) {
                    override = true;
                    break;
                }
                if (field.fieldIndexOptional.isPresent() && !fieldIndexesSeen.add(field.getFieldIndex())) {
                    override = true;
                    break;
                }
            }

            //
            // If any field info is missing, the type that is about to be constructed comes from a constructing
            // code path. We should be able to just set these field names and indexes as we wish.
            //
            Set<String> fieldNamesSeen = Sets.newHashSet();
            final ImmutableList.Builder<Field> resultFieldsBuilder = ImmutableList.builder();
            for (int i = 0; i < fields.size(); i++) {
                final var field = fields.get(i);
                final Field fieldToBeAdded;
                if (!override) {
                    fieldToBeAdded = field;
                } else {
                    final var explicitFieldName =
                            field.getFieldNameOptional()
                                    .flatMap(fieldName -> Field.isAutoGenerated(fieldName)
                                                          ? Optional.empty()
                                                          : Optional.of(fieldName))
                                    .orElse("_" + i);
                    final var fieldStorageName =
                            field.getFieldStorageNameOptional()
                                    .orElseGet(() -> ProtoUtils.toProtoBufCompliantName(explicitFieldName));
                    fieldToBeAdded =
                            new Field(field.getFieldType(),
                                    Optional.of(explicitFieldName),
                                    Optional.of(i + 1),
                                    Optional.of(fieldStorageName));
                }

                if (!(fieldNamesSeen.add(fieldToBeAdded.getFieldName()))) {
                    throw new RecordCoreException("fields contain duplicate field names");
                }
                resultFieldsBuilder.add(fieldToBeAdded);
            }

            return resultFieldsBuilder.build();
        }

        /**
         * Represents a field type in a {@link Record} type.
         */
        @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
        public static class Field implements Comparable<Field>, PlanSerializable {
            /**
             * The field {@link Type}.
             */
            @Nonnull
            private final Type fieldType;

            /**
             * The field name.
             */
            @Nonnull
            private final Optional<String> fieldNameOptional;

            /**
             * The field index.
             */
            @Nonnull
            private final Optional<Integer> fieldIndexOptional;

            @Nonnull
            private final Optional<String> fieldStorageNameOptional;

            /**
             * Memoized hash function.
             */
            @Nonnull
            private final Supplier<Integer> hashFunctionSupplier = Suppliers.memoize(this::computeHashFunction);

            private int computeHashFunction() {
                return Objects.hash(getFieldType(), getFieldNameOptional(), getFieldIndexOptional());
            }

            /**
             * Constructs a new field.
             *
             * @param fieldType The field {@link Type}.
             * @param fieldNameOptional The field name.
             * @param fieldIndexOptional The field index.
             */
            protected Field(@Nonnull final Type fieldType, @Nonnull final Optional<String> fieldNameOptional, @Nonnull Optional<Integer> fieldIndexOptional, @Nonnull Optional<String> fieldStorageNameOptional) {
                this.fieldType = fieldType;
                this.fieldNameOptional = fieldNameOptional;
                this.fieldIndexOptional = fieldIndexOptional;
                this.fieldStorageNameOptional = fieldStorageNameOptional;
            }

            /**
             * Returns the field {@link Type}.
             * @return The field {@link Type}.
             */
            @Nonnull
            public Type getFieldType() {
                return fieldType;
            }

            /**
             * Returns the field name if set. This should be the name of the field as the user would refer to it.
             * This may not be set if the user has used un-named fields, in which case names based on the field
             * index will be generated.
             *
             * @return The field name if set.
             */
            @Nonnull
            public Optional<String> getFieldNameOptional() {
                return fieldNameOptional;
            }

            /**
             * Returns the field name. If the underlying {@link Optional} is not set, this will throw an error.
             *
             * @return The field name.
             * @see #getFieldStorageNameOptional()
             * @throws RecordCoreException if the field is not set
             */
            @Nonnull
            public String getFieldName() {
                return getFieldNameOptional().orElseThrow(() -> new RecordCoreException("field name should have been set"));
            }

            /**
             * Returns the name of the underlying field in protobuf storage if set. This can differ from the user-visible
             * field name if, for example, there are characters in there are fields that need to be adjusted in order
             * to produce a valid protobuf identifier.
             *
             * @return The protobuf field name used to serialize this field if set.
             * @see ProtoUtils#toProtoBufCompliantName(String) for the escaping used
             */
            @Nonnull
            public Optional<String> getFieldStorageNameOptional() {
                return fieldStorageNameOptional;
            }

            /**
             * Returns the name of the underlying field in protobuf storage if set. If the underlying {@link Optional}
             * is not set, this will throw an error.
             *
             * @return The protobuf field name used to serialize this field.
             * @see #getFieldStorageNameOptional()
             */
            @Nonnull
            public String getFieldStorageName() {
                return getFieldStorageNameOptional().orElseThrow(() -> new RecordCoreException("field name should have been set"));
            }

            /**
             * Returns the field index.
             * @return The field index.
             */
            @Nonnull
            public Optional<Integer> getFieldIndexOptional() {
                return fieldIndexOptional;
            }

            /**
             * Returns the field index.
             * @return The field index.
             */
            public int getFieldIndex() {
                return getFieldIndexOptional().orElseThrow(() -> new RecordCoreException("field index should have been set"));
            }

            @Nonnull
            public Field withNullability(boolean newNullability) {
                if (getFieldType().isNullable() == newNullability) {
                    return this;
                }
                var newFieldType = getFieldType().withNullability(newNullability);
                return new Field(newFieldType, fieldNameOptional, fieldIndexOptional, fieldStorageNameOptional);
            }

            @Nonnull
            public Field withOverriddenTypeIfNullable(boolean shouldBeNullable) {
                return shouldBeNullable ? withNullability(true) : this;
            }

            @Override
            public boolean equals(final Object o) {
                if (o == null) {
                    return false;
                }
                if (this == o) {
                    return true;
                }
                if (!(o instanceof Field)) {
                    return false;
                }
                final var field = (Field)o;
                return getFieldType().equals(field.getFieldType()) &&
                        getFieldNameOptional().equals(field.getFieldNameOptional()) &&
                        getFieldIndexOptional().equals(field.getFieldIndexOptional());
            }

            @Override
            public int hashCode() {
                return hashFunctionSupplier.get();
            }

            @Override
            public int compareTo(final Field o) {
                Verify.verifyNotNull(o);
                return Integer.compare(getFieldIndex(), o.getFieldIndex());
            }

            @Nonnull
            @Override
            public PRecordType.PField toProto(@Nonnull final PlanSerializationContext serializationContext) {
                final PRecordType.PField.Builder fieldProtoBuilder = PRecordType.PField.newBuilder();
                fieldProtoBuilder.setFieldType(fieldType.toTypeProto(serializationContext));
                fieldNameOptional.ifPresent(fieldProtoBuilder::setFieldName);
                fieldIndexOptional.ifPresent(fieldProtoBuilder::setFieldIndex);
                fieldStorageNameOptional.ifPresent(storageFieldName -> {
                    if (!fieldProtoBuilder.getFieldName().equals(storageFieldName)) {
                        fieldProtoBuilder.setFieldStorageName(storageFieldName);
                    }
                });
                return fieldProtoBuilder.build();
            }

            @Nonnull
            private static Field fromDescriptor(@Nonnull Descriptors.FieldDescriptor fieldDescriptor, boolean preserveNames) {
                final Type fieldType = Type.fromProtoType(Type.getTypeSpecificDescriptor(fieldDescriptor),
                        fieldDescriptor.getType(),
                        fieldDescriptor.toProto().getLabel(),
                        fieldDescriptor.getOptions(),
                        !fieldDescriptor.isRequired(),
                        preserveNames);
                return new Field(fieldType,
                        Optional.of(ProtoUtils.toUserIdentifier(fieldDescriptor.getName())),
                        Optional.of(fieldDescriptor.getNumber()),
                        Optional.of(fieldDescriptor.getName()));
            }

            @Nonnull
            public static Field fromProto(@Nonnull final PlanSerializationContext serializationContext, @Nonnull final PRecordType.PField fieldProto) {
                final Type fieldType = Type.fromTypeProto(serializationContext, Objects.requireNonNull(fieldProto.getFieldType()));
                final Optional<String> fieldNameOptional = fieldProto.hasFieldName() ? Optional.of(fieldProto.getFieldName()) : Optional.empty();
                final Optional<Integer> fieldIndexOptional = fieldProto.hasFieldIndex() ? Optional.of(fieldProto.getFieldIndex()) : Optional.empty();
                final Optional<String> storageFieldNameOptional = fieldProto.hasFieldStorageName() ? Optional.of(fieldProto.getFieldStorageName()) : fieldNameOptional;
                return new Field(fieldType, fieldNameOptional, fieldIndexOptional, storageFieldNameOptional);
            }

            /**
             * Constructs a new field.
             *
             * @param fieldType The field {@link Type}.
             * @param fieldNameOptional The field name.
             * @param fieldIndexOptional The field index.
             * @return a new field
             */
            public static Field of(@Nonnull final Type fieldType, @Nonnull final Optional<String> fieldNameOptional, @Nonnull Optional<Integer> fieldIndexOptional) {
                return new Field(fieldType, fieldNameOptional, fieldIndexOptional, fieldNameOptional.map(ProtoUtils::toProtoBufCompliantName));
            }

            /**
             * Constructs a new field.
             *
             * @param fieldType The field {@link Type}.
             * @param fieldNameOptional The field name.
             * @return a new field
             */
            public static Field of(@Nonnull final Type fieldType, @Nonnull final Optional<String> fieldNameOptional) {
                return new Field(fieldType, fieldNameOptional, Optional.empty(), fieldNameOptional.map(ProtoUtils::toProtoBufCompliantName));
            }

            /**
             * Constructs a new field that has no name and no protobuf field index.
             *
             * @param fieldType The field {@link Type}.
             * @return a new field
             */
            public static Field unnamedOf(@Nonnull final Type fieldType) {
                return new Field(fieldType, Optional.empty(), Optional.empty(), Optional.empty());
            }

            public static boolean isAutoGenerated(@Nonnull final String fieldName) {
                return fieldName.startsWith("_") && StringUtils.isNumeric(fieldName, 1);
            }
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PRecordType, Record> {
            @Nonnull
            @Override
            public Class<PRecordType> getProtoMessageClass() {
                return PRecordType.class;
            }

            @Nonnull
            @Override
            public Record fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                    @Nonnull final PRecordType recordTypeProto) {
                return Record.fromProto(serializationContext, recordTypeProto);
            }
        }
    }

    /**
     * Represents a relational type.
     */
    class Relation implements Type, Erasable {
        /**
         * The type of the stream values.
         */
        @Nullable
        private final Type innerType;

        /**
         * Memoized hash function.
         */
        @Nonnull
        private final Supplier<Integer> hashFunctionSupplier = Suppliers.memoize(this::computeHashFunction);

        private int computeHashFunction() {
            return Objects.hash(getTypeCode().name().hashCode(), isNullable(), innerType);
        }

        /**
         * Constructs a new {@link Relation} object without a value type.
         */
        public Relation() {
            this(null);
        }

        /**
         * Constructs a new {@link Relation} object.
         *
         * @param innerType The {@code Type} of the stream values.
         */
        public Relation(@Nullable final Type innerType) {
            this.innerType = innerType;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public TypeCode getTypeCode() {
            return TypeCode.RELATION;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Class<?> getJavaClass() {
            throw new UnsupportedOperationException("should not have been asked");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isNullable() {
            return false;
        }

        @Nonnull
        @Override
        public Relation withNullability(final boolean newIsNullable) {
            Verify.verify(!newIsNullable);
            return this;
        }

        /**
         * Returns the values {@link Type}.
         * @return The values {@link Type}.
         */
        @Nullable
        public Type getInnerType() {
            return innerType;
        }

        /**
         * Checks whether the stream type is erased or not.
         *
         * @return <code>true</code> if the stream type is erased, otherwise <code>false</code>.
         */
        @Override
        public boolean isErased() {
            return getInnerType() == null;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void addProtoField(@Nonnull final TypeRepository.Builder typeRepositoryBuilder,
                                  @Nonnull final DescriptorProto.Builder descriptorBuilder,
                                  final int fieldNumber,
                                  @Nonnull final String fieldName,
                                  @Nonnull final Optional<String> typeNameOptional,
                                  @Nonnull final FieldDescriptorProto.Label label) {
            throw new IllegalStateException("this should not have been called");
        }

        @Override
        public int hashCode() {
            return hashFunctionSupplier.get();
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == null) {
                return false;
            }

            if (obj == this) {
                return true;
            }

            if (getClass() != obj.getClass()) {
                return false;
            }

            final var otherType = (Relation)obj;
            return getTypeCode() == otherType.getTypeCode() && isNullable() == otherType.isNullable() &&
                   ((isErased() && otherType.isErased()) || Objects.requireNonNull(innerType).equals(otherType.innerType));
        }

        @Nonnull
        @Override
        public String toString() {
            return describe().render(DefaultExplainFormatter.forDebugging()).toString();
        }

        @Nonnull
        @Override
        public ExplainTokens describe() {
            final var resultExplainTokens = new ExplainTokens();
            resultExplainTokens.addKeyword(getTypeCode().toString());
            if (isErased()) {
                return resultExplainTokens;
            }
            return resultExplainTokens.addOptionalWhitespace().addOpeningParen().addOptionalWhitespace()
                    .addNested(Objects.requireNonNull(getInnerType()).describe()).addOptionalWhitespace()
                    .addClosingParen();
        }

        @Nonnull
        @Override
        public PRelationType toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PRelationType.newBuilder().setInnerType(Objects.requireNonNull(innerType).toTypeProto(serializationContext)).build();
        }

        @Nonnull
        @Override
        public PType toTypeProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PType.newBuilder().setRelationType(toProto(serializationContext)).build();
        }

        @Nonnull
        public static Relation fromProto(@Nonnull final PlanSerializationContext serializationContext, @Nonnull final PRelationType relationTypeProto) {
            return new Relation(Objects.requireNonNull(Type.fromTypeProto(serializationContext, relationTypeProto.getInnerType())));
        }

        public static Type scalarOf(@Nonnull final Type relationType) {
            Verify.verify(relationType.getTypeCode() == TypeCode.RELATION && relationType instanceof Relation);
            return ((Relation)relationType).getInnerType();
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PRelationType, Relation> {
            @Nonnull
            @Override
            public Class<PRelationType> getProtoMessageClass() {
                return PRelationType.class;
            }

            @Nonnull
            @Override
            public Relation fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                      @Nonnull final PRelationType relationTypeProto) {
                return Relation.fromProto(serializationContext, relationTypeProto);
            }
        }
    }

    /**
     * A type representing an array of elements sharing the same type.
     */
    class Array implements Type, Erasable {
        /**
         * Whether the array is nullable or not.
         */
        private final boolean isNullable;

        /**
         * The type of the array values.
         */
        @Nullable
        private final Type elementType;

        /**
         * Memoized hash function.
         */
        @Nonnull
        private final Supplier<Integer> hashFunctionSupplier = Suppliers.memoize(this::computeHashFunction);

        private int computeHashFunction() {
            return Objects.hash(getTypeCode().name().hashCode(), isNullable(), elementType);
        }

        /**
         * Constructs a new <i>nullable</i> array type instance without a value {@link Type}.
         */
        public Array() {
            this(null);
        }

        /**
         * Constructs a new <i>nullable</i> array type instance.
         *
         * @param elementType the {@link Type} of the array type elements.
         */
        public Array(@Nullable final Type elementType) {
            this(false, elementType);
        }

        public Array(final boolean isNullable, @Nullable final Type elementType) {
            this.isNullable = isNullable;
            this.elementType = elementType;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public TypeCode getTypeCode() {
            return TypeCode.ARRAY;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Class<?> getJavaClass() {
            return java.util.Collection.class;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean isNullable() {
            return isNullable;
        }

        @Nonnull
        @Override
        public Array withNullability(final boolean newIsNullable) {
            if (newIsNullable == isNullable) {
                return this;
            }
            return new Array(newIsNullable, elementType);
        }

        /**
         * Returns the array element {@link Type}.
         * @return The array element {@link Type}.
         */
        @Nullable
        public Type getElementType() {
            return elementType;
        }

        /**
         * Return the array with a given element {@link Type} and the same nullability semantics.
         * @param elementType The new element type, can be {@code null}.
         * @return the array with a given element {@link Type} and the same nullability semantics, if the element type
         * matches the current element type, the same instance is returned.
         */
        @Nonnull
        @SuppressWarnings("PMD.BrokenNullCheck") // I think PMD got confused or the null check conjunctions below.
        public Type.Array withElementType(@Nullable final Type elementType) {
            if (elementType == null && this.elementType == null) {
                return this;
            }
            if (elementType != null && elementType.equals(this.elementType)) {
                return this;
            }
            return new Array(isNullable(), elementType);
        }

        /**
         * Checks whether the array type is erased or not.
         *
         * @return <code>true</code> if the array type is erased, otherwise <code>false</code>.
         */
        @Override
        public boolean isErased() {
            return getElementType() == null;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void defineProtoType(final TypeRepository.Builder typeRepositoryBuilder) {
            Objects.requireNonNull(elementType);
            final var typeName = ProtoUtils.uniqueTypeName();
            typeRepositoryBuilder.registerTypeToTypeNameMapping(this, typeName);
            if (isNullable && elementType.getTypeCode() != TypeCode.UNKNOWN) {
                Type wrapperType = Record.fromFields(List.of(Record.Field.of(new Array(elementType), Optional.of(NullableArrayTypeUtils.getRepeatedFieldName()))));
                typeRepositoryBuilder.defineAndResolveType(wrapperType);
            } else {
                typeRepositoryBuilder.defineAndResolveType(elementType);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void addProtoField(@Nonnull final TypeRepository.Builder typeRepositoryBuilder,
                                  @Nonnull final DescriptorProto.Builder descriptorBuilder,
                                  final int fieldNumber,
                                  @Nonnull final String fieldName,
                                  @Nonnull final Optional<String> typeNameOptional,
                                  @Nonnull final FieldDescriptorProto.Label label) {
            Objects.requireNonNull(elementType);
            if (isNullable && elementType.getTypeCode() != TypeCode.UNKNOWN) {
                Type wrapperType = Record.fromFields(List.of(Record.Field.of(new Array(elementType), Optional.of(NullableArrayTypeUtils.getRepeatedFieldName()))));
                wrapperType.addProtoField(typeRepositoryBuilder,
                        descriptorBuilder,
                        fieldNumber,
                        fieldName,
                        typeRepositoryBuilder.defineAndResolveType(wrapperType),
                        FieldDescriptorProto.Label.LABEL_OPTIONAL);
            } else {
                // put repeated field straight into its parent
                elementType.addProtoField(typeRepositoryBuilder,
                        descriptorBuilder,
                        fieldNumber,
                        fieldName,
                        typeRepositoryBuilder.defineAndResolveType(elementType),
                        FieldDescriptorProto.Label.LABEL_REPEATED);
            }
        }

        @Override
        public int hashCode() {
            return hashFunctionSupplier.get();
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == null) {
                return false;
            }

            if (obj == this) {
                return true;
            }

            if (getClass() != obj.getClass()) {
                return false;
            }

            final var otherType = (Array)obj;
            return getTypeCode() == otherType.getTypeCode() && isNullable() == otherType.isNullable() &&
                   ((isErased() && otherType.isErased()) || Objects.requireNonNull(elementType).equals(otherType.elementType));
        }

        @Nonnull
        @Override
        public String toString() {
            return describe().render(DefaultExplainFormatter.forDebugging()).toString();
        }

        @Nonnull
        @Override
        public ExplainTokens describe() {
            final var resultExplainTokens = new ExplainTokens();
            resultExplainTokens.addKeyword(getTypeCode().toString());
            if (isErased()) {
                return resultExplainTokens;
            }
            return resultExplainTokens.addOptionalWhitespace().addOpeningParen().addOptionalWhitespace()
                    .addNested(Objects.requireNonNull(getElementType()).describe()).addOptionalWhitespace()
                    .addClosingParen();
        }

        @Nonnull
        @Override
        public PArrayType toProto(@Nonnull final PlanSerializationContext serializationContext) {
            final PArrayType.Builder arrayTypeProtoBuilder = PArrayType.newBuilder();
            arrayTypeProtoBuilder.setIsNullable(isNullable);
            arrayTypeProtoBuilder.setElementType(Objects.requireNonNull(elementType).toTypeProto(serializationContext));
            return arrayTypeProtoBuilder.build();
        }

        @Nonnull
        @Override
        public PType toTypeProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PType.newBuilder().setArrayType(toProto(serializationContext)).build();
        }

        @Nonnull
        public static Array fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                      @Nonnull final PArrayType arrayTypeProto) {
            Verify.verify(arrayTypeProto.hasIsNullable());
            return new Array(arrayTypeProto.getIsNullable(), Type.fromTypeProto(serializationContext, arrayTypeProto.getElementType()));
        }

        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PArrayType, Array> {
            @Nonnull
            @Override
            public Class<PArrayType> getProtoMessageClass() {
                return PArrayType.class;
            }

            @Nonnull
            @Override
            public Array fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                   @Nonnull final PArrayType arrayTypeProto) {
                return Array.fromProto(serializationContext, arrayTypeProto);
            }
        }
    }

    class Uuid implements Type {

        public static final String MESSAGE_NAME = TupleFieldsProto.UUID.getDescriptor().getName();

        private final boolean isNullable;

        private Uuid(boolean isNullable) {
            this.isNullable = isNullable;
        }

        @Override
        public TypeCode getTypeCode() {
            return TypeCode.UUID;
        }

        @Override
        public boolean isNullable() {
            return isNullable;
        }

        @Nonnull
        @Override
        public Type withNullability(final boolean newIsNullable) {
            if (newIsNullable) {
                return UUID_NULL_INSTANCE;
            } else {
                return UUID_NON_NULL_INSTANCE;
            }
        }

        @Nonnull
        @Override
        public ExplainTokens describe() {
            return new ExplainTokens().addKeyword(getTypeCode().toString());
        }

        @Override
        public void addProtoField(@Nonnull final TypeRepository.Builder typeRepositoryBuilder, @Nonnull final DescriptorProto.Builder descriptorBuilder, final int fieldNumber, @Nonnull final String fieldName, @Nonnull final Optional<String> typeNameOptional, @Nonnull final FieldDescriptorProto.Label label) {
            FieldDescriptorProto.Builder builder = FieldDescriptorProto.newBuilder()
                    .setNumber(fieldNumber)
                    .setName(fieldName)
                    .setLabel(label)
                    .setTypeName(TupleFieldsProto.UUID.getDescriptor().getFullName());
            typeNameOptional.ifPresent(builder::setTypeName);
            descriptorBuilder.addField(builder);
        }

        @Nonnull
        @Override
        public PType toTypeProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PType.newBuilder().setUuidType(toProto(serializationContext)).build();
        }

        @Nonnull
        @Override
        public PUuidType toProto(@Nonnull final PlanSerializationContext serializationContext) {
            return PUuidType.newBuilder()
                    .setIsNullable(isNullable)
                    .build();
        }

        @Override
        public boolean isUuid() {
            return true;
        }

        @Nonnull
        public static Uuid fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                          @Nonnull final PUuidType uuidTypeProto) {
            Verify.verify(uuidTypeProto.hasIsNullable());
            return Type.uuidType(uuidTypeProto.getIsNullable());
        }


        /**
         * Deserializer.
         */
        @AutoService(PlanDeserializer.class)
        public static class Deserializer implements PlanDeserializer<PUuidType, Uuid> {
            @Nonnull
            @Override
            public Class<PUuidType> getProtoMessageClass() {
                return PUuidType.class;
            }

            @Nonnull
            @Override
            public Uuid fromProto(@Nonnull final PlanSerializationContext serializationContext,
                                   @Nonnull final PUuidType uuidTypeProto) {
                return Uuid.fromProto(serializationContext, uuidTypeProto);
            }
        }
    }
}
