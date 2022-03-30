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

package com.apple.foundationdb.record.query.plan.temp;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.query.plan.temp.dynamic.TypeRepository;
import com.apple.foundationdb.record.query.predicates.Value;
import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Provides type information about the output of an expression such as {@link Value} in a QGM.
 *
 * Types bear a resemblance to protobuf types; they are either primitive such as <code>boolean</code>, <code>int</code>,
 * and <code>string</code> or structured such as {@link Record} and {@link Array}. Moreover, it is possible to switch
 * between a {@link Type} instance and an equivalent protobuf {@link Descriptors} in a lossless manner.
 *
 * Finally, {@link Type}s are non-referential, so two structural types are considered equal iff their structures
 * are equal.
 */
public interface Type extends Narrowable<Type> {
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
     * Checks whether a {@link Type} is nullable.
     *
     * @return <code>true</code> if the {@link Type} is nullable, otherwise <code>false</code>.
     */
    boolean isNullable();

    /**
     * Checks whether a {@link Type} is numeric.
     * @return <code>true</code> if the {@link Type} is numeric, otherwise <code>false</code>.
     */
    default boolean isNumeric() {
        return getTypeCode().isNumeric();
    }

    @Nonnull
    default String describe(@Nonnull final Formatter formatter) {
        // TODO make better
        return toString();
    }

    /**
     * Creates a synthetic protobuf descriptor that is equivalent to <code>this</code> {@link Type}.
     *
     * @param typeRepositoryBuilder The type repository builder.
     * @param typeName The name of the descriptor.
     * @return a syncthetic protobuf descriptor that is equivalent to <code>this</code> {@link Type}.
     */
    @Nullable
    DescriptorProto buildDescriptor(final TypeRepository.Builder typeRepositoryBuilder, @Nonnull final String typeName);

    /**
     * Creates a synthetic protobuf descriptor that is equivalent to the <code>this</code> {@link Type} within a given
     * protobuf descriptor.
     * @param typeRepositoryBuilder The type repository.
     * @param descriptorBuilder The parent descriptor into which the newly created descriptor will be created.
     * @param fieldIndex The field index of the descriptor.
     * @param fieldName The field name of the descriptor.
     * @param typeName The type name of the descriptor.
     * @param label The label of the descriptor.
     */
    void addProtoField(@Nonnull final TypeRepository.Builder typeRepositoryBuilder,
                       @Nonnull final DescriptorProto.Builder descriptorBuilder,
                       final int fieldIndex,
                       @Nonnull final String fieldName,
                       @Nonnull final String typeName,
                       @Nonnull final FieldDescriptorProto.Label label);


    /**
     * Returns a map from Java {@link Class} to corresponding {@link TypeCode}.
     *
     * @return A map from Java {@link Class} to corresponding {@link TypeCode}.
     */
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

    /**
     * For a given {@link TypeCode}, it returns a corresponding <i>nullable</i> {@link Type}.
     *
     * pre-condition: The {@link TypeCode} is primitive.
     *
     * @param typeCode The primitive type code.
     * @return the corresponding {@link Type}.
     */
    @Nonnull
    static Type primitiveType(@Nonnull final TypeCode typeCode) {
        return primitiveType(typeCode, true);
    }

    /**
     * For a given {@link TypeCode}, it returns a corresponding {@link Type}.
     *
     * pre-condition: The {@link TypeCode} is primitive.
     *
     * @param typeCode The primitive type code.
     * @param isNullable True, if the {@link Type} is supposed to be nullable, otherwise, false.
     * @return the corresponding {@link Type}.
     */
    @Nonnull
    static Type primitiveType(@Nonnull final TypeCode typeCode, final boolean isNullable) {
        Verify.verify(typeCode.isPrimitive());
        return new Type() {

            private final int memoizedHashCode = Objects.hash(getTypeCode().hashCode(), isNullable());

            @Override
            public TypeCode getTypeCode() {
                return typeCode;
            }

            @Override
            public boolean isNullable() {
                return isNullable;
            }

            @Nullable
            @Override
            public DescriptorProto buildDescriptor(final TypeRepository.Builder typeRepositoryBuilder, @Nonnull final String typeName) {
                return null;
            }

            @Override
            public void addProtoField(@Nonnull final TypeRepository.Builder typeRepositoryBuilder,
                                      @Nonnull final DescriptorProto.Builder descriptorBuilder,
                                      final int fieldIndex,
                                      @Nonnull final String fieldName,
                                      @Nonnull final String ignored,
                                      @Nonnull final FieldDescriptorProto.Label label) {
                final FieldDescriptorProto.Type protoType = Objects.requireNonNull(getTypeCode().getProtoType());
                descriptorBuilder.addField(FieldDescriptorProto.newBuilder()
                        .setNumber(fieldIndex)
                        .setName(fieldName)
                        .setType(protoType)
                        .setLabel(label)
                        .build());
            }

            @Override
            public int hashCode() {
                return memoizedHashCode;
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

                final Type otherType = (Type)obj;

                return getTypeCode() == otherType.getTypeCode() && isNullable() == otherType.isNullable();
            }

            @Override
            public String toString() {
                return getTypeCode().toString();
            }
        };
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
     * Generates a JVM-wide unique type name.
     * @return a unique type name.
     */
    static String uniqueCompliantTypeName() {
        final String safeUuid = UUID.randomUUID().toString().replace('-', '_');
        return "__type__" + safeUuid;
    }

    /**
     * All supported {@link Type}s.
     */
    enum TypeCode {
        UNKNOWN(null, null, true, false),
        ANY(Object.class, null, false, false),
        BOOLEAN(Boolean.class, FieldDescriptorProto.Type.TYPE_BOOL, true, false),
        BYTES(ByteString.class, FieldDescriptorProto.Type.TYPE_BYTES, true, false),
        DOUBLE(Double.class, FieldDescriptorProto.Type.TYPE_DOUBLE, true, true),
        FLOAT(Float.class, FieldDescriptorProto.Type.TYPE_FLOAT, true, true),
        INT(Integer.class, FieldDescriptorProto.Type.TYPE_INT32, true, true),
        LONG(Long.class, FieldDescriptorProto.Type.TYPE_INT64, true, true),
        STRING(String.class, FieldDescriptorProto.Type.TYPE_STRING, true, false),
        RECORD(Message.class, null, false, false),
        ARRAY(Array.class, null, false, false),
        RELATION(null, null, false, false);

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
            ImmutableBiMap.Builder<Class<?>, TypeCode> builder = ImmutableBiMap.builder();
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
         * @param protobufType The protobuf type.
         * @return A corresponding {@link TypeCode} instance.
         */
        @Nonnull
        public static TypeCode fromProtobufType(@Nonnull final Descriptors.FieldDescriptor.Type protobufType) {
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
                    throw new IllegalArgumentException("protobuf type " + protobufType + " is not supported");
                case MESSAGE:
                    return TypeCode.RECORD;
                case BYTES:
                    return TypeCode.BYTES;
                default:
                    throw new IllegalArgumentException("unknown protobuf type " + protobufType);
            }
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

        private int computeHashCode() {
            return Objects.hash(getTypeCode().hashCode(), isNullable());
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

        /**
         * {@inheritDoc}
         */
        @Nullable
        @Override
        public DescriptorProto buildDescriptor(final TypeRepository.Builder typeRepositoryBuilder, @Nonnull final String typeName) {
            throw new UnsupportedOperationException("type any cannot be represented in protobuf");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void addProtoField(@Nonnull final TypeRepository.Builder typeRepositoryBuilder,
                                  final DescriptorProto.Builder descriptorBuilder, final int fieldIndex,
                                  @Nonnull final String fieldName,
                                  @Nonnull final String typeName,
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

            final Type otherType = (Type)obj;

            return getTypeCode() == otherType.getTypeCode() && isNullable() == otherType.isNullable();
        }

        @Override
        public String toString() {
            return getTypeCode().toString();
        }
    }

    /**
     * A structured {@link Type} that contains a list of {@link Field} types.
     */
    class Record implements Type {
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
         * function that returns a mapping betweeen field names and their {@link Type}s.
         */
        @Nonnull
        private final Supplier<Map<String, Type>> fieldTypeMapSupplier;

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
            return Objects.hash(getTypeCode().hashCode(), isNullable(), fields);
        }

        /**
         * Constructs a new {@link Record} using a list of {@link Field}s.
         * @param isNullable True if the record type is nullable, otherwise false.
         * @param fields The list of {@link Record} {@link Field}s.
         */
        private Record(final boolean isNullable,
                       @Nullable final List<Field> fields) {
            this.isNullable = isNullable;
            this.fields = fields == null ? null : normalizeFields(fields);
            this.fieldTypeMapSupplier = Suppliers.memoize(this::computeFieldTypeMap);
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

        /**
         * Returns the list of {@link Record} {@link Field}s.
         * @return the list of {@link Record} {@link Field}s.
         */
        @Nullable
        public List<Field> getFields() {
            return fields;
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
        @Nullable
        public Map<String, Type> getFieldTypeMap() {
            return fieldTypeMapSupplier.get();
        }

        /**
         * Computes a mapping from {@link Field} names to their {@link Type}s.
         * @return a mapping from {@link Field} names to their {@link Type}s.
         */
        @SuppressWarnings("OptionalGetWithoutIsPresent")
        private Map<String, Type> computeFieldTypeMap() {
            return Objects.requireNonNull(fields)
                    .stream()
                    .collect(ImmutableMap.toImmutableMap(field -> field.getFieldNameOptional().get(), Field::getFieldType));
        }

        /**
         * Checks whether the {@link Record} type instance is erased or not.
         * @return <code>true</code> if the {@link Record} type is erased, other <code>false</code>.
         */
        boolean isErased() {
            return fields == null;
        }

        /**
         * {@inheritDoc}
         */
        @Nullable
        @Override
        public DescriptorProto buildDescriptor(final TypeRepository.Builder typeRepositoryBuilder, @Nonnull final String typeName) {
            Objects.requireNonNull(fields);
            final DescriptorProto.Builder recordMsgBuilder = DescriptorProto.newBuilder();
            recordMsgBuilder.setName(typeName);

            for (final Field field : fields) {
                final Type fieldType = field.getFieldType();
                final String fieldName = field.getFieldName();
                if (!fieldType.isPrimitive()) {
                    fieldType.addProtoField(typeRepositoryBuilder, recordMsgBuilder,
                            field.getFieldIndex(),
                            fieldName,
                            typeRepositoryBuilder.addTypeAndGetName(fieldType),
                            FieldDescriptorProto.Label.LABEL_OPTIONAL);
                } else {
                    fieldType.addProtoField(typeRepositoryBuilder, recordMsgBuilder,
                            field.getFieldIndex(),
                            fieldName,
                            "ignored", // gets the built-in PB primitive type name.
                            FieldDescriptorProto.Label.LABEL_OPTIONAL);
                }
            }
            return recordMsgBuilder.build();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void addProtoField(@Nonnull final TypeRepository.Builder typeRepositoryBuilder,
                                  final DescriptorProto.Builder descriptorBuilder,
                                  final int fieldIndex,
                                  @Nonnull final String fieldName,
                                  @Nonnull final String typeName,
                                  @Nonnull final FieldDescriptorProto.Label label) {
            descriptorBuilder.addField(FieldDescriptorProto.newBuilder()
                    .setName(fieldName)
                    .setNumber(fieldIndex)
                    .setTypeName(typeName)
                    .setLabel(label)
                    .build());
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

            final Record otherType = (Record)obj;

            return getTypeCode() == otherType.getTypeCode() && isNullable() == otherType.isNullable() &&
                   ((isErased() && otherType.isErased()) ||
                    (Objects.requireNonNull(fields).equals(otherType.fields)));
        }

        @Override
        public String toString() {
            return isErased()
                   ? getTypeCode().toString()
                   : getTypeCode() + "(" +
                     Objects.requireNonNull(getFields()).stream().map(field -> {
                         final Optional<String> fieldNameOptional = field.getFieldNameOptional();
                         return fieldNameOptional.map(s -> field.getFieldType() + " as " + s)
                                 .orElseGet(() -> field.getFieldType().toString());
                     }).collect(Collectors.joining(", ")) + ")";
        }

        /**
         * Creates a new erased {@link Record} type instance and returns it.
         *
         * @return a new erased {@link Record} type instance.
         */
        @Nonnull
        public static Record erased() {
            return new Record(true,  null);
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
            return new Record(isNullable, fields);
        }

        /**
         * Creates a new <i>nullable</i> {@link Record} type instance using the given map of field names to their protobuf
         * {@link com.google.protobuf.Descriptors.FieldDescriptor}s.
         *
         * @param fieldDescriptorMap A map of field names to their protobuf {@link com.google.protobuf.Descriptors.FieldDescriptor}s.
         * @return a new <i>nullable</i> {@link Record} type instance using the given map of field names to their protobuf
         * {@link com.google.protobuf.Descriptors.FieldDescriptor}s.
         */
        @Nonnull
        public static Record fromFieldDescriptorsMap(@Nonnull final Map<String, Descriptors.FieldDescriptor> fieldDescriptorMap) {
            return fromFieldDescriptorsMap(true, fieldDescriptorMap);
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
            final ImmutableList.Builder<Field> fieldsBuilder = ImmutableList.builder();
            for (final Map.Entry<String, Descriptors.FieldDescriptor> entry : Objects.requireNonNull(fieldDescriptorMap).entrySet()) {
                final Descriptors.FieldDescriptor fieldDescriptor = entry.getValue();
                fieldsBuilder.add(
                        new Field(fromProtoType(getMessageTypeIfMessage(fieldDescriptor),
                                fieldDescriptor.getType(),
                                fieldDescriptor.toProto().getLabel(),
                                true),
                                Optional.of(entry.getKey()),
                                Optional.of(fieldDescriptor.getNumber())));
            }

            return fromFields(isNullable, fieldsBuilder.build());
        }

        /**
         * For a given {@link com.google.protobuf.Descriptors.FieldDescriptor} descriptor, returns the message type descriptor
         * if the field is a message, otherwise <code>null</code>.
         *
         * @param fieldDescriptor The descriptor.
         * @return the message type descriptor if the field is a message, otherwise <code>null</code>.
         */
        @Nullable
        private static Descriptors.Descriptor getMessageTypeIfMessage(@Nonnull final Descriptors.FieldDescriptor fieldDescriptor) {
            return fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE ? fieldDescriptor.getMessageType() : null;
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
        private static Type fromProtoType(@Nullable Descriptors.Descriptor descriptor,
                                          @Nonnull Descriptors.FieldDescriptor.Type protoType,
                                          @Nonnull FieldDescriptorProto.Label protoLabel,
                                          boolean isNullable) {
            final TypeCode typeCode = TypeCode.fromProtobufType(protoType);
            if (protoLabel == FieldDescriptorProto.Label.LABEL_REPEATED) {
                // collection type
                // case 1: primitive types -- assumed to be non-nullable array of that type
                if (typeCode.isPrimitive()) {
                    final Type primitiveType = primitiveType(typeCode, false);
                    return new Array(primitiveType);
                } else {
                    Objects.requireNonNull(descriptor);
                    // case 2: helper type to model null-ed out array elements
                    final Optional<Descriptors.FieldDescriptor> elementFieldDescriptorMaybe =
                            arrayElementFieldDescriptorMaybe(descriptor);

                    if (elementFieldDescriptorMaybe.isPresent()) {
                        final Descriptors.FieldDescriptor elementFieldDescriptor = elementFieldDescriptorMaybe.get();
                        return new Array(fromProtoType(getMessageTypeIfMessage(elementFieldDescriptor), elementFieldDescriptor.getType(), FieldDescriptorProto.Label.LABEL_OPTIONAL, true));
                    } else {
                        // case 3: any arbitrary sub message we don't understand
                        return new Array(fromProtoType(descriptor, protoType, FieldDescriptorProto.Label.LABEL_OPTIONAL, false));
                    }
                }
            } else {
                if (typeCode.isPrimitive()) {
                    return primitiveType(typeCode, isNullable);
                } else if (typeCode == TypeCode.RECORD) {
                    Objects.requireNonNull(descriptor);
                    return fromFieldDescriptorsMap(isNullable, toFieldDescriptorMap(descriptor.getFields()));
                }
            }

            throw new IllegalStateException("unable to translate protobuf descriptor to type");
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

        /**
         * Examines the given {@link com.google.protobuf.Descriptors.Descriptor}'s {@link Field}s and returns an {@link Optional}
         * containing a field that can be used as an array element type, if possible, otherwise, an empty {@link Optional}.
         *
         * @param descriptor The {@link com.google.protobuf.Descriptors.Descriptor} to examine.
         * @return Optionally, a field that can be used as an array element type.
         */
        @Nonnull
        private static Optional<Descriptors.FieldDescriptor> arrayElementFieldDescriptorMaybe(@Nonnull final Descriptors.Descriptor descriptor) {
            final List<Descriptors.FieldDescriptor> fields = descriptor.getFields();
            if (fields.size() == 1) {
                final Descriptors.FieldDescriptor field0 = fields.get(0);
                if (field0.isOptional() && !field0.isRepeated() && field0.getNumber() == 1 && "values".equals(field0.getName())) {
                    return Optional.of(field0);
                }
            }
            return Optional.empty();
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
        @Nonnull
        private static List<Field> normalizeFields(@Nonnull final List<Field> fields) {
            Objects.requireNonNull(fields);
            final ImmutableList.Builder<Field> resultFieldsBuilder = ImmutableList.builder();

            int i = 0;
            for (final Field field : fields) {
                resultFieldsBuilder.add(
                        new Field(field.getFieldType(),
                                Optional.of(field.getFieldNameOptional().orElse(fieldName(i + 1))),
                                Optional.of(field.getFieldIndexOptional().orElse(i + 1))));
                i++;
            }

            return resultFieldsBuilder.build();
        }

        /**
         * Represents a field type in a {@link Record} type.
         */
        @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
        public static class Field {

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
            private Field(@Nonnull final Type fieldType, @Nonnull final Optional<String> fieldNameOptional, @Nonnull Optional<Integer> fieldIndexOptional) {
                this.fieldType = fieldType;
                this.fieldNameOptional = fieldNameOptional;
                this.fieldIndexOptional = fieldIndexOptional;
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
             * Returns the field name.
             * @return The field name.
             */
            @Nonnull
            public Optional<String> getFieldNameOptional() {
                return fieldNameOptional;
            }

            /**
             * Returns the field name.
             * @return The field name.
             */
            @Nonnull
            public String getFieldName() {
                return getFieldNameOptional().orElseThrow(() -> new RecordCoreException("field name should have been set"));
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
            @Nonnull
            public int getFieldIndex() {
                return getFieldIndexOptional().orElseThrow(() -> new RecordCoreException("field index should have been set"));
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
                final Field field = (Field)o;
                return getFieldType().equals(field.getFieldType()) &&
                       getFieldNameOptional().equals(field.getFieldNameOptional()) &&
                       getFieldIndexOptional().equals(field.getFieldIndexOptional());
            }

            @Override
            public int hashCode() {
                return hashFunctionSupplier.get();
            }

            /**
             * Constructs a new field.
             *
             * @param fieldType The field {@link Type}.
             * @param fieldNameOptional The field name.
             * @param fieldIndexOptional The field index.
             */
            public static Field of(@Nonnull final Type fieldType, @Nonnull final Optional<String> fieldNameOptional, @Nonnull Optional<Integer> fieldIndexOptional) {
                return new Field(fieldType, fieldNameOptional, fieldIndexOptional);
            }

            /**
             * Constructs a new field.
             *
             * @param fieldType The field {@link Type}.
             * @param fieldNameOptional The field name.
             */
            public static Field of(@Nonnull final Type fieldType, @Nonnull final Optional<String> fieldNameOptional) {
                return new Field(fieldType, fieldNameOptional, Optional.empty());
            }

            /**
             * Constructs a new field that has no name and no protobuf field index.
             *
             * @param fieldType The field {@link Type}.
             */
            public static Field unnamedOf(@Nonnull final Type fieldType) {
                return new Field(fieldType, Optional.empty(), Optional.empty());
            }
        }
    }

    /**
     * Represents a relational type.
     */
    class Relation implements Type {
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
            return Objects.hash(getTypeCode().hashCode(), isNullable(), innerType);
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
        boolean isErased() {
            return getInnerType() == null;
        }

        /**
         * {@inheritDoc}
         */
        @Nullable
        @Override
        public DescriptorProto buildDescriptor(final TypeRepository.Builder typeRepositoryBuilder, @Nonnull final String typeName) {
            throw new IllegalStateException("this should not have been called");
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void addProtoField(@Nonnull final TypeRepository.Builder typeRepositoryBuilder, final DescriptorProto.Builder descriptorBuilder, final int fieldIndex, @Nonnull final String fieldName, @Nonnull final String typeName, @Nonnull final FieldDescriptorProto.Label label) {
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

            final Relation otherType = (Relation)obj;

            return getTypeCode() == otherType.getTypeCode() && isNullable() == otherType.isNullable() &&
                   ((isErased() && otherType.isErased()) || Objects.requireNonNull(innerType).equals(otherType.innerType));
        }

        @Override
        public String toString() {
            return isErased()
                   ? getTypeCode().toString()
                   : getTypeCode() + "(" + Objects.requireNonNull(getInnerType()) + ")";
        }

        public static Type scalarOf(@Nonnull final Type relationType) {
            Verify.verify(relationType.getTypeCode() == TypeCode.RELATION && relationType instanceof Relation);
            return ((Relation)relationType).getInnerType();
        }
    }

    /**
     * A type representing an array of elements sharing the same type.
     */
    class Array implements Type {
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
            return Objects.hash(getTypeCode().hashCode(), isNullable(), elementType);
        }

        /**
         * Constructs a new <i>nullable</i> array type instance without a value {@link Type}.
         */
        public Array() {
            this( null);
        }

        /**
         * Constructs a new <i>nullable</i> array type instance.
         *
         * @param elementType the {@link Type} of the array type elements.
         */
        public Array(@Nullable final Type elementType) {
            this(true, elementType);
        }

        /**
         * Constructs a new array type instance.
         *
         * @param isNullable <code>true</code> if the array type is nullable, otherwise <code>false</code>.
         * @param elementType the {@link Type} of the array type elements.
         */
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

        /**
         * Returns <code>true</code> if a nested protobuf message is required for the array element type, otherwise <code>false</code>.
         * @return <code>true</code> if a nested protobuf message is required for the array element type, otherwise <code>false</code>.
         */
        public boolean definesNestedProto() {
            return needsNestedProto(elementType);
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
         * Checks whether the array type is erased or not.
         *
         * @return <code>true</code> if the array type is erased, otherwise <code>false</code>.
         */
        boolean isErased() {
            return getElementType() == null;
        }

        /**
         * {@inheritDoc}
         */
        @Nullable
        @Override
        public DescriptorProto buildDescriptor(final TypeRepository.Builder typeRepositoryBuilder, @Nonnull final String typeName) {
            Objects.requireNonNull(elementType);
            final DescriptorProto.Builder helperDescriptorBuilder = DescriptorProto.newBuilder();
            helperDescriptorBuilder.setName(typeName);
            elementType.addProtoField(typeRepositoryBuilder, helperDescriptorBuilder, 1, "value", typeName, FieldDescriptorProto.Label.LABEL_OPTIONAL);

            return helperDescriptorBuilder.build();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void addProtoField(@Nonnull final TypeRepository.Builder typeRepositoryBuilder, @Nonnull final DescriptorProto.Builder descriptorBuilder, final int fieldIndex, @Nonnull final String fieldName, @Nonnull final String typeName, @Nonnull final FieldDescriptorProto.Label label) {
            Objects.requireNonNull(elementType);
            //
            // If the inner type is nullable, we need to create a nested helper message to keep track of
            // nulls.
            //
            if (definesNestedProto()) {
                buildDescriptor(typeRepositoryBuilder, typeName);
                descriptorBuilder.addField(FieldDescriptorProto.newBuilder()
                        .setName(fieldName)
                        .setNumber(fieldIndex)
                        .setTypeName(typeName)
                        .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)
                        .build());
            } else {
                // if inner type is not nullable we can just put is straight into its parent
                elementType.addProtoField(typeRepositoryBuilder, descriptorBuilder, fieldIndex, fieldName, typeName, FieldDescriptorProto.Label.LABEL_REPEATED);
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

            final Array otherType = (Array)obj;

            return getTypeCode() == otherType.getTypeCode() && isNullable() == otherType.isNullable() &&
                   ((isErased() && otherType.isErased()) || Objects.requireNonNull(elementType).equals(otherType.elementType));
        }

        @Override
        public String toString() {
            return isErased()
                   ? getTypeCode().toString()
                   : getTypeCode() + "(" + Objects.requireNonNull(getElementType()) + ")";
        }

        /**
         * Returns <code>true</code> if a nested protobuf message is required for the given type, otherwise <code>false</code>.
         *
         * @param elementType the type to check whether a nested protobuf message is required for.
         *
         * @return <code>true</code> if a nested protobuf message is required for the given type, otherwise <code>false</code>.
         */
        public static boolean needsNestedProto(final Type elementType) {
            return Objects.requireNonNull(elementType).isNullable() || elementType.getTypeCode() == TypeCode.ARRAY;
        }
    }
}
