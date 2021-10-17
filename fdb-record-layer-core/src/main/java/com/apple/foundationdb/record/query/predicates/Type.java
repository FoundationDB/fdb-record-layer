/*
 * Type.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.query.predicates;

import com.google.common.base.Suppliers;
import com.google.common.base.Verify;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
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

public interface Type {
    TypeCode getTypeCode();

    default Class<?> getJavaClass() {
        return getTypeCode().getJavaClass();
    }

    default boolean isPrimitive() {
        return getTypeCode().isPrimitive();
    }

    boolean isNullable();

    default boolean isNumeric() {
        return getTypeCode().isNumeric();
    }

    default <T extends Type> Optional<T> narrow(@Nonnull final Class<T> clazz) {
        if (clazz.isInstance(this)) {
            return Optional.of(clazz.cast(this));
        } else {
            return Optional.empty();
        }
    }

    @Nullable
    DescriptorProto buildDescriptor(@Nonnull final String typeName);

    void addProtoField(@Nonnull final DescriptorProto.Builder descriptorBuilder,
                       final int fieldIndex,
                       @Nonnull final String fieldName,
                       @Nonnull final String typeName,
                       @Nonnull final FieldDescriptorProto.Label label);

    @Nonnull
    Supplier<BiMap<Class<?>, TypeCode>> CLASS_TO_TYPE_CODE_SUPPLIER = Suppliers.memoize(TypeCode::computeClassToTypeCodeMap);

    static Map<Class<?>, TypeCode> getClassToTypeCodeMap() {
        return CLASS_TO_TYPE_CODE_SUPPLIER.get();
    }

    static String typeName(final Object fieldSuffix) {
        return "__type__" + fieldSuffix;
    }

    static String fieldName(final Object fieldSuffix) {
        return "__field__" + fieldSuffix;
    }

    @Nonnull
    static Type primitiveType(@Nonnull final TypeCode typeCode) {
        return primitiveType(typeCode, true);
    }

    @Nonnull
    static Type primitiveType(@Nonnull final TypeCode typeCode, final boolean isNullable) {
        Verify.verify(typeCode.isPrimitive());
        return new Type() {
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
            public DescriptorProto buildDescriptor(@Nonnull final String typeName) {
                return null;
            }

            @Override
            public void addProtoField(@Nonnull final DescriptorProto.Builder descriptorBuilder,
                                      final int fieldIndex,
                                      @Nonnull final String fieldName,
                                      @Nonnull final String typeName,
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
                return Objects.hash(getTypeCode().hashCode(), isNullable());
            }

            @Override
            public boolean equals(final Object obj) {
                if (obj == null) {
                    return false;
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

    @Nonnull
    static List<Type> fromAtoms(@Nonnull List<? extends Atom> atom) {
        return atom.stream()
                .map(Atom::getResultType)
                .collect(ImmutableList.toImmutableList());
    }

    static String uniqueCompliantTypeName() {
        final String safeUuid = UUID.randomUUID().toString().replace('-', '_');
        return "__type__" + safeUuid;
    }

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
        TUPLE(List.class, null, false, false),
        RECORD(Message.class, null, false, false),
        ARRAY(Array.class, null, false, false),
        STREAM(null, null, false, false),
        FUNCTION(null, null, false, false);

        @Nullable
        private final Class<?> javaClass;
        @Nullable
        private final FieldDescriptorProto.Type protoType;

        private final boolean isPrimitive;
        private final boolean isNumeric;

        TypeCode(@Nullable final Class<?> javaClass,
                 @Nullable final FieldDescriptorProto.Type protoType,
                 final boolean isPrimitive,
                 final boolean isNumeric) {
            this.javaClass = javaClass;
            this.protoType = protoType;
            this.isPrimitive = isPrimitive;
            this.isNumeric = isNumeric;
        }

        @Nullable
        public Class<?> getJavaClass() {
            return javaClass;
        }

        @Nullable
        public FieldDescriptorProto.Type getProtoType() {
            return protoType;
        }

        public boolean isPrimitive() {
            return isPrimitive;
        }

        public boolean isNumeric() {
            return isNumeric;
        }

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

    class Any implements Type {
        @Override
        public TypeCode getTypeCode() {
            return TypeCode.ANY;
        }

        @Override
        public boolean isNullable() {
            return true;
        }

        @Nullable
        @Override
        public DescriptorProto buildDescriptor(@Nonnull final String typeName) {
            throw new UnsupportedOperationException("type any cannot be represented in protobuf");
        }

        @Override
        public void addProtoField(@Nonnull final DescriptorProto.Builder descriptorBuilder,
                                  final int fieldIndex,
                                  @Nonnull final String fieldName,
                                  @Nonnull final String typeName,
                                  @Nonnull final FieldDescriptorProto.Label label) {
            throw new UnsupportedOperationException("type any cannot be represented in protobuf");
        }

        @Override
        public int hashCode() {
            return Objects.hash(getTypeCode().hashCode(), isNullable());
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == null) {
                return false;
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

    class Function implements Type {
        @Nullable
        private final List<Type> parameterTypes;

        @Nullable
        private final Type resultType;

        public Function() {
            this(null, null);
        }

        public Function(@Nullable final List<Type> parameterTypes, @Nullable final Type resultType) {
            this.parameterTypes = parameterTypes == null ? null : ImmutableList.copyOf(parameterTypes);
            this.resultType = resultType;
        }

        @Override
        public TypeCode getTypeCode() {
            return TypeCode.FUNCTION;
        }

        @Override
        public Class<?> getJavaClass() {
            throw new UnsupportedOperationException("should not have been asked");
        }

        @Override
        public boolean isNullable() {
            return false;
        }

        @Nullable
        public List<Type> getParameterTypes() {
            return parameterTypes;
        }

        @Nullable
        public Type getResultType() {
            return resultType;
        }

        public boolean isErased() {
            return getParameterTypes() == null;
        }

        @Nullable
        @Override
        public DescriptorProto buildDescriptor(@Nonnull final String typeName) {
            throw new UnsupportedOperationException("type function cannot be represented in protobuf");
        }

        @Override
        public void addProtoField(@Nonnull final DescriptorProto.Builder descriptorBuilder,
                                  final int fieldIndex,
                                  @Nonnull final String fieldName,
                                  @Nonnull final String typeName,
                                  @Nonnull final FieldDescriptorProto.Label label) {
            throw new UnsupportedOperationException("type function cannot be represented in protobuf");
        }

        @Override
        public int hashCode() {
            return Objects.hash(getTypeCode().hashCode(), isNullable());
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == null) {
                return false;
            }

            if (getClass() != obj.getClass()) {
                return false;
            }

            final Type otherType = (Type)obj;

            return getTypeCode() == otherType.getTypeCode() && isNullable() == otherType.isNullable();
        }

        @Override
        public String toString() {
            return isErased()
                   ? getTypeCode().toString()
                   : getTypeCode() + ":(" + Objects.requireNonNull(getParameterTypes()).stream().map(Object::toString).collect(Collectors.joining(", ")) + ") -> " + getResultType();
        }
    }

    class Record implements Type {
        private final boolean isNullable;

        @Nullable
        private final List<Field> fields;
        @Nonnull
        private final Supplier<Map<String, Type>> fieldTypeMapSupplier;
        @Nonnull
        private final Supplier<List<Type>> elementTypesSupplier;

        private Record(final boolean isNullable,
                       @Nullable final List<Field> fields) {
            this.isNullable = isNullable;
            this.fields = fields == null ? null : ImmutableList.copyOf(fields);
            this.fieldTypeMapSupplier = Suppliers.memoize(this::computeFieldTypeMap);
            this.elementTypesSupplier = Suppliers.memoize(this::computeElementTypes);
        }

        @Override
        public TypeCode getTypeCode() {
            return TypeCode.TUPLE;
        }

        @Override
        public boolean isNullable() {
            return isNullable;
        }

        @Nullable
        public List<Field> getFields() {
            return fields;
        }

        @Nullable
        public List<Type> getElementTypes() {
            return elementTypesSupplier.get();
        }

        private List<Type> computeElementTypes() {
            return Objects.requireNonNull(fields)
                    .stream()
                    .map(Field::getFieldType)
                    .collect(ImmutableList.toImmutableList());
        }

        @Nullable
        public Map<String, Type> getFieldTypeMap() {
            return fieldTypeMapSupplier.get();
        }

        @SuppressWarnings("OptionalGetWithoutIsPresent")
        private Map<String, Type> computeFieldTypeMap() {
            return Objects.requireNonNull(fields).stream()
                    .filter(field -> field.getFieldNameOptional().isPresent())
                    .collect(ImmutableMap.toImmutableMap(field -> field.getFieldNameOptional().get(), Field::getFieldType));
        }

        boolean isErased() {
            return fields == null;
        }

        @Nullable
        @Override
        public DescriptorProto buildDescriptor(@Nonnull final String typeName) {
            Objects.requireNonNull(fields);
            final DescriptorProto.Builder recordMsgBuilder = DescriptorProto.newBuilder();
            recordMsgBuilder.setName(typeName);

            int i = 0;
            for (final Field field : fields) {
                final Type fieldType = field.getFieldType();
                final Optional<String> fieldNameOptional = field.getFieldNameOptional();
                if (fieldNameOptional.isPresent()) {
                    final String fieldName = fieldNameOptional.get();
                    fieldType.addProtoField(recordMsgBuilder, field.getFieldIndexOptional().orElse(i + 1), fieldName, typeName(fieldName), FieldDescriptorProto.Label.LABEL_OPTIONAL);
                } else {
                    // anonymous field
                    fieldType.addProtoField(recordMsgBuilder, field.getFieldIndexOptional().orElse(i + 1), fieldName(i + 1), typeName(i + 1), FieldDescriptorProto.Label.LABEL_OPTIONAL);
                }
                i++;
            }

            return recordMsgBuilder.build();
        }

        @Override
        public void addProtoField(@Nonnull final DescriptorProto.Builder descriptorBuilder, final int fieldIndex, @Nonnull final String fieldName, @Nonnull final String typeName, @Nonnull final FieldDescriptorProto.Label label) {
            descriptorBuilder.addNestedType(buildDescriptor(typeName));
            descriptorBuilder.addField(FieldDescriptorProto.newBuilder()
                    .setName(fieldName)
                    .setNumber(fieldIndex)
                    .setTypeName(typeName)
                    .setLabel(label)
                    .build());
        }

        @Override
        public int hashCode() {
            return Objects.hash(getTypeCode().hashCode(), isNullable(), fields);
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == null) {
                return false;
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
                   : getTypeCode() + "(" + Objects.requireNonNull(getFieldTypeMap()).entrySet().stream().map(entry -> entry.getKey() + " -> " + entry.getValue()).collect(Collectors.joining(", ")) + ")";
        }

        public static Record erased() {
            return new Record(true,  null);
        }

        public static Record fromFields(@Nonnull final List<Field> fields) {
            return fromFields(true, fields);
        }

        public static Record fromFields(final boolean isNullable, @Nonnull final List<Field> fields) {
            return new Record(isNullable, fields);
        }

        public static Record fromFieldDescriptorsMap(final Map<String, Descriptors.FieldDescriptor> fieldDescriptorMap) {
            return fromFieldDescriptorsMap(true, fieldDescriptorMap);
        }

        public static Record fromFieldDescriptorsMap(final boolean isNullable, final Map<String, Descriptors.FieldDescriptor> fieldDescriptorMap) {
            final ImmutableList.Builder<Field> fieldsBuilder = ImmutableList.builder();
            for (final Map.Entry<String, Descriptors.FieldDescriptor> entry : Objects.requireNonNull(fieldDescriptorMap).entrySet()) {
                final Descriptors.FieldDescriptor fieldDescriptor = entry.getValue();
                //final TypeCode typeCode = TypeCode.fromProtobufType(fieldDescriptor.getType());
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

        @Nullable
        private static Descriptors.Descriptor getMessageTypeIfMessage(@Nonnull final Descriptors.FieldDescriptor fieldDescriptor) {
            return fieldDescriptor.getType() == Descriptors.FieldDescriptor.Type.MESSAGE ? fieldDescriptor.getMessageType() : null;
        }

        private static Type fromProtoType(@Nullable Descriptors.Descriptor descriptor,
                                          @Nonnull Descriptors.FieldDescriptor.Type protoType,
                                          @Nonnull DescriptorProtos.FieldDescriptorProto.Label protoLabel,
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

        public static Record fromDescriptor(final Descriptors.Descriptor descriptor) {
            return fromFieldDescriptorsMap(toFieldDescriptorMap(descriptor.getFields()));
        }

        @Nonnull
        private static Optional<Descriptors.FieldDescriptor> arrayElementFieldDescriptorMaybe(final Descriptors.Descriptor descriptor) {
            final List<Descriptors.FieldDescriptor> fields = descriptor.getFields();
            if (fields.size() == 1) {
                final Descriptors.FieldDescriptor field0 = fields.get(0);
                if (field0.isOptional() && !field0.isRepeated() && field0.getNumber() == 1 && "values".equals(field0.getName())) {
                    return Optional.of(field0);
                }
            }
            return Optional.empty();
        }

        @Nonnull
        public static Map<String, Descriptors.FieldDescriptor> toFieldDescriptorMap(@Nonnull final List<Descriptors.FieldDescriptor> fieldDescriptors) {
            return fieldDescriptors
                    .stream()
                    .collect(ImmutableMap.toImmutableMap(Descriptors.FieldDescriptor::getName, fieldDescriptor -> fieldDescriptor));
        }

        @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
        public static class Field {
            @Nonnull
            private final Type fieldType;
            @Nonnull
            private final Optional<String> fieldNameOptional;
            @Nonnull
            private final Optional<Integer> fieldIndexOptional;

            private Field(@Nonnull final Type fieldType, @Nonnull final Optional<String> fieldNameOptional, @Nonnull Optional<Integer> fieldIndexOptional) {
                this.fieldType = fieldType;
                this.fieldNameOptional = fieldNameOptional;
                this.fieldIndexOptional = fieldIndexOptional;
            }

            @Nonnull
            public Type getFieldType() {
                return fieldType;
            }

            @Nonnull
            public Optional<String> getFieldNameOptional() {
                return fieldNameOptional;
            }

            @Nonnull
            public Optional<Integer> getFieldIndexOptional() {
                return fieldIndexOptional;
            }

            @Override
            public boolean equals(final Object o) {
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
                return Objects.hash(getFieldType(), getFieldNameOptional(), getFieldIndexOptional());
            }

            public static Field of(@Nonnull final Type fieldType, @Nonnull final Optional<String> fieldNameOptional, @Nonnull Optional<Integer> fieldIndexOptional) {
                return new Field(fieldType, fieldNameOptional, fieldIndexOptional);
            }

            public static Field of(@Nonnull final Type fieldType, @Nonnull final Optional<String> fieldNameOptional) {
                return new Field(fieldType, fieldNameOptional, Optional.empty());
            }
        }
    }

    class Stream implements Type {
        @Nullable
        private final Type innerType;

        public Stream() {
            this(null);
        }

        public Stream(@Nullable final Type innerType) {
            this.innerType = innerType;
        }

        @Override
        public TypeCode getTypeCode() {
            return TypeCode.STREAM;
        }

        @Override
        public Class<?> getJavaClass() {
            throw new UnsupportedOperationException("should not have been asked");
        }

        @Override
        public boolean isNullable() {
            return false;
        }

        @Nullable
        public Type getInnerType() {
            return innerType;
        }

        boolean isErased() {
            return getInnerType() == null;
        }

        @Nullable
        @Override
        public DescriptorProto buildDescriptor(@Nonnull final String typeName) {
            throw new IllegalStateException("this should not have been called");
        }

        @Override
        public void addProtoField(@Nonnull final DescriptorProto.Builder descriptorBuilder, final int fieldIndex, @Nonnull final String fieldName, @Nonnull final String typeName, @Nonnull final FieldDescriptorProto.Label label) {
            throw new IllegalStateException("this should not have been called");
        }

        @Override
        public int hashCode() {
            return Objects.hash(getTypeCode().hashCode(), isNullable(), innerType);
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == null) {
                return false;
            }

            if (getClass() != obj.getClass()) {
                return false;
            }

            final Stream otherType = (Stream)obj;

            return getTypeCode() == otherType.getTypeCode() && isNullable() == otherType.isNullable() &&
                   ((isErased() && otherType.isErased()) || Objects.requireNonNull(innerType).equals(otherType.innerType));
        }

        @Override
        public String toString() {
            return isErased()
                   ? getTypeCode().toString()
                   : getTypeCode() + "(" + Objects.requireNonNull(getInnerType()) + ")";
        }
    }

    class Array implements Type {
        private final boolean isNullable;

        @Nullable
        private final Type elementType;

        public Array() {
            this( null);
        }

        public Array(@Nullable final Type elementType) {
            this(true, elementType);
        }

        public Array(final boolean isNullable, @Nullable final Type elementType) {
            this.isNullable = isNullable;
            this.elementType = elementType;
        }

        @Override
        public TypeCode getTypeCode() {
            return TypeCode.ARRAY;
        }

        @Override
        public Class<?> getJavaClass() {
            return java.util.Collection.class;
        }

        @Override
        public boolean isNullable() {
            return isNullable;
        }

        public boolean definesNestedProto() {
            return needsNestedProto(elementType);
        }

        @Nullable
        public Type getElementType() {
            return elementType;
        }

        boolean isErased() {
            return getElementType() == null;
        }

        @Nullable
        @Override
        public DescriptorProto buildDescriptor(@Nonnull final String typeName) {
            Objects.requireNonNull(elementType);
            final DescriptorProto.Builder helperDescriptorBuilder = DescriptorProto.newBuilder();
            helperDescriptorBuilder.setName(typeName);
            elementType.addProtoField(helperDescriptorBuilder, 1, "value", typeName, FieldDescriptorProto.Label.LABEL_OPTIONAL);

            return helperDescriptorBuilder.build();
        }

        @Override
        public void addProtoField(@Nonnull final DescriptorProto.Builder descriptorBuilder, final int fieldIndex, @Nonnull final String fieldName, @Nonnull final String typeName, @Nonnull final FieldDescriptorProto.Label label) {
            Objects.requireNonNull(elementType);
            //
            // If the inner type is nullable, we need to create a nested helper message to keep track of
            // nulls.
            //
            if (definesNestedProto()) {
                final DescriptorProto helperDescriptor = buildDescriptor(typeName);

                descriptorBuilder.addNestedType(helperDescriptor);
                descriptorBuilder.addField(FieldDescriptorProto.newBuilder()
                        .setName(fieldName)
                        .setNumber(fieldIndex)
                        .setTypeName(typeName)
                        .setLabel(FieldDescriptorProto.Label.LABEL_REPEATED)
                        .build());
            } else {
                // if inner type is not nullable we can just put is straight into its parent
                elementType.addProtoField(descriptorBuilder, fieldIndex, fieldName, typeName, FieldDescriptorProto.Label.LABEL_REPEATED);
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(getTypeCode().hashCode(), isNullable(), elementType);
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == null) {
                return false;
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

        public static boolean needsNestedProto(final Type elementType) {
            return Objects.requireNonNull(elementType).isNullable() || elementType.getTypeCode() == TypeCode.ARRAY;
        }
    }
}
