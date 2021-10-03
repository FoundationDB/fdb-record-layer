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
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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

    default boolean isNullable() {
        return true;
    }

    default boolean isNumeric() {
        return getTypeCode().isNumeric();
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
        Verify.verify(typeCode.isPrimitive());
        return new Type() {
            @Override
            public TypeCode getTypeCode() {
                return typeCode;
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
    static List<Type> fromTyped(@Nonnull List<Atom> atom) {
        return atom.stream()
                .map(Atom::getResultType)
                .collect(ImmutableList.toImmutableList());
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
        COLLECTION(Collection.class, null, false, false),
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

    class Tuple implements Type {
        @Nullable
        private final List<Type> elementTypes;

        public Tuple() {
            this(null);
        }

        public Tuple(@Nullable final List<Type> elementTypes) {
            this.elementTypes = elementTypes == null ? null : ImmutableList.copyOf(elementTypes);
        }

        @Override
        public TypeCode getTypeCode() {
            return TypeCode.TUPLE;
        }

        @Override
        public boolean isNullable() {
            return true;
        }

        @Nullable
        public List<Type> getElementTypes() {
            return elementTypes;
        }

        boolean isErased() {
            return getElementTypes() == null;
        }

        @Nullable
        @Override
        public DescriptorProto buildDescriptor(@Nonnull final String typeName) {
            final DescriptorProto.Builder tupleMsgBuilder = DescriptorProto.newBuilder();

            tupleMsgBuilder.setName(typeName);

            for (int i = 0; i < Objects.requireNonNull(elementTypes).size(); i++) {
                final Type elementType = elementTypes.get(i);
                elementType.addProtoField(tupleMsgBuilder, i + 1, fieldName(i + 1), typeName(i + 1), FieldDescriptorProto.Label.LABEL_OPTIONAL);
            }

            return tupleMsgBuilder.build();
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
            return Objects.hash(getTypeCode().hashCode(), isNullable(), elementTypes);
        }

        @Override
        public boolean equals(final Object obj) {
            if (obj == null) {
                return false;
            }

            if (getClass() != obj.getClass()) {
                return false;
            }

            final Tuple otherType = (Tuple)obj;

            return getTypeCode() == otherType.getTypeCode() && isNullable() == otherType.isNullable() &&
                   ((isErased() && otherType.isErased()) || Objects.requireNonNull(elementTypes).equals(otherType.elementTypes));
        }

        @Override
        public String toString() {
            return isErased()
                   ? getTypeCode().toString()
                   : getTypeCode() + "(" + Objects.requireNonNull(getElementTypes()).stream().map(Object::toString).collect(Collectors.joining(", ")) + ")";
        }
    }

    class Record implements Type {
        @Nullable
        private final Map<String, Type> fieldTypeMap;
        @Nullable
        private final Map<String, Integer> fieldIndexMap;

        public Record() {
            this(null, null);
        }

        private Record(@Nullable final Map<String, Type> fieldTypeMap, @Nullable final Map<String, Integer> fieldIndexMap) {
            this.fieldTypeMap = fieldTypeMap == null ? null : ImmutableMap.copyOf(fieldTypeMap);
            this.fieldIndexMap = fieldIndexMap == null ? null : ImmutableMap.copyOf(fieldIndexMap);
        }

        @Override
        public TypeCode getTypeCode() {
            return TypeCode.RECORD;
        }

        @Override
        public boolean isNullable() {
            return true;
        }

        @Nullable
        public Map<String, Type> getFieldTypeMap() {
            return fieldTypeMap;
        }

        boolean isErased() {
            return fieldTypeMap == null || fieldIndexMap == null;
        }

        @Nullable
        @Override
        public DescriptorProto buildDescriptor(@Nonnull final String typeName) {
            Objects.requireNonNull(fieldTypeMap);
            Objects.requireNonNull(fieldIndexMap);
            final DescriptorProto.Builder recordMsgBuilder = DescriptorProto.newBuilder();
            recordMsgBuilder.setName(typeName);

            int i = 0;
            final Set<Map.Entry<String, Type>> fieldsAndTypes = Objects.requireNonNull(getFieldTypeMap()).entrySet();
            for (final Map.Entry<String, Type> fieldTypeEntry : fieldsAndTypes) {
                fieldTypeEntry.getValue().addProtoField(recordMsgBuilder, fieldIndexMap.getOrDefault(fieldTypeEntry.getKey(), i + 1), fieldTypeEntry.getKey(), typeName(fieldTypeEntry.getKey()), FieldDescriptorProto.Label.LABEL_OPTIONAL);
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
            return Objects.hash(getTypeCode().hashCode(), isNullable(), fieldTypeMap, fieldIndexMap);
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
                    (Objects.requireNonNull(fieldTypeMap).equals(otherType.fieldTypeMap) &&
                     Objects.requireNonNull(fieldIndexMap).equals(otherType.fieldIndexMap)));
        }

        @Override
        public String toString() {
            return isErased()
                   ? getTypeCode().toString()
                   : getTypeCode() + "(" + Objects.requireNonNull(getFieldTypeMap()).entrySet().stream().map(entry -> entry.getKey() + " -> " + entry.getValue()).collect(Collectors.joining(", ")) + ")";
        }

        public static Record erased() {
            return new Record(null, null);
        }

        public static Record fromTypeMap(@Nonnull final Map<String, Type> fieldTypeMap) {
            return new Record(fieldTypeMap, computeDefaultFieldIndexMap(fieldTypeMap));
        }

        private static Map<String, Integer> computeDefaultFieldIndexMap(@Nonnull final Map<String, Type> fieldTypeMap) {
            final ImmutableMap.Builder<String, Integer> resultBuilder = ImmutableMap.builder();
            int i = 0;
            for (Map.Entry<String, Type> entry : fieldTypeMap.entrySet()) {
                resultBuilder.put(entry.getKey(), i + 1);
                i ++;
            }
            return resultBuilder.build();
        }

        public static Record fromFieldDescriptorsMap(final Map<String, Descriptors.FieldDescriptor> fieldDescriptorMap) {
            final ImmutableMap.Builder<String, Type> fieldTypeMapBuilder = ImmutableMap.builder();
            final ImmutableMap.Builder<String, Integer> fieldIndexMapBuilder = ImmutableMap.builder();
            for (final Map.Entry<String, Descriptors.FieldDescriptor> entry : Objects.requireNonNull(fieldDescriptorMap).entrySet()) {
                final Descriptors.FieldDescriptor fieldDescriptor = entry.getValue();
                final TypeCode typeCode = TypeCode.fromProtobufType(fieldDescriptor.getType());
                fieldIndexMapBuilder.put(entry.getKey(), fieldDescriptor.getNumber());
                if (typeCode.isPrimitive()) {
                    final Type primitiveType = primitiveType(typeCode);
                    fieldTypeMapBuilder.put(entry.getKey(), fieldDescriptor.isRepeated() ? new Type.Collection(primitiveType) : primitiveType);
                } else if (typeCode == TypeCode.RECORD) {
                    final Record recordType = fromFieldDescriptorsMap(toFieldDescriptorMap(fieldDescriptor.getMessageType().getFields()));
                    fieldTypeMapBuilder.put(entry.getKey(), fieldDescriptor.isRepeated() ? new Type.Collection(recordType) : recordType);
                }
            }

            return new Record(fieldTypeMapBuilder.build(), fieldIndexMapBuilder.build());
        }

        public static Record fromDescriptor(final Descriptors.Descriptor descriptor) {
            return fromFieldDescriptorsMap(toFieldDescriptorMap(descriptor.getFields()));
        }

        @Nonnull
        public static Map<String, Descriptors.FieldDescriptor> toFieldDescriptorMap(@Nonnull final List<Descriptors.FieldDescriptor> fieldDescriptors) {
            return fieldDescriptors
                    .stream()
                    .collect(ImmutableMap.toImmutableMap(Descriptors.FieldDescriptor::getName, fieldDescriptor -> fieldDescriptor));
        }
    }

    class Stream implements Type {
        @Nullable
        private final Tuple innerType;

        public Stream() {
            this(null);
        }

        public Stream(@Nullable final Tuple innerType) {
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
            return true;
        }

        @Nullable
        public Tuple getInnerType() {
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

    class Collection implements Type {
        @Nullable
        private final Type innerType;

        public Collection() {
            this(null);
        }

        public Collection(@Nullable final Type innerType) {
            this.innerType = innerType;
        }

        @Override
        public TypeCode getTypeCode() {
            return TypeCode.COLLECTION;
        }

        @Override
        public Class<?> getJavaClass() {
            return java.util.Collection.class;
        }

        @Override
        public boolean isNullable() {
            return true;
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
            return null;
        }

        @Override
        public void addProtoField(@Nonnull final DescriptorProto.Builder descriptorBuilder, final int fieldIndex, @Nonnull final String fieldName, @Nonnull final String typeName, @Nonnull final FieldDescriptorProto.Label label) {
            Objects.requireNonNull(innerType).addProtoField(descriptorBuilder, fieldIndex, fieldName, typeName, FieldDescriptorProto.Label.LABEL_REPEATED);
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

            final Collection otherType = (Collection)obj;

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
}
