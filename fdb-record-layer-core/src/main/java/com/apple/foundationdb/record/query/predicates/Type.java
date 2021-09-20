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
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

    @Nonnull
    Supplier<BiMap<Class<?>, TypeCode>> CLASS_TO_TYPE_CODE_SUPPLIER = Suppliers.memoize(TypeCode::computeClassToTypeCodeMap);

    static Map<Class<?>, TypeCode> getClassToTypeCodeMap() {
        return CLASS_TO_TYPE_CODE_SUPPLIER.get();
    }

    @Nonnull
    static Type primitiveType(@Nonnull final TypeCode typeCode) {
        Verify.verify(typeCode.isPrimitive());
        return new Type() {
            @Override
            public TypeCode getTypeCode() {
                return typeCode;
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
        UNKNOWN(Void.class, true, false),
        ANY(Void.class, false, false),
        BOOLEAN(Boolean.class, true, false),
        BYTES(ByteString.class, true, false),
        DOUBLE(Double.class, true, true),
        FLOAT(Float.class, true, true),
        INT(Integer.class, true, true),
        LONG(Long.class, true, true),
        STRING(String.class, true, false),
        TUPLE(List.class, false, false),
        RECORD(Message.class, false, false),
        COLLECTION(Collection.class, false, false),
        STREAM(Void.class, false, false),
        FUNCTION(Void.class, false, false);

        @Nonnull
        private final Class<?> javaClass;
        private final boolean isPrimitive;
        private final boolean isNumeric;

        TypeCode(@Nonnull final Class<?> javaClass, final boolean isPrimitive, final boolean isNumeric) {
            this.javaClass = javaClass;
            this.isPrimitive = isPrimitive;
            this.isNumeric = isNumeric;
        }

        @Nonnull
        public Class<?> getJavaClass() {
            return javaClass;
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
                builder.put(typeCode.getJavaClass(), typeCode);
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

        @Override
        public String toString() {
            return isErased()
                   ? getTypeCode().toString()
                   : getTypeCode() + ":(" + Objects.requireNonNull(getParameterTypes()).stream().map(Object::toString).collect(Collectors.joining(",")) + ")->" + getResultType();
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

        @Override
        public String toString() {
            return isErased()
                   ? getTypeCode().toString()
                   : getTypeCode() + "(" + Objects.requireNonNull(getElementTypes()).stream().map(Object::toString).collect(Collectors.joining(",")) + ")";
        }
    }

    class Record implements Type {
        @Nullable
        private final Map<String, Descriptors.FieldDescriptor> fieldDescriptorMap;
        @Nonnull
        private final Supplier<Map<String, Type>> fieldTypeMapSupplier;

        public Record() {
            this(null);
        }

        public Record(@Nullable final Map<String, Descriptors.FieldDescriptor> fieldDescriptorMap) {
            this.fieldDescriptorMap = fieldDescriptorMap == null ? null : ImmutableMap.copyOf(fieldDescriptorMap);
            this.fieldTypeMapSupplier = Suppliers.memoize(this::computeFieldTypeMap);
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
            return fieldTypeMapSupplier.get();
        }

        @Nullable
        private Map<String, Type> computeFieldTypeMap() {
            if (isErased()) {
                return null;
            }
            final ImmutableMap.Builder<String, Type> fieldTypeMapBuilder = ImmutableMap.builder();
            for (final Map.Entry<String, Descriptors.FieldDescriptor> entry : Objects.requireNonNull(fieldDescriptorMap).entrySet()) {
                final Descriptors.FieldDescriptor fieldDescriptor = entry.getValue();
                final TypeCode typeCode = TypeCode.fromProtobufType(fieldDescriptor.getType());
                if (typeCode.isPrimitive()) {
                    final Type primitiveType = primitiveType(typeCode);
                    fieldTypeMapBuilder.put(entry.getKey(), fieldDescriptor.isRepeated() ? new Type.Collection(primitiveType) : primitiveType);
                } else if (typeCode == TypeCode.RECORD) {
                    final Record recordType = new Record(toFieldDescriptorMap(fieldDescriptor.getMessageType().getFields()));
                    fieldTypeMapBuilder.put(entry.getKey(), fieldDescriptor.isRepeated() ? new Type.Collection(recordType) : recordType);
                }
            }

            return fieldTypeMapBuilder.build();
        }

        boolean isErased() {
            return fieldDescriptorMap == null;
        }

        @Override
        public String toString() {
            return isErased()
                   ? getTypeCode().toString()
                   : getTypeCode() + "(" + Objects.requireNonNull(getFieldTypeMap()).entrySet().stream().map(entry -> entry.getKey() + "->" + entry.getValue()).collect(Collectors.joining(",")) + ")";
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

        @Override
        public String toString() {
            return isErased()
                   ? getTypeCode().toString()
                   : getTypeCode() + "(" + Objects.requireNonNull(getInnerType()) + ")";
        }
    }
}
