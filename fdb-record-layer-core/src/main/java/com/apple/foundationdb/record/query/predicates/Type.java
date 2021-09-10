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

    enum TypeCode {
        UNKNOWN(Void.class, true, false),
        ANY(Void.class, false, false),
        BOOLEAN(Boolean.class, true, false),
        BYTES(byte[].class, true, false),
        DOUBLE(Double.class, true, true),
        FLOAT(Float.class, true, true),
        INT(Integer.class, true, true),
        LONG(Long.class, true, true),
        STRING(String.class, true, false),
        RECORD(Message.class, false, false),
        RELATION(Void.class, false, false),
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
        static BiMap<Class<?>, TypeCode> computeClassToTypeCodeMap() {
            ImmutableBiMap.Builder<Class<?>, TypeCode> builder = ImmutableBiMap.builder();
            for (final TypeCode typeCode : TypeCode.values()) {
                builder.put(typeCode.getJavaClass(), typeCode);
            }
            return builder.build();
        }
    }

    class Any implements Type {
        @Override
        public TypeCode getTypeCode() {
            return TypeCode.ANY;
        }
    }

    class Relation implements Type {
        @Nullable
        final List<Type> columnTypes;

        public Relation() {
            this(null);
        }

        public Relation(@Nullable final List<Type> columnTypes) {
            this.columnTypes = columnTypes == null ? null : ImmutableList.copyOf(columnTypes);
        }

        @Override
        public TypeCode getTypeCode() {
            return TypeCode.RELATION;
        }

        @Override
        public Class<?> getJavaClass() {
            throw new UnsupportedOperationException("should not have been asked");
        }

        @Override
        public boolean isPrimitive() {
            return false;
        }

        @Override
        public boolean isNullable() {
            return true;
        }

        @Nullable
        public List<Type> getColumnTypes() {
            return columnTypes;
        }

        boolean isErased() {
            return getColumnTypes() == null;
        }

        @Override
        public String toString() {
            return isErased()
                   ? getTypeCode().toString()
                   : getTypeCode() + "(" + Objects.requireNonNull(getColumnTypes()).stream().map(Object::toString).collect(Collectors.joining(",")) + ")";
        }
    }

    class Function implements Type {
        @Nullable
        final List<Type> parameterTypes;

        @Nullable
        final Type resultType;

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
        public boolean isPrimitive() {
            return false;
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

        boolean isErased() {
            return getParameterTypes() == null;
        }

        @Override
        public String toString() {
            return isErased()
                   ? getTypeCode().toString()
                   : getTypeCode() + ":(" + Objects.requireNonNull(getParameterTypes()).stream().map(Object::toString).collect(Collectors.joining(",")) + ")->" + getResultType();
        }
    }
}
