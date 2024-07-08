/*
 * DataType.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.api.metadata;

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import com.google.common.base.Suppliers;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * This class represents a Relational data type. A data type has the following characterstics:
 *
 * <ul>
 *   <li>it can be either flat or nested.
 *   <li>it maps to a JDBC SQL Type {@link java.sql.Types}.</li>
 *   <li>it is self-contained.</li>
 *   <li>has a nullability flag.</li>
 *   <li>it is immutable.</li>
 * </ul>
 * A type might be unresolved. This could happen e.g. while constructing a complex type hierarchy
 * where some types are forward-referenced. If a type is unresolved, it is possible to resolve it
 * using a type-resolution map, one example of doing this can be found in build() method in RecordLayerSchemaTemplate.Builder.
 */
public abstract class DataType {
    @Nonnull
    private static final BiMap<Code, Integer> typeCodeJdbcTypeMap;

    static {
        typeCodeJdbcTypeMap = HashBiMap.create();

        typeCodeJdbcTypeMap.put(Code.BOOLEAN, Types.BOOLEAN);
        typeCodeJdbcTypeMap.put(Code.LONG, Types.BIGINT);
        typeCodeJdbcTypeMap.put(Code.INTEGER, Types.INTEGER);
        typeCodeJdbcTypeMap.put(Code.FLOAT, Types.FLOAT);
        typeCodeJdbcTypeMap.put(Code.DOUBLE, Types.DOUBLE);
        typeCodeJdbcTypeMap.put(Code.STRING, Types.VARCHAR);
        typeCodeJdbcTypeMap.put(Code.ENUM, Types.JAVA_OBJECT); // TODO (Rethink Relational Enum mapping to SQL type)
        typeCodeJdbcTypeMap.put(Code.BYTES, Types.BINARY);
        typeCodeJdbcTypeMap.put(Code.STRUCT, Types.STRUCT);
        typeCodeJdbcTypeMap.put(Code.ARRAY, Types.ARRAY);
    }

    private final boolean isNullable;

    private final boolean isPrimitive;

    @Nonnull
    private final Code code;

    private DataType(boolean isNullable, boolean isPrimitive, @Nonnull Code code) {
        this.isNullable = isNullable;
        this.isPrimitive = isPrimitive;
        this.code = code;
    }

    /**
     * Returns the {@link Code} of the type.
     *
     * @return The {@link Code} of the type.
     */
    @Nonnull
    public Code getCode() {
        return code;
    }

    /**
     * Returns a corresponding JDBC SQL type from {@link java.sql.Types}.
     *
     * @return a corresponding JDBC SQL type.
     */
    @SpotBugsSuppressWarnings(value = {"NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE"}, justification = "there is protection against nulls")
    public int getJdbcSqlCode() {
        return typeCodeJdbcTypeMap.get(Objects.requireNonNull(getCode()));
    }

    /**
     * Checks whether the type is primitive.
     *
     * @return {@code True} if the type is primitive, otherwise {@code False}.
     */
    public boolean isPrimitive() {
        return isPrimitive;
    }

    /**
     * Checks whether the type is nullable.
     *
     * @return {@code True} if the type is nullable, otherwise {@code False}.
     */
    public boolean isNullable() {
        return isNullable;
    }

    /**
     * Checks whether the type is resolved. A type is resolved when it, and all of its constituents are resolved.
     *
     * @return {@code True} if the type is resolved, otherwise {@code False}.
     */
    public abstract boolean isResolved();

    /**
     * Returns a new instance of {@code this} type with {@code nullable} field set accordingly.
     *
     * @param isNullable the nullable flag of the newly created {@link DataType} instance.
     * @return a new instance of {@code this} type with {@code nullable} field set accordingly.
     */
    @Nonnull
    public abstract DataType withNullable(boolean isNullable);

    /**
     * Resolves {@code this} {@link DataType}.
     *
     * @param resolutionMap A list of all resolved types used for resolving this type.
     * @return a new {@link DataType} which is resolved.
     */
    @Nonnull
    public abstract DataType resolve(@Nonnull final Map<String, Named> resolutionMap);

    /**
     * Trait representing a type that has a name.
     */
    public interface Named {

        /**
         * Returns the name of the {@link DataType}.
         *
         * @return the name of the {@link DataType}.
         */
        @Nonnull
        String getName();
    }

    // todo: this is ugly, DataType should be an interface.
    public abstract static class NumericType extends DataType {
        private NumericType(boolean isNullable, boolean isPrimitive, @Nonnull Code code) {
            super(isNullable, isPrimitive, code);
        }
    }

    public static final class BooleanType extends DataType {
        @Nonnull
        private static final BooleanType NOT_NULLABLE_INSTANCE = new BooleanType(false);

        @Nonnull
        private static final BooleanType NULLABLE_INSTANCE = new BooleanType(true);

        @Nonnull
        private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

        private BooleanType(boolean isNullable) {
            super(isNullable, true, Code.BOOLEAN);
        }

        @Nonnull
        @Override
        public DataType withNullable(boolean isNullable) {
            if (isNullable) {
                return Primitives.NULLABLE_BOOLEAN.type();
            } else {
                return Primitives.BOOLEAN.type();
            }
        }

        @Override
        public boolean isResolved() {
            return true;
        }

        @Nonnull
        @Override
        public DataType resolve(@Nonnull final Map<String, Named> resolutionMap) {
            return this;
        }

        @Nonnull
        public static BooleanType nullable() {
            return NULLABLE_INSTANCE;
        }

        @Nonnull
        public static BooleanType notNullable() {
            return NOT_NULLABLE_INSTANCE;
        }

        private int computeHashCode() {
            return Objects.hash(getCode(), isNullable());
        }

        @Override
        public int hashCode() {
            return hashCodeSupplier.get();
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (!(other instanceof BooleanType)) {
                return false;
            }
            final var otherBooleanType = (BooleanType) other;
            return this.isNullable() == otherBooleanType.isNullable();
        }

        @Override
        public String toString() {
            return "boolean" + (isNullable() ? " ∪ ∅" : "");
        }
    }

    public static final class IntegerType extends NumericType {
        @Nonnull
        private static final IntegerType NOT_NULLABLE_INSTANCE = new IntegerType(false);

        @Nonnull
        private static final IntegerType NULLABLE_INSTANCE = new IntegerType(true);

        @Nonnull
        private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

        private IntegerType(boolean isNullable) {
            super(isNullable, true, Code.INTEGER);
        }

        @Override
        @Nonnull
        public DataType withNullable(boolean isNullable) {
            if (isNullable) {
                return Primitives.NULLABLE_INTEGER.type();
            } else {
                return Primitives.INTEGER.type();
            }
        }

        @Override
        public boolean isResolved() {
            return true;
        }

        @Nonnull
        @Override
        public DataType resolve(@Nonnull final Map<String, Named> resolutionMap) {
            return this;
        }

        @Nonnull
        public static IntegerType nullable() {
            return NULLABLE_INSTANCE;
        }

        @Nonnull
        public static IntegerType notNullable() {
            return NOT_NULLABLE_INSTANCE;
        }

        private int computeHashCode() {
            return Objects.hash(getCode(), isNullable());
        }

        @Override
        public int hashCode() {
            return hashCodeSupplier.get();
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (!(other instanceof IntegerType)) {
                return false;
            }
            final var otherIntegerType = (IntegerType) other;
            return this.isNullable() == otherIntegerType.isNullable();
        }

        @Override
        public String toString() {
            return "int" + (isNullable() ? " ∪ ∅" : "");
        }
    }

    public static final class LongType extends NumericType {
        @Nonnull
        private static final LongType NOT_NULLABLE_INSTANCE = new LongType(false);

        @Nonnull
        private static final LongType NULLABLE_INSTANCE = new LongType(true);

        @Nonnull
        private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

        private LongType(boolean isNullable) {
            super(isNullable, true, Code.LONG);
        }

        @Override
        @Nonnull
        public DataType withNullable(boolean isNullable) {
            if (isNullable) {
                return Primitives.NULLABLE_LONG.type();
            } else {
                return Primitives.LONG.type();
            }
        }

        @Override
        public boolean isResolved() {
            return true;
        }

        @Nonnull
        @Override
        public DataType resolve(@Nonnull final Map<String, Named> resolutionMap) {
            return this;
        }

        private int computeHashCode() {
            return Objects.hash(getCode(), isNullable());
        }

        @Override
        public int hashCode() {
            return hashCodeSupplier.get();
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (!(other instanceof LongType)) {
                return false;
            }
            final var otherLongType = (LongType) other;
            return this.isNullable() == otherLongType.isNullable();
        }

        @Override
        public String toString() {
            return "long" + (isNullable() ? " ∪ ∅" : "");
        }

        @Nonnull
        public static LongType nullable() {
            return NULLABLE_INSTANCE;
        }

        @Nonnull
        public static LongType notNullable() {
            return NOT_NULLABLE_INSTANCE;
        }
    }

    public static final class FloatType extends NumericType {
        @Nonnull
        private static final FloatType NOT_NULLABLE_INSTANCE = new FloatType(false);

        @Nonnull
        private static final FloatType NULLABLE_INSTANCE = new FloatType(true);

        @Nonnull
        private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

        private FloatType(boolean isNullable) {
            super(isNullable, true, Code.FLOAT);
        }

        @Override
        @Nonnull
        public DataType withNullable(boolean isNullable) {
            if (isNullable) {
                return Primitives.NULLABLE_FLOAT.type();
            } else {
                return Primitives.FLOAT.type();
            }
        }

        @Override
        public boolean isResolved() {
            return true;
        }

        @Nonnull
        @Override
        public DataType resolve(@Nonnull final Map<String, Named> resolutionMap) {
            return this;
        }

        private int computeHashCode() {
            return Objects.hash(getCode(), isNullable());
        }

        @Override
        public int hashCode() {
            return hashCodeSupplier.get();
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (!(other instanceof FloatType)) {
                return false;
            }
            final var otherFloatType = (FloatType) other;
            return this.isNullable() == otherFloatType.isNullable();
        }

        @Override
        public String toString() {
            return "float" + (isNullable() ? " ∪ ∅" : "");
        }

        @Nonnull
        public static FloatType nullable() {
            return NULLABLE_INSTANCE;
        }

        @Nonnull
        public static FloatType notNullable() {
            return NOT_NULLABLE_INSTANCE;
        }
    }

    public static final class DoubleType extends NumericType {
        @Nonnull
        private static final DoubleType NOT_NULLABLE_INSTANCE = new DoubleType(false);

        @Nonnull
        private static final DoubleType NULLABLE_INSTANCE = new DoubleType(true);
        @Nonnull
        private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

        private DoubleType(boolean isNullable) {
            super(isNullable, true, Code.DOUBLE);
        }

        @Override
        @Nonnull
        public DataType withNullable(boolean isNullable) {
            if (isNullable) {
                return Primitives.NULLABLE_DOUBLE.type();
            } else {
                return Primitives.DOUBLE.type();
            }
        }

        @Override
        public boolean isResolved() {
            return true;
        }

        @Nonnull
        @Override
        public DataType resolve(@Nonnull final Map<String, Named> resolutionMap) {
            return this;
        }

        @Nonnull
        public static DoubleType nullable() {
            return NULLABLE_INSTANCE;
        }

        @Nonnull
        public static DoubleType notNullable() {
            return NOT_NULLABLE_INSTANCE;
        }

        private int computeHashCode() {
            return Objects.hash(getCode(), isNullable());
        }

        @Override
        public int hashCode() {
            return hashCodeSupplier.get();
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (!(other instanceof DoubleType)) {
                return false;
            }
            final var otherDoubleType = (DoubleType) other;
            return this.isNullable() == otherDoubleType.isNullable();
        }

        @Override
        public String toString() {
            return "double" + (isNullable() ? " ∪ ∅" : "");
        }
    }

    public static final class StringType extends DataType {
        @Nonnull
        private static final StringType NOT_NULLABLE_INSTANCE = new StringType(false);

        @Nonnull
        private static final StringType NULLABLE_INSTANCE = new StringType(true);

        @Nonnull
        private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

        private StringType(boolean isNullable) {
            super(isNullable, true, Code.STRING);
        }

        @Override
        @Nonnull
        public DataType withNullable(boolean isNullable) {
            if (isNullable) {
                return Primitives.NULLABLE_STRING.type();
            } else {
                return Primitives.STRING.type();
            }
        }

        @Override
        public boolean isResolved() {
            return true;
        }

        @Nonnull
        @Override
        public DataType resolve(@Nonnull final Map<String, Named> resolutionMap) {
            return this;
        }

        private int computeHashCode() {
            return Objects.hash(getCode(), isNullable());
        }

        @Override
        public int hashCode() {
            return hashCodeSupplier.get();
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (!(other instanceof StringType)) {
                return false;
            }
            final var otherStringType = (StringType) other;
            return this.isNullable() == otherStringType.isNullable();
        }

        @Override
        public String toString() {
            return "string" + (isNullable() ? " ∪ ∅" : "");
        }

        @Nonnull
        public static StringType nullable() {
            return NULLABLE_INSTANCE;
        }

        @Nonnull
        public static StringType notNullable() {
            return NOT_NULLABLE_INSTANCE;
        }
    }

    public static final class BytesType extends DataType {
        @Nonnull
        private static final BytesType NOT_NULLABLE_INSTANCE = new BytesType(false);

        @Nonnull
        private static final BytesType NULLABLE_INSTANCE = new BytesType(true);

        @Nonnull
        private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

        private BytesType(boolean isNullable) {
            super(isNullable, true, Code.BYTES);
        }

        @Override
        @Nonnull
        public DataType withNullable(boolean isNullable) {
            if (isNullable) {
                return Primitives.NULLABLE_BYTES.type();
            } else {
                return Primitives.BYTES.type();
            }
        }

        @Override
        public boolean isResolved() {
            return true;
        }

        @Nonnull
        @Override
        public DataType resolve(@Nonnull Map<String, Named> resolutionMap) {
            return this;
        }

        @Nonnull
        public static BytesType nullable() {
            return NULLABLE_INSTANCE;
        }

        @Nonnull
        public static BytesType notNullable() {
            return NOT_NULLABLE_INSTANCE;
        }

        private int computeHashCode() {
            return Objects.hash(getCode(), isNullable());
        }

        @Override
        public int hashCode() {
            return hashCodeSupplier.get();
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (!(other instanceof BytesType)) {
                return false;
            }
            final var otherBytesType = (BytesType) other;
            return this.isNullable() == otherBytesType.isNullable();
        }

        @Override
        public String toString() {
            return "bytes" + (isNullable() ? " ∪ ∅" : "");
        }
    }

    public static final class VersionType extends DataType {
        @Nonnull
        private static final VersionType NOT_NULLABLE_INSTANCE = new VersionType(false);

        @Nonnull
        private static final VersionType NULLABLE_INSTANCE = new VersionType(true);

        @Nonnull
        private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

        private VersionType(boolean isNullable) {
            super(isNullable, true, Code.VERSION);
        }

        @Override
        @Nonnull
        public DataType withNullable(boolean isNullable) {
            if (isNullable) {
                return Primitives.NULLABLE_VERSION.type();
            } else {
                return Primitives.VERSION.type();
            }
        }

        @Override
        public boolean isResolved() {
            return true;
        }

        @Nonnull
        @Override
        public DataType resolve(@Nonnull Map<String, Named> resolutionMap) {
            return this;
        }

        @Nonnull
        public static VersionType nullable() {
            return NULLABLE_INSTANCE;
        }

        @Nonnull
        public static VersionType notNullable() {
            return NOT_NULLABLE_INSTANCE;
        }

        private int computeHashCode() {
            return Objects.hash(getCode(), isNullable());
        }

        @Override
        public int hashCode() {
            return hashCodeSupplier.get();
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (!(other instanceof VersionType)) {
                return false;
            }
            final var otherVersionType = (VersionType) other;
            return this.isNullable() == otherVersionType.isNullable();
        }

        @Override
        public String toString() {
            return "version" + (isNullable() ? " ∪ ∅" : "");
        }
    }

    public static final class EnumType extends DataType implements Named {
        @Nonnull
        private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

        @Nonnull
        private final String name;

        @Nonnull
        private final List<EnumValue> values;

        public static class EnumValue {
            @Nonnull
            private final String name;

            private final int number;

            private EnumValue(@Nonnull final String name, int number) {
                this.name = name;
                this.number = number;
            }

            @Nonnull
            public static EnumValue of(@Nonnull final String name, int number) {
                return new EnumValue(name, number);
            }

            @Nonnull
            public String getName() {
                return name;
            }

            public int getNumber() {
                return number;
            }

            @Override
            public String toString() {
                return name;
            }
        }

        @Override
        public String toString() {
            return String.format("enum(%s){%s}", name, values.stream().map(EnumValue::toString).collect(Collectors.joining(",")));
        }

        private EnumType(@Nonnull String name, @Nonnull final List<EnumValue> values, boolean isNullable) {
            super(isNullable, true, Code.ENUM);
            this.name = name;
            this.values = values;
        }

        @Nonnull
        public List<EnumValue> getValues() {
            return values;
        }

        @Nonnull
        public static EnumType from(@Nonnull final String name, @Nonnull final List<EnumValue> values, boolean isNullable) {
            Assert.thatUnchecked(!values.isEmpty());
            Assert.thatUnchecked(!name.isEmpty());
            return new EnumType(name, values, isNullable);
        }

        @Override
        @Nonnull
        public EnumType withNullable(boolean isNullable) {
            if (isNullable == isNullable()) {
                return this;
            }
            return new EnumType(getName(), values, isNullable);
        }

        @Override
        @Nonnull
        public String getName() {
            return name;
        }

        @Override
        public boolean isResolved() {
            return true;
        }

        @Nonnull
        @Override
        public DataType resolve(@Nonnull final Map<String, Named> resolutionMap) {
            return this;
        }

        private int computeHashCode() {
            return Objects.hash(getCode(), isNullable(), values);
        }

        @Override
        public int hashCode() {
            return hashCodeSupplier.get();
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (!(other instanceof EnumType)) {
                return false;
            }
            final var otherEnumType = (EnumType) other;
            return this.isNullable() == otherEnumType.isNullable() &&
                    name.equals(otherEnumType.name) &&
                    values.equals(otherEnumType.values);
        }
    }

    public static final class ArrayType extends DataType {
        @Nonnull
        private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

        @Nonnull
        private final DataType elementType;

        private ArrayType(boolean isNullable, @Nonnull final DataType elementType) {
            super(isNullable, false, Code.ARRAY);
            this.elementType = elementType;
        }

        @Nonnull
        public static ArrayType from(@Nonnull final DataType type) {
            return from(type, false);
        }

        @Nonnull
        public static ArrayType from(@Nonnull final DataType type, boolean isNullable) {
            return new ArrayType(isNullable, type);
        }

        @Nonnull
        public DataType getElementType() {
            return elementType;
        }

        @Override
        @Nonnull
        public ArrayType withNullable(boolean isNullable) {
            if (isNullable == isNullable()) {
                return this;
            }
            return new ArrayType(isNullable, elementType);
        }

        @Override
        public boolean isResolved() {
            return elementType.isResolved();
        }

        @Nonnull
        @Override
        public DataType resolve(@Nonnull final Map<String, Named> resolutionMap) {
            if (isResolved()) {
                return this;
            } else {
                return ArrayType.from(elementType.resolve(resolutionMap));
            }
        }

        private int computeHashCode() {
            return Objects.hash(getCode(), isNullable(), elementType);
        }

        @Override
        public int hashCode() {
            return hashCodeSupplier.get();
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (!(other instanceof ArrayType)) {
                return false;
            }
            final var otherArrayType = (ArrayType) other;
            return this.isNullable() == otherArrayType.isNullable() &&
                    elementType.equals(otherArrayType.elementType);
        }

        @Override
        public String toString() {
            return "[" + elementType + "]" + (isNullable() ? " ∪ ∅" : "");
        }
    }

    public static final class StructType extends DataType implements Named {
        @Nonnull
        private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

        @Nonnull
        private final List<Field> fields;

        @Nonnull
        private final String name;

        @Nonnull
        private final Supplier<Boolean> resolvedSupplier = Suppliers.memoize(this::calculateResolved);

        private StructType(@Nonnull final String name, boolean isNullable, @Nonnull final List<Field> fields) {
            super(isNullable, false, Code.STRUCT);
            this.name = name;
            this.fields = ImmutableList.copyOf(fields);
        }

        @Nonnull
        public List<Field> getFields() {
            return fields;
        }

        @Override
        @Nonnull
        public String getName() {
            return name;
        }

        public static class Field {
            @Nonnull
            private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

            @Nonnull
            private final String name;

            @Nonnull
            private final DataType type;

            private final int index;

            private Field(@Nonnull final String name, @Nonnull final DataType type, int index) {
                Assert.thatUnchecked(index >= 0);
                this.name = name;
                this.type = type;
                this.index = index;
            }

            @Nonnull
            public static Field from(@Nonnull final String name, @Nonnull final DataType type, int index) {
                return new Field(name, type, index);
            }

            @Nonnull
            public String getName() {
                return name;
            }

            @Nonnull
            public DataType getType() {
                return type;
            }

            public int getIndex() {
                return index;
            }

            private int computeHashCode() {
                return Objects.hash(name, index, type);
            }

            @Override
            public int hashCode() {
                return hashCodeSupplier.get();
            }

            @Override
            public boolean equals(Object other) {
                if (this == other) {
                    return true;
                }

                if (!(other instanceof Field)) {
                    return false;
                }
                final var otherField = (Field) other;
                return name.equals(otherField.name) &&
                        index == otherField.index &&
                        type.equals(otherField.type);
            }

            @Override
            public String toString() {
                return name;
            }
        }

        @Nonnull
        public static StructType from(@Nonnull final String name, @Nonnull final List<Field> fields, boolean isNullable) {
            return new StructType(name, isNullable, fields);
        }

        @Override
        @Nonnull
        public StructType withNullable(boolean isNullable) {
            if (isNullable == isNullable()) {
                return this;
            }
            return new StructType(name, isNullable, fields);
        }

        private boolean calculateResolved() {
            for (final var column : fields) {
                if (!column.type.isResolved()) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public boolean isResolved() {
            return resolvedSupplier.get();
        }

        @Nonnull
        @Override
        public DataType resolve(@Nonnull final Map<String, Named> resolutionMap) {
            if (isResolved()) {
                return this;
            } else {
                final var resolvedFields = ImmutableList.<Field>builder();
                for (final var field : fields) {
                    resolvedFields.add(Field.from(field.name, field.getType().resolve(resolutionMap), field.index));
                }
                return StructType.from(name, resolvedFields.build(), isNullable());
            }
        }

        private int computeHashCode() {
            return Objects.hash(getCode(), isNullable(), name, fields);
        }

        @Override
        public int hashCode() {
            return hashCodeSupplier.get();
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (!(other instanceof StructType)) {
                return false;
            }
            final var otherStructType = (StructType) other;
            return this.isNullable() == otherStructType.isNullable() &&
                    name.equals(otherStructType.name) &&
                    fields.equals(otherStructType.fields);
        }

        @Override
        public String toString() {
            return name.substring(0, Math.min(name.length(), 5)) + " { " + fields.stream().map(field -> field.getName() + ":" + field.getType()).collect(Collectors.joining(",")) + " } ";
        }
    }

    /**
     * Represents an unknown type, i.e. a type which is not resolved yet, it only keeps reference information
     * about the type (to be resolved), and nullability information.
     * Unknown types can arise during parsing, in situation where a type is used before it is defined (late binding),
     * in these scenarios, we create an {@code UnknownType} and we resolve it later. (resolution in the sense of
     * effectively <i>replacing</i> it with a corresponding resolved type, since {@link DataType}s are immutable.
     * To see how this type is used as resolved, check build() method in RecordLayerSchemaTemplate.Builder.
     */
    public static final class UnresolvedType extends DataType implements Named {
        @Nonnull
        private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

        @Nonnull
        private final String name;

        private UnresolvedType(@Nonnull final String name, boolean isNullable) {
            super(isNullable, false, Code.UNKNOWN);
            this.name = name;
        }

        @Nonnull
        @Override
        public String getName() {
            return name;
        }

        @Nonnull
        @Override
        public DataType withNullable(boolean isNullable) {
            if (isNullable == isNullable()) {
                return this;
            }
            return new UnresolvedType(name, isNullable);
        }

        @Nonnull
        public static UnresolvedType of(@Nonnull final String name, boolean isNullable) {
            return new UnresolvedType(name, isNullable);
        }

        @Override
        public boolean isResolved() {
            return false;
        }

        @Nonnull
        @Override
        public DataType resolve(@Nonnull final Map<String, Named> resolutionMap) {
            Assert.thatUnchecked(resolutionMap.containsKey(name), ErrorCode.INTERNAL_ERROR, "Could not find type %s", name);
            return ((DataType) resolutionMap.get(name)).withNullable(isNullable());
        }

        private int computeHashCode() {
            return Objects.hash(getCode(), isNullable(), name);
        }

        @Override
        public int hashCode() {
            return hashCodeSupplier.get();
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }

            if (!(other instanceof UnresolvedType)) {
                return false;
            }
            final var otherUnresolvedType = (UnresolvedType) other;
            return this.isNullable() == otherUnresolvedType.isNullable() &&
                    name.equals(otherUnresolvedType.name);
        }
    }

    public static final class UnknownType extends DataType {
        @Nonnull
        private final Supplier<Integer> hashCodeSupplier = Suppliers.memoize(this::computeHashCode);

        @Nonnull
        private static final UnknownType INSTANCE = new UnknownType();

        private UnknownType() {
            super(false, false, Code.UNKNOWN);
        }

        @Nonnull
        @Override
        public DataType withNullable(boolean isNullable) {
            throw new RelationalException("Attempt to set nullability on unknown type", ErrorCode.INTERNAL_ERROR).toUncheckedWrappedException();
        }

        @Override
        public boolean isResolved() {
            return false;
        }

        @Nonnull
        @Override
        public DataType resolve(@Nonnull Map<String, Named> resolutionMap) {
            throw new RelationalException("Can not resolve unknown type", ErrorCode.INTERNAL_ERROR).toUncheckedWrappedException();
        }

        private int computeHashCode() {
            return Objects.hash(getCode());
        }

        @Override
        public int hashCode() {
            return hashCodeSupplier.get();
        }

        @Override
        public boolean equals(Object o) {
            // singleton.
            return super.equals(o);
        }

        @Nonnull
        public static UnknownType instance() {
            return INSTANCE;
        }

        @Override
        public String toString() {
            return "???";
        }
    }

    @Nonnull
    public enum Code {
        BOOLEAN,
        LONG,
        INTEGER,
        FLOAT,
        DOUBLE,
        STRING,
        BYTES,
        VERSION,
        ENUM,
        STRUCT,
        ARRAY,
        UNKNOWN
    }

    @Nonnull
    public enum Primitives {
        BOOLEAN(BooleanType.notNullable()),
        LONG(LongType.notNullable()),
        INTEGER(IntegerType.notNullable()),
        FLOAT(FloatType.notNullable()),
        DOUBLE(DoubleType.notNullable()),
        STRING(StringType.notNullable()),
        BYTES(BytesType.notNullable()),
        VERSION(VersionType.notNullable()),
        NULLABLE_BOOLEAN(BooleanType.nullable()),
        NULLABLE_LONG(LongType.nullable()),
        NULLABLE_INTEGER(IntegerType.nullable()),
        NULLABLE_FLOAT(FloatType.nullable()),
        NULLABLE_DOUBLE(DoubleType.nullable()),
        NULLABLE_STRING(StringType.nullable()),
        NULLABLE_BYTES(BytesType.nullable()),
        NULLABLE_VERSION(VersionType.nullable())
        ;

        @Nonnull
        private final DataType datatype;

        Primitives(@Nonnull DataType datatype) {
            this.datatype = datatype;
        }

        @Nonnull
        public DataType type() {
            return datatype;
        }
    }
}
