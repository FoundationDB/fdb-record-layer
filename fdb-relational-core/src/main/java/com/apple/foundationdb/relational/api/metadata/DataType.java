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

import com.apple.foundationdb.relational.recordlayer.util.Assert;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
 *
 * TODO (yhatem) implement equality and hash code semantics.
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

    public static final class BooleanType extends DataType {

        private BooleanType(boolean isNullable) {
            super(isNullable, true, Code.BOOLEAN);
        }

        @Nonnull
        @Override
        public DataType withNullable(boolean isNullable) {
            if (isNullable) {
                return Primitives.BOOLEAN.type();
            } else {
                return Primitives.NULLABLE_BOOLEAN.type();
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

    }

    public static final class IntegerType extends DataType {
        private IntegerType(boolean isNullable) {
            super(isNullable, true, Code.INTEGER);
        }

        @Override
        @Nonnull
        public DataType withNullable(boolean isNullable) {
            if (isNullable) {
                return Primitives.INTEGER.type();
            } else {
                return Primitives.NULLABLE_INTEGER.type();
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
    }

    public static final class LongType extends DataType {
        private LongType(boolean isNullable) {
            super(isNullable, true, Code.LONG);
        }

        @Override
        @Nonnull
        public DataType withNullable(boolean isNullable) {
            if (isNullable) {
                return Primitives.LONG.type();
            } else {
                return Primitives.NULLABLE_LONG.type();
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
    }

    public static final class FloatType extends DataType {
        private FloatType(boolean isNullable) {
            super(isNullable, true, Code.FLOAT);
        }

        @Override
        @Nonnull
        public DataType withNullable(boolean isNullable) {
            if (isNullable) {
                return Primitives.FLOAT.type();
            } else {
                return Primitives.NULLABLE_FLOAT.type();
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
    }

    public static final class DoubleType extends DataType {
        private DoubleType(boolean isNullable) {
            super(isNullable, true, Code.DOUBLE);
        }

        @Override
        @Nonnull
        public DataType withNullable(boolean isNullable) {
            if (isNullable) {
                return Primitives.DOUBLE.type();
            } else {
                return Primitives.NULLABLE_DOUBLE.type();
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
    }

    public static final class StringType extends DataType {
        private StringType(boolean isNullable) {
            super(isNullable, true, Code.STRING);
        }

        @Override
        @Nonnull
        public DataType withNullable(boolean isNullable) {
            if (isNullable) {
                return Primitives.STRING.type();
            } else {
                return Primitives.NULLABLE_STRING.type();
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
    }

    public static final class BytesType extends DataType {
        private BytesType(boolean isNullable) {
            super(isNullable, true, Code.BYTES);
        }

        @Override
        @Nonnull
        public DataType withNullable(boolean isNullable) {
            if (isNullable) {
                return Primitives.BYTES.type();
            } else {
                return Primitives.NULLABLE_BYTES.type();
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
    }

    public static final class EnumType extends DataType implements Named {

        @Nonnull
        private final String name;

        @Nonnull
        private final List<EnumValue> values;

        public static class EnumValue {
            @Nonnull
            private final String name;

            private final int number;

            private EnumValue(@Nonnull String name, int number) {
                this.name = name;
                this.number = number;
            }

            @Nonnull
            public static EnumValue of(@Nonnull String name, int number) {
                return new EnumValue(name, number);
            }

            @Nonnull
            public String getName() {
                return name;
            }

            public int getNumber() {
                return number;
            }
        }

        @Nonnull

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
    }

    public static final class ArrayType extends DataType {

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
    }

    public static final class StructType extends DataType implements Named {

        @Nonnull
        private final List<Field> fields;

        @Nonnull
        private final String name;

        @Nonnull
        private final Supplier<Boolean> resolvedSupplier = Suppliers.memoize(this::calculateResolved);

        private StructType(String name, boolean isNullable, @Nonnull final List<Field> fields) {
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
            private final String name;

            private final DataType type;

            private int index;

            private Field(@Nonnull final String name, @Nonnull final DataType type, int index) {
                this.name = name;
                this.type = type;
                this.index = index;
            }

            @Nonnull
            public static Field from(@Nonnull final String name, @Nonnull final DataType type, int index) {
                return new Field(name, type, index);
            }

            public String getName() {
                return name;
            }

            public DataType getType() {
                return type;
            }

            public int getIndex() {
                return index;
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
    }

    /**
     * Represents an unknown type, i.e. a type which is not resolved yet, it only keeps reference information
     * about the type (to be resolved), and nullability information.
     * Unknown types can arise during parsing, in situation where a type is used before it is defined (late binding),
     * in these scenarios, we create an {@code UnknownType} and we resolve it later. (resolution in the sense of
     * effectively <i>replacing</i> it with a corresponding resolved type, since {@link DataType}s are immutable.
     * To see how this type is used as resolved, check build() method in RecordLayerSchemaTemplate.Builder.
     */
    public static final class UnknownType extends DataType implements Named {

        @Nonnull
        private final String name;

        private UnknownType(@Nonnull final String name, boolean isNullable) {
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
            return new UnknownType(name, isNullable);
        }

        @Nonnull
        public static UnknownType of(@Nonnull final String name, boolean isNullable) {
            return new UnknownType(name, isNullable);
        }

        @Override
        public boolean isResolved() {
            return false;
        }

        @Nonnull
        @Override
        public DataType resolve(@Nonnull Map<String, Named> resolutionMap) {
            Assert.thatUnchecked(resolutionMap.containsKey(name), String.format("Could not find type '%s'", name));
            return ((DataType) resolutionMap.get(name)).withNullable(isNullable());
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
        ENUM,
        STRUCT,
        ARRAY,
        UNKNOWN
    }

    @Nonnull
    public enum Primitives {
        BOOLEAN(new BooleanType(false)),
        LONG(new LongType(false)),
        INTEGER(new IntegerType(false)),
        FLOAT(new FloatType(false)),
        DOUBLE(new DoubleType(false)),
        STRING(new StringType(false)),
        BYTES(new BytesType(false)),
        NULLABLE_BOOLEAN(new BooleanType(true)),
        NULLABLE_LONG(new LongType(true)),
        NULLABLE_INTEGER(new IntegerType(true)),
        NULLABLE_FLOAT(new FloatType(true)),
        NULLABLE_DOUBLE(new DoubleType(true)),
        NULLABLE_STRING(new StringType(true)),
        NULLABLE_BYTES(new BytesType(true));

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
