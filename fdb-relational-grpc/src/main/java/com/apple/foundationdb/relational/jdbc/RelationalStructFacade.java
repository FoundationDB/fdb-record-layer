/*
 * RelationalStructFacade.java
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

package com.apple.foundationdb.relational.jdbc;

import com.apple.foundationdb.relational.api.ArrayMetaData;
import com.apple.foundationdb.relational.api.RelationalArray;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.RelationalStructBuilder;
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.Column;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.ColumnMetadata;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.EnumMetadata;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.ListColumn;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.ListColumnMetadata;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.Struct;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.Type;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.Uuid;
import com.apple.foundationdb.relational.util.PositionalIndex;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.protobuf.ByteString;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Facade over grpc protobuf objects that offers a {@link RelationalStruct} view.
 * Used by jdbc client but also serializable (protobuf) so can be passed over
 * grpc and used server-side doing direct access api inserts.
 * Package-private. Used internally. Not for general consumption.
 * Use {@link #newBuilder()} building instances.
 */
// Metadata is one protobuf and the Struct data is another protobuf. This is what works best for the moment.
// A single protobuf of metadata and data for the RelationalStruct would be awkward given we create instances in
// ResultSet where metadata and data are separate and inserting RelationalStruct, we want to have one metadata
// instance only no matter how many Struct instances.
class RelationalStructFacade implements RelationalStruct {
    /**
     * A StructMetaData facade over {@link #delegateMetadata}.
     */
    private final StructMetaData structMetaData;
    private final Supplier<DataType.StructType> type;

    /**
     * Column metadata for this Struct.
     * Package-private so protobuf is available to serializer (in same package).
     */
    private final ListColumnMetadata delegateMetadata;

    /**
     * Struct data as protobuf.

     */
    private final Struct delegate;

    private boolean wasNull;

    RelationalStructFacade(ListColumnMetadata delegateMetadata, Struct delegate) {
        type = Suppliers.memoize(() -> TypeConversion.getStructDataType(delegateMetadata.getColumnMetadataList(), false));
        this.delegate = delegate;
        this.delegateMetadata = delegateMetadata;
        this.structMetaData = new RelationalStructFacadeMetaData(delegateMetadata);
        this.wasNull = false;
    }

    /**
     * Package-private so protobuf is available to serializer (in same package).
     * @return The backing protobuf used to keep Struct data.
     */
    Struct getDelegate() {
        return delegate;
    }

    /**
     * Package-private so protobuf is available to serializer (in same package).
     * @return The backing protobuf used to keep Struct metadata.
     */
    ListColumnMetadata getDelegateMetadata() {
        return delegateMetadata;
    }

    /**
     * Facade over protobuf metadata to provide a {@link StructMetaData} view.
     */
    @Override
    public StructMetaData getMetaData() {
        return type.get() != null ? RelationalStructMetaData.of(type.get()) : structMetaData;
    }

    @Override
    public boolean getBoolean(int oneBasedColumn) throws SQLException {
        Column c = getColumnInternal(oneBasedColumn);
        if (wasNull) {
            return false;
        }
        if (!(c.hasBoolean())) {
            throw new SQLException("Boolean", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }

        return c.getBoolean();
    }

    @Override
    public boolean getBoolean(String fieldName) throws SQLException {
        return getBoolean(RelationalStruct.getOneBasedPosition(fieldName, this));
    }

    @Override
    public int getInt(int oneBasedColumn) throws SQLException {
        Column c = getColumnInternal(oneBasedColumn);
        if (wasNull) {
            return 0;
        }
        if (!(c.hasInteger())) {
            throw new SQLException("Integer", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }

        return c.getInteger();
    }

    @Override
    public int getInt(String fieldName) throws SQLException {
        return getInt(RelationalStruct.getOneBasedPosition(fieldName, this));
    }

    @Override
    public long getLong(int oneBasedColumn) throws SQLException {
        Column c = getColumnInternal(oneBasedColumn);
        if (wasNull) {
            return 0;
        }
        if (!(c.hasLong())) {
            throw new SQLException("Long", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }

        return c.getLong();
    }

    @Override
    public long getLong(String fieldName) throws SQLException {
        return getLong(RelationalStruct.getOneBasedPosition(fieldName, this));
    }

    @Override
    public float getFloat(int oneBasedColumn) throws SQLException {
        throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public float getFloat(String fieldName) throws SQLException {
        throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public double getDouble(int oneBasedColumn) throws SQLException {
        Column c = getColumnInternal(oneBasedColumn);
        if (wasNull) {
            return 0;
        }
        if (!(c.hasDouble())) {
            throw new SQLException("Double", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }

        return c.getDouble();
    }

    @Override
    public double getDouble(String fieldName) throws SQLException {
        return getDouble(RelationalStruct.getOneBasedPosition(fieldName, this));
    }

    @Override
    public byte[] getBytes(int oneBasedColumn) throws SQLException {
        Column c = getColumnInternal(oneBasedColumn);
        if (wasNull) {
            return null;
        }
        if (!(c.hasBinary())) {
            throw new SQLException("Binary", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }

        return c.getBinary().toByteArray();
    }

    @Override
    public byte[] getBytes(String fieldName) throws SQLException {
        return getBytes(RelationalStruct.getOneBasedPosition(fieldName, this));
    }

    @Override
    public String getString(int oneBasedColumn) throws SQLException {
        Column c = getColumnInternal(oneBasedColumn);
        if (wasNull) {
            return null;
        }
        if (!(c.hasString())) {
            throw new SQLException("String", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }

        return c.getString();
    }

    @Override
    public String getString(String fieldName) throws SQLException {
        return getString(RelationalStruct.getOneBasedPosition(fieldName, this));
    }

    @Override
    public Object getObject(int oneBasedColumn) throws SQLException {
        int type = getMetaData().getColumnType(oneBasedColumn);
        Object obj = null;
        switch (type) {
            case Types.ARRAY:
                obj = getArray(oneBasedColumn);
                break;
            case Types.BIGINT:
                obj = getLong(oneBasedColumn);
                break;
            case Types.BINARY:
                obj = getBytes(oneBasedColumn);
                break;
            case Types.VARCHAR:
                obj = getString(oneBasedColumn);
                break;
            case Types.STRUCT:
                obj = getStruct(oneBasedColumn);
                break;
            case Types.DOUBLE:
                obj = getDouble(oneBasedColumn);
                break;
            case Types.BOOLEAN:
                obj = getBoolean(oneBasedColumn);
                break;
            case Types.INTEGER:
                obj = getInt(oneBasedColumn);
                break;
            case Types.OTHER:
                obj = getUUID(oneBasedColumn);
                break;
            default:
                throw new SQLException("Unsupported object type: " + type);
        }
        if (wasNull()) {
            return null;
        }
        return obj;
    }

    @Override
    public Object getObject(String fieldName) throws SQLException {
        return getObject(RelationalStruct.getOneBasedPosition(fieldName, this));
    }

    @Override
    public RelationalStruct getStruct(int oneBasedColumn) throws SQLException {
        Column c = getColumnInternal(oneBasedColumn);
        if (wasNull) {
            return null;
        }
        if (!(c.hasStruct())) {
            throw new SQLException("Struct", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }

        return new RelationalStructFacade(this.delegateMetadata.getColumnMetadata(PositionalIndex.toProtobuf(oneBasedColumn)).getStructMetadata(), c.getStruct());
    }

    @Override
    public RelationalStruct getStruct(String fieldName) throws SQLException {
        throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public RelationalArray getArray(int oneBasedColumn) throws SQLException {
        Column c = getColumnInternal(oneBasedColumn);
        if (wasNull) {
            return null;
        }
        if (!(c.hasArray())) {
            throw new SQLException("Array", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }
        return new RelationalArrayFacade(this.delegateMetadata.getColumnMetadata(PositionalIndex.toProtobuf(oneBasedColumn)).getArrayMetadata(), c.getArray());
    }

    @Override
    public RelationalArray getArray(String fieldName) throws SQLException {
        throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public UUID getUUID(final int oneBasedPosition) throws SQLException {
        Column c = getColumnInternal(oneBasedPosition);
        if (wasNull) {
            return null;
        }
        if (!(c.hasUuid())) {
            throw new SQLException("UUID", ErrorCode.CANNOT_CONVERT_TYPE.getErrorCode());
        }
        return new UUID(c.getUuid().getMostSignificantBits(), c.getUuid().getLeastSignificantBits());
    }

    @Override
    public UUID getUUID(final String fieldName) throws SQLException {
        return getUUID(RelationalStruct.getOneBasedPosition(fieldName, this));
    }

    @Override
    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    private Column getColumnInternal(int oneBasedColumn) {
        Column c = this.delegate.getColumns().getColumn(PositionalIndex.toProtobuf(oneBasedColumn));
        wasNull = c.hasNullType() || c.getKindCase().equals(Column.KindCase.KIND_NOT_SET);
        return c;
    }

    static RelationalStructBuilder newBuilder() {
        return new RelationalStructFacadeBuilder();
    }

    /**
     * Builder for a {@link RelationalStruct} view over GRPC Protobuf objects.
     * Not thread-safe: presumes single-threaded access.
     */
    @VisibleForTesting
    static final class RelationalStructFacadeBuilder implements RelationalStructBuilder {
        /**
         * Builder to hold columns during construction.
         */
        private final ListColumn.Builder listColumnBuilder = ListColumn.newBuilder();

        /**
         * Fields kept in the order-of-insert.
         * Used ensuring we maintain insert-order.
         */
        // TODO: Use LinkedHashMap instead of Map and List as in the below! Stack 01/31/2023.
        private final List<String> fieldOrder = new ArrayList<>();
        /**
         * Metadata accumulated so far.
         */
        private final Map<String, ColumnMetadata> metadata = new HashMap<>();

        RelationalStructFacadeBuilder() {
        }

        /**
         * Update metadata.
         * Save the metadata built for the field we are about to insert so it is available when we go to build a
         * struct instance. Also return offset at where to insert field data.
         * Checks for overwrite: if overwrite, returns original insert offset; else return end-of-the-list offset to
         * insert at.
         * @param columnMetadata Metadata for the column data we are about to insert.
         * @return Zero-based offset at where to add <code>fieldName</code>.
         */
        @VisibleForTesting
        int addMetadata(ColumnMetadata columnMetadata) {
            int offset = getZeroBasedOffset(columnMetadata.getName());
            if (offset == -1) {
                // New field.
                offset = this.metadata.size();
                this.fieldOrder.add(columnMetadata.getName());
            }
            this.metadata.put(columnMetadata.getName(), columnMetadata);
            return offset;
        }

        /**
         * Returns field offset or -1 if field not (yet) present.
         * @param fieldName Name of field whose presence we are to check for.
         * @return Zero-based offset at where <code>fieldName</code> was inserted else -1 if unknown field.
         */
        @VisibleForTesting
        int getZeroBasedOffset(String fieldName) {
            int offset = -1;
            if (this.metadata.containsKey(fieldName)) {
                for (int i = 0; i < this.fieldOrder.size(); i++) {
                    if (this.fieldOrder.get(i).equals(fieldName)) {
                        return offset;
                    }
                }
            }
            return offset;
        }

        /**
         * Like {@link #getZeroBasedOffset(String)} only it throws an exception if field not found (rather than just
         * return -1).
         * @param fieldName Name of field whose presence we are to check for.
         * @return Zero-based offset at where <code>fieldName</code> was inserted else throws exception.
         * @throws SQLException Thrown if <code>fieldName</code> unknown.
         */
        @VisibleForTesting
        int getZeroBasedOffsetOrThrow(String fieldName) throws SQLException {
            int offset = getZeroBasedOffset(fieldName);
            if (offset == -1) {
                throw new SQLException("Unknown " + fieldName);
            }
            return offset;
        }

        @Override
        public RelationalStructBuilder addBoolean(String fieldName, boolean b) throws SQLException {
            // Add the metadata and get offset at where to insert data.
            int offset = addMetadata(ColumnMetadata.newBuilder()
                    .setName(fieldName).setJavaSqlTypesCode(Types.BOOLEAN).setType(Type.BOOLEAN).build());
            // Add field data.
            this.listColumnBuilder.addColumn(offset, Column.newBuilder().setBoolean(b).build());
            return this;
        }

        @Override
        public RelationalStructBuilder addLong(String fieldName, long l) throws SQLException {
            int offset = addMetadata(ColumnMetadata.newBuilder()
                    .setName(fieldName).setJavaSqlTypesCode(Types.BIGINT).setType(Type.LONG).build());
            this.listColumnBuilder.addColumn(offset, Column.newBuilder().setLong(l).build());
            return this;
        }

        @Override
        public RelationalStructBuilder addFloat(String fieldName, float f) throws SQLException {
            throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
        }

        @Override
        public RelationalStructBuilder addDouble(String fieldName, double d) throws SQLException {
            int offset = addMetadata(ColumnMetadata.newBuilder()
                    .setName(fieldName).setJavaSqlTypesCode(Types.DOUBLE).setType(Type.DOUBLE).build());
            this.listColumnBuilder.addColumn(offset, Column.newBuilder().setDouble(d).build());
            return this;
        }

        @Override
        public RelationalStructBuilder addBytes(String fieldName, byte[] bytes) throws SQLException {
            int offset = addMetadata(ColumnMetadata.newBuilder()
                    .setName(fieldName).setJavaSqlTypesCode(Types.BINARY).setType(Type.BYTES).build());
            this.listColumnBuilder.addColumn(offset, Column.newBuilder().setBinary(ByteString.copyFrom(bytes)).build());
            return this;
        }

        @Override
        @SpotBugsSuppressWarnings("NP")
        public RelationalStructBuilder addString(String fieldName, @Nullable String s) throws SQLException {
            int offset = addMetadata(ColumnMetadata.newBuilder()
                    .setName(fieldName).setJavaSqlTypesCode(Types.VARCHAR).setType(Type.STRING).build());
            // TODO: setString requires a non-null string, but this method takes a nullable string
            this.listColumnBuilder.addColumn(offset, Column.newBuilder().setString(s).build());
            return this;
        }

        @Override
        public RelationalStructBuilder addUuid(final String fieldName, @Nullable final UUID uuid) {
            int offset = addMetadata(ColumnMetadata.newBuilder()
                    .setName(fieldName).setJavaSqlTypesCode(Types.OTHER).setType(Type.UUID).build());
            if (uuid == null) {
                this.listColumnBuilder.addColumn(offset, Column.newBuilder().build());
            } else {
                final var uuidColumn = Uuid.newBuilder().setMostSignificantBits(uuid.getMostSignificantBits()).setLeastSignificantBits(uuid.getLeastSignificantBits()).build();
                this.listColumnBuilder.addColumn(offset, Column.newBuilder().setUuid(uuidColumn).build());
            }
            return this;
        }

        @Override
        public RelationalStructBuilder addObject(final String fieldName, @Nullable final Object obj) throws SQLException {
            throw new SQLException("Not implemented " + Thread.currentThread() .getStackTrace()[1] .getMethodName());
        }

        @Override
        public RelationalStructBuilder addStruct(String fieldName, @Nonnull RelationalStruct struct) throws SQLException {
            // We're building. Must be a RelationalStructFacade instance of RelationalStruct when here. Unwrap.
            // This will allow us to access the backing data and metadata protobufs.
            // Insert the data portion of RelationalStruct here.
            RelationalStructFacade relationalStructFacade = struct.unwrap(RelationalStructFacade.class);
            int offset = addMetadata(ColumnMetadata.newBuilder().setName(fieldName)
                    .setJavaSqlTypesCode(Types.STRUCT).setType(Type.STRUCT).setStructMetadata(relationalStructFacade.delegateMetadata).build());
            this.listColumnBuilder
                    .addColumn(offset, Column.newBuilder().setStruct(relationalStructFacade.delegate).build());
            return this;
        }

        @Override
        public RelationalStructBuilder addArray(String fieldName, @Nonnull RelationalArray array) throws SQLException {
            // We're building. Must be a RelationalArrayFacade instance of RelationalArray when here. Unwrap.
            // This will allow us to access the backing data and metadata protobufs.
            // Insert the data portion of RelationalStruct here.
            RelationalArrayFacade relationalArrayFacade = array.unwrap(RelationalArrayFacade.class);
            int offset = addMetadata(ColumnMetadata.newBuilder()
                    .setName(fieldName).setJavaSqlTypesCode(Types.ARRAY).setType(Type.ARRAY)
                    .setArrayMetadata(relationalArrayFacade.getDelegateMetadata())
                    .build());
            this.listColumnBuilder.addColumn(offset,
                    Column.newBuilder().setArray(relationalArrayFacade.getDelegate()).build());
            return this;
        }

        @Override
        public RelationalStructBuilder addInt(String fieldName, int i) throws SQLException {
            int offset = addMetadata(ColumnMetadata.newBuilder()
                    .setName(fieldName).setType(Type.INTEGER).build());
            this.listColumnBuilder.addColumn(offset, Column.newBuilder().setInteger(i).build());
            return this;
        }

        @Override
        public RelationalStruct build() {
            var columnListMetadataBuilder = ListColumnMetadata.newBuilder();
            this.fieldOrder.forEach(s -> columnListMetadataBuilder.addColumnMetadata(this.metadata.get(s)));
            var struct = Struct.newBuilder().setColumns(this.listColumnBuilder.build()).build();
            return new RelationalStructFacade(columnListMetadataBuilder.build(), struct);
        }
    }

    /**
     * Facade over protobuf column metadata to present a StructMetaData view.
     */
    static final class RelationalStructFacadeMetaData implements StructMetaData {
        private final ListColumnMetadata metadata;
        private final Supplier<DataType.StructType> type;

        RelationalStructFacadeMetaData(ListColumnMetadata metadata) {
            this.metadata = metadata;
            this.type = Suppliers.memoize(this::computeType);
        }

        private DataType.StructType computeType() {
            return getStructDataType(metadata.getColumnMetadataList(), false);
        }

        private static DataType.EnumType getEnumDataType(@Nonnull EnumMetadata enumMetadata, boolean nullable) {
            final var enumValues = new ArrayList<DataType.EnumType.EnumValue>();
            int i = 1;
            for (var value: enumMetadata.getValuesList()) {
                enumValues.add(DataType.EnumType.EnumValue.of(value, i++));
            }
            return DataType.EnumType.from(enumMetadata.getName(), enumValues, nullable);
        }

        private static DataType.StructType getStructDataType(@Nonnull List<ColumnMetadata> columnMetadataList, boolean nullable) {
            final var fields = new ArrayList<DataType.StructType.Field>();
            for (int i = 0; i < columnMetadataList.size(); i++) {
                final var columnMetadata = columnMetadataList.get(i);
                fields.add(DataType.StructType.Field.from(columnMetadata.getName(), getDataType(columnMetadata.getType(), columnMetadata, columnMetadata.getNullable()), i));
            }
            return DataType.StructType.from("ANONYMOUS_STRUCT", fields, nullable);
        }

        static DataType getDataType(@Nonnull Type type, @Nonnull ColumnMetadata columnMetadata, boolean nullable) {
            switch (type) {
                case LONG:
                    return nullable ? DataType.Primitives.NULLABLE_LONG.type() : DataType.Primitives.LONG.type();
                case INTEGER:
                    return nullable ? DataType.Primitives.NULLABLE_INTEGER.type() : DataType.Primitives.INTEGER.type();
                case DOUBLE:
                    return nullable ? DataType.Primitives.NULLABLE_DOUBLE.type() : DataType.Primitives.DOUBLE.type();
                case FLOAT:
                    return nullable ? DataType.Primitives.NULLABLE_FLOAT.type() : DataType.Primitives.FLOAT.type();
                case BOOLEAN:
                    return nullable ? DataType.Primitives.NULLABLE_BOOLEAN.type() : DataType.Primitives.BOOLEAN.type();
                case BYTES:
                    return nullable ? DataType.Primitives.NULLABLE_BYTES.type() : DataType.Primitives.BYTES.type();
                case UUID:
                    return nullable ? DataType.Primitives.NULLABLE_UUID.type() : DataType.Primitives.UUID.type();
                case STRING:
                    return nullable ? DataType.Primitives.NULLABLE_STRING.type() : DataType.Primitives.STRING.type();
                case VERSION:
                    return nullable ? DataType.Primitives.NULLABLE_VERSION.type() : DataType.Primitives.VERSION.type();
                case STRUCT:
                    return getStructDataType(columnMetadata.getStructMetadata().getColumnMetadataList(), nullable);
                case ENUM:
                    return getEnumDataType(columnMetadata.getEnumMetadata(), nullable);
                case ARRAY:
                    final var arrayMetadata = columnMetadata.getArrayMetadata();
                    return DataType.ArrayType.from(getDataType(arrayMetadata.getType(), arrayMetadata, arrayMetadata.getNullable()), nullable);
                default:
                    throw new RelationalException("Not implemeneted: " + type.name(), ErrorCode.INTERNAL_ERROR).toUncheckedWrappedException();
            }
        }

        @Override
        public int getColumnCount() throws SQLException {
            return metadata.getColumnMetadataCount();
        }

        @Override
        public int isNullable(int oneBasedColumn) throws SQLException {
            throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }

        @Override
        public String getTypeName() throws SQLException {
            throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }

        @Override
        public String getColumnLabel(int oneBasedColumn) throws SQLException {
            throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }

        @Override
        public String getColumnName(int oneBasedColumn) throws SQLException {
            return metadata.getColumnMetadata(PositionalIndex.toProtobuf(oneBasedColumn)).getName();
        }

        @Override
        public String getSchemaName(int oneBasedColumn) throws SQLException {
            throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }

        @Override
        public String getTableName(int oneBasedColumn) throws SQLException {
            throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }

        @Override
        public String getCatalogName(int oneBasedColumn) throws SQLException {
            throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }

        @Override
        public int getColumnType(int oneBasedColumn) throws SQLException {
            return metadata.getColumnMetadata(PositionalIndex.toProtobuf(oneBasedColumn)).getJavaSqlTypesCode();
        }

        @Override
        public String getColumnTypeName(int oneBasedColumn) throws SQLException {
            throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }

        @Override
        public StructMetaData getStructMetaData(int oneBasedColumn) throws SQLException {
            throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }

        @Override
        public ArrayMetaData getArrayMetaData(int oneBasedColumn) throws SQLException {
            throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }

        @Override
        public int getLeadingPhantomColumnCount() {
            return -1000;
        }

        @Nonnull
        @Override
        public DataType.StructType getRelationalDataType() {
            return type.get();
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) throws SQLException {
            throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
        }
    }
}
