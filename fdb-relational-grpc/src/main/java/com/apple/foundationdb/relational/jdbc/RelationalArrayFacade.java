/*
 * RelationalArrayFacade.java
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
import com.apple.foundationdb.relational.api.RelationalArrayBuilder;
import com.apple.foundationdb.relational.api.RelationalArrayMetaData;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.SqlTypeNamesSupport;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.jdbc.grpc.v1.ResultSet;
import com.apple.foundationdb.relational.jdbc.grpc.v1.ResultSetMetadata;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.Array;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.Column;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.ColumnMetadata;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.ListColumn;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.ListColumnMetadata;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.Struct;
import com.apple.foundationdb.relational.jdbc.grpc.v1.column.Type;
import com.apple.foundationdb.relational.util.Assert;
import com.apple.foundationdb.relational.util.PositionalIndex;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.UUID;

/**
 * Facade over grpc protobuf objects that offers a {@link RelationalArray} view.
 * Used by jdbc client but also serializable (protobuf) so can be passed over
 * grpc and used server-side doing direct access api inserts.
 * Package-private. Used internally. Not for general consumption.
 * Use {@link #newBuilder()} building instances.
 */
class RelationalArrayFacade implements RelationalArray {
    /**
     * Column metadata for the Array.
     * Package-private so protobuf is available to serializer (in same package).
     */
    private final ColumnMetadata delegateMetadata;
    private final Supplier<DataType.ArrayType> type;

    /**
     * Array data as protobuf.
     * Array of Struct data.
     * Package-private so protobuf is available to serializer (in same package).
     */
    private final Array delegate;

    RelationalArrayFacade(@Nonnull ColumnMetadata delegateMetadata, Array array) {
        this.delegateMetadata = delegateMetadata;
        this.type = Suppliers.memoize(this::computeType);
        this.delegate = array;
    }

    @Nullable
    private DataType.ArrayType computeType() {
        return delegateMetadata.getType() == Type.UNKNOWN ? null :
               DataType.ArrayType.from(RelationalStructFacade.RelationalStructFacadeMetaData.getDataType(delegateMetadata.getType(), delegateMetadata, delegateMetadata.getNullable()));
    }

    /**
     * Package-private so protobuf is available to serializer (in same package).
     * @return The backing protobuf used to keep Array data.
     */
    Array getDelegate() {
        return delegate;
    }

    /**
     * Package-private so protobuf is available to serializer (in same package).
     * @return The backing protobuf used to keep Array metadata.
     */
    ColumnMetadata getDelegateMetadata() {
        return delegateMetadata;
    }

    @Override
    public ArrayMetaData getMetaData() throws SQLException {
        if (type.get() != null) {
            return RelationalArrayMetaData.of(type.get());
        }
        throw new SQLFeatureNotSupportedException("get Metadata not supported in JDBC Relational Arrays");
    }

    @Override
    public String getBaseTypeName() throws SQLException {
        // Copied from com.apple.foundationdb.relational.apiRowArray.
        return SqlTypeNamesSupport.getSqlTypeName(getBaseType());
    }

    @Override
    public int getBaseType() {
        // Copied from com.apple.foundationdb.relational.apiRowArray.
        /*
         * Confusion alert!
         *
         * The JDBC API for array uses this method to refer to "the JDBC type of the elements in the array
         * designated by this Array object."--i.e. this is the Sql Type for the contents of the array.
         * In Relational, the contents of an Array is _always_ a Struct (from the JDBC API's point view), mostly
         * because it makes it much easier for us to implement the nested ResultSet implementation.
         */
        return Types.STRUCT;
    }

    // Javadoc copied from com.apple.foundationdb.relational.apiRowArray.
    /**
     * Retrieves a slice of the SQL <code>ARRAY</code>
     * value designated by this <code>Array</code> object, beginning with the
     * specified <code>index</code> and containing up to <code>count</code>
     * successive elements of the SQL array.  This method uses the type map
     * associated with the connection for customizations of the type mappings.
     * <p>
     * Note: In Relational, this method will always return an array of Struct types. This is
     * _technically_ in violation of the SQL spec, but it allows for a user experience which
     * is more consistent w.r.t how metadata is available and handled.
     * @param oneBasedIndex index the array index of the first element to retrieve;
     *              the first element is at index 1
     * @param askedForCount the number of successive SQL array elements to retrieve
     * @return an array containing up to <code>count</code> consecutive elements
     * of the SQL array, beginning with element <code>index</code>
     * @exception SQLException if an error occurs while attempting to
     * access the array
     */
    @Override
    public Object getArray(long oneBasedIndex, int askedForCount) throws SQLException {
        int index = PositionalIndex.toProtobuf(Math.toIntExact(oneBasedIndex));
        int count = getCount(askedForCount, this.delegate.getElementCount(), index);
        final var array = new Object[count];
        int j = 0;
        final var componentType = this.delegateMetadata.getType();
        for (int i = index; i < count; i++) {
            if (componentType == Type.STRUCT) {
                array[j++] = new RelationalStructFacade(delegateMetadata.getStructMetadata(), delegate.getElement(i).getStruct());
            } else {
                Assert.failUnchecked(ErrorCode.UNKNOWN_TYPE, "Type not supported: " + componentType.name());
            }
        }
        return array;
    }

    /**
     * Actionable record count.
     * @param askedForCount Could be reasonable or it could be MAX_INT.
     * @param elementCount How many actual elements in backing array.
     * @param zeroBasedOffset Where to start counting from.
     * @return Count to use returning records.
     */
    private static int getCount(int askedForCount, int elementCount, int zeroBasedOffset) {
        // Protect against count being MAX_INT.
        return Math.min(askedForCount, elementCount - zeroBasedOffset);
    }

    @Override
    public RelationalResultSet getResultSet(long oneBasedIndex, int askedForCount) throws SQLException {
        int index = PositionalIndex.toProtobuf(Math.toIntExact(oneBasedIndex));
        int count = getCount(askedForCount, this.delegate.getElementCount(), index);
        var resultSetBuilder = ResultSet.newBuilder();

        final var componentType = this.delegateMetadata.getType();
        final var componentColumnBuilder = ColumnMetadata.newBuilder().setName("VALUE").setType(componentType);
        if (componentType == Type.ARRAY) {
            componentColumnBuilder.setArrayMetadata(this.delegateMetadata.getArrayMetadata());
        } else if (componentType == Type.STRUCT) {
            componentColumnBuilder.setStructMetadata(this.delegateMetadata.getStructMetadata());
        }
        resultSetBuilder.setMetadata(ResultSetMetadata.newBuilder().setColumnMetadata(ListColumnMetadata.newBuilder()
                .addColumnMetadata(ColumnMetadata.newBuilder().setName("INDEX").setType(Type.INTEGER).build())
                .addColumnMetadata(componentColumnBuilder.build()).build()).build());
        for (int i = index; i < count; i++) {
            final var listColumnBuilder = ListColumn.newBuilder();
            listColumnBuilder.addColumn(Column.newBuilder().setInteger(i + 1).build());
            final var valueColumnBuilder = Column.newBuilder();
            if (componentType == Type.STRUCT) {
                valueColumnBuilder.setStruct(delegate.getElement(i).getStruct());
            } else if (componentType == Type.INTEGER) {
                valueColumnBuilder.setInteger(delegate.getElement(i).getInteger());
            } else {
                Assert.failUnchecked(ErrorCode.UNKNOWN_TYPE, "Type not supported: " + componentType.name());
            }
            resultSetBuilder.addRow(Struct.newBuilder()
                    .setColumns(listColumnBuilder
                            .addColumn(valueColumnBuilder.build())).build());
        }
        return new RelationalResultSetFacade(resultSetBuilder.build());
    }

    static RelationalArrayBuilder newBuilder() {
        return new RelationalArrayFacadeBuilder();
    }

    static class RelationalArrayFacadeBuilder implements RelationalArrayBuilder {
        /**
         * Protobuf Struct Builder.
         * This class hides the backing protobuf.
         * TODO: See if order is respected! If we need the List and Set?
         */
        private final Array.Builder builder = Array.newBuilder();

        /**
         * Column metadata for the Array Structs.
         * Package-private so protobuf is available to serializer (in same package).
         */
        private ColumnMetadata metadata;

        RelationalArrayFacadeBuilder() {
        }

        @Override
        public RelationalArray build() {
            return new RelationalArrayFacade(this.metadata, this.builder.build());
        }

        @Override
        public RelationalArrayBuilder addAll(@Nonnull Object... value) throws SQLException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public RelationalArrayBuilder addBytes(@Nonnull byte[] value) throws SQLException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public RelationalArrayBuilder addString(@Nonnull String value) throws SQLException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public RelationalArrayBuilder addLong(@Nonnull long value) throws SQLException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public RelationalArrayBuilder addUuid(@Nonnull final UUID value) throws SQLException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public RelationalArrayBuilder addObject(@Nonnull final Object value) throws SQLException {
            throw new SQLFeatureNotSupportedException();
        }

        @Override
        public RelationalArrayBuilder addStruct(RelationalStruct struct) throws SQLException {
            final var structFacade = struct.unwrap(RelationalStructFacade.class);
            initOrCheckMetadata(structFacade.getDelegateMetadata());
            builder.addElement(Column.newBuilder().setStruct(structFacade.getDelegate()));
            return this;
        }

        private void initOrCheckMetadata(ListColumnMetadata innerMetadata) {
            if (metadata == null) {
                final var builder = ColumnMetadata.newBuilder().setName("ARRAY").setType(Type.STRUCT);
                builder.setStructMetadata(innerMetadata);
                metadata = builder.build();
            } else {
                Assert.thatUnchecked(metadata.getType() == Type.ARRAY, ErrorCode.DATATYPE_MISMATCH, "dataType mismatch!");
            }
        }
    }
}
