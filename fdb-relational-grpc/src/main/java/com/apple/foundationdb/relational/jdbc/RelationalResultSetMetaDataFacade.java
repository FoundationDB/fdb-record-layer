/*
 * RelationalResultSetMetaDataFacade.java
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
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.RelationalResultSetMetaData;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.jdbc.grpc.v1.ResultSetMetadata;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;
import com.apple.foundationdb.relational.util.PositionalIndex;

import javax.annotation.Nonnull;
import java.sql.JDBCType;
import java.sql.SQLException;

/**
 * Facade over grpc protobuf objects that offers a {@link com.apple.foundationdb.relational.api.RelationalResultSetMetaData} view.
 */
class RelationalResultSetMetaDataFacade implements RelationalResultSetMetaData  {
    private final ResultSetMetadata delegate;

    RelationalResultSetMetaDataFacade(ResultSetMetadata delegate) {
        this.delegate = delegate;
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public ArrayMetaData getArrayMetaData(int oneBasedColumn) throws SQLException {
        throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Nonnull
    @Override
    public DataType.StructType getRelationalDataType() throws SQLException {
        throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public StructMetaData getStructMetaData(int oneBasedColumn) {
        return RelationalStructMetaData.of(TypeConversion.getStructDataType(
                this.delegate.getColumnMetadata().getColumnMetadata(
                        PositionalIndex.toProtobuf(oneBasedColumn)).getStructMetadata().getColumnMetadataList(), false));
    }

    @Override
    public String getTypeName() throws SQLException {
        throw new SQLException("Not implemented", ErrorCode.UNSUPPORTED_OPERATION.getErrorCode());
    }

    @Override
    public int getColumnCount() throws SQLException {
        return this.delegate.getColumnMetadata().getColumnMetadataCount();
    }

    @Override
    public String getColumnName(int oneBasedColumn) throws SQLException {
        return this.delegate.getColumnMetadata()
                .getColumnMetadata(PositionalIndex.toProtobuf(oneBasedColumn)).getName();
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        // TODO: For now, return name.
        return getColumnName(column);
    }

    @Override
    public int getColumnType(int oneBasedColumn) throws SQLException {
        return TypeConversion.toSqlType(this.delegate.getColumnMetadata()
                .getColumnMetadata(PositionalIndex.toProtobuf(oneBasedColumn)).getType());
    }

    @Override
    public String getColumnTypeName(int oneBasedColumn) throws SQLException {
        int code = getColumnType(oneBasedColumn);
        return JDBCType.valueOf(code).getName();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return iface.cast(this);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this) || iface.isAssignableFrom(StructMetaData.class);
    }
}
