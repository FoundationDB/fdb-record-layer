/*
 * RelationalArrayMetaData.java
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

package com.apple.foundationdb.relational.api;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.google.common.base.Suppliers;

import javax.annotation.Nonnull;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Metadata describing what is in a {@link RelationalArray}.
 */
@API(API.Status.EXPERIMENTAL)
public final class RelationalArrayMetaData implements ArrayMetaData {

    private final DataType.ArrayType type;

    private final Supplier<Integer> hashCodeSupplier;

    private RelationalArrayMetaData(@Nonnull DataType.ArrayType type) {
        this.type = type;
        this.hashCodeSupplier = Suppliers.memoize(this::calculateHashCode);
    }

    @Nonnull
    public static RelationalArrayMetaData of(@Nonnull DataType.ArrayType type) {
        return new RelationalArrayMetaData(type);
    }

    @Override
    public int isElementNullable() {
        if (type.getElementType().isNullable()) {
            return DatabaseMetaData.columnNullable;
        } else {
            return DatabaseMetaData.columnNoNulls;
        }
    }

    @Override
    public String getElementName() throws SQLException {
        return "VALUE";
    }

    @Override
    public int getElementType() throws SQLException {
        return type.getElementType().getJdbcSqlCode();
    }

    @Override
    public String getElementTypeName() {
        return SqlTypeNamesSupport.getSqlTypeName(type.getElementType().getJdbcSqlCode());
    }

    /**
     * Get the Metadata for a nested struct type.
     *
     * If the column is not a struct type, this will throw an error.
     *
     * @return the metadata for the struct at column {@code oneBasedColumn}
     * @throws SQLException if the type of the column is not a struct, or if something else goes wrong.
     */
    @Override
    public StructMetaData getElementStructMetaData() throws SQLException {
        if (type.getElementType().getCode() != DataType.Code.STRUCT) {
            throw new RelationalException("Element is not of STRUCT type", ErrorCode.CANNOT_CONVERT_TYPE).toSqlException();
        }
        return RelationalStructMetaData.of((DataType.StructType) type.getElementType());
    }

    /**
     * Get the Metadata for an array type.
     *
     * If the column is not an array type, this will throw an error.
     *
     * @return the metadata for the array at column {@code oneBasedColumn}
     * @throws SQLException if the type of the column is not an array, or if something else goes wrong.
     */
    @Override
    public ArrayMetaData getElementArrayMetaData() throws SQLException {
        if (type.getElementType().getCode() != DataType.Code.ARRAY) {
            throw new RelationalException("Element is not of ARRAY type", ErrorCode.CANNOT_CONVERT_TYPE).toSqlException();
        }
        return RelationalArrayMetaData.of((DataType.ArrayType) type.getElementType());
    }

    @Nonnull
    @Override
    public DataType.ArrayType asRelationalType() throws SQLException {
        return type;
    }

    @Nonnull
    public DataType getElementDataType() {
        return type.getElementType();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return iface.cast(this);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isAssignableFrom(this.getClass());
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof RelationalArrayMetaData)) {
            return false;
        }
        final var otherMetadata = (RelationalArrayMetaData) other;
        if (otherMetadata == this) {
            return true;
        }
        return type.equals(otherMetadata.type);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int calculateHashCode() {
        return Objects.hash(type);
    }

}
