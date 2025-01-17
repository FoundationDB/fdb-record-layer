/*
 * RelationalArrayMetaData.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.google.common.base.Suppliers;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Metadata describing what is in a {@link RelationalArray}.
 */
@API(API.Status.EXPERIMENTAL)
public final class RelationalArrayMetaData implements ArrayMetaData {

    private final FieldDescription element;

    private final Supplier<Integer> hashCodeSupplier;

    private RelationalArrayMetaData(@Nonnull FieldDescription element) {
        this.element = element;
        this.hashCodeSupplier = Suppliers.memoize(this::calculateHashCode);
    }

    public static RelationalArrayMetaData ofPrimitive(int sqlType, int nullable) {
        return new RelationalArrayMetaData(FieldDescription.primitive("VALUE", sqlType, nullable));
    }

    public static RelationalArrayMetaData ofStruct(@Nonnull StructMetaData metaData, int nullable) {
        return new RelationalArrayMetaData(FieldDescription.struct("VALUE", nullable, metaData));
    }

    @Override
    public int isElementNullable() throws SQLException {
        return element.isNullable();
    }

    @Override
    public String getElementName() throws SQLException {
        return element.getName();
    }

    @Override
    public int getElementType() throws SQLException {
        return element.getSqlTypeCode();
    }

    @Override
    public String getElementTypeName() throws SQLException {
        return SqlTypeNamesSupport.getSqlTypeName(element.getSqlTypeCode());
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
        if (element.getSqlTypeCode() != Types.STRUCT) {
            throw new RelationalException("Element is not of STRUCT type", ErrorCode.CANNOT_CONVERT_TYPE).toSqlException();
        }
        return element.getFieldMetaData();
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
        if (element.getSqlTypeCode() != Types.ARRAY) {
            throw new RelationalException("Element is not of ARRAY type", ErrorCode.CANNOT_CONVERT_TYPE).toSqlException();
        }
        return element.getArrayMetaData();
    }

    @Nonnull
    public FieldDescription getElementField() {
        return element;
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
        return element.equals(otherMetadata.element);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int calculateHashCode() {
        return Objects.hash(element);
    }

}
