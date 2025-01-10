/*
 * RelationalStructMetaData.java
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

import com.apple.foundationdb.relational.api.exceptions.InvalidColumnReferenceException;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

@API(API.Status.EXPERIMENTAL)
public class RelationalStructMetaData implements StructMetaData {

    @Nonnull
    private final String name;

    private final FieldDescription[] columns;
    //the number of phantom columns that are at the front of the metadata
    private final int leadingPhantomColumnOffset;
    private final Supplier<Integer> hashCodeSupplier;

    public RelationalStructMetaData(FieldDescription... columns) {
        this("ANONYMOUS_STRUCT", columns);
    }

    public RelationalStructMetaData(@Nonnull final String name, FieldDescription... columns) {
        this.name = name;
        this.columns = columns;
        this.leadingPhantomColumnOffset = countLeadingPhantomColumns();
        this.hashCodeSupplier = Suppliers.memoize(this::calculateHashCode);
    }

    @Override
    public String getTypeName() {
        return name;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columns.length - leadingPhantomColumnOffset;
    }

    @Override
    public int isNullable(int oneBasedColumn) throws SQLException {
        return getField(oneBasedColumn).isNullable();
    }

    @Override
    public String getColumnLabel(int oneBasedColumn) throws SQLException {
        return getColumnName(oneBasedColumn);
    }

    @Override
    public String getColumnName(int oneBasedColumn) throws SQLException {
        return getField(oneBasedColumn).getName();
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public String getSchemaName(int oneBasedColumn) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not yet implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public String getTableName(int oneBasedColumn) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not yet implemented");
    }

    @Override
    @ExcludeFromJacocoGeneratedReport
    public String getCatalogName(int oneBasedColumn) throws SQLException {
        throw new SQLFeatureNotSupportedException("Not yet implemented");
    }

    @Override
    public int getColumnType(int oneBasedColumn) throws SQLException {
        return getField(oneBasedColumn).getSqlTypeCode();
    }

    @Override
    public String getColumnTypeName(int oneBasedColumn) throws SQLException {
        return SqlTypeNamesSupport.getSqlTypeName(getColumnType(oneBasedColumn));
    }

    @Override
    public StructMetaData getStructMetaData(int oneBasedColumn) throws SQLException {
        FieldDescription field = getField(oneBasedColumn);
        if (field.isStruct()) {
            return field.getFieldMetaData();
        }
        throw new InvalidColumnReferenceException("Position <" + oneBasedColumn + "> is not a struct type").toSqlException();
    }

    @Override
    public ArrayMetaData getArrayMetaData(int oneBasedColumn) throws SQLException {
        FieldDescription field = getField(oneBasedColumn);
        if (field.isArray()) {
            return field.getArrayMetaData();
        }
        throw new InvalidColumnReferenceException("Position <" + oneBasedColumn + "> is not a struct type").toSqlException();
    }

    @Override
    public int getLeadingPhantomColumnCount() {
        return leadingPhantomColumnOffset;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return iface.cast(this);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isAssignableFrom(this.getClass());
    }

    @Nonnull
    public List<FieldDescription> getFields() {
        return ImmutableList.copyOf(columns);
    }

    @SuppressWarnings("PMD.PreserveStackTrace")
    private FieldDescription getField(int oneBasedColumn) throws SQLException {
        try {
            int adjustedColPosition = leadingPhantomColumnOffset + (oneBasedColumn - 1);
            return columns[adjustedColPosition];
        } catch (ArrayIndexOutOfBoundsException aie) {
            throw new InvalidColumnReferenceException("Position <" + oneBasedColumn + "> is not valid. Struct has " + getColumnCount() + " columns")
                    .toSqlException();
        }
    }

    private int countLeadingPhantomColumns() {
        /*
         * Adjust the one-based position to account for any leading Phantom columns
         */
        int count = columns.length;
        for (int i = 0; i < columns.length; i++) {
            if (!columns[i].isPhantom()) {
                count = i;
                break;
            }
        }
        return count;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof RelationalStructMetaData)) {
            return false;
        }
        final var otherMetadata = (RelationalStructMetaData) other;
        if (otherMetadata == this) {
            return true;
        }
        return leadingPhantomColumnOffset == otherMetadata.leadingPhantomColumnOffset &&
                Arrays.equals(columns, otherMetadata.columns);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int calculateHashCode() {
        return Objects.hash(Arrays.hashCode(columns), leadingPhantomColumnOffset);
    }
}
