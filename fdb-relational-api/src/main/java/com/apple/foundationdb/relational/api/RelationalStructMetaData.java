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
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

@API(API.Status.EXPERIMENTAL)
public class RelationalStructMetaData implements StructMetaData {

    //TODO(bfines) eventually this should move into the Planner (or closer to there, anyway), but for now we will hold on to it here
    private static final Set<String> KNOWN_PHANTOM_COLUMNS = Set.of("__TYPE_KEY");

    @Nonnull
    private final DataType.StructType type;
    //the number of phantom columns that are at the front of the metadata
    private final int leadingPhantomColumnOffset;
    private final Supplier<Integer> hashCodeSupplier;

    private RelationalStructMetaData(@Nonnull DataType.StructType type) {
        this.type = type;
        this.leadingPhantomColumnOffset = countLeadingPhantomColumns();
        this.hashCodeSupplier = Suppliers.memoize(this::calculateHashCode);
    }

    @Nonnull
    public static RelationalStructMetaData of(@Nonnull DataType.StructType type) {
        return new RelationalStructMetaData(type);
    }

    @Override
    public String getTypeName() {
        return type.getName();
    }

    @Override
    public int getColumnCount() throws SQLException {
        return type.getFields().size() - leadingPhantomColumnOffset;
    }

    @Override
    public int isNullable(int oneBasedColumn) throws SQLException {
        if (getField(oneBasedColumn).getType().isNullable()) {
            return DatabaseMetaData.columnNullable;
        } else {
            return DatabaseMetaData.columnNoNulls;
        }
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
        return getField(oneBasedColumn).getType().getJdbcSqlCode();
    }

    @Override
    public String getColumnTypeName(int oneBasedColumn) throws SQLException {
        return SqlTypeNamesSupport.getSqlTypeName(getColumnType(oneBasedColumn));
    }

    @Override
    public StructMetaData getStructMetaData(int oneBasedColumn) throws SQLException {
        final DataType type = getField(oneBasedColumn).getType();
        if (type.getCode() == DataType.Code.STRUCT) {
            return RelationalStructMetaData.of((DataType.StructType) type);
        }
        throw new InvalidColumnReferenceException("Position <" + oneBasedColumn + "> is not a struct type").toSqlException();
    }

    @Override
    public ArrayMetaData getArrayMetaData(int oneBasedColumn) throws SQLException {
        final DataType type = getField(oneBasedColumn).getType();
        if (type.getCode() == DataType.Code.ARRAY) {
            return RelationalArrayMetaData.of((DataType.ArrayType) type);
        }
        throw new InvalidColumnReferenceException("Position <" + oneBasedColumn + "> is not an array type").toSqlException();
    }

    @Override
    public int getLeadingPhantomColumnCount() {
        return leadingPhantomColumnOffset;
    }

    @Nonnull
    @Override
    public DataType.StructType getRelationalDataType() throws SQLException {
        return type;
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
    private List<DataType.StructType.Field> getFields() {
        return ImmutableList.copyOf(type.getFields());
    }

    @SuppressWarnings("PMD.PreserveStackTrace")
    private DataType.StructType.Field getField(int oneBasedColumn) throws SQLException {
        try {
            int adjustedColPosition = leadingPhantomColumnOffset + (oneBasedColumn - 1);
            return type.getFields().get(adjustedColPosition);
        } catch (IndexOutOfBoundsException exception) {
            throw new InvalidColumnReferenceException("Position <" + oneBasedColumn + "> is not valid. Struct has " + getColumnCount() + " columns")
                    .toSqlException();
        }
    }

    private int countLeadingPhantomColumns() {
        /*
         * Adjust the one-based position to account for any leading Phantom columns
         */
        final var fields = getFields();
        int count = 0;
        for (var field: fields) {
            if (!KNOWN_PHANTOM_COLUMNS.contains(field.getName())) {
                return count;
            }
            count++;
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
                type.equals(otherMetadata.type);
    }

    @Override
    public int hashCode() {
        return hashCodeSupplier.get();
    }

    private int calculateHashCode() {
        return Objects.hash(type, leadingPhantomColumnOffset);
    }
}
