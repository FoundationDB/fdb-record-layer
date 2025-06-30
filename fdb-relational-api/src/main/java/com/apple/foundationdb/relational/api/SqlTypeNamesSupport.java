/*
 * SqlTypeNamesSupport.java
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
import com.apple.foundationdb.relational.api.metadata.DataType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Types;

/**
 * Class to host method taken from SqlTypeSupport needed by
 * {@link RelationalStructMetaData}. SqlTypeSupport#getSqlTypeName
 * will now call through to the method below. We can't bring
 * SqlTypeSupport back here to fdb-relational-api. It depends on
 * recordlayer.
 */
// Used by fdb-relational-jdbc module in JDBCRelationalArray.
@API(API.Status.EXPERIMENTAL)
public final class SqlTypeNamesSupport {
    private SqlTypeNamesSupport() {
    }

    public static String getSqlTypeName(int sqlTypeCode) {
        switch (sqlTypeCode) {
            case Types.INTEGER:
                return "INTEGER";
            case Types.BIGINT:
                return "BIGINT";
            case Types.FLOAT:
                return "FLOAT";
            case Types.DOUBLE:
                return "DOUBLE";
            case Types.VARCHAR:
                return "STRING";
            case Types.STRUCT:
                return "STRUCT";
            case Types.ARRAY:
                return "ARRAY";
            case Types.BINARY:
                return "BINARY";
            case Types.NULL:
                return "NULL";
            case Types.OTHER:
                return "OTHER";
            case Types.BOOLEAN:
                return "BOOLEAN";
            default:
                throw new IllegalStateException("Unexpected sql type code :" + sqlTypeCode);
        }
    }

    public static int getSqlTypeCode(String sqlTypeName) {
        switch (sqlTypeName) {
            case "INTEGER":
                return Types.INTEGER;
            case "BINARY":
                return Types.BINARY;
            case "BIGINT":
                return Types.BIGINT;
            case "FLOAT":
                return Types.FLOAT;
            case "DOUBLE":
                return Types.DOUBLE;
            case "STRING":
                return Types.VARCHAR;
            case "STRUCT":
                return Types.STRUCT;
            case "ARRAY":
                return Types.ARRAY;
            case "NULL":
                return Types.NULL;
            case "BOOLEAN":
                return Types.BOOLEAN;
            default:
                throw new IllegalStateException("Unexpected sql type name:" + sqlTypeName);
        }
    }

    /**
     * This method does the best-case effort to match the JDBC SQL Type name with the corresponding non-nullable version
     * of the DataType. As evident, we cannot actually match all the type, like composite type struct and array. In
     * that case, we simply return a {@code null}.
     *
     * @param sqlTypeName the name of the JDBC-supported SQL types takem from {@link Types}
     * @return the equivalent {@link DataType} for the type name
     */
    @Nullable
    public static DataType getDataTypeFromSqlTypeName(@Nonnull String sqlTypeName) {
        switch (sqlTypeName) {
            case "INTEGER":
                return DataType.Primitives.INTEGER.type();
            case "BINARY":
                return DataType.Primitives.BYTES.type();
            case "BIGINT":
                return DataType.Primitives.LONG.type();
            case "FLOAT":
                return DataType.Primitives.FLOAT.type();
            case "DOUBLE":
                return DataType.Primitives.DOUBLE.type();
            case "STRING":
                return DataType.Primitives.STRING.type();
            case "BOOLEAN":
                return DataType.Primitives.BOOLEAN.type();
            case "NULL":
                return DataType.Primitives.NULL.type();
            default:
                return null;
        }
    }
}
