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

import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import java.sql.Types;

/**
 * Class to host method taken from SqlTypeSupport needed by
 * {@link RelationalStructMetaData}. SqlTypeSupport#getSqlTypeName
 * will now call through to the method below. We can't bring
 * SqlTypeSupport back here to fdb-relational-api. It depends on
 * recordlayer.
 */
// Used by fdb-relational-jdbc module in JDBCRelationalArray.
@ExcludeFromJacocoGeneratedReport
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
            default:
                throw new IllegalStateException("Unexpected sql type code :" + sqlTypeCode);
        }
    }

    public static int getSqlTypeCode(String sqlTypeName) {
        switch (sqlTypeName) {
            case "INTEGER":
                return Types.INTEGER;
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
            default:
                throw new IllegalStateException("Unexpected sql type name:" + sqlTypeName);
        }
    }
}
