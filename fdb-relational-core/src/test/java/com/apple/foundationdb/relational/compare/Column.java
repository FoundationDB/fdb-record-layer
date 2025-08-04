/*
 * Column.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.compare;

import java.util.HashMap;
import java.util.Map;

class Column {

    private static final Map<Class<?>, String> sqlDataTypeMap;

    static {
        sqlDataTypeMap = new HashMap<>();
        sqlDataTypeMap.put(Boolean.class, "BOOLEAN");
        sqlDataTypeMap.put(Byte.class, "SMALLINT");
        sqlDataTypeMap.put(Character.class, "SMALLINT");
        sqlDataTypeMap.put(Integer.class, "INTEGER");
        sqlDataTypeMap.put(Long.class, "BIGINT");
        sqlDataTypeMap.put(Float.class, "DOUBLE");
        sqlDataTypeMap.put(Double.class, "DOUBLE");
        sqlDataTypeMap.put(String.class, "VARCHAR(65535)");
        sqlDataTypeMap.put(byte[].class, "BLOB(65535)");
    }

    final String columnName;
    final String columnType;

    public Column(String columnName, String columnType) {
        this.columnName = columnName;
        this.columnType = columnType;
    }

    static Column intType(String columnName) {
        return new Column(columnName, sqlDataTypeMap.get(Integer.class));
    }

    static Column longType(String columnName) {
        return new Column(columnName, sqlDataTypeMap.get(Long.class));
    }

    static Column floatType(String columnName) {
        return new Column(columnName, sqlDataTypeMap.get(Float.class));
    }

    static Column doubleType(String columnName) {
        return new Column(columnName, sqlDataTypeMap.get(Double.class));
    }

    static Column stringType(String columnName) {
        return new Column(columnName, sqlDataTypeMap.get(String.class));
    }

    static Column bytesType(String columnName) {
        return new Column(columnName, sqlDataTypeMap.get(byte[].class));
    }

    public static Column booleanType(String columnName) {
        return new Column(columnName, sqlDataTypeMap.get(Boolean.class));
    }

    public String getName() {
        return columnName;
    }
}
