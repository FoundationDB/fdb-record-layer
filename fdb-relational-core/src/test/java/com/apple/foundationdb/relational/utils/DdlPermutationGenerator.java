/*
 * DdlPermutationGenerator.java
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

package com.apple.foundationdb.relational.utils;

import com.apple.foundationdb.relational.api.EmbeddedRelationalArray;
import com.apple.foundationdb.relational.api.EmbeddedRelationalStruct;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * Generates DDL definitions for tables and types using permutative exhaustion.
 */
public final class DdlPermutationGenerator {
    private static final String[] validPrimitiveDataTypes = new String[]{
            "bigint", "double", "boolean", "string", "bytes"
    };

    public static Stream<NamedPermutation> generateTables(String prefix, int numColumns) {
        List<String> items = List.of(validPrimitiveDataTypes);

        final PermutationIterator<String> permutations = PermutationIterator.generatePermutations(items, numColumns);
        final AtomicInteger counter = new AtomicInteger(0);
        // generate valid unquoted identifiers.
        return permutations.stream().map(cols -> new NamedPermutation(prefix + counter.getAndIncrement(), cols));
    }

    public static final class NamedPermutation {
        private final String name;
        private final List<String> columnTypes;

        public NamedPermutation(String name, List<String> columnTypes) {
            this.name = name;
            this.columnTypes = columnTypes;
        }

        public String getTableDefinition(String tableName) {
            return tableName + columnTypeDefinition(true);
        }

        public String getTypeDefinition(String typeName) {
            return typeName + columnTypeDefinition(false);
        }

        public RelationalStruct getPermutation(String typeName) throws SQLException {
            List<RelationalStruct> rows = new ArrayList<>();
            int colNum = 0;
            for (String col : columnTypes) {
                int typeCode = toDataType(col).getJdbcSqlCode();
                rows.add(EmbeddedRelationalStruct.newBuilder()
                        .addString("COLUMN_NAME", "COL" + colNum)
                        .addInt("COLUMN_TYPE", typeCode)
                        .build());
                colNum++;
            }
            return EmbeddedRelationalStruct.newBuilder()
                    .addString("TABLE_NAME", typeName)
                    .addArray("COLUMNS", EmbeddedRelationalArray.newBuilder().addAll(rows.toArray()).build())
                    .build();
        }

        public String getName() {
            return name;
        }

        public List<String> getColumnTypes() {
            return columnTypes;
        }

        private String columnTypeDefinition(boolean isTable) {
            StringBuilder columnStatement = new StringBuilder("(");
            int pos = 0;
            for (String col : columnTypes) {
                if (pos != 0) {
                    columnStatement.append(",");
                }
                columnStatement.append("col").append(pos).append(" ").append(col);
                pos++;
            }
            //add a primary key
            if (isTable) {
                columnStatement.append(", PRIMARY KEY(").append("COL0").append(")");
            }
            return columnStatement.append(")").toString();
        }

        @Override
        public String toString() {
            return "NamedPermutation{" +
                    "name='" + name + '\'' +
                    ", columnTypes=" + columnTypes +
                    '}';
        }
    }

    private DdlPermutationGenerator() {
    }

    @Nonnull
    public static DataType toDataType(@Nonnull final String text) {
        switch (text.toUpperCase(Locale.ROOT)) {
            case "STRING":
                return DataType.Primitives.STRING.type();
            case "INTEGER":
                return DataType.Primitives.INTEGER.type();
            case "BIGINT":
                return DataType.Primitives.LONG.type();
            case "FLOAT":
                return DataType.Primitives.FLOAT.type();
            case "DOUBLE":
                return DataType.Primitives.DOUBLE.type();
            case "BOOLEAN":
                return DataType.Primitives.BOOLEAN.type();
            case "BYTES":
                return DataType.Primitives.BYTES.type();
            default: // assume it is a custom type, will fail in upper layers if the type can not be resolved.
                throw new RelationalException("Invalid type", ErrorCode.INTERNAL_ERROR).toUncheckedWrappedException();
        }
    }
}
