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

import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.SqlTypeSupport;
import com.apple.foundationdb.relational.recordlayer.ArrayRow;
import com.apple.foundationdb.relational.recordlayer.query.ParserUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

/**
 * Generates DDL definitions for tables and types using permutative exhaustion.
 */
public final class DdlPermutationGenerator {
    private static final String[] validPrimitiveDataTypes = new String[]{
            "int64", "double", "boolean", "string", "bytes"
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

        public Row getPermutationAsRow(String typeName) {
            List<Row> rows = new ArrayList<>();
            int colNum = 0;
            for (String col : columnTypes) {
                int typeCode = SqlTypeSupport.recordTypeToSqlType(ParserUtils.toProtoType(col));
                rows.add(new ArrayRow("COL" + colNum, typeCode));
                colNum++;
            }
            return new ArrayRow(typeName,
                    rows);
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
}
