/*
 * SchemaGenerator.java
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

package com.apple.foundationdb.relational.autotest.datagen;

import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.relational.api.FieldDescription;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.RelationalArrayMetaData;
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.autotest.SchemaDescription;
import com.apple.foundationdb.relational.autotest.TableDescription;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A Generator for Schemas--this will create different schema layouts permutatively,
 * varying the columns, indexes (eventually), and primary keys. It's useful for creating a large variety
 * of different data layouts.
 */
public class SchemaGenerator {

    private static final List<NonnullPair<String, Integer>> primitiveDataTypes = List.of(
            NonnullPair.of("bigint", Types.BIGINT),
            NonnullPair.of("double", Types.DOUBLE),
            NonnullPair.of("string", Types.VARCHAR),
            NonnullPair.of("bytes", Types.BINARY)
    // removing boolean to avoid pk violations with small tables
    );
    private final RandomDataSource random;
    private final int maxTables;
    private final int maxNumStructs;
    private final int maxNumColumns;

    public SchemaGenerator(RandomDataSource random, int maxTables, int maxNumStructs, int maxNumColumns) {
        this.random = random;
        this.maxTables = maxTables;
        this.maxNumStructs = maxNumStructs;
        this.maxNumColumns = maxNumColumns;
    }

    public SchemaDescription generateSchemaDescription(String templateName, String schemaName) throws SQLException {
        List<NonnullPair<String, Integer>> availableColumnTypes = new ArrayList<>(primitiveDataTypes);
        List<String> schemaEntries = new ArrayList<>();
        Map<String, StructMetaData> structMap = new HashMap<>();
        int numStructs = random.nextInt(maxNumStructs + 1);
        for (int i = 0; i < numStructs; i++) {
            Map.Entry<StructMetaData, String> struct = generateStruct(availableColumnTypes);
            //add the structs to the column type so that you can CREATE TYPE AS STRUCTs within structs
            availableColumnTypes.add(NonnullPair.of(struct.getKey().getTypeName(), Types.STRUCT));
            structMap.put(struct.getKey().getTypeName(), struct.getKey());
            schemaEntries.add(struct.getValue());
        }
        //now generate a random number of tables with the specified types
        int numTables = random.nextInt(1, maxTables + 1);

        List<TableDescription> tableNames = new ArrayList<>();
        for (int tableNum = 0; tableNum < numTables; tableNum++) {
            Map.Entry<TableDescription, String> table = generateTable(availableColumnTypes, structMap);
            schemaEntries.add(table.getValue());
            tableNames.add(table.getKey());
        }

        String templateDescription = String.join("\r\n", schemaEntries);

        return new SchemaDescription(templateName, templateDescription, schemaName, templateName + "_" + schemaName, tableNames);
    }

    private Map.Entry<TableDescription, String> generateTable(List<NonnullPair<String, Integer>> availableColumnTypes, Map<String, StructMetaData> structMetaDataMap) {
        List<ColumnDesc> columns = generateColumns(availableColumnTypes, structMetaDataMap);
        List<String> pkColumns = selectPrimaryKeys(columns);
        String typeName = "table_" + random.nextAlphaNumeric(5);
        String sb = "CREATE TABLE \"" + typeName + "\"(" +
                columns.stream().map(Object::toString).collect(Collectors.joining(",")) +
                ", PRIMARY KEY(" + pkColumns.stream().map(pk -> "\"" + pk + "\"").collect(Collectors.joining(",")) + ")" +
                ")";
        final var fieldDescs = new FieldDescription[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            final var col = columns.get(i);
            if (col.sqlType == Types.STRUCT) {
                if (col.isRepeated) {
                    fieldDescs[i] =  FieldDescription.array(col.name, DatabaseMetaData.columnNoNulls, RelationalArrayMetaData.ofStruct(col.structMetaData, DatabaseMetaData.columnNoNulls));
                } else {
                    fieldDescs[i] =  FieldDescription.struct(col.name, DatabaseMetaData.columnNoNulls, col.structMetaData);
                }
            } else {
                if (col.isRepeated) {
                    fieldDescs[i] =  FieldDescription.array(col.name, DatabaseMetaData.columnNoNulls, RelationalArrayMetaData.ofPrimitive(col.sqlType, DatabaseMetaData.columnNoNulls));
                } else {
                    fieldDescs[i] =  FieldDescription.primitive(col.name, col.sqlType, DatabaseMetaData.columnNoNulls);
                }
            }
        }
        TableDescription tableDef = new TableDescription(new RelationalStructMetaData(typeName, fieldDescs), pkColumns);
        return new AbstractMap.SimpleEntry<>(tableDef, sb);
    }

    private Map.Entry<StructMetaData, String> generateStruct(List<NonnullPair<String, Integer>> availableColumnTypes) {
        List<ColumnDesc> columns = generateColumns(availableColumnTypes, null);
        final var fieldDescs = new FieldDescription[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            final var col = columns.get(0);
            fieldDescs[i] = FieldDescription.primitive(col.name, col.sqlType, DatabaseMetaData.columnNoNulls);
        }
        String typeName = "struct_" + random.nextAlphaNumeric(5);
        final var metaData = new RelationalStructMetaData(typeName, fieldDescs);
        String sb = "CREATE TYPE AS STRUCT \"" + typeName + "\" (" +
                columns.stream().map(Object::toString).collect(Collectors.joining(",")) +
                ")";
        return new AbstractMap.SimpleEntry<>(metaData, sb);
    }

    private List<ColumnDesc> generateColumns(List<NonnullPair<String, Integer>> availableColumnTypes, @Nullable Map<String, StructMetaData> structMetaDataMap) {
        //generate some columns, but we need at least 1
        int numCols = random.nextInt(1, maxNumColumns);
        List<ColumnDesc> columnDescs = new ArrayList<>(numCols);
        /*
         * We need to ensure that there is at least one primitive type in the struct. This is mainly
         * for mechanical reasons--we use this code to generate tables _and_ structs, and tables require
         * primary keys; our current primary key generator logic requires selecting non-struct and non-array
         * types, so we need to make sure that there is at least one.
         *
         * For structs, this doesn't matter at all, but because we want to share code we put this in. If we later
         * change the PK generation logic to not have that restriction, we can remove this code block
         */

        NonnullPair<String, Integer> primitiveType = primitiveDataTypes.get(random.nextInt(primitiveDataTypes.size()));
        int ptColNumber = random.nextInt(numCols);
        ColumnDesc reqPrimitiveType = new ColumnDesc("col_" + ptColNumber, primitiveType.getRight(), primitiveType.getLeft(), false);
        columnDescs.add(reqPrimitiveType);

        //now generate the rest of the columns
        Set<Integer> takenColumnNumbers = new HashSet<>();
        takenColumnNumbers.add(ptColNumber);
        OUTER: while (columnDescs.size() < numCols) {
            NonnullPair<String, Integer> nameAndType = availableColumnTypes.get(random.nextInt(availableColumnTypes.size()));
            int colNum = random.nextInt(numCols);
            int finalColNum = colNum;
            while (takenColumnNumbers.contains(colNum)) {
                colNum = (colNum + 1) % numCols;
                if (colNum == finalColNum) {
                    //we have filled the loop. Shouldn't happen but you never know
                    break OUTER;
                }
            }
            if (nameAndType.getRight() == Types.STRUCT) {
                columnDescs.add(new ColumnDesc("col_" + colNum, nameAndType.getRight(), nameAndType.getLeft(), random.nextBoolean(), structMetaDataMap.get(nameAndType.getLeft())));
            } else {
                columnDescs.add(new ColumnDesc("col_" + colNum, nameAndType.getRight(), nameAndType.getLeft(), random.nextBoolean()));
            }
            takenColumnNumbers.add(colNum);
        }

        return columnDescs;
    }

    // Currently only allows selecting primitive type columns as primary keys
    private List<String> selectPrimaryKeys(List<ColumnDesc> columns) {
        int numPks = random.nextInt(1, columns.size());
        List<String> pkCols = new ArrayList<>(numPks);
        Set<String> chosenColumns = new HashSet<>(); //to make sure we don't select the same column twice
        RUN_LOOP: while (pkCols.size() < numPks) {
            int nextColPos = random.nextInt(columns.size());
            ColumnDesc chosenCol = columns.get(nextColPos);
            int loopPos = nextColPos;
            while ((chosenColumns.contains(chosenCol.name) || !chosenCol.allowedInPrimaryKey())) {
                nextColPos = (nextColPos + 1) % columns.size();
                chosenCol = columns.get(nextColPos);
                if (nextColPos == loopPos) {
                    //we went all the way around and couldn't find any PKs
                    break RUN_LOOP;
                }
            }
            chosenColumns.add(chosenCol.name);
            pkCols.add(chosenCol.name);
        }

        return pkCols;
    }

    private static class ColumnDesc {
        private final String name;
        private final int sqlType;
        private final String sqlName;
        private final boolean isRepeated;
        @Nullable
        private final StructMetaData structMetaData;

        public ColumnDesc(String name, int sqlType, @Nonnull String sqlName, boolean isRepeated, @Nullable StructMetaData structMetaData) {
            this.name = name;
            this.sqlType = sqlType;
            this.sqlName = sqlName;
            this.isRepeated = isRepeated;
            this.structMetaData = structMetaData;
        }

        public ColumnDesc(String name, int sqlType, @Nonnull String sqlName, boolean isRepeated) {
            this(name, sqlType, sqlName, isRepeated, null);
        }

        /*
         * returns true if this column is a non-repeated primitive type
         */
        public boolean isSinglePrimitiveType() {
            return !isRepeated && primitiveDataTypes.stream().map(NonnullPair::getRight).anyMatch(v -> v.equals(sqlType));
        }

        public boolean allowedInPrimaryKey() {
            //TODO(bfines) allow booleans in pks again, but right now the cardinality is goofy
            return isSinglePrimitiveType() && Types.BOOLEAN != sqlType;
        }

        @Override
        public String toString() {
            String type = " " + sqlName + (isRepeated ? " ARRAY" : "");
            return "\"" + name + "\"" + type;
        }
    }
}
