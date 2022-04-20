/*
 * RelationalStructure.java
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

package com.apple.foundationdb.relational.compare;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class RelationalStructure {
    private final Map<String, Table> tables;
    private final Map<String, AtomicLong> recordIdCounters = new HashMap<>();
    private final Descriptors.Descriptor topLevelDescriptor;

    public RelationalStructure(Descriptors.Descriptor messageDescriptor) {
        this.topLevelDescriptor = messageDescriptor;
        Set<Table> structures = createFullStructure(messageDescriptor);
        this.tables = new HashMap<>();
        for (Table structure : structures) {
            tables.put(structure.getName(), structure);
        }
    }

    void createTables(java.sql.Statement statement) throws SQLException {
    }

    void dropTables(java.sql.Statement statement) throws SQLException {
        for (Table table : tables.values()) {
            statement.execute("DROP TABLE " + table.getName());
        }
    }

    Map<String, ValuesClause> flattenToValues(Message m) {
        Map<String, ValuesClause> flattenedMap = new HashMap<>();
        long recordID = generateRecordId(m.getDescriptorForType().getName());
        flattenToValues(m, recordID, flattenedMap);
        return flattenedMap;
    }

    private void flattenToValues(Message m, long recordId, Map<String, ValuesClause> destination) {
        String parentTableName = m.getDescriptorForType().getName();

        ValuesClause destClause = destination.computeIfAbsent(parentTableName, k -> new ValuesClause(columnsForTable(k)));
        destClause.addValues(Long.toString(recordId));
        for (Descriptors.FieldDescriptor field : m.getDescriptorForType().getFields()) {
            if (field.isRepeated()) {
                if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                    for (int i = 0; i < m.getRepeatedFieldCount(field); i++) {
                        Message nestedMessage = (Message) m.getRepeatedField(field, i);
                        //add an entry to the nested table
                        final long nestedRecordId = generateRecordId(field.getName());
                        flattenToValues(nestedMessage, nestedRecordId, destination);
                        //now add an entry to the link table
                        ValuesClause linkTableClause = destination.computeIfAbsent(parentTableName + "_" + field.getName(), k -> new ValuesClause(columnsForTable(k)));
                        linkTableClause.addValues(Long.toString(recordId));
                        linkTableClause.addValues(Long.toString(nestedRecordId));
                    }
                } else {
                    String leafTableName = parentTableName + "_" + field.getName();

                    //add an entry to the link table for this recordId
                    ValuesClause linkTableClause = destination.computeIfAbsent(leafTableName, k -> new ValuesClause(columnsForTable(k)));
                    for (int i = 0; i < m.getRepeatedFieldCount(field); i++) {
                        Object o = m.getRepeatedField(field, i);
                        String value = o == null ? "NULL" : o.toString();
                        if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.STRING) {
                            value = "'" + escapeQuotes(value) + "'";
                        }
                        linkTableClause.addValues(Long.toString(recordId));
                        linkTableClause.addValues(value);
                    }
                }

            } else if (!m.hasField(field)) {
                //TODO(bfines) this where we use default values
                destClause.addValues("NULL");
            } else {
                if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                    Message nestedMessage = (Message) m.getField(field);
                    //add an entry in the nested table
                    final long nestedRecordId = generateRecordId(field.getName());
                    flattenToValues(nestedMessage, nestedRecordId, destination);
                    destClause.addValues(Long.toString(nestedRecordId));
                }  else if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.STRING) {
                    Object o = m.getField(field);
                    String s = o == null ? "NULL" : "'" + escapeQuotes((String) o) + "'";
                    destClause.addValues(s);
                } else {
                    Object o = m.getField(field);
                    destClause.addValues(o == null ? "NULL" : o.toString());
                }
            }
        }
    }

    private String escapeQuotes(String o) {
        return o.replaceAll("'", "''");
    }

    private List<String> columnsForTable(String tableName) {
        final Table structure = tables.get(tableName);
        return structure.getColumns().stream().map(Column::getName).collect(Collectors.toList());
    }

    private long generateRecordId(String name) {
        return recordIdCounters.computeIfAbsent(name, k -> new AtomicLong(0L)).getAndIncrement();
    }

    private static Table createLinkTable(String parentFieldName, Descriptors.FieldDescriptor field) {
        String tableName = parentFieldName + "_" + field.getName();
        List<Column> columns = new ArrayList<>();
        columns.add(Column.intType("PARENT_RECORD_ID"));
        switch (field.getJavaType()) {
            case INT:
                columns.add(Column.intType(field.getName()));
                break;
            case LONG:
                columns.add(Column.longType(field.getName()));
                break;
            case FLOAT:
                columns.add(Column.floatType(field.getName()));
                break;
            case DOUBLE:
                columns.add(Column.doubleType(field.getName()));
                break;
            case BOOLEAN:
                columns.add(Column.booleanType(field.getName()));
                break;
            case STRING:
            case ENUM:
                columns.add(Column.stringType(field.getName()));
                break;
            case BYTE_STRING:
                columns.add(Column.bytesType(field.getName()));
                break;
            case MESSAGE:
                columns.add(Column.intType(field.getName() + "_RECORD_ID"));
                break;
            default:
                throw new IllegalStateException("Unexpected java type <" + field.getJavaType() + ">");
        }
        return new Table(tableName, columns);
    }

    public static Set<Table> createFullStructure(Descriptors.Descriptor protobufDescriptor) {
        /*
         * The basic structure is as follows:
         *
         * 1. Each Protobuf Descriptor gets a table.
         * 2. Each record has a RECORD_ID column (used for link joins)
         * 3. Each nested structure get their own table with two extra columns (RECORD_ID and PARENT_RECORD_ID).
         * 4. Each repeated structure gets a join table with columns (PARENT_RECORD_ID,REPEATED_VALUE) OR (if it's a
         * repeated nested structure), with (PARENT_RECORD_ID,CHILD_RECORD_ID).
         * 5. All other data types are converted to their SQL equivalents:
         *     a. byte = SMALLINT
         *     b. char = SMALLINT
         *     c. int = INT
         *     d. long = SIGNED BIGINT
         *     e. float = double (? -bfines- is this right? I can't remember...)
         *     f. double = double
         *     g. string = varchar(65535) -- WE CAN HOLD IT ALLLLLLLLLLLLL
         *     h. bytes = BLOB(65535)
         *     i. enum = VARCHAR(255)
         */

        String tableName = protobufDescriptor.getName();
        Set<Table> nestedStructures = new HashSet<>();

        List<Column> columns = new ArrayList<>();
        columns.add(Column.intType("RECORD_ID"));
        for (Descriptors.FieldDescriptor field : protobufDescriptor.getFields()) {
            if (field.isRepeated()) {
                nestedStructures.add(createLinkTable(tableName, field));
                if (field.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                    nestedStructures.addAll(createFullStructure(field.getMessageType()));
                }
            } else {

                switch (field.getJavaType()) {
                    case INT:
                        columns.add(Column.intType(field.getName()));
                        break;
                    case LONG:
                        columns.add(Column.longType(field.getName()));
                        break;
                    case FLOAT:
                        columns.add(Column.floatType(field.getName()));
                        break;
                    case DOUBLE:
                        columns.add(Column.doubleType(field.getName()));
                        break;
                    case BOOLEAN:
                        break;
                    case STRING:
                    case ENUM:
                        columns.add(Column.stringType(field.getName()));
                        break;
                    case BYTE_STRING:
                        columns.add(Column.bytesType(field.getName()));
                        break;
                    case MESSAGE:
                        //TODO(bfines) do the work to create the table for the nested type
                        nestedStructures.addAll(createFullStructure(field.getMessageType()));
                        columns.add(Column.intType(field.getName() + "_id"));
                        break;
                    default:
                        throw new IllegalStateException("Unexpected java type <" + field.getJavaType() + ">");
                }
            }
        }

        DynamicMessage dm = DynamicMessage.newBuilder(protobufDescriptor).getDefaultInstanceForType();
        nestedStructures.add(new Table(tableName, columns, protobufDescriptor, dm));
        return nestedStructures;
    }

    public Table getTable(String tableName) {
        Table t = tables.get(tableName);
        if (t == null) {
            throw new IllegalArgumentException("Table <" + tableName + "> does not exist");
        }
        return t;
    }

    public Collection<Table> getAllTables() {
        return tables.values();
    }

}
