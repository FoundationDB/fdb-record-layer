/*
 * PlaceholderCatalog.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.record.RecordMetaDataOptionsProto;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.protobuf.Descriptors;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A quick placeholder until the main Catalog work lands.
 */
public class PlaceholderCatalog {
    private final Map<String, SchemaData> schemaMap = new HashMap<>();

    public PlaceholderCatalog() {
    }

    public void registerSchema(SchemaData schemaData) throws RelationalException {
        SchemaData old = schemaMap.putIfAbsent(schemaData.getSchemaName(), schemaData);
        if (old != schemaData) {
            throw new RelationalException("Schema <" + schemaData + "> already exists!", ErrorCode.SCHEMA_MAPPING_ALREADY_EXISTS);
        }
    }

    @Nullable
    public SchemaData loadSchema(String name) throws RelationalException {
        return schemaMap.get(name);
    }

    public static class SchemaData {
        private final String name;
        private final Map<String, TableInfo> tableMap;
        private Descriptors.FileDescriptor fileDesc;

        public SchemaData(String name, Descriptors.FileDescriptor fileDesc, Map<String, TableInfo> tableMap) {
            this.name = name;
            this.tableMap = tableMap;
            this.fileDesc = fileDesc;
        }

        public TableInfo getTableInfo(String tableName) {
            return tableMap.get(tableName);
        }

        public Set<TableInfo> getAllTables() {
            return new HashSet<>(tableMap.values());
        }

        public String getSchemaName() {
            return name;
        }

        public Descriptors.FileDescriptor getFileDescriptor() {
            return fileDesc;
        }
    }

    public static class TableInfo {
        private final Descriptors.Descriptor tableDescriptor;
        private final List<String> primaryKeys;

        public TableInfo(Descriptors.Descriptor tableDescriptor, List<String> primaryKeys) {
            this.tableDescriptor = tableDescriptor;
            this.primaryKeys = primaryKeys;
        }

        // The order matters here--it should come back in primary key order.
        List<String> getPrimaryKeys() {
            return primaryKeys;
        }

        public Descriptors.Descriptor getTableDescriptor() {
            return tableDescriptor;
        }

        public String getTableStatement() {
            StringBuilder sb = new StringBuilder("CREATE TABLE " + tableDescriptor.getName() + " (");
            boolean isFirst = true;
            for (Descriptors.FieldDescriptor fd : tableDescriptor.getFields()) {
                if (isFirst) {
                    isFirst = false;
                } else {
                    sb.append(",");
                }
                sb.append(fd.getName()).append(" ").append(typeForDescriptor(fd));
                if (!fd.getOptions().getAllFieldsRaw().isEmpty()) {
                    for (Object o : fd.getOptions().getAllFieldsRaw().values()) {
                        if (o instanceof RecordMetaDataOptionsProto.FieldOptions) {
                            RecordMetaDataOptionsProto.FieldOptions fo = (RecordMetaDataOptionsProto.FieldOptions) o;
                            if (fo.getPrimaryKey()) {
                                sb.append(", PRIMARY KEY");
                            }
                        }
                    }
                }
            }
            return sb.append(")").toString();
        }

        private String typeForDescriptor(Descriptors.FieldDescriptor field) {
            String type = field.isRepeated() ? "repeated " : "";
            switch (field.getJavaType()) {
                case INT:
                case LONG:
                    return type + "int64";
                case FLOAT:
                case DOUBLE:
                    return type + "double";
                case BOOLEAN:
                    return type + "boolean";
                case STRING:
                    return type + "string";
                case BYTE_STRING:
                    return type + "bytes";
                case MESSAGE:
                    return type + "message " + field.getMessageType().getName();
                case ENUM:
                default:
                    throw new UnsupportedOperationException("Not Implemented in the Relational layer");
            }
        }
    }
}
