/*
 * DdlTestUtil.java
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

package com.apple.foundationdb.relational.api.ddl;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.RecordMetaDataOptionsProto;

import com.google.protobuf.DescriptorProtos;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@API(API.Status.EXPERIMENTAL)
public class DdlTestUtil {

    public static class ParsedColumn {
        private final DescriptorProtos.FieldDescriptorProto descriptor;

        public ParsedColumn(DescriptorProtos.FieldDescriptorProto descriptor) {
            this.descriptor = descriptor;
        }

        String getName() {
            return descriptor.getName();
        }

        String getType() {
            String type = "";
            switch (descriptor.getType()) {
                case TYPE_INT32:
                    type += "INTEGER";
                    break;
                case TYPE_INT64:
                    type += "BIGINT";
                    break;
                case TYPE_FLOAT:
                    type += "FLOAT";
                    break;
                case TYPE_DOUBLE:
                    type += "DOUBLE";
                    break;
                case TYPE_BOOL:
                    type += "BOOLEAN";
                    break;
                case TYPE_STRING:
                    type += "STRING";
                    break;
                case TYPE_BYTES:
                    type += "BYTES";
                    break;
                case TYPE_MESSAGE:
                    type += "MESSAGE " + descriptor.getTypeName();
                    break;
                case TYPE_ENUM:
                //TODO(Bfines) figure this one out
                default:
                    throw new IllegalStateException("Unexpected descriptor java type <" + descriptor.getType());
            }

            if (descriptor.getLabel() == DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED) {
                type = " array";
            }

            return type;
        }
    }

    public static class ParsedType {
        private final DescriptorProtos.DescriptorProto descriptor;

        private List<ParsedColumn> columns;

        public ParsedType(DescriptorProtos.DescriptorProto descriptor) {
            this.descriptor = descriptor;
            this.columns = parseColumns();
        }

        public String getName() {
            return descriptor.getName();
        }

        List<ParsedColumn> getColumns() {
            return columns;
        }

        private List<ParsedColumn> parseColumns() {
            List<ParsedColumn> cols = new ArrayList<>(descriptor.getFieldCount());
            for (DescriptorProtos.FieldDescriptorProto field :descriptor.getFieldList()) {
                cols.add(new ParsedColumn(field));
            }
            return cols;
        }

        /**
         * Get the Columns listed the way the DDL language would expect them, with 1 entry per column of the form.
         * [NAME TYPE]
         */
        public List<String> getColumnStrings() {
            return columns.stream().map(col -> col.getName() + " " + col.getType()).collect(Collectors.toList());
        }
    }

    public static class ParsedTable extends ParsedType {
        public ParsedTable(DescriptorProtos.DescriptorProto descriptor) {
            super(descriptor);
        }
    }

    public static class ParsedSchema {
        private final DescriptorProtos.FileDescriptorProto schemaDescriptor;

        private List<ParsedTable> tables;
        private List<ParsedType> types;

        public ParsedSchema(DescriptorProtos.FileDescriptorProto schemaDescriptor) {
            this.schemaDescriptor = schemaDescriptor;

            buildTypesAndTables();
        }

        List<ParsedTable> getTables() {
            return tables;
        }

        List<ParsedType> getTypes() {
            return types;
        }

        private void buildTypesAndTables() {
            /*
             * Parses the FileDescriptor into types and tables
             */
            //first, find the UnionDescriptor so that we know what are tables
            Set<String> tableNames = new HashSet<>();
            for (DescriptorProtos.DescriptorProto typeDesc : schemaDescriptor.getMessageTypeList()) {
                final RecordMetaDataOptionsProto.RecordTypeOptions extension = typeDesc.getOptions().getExtension(RecordMetaDataOptionsProto.record);
                if (extension != null && extension.hasUsage() && extension.getUsage() == RecordMetaDataOptionsProto.RecordTypeOptions.Usage.UNION) {
                    //we found the Union Descriptor
                    for (DescriptorProtos.FieldDescriptorProto tableDescs : typeDesc.getFieldList()) {
                        tableNames.add(tableDescs.getTypeName());
                    }
                }
            }

            //now parse types and tables
            types = new ArrayList<>();
            tables = new ArrayList<>();

            for (DescriptorProtos.DescriptorProto typeDesc : schemaDescriptor.getMessageTypeList()) {
                if (tableNames.contains(typeDesc.getName())) {
                    tables.add(new ParsedTable(typeDesc));
                } else {
                    types.add(new ParsedType(typeDesc));
                }
            }
        }

        public ParsedType getType(String typeName) {
            for (ParsedType parsedType : types) {
                if (parsedType.getName().equalsIgnoreCase(typeName)) {
                    return parsedType;
                }
            }
            return null;
        }

        public ParsedType getTable(String tableName) {
            for (ParsedType table : tables) {
                if (table.getName().equalsIgnoreCase(tableName)) {
                    return table;
                }
            }
            return null;
        }
    }

}
