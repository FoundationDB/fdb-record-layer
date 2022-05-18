/*
 * SchemaTemplateDescriptor.java
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

import com.apple.foundationdb.record.RecordMetaDataOptionsProto;
import com.apple.foundationdb.relational.api.catalog.DatabaseSchema;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplate;
import com.apple.foundationdb.relational.api.catalog.TableInfo;
import com.apple.foundationdb.relational.api.catalog.TypeInfo;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.generated.CatalogData;

import com.google.protobuf.DescriptorProtos;

import java.util.LinkedHashSet;
import java.util.Set;

import javax.annotation.Nonnull;

public class SchemaTemplateDescriptor implements SchemaTemplate {
    private final String name;
    @SuppressWarnings("PMD.LooseCoupling")
    private final LinkedHashSet<TableInfo> tables;
    private final Set<TypeInfo> customTypes;
    @SuppressWarnings("PMD.UnusedPrivateField") //this will be used eventually
    private final int version;

    @SuppressWarnings("PMD.LooseCoupling")
    public SchemaTemplateDescriptor(String name,
                                    LinkedHashSet<TableInfo> tables,
                                    Set<TypeInfo> customTypes,
                                    int version) {
        this.name = name;
        this.tables = tables;
        this.customTypes = customTypes;
        this.version = version;
    }

    @Override
    public String getUniqueId() {
        return name;
    }

    @Override
    public CatalogData.Schema generateSchema(String databaseId, String schemaName) {
        final CatalogData.Schema.Builder builder = CatalogData.Schema.newBuilder().setSchemaName(schemaName)
                .setDatabaseId(databaseId)
                .setSchemaTemplateName(getUniqueId())
                .setSchemaVersion(1) //TODO(bfines) add this to the SchemaTemplate object
                .setRecord(toProtobufDescriptor().toByteString());

        tables.stream().map(TableInfo::getTable).forEach(builder::addTables);
        return builder.build();
    }

    @Override
    public boolean isValid(@Nonnull DatabaseSchema schema) throws RelationalException {
        return false;
    }

    @Override
    public DescriptorProtos.FileDescriptorProto toProtobufDescriptor() {
        DescriptorProtos.FileDescriptorProto.Builder fileBuilder = DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName(name);
        for (TypeInfo customType : customTypes) {
            fileBuilder.addMessageType(customType.getDescriptor());
        }

        DescriptorProtos.DescriptorProto.Builder unionDescriptor = DescriptorProtos.DescriptorProto.newBuilder()
                .setName("RecordTypeUnion");
        //TODO(bfines) move this dependency somehow
        final RecordMetaDataOptionsProto.RecordTypeOptions options = RecordMetaDataOptionsProto.RecordTypeOptions.newBuilder().setUsage(RecordMetaDataOptionsProto.RecordTypeOptions.Usage.UNION).build();
        unionDescriptor.getOptionsBuilder().setExtension(RecordMetaDataOptionsProto.record, options);

        int tableCnt = 1;
        for (TableInfo table : tables) {
            //add the table as a custom type
            fileBuilder.addMessageType(table.toDescriptor());

            //now add a table entry to the Union descriptor
            DescriptorProtos.FieldDescriptorProto fdProto = DescriptorProtos.FieldDescriptorProto.newBuilder()
                    .setNumber(tableCnt)
                    .setName("_" + table.getTableName())
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                    .setTypeName(table.toDescriptor().getName())
                    .build();
            unionDescriptor.addField(fdProto);
            tableCnt++;
        }
        fileBuilder.addMessageType(unionDescriptor);
        return fileBuilder.build();
    }

    @SuppressWarnings("PMD.LooseCoupling")
    @Override
    public LinkedHashSet<TableInfo> getTables() {
        return tables;
    }

    @Override
    public Set<TypeInfo> getTypes() {
        return customTypes;
    }

}
