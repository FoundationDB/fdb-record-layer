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

package com.apple.foundationdb.relational.recordlayer.ddl;

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataOptionsProto;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.relational.api.catalog.DatabaseSchema;
import com.apple.foundationdb.relational.api.catalog.TypeInfo;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.catalog.Schema;
import com.apple.foundationdb.relational.recordlayer.catalog.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.catalog.TableInfo;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

public class SchemaTemplateDescriptor implements SchemaTemplate {
    private final String name;
    @SuppressWarnings("PMD.LooseCoupling")
    private final LinkedHashSet<TableInfo> tables;
    private final Set<TypeInfo> customTypes;
    @SuppressWarnings("PMD.UnusedPrivateField") //this will be used eventually
    private long version;
    private final RecordMetaDataProto.MetaData metaData;

    @SuppressWarnings("PMD.LooseCoupling")
    public SchemaTemplateDescriptor(String name,
                                    LinkedHashSet<TableInfo> tables,
                                    Set<TypeInfo> customTypes,
                                    long version) {
        this.name = name;
        this.tables = tables;
        this.customTypes = customTypes;
        this.version = version;
        this.metaData = null;
    }

    public SchemaTemplateDescriptor(String name, @Nonnull RecordMetaDataProto.MetaData metaData, long version) {
        this.name = name;
        this.metaData = metaData;
        this.tables = new LinkedHashSet<>();
        RecordMetaData recordMetaData = RecordMetaData.build(metaData);
        Map<String, RecordType> recordTypeMap = recordMetaData.getRecordTypes();
        for (RecordType recordType : recordTypeMap.values()) {
            this.tables.add(TableInfo.fromRecordType(recordType));
        }
        this.customTypes = recordMetaData.getRecordsDescriptor().getMessageTypes().stream().filter(mType -> !recordTypeMap.containsKey(mType.getName())).map(mType -> new TypeInfo(mType.toProto())).collect(Collectors.toSet());
        this.version = version;
    }

    @Override
    public String getUniqueId() {
        return name;
    }

    @Override
    public long getVersion() {
        return version;
    }

    @Override
    @Nonnull
    public RecordMetaDataProto.MetaData getMetaData() throws RelationalException {
        return metaData == null ? buildMetaData() : metaData;
    }

    @Override
    public Schema generateSchema(String databaseId, String schemaName) throws RelationalException {

        try {
            final Descriptors.FileDescriptor fd = Descriptors.FileDescriptor.buildFrom(toProtobufDescriptor(),
                    new Descriptors.FileDescriptor[]{RecordMetaDataProto.getDescriptor()});
            RecordMetaDataBuilder rmd = RecordMetaData.newBuilder().setRecords(fd);
            int typeKey = 0;
            for (TableInfo table : tables) {
                final KeyExpression keyExpression = table.getPrimaryKey();
                final RecordTypeBuilder recordType = rmd.getRecordType(table.getTableName());
                recordType.setRecordTypeKey(typeKey++);
                recordType.setPrimaryKey(keyExpression);
                table.getIndexes().forEach(index -> rmd.addIndex(table.getTableName(), new Index(index)));
            }
            return new Schema(databaseId, schemaName, rmd.build().toProto(), name, version);
        } catch (Descriptors.DescriptorValidationException e) {
            throw ExceptionUtil.toRelationalException(e);
        }
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

    private RecordMetaDataProto.MetaData buildMetaData() throws RelationalException {
        try {
            final Descriptors.FileDescriptor fd = Descriptors.FileDescriptor.buildFrom(toProtobufDescriptor(),
                    new Descriptors.FileDescriptor[]{RecordMetaDataProto.getDescriptor()});
            RecordMetaDataBuilder rmd = RecordMetaData.newBuilder().setRecords(fd);
            int typeKey = 0;
            for (TableInfo table : tables) {
                final KeyExpression keyExpression = table.getPrimaryKey();
                final RecordTypeBuilder recordType = rmd.getRecordType(table.getTableName());
                recordType.setRecordTypeKey(typeKey++);
                recordType.setPrimaryKey(keyExpression);
                table.getIndexes().forEach(index -> rmd.addIndex(table.getTableName(), new Index(index)));
            }
            return rmd.build().toProto();
        } catch (Descriptors.DescriptorValidationException e) {
            throw ExceptionUtil.toRelationalException(e);
        }
    }

}
