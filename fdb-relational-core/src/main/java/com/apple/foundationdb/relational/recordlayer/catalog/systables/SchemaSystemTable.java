/*
 * SchemaSystemTable.java
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

package com.apple.foundationdb.relational.recordlayer.catalog.systables;

import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.relational.api.catalog.TypeInfo;
import com.apple.foundationdb.relational.api.ddl.DdlListener;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.protobuf.DescriptorProtos;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

/**
 * This class represents the {@code Schema} system table. This system table contains information
 * about all available schemas.
 */
public class SchemaSystemTable implements SystemTable {

    public static final String TABLE_NAME = "Schema";

    private static final String SCHEMA_NAME = "schema_name";
    private static final String DATABASE_ID = "database_id";
    private static final String SCHEMA_TEMPLATE_NAME = "schema_template_name";
    private static final String SCHEMA_VERSION = "schema_version";
    private static final String TABLES = "tables";
    private static final String RECORD = "record";

    @Nonnull
    @Override
    public String getName() {
        return TABLE_NAME;
    }

    @Override
    @Nonnull
    public DdlListener.TableBuilder getDefinition(@Nonnull final DdlListener.SchemaTemplateBuilder schemaTemplateBuilder) {

        // construct the Index type.
        final DescriptorProtos.FieldDescriptorProto.Builder indexNameField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName(SCHEMA_NAME)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING);
        final DescriptorProtos.FieldDescriptorProto.Builder indexDefField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName("indexDef")
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES);

        final DdlListener.TableBuilder indexBuilder = new DdlListener.TableBuilder(Map.of(), "IndexAlias");
        try {
            indexBuilder.registerField(indexNameField, null);
            indexBuilder.registerField(indexDefField, null);
        } catch (RelationalException e) {
            throw e.toUncheckedWrappedException(); // should never happen.
        }

        // construct the Table type.
        final TypeInfo indexTypeInfo = new TypeInfo(indexBuilder.buildDescriptor());
        final DdlListener.TableBuilder tableBuilder = new DdlListener.TableBuilder(Map.of(indexTypeInfo.getTypeName(), indexTypeInfo), "Table");
        try {
            schemaTemplateBuilder.registerType(indexBuilder.getTypeName(), indexBuilder.buildDescriptor());

            final DescriptorProtos.FieldDescriptorProto.Builder tableNameField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                    .setName("name")
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING);
            final DescriptorProtos.FieldDescriptorProto.Builder primaryKeyField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                    .setName("primary_key")
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES);
            final DescriptorProtos.FieldDescriptorProto.Builder indexesField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                    .setName("indexes")
                    .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                    .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED);
            tableBuilder.registerField(tableNameField, null);
            tableBuilder.registerField(primaryKeyField, null);
            tableBuilder.registerField(indexesField, indexBuilder.getTypeName());
        } catch (RelationalException e) {
            throw e.toUncheckedWrappedException(); // should never happen.
        }

        final DescriptorProtos.FieldDescriptorProto.Builder schemaNameField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName(SCHEMA_NAME)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING);
        final DescriptorProtos.FieldDescriptorProto.Builder databaseIdField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName(DATABASE_ID)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING);
        final DescriptorProtos.FieldDescriptorProto.Builder schemaTemplateField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName(SCHEMA_TEMPLATE_NAME)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING);
        final DescriptorProtos.FieldDescriptorProto.Builder schemaVersionField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName(SCHEMA_VERSION)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32);
        final DescriptorProtos.FieldDescriptorProto.Builder tablesField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName(TABLES)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED);
        final DescriptorProtos.FieldDescriptorProto.Builder recordField = DescriptorProtos.FieldDescriptorProto.newBuilder()
                .setName(RECORD)
                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_BYTES);

        final TypeInfo tableTypeInfo = new TypeInfo(tableBuilder.buildDescriptor());
        final DdlListener.TableBuilder result = new DdlListener.TableBuilder(Map.of(tableTypeInfo.getTypeName(), tableTypeInfo), getName());
        result.addPrimaryKeyColumns(List.of(DATABASE_ID, SCHEMA_NAME));
        try {
            schemaTemplateBuilder.registerType(tableTypeInfo.getTypeName(), tableTypeInfo.getDescriptor());
            result.registerField(schemaNameField, null);
            result.registerField(databaseIdField, null);
            result.registerField(schemaTemplateField, null);
            result.registerField(schemaVersionField, null);
            result.registerField(tablesField, tableTypeInfo.getTypeName());
            result.registerField(recordField, null);
        } catch (RelationalException e) {
            throw e.toUncheckedWrappedException(); // should never happen.
        }
        return result;
    }

    @Nonnull
    @Override
    public KeyExpression getPrimaryKeyDefinition() {
        return Key.Expressions.concat(Key.Expressions.recordType(), Key.Expressions.concatenateFields(DATABASE_ID, SCHEMA_NAME));
    }

    @Override
    public int getRecordTypeKey() {
        return SystemTableRegistry.SCHEMA_RECORD_TYPE_KEY;
    }
}
