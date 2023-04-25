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

import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerColumn;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerIndex;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;

import javax.annotation.Nonnull;
import java.util.List;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;

/**
 * This class represents the {@code Schema} system table. This system table contains information
 * about all available schemas.
 */
public class SchemaSystemTable implements SystemTable {

    public static final String TABLE_NAME = SystemTableRegistry.SCHEMAS_TABLE_NAME;

    private static final String SCHEMA_NAME = "SCHEMA_NAME";

    private static final String DATABASE_ID = "DATABASE_ID";
    private static final String TEMPLATE_NAME = "TEMPLATE_NAME";
    private static final String TEMPLATE_VERSION = "TEMPLATE_VERSION";

    private static final RecordLayerIndex TEMPLATES_COUNT_INDEX = RecordLayerIndex.newBuilder()
            .setName("TEMPLATES_COUNT_INDEX")
            .setTableName(TABLE_NAME)
            .setIndexType(IndexTypes.COUNT)
            .setKeyExpression(new GroupingKeyExpression(concat(field(TEMPLATE_NAME), field(TEMPLATE_VERSION)), 0))
            .build();

    private static final RecordLayerIndex TEMPLATES_VALUE_INDEX = RecordLayerIndex.newBuilder()
            .setName("TEMPLATES_VALUE_INDEX")
            .setTableName(TABLE_NAME)
            .setIndexType(IndexTypes.VALUE)
            .setKeyExpression(concat(field(TEMPLATE_NAME), field(TEMPLATE_VERSION), field(DATABASE_ID), field(SCHEMA_NAME)))
            .build();

    @Nonnull
    @Override
    public String getName() {
        return TABLE_NAME;
    }

    @Override
    public void addDefinition(@Nonnull final RecordLayerSchemaTemplate.Builder schemaBuilder) {
        // construct the table type.
        schemaBuilder.addTable(getType());
    }

    @Override
    public RecordLayerTable getType() {
        return RecordLayerTable
                .newBuilder(false)
                .setName(TABLE_NAME)
                .addColumn(RecordLayerColumn.newBuilder().setName(DATABASE_ID).setDataType(DataType.Primitives.STRING.type()).build())
                .addColumn(RecordLayerColumn.newBuilder().setName(SCHEMA_NAME).setDataType(DataType.Primitives.STRING.type()).build())
                .addColumn(RecordLayerColumn.newBuilder().setName(TEMPLATE_NAME).setDataType(DataType.Primitives.STRING.type()).build())
                .addColumn(RecordLayerColumn.newBuilder().setName(TEMPLATE_VERSION).setDataType(DataType.Primitives.INTEGER.type()).build())
                .addPrimaryKeyPart(List.of(DATABASE_ID))
                .addPrimaryKeyPart(List.of(SCHEMA_NAME))
                .addIndex(TEMPLATES_COUNT_INDEX)
                .addIndex(TEMPLATES_VALUE_INDEX)
                .build();
    }

    @Nonnull
    @Override
    public KeyExpression getPrimaryKeyDefinition() {
        return concat(Key.Expressions.recordType(), Key.Expressions.concatenateFields(DATABASE_ID, SCHEMA_NAME));
    }
}
