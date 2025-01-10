/*
 * SchemaTemplateSystemTable.java
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

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerColumn;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;

import javax.annotation.Nonnull;
import java.util.List;

@API(API.Status.EXPERIMENTAL)
public class SchemaTemplateSystemTable implements SystemTable {
    /**
     * Name of this catalog system table.
     */
    public static final String TABLE_NAME = SystemTableRegistry.SCHEMA_TEMPLATE_TABLE_NAME;
    /**
     * MetaData RecordLayer Column name.
     */
    public static final String METADATA = "META_DATA";

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
                .addColumn(RecordLayerColumn.newBuilder().setName(TEMPLATE_NAME).setDataType(DataType.Primitives.STRING.type()).build())
                .addColumn(RecordLayerColumn.newBuilder().setName(TEMPLATE_VERSION).setDataType(DataType.Primitives.INTEGER.type()).build())
                .addColumn(RecordLayerColumn.newBuilder().setName(METADATA).setDataType(DataType.Primitives.BYTES.type()).build())
                .addPrimaryKeyPart(List.of(TEMPLATE_NAME))
                .addPrimaryKeyPart(List.of(TEMPLATE_VERSION))
                .build();
    }

    @Nonnull
    @Override
    public KeyExpression getPrimaryKeyDefinition() {
        return Key.Expressions.concat(Key.Expressions.recordType(), Key.Expressions.concatenateFields(TEMPLATE_NAME, TEMPLATE_VERSION));
    }
}
