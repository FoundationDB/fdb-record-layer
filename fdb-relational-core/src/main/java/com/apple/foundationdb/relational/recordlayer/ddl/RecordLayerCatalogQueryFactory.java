/*
 * RecordLayerCatalogQueryFactory.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.ImmutableRowStruct;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.ddl.CatalogQueryFactory;
import com.apple.foundationdb.relational.api.ddl.DdlQuery;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.metadata.Metadata;
import com.apple.foundationdb.relational.api.metadata.Schema;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.ArrayRow;
import com.apple.foundationdb.relational.recordlayer.IteratorResultSet;
import com.apple.foundationdb.relational.recordlayer.metadata.DataTypeUtils;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@API(API.Status.EXPERIMENTAL)
public class RecordLayerCatalogQueryFactory extends CatalogQueryFactory {
    public RecordLayerCatalogQueryFactory(StoreCatalog catalog) {
        super(catalog);
    }

    @Override
    public DdlQuery getDescribeSchemaQueryAction(@Nonnull URI dbId, @Nonnull String schemaId) {
        return new DdlQuery() {
            @Nonnull
            @Override
            public Type getResultSetMetadata() {
                return DdlQuery.constructTypeFrom(List.of("DATABASE_PATH", "SCHEMA_NAME", "TABLES", "INDEXES"));
            }

            @Override
            public RelationalResultSet executeAction(Transaction txn) throws RelationalException {
                final Schema schema = catalog.loadSchema(txn, dbId, schemaId);

                final List<String> tableNames = schema.getTables().stream().map(Metadata::getName).map(DataTypeUtils::toUserIdentifier)
                        .collect(Collectors.toList());

                final List<String> indexNames = schema.getTables().stream().flatMap(t -> t.getIndexes().stream()).map(Metadata::getName)
                        .collect(Collectors.toList());

                final Row tuple = new ArrayRow(schema.getDatabaseName(),
                        schema.getName(),
                        tableNames,
                        indexNames);

                final var describeSchemaStructType = DataType.StructType.from("DESCRIBE_SCHEMA", List.of(
                        DataType.StructType.Field.from("DATABASE_PATH", DataType.Primitives.STRING.type(), 0),
                        DataType.StructType.Field.from("SCHEMA_NAME", DataType.Primitives.NULLABLE_STRING.type(), 1),
                        DataType.StructType.Field.from("TABLES", DataType.ArrayType.from(DataType.Primitives.STRING.type()), 2),
                        DataType.StructType.Field.from("INDEXES", DataType.ArrayType.from(DataType.Primitives.STRING.type()), 3)
                ), true);
                return new IteratorResultSet(RelationalStructMetaData.of(describeSchemaStructType), Collections.singleton(tuple).iterator(), 0);
            }
        };
    }

    @Override
    public DdlQuery getDescribeSchemaTemplateQueryAction(@Nonnull String schemaId) {
        return new DdlQuery() {
            @Nonnull
            @Override
            public Type getResultSetMetadata() {
                return DdlQuery.constructTypeFrom(List.of("TEMPLATE_NAME", "TABLES"));
            }

            @Override
            public RelationalResultSet executeAction(Transaction txn) throws RelationalException {
                final SchemaTemplate schemaTemplate = catalog.getSchemaTemplateCatalog().loadSchemaTemplate(txn, schemaId);
                final var columnType = DataType.StructType.from("COLUMN", List.of(
                        DataType.StructType.Field.from("COLUMN_NAME", DataType.Primitives.STRING.type(), 0),
                                DataType.StructType.Field.from("COLUMN_TYPE", DataType.Primitives.INTEGER.type(), 1)),
                        true);
                final var tableType = DataType.StructType.from("TABLE", List.of(
                        DataType.StructType.Field.from("TABLE_NAME", DataType.Primitives.STRING.type(), 0),
                                DataType.StructType.Field.from("COLUMNS", DataType.ArrayType.from(columnType, true), 1)),
                        true);
                final var describeSchemaType = DataType.StructType.from("DESCRIBE_SCHEMA_TEMPLATE", List.of(
                        DataType.StructType.Field.from("TEMPLATE_NAME", DataType.Primitives.STRING.type(), 0),
                                DataType.StructType.Field.from("TABLES", DataType.ArrayType.from(tableType, true), 1)),
                        true);

                final var tableStructs = new ArrayList<>();
                for (var table : schemaTemplate.getTables()) {
                    final var columnStructs = new ArrayList<>();
                    for (var col : table.getColumns()) {
                        columnStructs.add(new ImmutableRowStruct(new ArrayRow(col.getName(), col.getDataType().getJdbcSqlCode()), RelationalStructMetaData.of(columnType)));
                    }
                    tableStructs.add(new ImmutableRowStruct(new ArrayRow(table.getName(), columnStructs), RelationalStructMetaData.of(tableType)));
                }
                final Row tuple = new ArrayRow(schemaTemplate.getName(), tableStructs);
                return new IteratorResultSet(RelationalStructMetaData.of(describeSchemaType), Collections.singleton(tuple).iterator(), 0);
            }
        };
    }
}
