/*
 * RecordLayerCatalogQueryFactory.java
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

import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.FieldDescription;
import com.apple.foundationdb.relational.api.ImmutableRowStruct;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalArrayMetaData;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.ddl.CatalogQueryFactory;
import com.apple.foundationdb.relational.api.ddl.DdlQuery;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.Metadata;
import com.apple.foundationdb.relational.api.metadata.Schema;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.ArrayRow;
import com.apple.foundationdb.relational.recordlayer.IteratorResultSet;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

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

                final List<String> tableNames = schema.getTables().stream().map(Metadata::getName)
                        .collect(Collectors.toList());

                final List<String> indexNames = schema.getTables().stream().flatMap(t -> t.getIndexes().stream()).map(Metadata::getName)
                        .collect(Collectors.toList());

                final Row tuple = new ArrayRow(schema.getDatabaseName(),
                        schema.getName(),
                        tableNames,
                        indexNames);

                final FieldDescription[] fields = new FieldDescription[]{
                        FieldDescription.primitive("DATABASE_PATH", Types.VARCHAR, DatabaseMetaData.columnNoNulls),
                        FieldDescription.primitive("SCHEMA_NAME", Types.VARCHAR, DatabaseMetaData.columnNullable),
                        FieldDescription.array("TABLES", DatabaseMetaData.columnNullable, RelationalArrayMetaData.ofPrimitive(Types.VARCHAR, DatabaseMetaData.columnNoNulls)),
                        FieldDescription.array("INDEXES", DatabaseMetaData.columnNullable, RelationalArrayMetaData.ofPrimitive(Types.VARCHAR, DatabaseMetaData.columnNoNulls))
                };
                return new IteratorResultSet(new RelationalStructMetaData(fields),
                        Collections.singleton(tuple).iterator(), 0);
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
                final var columnStructMetadata = new RelationalStructMetaData(
                        FieldDescription.primitive("COLUMN_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls),
                        FieldDescription.primitive("COLUMN_TYPE", Types.INTEGER, DatabaseMetaData.columnNoNulls));
                final var tableStructMetadata = new RelationalStructMetaData(
                        FieldDescription.primitive("TABLE_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls),
                        FieldDescription.array("COLUMNS", DatabaseMetaData.columnNoNulls, RelationalArrayMetaData.ofStruct(
                                columnStructMetadata, DatabaseMetaData.columnNoNulls)));
                final var schemaTemplateStructMetadata = new RelationalStructMetaData(
                        FieldDescription.primitive("TEMPLATE_NAME", Types.VARCHAR, DatabaseMetaData.columnNoNulls),
                        FieldDescription.array("TABLES", DatabaseMetaData.columnNullable, RelationalArrayMetaData.ofStruct(
                                tableStructMetadata, DatabaseMetaData.columnNoNulls)));
                final var tableStructs = new ArrayList<>();
                for (var table: schemaTemplate.getTables()) {
                    final var columnStructs = new ArrayList<>();
                    for (var col: table.getColumns()) {
                        columnStructs.add(new ImmutableRowStruct(new ArrayRow(col.getName(), col.getDatatype().getJdbcSqlCode()), columnStructMetadata));
                    }
                    tableStructs.add(new ImmutableRowStruct(new ArrayRow(table.getName(), columnStructs), tableStructMetadata));
                }
                final Row tuple = new ArrayRow(schemaTemplate.getName(), tableStructs);
                return new IteratorResultSet(schemaTemplateStructMetadata, Collections.singleton(tuple).iterator(), 0);
            }
        };
    }
}
