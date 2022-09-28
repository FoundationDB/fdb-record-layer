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

import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.FieldDescription;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.SqlTypeSupport;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplateCatalog;
import com.apple.foundationdb.relational.api.ddl.CatalogQueryFactory;
import com.apple.foundationdb.relational.api.ddl.DdlQuery;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.ArrayRow;
import com.apple.foundationdb.relational.recordlayer.IteratorResultSet;
import com.apple.foundationdb.relational.recordlayer.catalog.Schema;
import com.apple.foundationdb.relational.recordlayer.catalog.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.catalog.StoreCatalog;

import com.google.protobuf.Descriptors;

import java.net.URI;
import java.sql.Types;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

public class RecordLayerCatalogQueryFactory extends CatalogQueryFactory {
    public RecordLayerCatalogQueryFactory(StoreCatalog catalog, SchemaTemplateCatalog templateCatalog) {
        super(catalog, templateCatalog);
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

                List<String> tableNames = schema.getMetaData().getRecordTypesList().stream()
                        .map(RecordMetaDataProto.RecordType::getName)
                        .collect(Collectors.toList());

                List<String> indexNames = schema.getMetaData().getIndexesList().stream()
                        .map(RecordMetaDataProto.Index::getName)
                        .collect(Collectors.toList());

                Row tuple = new ArrayRow(new Object[]{
                        schema.getDatabaseId(),
                        schema.getSchemaName(),
                        tableNames,
                        indexNames,
                });

                FieldDescription[] fields = new FieldDescription[]{
                        FieldDescription.primitive("DATABASE_PATH", Types.VARCHAR, false),
                        FieldDescription.primitive("SCHEMA_NAME", Types.VARCHAR, true),
                        FieldDescription.array("TABLES", true, new RelationalStructMetaData(FieldDescription.primitive("TABLE", Types.VARCHAR, false))),
                        FieldDescription.array("INDEXES", true, new RelationalStructMetaData(FieldDescription.primitive("INDEX", Types.VARCHAR, false)))
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
                return DdlQuery.constructTypeFrom(List.of("TEMPLATE_NAME", "TYPES", "TABLES"));
            }

            @Override
            public RelationalResultSet executeAction(Transaction txn) throws RelationalException {
                final SchemaTemplate schemaTemplate = templateCatalog.loadTemplate(txn, schemaId);

                FieldDescription[] typeDescription = new FieldDescription[]{
                        FieldDescription.primitive("TYPE_NAME", Types.VARCHAR, false),
                        FieldDescription.array("COLUMNS", false, new RelationalStructMetaData(
                                FieldDescription.primitive("COLUMN_NAME", Types.VARCHAR, false),
                                FieldDescription.primitive("COLUMN_TYPE", Types.INTEGER, false)
                        ))
                };

                FieldDescription[] tableDescription = new FieldDescription[]{
                        FieldDescription.primitive("TABLE_NAME", Types.VARCHAR, false),
                        FieldDescription.array("COLUMNS", false, new RelationalStructMetaData(
                                FieldDescription.primitive("COLUMN_NAME", Types.VARCHAR, false),
                                FieldDescription.primitive("COLUMN_TYPE", Types.INTEGER, false)
                        ))
                };

                List<Row> typeRows = schemaTemplate.getTypes().stream()
                        .map(typeInfo -> new ArrayRow(new Object[]{
                                typeInfo.getTypeName(),
                                typeInfo.getDescriptor().getFieldList().stream()
                                        .map(fieldProto -> new ArrayRow(new Object[]{
                                                fieldProto.getName(),
                                                SqlTypeSupport.recordTypeToSqlType(Type.TypeCode.fromProtobufType(Descriptors.FieldDescriptor.Type.valueOf(fieldProto.getType())))
                                        })).collect(Collectors.toList())
                        }))
                        .collect(Collectors.toList());

                List<Row> tableRows = schemaTemplate.getTables().stream()
                        .map(tableInfo -> new ArrayRow(new Object[]{
                                tableInfo.getTableName(),
                                tableInfo.toDescriptor().getFieldList().stream()
                                        .map(field -> new ArrayRow(new Object[]{
                                                field.getName(),
                                                SqlTypeSupport.recordTypeToSqlType(Type.TypeCode.fromProtobufType(Descriptors.FieldDescriptor.Type.valueOf(field.getType())))
                                        }))
                                        .collect(Collectors.toList())
                        })).collect(Collectors.toList());

                Object[] fields = new Object[]{
                        schemaTemplate.getUniqueId(),
                        typeRows,
                        tableRows
                };

                Row tuple = new ArrayRow(fields);
                FieldDescription[] fieldDescriptions = new FieldDescription[]{
                        FieldDescription.primitive("TEMPLATE_NAME", Types.VARCHAR, false),
                        FieldDescription.array("TYPES", true, new RelationalStructMetaData(typeDescription)),
                        FieldDescription.array("TABLES", true, new RelationalStructMetaData(tableDescription))
                };
                return new IteratorResultSet(new RelationalStructMetaData(fieldDescriptions), Collections.singleton(tuple).iterator(), 0);
            }
        };
    }
}
