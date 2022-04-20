/*
 * CatalogQueryFactory.java
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

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplate;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplateCatalog;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.generated.CatalogData;
import com.apple.foundationdb.relational.recordlayer.AbstractRow;
import com.apple.foundationdb.relational.recordlayer.IteratorResultSet;

import java.net.URI;
import java.util.Collections;

import javax.annotation.Nonnull;

public class CatalogQueryFactory implements DdlQueryFactory {

    private final StoreCatalog catalog;
    private final SchemaTemplateCatalog templateCatalog;

    public CatalogQueryFactory(StoreCatalog catalog, SchemaTemplateCatalog templateCatalog) {
        this.catalog = catalog;
        this.templateCatalog = templateCatalog;
    }

    @Override
    public DdlQuery getListDatabasesQueryAction(@Nonnull URI prefixPath) {
        //TODO(bfines) make use of this prefix
        return txn -> catalog.listDatabases(txn, Continuation.BEGIN);
    }

    @Override
    public DdlQuery getListSchemasQueryAction(@Nonnull URI dbPath) {
        return txn -> catalog.listSchemas(txn, dbPath, Continuation.BEGIN);
    }

    @Override
    public DdlQuery getListSchemaTemplatesQueryAction() {
        return templateCatalog::listTemplates;
    }

    @Override
    public DdlQuery getDescribeSchemaTemplateQueryAction(@Nonnull String schemaId) {
        return txn -> {
            final SchemaTemplate schemaTemplate = templateCatalog.loadTemplate(txn, schemaId);

            Object[] fields = new Object[]{
                    schemaTemplate.getUniqueId(),
                    schemaTemplate.getTables(),
                    schemaTemplate.getTypes()
            };

            Row tuple = new AbstractRow() {
                @Override
                public int getNumFields() {
                    return 3;
                }

                @Override
                public Object getObject(int position) {
                    return fields[position];
                }
            };
            return new IteratorResultSet(new String[]{"TEMPLATE_NAME", "TYPES", "TABLES"}, Collections.singleton(tuple).iterator(), 0);
        };
    }

    @Override
    public DdlQuery getDescribeSchemaQueryAction(@Nonnull URI dbId, @Nonnull String schemaId) {
        return txn -> {
            final CatalogData.Schema schema = catalog.loadSchema(txn, dbId, schemaId);

            Object[] fields = new Object[]{
                    schema.getDatabaseId(),
                    schema.getSchemaName(),
                    schema.getTablesList(),
                    Collections.emptyList()
            };

            Row tuple = new AbstractRow() {
                @Override
                public int getNumFields() {
                    return 3;
                }

                @Override
                public Object getObject(int position) {
                    return fields[position];
                }
            };
            return new IteratorResultSet(new String[]{"DATABASE_PATH", "SCHEMA_NAME", "TABLES", "INDEXES"},
                    Collections.singleton(tuple).iterator(), 0);
        };
    }
}
