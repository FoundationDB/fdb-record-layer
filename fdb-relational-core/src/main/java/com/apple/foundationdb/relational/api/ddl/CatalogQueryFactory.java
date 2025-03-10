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

import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.Metadata;
import com.apple.foundationdb.relational.recordlayer.ContinuationImpl;
import com.apple.foundationdb.relational.recordlayer.catalog.systables.SystemTableRegistry;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

public abstract class CatalogQueryFactory implements DdlQueryFactory {

    protected final StoreCatalog catalog;

    public CatalogQueryFactory(StoreCatalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public DdlQuery getListDatabasesQueryAction(@Nonnull URI prefixPath) {
        //TODO(bfines) make use of this prefix
        return new DdlQuery() {
            @Override
            @Nonnull
            public Type getResultSetMetadata() {
                final List<String> fieldNames = SystemTableRegistry.getSystemTable(SystemTableRegistry.DATABASE_TABLE_NAME).getType().getColumns().stream()
                        .map(Metadata::getName)
                        .collect(Collectors.toList());
                return DdlQuery.constructTypeFrom(fieldNames);
            }

            @Override
            public RelationalResultSet executeAction(Transaction txn) throws RelationalException {
                return catalog.listDatabases(txn, ContinuationImpl.BEGIN);
            }
        };
    }

    @Override
    public DdlQuery getListSchemaTemplatesQueryAction() {
        final var columns = List.of("TEMPLATE_NAME");
        return new DdlQuery() {
            @Override
            @Nonnull
            public Type getResultSetMetadata() {
                return DdlQuery.constructTypeFrom(columns);
            }

            @Override
            public RelationalResultSet executeAction(Transaction txn) throws RelationalException {
                return catalog.getSchemaTemplateCatalog().listTemplates(txn);
            }
        };
    }
}
