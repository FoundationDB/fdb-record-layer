/*
 * JDBCQueryFactory.java
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

package com.apple.foundationdb.relational.compare;

import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplate;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplateCatalog;
import com.apple.foundationdb.relational.api.ddl.DdlQuery;
import com.apple.foundationdb.relational.api.ddl.DdlQueryFactory;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.generated.CatalogData;
import com.apple.foundationdb.relational.recordlayer.AbstractRow;
import com.apple.foundationdb.relational.recordlayer.IteratorResultSet;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nonnull;

public class JDBCQueryFactory implements DdlQueryFactory {
    private final SchemaTemplateCatalog templateCatalog;

    public JDBCQueryFactory(SchemaTemplateCatalog templateCatalog) {
        this.templateCatalog = templateCatalog;
    }

    @Override
    public DdlQuery getListDatabasesQueryAction(@Nonnull URI prefixPath) {
        throw new UnsupportedOperationException("Unimplemented in JDBC");
    }

    @Override
    public DdlQuery getListSchemasQueryAction(@Nonnull URI dbPath) {
        return new DdlQuery() {
            @Nonnull
            @Override
            public Type getResultSetMetadata() {
                return Type.Record.fromDescriptor(CatalogData.Schema.getDescriptor());
            }

            @Override
            public RelationalResultSet executeAction(Transaction txn) throws RelationalException {
                try (Connection sqlConn = getConnection(dbPath)) {
                    return new JDBCRelationalResultSet(sqlConn.getMetaData().getSchemas(dbPath.getPath().replaceAll("/", "_"), null));
                } catch (SQLException e) {
                    throw ExceptionUtil.toRelationalException(e);
                }
            }
        };
    }

    @Override
    public DdlQuery getListSchemaTemplatesQueryAction() {
        return new DdlQuery() {
            @Nonnull
            @Override
            public Type getResultSetMetadata() {
                return Type.primitiveType(Type.TypeCode.STRING);
            }

            @Override
            public RelationalResultSet executeAction(Transaction txn) throws RelationalException {
                return templateCatalog.listTemplates(txn);
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
            }
        };
    }

    @Override
    public DdlQuery getDescribeSchemaQueryAction(@Nonnull URI dbId, @Nonnull String schemaId) {
        throw new UnsupportedOperationException("unimplemented in JDBC");
    }

    private Connection getConnection(@Nonnull URI dbPath) throws SQLException {
        return DriverManager.getConnection("jdbc:h2:./src/test/resources/" + dbPath.getPath().replaceAll("/", "_"));
    }
}
