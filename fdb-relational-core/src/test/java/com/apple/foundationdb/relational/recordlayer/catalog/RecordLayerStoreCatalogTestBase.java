/*
 * RecordLayerStoreCatalogTestBase.java
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

package com.apple.foundationdb.relational.recordlayer.catalog;

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.metadata.Schema;
import com.apple.foundationdb.relational.recordlayer.ContinuationImpl;
import com.apple.foundationdb.relational.recordlayer.RecordContextTransaction;
import com.apple.foundationdb.relational.recordlayer.RelationalKeyspaceProvider;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerColumn;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchema;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public abstract class RecordLayerStoreCatalogTestBase {
    FDBDatabase fdb;

    StoreCatalog storeCatalog;

    KeySpace keySpace;

    RecordLayerStoreCatalogTestBase() {
        RelationalKeyspaceProvider.registerDomainIfNotExists("TEST");
        keySpace = RelationalKeyspaceProvider.getKeySpace();
    }

    @Test
    void testListSchemasEmptyResult() throws RelationalException, SQLException {
        // list all schemas
        Set<String> fullSchemaNames = new HashSet<>();
        try (Transaction listTxn = new RecordContextTransaction(fdb.openContext())) {
            Continuation continuation = ContinuationImpl.BEGIN;
            do {
                try (RelationalResultSet result = storeCatalog.listSchemas(listTxn, continuation)) {
                    if (result.next()) {
                        fullSchemaNames.add(result.getString("DATABASE_ID") + "?schema=" + result.getString("SCHEMA_NAME"));
                    }
                    continuation = result.getContinuation();
                }
            } while (!continuation.atEnd());
        }
        // assert
        Assertions.assertEquals(1, fullSchemaNames.size());
        //the only entry should be the catalog
        Assertions.assertTrue(fullSchemaNames.contains("/__SYS?schema=CATALOG"));
    }

    @Test
    void testListSchemas() throws RelationalException, SQLException {
        int n = 24;
        // save schemas
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            for (int i = 0; i < n; i++) {
                Schema schema = generateTestSchema("test_schema_name" + i, "/TEST/test_database_id" + i / 2, "test_template_name", 1);
                storeCatalog.createDatabase(txn, URI.create(schema.getDatabaseName()));
                storeCatalog.getSchemaTemplateCatalog().updateTemplate(txn, schema.getSchemaTemplate());
                storeCatalog.saveSchema(txn, schema, false);
            }
            txn.commit();
        }
        // list all schemas
        Set<String> fullSchemaNames = new HashSet<>();
        try (Transaction listTxn = new RecordContextTransaction(fdb.openContext())) {
            Continuation continuation = ContinuationImpl.BEGIN;
            do {
                try (RelationalResultSet result = storeCatalog.listSchemas(listTxn, continuation)) {
                    // to test continuation, only read 1 result at once
                    if (result.next()) {
                        fullSchemaNames.add(result.getString("DATABASE_ID") + "?schema=" + result.getString("SCHEMA_NAME"));
                    }
                    continuation = result.getContinuation();
                }
            } while (!continuation.atEnd());
        }
        Assertions.assertEquals(n + 1, fullSchemaNames.size());
        for (int i = 0; i < n; i++) {
            Assertions.assertTrue(fullSchemaNames.contains("/TEST/test_database_id" + i / 2 + "?schema=test_schema_name" + i));
        }
        //should also contain the sys catalog schema
        Assertions.assertTrue(fullSchemaNames.contains("/__SYS?schema=CATALOG"));
        // list schemas of 1 database
        Set<String> resultSet = new HashSet<>();
        try (Transaction listTxn = new RecordContextTransaction(fdb.openContext())) {
            Continuation continuation = ContinuationImpl.BEGIN;
            do {
                try (RelationalResultSet result = storeCatalog.listSchemas(listTxn, URI.create("/TEST/test_database_id1"), continuation)) {
                    if (result.next()) {
                        resultSet.add(result.getString("DATABASE_ID") + "?schema=" + result.getString("SCHEMA_NAME"));
                    }
                    continuation = result.getContinuation();
                }
            } while (!continuation.atEnd());
        }
        Assertions.assertEquals(2, resultSet.size());
        Assertions.assertTrue(resultSet.contains("/TEST/test_database_id1?schema=test_schema_name2"));
        Assertions.assertTrue(resultSet.contains("/TEST/test_database_id1?schema=test_schema_name3"));
    }

    @Test
    void testSaveSchema() throws RelationalException {
        String templateName = "test_template_name";
        final var templateVersion = 1;
        Schema schema1 = generateTestSchema("test_schema_name", "/TEST/test_database_id", templateName, templateVersion);
        // save record in FDB
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.getSchemaTemplateCatalog().updateTemplate(txn, schema1.getSchemaTemplate());
            storeCatalog.createDatabase(txn, URI.create(schema1.getDatabaseName()));
            storeCatalog.saveSchema(txn, schema1, false);
            txn.commit();
        }
        // check schema exists!
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            final var exists = storeCatalog.doesSchemaExist(txn, URI.create(schema1.getDatabaseName()), schema1.getName());
            Assertions.assertTrue(exists);
        }
        // check database exists!
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            final var exists = storeCatalog.doesDatabaseExist(txn, URI.create(schema1.getDatabaseName()));
            Assertions.assertTrue(exists);
        }
    }

    @Test
    void testSaveSchemaWithCreateDatabase() throws RelationalException {
        String templateName = "test_template_name";
        final var templateVersion = 1;
        Schema schema1 = generateTestSchema("test_schema_name", "/TEST/test_database_id", templateName, templateVersion);
        // save record in FDB
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.getSchemaTemplateCatalog().updateTemplate(txn, schema1.getSchemaTemplate());
            storeCatalog.saveSchema(txn, schema1, true);
            txn.commit();
        }
        // check schema exists!
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            final var exists = storeCatalog.doesSchemaExist(txn, URI.create(schema1.getDatabaseName()), schema1.getName());
            Assertions.assertTrue(exists);
        }
        // check database exists!
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            final var exists = storeCatalog.doesDatabaseExist(txn, URI.create(schema1.getDatabaseName()));
            Assertions.assertTrue(exists);
        }
    }

    @Test
    void testDatabaseDeleteWorks() throws RelationalException {
        final Schema schema1 = generateTestSchema("test_schema_name1", "/TEST/test_database_id1", "test_template_name", 1);
        final Schema schema2 = generateTestSchema("test_schema_name2", "/TEST/test_database_id1", "test_template_name", 1);
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.getSchemaTemplateCatalog().updateTemplate(txn, schema1.getSchemaTemplate());
            storeCatalog.getSchemaTemplateCatalog().updateTemplate(txn, schema2.getSchemaTemplate());
            storeCatalog.createDatabase(txn, URI.create(schema1.getDatabaseName()));
            storeCatalog.saveSchema(txn, schema1, false);
            storeCatalog.saveSchema(txn, schema2, false);
            txn.commit();
        }

        //it should exist
        try (Transaction listTxn = new RecordContextTransaction(fdb.openContext())) {
            Assertions.assertTrue(storeCatalog.doesDatabaseExist(listTxn, URI.create(schema1.getDatabaseName())), "Did not find a database!");
            Assertions.assertTrue(storeCatalog.doesSchemaExist(listTxn, URI.create(schema1.getDatabaseName()), schema1.getName()), "Did not find a schema!");
            Assertions.assertTrue(storeCatalog.doesSchemaExist(listTxn, URI.create(schema2.getDatabaseName()), schema2.getName()), "Did not find a schema!");
        }

        //now delete it
        boolean operationCompleted = false;
        while (!operationCompleted) {
            try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
                operationCompleted = storeCatalog.deleteDatabase(txn, URI.create(schema1.getDatabaseName()));
                try {
                    txn.commit();
                } catch (RelationalException ex) {
                    if (ex.getErrorCode() != ErrorCode.TRANSACTION_INACTIVE && ex.getErrorCode() != ErrorCode.TRANSACTION_TIMEOUT) {
                        throw ex;
                    }
                }
            }
        }
        //now database and its schemas shouldn't exist
        try (Transaction listTxn = new RecordContextTransaction(fdb.openContext())) {
            Assertions.assertFalse(storeCatalog.doesDatabaseExist(listTxn, URI.create(schema1.getDatabaseName())), "Did not find a database!");
            Assertions.assertFalse(storeCatalog.doesSchemaExist(listTxn, URI.create(schema1.getDatabaseName()), schema1.getName()), "Did not find a schema!");
            Assertions.assertFalse(storeCatalog.doesSchemaExist(listTxn, URI.create(schema2.getDatabaseName()), schema2.getName()), "Did not find a schema!");
        }
    }

    @Test
    void testDatabaseDeleteWithPrefixWorks() throws RelationalException, SQLException {
        for (int i = 0; i < 100; i++) {
            final Schema schema = generateTestSchema("test_schema_name", "/TEST/test_db_prefix" + i, "test_template_name", 1);
            try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
                storeCatalog.getSchemaTemplateCatalog().updateTemplate(txn, schema.getSchemaTemplate());
                storeCatalog.createDatabase(txn, URI.create(schema.getDatabaseName()));
                storeCatalog.saveSchema(txn, schema, false);
                txn.commit();
            }
        }

        //it should exist
        for (int i = 0; i < 100; i++) {
            try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
                Assertions.assertTrue(storeCatalog.doesDatabaseExist(txn, URI.create("/TEST/test_db_prefix" + i)), "Did not find a database!");
                Assertions.assertTrue(storeCatalog.doesSchemaExist(txn, URI.create("/TEST/test_db_prefix" + i), "test_schema_name"), "Did not find a schema!");
            }
        }

        //now delete it
        boolean operationCompleted = false;
        while (!operationCompleted) {
            try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
                operationCompleted = storeCatalog.deleteDatabasesWithPrefix(txn, "/TEST/test_db_prefix1");
                try {
                    txn.commit();
                } catch (RelationalException ex) {
                    if (ex.getErrorCode() != ErrorCode.TRANSACTION_INACTIVE && ex.getErrorCode() != ErrorCode.TRANSACTION_TIMEOUT) {
                        throw ex;
                    }
                }
            }
        }

        //now database and its schemas shouldn't exist
        final var dbnos = Set.of(1, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 100);
        for (int i = 0; i < 100; i++) {
            try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
                final var dbPath = "/TEST/test_db_prefix" + i;
                if (dbnos.contains(i)) {
                    Assertions.assertFalse(storeCatalog.doesDatabaseExist(txn, URI.create(dbPath)), "Found a database that should have been deleted! dbPath:" + dbPath);
                    Assertions.assertFalse(storeCatalog.doesSchemaExist(txn, URI.create(dbPath), "test_schema_name"), "Found a schema that should have been deleted! dbPath:" + dbPath);
                } else {
                    Assertions.assertTrue(storeCatalog.doesDatabaseExist(txn, URI.create(dbPath)), "Did not find a database! dbPath:" + dbPath);
                    Assertions.assertTrue(storeCatalog.doesSchemaExist(txn, URI.create(dbPath), "test_schema_name"), "Did not find a schema! dbPath:" + dbPath);
                }
            }
        }
    }

    @Test
    void testDoesDatabaseExist() throws RelationalException {
        final Schema schema1 = generateTestSchema("test_schema_name1", "/TEST/test_database_id1", "test_template_name", 1);
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.getSchemaTemplateCatalog().updateTemplate(txn, schema1.getSchemaTemplate());
            storeCatalog.createDatabase(txn, URI.create(schema1.getDatabaseName()));
            storeCatalog.saveSchema(txn, schema1, false);
            txn.commit();
        }

        //check if it exists
        try (Transaction listTxn = new RecordContextTransaction(fdb.openContext())) {
            Assertions.assertTrue(storeCatalog.doesDatabaseExist(listTxn, URI.create(schema1.getDatabaseName())), "Did not find a database!");
        }
    }

    @Test
    void testListDatabases() throws RelationalException, SQLException {
        // save 2 schemas
        final Schema schema1 = generateTestSchema("test_schema_name1", "/TEST/test_database_id1", "test_template_name", 1);
        final Schema schema2 = generateTestSchema("test_schema_name2", "/TEST/test_database_id2", "test_template_name", 1);
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.createDatabase(txn, URI.create(schema1.getDatabaseName()));
            storeCatalog.createDatabase(txn, URI.create(schema2.getDatabaseName()));
            storeCatalog.getSchemaTemplateCatalog().updateTemplate(txn, schema1.getSchemaTemplate());
            storeCatalog.getSchemaTemplateCatalog().updateTemplate(txn, schema2.getSchemaTemplate());
            storeCatalog.saveSchema(txn, schema1, false);
            storeCatalog.saveSchema(txn, schema2, false);
            txn.commit();
        }
        // list databases
        Set<String> databases = new HashSet<>();
        try (Transaction listTxn = new RecordContextTransaction(fdb.openContext())) {
            Continuation continuation = ContinuationImpl.BEGIN;
            do {
                RelationalResultSet result = storeCatalog.listDatabases(listTxn, continuation);
                while (result.next()) {
                    databases.add(result.getString("DATABASE_ID"));
                }
                continuation = result.getContinuation();
            } while (!continuation.atEnd());
        }
        Assertions.assertEquals(3, databases.size());
        Assertions.assertTrue(databases.contains("/TEST/test_database_id1"));
        Assertions.assertTrue(databases.contains("/TEST/test_database_id2"));
        Assertions.assertTrue(databases.contains("/__SYS"));
    }

    @Test
    void testAllTheSchemas() throws RelationalException, SQLException {
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            final RelationalResultSet relationalResultSet = storeCatalog.listSchemas(txn, ContinuationImpl.BEGIN);
            int schemaCount = 0;
            final var schemas = new ArrayList<String>();
            while (relationalResultSet.next()) {
                schemaCount += 1;
                schemas.add(relationalResultSet.getString("SCHEMA_NAME"));
            }
            Assertions.assertEquals(1, schemaCount);
            Assertions.assertTrue(schemas.contains("CATALOG"));
        }

    }

    @SuppressWarnings({"SameParameterValue"})
    static RecordLayerSchema generateTestSchema(@Nonnull final String schemaName,
                                                @Nonnull final String databaseId,
                                                @Nonnull final String schemaTemplateName,
                                                final int schemaTemplateVersion) {
        final var template = generateTestSchemaTemplate(schemaTemplateName, schemaTemplateVersion);
        return template.generateSchema(databaseId, schemaName);
    }

    @Nonnull
    static RecordLayerSchemaTemplate generateTestSchemaTemplate(@Nonnull final String schemaTemplateName, int version) {
        return RecordLayerSchemaTemplate
                .newBuilder()
                .addTable(
                        RecordLayerTable
                                .newBuilder(false)
                                .addColumn(
                                        RecordLayerColumn
                                                .newBuilder()
                                                .setName("A")
                                                .setDataType(DataType.Primitives.STRING.type())
                                                .build())
                                .setName("test_table1")
                                .build())
                .addTable(
                        RecordLayerTable
                                .newBuilder(false)
                                .addColumn(
                                        RecordLayerColumn
                                                .newBuilder()
                                                .setName("A")
                                                .setDataType(DataType.Primitives.STRING.type())
                                                .build())
                                .setName("test_table2")
                                .build())
                .setVersion(version)
                .setName(schemaTemplateName)
                .build();
    }
}
