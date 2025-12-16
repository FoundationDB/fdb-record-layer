/*
 * RecordLayerStoreCatalogWithNoTemplateOperationsTest.java
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

package com.apple.foundationdb.relational.recordlayer.catalog;

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.test.FDBTestEnvironment;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.Schema;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.RecordContextTransaction;
import com.apple.foundationdb.relational.recordlayer.RelationalKeyspaceProvider;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;

public class RecordLayerStoreCatalogWithNoTemplateOperationsTest extends RecordLayerStoreCatalogTestBase {

    @BeforeEach
    void setUpCatalog() throws RelationalException {
        fdb = FDBDatabaseFactory.instance().getDatabase(FDBTestEnvironment.randomClusterFile());
        // create a FDBRecordStore
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog = StoreCatalogProvider.getCatalogWithNoTemplateOperations(txn);
            txn.commit();
        }
    }

    @AfterEach
    void deleteAllRecords() throws RelationalException {
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            final KeySpacePath keySpacePath = RelationalKeyspaceProvider.instance().toDatabasePath(URI.create("/__SYS")).schemaPath("CATALOG");
            FDBRecordStore.deleteStore(txn.unwrap(FDBRecordContext.class), keySpacePath);
            txn.commit();
        }
    }

    @Test
    void testLoadSchema() throws RelationalException {
        String templateName = "test_template_name";
        final var templateVersion = 1;
        // save record in FDB
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            Schema schema1 = generateTestSchema("test_schema_name", "/TEST/test_database_id", templateName, templateVersion);
            storeCatalog.getSchemaTemplateCatalog().createTemplate(txn, schema1.getSchemaTemplate());
            storeCatalog.createDatabase(txn, URI.create(schema1.getDatabaseName()));
            storeCatalog.saveSchema(txn, schema1, false);
            txn.commit();
        }

        // test loadSchema method with correct schema name
        try (Transaction loadTxn1 = new RecordContextTransaction(fdb.openContext())) {
            Schema result = storeCatalog.loadSchema(loadTxn1, URI.create("/TEST/test_database_id"), "test_schema_name");
            Assertions.assertEquals("test_schema_name", result.getName());
            Assertions.assertEquals("test_template_name", result.getSchemaTemplate().getName());
            Assertions.assertEquals(1, result.getSchemaTemplate().getVersion());
            RelationalException ex1 = Assertions.assertThrows(RelationalException.class, () -> result.getTables());
            Assertions.assertEquals("NoOpSchemaTemplate doesn't have tables!", ex1.getMessage());
            RelationalException ex2 = Assertions.assertThrows(RelationalException.class, () -> result.getIndexes());
            Assertions.assertEquals("NoOpSchemaTemplate doesn't have indexes!", ex2.getMessage());
        }
    }

    @Test
    void testSaveSchemaWithoutTemplate() throws RelationalException {
        String templateName = "test_template_name";
        final var templateVersion = 1;
        // save record in FDB
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            Schema schema1 = generateTestSchema("test_schema_name", "/TEST/test_database_id", templateName, templateVersion);
            storeCatalog.createDatabase(txn, URI.create(schema1.getDatabaseName()));
            storeCatalog.saveSchema(txn, schema1, false);
            txn.commit();
        }
    }

    @Test
    void testRepairSchema() throws RelationalException {
        // save schema with template version 1
        Schema schema1 = generateTestSchema("test_schema_name", "/TEST/test_database_id", "test_template_name", 1);
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.getSchemaTemplateCatalog().createTemplate(txn, schema1.getSchemaTemplate());
            storeCatalog.createDatabase(txn, URI.create(schema1.getDatabaseName()));
            storeCatalog.saveSchema(txn, schema1, false);
            txn.commit();
        }
        // save schema template with version  2
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            SchemaTemplate template2 = generateTestSchemaTemplate("test_template_name", 2);
            storeCatalog.getSchemaTemplateCatalog().createTemplate(txn, template2);
            txn.commit();
        }
        // repair schema
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            final var thrown = Assertions.assertThrows(RelationalException.class, () -> storeCatalog.repairSchema(txn, schema1.getDatabaseName(), schema1.getName()));
            Assertions.assertEquals(ErrorCode.UNSUPPORTED_OPERATION, thrown.getErrorCode());
            Assertions.assertTrue(thrown.getMessage().contains("does not support"));
            txn.commit();
        }
    }
}
