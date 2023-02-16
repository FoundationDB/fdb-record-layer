/*
 * RecordLayerStoreCatalogWithNoTemplateOperationsTest.java
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

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.Schema;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.KeySpaceUtils;
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
        fdb = FDBDatabaseFactory.instance().getDatabase();
        // create a FDBRecordStore
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog = StoreCatalogProvider.getCatalogWithNoTemplateOperations(txn);
            txn.commit();
        }
    }

    @AfterEach
    void deleteAllRecords() throws RelationalException {
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            final KeySpacePath keySpacePath = KeySpaceUtils.uriToPath(URI.create("/__SYS/CATALOG"), RelationalKeyspaceProvider.getKeySpace());
            FDBRecordStore.deleteStore(txn.unwrap(FDBRecordContext.class), keySpacePath);
            txn.commit();
        }
    }

    @Test
    void testLoadSchema() throws RelationalException {
        String templateName = "test_template_name";
        long templateVersion = 1L;
        // save record in FDB
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            Schema schema1 = generateTestSchema("test_schema_name", "test_database_id", templateName, templateVersion);
            storeCatalog.getSchemaTemplateCatalog().updateTemplate(txn, schema1.getSchemaTemplate());
            storeCatalog.createDatabase(txn, URI.create(schema1.getDatabaseName()));
            storeCatalog.saveSchema(txn, schema1);
            txn.commit();
        }

        try (Transaction loadTxn1 = new RecordContextTransaction(fdb.openContext())) {
            final var thrown = Assertions.assertThrows(RelationalException.class, () -> storeCatalog.loadSchema(loadTxn1, URI.create("test_database_id"), "test_schema_name"));
            Assertions.assertEquals(ErrorCode.UNSUPPORTED_OPERATION, thrown.getErrorCode());
            Assertions.assertTrue(thrown.getMessage().contains("does not support"));
        }
    }

    @Test
    void testSaveSchemaWithoutTemplate() throws RelationalException {
        String templateName = "test_template_name";
        long templateVersion = 1L;
        // save record in FDB
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            Schema schema1 = generateTestSchema("test_schema_name", "test_database_id", templateName, templateVersion);
            storeCatalog.createDatabase(txn, URI.create(schema1.getDatabaseName()));
            storeCatalog.saveSchema(txn, schema1);
            txn.commit();
        }
    }

    @Test
    void testSaveSchema() throws RelationalException {
        String templateName = "test_template_name";
        long templateVersion = 1L;
        Schema schema1 = generateTestSchema("test_schema_name", "test_database_id", templateName, templateVersion);
        // save record in FDB
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.getSchemaTemplateCatalog().updateTemplate(txn, schema1.getSchemaTemplate());
            storeCatalog.createDatabase(txn, URI.create(schema1.getDatabaseName()));
            storeCatalog.saveSchema(txn, schema1);
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
    void testRepairSchema() throws RelationalException {
        // save schema with template version 1L
        Schema schema1 = generateTestSchema("test_schema_name", "test_database_id", "test_template_name", 1);
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.getSchemaTemplateCatalog().updateTemplate(txn, schema1.getSchemaTemplate());
            storeCatalog.createDatabase(txn, URI.create(schema1.getDatabaseName()));
            storeCatalog.saveSchema(txn, schema1);
            txn.commit();
        }
        // save schema template with version  2L
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            SchemaTemplate template2 = generateTestSchemaTemplate("test_template_name", 2L);
            storeCatalog.getSchemaTemplateCatalog().updateTemplate(txn, template2);
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
