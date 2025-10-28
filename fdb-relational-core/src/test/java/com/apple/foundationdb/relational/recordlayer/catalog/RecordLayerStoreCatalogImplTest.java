/*
 * RecordLayerStoreCatalogImplTest.java
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
import com.apple.foundationdb.relational.api.metadata.Metadata;
import com.apple.foundationdb.relational.api.metadata.Schema;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.api.metadata.View;
import com.apple.foundationdb.relational.recordlayer.RecordContextTransaction;
import com.apple.foundationdb.relational.recordlayer.RelationalKeyspaceProvider;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class RecordLayerStoreCatalogImplTest extends RecordLayerStoreCatalogTestBase {

    @BeforeEach
    void setUpCatalog() throws RelationalException {
        fdb = FDBDatabaseFactory.instance().getDatabase(FDBTestEnvironment.randomClusterFile());
        // create a FDBRecordStore
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog = StoreCatalogProvider.getCatalog(txn, keySpace);
            txn.commit();
        }
    }

    @AfterEach
    void deleteAllRecords() throws RelationalException {
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {

            final KeySpacePath keySpacePath = RelationalKeyspaceProvider.toDatabasePath(URI.create("/__SYS"), keySpace).schemaPath("CATALOG");
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
            Schema schema1 = generateTestSchema("test_schema_name", "/TEST/test_database_id", templateName, templateVersion, true);
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
            Assertions.assertEquals(2, result.getTables().size());
            assertThat(result.getTables().stream().map(Metadata::getName).collect(Collectors.toSet()))
                    .containsExactlyInAnyOrder("test_table1", "test_table2");
            assertThat(result.getViews().stream().map(Metadata::getName).collect(Collectors.toSet()))
                    .containsExactlyInAnyOrder("test_view1", "test_view2");
            assertThat(result.getViews().stream().map(View::getDescription).collect(Collectors.toSet()))
                    .containsExactlyInAnyOrder("select * from test_table1", "select * from test_table2 where A = 'foo'");
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
            final var thrown = Assertions.assertThrows(RelationalException.class, () -> storeCatalog.saveSchema(txn, schema1, false));
            Assertions.assertEquals("Cannot create schema test_schema_name because schema template test_template_name version 1 does not exist.",
                    thrown.getMessage());
            Assertions.assertEquals(ErrorCode.UNKNOWN_SCHEMA_TEMPLATE, thrown.getErrorCode());
            txn.commit();
        }
    }

    @Test
    void testLoadSchemaWithoutTemplate() throws RelationalException {
        String templateName = "test_template_name";
        final var templateVersion = 1;
        // save record in FDB
        Schema schema1 = generateTestSchema("test_schema_name", "/TEST/test_database_id", templateName, templateVersion);
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.getSchemaTemplateCatalog().createTemplate(txn, schema1.getSchemaTemplate());
            storeCatalog.createDatabase(txn, URI.create(schema1.getDatabaseName()));
            storeCatalog.saveSchema(txn, schema1, false);
            txn.commit();
        }

        // delete the schema template.
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.getSchemaTemplateCatalog().deleteTemplate(txn, schema1.getSchemaTemplate().getName(), true);
            txn.commit();
        }

        // test loadSchema method with correct schema name
        try (Transaction loadTxn1 = new RecordContextTransaction(fdb.openContext())) {
            RelationalException exception = Assertions.assertThrows(RelationalException.class, () ->
                    storeCatalog.loadSchema(loadTxn1, URI.create("/TEST/test_database_id"), "test_schema_name"));
            Assertions.assertEquals(ErrorCode.UNKNOWN_SCHEMA_TEMPLATE, exception.getErrorCode());
        }
    }

    @Test
    void testRepairSchema() throws RelationalException {
        // save schema with template version 1L
        Schema schema1 = generateTestSchema("test_schema_name", "/TEST/test_database_id", "test_template_name", 1);
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.getSchemaTemplateCatalog().createTemplate(txn, schema1.getSchemaTemplate());
            storeCatalog.createDatabase(txn, URI.create(schema1.getDatabaseName()));
            storeCatalog.saveSchema(txn, schema1, false);
            txn.commit();
        }
        // save schema template with version 2
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            SchemaTemplate template2 = generateTestSchemaTemplate("test_template_name", 2);
            storeCatalog.getSchemaTemplateCatalog().createTemplate(txn, template2);
            txn.commit();
        }
        // repair schema
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.repairSchema(txn, "/TEST/test_database_id", "test_schema_name");
            txn.commit();
        }
        // load schema
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            Schema newSchema = storeCatalog.loadSchema(txn, URI.create("/TEST/test_database_id"), "test_schema_name");
            txn.commit();
            // template version should be the latest version
            Assertions.assertEquals(2L, newSchema.getSchemaTemplate().getVersion());
        }
    }

    @Test
    void loadSchemaFailsWithNonexistentDatabase() throws RelationalException {
        // test loadSchema method with a nonexistent database_id
        try (Transaction loadTxn2 = new RecordContextTransaction(fdb.openContext())) {
            RelationalException exception = Assertions.assertThrows(RelationalException.class, () ->
                    storeCatalog.loadSchema(loadTxn2, URI.create("/TEST/test_wrong_database_id"), "test_schema_name"));
            Assertions.assertEquals(ErrorCode.UNDEFINED_SCHEMA, exception.getErrorCode());
        }
    }

    @Test
    void loadSchemaFailsWithNOnExistentSchemaName() throws RelationalException {
        // test loadSchema method with a nonexistent schema
        try (Transaction loadTxn3 = new RecordContextTransaction(fdb.openContext())) {
            RelationalException exception2 = Assertions.assertThrows(RelationalException.class, () ->
                    storeCatalog.loadSchema(loadTxn3, URI.create("/TEST/test_database_id"), "test_wrong_schema_name"));
            Assertions.assertEquals(ErrorCode.UNDEFINED_SCHEMA, exception2.getErrorCode());
        }
    }

    @Test
    void testLoadSchemaWithCommittedTransaction() throws RelationalException {
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            txn.commit();
            RelationalException exception3 = Assertions.assertThrows(RelationalException.class, () ->
                    storeCatalog.loadSchema(txn, URI.create("/TEST/test_database_id"), "test_schema_name"));
            Assertions.assertEquals(ErrorCode.TRANSACTION_INACTIVE, exception3.getErrorCode());
        }
    }

    @Test
    void testLoadSchemaWithAbortedTransaction() throws RelationalException {
        // abort
        try (Transaction txn2 = new RecordContextTransaction(fdb.openContext())) {
            txn2.abort();
            RelationalException exception4 = Assertions.assertThrows(RelationalException.class, () ->
                    storeCatalog.loadSchema(txn2, URI.create("/TEST/test_database_id"), "test_schema_name"));
            Assertions.assertEquals(ErrorCode.TRANSACTION_INACTIVE, exception4.getErrorCode());
        }
    }

    @Test
    void testLoadSchemaWithClosedTransaction() throws RelationalException {
        // close
        Transaction txn3 = new RecordContextTransaction(fdb.openContext());
        txn3.close();
        RelationalException exception5 = Assertions.assertThrows(RelationalException.class, () ->
                storeCatalog.loadSchema(txn3, URI.create("/TEST/test_database_id"), "test_schema_name"));
        Assertions.assertEquals(ErrorCode.TRANSACTION_INACTIVE, exception5.getErrorCode());
    }

    @Test
    void testUpdateSchemaWithCommittedTransaction() throws RelationalException {
        Schema schema1 = generateTestSchema("test_schema_name", "/TEST/test_database_id", "test_template_name", 1);
        try (Transaction txn1 = new RecordContextTransaction(fdb.openContext())) {
            // committed
            txn1.commit();
            RelationalException exception1 = Assertions.assertThrows(RelationalException.class, () ->
                    storeCatalog.saveSchema(txn1, schema1, false));
            Assertions.assertEquals(ErrorCode.TRANSACTION_INACTIVE, exception1.getErrorCode());
        }
    }

    @Test
    void testUpdateSchemaWithAbortedTransaction() throws RelationalException {
        Schema schema1 = generateTestSchema("test_schema_name", "/TEST/test_database_id", "test_template_name", 1);
        try (Transaction txn2 = new RecordContextTransaction(fdb.openContext())) {
            // aborted
            txn2.abort();
            RelationalException exception2 = Assertions.assertThrows(RelationalException.class, () ->
                    storeCatalog.saveSchema(txn2, schema1, false));
            Assertions.assertEquals(ErrorCode.TRANSACTION_INACTIVE, exception2.getErrorCode());
        }
    }

    @Test
    void testUpdateSchemaWithClosedTransaction() throws RelationalException {
        Schema schema1 = generateTestSchema("test_schema_name", "/TEST/test_database_id", "test_template_name", 1);
        Transaction txn3 = new RecordContextTransaction(fdb.openContext());
        txn3.close();
        RelationalException exception3 = Assertions.assertThrows(RelationalException.class, () ->
                storeCatalog.saveSchema(txn3, schema1, false));
        Assertions.assertEquals(ErrorCode.TRANSACTION_INACTIVE, exception3.getErrorCode());
    }

    @Test
    void testUpdateSchemaWithTwoConsecutiveTransactions() throws RelationalException {
        // 2 schemas with different versions
        final Schema schema1 = generateTestSchema("test_schema_name", "/TEST/test_database_id", "test_template_name", 1);
        final Schema schema2 = generateTestSchema("test_schema_name", "/TEST/test_database_id", "test_template_name", 2);
        final SchemaTemplate template1 = generateTestSchemaTemplate("test_template_name", 1, true);
        final SchemaTemplate template2 = generateTestSchemaTemplate("test_template_name", 2, true);
        // test 2 successful consecutive transactions
        // update with schema1 (version = 1)
        try (Transaction txn1 = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.getSchemaTemplateCatalog().createTemplate(txn1, template1);
            storeCatalog.createDatabase(txn1, URI.create(schema1.getDatabaseName()));
            storeCatalog.saveSchema(txn1, schema1, false);
            // commit and close the write transaction
            txn1.commit();
        }
        // read after 1st transaction commit
        try (Transaction readTransaction1 = new RecordContextTransaction(fdb.openContext())) {
            Schema result1 = storeCatalog.loadSchema(readTransaction1, URI.create("/TEST/test_database_id"), "test_schema_name");
            // Assert result is correct
            Assertions.assertEquals("test_schema_name", result1.getName());
            Assertions.assertEquals("test_template_name", result1.getSchemaTemplate().getName());
            Assertions.assertEquals(1, result1.getSchemaTemplate().getVersion());

            assertThat(result1.getTables().stream().map(Metadata::getName).collect(Collectors.toSet()))
                    .containsExactlyInAnyOrder("test_table1", "test_table2");
            assertThat(result1.getViews().stream().map(Metadata::getName).collect(Collectors.toSet()))
                    .containsExactlyInAnyOrder("test_view1", "test_view2");
            assertThat(result1.getViews().stream().map(View::getDescription).collect(Collectors.toSet()))
                    .containsExactlyInAnyOrder("select * from test_table1", "select * from test_table2 where A = 'foo'");
        }
        // update with schema2 (version = 2)
        try (Transaction txn2 = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.getSchemaTemplateCatalog().createTemplate(txn2, template2);
            storeCatalog.saveSchema(txn2, schema2, false);
            txn2.commit();
        }

        // read after 2nd transaction
        try (Transaction readTransaction2 = new RecordContextTransaction(fdb.openContext())) {
            Schema result2 = storeCatalog.loadSchema(readTransaction2, URI.create("/TEST/test_database_id"), "test_schema_name");
            // Assert result is correct
            Assertions.assertEquals("test_schema_name", result2.getName());
            Assertions.assertEquals("test_template_name", result2.getSchemaTemplate().getName());
            Assertions.assertEquals(2, result2.getSchemaTemplate().getVersion());

            assertThat(result2.getTables().stream().map(Metadata::getName).collect(Collectors.toSet()))
                    .containsExactlyInAnyOrder("test_table1", "test_table2");
            assertThat(result2.getViews().stream().map(Metadata::getName).collect(Collectors.toSet()))
                    .containsExactlyInAnyOrder("test_view1", "test_view2");
            assertThat(result2.getViews().stream().map(View::getDescription).collect(Collectors.toSet()))
                    .containsExactlyInAnyOrder("select * from test_table1", "select * from test_table2 where A = 'foo'");
        }
    }

    @Test
    void testUpdateSchemaWithTwoSimultaneousTransactions() throws RelationalException {
        // 2 schemas with different versions
        final Schema schema1 = generateTestSchema("test_schema_name", "/TEST/test_database_id", "test_template_name", 1);
        final Schema schema2 = generateTestSchema("test_schema_name", "/TEST/test_database_id", "test_template_name", 2);

        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.createDatabase(txn, URI.create(schema1.getDatabaseName()));
            txn.commit();
        }

        // test 2 conflicting transactions
        try (Transaction txn3 = new RecordContextTransaction(fdb.openContext()); Transaction txn4 = new RecordContextTransaction(fdb.openContext())) {
            // update with 2 different schemas
            storeCatalog.getSchemaTemplateCatalog().createTemplate(txn3, schema1.getSchemaTemplate());
            storeCatalog.getSchemaTemplateCatalog().createTemplate(txn4, schema2.getSchemaTemplate());
            storeCatalog.saveSchema(txn3, schema1, false);
            storeCatalog.saveSchema(txn4, schema2, false);
            // commit the first write transaction
            txn3.commit();
            // assert that the second transaction couldn't be committed
            org.assertj.core.api.Assertions.assertThatThrownBy(txn4::commit)
                    .isInstanceOf(RelationalException.class)
                    .extracting("errorCode")
                    .isEqualTo(ErrorCode.SERIALIZATION_FAILURE);
        }
    }

    @Test
    void testUpdateSchemaWithBadSchema() throws RelationalException {
        // bad schema, schema_version must not be negative
        final Schema schema1 = generateTestSchema("test_schema_name", "/TEST/test_database_id", "test_template_name", -34);
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            RelationalException exception = Assertions.assertThrows(RelationalException.class, () ->
                    storeCatalog.saveSchema(txn, schema1, false));
            Assertions.assertEquals(ErrorCode.INVALID_PARAMETER, exception.getErrorCode());
            Assertions.assertEquals("Field schema_version cannot be < 0!", exception.getMessage());
        }
    }

    @Test
    void testCreateSchemaWithSchemaTemplateVersionZero() throws RelationalException {
        // bad schema, schema_version must not be negative
        final Schema schema1 = generateTestSchema("test_schema_name", "/TEST/test_database_id", "test_template_name", 0);
        final SchemaTemplate template1 = generateTestSchemaTemplate("test_template_name", 0);
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.getSchemaTemplateCatalog().createTemplate(txn, template1);
            storeCatalog.createDatabase(txn, URI.create(schema1.getDatabaseName()));
            storeCatalog.saveSchema(txn, schema1, false);
            txn.commit();
        }

        // read after transaction commit
        try (Transaction readTransaction1 = new RecordContextTransaction(fdb.openContext())) {
            Schema result1 = storeCatalog.loadSchema(readTransaction1, URI.create(schema1.getDatabaseName()), schema1.getName());
            // Assert template version is 0
            Assertions.assertEquals(0, result1.getSchemaTemplate().getVersion());
        }
    }
}
