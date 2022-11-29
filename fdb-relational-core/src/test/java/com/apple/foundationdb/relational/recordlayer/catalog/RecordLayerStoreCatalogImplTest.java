/*
 * RecordLayerStoreCatalogImplTest.java
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
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.KeySpaceExtension;
import com.apple.foundationdb.relational.recordlayer.KeySpaceUtils;
import com.apple.foundationdb.relational.recordlayer.RecordContextTransaction;
import com.apple.foundationdb.relational.recordlayer.query.TypingContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class RecordLayerStoreCatalogImplTest {
    private FDBDatabase fdb;
    private StoreCatalog storeCatalog;

    @RegisterExtension
    public static final KeySpaceExtension keySpaceExt = new KeySpaceExtension();

    @BeforeEach
    void setUpCatalog() throws RelationalException {
        fdb = FDBDatabaseFactory.instance().getDatabase();
        RecordLayerStoreCatalogImpl rlsci = new RecordLayerStoreCatalogImpl(keySpaceExt.getKeySpace());
        // create a FDBRecordStore
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            rlsci.initialize(txn);
            txn.commit();
        }
        storeCatalog = rlsci;
    }

    @AfterEach
    void deleteAllRecords() throws RelationalException {
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {

            final KeySpacePath keySpacePath = KeySpaceUtils.uriToPath(URI.create("/__SYS/CATALOG"), keySpaceExt.getKeySpace());
            FDBRecordStore.deleteStore(txn.unwrap(FDBRecordContext.class), keySpacePath);
            txn.commit();
        }
    }

    @Test
    void testLoadSchema() throws RelationalException {
        // save record in FDB
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            Schema schema1 = generateTestSchema("test_schema_name", "test_database_id", "test_template_name", 1, Arrays.asList("test_table1", "test_table2"));
            storeCatalog.createDatabase(txn, URI.create(schema1.getDatabaseId()));
            storeCatalog.updateSchema(txn, schema1);
            txn.commit();
        }

        // test loadSchema method with correct schema name
        try (Transaction loadTxn1 = new RecordContextTransaction(fdb.openContext())) {
            Schema result = storeCatalog.loadSchema(loadTxn1, URI.create("test_database_id"), "test_schema_name");
            Assertions.assertEquals("test_schema_name", result.getSchemaName());
            Assertions.assertEquals("test_template_name", result.getSchemaTemplateName());
            Assertions.assertEquals(1, result.getTemplateVersion());
            Assertions.assertEquals(2, result.getTableNames().size());
            org.assertj.core.api.Assertions.assertThat(result.getTableNames())
                    .containsExactlyInAnyOrder("test_table1", "test_table2");
        }
    }

    @Test
    void testDoesSchemaTemplateExist() throws RelationalException {
        // save record in FDB
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            SchemaTemplate template1 = generateTestSchemaTemplate("test_template_name", 1L);
            storeCatalog.updateSchemaTemplate(txn, template1);
            txn.commit();
        }

        // test doesSchemaTemplateExist method
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            boolean exist = storeCatalog.doesSchemaTemplateExist(txn, "test_template_name");
            Assertions.assertTrue(exist);
        }
    }

    @Test
    void testLoadSchemaTemplateLatestVersion() throws RelationalException {
        // save record in FDB
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            SchemaTemplate template1 = generateTestSchemaTemplate("test_template_name", 1L);
            // a new template with an unset version number, its version number will be set to 2L automatically
            SchemaTemplate template2 = generateTestSchemaTemplate("test_template_name", 0L);
            storeCatalog.updateSchemaTemplate(txn, template1);
            storeCatalog.updateSchemaTemplate(txn, template2);
            txn.commit();
        }

        // load latest version returns version = 2L
        try (Transaction loadTxn3 = new RecordContextTransaction(fdb.openContext())) {
            SchemaTemplate result = storeCatalog.loadSchemaTemplate(loadTxn3, "test_template_name");
            Assertions.assertEquals("test_template_name", result.getUniqueId());
            Assertions.assertEquals(2L, result.getVersion());
            Assertions.assertEquals(2, result.getTables().size());
            Set<String> resultTableNames = result.getTables().stream().map(TableInfo::getTableName).collect(Collectors.toSet());
            org.assertj.core.api.Assertions.assertThat(resultTableNames)
                    .containsExactlyInAnyOrder("test_table1", "test_table2");
        }
    }

    @Test
    void testLoadSchemaTemplate() throws RelationalException {
        // save record in FDB
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            SchemaTemplate template1 = generateTestSchemaTemplate("test_template_name", 1L);
            // a new template with an unset version number, its version number will be set to 2L automatically
            SchemaTemplate template2 = generateTestSchemaTemplate("test_template_name", 0L);
            storeCatalog.updateSchemaTemplate(txn, template1);
            storeCatalog.updateSchemaTemplate(txn, template2);
            txn.commit();
        }

        // check that both templates are stored. method loadSchemaTemplate is tested.
        try (Transaction loadTxn1 = new RecordContextTransaction(fdb.openContext())) {
            SchemaTemplate result = storeCatalog.loadSchemaTemplate(loadTxn1, "test_template_name", 1L);
            Assertions.assertEquals("test_template_name", result.getUniqueId());
            Assertions.assertEquals(1L, result.getVersion());
            Assertions.assertEquals(2, result.getTables().size());
            Set<String> resultTableNames = result.getTables().stream().map(TableInfo::getTableName).collect(Collectors.toSet());
            org.assertj.core.api.Assertions.assertThat(resultTableNames)
                    .containsExactlyInAnyOrder("test_table1", "test_table2");
        }

        try (Transaction loadTxn2 = new RecordContextTransaction(fdb.openContext())) {
            SchemaTemplate result = storeCatalog.loadSchemaTemplate(loadTxn2, "test_template_name", 2L);
            Assertions.assertEquals("test_template_name", result.getUniqueId());
            Assertions.assertEquals(2L, result.getVersion());
            Assertions.assertEquals(2, result.getTables().size());
            Set<String> resultTableNames = result.getTables().stream().map(TableInfo::getTableName).collect(Collectors.toSet());
            org.assertj.core.api.Assertions.assertThat(resultTableNames)
                    .containsExactlyInAnyOrder("test_table1", "test_table2");
        }
    }

    @Test
    void testRepairSchema() throws RelationalException {
        // save schema with template version 1L
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            Schema schema1 = generateTestSchema("test_schema_name", "test_database_id", "test_template_name", 1, Arrays.asList("test_table1", "test_table2"));
            storeCatalog.createDatabase(txn, URI.create(schema1.getDatabaseId()));
            storeCatalog.updateSchema(txn, schema1);
            txn.commit();
        }
        // save schema template with version 1L and 2L
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            SchemaTemplate template1 = generateTestSchemaTemplate("test_template_name", 1L);
            // a new template with an unset version number, its version number will be set to 2L automatically
            SchemaTemplate template2 = generateTestSchemaTemplate("test_template_name", 0L);
            storeCatalog.updateSchemaTemplate(txn, template1);
            storeCatalog.updateSchemaTemplate(txn, template2);
            txn.commit();
        }
        // repair schema
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.repairSchema(txn, "test_database_id", "test_schema_name");
            txn.commit();
        }
        // load schema
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            Schema newSchema = storeCatalog.loadSchema(txn, URI.create("test_database_id"), "test_schema_name");
            txn.commit();
            // template version should be the latest version
            Assertions.assertEquals(newSchema.getTemplateVersion(), 2L);
        }
    }

    @Test
    void loadSchemaFailsWithNonexistentDatabase() throws RelationalException {
        // test loadSchema method with a nonexistent database_id
        try (Transaction loadTxn2 = new RecordContextTransaction(fdb.openContext())) {
            RelationalException exception = Assertions.assertThrows(RelationalException.class, () ->
                    storeCatalog.loadSchema(loadTxn2, URI.create("test_wrong_database_id"), "test_schema_name"));
            Assertions.assertEquals(ErrorCode.UNDEFINED_SCHEMA, exception.getErrorCode());
        }
    }

    @Test
    void loadSchemaFailsWithNOnExistentSchemaName() throws RelationalException {
        // test loadSchema method with a nonexistent schema
        try (Transaction loadTxn3 = new RecordContextTransaction(fdb.openContext())) {
            RelationalException exception2 = Assertions.assertThrows(RelationalException.class, () ->
                    storeCatalog.loadSchema(loadTxn3, URI.create("test_database_id"), "test_wrong_schema_name"));
            Assertions.assertEquals(ErrorCode.UNDEFINED_SCHEMA, exception2.getErrorCode());
        }
    }

    @Test
    void listAllTheSchemas() throws RelationalException, SQLException {
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            final RelationalResultSet relationalResultSet = storeCatalog.listSchemas(txn, Continuation.BEGIN);
            int schemaCount = 0;
            String schemaName = "";
            while (relationalResultSet.next()) {
                schemaCount += 1;
                schemaName = relationalResultSet.getString("SCHEMA_NAME");
            }
            Assertions.assertEquals(1, schemaCount);
            Assertions.assertEquals("CATALOG", schemaName);
        }
    }

    @Test
    void testLoadSchemaWithCommittedTransaction() throws RelationalException {
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            // commit
            txn.commit();
            RelationalException exception3 = Assertions.assertThrows(RelationalException.class, () ->
                    storeCatalog.loadSchema(txn, URI.create("test_database_id"), "test_schema_name"));
            Assertions.assertEquals(ErrorCode.TRANSACTION_INACTIVE, exception3.getErrorCode());
        }
    }

    @Test
    void testLoadSchemaWithAbortedTransaction() throws RelationalException {
        // abort
        try (Transaction txn2 = new RecordContextTransaction(fdb.openContext())) {
            txn2.abort();
            RelationalException exception4 = Assertions.assertThrows(RelationalException.class, () ->
                    storeCatalog.loadSchema(txn2, URI.create("test_database_id"), "test_schema_name"));
            Assertions.assertEquals(ErrorCode.TRANSACTION_INACTIVE, exception4.getErrorCode());
        }
    }

    @Test
    void testLoadSchemaWithClosedTransaction() throws RelationalException {
        // close
        Transaction txn3 = new RecordContextTransaction(fdb.openContext());
        txn3.close();
        RelationalException exception5 = Assertions.assertThrows(RelationalException.class, () ->
                storeCatalog.loadSchema(txn3, URI.create("test_database_id"), "test_schema_name"));
        Assertions.assertEquals(ErrorCode.TRANSACTION_INACTIVE, exception5.getErrorCode());
    }

    @Test
    void testUpdateSchemaWithCommittedTransaction() throws RelationalException {
        Schema schema1 = generateTestSchema("test_schema_name", "test_database_id", "test_template_name", 1, Arrays.asList("test_table1", "test_table2"));
        try (Transaction txn1 = new RecordContextTransaction(fdb.openContext())) {
            // committed
            txn1.commit();
            RelationalException exception1 = Assertions.assertThrows(RelationalException.class, () ->
                    storeCatalog.updateSchema(txn1, schema1));
            Assertions.assertEquals(ErrorCode.TRANSACTION_INACTIVE, exception1.getErrorCode());
        }
    }

    @Test
    void testUpdateSchemaWithAbortedTransaction() throws RelationalException {
        Schema schema1 = generateTestSchema("test_schema_name", "test_database_id", "test_template_name", 1, Arrays.asList("test_table1", "test_table2"));
        try (Transaction txn2 = new RecordContextTransaction(fdb.openContext())) {
            // aborted
            txn2.abort();
            RelationalException exception2 = Assertions.assertThrows(RelationalException.class, () ->
                    storeCatalog.updateSchema(txn2, schema1));
            Assertions.assertEquals(ErrorCode.TRANSACTION_INACTIVE, exception2.getErrorCode());
        }
    }

    @Test
    void testUpdateSchemaWithClosedTransaction() throws RelationalException {
        Schema schema1 = generateTestSchema("test_schema_name", "test_database_id", "test_template_name", 1, Arrays.asList("test_table1", "test_table2"));
        Transaction txn3 = new RecordContextTransaction(fdb.openContext());
        txn3.close();
        RelationalException exception3 = Assertions.assertThrows(RelationalException.class, () ->
                storeCatalog.updateSchema(txn3, schema1));
        Assertions.assertEquals(ErrorCode.TRANSACTION_INACTIVE, exception3.getErrorCode());
    }

    @Test
    void testUpdateSchemaWithTwoConsecutiveTransactions() throws RelationalException {
        // 2 schemas with different versions
        final Schema schema1 = generateTestSchema("test_schema_name", "test_database_id", "test_template_name", 1, Arrays.asList("test_table1", "test_table2"));
        final Schema schema2 = generateTestSchema("test_schema_name", "test_database_id", "test_template_name", 2, Arrays.asList("test_table1", "test_table2"));

        // test 2 successful consecutive transactions
        // update with schema1 (version = 1)
        boolean updateSuccess1;
        try (Transaction txn1 = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.createDatabase(txn1, URI.create(schema1.getDatabaseId()));
            updateSuccess1 = storeCatalog.updateSchema(txn1, schema1);
            // commit and close the write transaction
            txn1.commit();
        }
        // read after 1st transaction commit
        try (Transaction readTransaction1 = new RecordContextTransaction(fdb.openContext())) {
            Schema result1 = storeCatalog.loadSchema(readTransaction1, URI.create("test_database_id"), "test_schema_name");
            // Assert result is correct
            Assertions.assertTrue(updateSuccess1);
            Assertions.assertEquals("test_schema_name", result1.getSchemaName());
            Assertions.assertEquals("test_template_name", result1.getSchemaTemplateName());
            Assertions.assertEquals(1, result1.getTemplateVersion());

            org.assertj.core.api.Assertions.assertThat(result1.getTableNames())
                    .containsExactlyInAnyOrder("test_table1", "test_table2");
        }
        // update with schema2 (version = 2)
        boolean updateSuccess2;
        try (Transaction txn2 = new RecordContextTransaction(fdb.openContext())) {
            updateSuccess2 = storeCatalog.updateSchema(txn2, schema2);
            txn2.commit();
        }

        // read after 2nd transaction
        try (Transaction readTransaction2 = new RecordContextTransaction(fdb.openContext())) {
            Schema result2 = storeCatalog.loadSchema(readTransaction2, URI.create("test_database_id"), "test_schema_name");
            // Assert result is correct
            Assertions.assertTrue(updateSuccess2);
            Assertions.assertEquals("test_schema_name", result2.getSchemaName());
            Assertions.assertEquals("test_template_name", result2.getSchemaTemplateName());
            Assertions.assertEquals(2, result2.getTemplateVersion());

            org.assertj.core.api.Assertions.assertThat(result2.getTableNames())
                    .containsExactlyInAnyOrder("test_table1", "test_table2");
        }
    }

    @Test
    void testUpdateSchemaWithTwoSimultaneousTransactions() throws RelationalException {
        // 2 schemas with different versions
        final Schema schema1 = generateTestSchema("test_schema_name", "test_database_id", "test_template_name", 1, Arrays.asList("test_table1", "test_table2"));
        final Schema schema2 = generateTestSchema("test_schema_name", "test_database_id", "test_template_name", 2, Arrays.asList("test_table1", "test_table2"));

        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.createDatabase(txn, URI.create(schema1.getDatabaseId()));
            txn.commit();
        }

        // test 2 conflicting transactions
        try (Transaction txn3 = new RecordContextTransaction(fdb.openContext()); Transaction txn4 = new RecordContextTransaction(fdb.openContext())) {
            // update with 2 different schemas
            storeCatalog.updateSchema(txn3, schema1);
            storeCatalog.updateSchema(txn4, schema2);
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
        // bad schema, schema_version must not be 0
        final Schema schema1 = generateTestSchema("test_schema_name", "test_database_id", "test_template_name", 0, Arrays.asList("test_table1", "test_table2"));
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            RelationalException exception = Assertions.assertThrows(RelationalException.class, () ->
                    storeCatalog.updateSchema(txn, schema1));
            Assertions.assertEquals(ErrorCode.INVALID_PARAMETER, exception.getErrorCode());
            Assertions.assertEquals("Field schema_version in Schema must be set, and must be > 0!", exception.getMessage());
        }
    }

    @Test
    void testListDatabases() throws RelationalException, SQLException {
        // save 2 schemas
        final Schema schema1 = generateTestSchema("test_schema_name1", "test_database_id1", "test_template_name", 1, Arrays.asList("test_table1", "test_table2"));
        final Schema schema2 = generateTestSchema("test_schema_name2", "test_database_id2", "test_template_name", 1, Arrays.asList("test_table3", "test_table4"));
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.createDatabase(txn, URI.create(schema1.getDatabaseId()));
            storeCatalog.createDatabase(txn, URI.create(schema2.getDatabaseId()));
            storeCatalog.updateSchema(txn, schema1);
            storeCatalog.updateSchema(txn, schema2);
            txn.commit();
        }
        // list databases
        Set<String> databases = new HashSet<>();
        try (Transaction listTxn = new RecordContextTransaction(fdb.openContext())) {
            Continuation continuation = Continuation.BEGIN;
            do {
                RelationalResultSet result = storeCatalog.listDatabases(listTxn, continuation);
                while (result.next()) {
                    databases.add(result.getString("DATABASE_ID"));
                }
                continuation = result.getContinuation();
            } while (!continuation.atEnd());
        }
        Assertions.assertEquals(3, databases.size());
        Assertions.assertTrue(databases.contains("test_database_id1"));
        Assertions.assertTrue(databases.contains("test_database_id2"));
        Assertions.assertTrue(databases.contains("/__SYS"));
    }

    @Test
    void testDoesDatabaseExist() throws RelationalException {
        final Schema schema1 = generateTestSchema("test_schema_name1", "/test_database_id1", "test_template_name", 1, Arrays.asList("test_table1", "test_table2"));
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.createDatabase(txn, URI.create(schema1.getDatabaseId()));
            storeCatalog.updateSchema(txn, schema1);
            txn.commit();
        }

        //check if it exists
        try (Transaction listTxn = new RecordContextTransaction(fdb.openContext())) {
            Assertions.assertTrue(storeCatalog.doesDatabaseExist(listTxn, URI.create(schema1.getDatabaseId())), "Did not find a database!");
        }
    }

    @Test
    void testDeleteDatabaseWorks() throws RelationalException {
        final Schema schema1 = generateTestSchema("test_schema_name1", "/test_database_id1", "test_template_name", 1, Arrays.asList("test_table1", "test_table2"));
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.createDatabase(txn, URI.create(schema1.getDatabaseId()));
            storeCatalog.updateSchema(txn, schema1);
            txn.commit();
        }

        //it should exist
        try (Transaction listTxn = new RecordContextTransaction(fdb.openContext())) {
            Assertions.assertTrue(storeCatalog.doesDatabaseExist(listTxn, URI.create(schema1.getDatabaseId())), "Did not find a database!");
        }
        //now delete it
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            storeCatalog.deleteDatabase(txn, URI.create(schema1.getDatabaseId()));
            txn.commit();
        }

        //now it shouldn't exist
        try (Transaction listTxn = new RecordContextTransaction(fdb.openContext())) {
            Assertions.assertFalse(storeCatalog.doesDatabaseExist(listTxn, URI.create(schema1.getDatabaseId())), "Did not find a database!");
        }
    }

    @Test
    void testListSchemas() throws RelationalException, SQLException {
        int n = 24;
        // save schemas
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            for (int i = 0; i < n; i++) {
                Schema schema = generateTestSchema("test_schema_name" + i, "test_database_id" + i / 2, "test_template_name", 1, List.of("test_table" + i));
                storeCatalog.createDatabase(txn, URI.create(schema.getDatabaseId()));
                storeCatalog.updateSchema(txn, schema);
            }
            txn.commit();
        }
        // list all schemas
        Set<String> fullSchemaNames = new HashSet<>();
        try (Transaction listTxn = new RecordContextTransaction(fdb.openContext())) {
            Continuation continuation = Continuation.BEGIN;
            do {
                try (RelationalResultSet result = storeCatalog.listSchemas(listTxn, continuation)) {
                    // to test continuation, only read 1 result at once
                    if (result.next()) {
                        fullSchemaNames.add(result.getString("DATABASE_ID") + "/" + result.getString("SCHEMA_NAME"));
                    }
                    continuation = result.getContinuation();
                }
            } while (!continuation.atEnd());
        }
        Assertions.assertEquals(n + 1, fullSchemaNames.size());
        for (int i = 0; i < n; i++) {
            Assertions.assertTrue(fullSchemaNames.contains("test_database_id" + i / 2 + "/test_schema_name" + i));
        }
        //should also contain the sys catalog schema
        Assertions.assertTrue(fullSchemaNames.contains("/__SYS/CATALOG"));
        // list schemas of 1 database
        Set<String> resultSet = new HashSet<>();
        try (Transaction listTxn = new RecordContextTransaction(fdb.openContext())) {
            Continuation continuation = Continuation.BEGIN;
            do {
                try (RelationalResultSet result = storeCatalog.listSchemas(listTxn, URI.create("test_database_id1"), continuation)) {
                    if (result.next()) {
                        resultSet.add(result.getString("DATABASE_ID") + "/" + result.getString("SCHEMA_NAME"));
                    }
                    continuation = result.getContinuation();
                }
            } while (!continuation.atEnd());
        }
        Assertions.assertEquals(2, resultSet.size());
        Assertions.assertTrue(resultSet.contains("test_database_id1/test_schema_name2"));
        Assertions.assertTrue(resultSet.contains("test_database_id1/test_schema_name3"));
    }

    @Test
    void testListSchemasEmptyResult() throws RelationalException, SQLException {
        // list all schemas
        Set<String> fullSchemaNames = new HashSet<>();
        try (Transaction listTxn = new RecordContextTransaction(fdb.openContext())) {
            Continuation continuation = Continuation.BEGIN;
            do {
                try (RelationalResultSet result = storeCatalog.listSchemas(listTxn, continuation)) {
                    if (result.next()) {
                        fullSchemaNames.add(result.getString("DATABASE_ID") + "/" + result.getString("SCHEMA_NAME"));
                    }
                    continuation = result.getContinuation();
                }
            } while (!continuation.atEnd());
        }
        // assert
        Assertions.assertEquals(1, fullSchemaNames.size());
        //the only entry should be the catalog
        Assertions.assertTrue(fullSchemaNames.contains("/__SYS/CATALOG"));
    }

    @SuppressWarnings({"SameParameterValue", "unused"})
    private Schema generateTestSchema(String schemaName,
                                      String databaseId,
                                      String schemaTemplateName,
                                      int schemaVersion,
                                      List<String> tableNames) throws RelationalException {
        final SchemaTemplate template = generateTestSchemaTemplate(schemaTemplateName, 1L);
        Schema genSchema = template.generateSchema(databaseId, schemaName);
        //set the schema version
        return new Schema(genSchema.getDatabaseId(), genSchema.getSchemaName(), genSchema.getMetaData(), genSchema.getSchemaTemplateName(), schemaVersion);
    }

    private SchemaTemplate generateTestSchemaTemplate(String schemaTemplateName, long version) {
        TypingContext ctx = TypingContext.create();

        TypingContext.FieldDefinition aField = new TypingContext.FieldDefinition("A", Type.TypeCode.STRING, null, false);
        ctx.addType(new TypingContext.TypeDefinition("test_table1", List.of(aField), true, List.of(List.of("A"))));

        ctx.addType(new TypingContext.TypeDefinition("test_table2", List.of(aField), true, List.of(List.of("A"))));

        ctx.addAllToTypeRepository();
        return ctx.generateSchemaTemplate(schemaTemplateName, version);
    }
}
