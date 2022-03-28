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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.generated.CatalogData;
import com.apple.foundationdb.relational.recordlayer.RecordContextTransaction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.Arrays;
import java.util.List;

public class RecordLayerStoreCatalogImplTest {
    private FDBDatabase fdb;
    private StoreCatalog storeCatalog;

    @BeforeEach
    void setUpCatalog() throws RelationalException {
        fdb = FDBDatabaseFactory.instance().getDatabase();
        storeCatalog = new RecordLayerStoreCatalogImpl(new KeySpace(new KeySpaceDirectory("catalog", KeySpaceDirectory.KeyType.NULL)));
        // create a FDBRecordStore
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            final RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(CatalogData.getDescriptor());
            builder.getRecordType("Schema").setPrimaryKey(Key.Expressions.concatenateFields("database_id", "schema_name"));

            FDBRecordStore.newBuilder()
                    .setKeySpacePath(new KeySpace(new KeySpaceDirectory("catalog", KeySpaceDirectory.KeyType.NULL)).path("catalog"))
                    .setMetaDataProvider(builder)
                    .setContext(txn.unwrap(FDBRecordContext.class))
                    .createOrOpen();
            txn.commit();
        }
    }

    @Test
    void testLoadSchema() throws RelationalException {
        // save record in FDB
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            CatalogData.Schema schema1 = generateTestSchema("test_schema_name", "test_database_id", "test_template_name", 1, Arrays.asList("test_table1", "test_table2"));
            storeCatalog.updateSchema(txn, schema1);
            txn.commit();
        }

        // test loadSchema method with correct schema name
        try (Transaction loadTxn1 = new RecordContextTransaction(fdb.openContext())) {
            CatalogData.Schema result = storeCatalog.loadSchema(loadTxn1, URI.create("test_database_id"), "test_schema_name");
            Assertions.assertEquals("test_schema_name", result.getSchemaName());
            Assertions.assertEquals("test_template_name", result.getSchemaTemplateName());
            Assertions.assertEquals(1, result.getSchemaVersion());
            Assertions.assertEquals(2, result.getTablesList().size());
            Assertions.assertEquals("test_table1", result.getTables(0).getName());
            Assertions.assertEquals("test_table2", result.getTables(1).getName());
        }

        // test loadSchema method with a not existed database_id
        try (Transaction loadTxn2 = new RecordContextTransaction(fdb.openContext())) {
            RelationalException exception = Assertions.assertThrows(RelationalException.class, () -> {
                storeCatalog.loadSchema(loadTxn2, URI.create("test_wrong_database_id"), "test_schema_name");
            });
            Assertions.assertEquals("Primary key (\"test_wrong_database_id\", \"test_schema_name\") not existed in Catalog!", exception.getMessage());
            Assertions.assertEquals(ErrorCode.SCHEMA_NOT_FOUND, exception.getErrorCode());
        }

        // test loadSchema method with a not existed database_id
        try (Transaction loadTxn3 = new RecordContextTransaction(fdb.openContext())) {
            RelationalException exception2 = Assertions.assertThrows(RelationalException.class, () -> {
                storeCatalog.loadSchema(loadTxn3, URI.create("test_database_id"), "test_wrong_schema_name");
            });
            Assertions.assertEquals("Primary key (\"test_database_id\", \"test_wrong_schema_name\") not existed in Catalog!", exception2.getMessage());
            Assertions.assertEquals(ErrorCode.SCHEMA_NOT_FOUND, exception2.getErrorCode());
        }
    }

    @Test
    void testLoadSchemaWithCommittedTransaction() throws RelationalException {
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            // commit
            txn.commit();
            RelationalException exception3 = Assertions.assertThrows(RelationalException.class, () -> {
                storeCatalog.loadSchema(txn, URI.create("test_database_id"), "test_schema_name");
            });
            Assertions.assertEquals(ErrorCode.TRANSACTION_INACTIVE, exception3.getErrorCode());
        }
    }

    @Test
    void testLoadSchemaWithAbortedTransaction() throws RelationalException {
        // abort
        try (Transaction txn2 = new RecordContextTransaction(fdb.openContext())) {
            txn2.abort();
            RelationalException exception4 = Assertions.assertThrows(RelationalException.class, () -> {
                storeCatalog.loadSchema(txn2, URI.create("test_database_id"), "test_schema_name");
            });
            Assertions.assertEquals(ErrorCode.TRANSACTION_INACTIVE, exception4.getErrorCode());
        }
    }

    @Test
    void testLoadSchemaWithClosedTransaction() throws RelationalException {
        // close
        Transaction txn3 = new RecordContextTransaction(fdb.openContext());
        txn3.close();
        RelationalException exception5 = Assertions.assertThrows(RelationalException.class, () -> {
            storeCatalog.loadSchema(txn3, URI.create("test_database_id"), "test_schema_name");
        });
        Assertions.assertEquals(ErrorCode.TRANSACTION_INACTIVE, exception5.getErrorCode());
    }

    @Test
    void testUpdateSchemaWithCommittedTransaction() throws RelationalException {
        CatalogData.Schema schema1 = generateTestSchema("test_schema_name", "test_database_id", "test_template_name", 1, Arrays.asList("test_table1", "test_table2"));
        try (Transaction txn1 = new RecordContextTransaction(fdb.openContext())) {
            // committed
            txn1.commit();
            RelationalException exception1 = Assertions.assertThrows(RelationalException.class, () -> {
                storeCatalog.updateSchema(txn1, schema1);
            });
            Assertions.assertEquals(ErrorCode.TRANSACTION_INACTIVE, exception1.getErrorCode());
        }
    }

    @Test
    void testUpdateSchemaWithAbortedTransaction() throws RelationalException {
        CatalogData.Schema schema1 = generateTestSchema("test_schema_name", "test_database_id", "test_template_name", 1, Arrays.asList("test_table1", "test_table2"));
        try (Transaction txn2 = new RecordContextTransaction(fdb.openContext())) {
            // aborted
            txn2.abort();
            RelationalException exception2 = Assertions.assertThrows(RelationalException.class, () -> {
                storeCatalog.updateSchema(txn2, schema1);
            });
            Assertions.assertEquals(ErrorCode.TRANSACTION_INACTIVE, exception2.getErrorCode());
        }
    }

    @Test
    void testUpdateSchemaWithClosedTransaction() throws RelationalException {
        CatalogData.Schema schema1 = generateTestSchema("test_schema_name", "test_database_id", "test_template_name", 1, Arrays.asList("test_table1", "test_table2"));
        Transaction txn3 = new RecordContextTransaction(fdb.openContext());
        txn3.close();
        RelationalException exception3 = Assertions.assertThrows(RelationalException.class, () -> {
            storeCatalog.updateSchema(txn3, schema1);
        });
        Assertions.assertEquals(ErrorCode.TRANSACTION_INACTIVE, exception3.getErrorCode());
    }

    @Test
    void testUpdateSchemaWithTwoConsecutiveTransactions() throws RelationalException {
        // 2 schemas with different versions
        final CatalogData.Schema schema1 = generateTestSchema("test_schema_name", "test_database_id", "test_template_name", 1, Arrays.asList("test_table1", "test_table2"));
        final CatalogData.Schema schema2 = generateTestSchema("test_schema_name", "test_database_id", "test_template_name", 2, Arrays.asList("test_table1", "test_table2"));

        // test 2 successful consecutive transactions
        // update with schema1 (version = 1)
        boolean updateSuccess1;
        try (Transaction txn1 = new RecordContextTransaction(fdb.openContext())) {
            updateSuccess1 = storeCatalog.updateSchema(txn1, schema1);
            // commit and close the write transaction
            txn1.commit();
        }
        // read after 1st transaction commit
        try (Transaction readTransaction1 = new RecordContextTransaction(fdb.openContext())) {
            CatalogData.Schema result1 = storeCatalog.loadSchema(readTransaction1, URI.create("test_database_id"), "test_schema_name");
            // Assert result is correct
            Assertions.assertTrue(updateSuccess1);
            Assertions.assertEquals("test_schema_name", result1.getSchemaName());
            Assertions.assertEquals("test_template_name", result1.getSchemaTemplateName());
            Assertions.assertEquals(1, result1.getSchemaVersion());
            Assertions.assertEquals(2, result1.getTablesList().size());
            Assertions.assertEquals("test_table1", result1.getTables(0).getName());
            Assertions.assertEquals("test_table2", result1.getTables(1).getName());
        }

        // update with schema2 (version = 2)
        boolean updateSuccess2;
        try (Transaction txn2 = new RecordContextTransaction(fdb.openContext())) {
            updateSuccess2 = storeCatalog.updateSchema(txn2, schema2);
            txn2.commit();
        }
        // read after 2nd transaction
        try (Transaction readTransaction2 = new RecordContextTransaction(fdb.openContext())) {
            CatalogData.Schema result2 = storeCatalog.loadSchema(readTransaction2, URI.create("test_database_id"), "test_schema_name");
            // Assert result is correct
            Assertions.assertTrue(updateSuccess2);
            Assertions.assertEquals("test_schema_name", result2.getSchemaName());
            Assertions.assertEquals("test_template_name", result2.getSchemaTemplateName());
            Assertions.assertEquals(2, result2.getSchemaVersion());
            Assertions.assertEquals(2, result2.getTablesList().size());
            Assertions.assertEquals("test_table1", result2.getTables(0).getName());
            Assertions.assertEquals("test_table2", result2.getTables(1).getName());
        }
    }

    @Test
    void testUpdateSchemaWithTwoSimultaneousTransactions() throws RelationalException {
        // 2 schemas with different versions
        final CatalogData.Schema schema1 = generateTestSchema("test_schema_name", "test_database_id", "test_template_name", 1, Arrays.asList("test_table1", "test_table2"));
        final CatalogData.Schema schema2 = generateTestSchema("test_schema_name", "test_database_id", "test_template_name", 2, Arrays.asList("test_table1", "test_table2"));

        // test 2 conflicting transactions
        try (Transaction txn3 = new RecordContextTransaction(fdb.openContext()); Transaction txn4 = new RecordContextTransaction(fdb.openContext())) {
            // update with 2 different schemas
            storeCatalog.updateSchema(txn3, schema1);
            storeCatalog.updateSchema(txn4, schema2);
            // commit the first write transaction
            txn3.commit();
            // assert that the second transaction couldn't be committed
            FDBExceptions.FDBStoreTransactionConflictException exception = Assertions.assertThrows(FDBExceptions.FDBStoreTransactionConflictException.class, () -> {
                txn4.commit();
            });
            Assertions.assertEquals("Transaction not committed due to conflict with another transaction", exception.getMessage());
        }
    }

    @Test
    void testUpdateSchemaWithBadSchema() throws RelationalException {
        // bad schema, schema_version must not be 0
        final CatalogData.Schema schema1 = generateTestSchema("test_schema_name", "test_database_id", "test_template_name", 0, Arrays.asList("test_table1", "test_table2"));
        try (Transaction txn = new RecordContextTransaction(fdb.openContext())) {
            RelationalException exception = Assertions.assertThrows(RelationalException.class, () -> {
                storeCatalog.updateSchema(txn, schema1);
            });
            Assertions.assertEquals(ErrorCode.INVALID_PARAMETER, exception.getErrorCode());
            Assertions.assertEquals("Field schema_version in Schema must be set, and must not be 0!", exception.getMessage());
        }
    }

    private CatalogData.Schema generateTestSchema(String schemaName, String databaseId, String schemaTemplateName, int schemaVersion, List<String> tableNames) {
        CatalogData.Schema.Builder schemaBuilder = CatalogData.Schema.newBuilder()
                .setSchemaName(schemaName)
                .setDatabaseId(databaseId)
                .setSchemaTemplateName(schemaTemplateName)
                .setSchemaVersion(schemaVersion);
        for (String tableName : tableNames) {
            schemaBuilder.addTables(CatalogData.Table.newBuilder().setName(tableName).build());
        }
        return schemaBuilder.build();
    }
}
