/*
 * KeySpacePathImportDataTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.keyspace;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory.KeyType;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link KeySpacePath#importData(FDBRecordContext, Iterable)}.
 */
@Tag(Tags.RequiresFDB)
class KeySpacePathImportDataTest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    private FDBDatabase database;

    @BeforeEach
    void setUp() {
        database = dbExtension.getDatabase();
    }

    @Test
    void importBasicData() {
        // Test importing basic data into a simple keyspace path
        // Should validate that data is correctly stored using logical values
        // rather than raw keys, allowing import across different clusters
        final String companyUuid = UUID.randomUUID().toString();
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("company", KeyType.STRING, companyUuid)
                        .addSubdirectory(new KeySpaceDirectory("department", KeyType.STRING)
                                .addSubdirectory(new KeySpaceDirectory("employee_id", KeyType.LONG))));

        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();

            // Create some test data to export and then import
            KeySpacePath deptPath = root.path("company").add("department", "engineering");
            KeySpacePath emp1Path = deptPath.add("employee_id", 100L);
            KeySpacePath emp2Path = deptPath.add("employee_id", 200L);

            // Store original data with remainder elements
            byte[] key1 = emp1Path.toSubspace(context).pack(Tuple.from("profile", "name"));
            byte[] value1 = Tuple.from("John Doe").pack();
            byte[] key2 = emp2Path.toSubspace(context).pack(Tuple.from("profile", "name"));
            byte[] value2 = Tuple.from("Jane Smith").pack();

            tr.set(key1, value1);
            tr.set(key2, value2);
            context.commit();
        }

        // Export the data
        final List<DataInKeySpacePath> exportedData = getExportedData(database, root.path("company").add("department", "engineering"));

        // Clear the data and import it back
        clearPath(database, root.path("company"));

        // Import the data
        importData(database, root.path("company").add("department", "engineering"), exportedData);

        // Verify the data was imported correctly
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath emp1Path = root.path("company").add("department", "engineering").add("employee_id", 100L);
            KeySpacePath emp2Path = root.path("company").add("department", "engineering").add("employee_id", 200L);

            byte[] key1 = emp1Path.toSubspace(context).pack(Tuple.from("profile", "name"));
            byte[] key2 = emp2Path.toSubspace(context).pack(Tuple.from("profile", "name"));

            Tuple value1 = getTuple(context, key1);
            Tuple value2 = getTuple(context, key2);

            assertEquals("John Doe", value1.getString(0));
            assertEquals("Jane Smith", value2.getString(0));
        }
    }

    @Test
    void importEmptyData() {
        // Test importing an empty collection of data
        // Should complete successfully without modifying the data under the path
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("test", KeyType.STRING, UUID.randomUUID().toString()));

        importData(database, root.path("test"), Collections.emptyList()); // should not throw any exception

        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath testPath = root.path("test");
            assertTrue(
                    testPath.exportAllData(context, null, ScanProperties.FORWARD_SCAN).first().join().isEmpty(),
                    "There should not have been any data created");
        }

    }

    @Test
    void importOverwriteExistingData() {
        // Test importing data that overwrites existing keys
        // Should verify that new data replaces old data when keys match
        final String rootUuid = UUID.randomUUID().toString();
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.STRING, rootUuid)
                        .addSubdirectory(new KeySpaceDirectory("data", KeyType.LONG)));

        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            
            // Set initial data
            KeySpacePath dataPath = root.path("root").add("data", 1L);
            final Subspace subspace = dataPath.toSubspace(context);
            byte[] key = subspace.pack(Tuple.from("record"));
            tr.set(key, Tuple.from("original_value").pack());
            tr.set(subspace.pack("other"), Tuple.from("other_value").pack());
            context.commit();
        }

        // Create import data with same key but different value
        List<DataInKeySpacePath> importData = new ArrayList<>();
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath dataPath = root.path("root").add("data", 1L);
            byte[] key = dataPath.toSubspace(context).pack(Tuple.from("record"));
            KeyValue kv = new KeyValue(key, Tuple.from("new_value").pack());
            importData.add(new DataInKeySpacePath(root.path("root"), kv, context));
        }

        // Verify we can re-import the data multiple times
        importData(database, root.path("root"), importData);
        importData(database, root.path("root"), importData);
        importData(database, root.path("root"), importData);

        // Verify the data was overwritten
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath dataPath = root.path("root").add("data", 1L);
            final Subspace subspace = dataPath.toSubspace(context);
            byte[] key = subspace.pack(Tuple.from("record"));
            assertEquals(Tuple.from("new_value"), getTuple(context, key));
            assertEquals(Tuple.from("other_value"), getTuple(context, subspace.pack("other")));
        }
    }

    @Test
    void importDataWithConstantValues() {
        // Test importing data into a keyspace with constant directory values
        // Should verify that constant values are handled correctly during import
        final String appUuid = UUID.randomUUID().toString();
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("application", KeyType.STRING, appUuid)
                        .addSubdirectory(new KeySpaceDirectory("version", KeyType.LONG, 1L)
                                .addSubdirectory(new KeySpaceDirectory("data", KeyType.STRING))));


        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            
            // Create test data using constant values
            KeySpacePath dataPath = root.path("application").add("version").add("data", "config");
            byte[] key = dataPath.toSubspace(context).pack(Tuple.from("setting"));
            tr.set(key, Tuple.from("value1").pack());
            context.commit();
        }

        // Export and import the data
        final List<DataInKeySpacePath> exportedData = getExportedData(database, root.path("application"));

        // Clear and import
        clearPath(database, root.path("application"));

        importData(database, root.path("application"), exportedData);

        // Verify the data
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath dataPath = root.path("application").add("version").add("data", "config");
            byte[] key = dataPath.toSubspace(context).pack(Tuple.from("setting"));
            Tuple value = getTuple(context, key);
            assertEquals("value1", value.getString(0));
        }
    }

    @Test
    void importDataWithDirectoryLayer() {
        // Test importing data into a keyspace using DirectoryLayer directories
        // Should verify that DirectoryLayer mappings work correctly during import
        final String tenantUuid = UUID.randomUUID().toString();
        KeySpace root = new KeySpace(
                new DirectoryLayerDirectory("tenant", tenantUuid)
                        .addSubdirectory(new KeySpaceDirectory("user_id", KeyType.LONG)));


        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            
            KeySpacePath userPath = root.path("tenant").add("user_id", 999L);
            byte[] key = userPath.toSubspace(context).pack(Tuple.from("data"));
            tr.set(key, Tuple.from("directory_test").pack());
            context.commit();
        }

        // Export and import
        List<DataInKeySpacePath> exportedData = new ArrayList<>();
        try (FDBRecordContext context = database.openContext()) {
            root.path("tenant").exportAllData(context, null, ScanProperties.FORWARD_SCAN)
                    .forEach(exportedData::add).join();
        }

        clearPath(database, root.path("tenant"));

        try (FDBRecordContext context = database.openContext()) {
            root.path("tenant").importData(context, exportedData).join();
            context.commit();
        }

        // Verify
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath userPath = root.path("tenant").add("user_id", 999L);
            byte[] key = userPath.toSubspace(context).pack(Tuple.from("data"));
            Tuple value = getTuple(context, key);
            assertEquals("directory_test", value.getString(0));
        }
    }

    @Test
    void importDataWithBinaryValues() {
        // Test importing data containing binary (byte array) values
        // Should verify that binary data is imported correctly
        final String storeUuid = UUID.randomUUID().toString();
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("binary_store", KeyType.STRING, storeUuid)
                        .addSubdirectory(new KeySpaceDirectory("blob_id", KeyType.BYTES)));

        
        byte[] blobId = {0x01, 0x02, 0x03, (byte) 0xFF, (byte) 0xFE};

        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            
            KeySpacePath blobPath = root.path("binary_store").add("blob_id", blobId);
            byte[] key = blobPath.toSubspace(context).pack(Tuple.from("metadata"));
            tr.set(key, "binary_test_data".getBytes());
            context.commit();
        }

        // Export and import
        List<DataInKeySpacePath> exportedData = new ArrayList<>();
        try (FDBRecordContext context = database.openContext()) {
            root.path("binary_store").exportAllData(context, null, ScanProperties.FORWARD_SCAN)
                    .forEach(exportedData::add).join();
        }

        clearPath(database, root.path("binary_store"));

        importData(database, root.path("binary_store"), exportedData);

        // Verify
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath blobPath = root.path("binary_store").add("blob_id", blobId);
            byte[] key = blobPath.toSubspace(context).pack(Tuple.from("metadata"));
            byte[] value = getJoin(context, key);
            assertArrayEquals("binary_test_data".getBytes(), value);
        }
    }

    @Test
    void importDataWithNullKeyType() {
        // Test importing data into a keyspace with NULL key type directories
        // Should verify that NULL directories are handled correctly
        final String baseUuid = UUID.randomUUID().toString();
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("base", KeyType.STRING, baseUuid)
                        .addSubdirectory(new KeySpaceDirectory("null_dir", KeyType.NULL)));


        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            
            KeySpacePath nullPath = root.path("base").add("null_dir");
            byte[] key = nullPath.toSubspace(context).pack(Tuple.from("item"));
            tr.set(key, Tuple.from("null_test").pack());
            context.commit();
        }

        // Export and import
        List<DataInKeySpacePath> exportedData = new ArrayList<>();
        try (FDBRecordContext context = database.openContext()) {
            root.path("base").exportAllData(context, null, ScanProperties.FORWARD_SCAN)
                    .forEach(exportedData::add).join();
        }

        clearPath(database, root.path("base"));

        try (FDBRecordContext context = database.openContext()) {
            root.path("base").importData(context, exportedData).join();
            context.commit();
        }

        // Verify
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath nullPath = root.path("base").add("null_dir");
            byte[] key = nullPath.toSubspace(context).pack(Tuple.from("item"));
            Tuple value = getTuple(context, key);
            assertEquals("null_test", value.getString(0));
        }
    }

    @Test
    void importDataWithComplexHierarchy() {
        // Test importing data into a multi-level directory hierarchy
        // Should verify that nested paths are reconstructed correctly
        final String rootUuid = UUID.randomUUID().toString();
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("org", KeyType.STRING, rootUuid)
                        .addSubdirectory(new KeySpaceDirectory("dept", KeyType.STRING)
                                .addSubdirectory(new KeySpaceDirectory("team", KeyType.LONG)
                                        .addSubdirectory(new KeySpaceDirectory("member", KeyType.UUID)))));

        UUID memberId = UUID.randomUUID();

        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            
            KeySpacePath memberPath = root.path("org").add("dept", "engineering")
                    .add("team", 42L).add("member", memberId);
            byte[] key = memberPath.toSubspace(context).pack(Tuple.from("info", "name"));
            tr.set(key, Tuple.from("Complex Test").pack());
            context.commit();
        }

        // Export and import
        List<DataInKeySpacePath> exportedData = getExportedData(database, root.path("org"));
        clearPath(database, root.path("org"));
        importData(database, root.path("org"), exportedData);

        // Verify
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath memberPath = root.path("org").add("dept", "engineering")
                    .add("team", 42L).add("member", memberId);
            byte[] key = memberPath.toSubspace(context).pack(Tuple.from("info", "name"));
            assertEquals(Tuple.from("Complex Test"), getTuple(context, key));
        }
    }

    @Test
    void importDataFromDifferentCluster() {
        // Test importing data that was exported from a different cluster
        // Should verify that logical values allow cross-cluster import
        // This simulates the scenario where DirectoryLayer mappings differ
        final String tenantUuid = UUID.randomUUID().toString();
        KeySpace root = new KeySpace(
                new DirectoryLayerDirectory("tenant", tenantUuid)
                        .addSubdirectory(new KeySpaceDirectory("user_id", KeyType.LONG)));


        // Create and export data in one "cluster" context
        List<DataInKeySpacePath> exportedData = new ArrayList<>();
        try (FDBRecordContext context1 = database.openContext()) {
            Transaction tr = context1.ensureActive();
            
            KeySpacePath userPath = root.path("tenant").add("user_id", 123L);
            byte[] key = userPath.toSubspace(context1).pack(Tuple.from("profile"));
            tr.set(key, Tuple.from("cross_cluster_test").pack());
            
            root.path("tenant").exportAllData(context1, null, ScanProperties.FORWARD_SCAN)
                    .forEach(exportedData::add).join();
            context1.commit();
        }

        // Clear the data to simulate different cluster
        clearPath(database, root.path("tenant"));

        // Import in a different "cluster" context
        try (FDBRecordContext context2 = database.openContext()) {
            root.path("tenant").importData(context2, exportedData).join();
            context2.commit();
        }

        // Verify the data was imported using logical values
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath userPath = root.path("tenant").add("user_id", 123L);
            byte[] key = userPath.toSubspace(context).pack(Tuple.from("profile"));
            Tuple value = getTuple(context, key);
            assertEquals("cross_cluster_test", value.getString(0));
        }
    }

    @Test
    void importDataWithMismatchedPath() {
        // Test importing data that doesn't belong to the target path
        // Should throw RecordCoreIllegalImportDataException
        final String root1Uuid = UUID.randomUUID().toString();
        final String root2Uuid = UUID.randomUUID().toString();
        
        KeySpace keySpace1 = new KeySpace(
                new KeySpaceDirectory("root1", KeyType.STRING, root1Uuid)
                        .addSubdirectory(new KeySpaceDirectory("data", KeyType.LONG)));
        
        KeySpace keySpace2 = new KeySpace(
                new KeySpaceDirectory("root2", KeyType.STRING, root2Uuid)
                        .addSubdirectory(new KeySpaceDirectory("data", KeyType.LONG)));


        // Create data in keySpace2
        List<DataInKeySpacePath> exportedData = new ArrayList<>();
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            KeySpacePath dataPath = keySpace2.path("root2").add("data", 1L);
            byte[] key = dataPath.toSubspace(context).pack(Tuple.from("record"));
            tr.set(key, Tuple.from("value").pack());
            
            KeyValue kv = new KeyValue(key, Tuple.from("value").pack());
            exportedData.add(new DataInKeySpacePath(keySpace2.path("root2"), kv, context));
            context.commit();
        }

        // Try to import into keySpace1 - should fail
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath root1Path = keySpace1.path("root1");
            
            CompletionException completionException = assertThrows(CompletionException.class, () ->
                    root1Path.importData(context, exportedData).join()
            );
            assertTrue(completionException.getCause() instanceof RecordCoreIllegalImportDataException);
        }
    }

    @Test
    void importDataWithInvalidPath() {
        // Test importing data with paths that don't exist in the keyspace
        // Should throw RecordCoreIllegalImportDataException
        final String rootUuid = UUID.randomUUID().toString();
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.STRING, rootUuid)
                        .addSubdirectory(new KeySpaceDirectory("valid", KeyType.LONG)));


        try (FDBRecordContext context = database.openContext()) {
            // Create a key that doesn't follow the keyspace structure
            byte[] invalidKey = Tuple.from("completely", "different", "structure").pack();
            KeyValue kv = new KeyValue(invalidKey, Tuple.from("value").pack());
            
            List<DataInKeySpacePath> invalidData = Collections.singletonList(
                    new DataInKeySpacePath(root.path("root"), kv, context)
            );

            CompletionException completionException = assertThrows(CompletionException.class, () ->
                    root.path("root").importData(context, invalidData).join()
            );
            // The cause might be RecordCoreIllegalImportDataException or RecordCoreArgumentException for invalid paths
            assertTrue(completionException.getCause() instanceof RecordCoreArgumentException);
        }
    }

    @Test
    void importDataWithSubdirectoryPath() {
        // Test importing data where the target path is a subdirectory of the import path
        // Should succeed and import only data that belongs to subdirectories
        final String rootUuid = UUID.randomUUID().toString();
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.STRING, rootUuid)
                        .addSubdirectory(new KeySpaceDirectory("level1", KeyType.LONG)
                                .addSubdirectory(new KeySpaceDirectory("level2", KeyType.STRING))));


        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            
            // Create data at different levels
            KeySpacePath level1Path = root.path("root").add("level1", 1L);
            KeySpacePath level2Path = level1Path.add("level2", "data");
            
            byte[] key1 = level1Path.toSubspace(context).pack(Tuple.from("item1"));
            byte[] key2 = level2Path.toSubspace(context).pack(Tuple.from("item2"));
            
            tr.set(key1, Tuple.from("value1").pack());
            tr.set(key2, Tuple.from("value2").pack());
            context.commit();
        }

        // Export from root, import to subdirectory
        List<DataInKeySpacePath> exportedData = new ArrayList<>();
        try (FDBRecordContext context = database.openContext()) {
            root.path("root").exportAllData(context, null, ScanProperties.FORWARD_SCAN)
                    .forEach(exportedData::add).join();
        }

        clearPath(database, root.path("root"));

        // Import only to level1 subdirectory
        importData(database, root.path("root").add("level1", 1L), exportedData);

        // Verify only level1 and below data exists
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath level2Path = root.path("root").add("level1", 1L).add("level2", "data");
            byte[] key2 = level2Path.toSubspace(context).pack(Tuple.from("item2"));
            Tuple value2 = getTuple(context, key2);
            assertEquals("value2", value2.getString(0));
        }
    }

    @Test
    void importDataWithParentPath() {
        // Test importing data where the target path is a parent of some import data paths
        // Should throw RecordCoreIllegalImportDataException for paths outside the target
        final String root1Uuid = UUID.randomUUID().toString();
        final String root2Uuid = UUID.randomUUID().toString();
        
        KeySpace keySpace1 = new KeySpace(
                new KeySpaceDirectory("root1", KeyType.STRING, root1Uuid)
                        .addSubdirectory(new KeySpaceDirectory("child", KeyType.LONG)));
        
        KeySpace keySpace2 = new KeySpace(
                new KeySpaceDirectory("root2", KeyType.STRING, root2Uuid)
                        .addSubdirectory(new KeySpaceDirectory("child", KeyType.LONG)));


        // Create data in both keyspaces
        List<DataInKeySpacePath> mixedData = new ArrayList<>();
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            
            KeySpacePath path1 = keySpace1.path("root1").add("child", 1L);
            KeySpacePath path2 = keySpace2.path("root2").add("child", 2L);
            
            byte[] key1 = path1.toSubspace(context).pack(Tuple.from("data"));
            byte[] key2 = path2.toSubspace(context).pack(Tuple.from("data"));
            
            tr.set(key1, Tuple.from("data1").pack());
            tr.set(key2, Tuple.from("data2").pack());
            
            KeyValue kv1 = new KeyValue(key1, Tuple.from("data1").pack());
            KeyValue kv2 = new KeyValue(key2, Tuple.from("data2").pack());
            
            mixedData.add(new DataInKeySpacePath(keySpace1.path("root1"), kv1, context));
            mixedData.add(new DataInKeySpacePath(keySpace2.path("root2"), kv2, context));
            
            context.commit();
        }

        // Try to import mixed data into keySpace1 - should fail due to keySpace2 data
        try (FDBRecordContext context = database.openContext()) {
            CompletionException completionException = assertThrows(CompletionException.class, () ->
                    keySpace1.path("root1").importData(context, mixedData).join()
            );
            assertTrue(completionException.getCause() instanceof RecordCoreIllegalImportDataException);
        }
    }

    @Test
    void importDataWithWrapperClasses() {
        // Test importing data using wrapper classes like EnvironmentKeySpace
        // Should verify that wrapper functionality works correctly with import
        final EnvironmentKeySpace keySpace = EnvironmentKeySpace.setupSampleData(database);

        // Export some data
        List<DataInKeySpacePath> exportedData = new ArrayList<>();
        try (FDBRecordContext context = database.openContext()) {
            EnvironmentKeySpace.DataPath dataStore = keySpace.root().userid(100L).application("app1").dataStore();
            dataStore.exportAllData(context, null, ScanProperties.FORWARD_SCAN)
                    .forEach(exportedData::add).join();
        }

        // Clear and import back
        clearPath(database, keySpace.root());

        try (FDBRecordContext context = database.openContext()) {
            keySpace.root().importData(context, exportedData).join();
            context.commit();
        }

        // Verify data exists
        try (FDBRecordContext context = database.openContext()) {
            EnvironmentKeySpace.DataPath dataStore = keySpace.root().userid(100L).application("app1").dataStore();
            byte[] key = dataStore.toSubspace(context).pack(Tuple.from("record2", 0));
            byte[] value = getJoin(context, key);
            assertTrue(value.length > 0);
        }
    }

    @Test
    void importDataWithDuplicateKeys() {
        // Test importing data where the same key appears multiple times in the input
        // Should verify that the last value wins for duplicate keys
        final String rootUuid = UUID.randomUUID().toString();
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.STRING, rootUuid)
                        .addSubdirectory(new KeySpaceDirectory("data", KeyType.LONG)));


        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath dataPath = root.path("root").add("data", 1L);
            byte[] key = dataPath.toSubspace(context).pack(Tuple.from("item"));
            
            // Create multiple DataInKeySpacePath objects with same key but different values
            List<DataInKeySpacePath> duplicateData = Arrays.asList(
                    new DataInKeySpacePath(root.path("root"), 
                            new KeyValue(key, Tuple.from("first_value").pack()), context),
                    new DataInKeySpacePath(root.path("root"), 
                            new KeyValue(key, Tuple.from("second_value").pack()), context),
                    new DataInKeySpacePath(root.path("root"), 
                            new KeyValue(key, Tuple.from("final_value").pack()), context)
            );

            root.path("root").importData(context, duplicateData).join();
            context.commit();
        }

        // Verify the final value is stored
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath dataPath = root.path("root").add("data", 1L);
            byte[] key = dataPath.toSubspace(context).pack(Tuple.from("item"));
            Tuple value = getTuple(context, key);
            assertEquals("final_value", value.getString(0));
        }
    }

    @Test
    void importDataRollback() {
        // Test import operation that fails and needs to be rolled back
        // Should verify that partial imports are properly cleaned up
        final String root1Uuid = UUID.randomUUID().toString();
        final String root2Uuid = UUID.randomUUID().toString();
        
        KeySpace keySpace1 = new KeySpace(
                new KeySpaceDirectory("root1", KeyType.STRING, root1Uuid)
                        .addSubdirectory(new KeySpaceDirectory("data", KeyType.LONG)));
        
        KeySpace keySpace2 = new KeySpace(
                new KeySpaceDirectory("root2", KeyType.STRING, root2Uuid)
                        .addSubdirectory(new KeySpaceDirectory("data", KeyType.LONG)));


        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            
            // Create mixed data that will partially succeed then fail
            KeySpacePath validPath = keySpace1.path("root1").add("data", 1L);
            KeySpacePath invalidPath = keySpace2.path("root2").add("data", 2L);
            
            byte[] validKey = validPath.toSubspace(context).pack(Tuple.from("item"));
            byte[] invalidKey = invalidPath.toSubspace(context).pack(Tuple.from("item"));
            
            List<DataInKeySpacePath> mixedData = Arrays.asList(
                    new DataInKeySpacePath(keySpace1.path("root1"), 
                            new KeyValue(validKey, Tuple.from("valid").pack()), context),
                    new DataInKeySpacePath(keySpace2.path("root2"), 
                            new KeyValue(invalidKey, Tuple.from("invalid").pack()), context)
            );

            // This should fail and rollback
            CompletionException completionException = assertThrows(CompletionException.class, () ->
                    keySpace1.path("root1").importData(context, mixedData).join()
            );
            assertTrue(completionException.getCause() instanceof RecordCoreIllegalImportDataException);
        }
        
        // Verify no data was written after the failed transaction
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath validPath = keySpace1.path("root1").add("data", 1L);
            byte[] validKey = validPath.toSubspace(context).pack(Tuple.from("item"));
            byte[] value = getJoin(context, validKey);
            assertTrue(value == null || value.length == 0);
        }
    }

    @Test
    void importDataValidation() {
        // Test various edge cases for data validation
        // Should verify that malformed DataInKeySpacePath objects are rejected
        final String rootUuid = UUID.randomUUID().toString();
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.STRING, rootUuid)
                        .addSubdirectory(new KeySpaceDirectory("data", KeyType.LONG)));


        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath rootPath = root.path("root");
            
            // Test with empty iterable
            rootPath.importData(context, Collections.emptyList()).join();
            
            // Test with null remainder (should work)
            KeySpacePath dataPath = rootPath.add("data", 1L);
            byte[] key = dataPath.toSubspace(context).pack(); // No remainder
            KeyValue kv = new KeyValue(key, Tuple.from("test").pack());
            List<DataInKeySpacePath> validData = Collections.singletonList(
                    new DataInKeySpacePath(rootPath, kv, context)
            );
            
            rootPath.importData(context, validData).join();
            context.commit();
        }

        // Verify the data was stored
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath dataPath = root.path("root").add("data", 1L);
            byte[] key = dataPath.toSubspace(context).pack();
            Tuple value = getTuple(context, key);
            assertEquals("test", value.getString(0));
        }
    }

    private static Tuple getTuple(final FDBRecordContext context, final byte[] key) {
        return Tuple.fromBytes(context.ensureActive().get(key).join());
    }

    private static byte[] getJoin(final FDBRecordContext context, final byte[] key) {
        return context.ensureActive().get(key).join();
    }

    private static void importData(final FDBDatabase database, final KeySpacePath path, final List<DataInKeySpacePath> exportedData) {
        try (FDBRecordContext context = database.openContext()) {
            path.importData(context, exportedData).join();
            context.commit();
        }
    }

    private static void clearPath(final FDBDatabase database, final KeySpacePath path) {
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            tr.clear(path.toSubspace(context).range());
            context.commit();
        }
        // just an extra check to make sure the test is working as expected
        assertTrue(getExportedData(database, path).isEmpty(),
                "Clearing should remove all the data");
    }

    @Nonnull
    private static List<DataInKeySpacePath> getExportedData(final FDBDatabase database, final KeySpacePath path) {
        List<DataInKeySpacePath> exportedData = new ArrayList<>();
        try (FDBRecordContext context = database.openContext()) {
            path.exportAllData(context, null, ScanProperties.FORWARD_SCAN)
                    .forEach(exportedData::add).join();
        }
        return exportedData;
    }
}
