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
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory.KeyType;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.assertj.core.api.Assertions;
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
    void importComprehensiveData() {
        // Test importing data covering ALL KeyType enum values in a single complex directory structure:
        // - NULL, BYTES, STRING, LONG, FLOAT, DOUBLE, BOOLEAN, UUID
        // - Constant value directories  
        // - Complex multi-level hierarchy
        // - Different data types in remainder elements
        // Given that the majority of complexity is in the components under the importData method, this is basically
        // an integration test
        final String rootUuid = UUID.randomUUID().toString();
        byte[] binaryId = {0x01, 0x02, 0x03, (byte) 0xFF, (byte) 0xFE};
        UUID memberId = UUID.randomUUID();
        
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("company", KeyType.STRING, rootUuid)  // STRING with constant
                        .addSubdirectory(new KeySpaceDirectory("version", KeyType.LONG, 1L)  // LONG with constant
                                .addSubdirectory(new KeySpaceDirectory("department", KeyType.STRING)  // STRING variable
                                        .addSubdirectory(new KeySpaceDirectory("employee_id", KeyType.LONG)  // LONG variable
                                                .addSubdirectory(new KeySpaceDirectory("binary_data", KeyType.BYTES)  // BYTES
                                                        .addSubdirectory(new KeySpaceDirectory("null_section", KeyType.NULL)  // NULL
                                                                .addSubdirectory(new KeySpaceDirectory("member", KeyType.UUID)  // UUID
                                                                        .addSubdirectory(new KeySpaceDirectory("active", KeyType.BOOLEAN)  // BOOLEAN
                                                                                .addSubdirectory(new KeySpaceDirectory("rating", KeyType.FLOAT))))))))));  // FLOAT


        // Create comprehensive test data covering ALL KeyType values
        KeySpacePath basePath = root.path("company").add("version").add("department", "engineering");

        // Build paths using all KeyType values
        KeySpacePath emp1Path = basePath.add("employee_id", 100L)
                .add("binary_data", binaryId)
                .add("null_section")
                .add("member", memberId)
                .add("active", true)
                .add("rating", 4.5f);

        KeySpacePath emp2Path = basePath.add("employee_id", 200L)
                .add("binary_data", binaryId)
                .add("null_section")
                .add("member", memberId)
                .add("active", false)
                .add("rating", 3.8f);

        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();

            byte[] key1 = emp1Path.toSubspace(context).pack(Tuple.from("profile", "name"));
            setToTuple(tr, key1, "John Doe");

            byte[] key2 = emp2Path.toSubspace(context).pack(Tuple.from("profile", "name"));
            setToTuple(tr, key2, "Jane Smith");

            byte[] longKey = emp1Path.toSubspace(context).pack(Tuple.from("salary"));
            setToTuple(tr, longKey, 75000);

            byte[] binaryKey = emp1Path.toSubspace(context).pack(Tuple.from("binary_metadata"));
            tr.set(binaryKey, "binary_test_data".getBytes());
            
            byte[] complexKey = emp1Path.toSubspace(context).pack(Tuple.from("info", 42, true, "complex"));
            setToTuple(tr, complexKey, "Complex Test");

            context.commit();
        }

        // Export the data
        final List<DataInKeySpacePath> exportedData = getExportedData(root.path("company"));

        // Clear the data and import it back
        clearPath(database, root.path("company"));

        // Import the data
        importData(database, root.path("company"), exportedData);

        // Verify all different KeyType values were handled correctly during import
        try (FDBRecordContext context = database.openContext()) {
            byte[] key1 = emp1Path.toSubspace(context).pack(Tuple.from("profile", "name"));
            byte[] key2 = emp2Path.toSubspace(context).pack(Tuple.from("profile", "name"));
            assertEquals(Tuple.from("John Doe"), getTuple(context, key1));
            assertEquals(Tuple.from("Jane Smith"), getTuple(context, key2));
            
            byte[] longKey = emp1Path.toSubspace(context).pack(Tuple.from("salary"));
            assertEquals(Tuple.from(75000), getTuple(context, longKey));
            
            // Verify BYTES data (raw binary, not in tuple)
            byte[] binaryKey = emp1Path.toSubspace(context).pack(Tuple.from("binary_metadata"));
            assertArrayEquals("binary_test_data".getBytes(), getValue(context, binaryKey));
            
            // Verify complex hierarchy with mixed types in remainder (LONG, BOOLEAN, STRING)
            byte[] complexKey = emp1Path.toSubspace(context).pack(Tuple.from("info", 42, true, "complex"));
            assertEquals(Tuple.from("Complex Test"), getTuple(context, complexKey));
        }
    }

    @Test
    void importEmptyData() {
        // Test importing an empty collection of data
        // Should complete successfully without modifying the data under the path
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("test", KeyType.STRING, UUID.randomUUID().toString()));

        KeySpacePath testPath = root.path("test");
        importData(database, testPath, Collections.emptyList()); // should not throw any exception

        assertTrue(getExportedData(testPath).isEmpty(),
                "there should not have been any data created");
    }

    @Test
    void importOverwriteExistingData() {
        // Test importing data that overwrites existing keys
        // Should verify that new data replaces old data when keys match
        final String rootUuid = UUID.randomUUID().toString();
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.STRING, rootUuid)
                        .addSubdirectory(new KeySpaceDirectory("data", KeyType.LONG)));

        final KeySpacePath dataPath = root.path("root").add("data", 1L);
        setSingleKey(dataPath, Tuple.from("record"), Tuple.from("original_value"));
        setSingleKey(dataPath, Tuple.from("other"), Tuple.from("other_value"));

        // Create import data with same key but different value
        List<DataInKeySpacePath> importData = new ArrayList<>();
        try (FDBRecordContext context = database.openContext()) {
            byte[] key = dataPath.toSubspace(context).pack(Tuple.from("record"));
            KeyValue kv = new KeyValue(key, Tuple.from("new_value").pack());
            importData.add(new DataInKeySpacePath(root.path("root"), kv, context));
        }

        // Verify we can re-import the data multiple times
        importData(database, root.path("root"), importData);
        importData(database, root.path("root"), importData);
        importData(database, root.path("root"), importData);

        // Verify the data was overwritten
        verifySingleKey(dataPath, Tuple.from("record"), Tuple.from("new_value"));
        verifySingleKey(dataPath, Tuple.from("other"), Tuple.from("other_value"));
    }

    @Test
    void importDataWithDirectoryLayer() {
        // Test importing data into a keyspace using DirectoryLayer directories
        // Should verify that DirectoryLayer mappings work correctly during import
        final String tenantUuid = UUID.randomUUID().toString();
        KeySpace root = new KeySpace(
                new DirectoryLayerDirectory("tenant", tenantUuid)
                        .addSubdirectory(new KeySpaceDirectory("user_id", KeyType.LONG)));

        final KeySpacePath dataPath = root.path("tenant").add("user_id", 999L);
        setSingleKey(dataPath, Tuple.from("data"), Tuple.from("directory_test"));

        List<DataInKeySpacePath> exportedData = getExportedData(root.path("tenant"));

        clearPath(database, root.path("tenant"));

        importData(database, root.path("tenant"), exportedData);

        verifySingleKey(dataPath, Tuple.from("data"), Tuple.from("directory_test"));
    }

    @Test
    void importDataWithMismatchedPath() {
        // Test importing data that doesn't belong to the target path
        // Should throw RecordCoreIllegalImportDataException
        final String root1Uuid = UUID.randomUUID().toString();
        final String root2Uuid = UUID.randomUUID().toString();
        
        KeySpace keySpace = new KeySpace(
                new KeySpaceDirectory("root1", KeyType.STRING, root1Uuid)
                        .addSubdirectory(new KeySpaceDirectory("data", KeyType.LONG)),
                new KeySpaceDirectory("root2", KeyType.STRING, root2Uuid)
                        .addSubdirectory(new KeySpaceDirectory("data", KeyType.LONG)));

        setSingleKey(keySpace.path("root1").add("data", 1L), Tuple.from("record"), Tuple.from("other"));

        // Now try to ipmort that into keySpace2
        List<DataInKeySpacePath> exportedData = getExportedData(keySpace.path("root1"));
        assertBadImport(keySpace.path("root2"), exportedData);
    }

    @Test
    void importDataWithInvalidPath() {
        // Test importing data with paths that don't exist in the keyspace
        // Should throw RecordCoreIllegalImportDataException
        final String root1Uuid = UUID.randomUUID().toString();
        final String root2Uuid = UUID.randomUUID().toString();

        KeySpace keySpace1 = new KeySpace(
                new KeySpaceDirectory("root1", KeyType.STRING, root1Uuid)
                        .addSubdirectory(new KeySpaceDirectory("data", KeyType.LONG)));

        KeySpace keySpace2 = new KeySpace(
                new KeySpaceDirectory("root2", KeyType.STRING, root2Uuid)
                        .addSubdirectory(new KeySpaceDirectory("data", KeyType.LONG)));

        setSingleKey(keySpace1.path("root1").add("data", 1L), Tuple.from("record"), Tuple.from("other"));

        // Now try to ipmort that into keySpace2
        List<DataInKeySpacePath> exportedData = getExportedData(keySpace1.path("root1"));
        assertBadImport(keySpace2.path("root2"), exportedData);
    }

    @Test
    void importDataWithSubdirectoryPath() {
        // Test importing data where the target path is a subdirectory of the import path
        // Should succeed only if all the data is in the subdirectory
        final String rootUuid = UUID.randomUUID().toString();
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.STRING, rootUuid)
                        .addSubdirectory(new KeySpaceDirectory("level1", KeyType.LONG)));

        KeySpacePath level1Path = root.path("root").add("level1", 1L);

        setSingleKey(level1Path, Tuple.from("item1"), Tuple.from("value1"));

        // Export from root, import to subdirectory
        List<DataInKeySpacePath> exportedData = getExportedData(root.path("root"));

        clearPath(database, root.path("root"));

        // Import only to level1 subdirectory
        importData(database, level1Path, exportedData);

        verifySingleKey(level1Path, Tuple.from("item1"), Tuple.from("value1"));
    }

    @Test
    void importDataWithSubdirectoryPathFailure() {
        // Test importing data where the target path is a subdirectory of the import path
        // Should succeed only if all the data is in the subdirectory
        final String rootUuid = UUID.randomUUID().toString();
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.STRING, rootUuid)
                        .addSubdirectory(new KeySpaceDirectory("level1", KeyType.LONG)));

        KeySpacePath level1Path = root.path("root").add("level1", 1L);

        setSingleKey(level1Path, Tuple.from("item1"), Tuple.from("value1"));
        setSingleKey(root.path("root").add("level1", 2L), Tuple.from("item1"), Tuple.from("value1"));

        // Export from root, import to subdirectory
        List<DataInKeySpacePath> exportedData = getExportedData(root.path("root"));

        clearPath(database, root.path("root"));

        // Import only to level1 subdirectory
        assertBadImport(level1Path, exportedData);
    }

    @Test
    void importDataWithPartialMismatch() {
        // Test importing data where the target path is a parent of some import data paths
        // Should throw RecordCoreIllegalImportDataException for paths outside the target
        final String root1Uuid = UUID.randomUUID().toString();
        final String root2Uuid = UUID.randomUUID().toString();
        
        KeySpace keySpace = new KeySpace(
                new KeySpaceDirectory("root1", KeyType.STRING, root1Uuid)
                        .addSubdirectory(new KeySpaceDirectory("child", KeyType.LONG)),
                new KeySpaceDirectory("root2", KeyType.STRING, root2Uuid)
                        .addSubdirectory(new KeySpaceDirectory("child", KeyType.LONG)));

        KeySpacePath path1 = keySpace.path("root1").add("child", 1L);
        KeySpacePath path2 = keySpace.path("root2").add("child", 2L);
        setSingleKey(path1, Tuple.from("data"), Tuple.from("data1"));
        setSingleKey(path2, Tuple.from("data"), Tuple.from("data2"));

        List<DataInKeySpacePath> mixedData = new ArrayList<>();
        mixedData.addAll(getExportedData(path1));
        mixedData.addAll(getExportedData(path2));

        assertBadImport(keySpace.path("root1"), mixedData);
    }

    @Test
    void importDataWithWrapperClasses() {
        // Test importing data using wrapper classes like EnvironmentKeySpace
        // Should verify that wrapper functionality works correctly with import
        final EnvironmentKeySpace keySpace = EnvironmentKeySpace.setupSampleData(database);

        EnvironmentKeySpace.DataPath dataStore = keySpace.root().userid(100L).application("app1").dataStore();
        List<DataInKeySpacePath> exportedData = getExportedData(dataStore);

        clearPath(database, keySpace.root());

        importData(database, keySpace.root(), exportedData);
        verifySingleKey(dataStore, Tuple.from("record2", 0), Tuple.from("user100_app1_data2_0"));
    }

    @Test
    void importDataWithDuplicateKeys() {
        // Test importing data where the same key appears multiple times in the input
        // Should verify that the last value wins for duplicate keys
        final String rootUuid = UUID.randomUUID().toString();
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.STRING, rootUuid)
                        .addSubdirectory(new KeySpaceDirectory("data", KeyType.LONG)));


        KeySpacePath dataPath = root.path("root").add("data", 1L);
        try (FDBRecordContext context = database.openContext()) {
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

        verifySingleKey(dataPath, Tuple.from("item"), Tuple.from("final_value"));
    }

    private void setSingleKey(KeySpacePath path, Tuple remainder, Tuple value) {
        try (FDBRecordContext context = database.openContext()) {
            byte[] key = path.toSubspace(context).pack(remainder);
            context.ensureActive().set(key, value.pack());
            context.commit();
        }
    }

    private void verifySingleKey(KeySpacePath path, Tuple remainder, Tuple expected) {
        try (FDBRecordContext context = database.openContext()) {
            byte[] key = path.toSubspace(context).pack(remainder);
            assertEquals(expected, getTuple(context, key));
        }
    }

    private static void setToTuple(final Transaction tr, final byte[] key1, Object items) {
        byte[] value1 = Tuple.from(items).pack();
        tr.set(key1, value1);
    }

    private static Tuple getTuple(final FDBRecordContext context, final byte[] key) {
        return Tuple.fromBytes(context.ensureActive().get(key).join());
    }

    private static byte[] getValue(final FDBRecordContext context, final byte[] key) {
        return context.ensureActive().get(key).join();
    }

    private static void importData(final FDBDatabase database, final KeySpacePath path, final List<DataInKeySpacePath> exportedData) {
        try (FDBRecordContext context = database.openContext()) {
            path.importData(context, exportedData).join();
            context.commit();
        }
    }

    private void assertBadImport(final KeySpacePath path, final List<DataInKeySpacePath> invalidData) {
        // Try to import into keySpace1 - should fail
        try (FDBRecordContext context = database.openContext()) {
            Assertions.assertThatThrownBy(() -> path.importData(context, invalidData).join())
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(RecordCoreIllegalImportDataException.class);
        }
    }

    private void clearPath(final FDBDatabase database, final KeySpacePath path) {
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            tr.clear(path.toSubspace(context).range());
            context.commit();
        }
        // just an extra check to make sure the test is working as expected
        assertTrue(getExportedData(path).isEmpty(),
                "Clearing should remove all the data");
    }

    @Nonnull
    private List<DataInKeySpacePath> getExportedData(final KeySpacePath path) {
        List<DataInKeySpacePath> exportedData = new ArrayList<>();
        try (FDBRecordContext context = database.openContext()) {
            path.exportAllData(context, null, ScanProperties.FORWARD_SCAN)
                    .forEach(exportedData::add).join();
        }
        return exportedData;
    }
}
