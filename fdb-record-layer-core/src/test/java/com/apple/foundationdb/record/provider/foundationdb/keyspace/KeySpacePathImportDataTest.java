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
    private FDBDatabase sourceDatabase;
    private FDBDatabase destinationDatabase;

    @BeforeEach
    void setUp() {
        final List<FDBDatabase> databases = dbExtension.getDatabases(2);
        sourceDatabase = databases.get(0);
        if (databases.size() > 1) {
            destinationDatabase = databases.get(1);
        } else {
            destinationDatabase = sourceDatabase;
        }
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
                new KeySpaceDirectory("company", KeyType.STRING, rootUuid)
                        .addSubdirectory(new KeySpaceDirectory("version", KeyType.LONG, 1L)
                                .addSubdirectory(new KeySpaceDirectory("department", KeyType.STRING)
                                        .addSubdirectory(new KeySpaceDirectory("employee_id", KeyType.LONG)
                                                .addSubdirectory(new KeySpaceDirectory("binary_data", KeyType.BYTES)
                                                        .addSubdirectory(new KeySpaceDirectory("null_section", KeyType.NULL)
                                                                .addSubdirectory(new KeySpaceDirectory("member", KeyType.UUID)
                                                                        .addSubdirectory(new KeySpaceDirectory("active", KeyType.BOOLEAN)
                                                                                .addSubdirectory(new KeySpaceDirectory("rating", KeyType.FLOAT))))))))));


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

        try (FDBRecordContext context = sourceDatabase.openContext()) {
            setInPath(emp1Path, context, Tuple.from("profile", "name"), "John Doe");
            setInPath(emp2Path, context, Tuple.from("profile", "name"), "Jane Smith");
            setInPath(emp1Path, context, Tuple.from("salary"), 75000);
            setInPath(emp1Path, context, Tuple.from("info", 42, true, "complex"), "Complex Test");

            byte[] binaryKey = emp1Path.toSubspace(context).pack(Tuple.from("binary_metadata"));
            context.ensureActive().set(binaryKey, "binary_test_data".getBytes());

            context.commit();
        }

        copyData(root.path("company"), root.path("company"));

        // Verify all different KeyType values were handled correctly during import
        try (FDBRecordContext context = destinationDatabase.openContext()) {
            assertEquals(Tuple.from("John Doe"),
                    getTupleFromPath(context, emp1Path, Tuple.from("profile", "name")));
            assertEquals(Tuple.from("Jane Smith"),
                    getTupleFromPath(context, emp2Path, Tuple.from("profile", "name")));
            assertEquals(Tuple.from(75000),
                    getTupleFromPath(context, emp1Path, Tuple.from("salary")));
            assertEquals(Tuple.from("Complex Test"),
                    getTupleFromPath(context, emp1Path, Tuple.from("info", 42, true, "complex")));
            
            // Verify BYTES data (raw binary, not in tuple)
            byte[] binaryKey = emp1Path.toSubspace(context).pack(Tuple.from("binary_metadata"));
            assertArrayEquals("binary_test_data".getBytes(), context.ensureActive().get(binaryKey).join());
        }
    }

    @Test
    void importEmptyData() {
        // Test importing an empty collection of data
        // Should complete successfully without modifying the data under the path
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("test", KeyType.STRING, UUID.randomUUID().toString()));

        KeySpacePath testPath = root.path("test");
        importData(testPath, Collections.emptyList()); // should not throw any exception

        assertTrue(getExportedData(testPath).isEmpty(),
                "there should not have been any data created");
    }

    @Test
    void importOverwriteExistingData() {
        // Test importing data that overwrites existing keys
        final String rootUuid = UUID.randomUUID().toString();
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.STRING, rootUuid)
                        .addSubdirectory(new KeySpaceDirectory("data", KeyType.LONG)));

        final KeySpacePath dataPath = root.path("root").add("data", 1L);

        // Import one value
        importData(root.path("root"), List.of(new DataInKeySpacePath(dataPath,
                Tuple.from("record"), Tuple.from("old_value").pack())));
        verifySingleKey(destinationDatabase, dataPath, Tuple.from("record"), Tuple.from("old_value"));

        // Import a different value
        importData(root.path("root"), List.of(new DataInKeySpacePath(dataPath,
                Tuple.from("record"), Tuple.from("new_value").pack())));

        // Verify the data was overwritten
        verifySingleKey(destinationDatabase, dataPath, Tuple.from("record"), Tuple.from("new_value"));
    }

    @Test
    void leaveExistingData() {
        // Test importing data leaves other data under the path untouched
        final String rootUuid = UUID.randomUUID().toString();
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.STRING, rootUuid)
                        .addSubdirectory(new KeySpaceDirectory("data", KeyType.LONG)));

        final KeySpacePath dataPath = root.path("root").add("data", 1L);

        // Write one key
        final Tuple remainder1 = Tuple.from("recordA");
        final Tuple value1 = Tuple.from("old_value");
        importData(root.path("root"), List.of(new DataInKeySpacePath(dataPath,
                remainder1, value1.pack())));
        verifySingleKey(destinationDatabase, dataPath, remainder1, value1);

        // write a different key
        final Tuple remainder2 = Tuple.from("recordB");
        final Tuple value2 = Tuple.from("new_value");
        importData(root.path("root"), List.of(new DataInKeySpacePath(dataPath,
                remainder2, value2.pack())));

        // Verify the data was overwritten
        verifySingleKey(destinationDatabase, dataPath, remainder1, value1);
        verifySingleKey(destinationDatabase, dataPath, remainder2, value2);
    }

    @Test
    void reimport() {
        final String rootUuid = UUID.randomUUID().toString();
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.STRING, rootUuid)
                        .addSubdirectory(new KeySpaceDirectory("data", KeyType.LONG)));

        final KeySpacePath dataPath = root.path("root").add("data", 1L);
        List<DataInKeySpacePath> importData = new ArrayList<>();
        final Tuple remainder = Tuple.from("record");
        importData.add(new DataInKeySpacePath(dataPath, remainder, Tuple.from("value").pack()));

        // Verify we can re-import the data multiple times
        importData(root.path("root"), importData);
        importData(root.path("root"), importData);
        importData(root.path("root"), importData);

        verifySingleKey(destinationDatabase, dataPath, remainder, Tuple.from("value"));
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

        copyData(root.path("tenant"), root.path("tenant"));

        verifySingleKey(destinationDatabase, dataPath, Tuple.from("data"), Tuple.from("directory_test"));
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

        // Now try to import that into root2
        assertBadImport(keySpace.path("root1"), keySpace.path("root2"));
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

        // Now try to import that into root2
        assertBadImport(keySpace1.path("root1"), keySpace2.path("root2"));
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
        copyData(root.path("root"), level1Path);

        verifySingleKey(destinationDatabase, level1Path, Tuple.from("item1"), Tuple.from("value1"));
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
        assertBadImport(root.path("root"), level1Path);
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
        final EnvironmentKeySpace keySpace = EnvironmentKeySpace.setupSampleData(sourceDatabase);

        EnvironmentKeySpace.DataPath dataStore = keySpace.root().userid(100L).application("app1").dataStore();

        copyData(keySpace.root(), keySpace.root());

        verifySingleKey(destinationDatabase, dataStore, Tuple.from("record2", 0), Tuple.from("user100_app1_data2_0"));
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

        // Create multiple DataInKeySpacePath objects with same key but different values
        List<DataInKeySpacePath> duplicateData = Arrays.asList(
                new DataInKeySpacePath(dataPath, Tuple.from("item"), Tuple.from("first_value").pack()),
                new DataInKeySpacePath(dataPath, Tuple.from("item"), Tuple.from("second_value").pack()),
                new DataInKeySpacePath(dataPath, Tuple.from("item"), Tuple.from("final_value").pack())
        );

        try (FDBRecordContext context = destinationDatabase.openContext()) {
            root.path("root").importData(context, duplicateData).join();
            context.commit();
        }

        verifySingleKey(destinationDatabase, dataPath, Tuple.from("item"), Tuple.from("final_value"));
    }

    private void setSingleKey(KeySpacePath path, Tuple remainder, Tuple value) {
        try (FDBRecordContext context = sourceDatabase.openContext()) {
            byte[] key = path.toSubspace(context).pack(remainder);
            context.ensureActive().set(key, value.pack());
            context.commit();
        }
    }

    private void verifySingleKey(FDBDatabase database, KeySpacePath path, Tuple remainder, Tuple expected) {
        try (FDBRecordContext context = database.openContext()) {
            byte[] key = path.toSubspace(context).pack(remainder);
            assertEquals(expected, Tuple.fromBytes(context.ensureActive().get(key).join()));
        }
    }

    private static void setInPath(final KeySpacePath path, final FDBRecordContext context,
                                  final Tuple remainder, final Object value) {
        byte[] key = path.toSubspace(context).pack(remainder);
        context.ensureActive().set(key, Tuple.from(value).pack());
    }

    private static Tuple getTupleFromPath(final FDBRecordContext context, final KeySpacePath path, final Tuple remainder) {
        byte[] key = path.toSubspace(context).pack(remainder);
        return Tuple.fromBytes(context.ensureActive().get(key).join());
    }

    private void copyData(final KeySpacePath sourcePath, KeySpacePath destinationPath) {
        // Export the data
        final List<DataInKeySpacePath> exportedData = getExportedData(sourcePath);

        if (sourceDatabase == destinationDatabase) {
            // Clear the data and import it back
            clearSourcePath(sourcePath);
        }

        // Import the data
        importData(destinationPath, exportedData);
    }

    private void importData(final KeySpacePath path, final List<DataInKeySpacePath> exportedData) {
        try (FDBRecordContext context = destinationDatabase.openContext()) {
            path.importData(context, exportedData).join();
            context.commit();
        }
    }

    private void assertBadImport(KeySpacePath sourcePath, KeySpacePath destinationPath) {
        List<DataInKeySpacePath> exportedData = getExportedData(sourcePath);
        assertBadImport(destinationPath, exportedData);
    }

    private void assertBadImport(final KeySpacePath path, final List<DataInKeySpacePath> invalidData) {
        try (FDBRecordContext context = destinationDatabase.openContext()) {
            Assertions.assertThatThrownBy(() -> path.importData(context, invalidData).join())
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(RecordCoreIllegalImportDataException.class);
        }
    }

    private void clearSourcePath(final KeySpacePath path) {
        try (FDBRecordContext context = sourceDatabase.openContext()) {
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
        try (FDBRecordContext context = sourceDatabase.openContext()) {
            return path.exportAllData(context, null, ScanProperties.FORWARD_SCAN)
                    .asList().join();
        }
    }
}
