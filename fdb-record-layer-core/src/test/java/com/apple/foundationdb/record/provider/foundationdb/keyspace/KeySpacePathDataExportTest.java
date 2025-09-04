/*
 * KeySpacePathDataExportTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorContinuation;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorStartContinuation;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory.KeyType;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the new KeySpacePath data export feature that fetches all data stored under a KeySpacePath
 * and returns it in a {@code RecordCursor<KeyValue>}.
 */
@Tag(Tags.RequiresFDB)
class KeySpacePathDataExportTest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();

    @Test
    void testExportAllDataFromSimplePath() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.STRING, UUID.randomUUID().toString())
                        .addSubdirectory(new KeySpaceDirectory("level1", KeyType.LONG)));

        final FDBDatabase database = dbExtension.getDatabase();
        
        // Store test data
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            KeySpacePath basePath = root.path("root");
            
            // Add data at different levels
            for (int i = 0; i < 5; i++) {
                Tuple key = basePath.add("level1", (long) i).toTuple(context);
                tr.set(key.pack(), Tuple.from("value" + i).pack());

                // Add some sub-data under each key
                for (int j = 0; j < 3; j++) {
                    Tuple subKey = key.add("sub" + j);
                    tr.set(subKey.pack(), Tuple.from("subvalue" + i + "_" + j).pack());
                }
            }
            context.commit();
        }
        
        // Export all data from the root path
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath rootPath = root.path("root");
            final List<KeyValue> allData = exportAllData(rootPath, context);

            // Should have 5 main entries + 15 sub-entries = 20 total
            assertEquals(20, allData.size());
            
            // Verify the data is sorted by key
            for (int i = 1; i < allData.size(); i++) {
                assertTrue(Tuple.fromBytes(allData.get(i - 1).getKey()).compareTo(
                          Tuple.fromBytes(allData.get(i).getKey())) < 0);
            }
        }
    }

    @Test
    void testExportAllDataFromSpecificSubPath() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("app", KeyType.STRING, UUID.randomUUID().toString())
                        .addSubdirectory(new KeySpaceDirectory("user", KeyType.LONG)
                                .addSubdirectory(new KeySpaceDirectory("data", KeyType.NULL))));

        final FDBDatabase database = dbExtension.getDatabase();
        
        // Store test data for multiple users
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            
            for (long userId = 1; userId <= 3; userId++) {
                KeySpacePath userPath = root.path("app").add("user", userId);
                KeySpacePath dataPath = userPath.add("data");
                
                // Add data for each user
                for (int i = 0; i < 4; i++) {
                    Tuple key = dataPath.toTuple(context).add("record" + i);
                    tr.set(key.pack(), Tuple.from("user" + userId + "_data" + i).pack());
                }
            }
            context.commit();
        }
        
        // Export data only for user 2
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath user2Path = root.path("app").add("user", 2L);
            final List<KeyValue> userData = exportAllData(user2Path, context);

            // Should have 4 records for user 2
            assertEquals(4, userData.size());
            
            // Verify all data belongs to user 2
            for (KeyValue kv : userData) {
                String value = Tuple.fromBytes(kv.getValue()).getString(0);
                assertTrue(value.startsWith("user2_"));
            }
        }
    }

    @Test
    void testExportAllDataWithDirectoryLayer() {
        KeySpace root = new KeySpace(
                new DirectoryLayerDirectory("env", UUID.randomUUID().toString())
                        .addSubdirectory(new KeySpaceDirectory("tenant", KeyType.LONG)
                                .addSubdirectory(new DirectoryLayerDirectory("service"))));

        final FDBDatabase database = dbExtension.getDatabase();
        
        // Store test data
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            
            KeySpacePath basePath = root.path("env").add("tenant", 100L);
            
            // Add data for different services
            String[] services = {"auth", "storage", "compute"};
            for (String service : services) {
                KeySpacePath servicePath = basePath.add("service", service);
                Tuple serviceKey = servicePath.toTuple(context);
                
                for (int i = 0; i < 2; i++) {
                    tr.set(serviceKey.add("config" + i).pack(),
                            Tuple.from(service + "_config_" + i).pack());
                }
            }
            context.commit();
        }
        
        // Export all data from tenant path
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath tenantPath = root.path("env").add("tenant", 100L);
            final List<KeyValue> allData = exportAllData(tenantPath, context);

            // Should have 6 records (3 services * 2 configs each)
            assertEquals(6, allData.size());
            
            // Verify we have data for all three services
            Set<String> serviceNames = new HashSet<>();
            for (KeyValue kv : allData) {
                String value = Tuple.fromBytes(kv.getValue()).getString(0);
                String serviceName = value.split("_")[0];
                serviceNames.add(serviceName);
            }
            assertEquals(3, serviceNames.size());
            assertTrue(serviceNames.containsAll(Arrays.asList("auth", "storage", "compute")));
        }
    }

    @Test
    void testExportAllDataWithDifferentKeyTypes() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("mixed", KeyType.STRING, UUID.randomUUID().toString())
                        .addSubdirectory(new KeySpaceDirectory("strings", KeyType.STRING))
                        .addSubdirectory(new KeySpaceDirectory("longs", KeyType.LONG))
                        .addSubdirectory(new KeySpaceDirectory("bytes", KeyType.BYTES))
                        .addSubdirectory(new KeySpaceDirectory("uuids", KeyType.UUID))
                        .addSubdirectory(new KeySpaceDirectory("booleans", KeyType.BOOLEAN)));

        final FDBDatabase database = dbExtension.getDatabase();
        
        // Store test data with different key types
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath basePath = root.path("mixed");
            
            // String keys (str0, str1, str2 -> string_value_0, string_value_1, string_value_2)
            setData(List.of("str0", "str1", "str2"), context, basePath, "strings", "string_value_");
            
            // Long keys (10, 11, 12 -> long_value_10, long_value_11, long_value_12)
            setData(List.of(10L, 11L, 12L), context, basePath, "longs", "long_value_");
            
            // Bytes keys (arrays -> bytes_value_[0, 1], bytes_value_[1, 2])
            setData(List.of(new byte[]{0, 1}, new byte[]{1, 2}), context, basePath, "bytes", "bytes_value_");
            
            // UUID keys (UUIDs -> uuid_value_UUID)
            setData(List.of(new UUID(0, 0), new UUID(1, 1)), context, basePath, "uuids", "uuid_value_");
            
            // Boolean keys (true, false -> boolean_value_true, boolean_value_false)
            setData(List.of(true, false), context, basePath, "booleans", "boolean_value_");
            
            context.commit();
        }
        
        // Export all data and verify different key types
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath mixedPath = root.path("mixed");
            final List<KeyValue> allData = exportAllData(mixedPath, context);

            // Should have 12 records total (3+3+2+2+2)
            assertEquals(12, allData.size());
            
            // Verify we have different value types
            List<String> valueTypes = new ArrayList<>();
            for (KeyValue kv : allData) {
                String value = Tuple.fromBytes(kv.getValue()).getString(0);
                String valueType = value.split("_")[0];
                if (!valueTypes.contains(valueType)) {
                    valueTypes.add(valueType);
                }
            }
            assertEquals(5, valueTypes.size());
            assertTrue(valueTypes.containsAll(Arrays.asList("string", "long", "bytes", "uuid", "boolean")));
        }
    }

    @Test
    void testExportAllDataWithConstantValues() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("app", KeyType.STRING, UUID.randomUUID().toString())
                        .addSubdirectory(new KeySpaceDirectory("version", KeyType.LONG, 1L)
                        .addSubdirectory(new KeySpaceDirectory("data", KeyType.STRING, "records"))));

        final FDBDatabase database = dbExtension.getDatabase();
        
        // Store test data using constant values
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            
            KeySpacePath dataPath = root.path("app").add("version").add("data");
            Tuple baseKey = dataPath.toTuple(context);
            
            // Add multiple records under the constant path
            for (int i = 0; i < 4; i++) {
                tr.set(baseKey.add("record" + i).pack(),
                        Tuple.from("constant_path_data_" + i).pack());
            }
            context.commit();
        }
        
        // Export data from path with constant values
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath appPath = root.path("app");
            final List<KeyValue> allData = exportAllData(appPath, context);

            // Should have 4 records
            assertEquals(4, allData.size());
            
            // Verify all data has expected prefix
            for (KeyValue kv : allData) {
                String value = Tuple.fromBytes(kv.getValue()).getString(0);
                assertTrue(value.startsWith("constant_path_data_"));
            }
        }
    }

    @Test
    void testExportAllDataEmpty() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("empty", KeyType.STRING, UUID.randomUUID().toString())
                        .addSubdirectory(new KeySpaceDirectory("level1", KeyType.LONG)));

        final FDBDatabase database = dbExtension.getDatabase();
        
        // Don't store any data
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath emptyPath = root.path("empty");
            final List<KeyValue> allData = exportAllData(emptyPath, context);

            // Should be empty
            assertEquals(0, allData.size());
        }
    }

    @Test
    void testExportAllDataWithDeepNestedStructure() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("org", KeyType.STRING, UUID.randomUUID().toString())
                        .addSubdirectory(new KeySpaceDirectory("dept", KeyType.STRING)
                                .addSubdirectory(new KeySpaceDirectory("team", KeyType.LONG)
                                        .addSubdirectory(new KeySpaceDirectory("member", KeyType.UUID)
                                                .addSubdirectory(new KeySpaceDirectory("data", KeyType.NULL))))));

        final FDBDatabase database = dbExtension.getDatabase();
        
        // Create deep nested structure
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            
            String[] departments = {"engineering", "sales"};
            for (String dept : departments) {
                for (long team = 1; team <= 2; team++) {
                    for (int member = 0; member < 2; member++) {
                        UUID memberId = new UUID(dept.hashCode(), team * 100 + member);
                        KeySpacePath memberPath = root.path("org")
                                .add("dept", dept)
                                .add("team", team)
                                .add("member", memberId)
                                .add("data");
                        
                        Tuple key = memberPath.toTuple(context);
                        tr.set(key.add("profile").pack(),
                                Tuple.from(dept + "_team" + team + "_member" + member).pack());
                        tr.set(key.add("settings").pack(),
                                Tuple.from("settings_" + member).pack());
                    }
                }
            }
            context.commit();
        }
        
        // Export all data from organization root
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath orgPath = root.path("org");
            final List<KeyValue> allData = exportAllData(orgPath, context);

            // Should have 16 records (2 depts * 2 teams * 2 members * 2 records each)
            assertEquals(16, allData.size());
        }
        
        // Export data from specific department
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath engPath = root.path("org").add("dept", "engineering");
            final List<KeyValue> allData = exportAllData(engPath, context);

            // Should have 8 records (1 dept * 2 teams * 2 members * 2 records each)
            assertEquals(8, allData.size());
            
            // Verify all belong to engineering
            for (KeyValue kv : allData) {
                String value = Tuple.fromBytes(kv.getValue()).getString(0);
                if (value.startsWith("engineering_")) {
                    assertTrue(value.contains("engineering_"));
                }
            }
        }
    }

    @Test
    void testExportAllDataWithBinaryData() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("binary", KeyType.STRING, UUID.randomUUID().toString())
                        .addSubdirectory(new KeySpaceDirectory("blob", KeyType.BYTES)));

        final FDBDatabase database = dbExtension.getDatabase();
        
        // Store binary data
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            KeySpacePath basePath = root.path("binary");
            
            // Store different types of binary data
            byte[][] binaryKeys = {
                {0x00, 0x01, 0x02},
                {(byte) 0xFF, (byte) 0xFE, (byte) 0xFD},
                {0x7F, 0x00, (byte) 0x80}
            };
            
            for (int i = 0; i < binaryKeys.length; i++) {
                Tuple key = basePath.add("blob", binaryKeys[i]).toTuple(context);
                byte[] value = ("binary_data_" + i).getBytes();
                tr.set(key.pack(), value);
            }
            context.commit();
        }
        
        // Export binary data
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath binaryPath = root.path("binary");
            final List<KeyValue> allData = exportAllData(binaryPath, context);

            assertEquals(3, allData.size());
            
            // Verify binary data integrity
            for (int i = 0; i < allData.size(); i++) {
                KeyValue kv = allData.get(i);
                String valueStr = new String(kv.getValue());
                assertTrue(valueStr.startsWith("binary_data_"));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 30})
    void testExportAllDataWithContinuation(int limit) {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("continuation", KeyType.STRING, UUID.randomUUID().toString())
                        .addSubdirectory(new KeySpaceDirectory("item", KeyType.LONG)));

        final FDBDatabase database = dbExtension.getDatabase();
        
        // Store test data
        final List<List<Tuple>> expectedBatches = new ArrayList<>();
        expectedBatches.add(new ArrayList<>());
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            KeySpacePath basePath = root.path("continuation");

            IntStream.range(0, 20).forEach(i -> {
                Tuple key = basePath.add("item", (long)i).toTuple(context);
                final Tuple value = Tuple.from("continuation_item_" + i);
                tr.set(key.pack(), value.pack());
                if (expectedBatches.get(expectedBatches.size() - 1).size() == limit) {
                    expectedBatches.add(new ArrayList<>());
                }
                expectedBatches.get(expectedBatches.size() - 1).add(value);
            });
            context.commit();
        }
        if (20 % limit == 0) {
            expectedBatches.add(List.of());
        }
        
        // Export with continuation support
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath continuationPath = root.path("continuation");

            final ScanProperties scanProperties = ScanProperties.FORWARD_SCAN.with(props -> props.setReturnedRowLimit(limit));
            List<List<Tuple>> actual = new ArrayList<>();
            RecordCursorContinuation continuation = RecordCursorStartContinuation.START;
            while (!continuation.isEnd()) {
                final RecordCursor<DataInKeySpacePath> cursor = continuationPath.exportAllData(context, continuation.toBytes(),
                        scanProperties);
                final AtomicReference<RecordCursorResult<Tuple>> tupleResult = new AtomicReference<>();
                final List<Tuple> batch = cursor.map(dataInPath -> {
                    KeyValue kv = dataInPath.getRawKeyValue();
                    return Tuple.fromBytes(kv.getValue());
                }).asList(tupleResult).join();
                actual.add(batch);
                continuation = tupleResult.get().getContinuation();
            }
            assertEquals(expectedBatches, actual);
        }
    }

    private void setData(List<Object> keys, FDBRecordContext context, KeySpacePath basePath,
                         String subdirectory, String valuePrefix) {
        Transaction tr = context.ensureActive();
        for (int i = 0; i < keys.size(); i++) {
            Tuple tuple = basePath.add(subdirectory, keys.get(i)).toTuple(context);
            tr.set(tuple.pack(), Tuple.from(valuePrefix + i).pack());
        }
    }

    private static List<KeyValue> exportAllData(final KeySpacePath rootPath, final FDBRecordContext context) {
        final List<KeyValue> asSingleExport = rootPath.exportAllData(context, null, ScanProperties.FORWARD_SCAN)
                .map(DataInKeySpacePath::getRawKeyValue).asList().join();

        final List<KeyValue> reversed = rootPath.exportAllData(context, null, ScanProperties.REVERSE_SCAN)
                .map(DataInKeySpacePath::getRawKeyValue).asList().join();
        Collections.reverse(reversed);
        assertEquals(asSingleExport, reversed);

        final ScanProperties scanProperties = ScanProperties.FORWARD_SCAN.with(props -> props.setReturnedRowLimit(1));
        List<KeyValue> asContinuations = new ArrayList<>();
        RecordCursorContinuation continuation = RecordCursorStartContinuation.START;
        while (!continuation.isEnd()) {
            final RecordCursor<DataInKeySpacePath> cursor = rootPath.exportAllData(context, continuation.toBytes(),
                    scanProperties);
            final AtomicReference<RecordCursorResult<KeyValue>> keyValueResult = new AtomicReference<>();
            final List<KeyValue> batch = cursor.map(DataInKeySpacePath::getRawKeyValue).asList(keyValueResult).join();
            asContinuations.addAll(batch);
            continuation = keyValueResult.get().getContinuation();
            if (keyValueResult.get().hasNext()) {
                assertEquals(1, batch.size());
            } else {
                assertThat(batch.size()).isLessThanOrEqualTo(1);
            }
        }
        assertEquals(asSingleExport, asContinuations);
        return asSingleExport;
    }

    @Test
    void testExportAllDataThroughKeySpacePathWrapper() {
        final FDBDatabase database = dbExtension.getDatabase();
        final EnvironmentKeySpace keySpace = setupEnvironmentKeySpaceData(database);

        // Test export at different levels through wrapper methods
        try (FDBRecordContext context = database.openContext()) {
            // Export from root level (should get all data)
            EnvironmentKeySpace.EnvironmentRoot root = keySpace.root();
            List<KeyValue> allData = exportAllData(root, context);
            assertEquals(5, allData.size(), "Root level should export all data");
            
            // Export from specific user level (should get data for user 100 only)
            EnvironmentKeySpace.UserPath user100Path = keySpace.root().userid(100L);
            verifyExtractedData(exportAllData(user100Path, context),
                    4, "User 100 should have 4 records",
                    "user100", "All user 100 data should contain 'user100'");

            // Export from specific application level (app1 for user 100)
            EnvironmentKeySpace.ApplicationPath app1User100 = user100Path.application("app1");
            verifyExtractedData(exportAllData(app1User100, context),
                    3, "App1 for user 100 should have 3 records (2 data + 1 metadata)",
                    "user100_app1", "All app1 user100 data should contain 'user100_app1'");

            // Export from specific data store level
            EnvironmentKeySpace.DataPath dataStore = app1User100.dataStore();
            List<KeyValue> dataStoreData = exportAllData(dataStore, context);
            verifyExtractedData(dataStoreData,
                    2, "Data store should have exactly 2 records",
                    "user100_app1_data", "Data should be from user100 app1 data store");
            
            // Export from metadata store level
            EnvironmentKeySpace.MetadataPath metadataStore = app1User100.metadataStore();
            verifyExtractedData(exportAllData(metadataStore, context),
                    1, "Metadata store should have exactly 1 record",
                    "user100_app1_meta1", "Metadata value should match");

            // Verify empty export for user with no data
            EnvironmentKeySpace.UserPath user300Path = keySpace.root().userid(300L);
            assertEquals(0, exportAllData(user300Path, context).size(), "User 300 should have no data");
        }
    }

    @Test
    void testExportAllDataThroughKeySpacePathWrapperResolvedPaths() {
        final FDBDatabase database = dbExtension.getDatabase();
        final EnvironmentKeySpace keySpace = setupEnvironmentKeySpaceData(database);

        // Test export at different levels through wrapper methods
        try (FDBRecordContext context = database.openContext()) {
            // Test 4: Export from specific data store level
            EnvironmentKeySpace.DataPath dataStore = keySpace.root().userid(100L).application("app1").dataStore();
            final List<ResolvedKeySpacePath> dataStoreData = dataStore.exportAllData(context, null, ScanProperties.FORWARD_SCAN)
                    .mapPipelined(DataInKeySpacePath::getResolvedPath, 1).asList().join();
            assertEquals(2, dataStoreData.size());
            // Verify data store records have correct remainder
            final ArrayList<Tuple> remainders = new ArrayList<>();
            for (ResolvedKeySpacePath kv : dataStoreData) {

                // Path tuple should be the same
                Tuple dataStoreTuple = dataStore.toTuple(context);
                assertEquals(dataStoreTuple, kv.toTuple());

                // Remainder should be the same
                remainders.add(kv.getRemainder());
            }
            assertEquals(List.of(
                    Tuple.from("record1"),
                    Tuple.from("record2", 0),
                    Tuple.from("record2", 1)
            ), remainders);

        }
    }

    @Nonnull
    private static EnvironmentKeySpace setupEnvironmentKeySpaceData(@Nonnull final FDBDatabase database) {
        EnvironmentKeySpace keySpace = new EnvironmentKeySpace("test_env");

        // Store test data at different levels of the hierarchy
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            
            // Create paths for different users and applications
            EnvironmentKeySpace.ApplicationPath app1User1 = keySpace.root().userid(100L).application("app1");
            EnvironmentKeySpace.ApplicationPath app2User1 = keySpace.root().userid(100L).application("app2");
            EnvironmentKeySpace.ApplicationPath app1User2 = keySpace.root().userid(200L).application("app1");
            
            EnvironmentKeySpace.DataPath dataUser1App1 = app1User1.dataStore();
            EnvironmentKeySpace.MetadataPath metaUser1App1 = app1User1.metadataStore();
            EnvironmentKeySpace.DataPath dataUser1App2 = app2User1.dataStore();
            EnvironmentKeySpace.DataPath dataUser2App1 = app1User2.dataStore();
            
            // Store data records with additional tuple elements after the KeySpacePath
            tr.set(dataUser1App1.toTuple(context).add("record1").pack(), Tuple.from("user100_app1_data1").pack());
            tr.set(dataUser1App1.toTuple(context).add("record2").add(0).pack(), Tuple.from("user100_app1_data2_0").pack());
            tr.set(dataUser1App1.toTuple(context).add("record2").add(1).pack(), Tuple.from("user100_app1_data2_1").pack());
            tr.set(metaUser1App1.toTuple(context).add("config1").pack(), Tuple.from("user100_app1_meta1").pack());
            tr.set(dataUser1App2.toTuple(context).add("record3").pack(), Tuple.from("user100_app2_data3").pack());
            tr.set(dataUser2App1.toTuple(context).add("record4").pack(), Tuple.from("user200_app1_data4").pack());
            
            context.commit();
        }
        return keySpace;
    }

    private static void verifyExtractedData(final List<KeyValue> app1User100Data,
                                            int expectedCount, String expectedCountMessage,
                                            String expectedValueContents, String contentMessage) {
        assertEquals(expectedCount, app1User100Data.size(), expectedCountMessage);

        for (KeyValue kv : app1User100Data) {
            String value = Tuple.fromBytes(kv.getValue()).getString(0);
            assertTrue(value.contains(expectedValueContents), contentMessage);
        }
    }

}
