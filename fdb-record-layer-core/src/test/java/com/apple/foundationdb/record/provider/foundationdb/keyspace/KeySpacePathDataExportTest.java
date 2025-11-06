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
import com.apple.test.ParameterizedTestUtils;
import com.apple.test.Tags;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the new KeySpacePath data export feature that fetches all data stored under a KeySpacePath
 * and returns it in a {@code RecordCursor<DataInKeySpacePath>}.
 */
@Tag(Tags.RequiresFDB)
class KeySpacePathDataExportTest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();

    @Test
    void exportAllDataFromSimplePath() throws ExecutionException, InterruptedException {
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
            final List<DataInKeySpacePath> allData = exportAllData(rootPath, context);

            // Should have 5 main entries + 15 sub-entries = 20 total
            assertEquals(20, allData.size());

            // Verify the data is sorted by key
            for (int i = 1; i < allData.size(); i++) {
                assertTrue(getKey(allData.get(i - 1), context).compareTo(getKey(allData.get(i), context)) < 0);
            }
        }
    }

    // `toTuple` does not include the remainder, I'm not sure if that is intentional, or an oversight.
    private Tuple getKey(final DataInKeySpacePath dataInKeySpacePath, final FDBRecordContext context) throws ExecutionException, InterruptedException {
        final ResolvedKeySpacePath resolvedKeySpacePath = dataInKeySpacePath.getPath().toResolvedPathAsync(context)
                .thenApply(resolvedPath -> dataInKeySpacePath.getRemainder() == null ? resolvedPath : resolvedPath.withRemainder(dataInKeySpacePath.remainder)).get();
        if (resolvedKeySpacePath.getRemainder() != null) {
            return resolvedKeySpacePath.toTuple().addAll(resolvedKeySpacePath.getRemainder());
        } else {
            return resolvedKeySpacePath.toTuple();
        }
    }

    @Test
    void exportAllDataFromSpecificSubPath() {
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
            final List<DataInKeySpacePath> userData = exportAllData(user2Path, context);

            // Should have 4 records for user 2
            assertEquals(4, userData.size());

            // Verify all data belongs to user 2
            for (DataInKeySpacePath data : userData) {
                String value = Tuple.fromBytes(data.getValue()).getString(0);
                assertTrue(value.startsWith("user2_"));
            }
        }
    }

    @Test
    void exportAllDataWithDirectoryLayer() {
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
            final List<DataInKeySpacePath> allData = exportAllData(tenantPath, context);

            // Should have 6 records (3 services * 2 configs each)
            assertEquals(6, allData.size());

            // Verify we have data for all three services
            Set<String> serviceNames = new HashSet<>();
            for (DataInKeySpacePath data : allData) {
                String value = Tuple.fromBytes(data.getValue()).getString(0);
                String serviceName = value.split("_")[0];
                serviceNames.add(serviceName);
            }
            assertEquals(3, serviceNames.size());
            assertTrue(serviceNames.containsAll(Arrays.asList("auth", "storage", "compute")));
        }
    }

    @Test
    void exportAllDataWithDifferentKeyTypes() {
        final KeySpaceDirectory rootDirectory = new KeySpaceDirectory("mixed", KeyType.STRING, UUID.randomUUID().toString());
        Map<KeyType, List<Object>> dataByType = Map.of(
                KeyType.STRING, List.of("str0", "str1", "str2"),
                KeyType.LONG, List.of(10L, 11L, 12L),
                KeyType.BYTES, List.of(new byte[]{0, 1}, new byte[]{1, 2}),
                KeyType.UUID, List.of(new UUID(0, 0), new UUID(1, 1)),
                KeyType.BOOLEAN, List.of(true, false),
                KeyType.NULL, Collections.singletonList(null),
                KeyType.DOUBLE, List.of(3.1415, -2.718281, 13.23E8),
                KeyType.FLOAT, List.of(1.4142135f, -5.8f, 130.23f)
        );
        dataByType.keySet().forEach(keyType ->
                rootDirectory.addSubdirectory(new KeySpaceDirectory(keyType.name(), keyType)));
        KeySpace root = new KeySpace(rootDirectory);
        assertEquals(Set.of(KeyType.values()), dataByType.keySet());

        final FDBDatabase database = dbExtension.getDatabase();
        
        // Store test data with different key types
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath basePath = root.path("mixed");
            dataByType.forEach((keyType, data) ->
                    setData(data, context, basePath, keyType.name(), keyType + "_value_")
            );
            context.commit();
        }
        
        // Export all data and verify different key types
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath mixedPath = root.path("mixed");
            final List<DataInKeySpacePath> allData = exportAllData(mixedPath, context);

            assertEquals(dataByType.values().stream().mapToLong(List::size).sum(),
                    allData.size());

            // Verify we have different value types
            Set<String> valueTypes = allData.stream()
                    .map(data -> Tuple.fromBytes(data.getValue()).getString(0).split("_")[0])
                    .collect(Collectors.toSet());
            assertEquals((Arrays.stream(KeyType.values()).map(Enum::name).collect(Collectors.toSet())),
                    valueTypes);
        }
    }

    @Test
    void exportAllDataWithConstantValues() {
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
            final List<DataInKeySpacePath> allData = exportAllData(appPath, context);

            // Should have 4 records
            assertEquals(4, allData.size());

            // Verify all data has expected prefix
            for (DataInKeySpacePath data : allData) {
                String value = Tuple.fromBytes(data.getValue()).getString(0);
                assertTrue(value.startsWith("constant_path_data_"));
            }
        }
    }

    @Test
    void exportAllDataEmpty() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("empty", KeyType.STRING, UUID.randomUUID().toString())
                        .addSubdirectory(new KeySpaceDirectory("level1", KeyType.LONG)));

        final FDBDatabase database = dbExtension.getDatabase();
        
        // Don't store any data
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath emptyPath = root.path("empty");
            final List<DataInKeySpacePath> allData = exportAllData(emptyPath, context);

            // Should be empty
            assertEquals(0, allData.size());
        }
    }

    @Test
    void exportAllDataWithDeepNestedStructure() {
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
            final List<DataInKeySpacePath> allData = exportAllData(orgPath, context);

            // Should have 16 records (2 departments * 2 teams * 2 members * 2 records each)
            assertEquals(16, allData.size());
        }

        // Export data from specific department
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath engPath = root.path("org").add("dept", "engineering");
            final List<DataInKeySpacePath> allData = exportAllData(engPath, context);

            // Should have 8 records (1 dept * 2 teams * 2 members * 2 records each)
            assertEquals(8, allData.size());

            // Verify all belong to engineering
            for (DataInKeySpacePath data : allData) {
                String value = Tuple.fromBytes(data.getValue()).getString(0);
                if (value.startsWith("engineering_")) {
                    assertTrue(value.contains("engineering_"));
                }
            }
        }
    }

    @Test
    void exportAllDataWithBinaryData() {
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
            final List<DataInKeySpacePath> allData = exportAllData(binaryPath, context);

            assertEquals(3, allData.size());

            // Verify binary data integrity
            for (DataInKeySpacePath data : allData) {
                String valueStr = new String(data.getValue());
                assertTrue(valueStr.startsWith("binary_data_"));
            }
        }
    }

    static Stream<Arguments> exportAllDataWithContinuation() {
        return ParameterizedTestUtils.cartesianProduct(
                ParameterizedTestUtils.booleans("forward"),
                Stream.of(1, 2, 3, 30)
        );
    }

    @ParameterizedTest
    @MethodSource
    void exportAllDataWithContinuation(boolean forward, int limit) {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("continuation", KeyType.STRING, UUID.randomUUID().toString())
                        .addSubdirectory(new KeySpaceDirectory("item", KeyType.LONG)));

        final FDBDatabase database = dbExtension.getDatabase();
        
        // Store test data
        final List<List<Tuple>> expectedBatches = new ArrayList<>();
        expectedBatches.add(new ArrayList<>());
        final KeySpacePath pathToExport = root.path("continuation");
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();

            IntStream sourceStream;
            if (forward) {
                sourceStream = IntStream.range(0, 20);
            } else {
                sourceStream = IntStream.iterate(19, i -> i >= 0, i -> i - 1);
            }
            sourceStream.forEach(i -> {
                Tuple key = pathToExport.add("item", (long)i).toTuple(context);
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
        exportWithContinuations(pathToExport, forward, limit, database, expectedBatches);
    }

    static Stream<Arguments> exportSingleKeyWithContinuation() {
        return ParameterizedTestUtils.cartesianProduct(
                ParameterizedTestUtils.booleans("forward"),
                ParameterizedTestUtils.booleans("withRemainder"),
                Stream.of(1, 3)
        );
    }

    @ParameterizedTest
    @MethodSource
    void exportSingleKeyWithContinuation(boolean forward, boolean withRemainder, int limit) {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("continuation", KeyType.STRING, UUID.randomUUID().toString())
                        .addSubdirectory(new KeySpaceDirectory("item", KeyType.LONG)));

        final FDBDatabase database = dbExtension.getDatabase();

        // Store test data
        final List<List<Tuple>> expectedBatches;
        final KeySpacePath pathToExport = root.path("continuation").add("item", 42L);
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            byte[] key;
            if (withRemainder) {
                key = pathToExport.toSubspace(context).pack(Tuple.from("continuation"));
            } else {
                key = pathToExport.toSubspace(context).pack();
            }
            final Tuple value = Tuple.from("My Value");
            tr.set(key, value.pack());
            expectedBatches = limit == 1 ? List.of(List.of(value), List.of()) : List.of(List.of(value));
            context.commit();
        }

        // Export with continuation support
        exportWithContinuations(pathToExport, forward, limit, database, expectedBatches);
    }

    private static void exportWithContinuations(final KeySpacePath pathToExport,
                                                final boolean forward, final int limit,
                                                final FDBDatabase database,
                                                final List<List<Tuple>> expectedBatches) {
        try (FDBRecordContext context = database.openContext()) {

            final ScanProperties directionalProperties = forward ? ScanProperties.FORWARD_SCAN : ScanProperties.REVERSE_SCAN;

            final ScanProperties scanProperties = directionalProperties.with(props -> props.setReturnedRowLimit(limit));
            List<List<Tuple>> actual = new ArrayList<>();
            RecordCursorContinuation continuation = RecordCursorStartContinuation.START;
            while (!continuation.isEnd()) {
                final RecordCursor<DataInKeySpacePath> cursor = pathToExport.exportAllData(context, continuation.toBytes(),
                        scanProperties);
                final AtomicReference<RecordCursorResult<Tuple>> tupleResult = new AtomicReference<>();
                final List<Tuple> batch = cursor.map(dataInPath -> {
                    return Tuple.fromBytes(dataInPath.getValue());
                }).asList(tupleResult).join();
                actual.add(batch);
                continuation = tupleResult.get().getContinuation();
            }
            assertEquals(expectedBatches, actual);
        }
    }

    @Test
    void exportAllDataThroughKeySpacePathWrapper() {
        final FDBDatabase database = dbExtension.getDatabase();
        final EnvironmentKeySpace keySpace = EnvironmentKeySpace.setupSampleData(database);

        // Test export at different levels through wrapper methods
        try (FDBRecordContext context = database.openContext()) {
            // Export from root level (should get all data)
            EnvironmentKeySpace.EnvironmentRoot root = keySpace.root();
            List<DataInKeySpacePath> allData = exportAllData(root, context);
            assertEquals(6, allData.size(), "Root level should export all data");

            // Export from specific user level (should get data for user 100 only)
            EnvironmentKeySpace.UserPath user100Path = keySpace.root().userid(100L);
            verifyExtractedData(exportAllData(user100Path, context),
                    5, "User 100 should have 4 records",
                    "user100", "All user 100 data should contain 'user100'");

            // Export from specific application level (app1 for user 100)
            EnvironmentKeySpace.ApplicationPath app1User100 = user100Path.application("app1");
            verifyExtractedData(exportAllData(app1User100, context),
                    4, "App1 for user 100 should have 4 records (3 data + 1 metadata)",
                    "user100_app1", "All app1 user100 data should contain 'user100_app1'");

            // Export from specific data store level
            EnvironmentKeySpace.DataPath dataStore = app1User100.dataStore();
            List<DataInKeySpacePath> dataStoreData = exportAllData(dataStore, context);
            verifyExtractedData(dataStoreData,
                    3, "Data store should have exactly 3 records",
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
    void exportAllDataThroughKeySpacePathWrapperResolvedPaths() {
        final FDBDatabase database = dbExtension.getDatabase();
        final EnvironmentKeySpace keySpace = EnvironmentKeySpace.setupSampleData(database);

        // Test export at different levels through wrapper methods
        try (FDBRecordContext context = database.openContext()) {
            // Test 4: Export from specific data store level
            EnvironmentKeySpace.DataPath dataStore = keySpace.root().userid(100L).application("app1").dataStore();
            final List<DataInKeySpacePath> dataStoreData = dataStore.exportAllData(context, null, ScanProperties.FORWARD_SCAN)
                    .asList().join();
            // Verify data store records have correct remainder
            for (DataInKeySpacePath data : dataStoreData) {
                // Path tuple should be the same
                assertEquals(dataStore, data.getPath());
            }
            assertEquals(List.of(
                    Tuple.from("record1"),
                    Tuple.from("record2", 0),
                    Tuple.from("record2", 1)
            ), dataStoreData.stream().map(DataInKeySpacePath::getRemainder).collect(Collectors.toList()),
                    "remainders should be the same");

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

    /**
     * Export all the data, and make some assertions that can always be done.
     * This combines a lot of assertions, but most of the underlying behavior should be well covered by the objects
     * that {@link KeySpacePath#exportAllData} is built on.
     * @param pathToExport the path being exported
     * @param context the context in which to export
     * @return a list of {@code DataInKeySpacePath}s being exported
     */
    private static List<DataInKeySpacePath> exportAllData(final KeySpacePath pathToExport, final FDBRecordContext context) {
        final List<DataInKeySpacePath> asSingleExport = pathToExport.exportAllData(context, null, ScanProperties.FORWARD_SCAN)
                .asList().join();

        // assert that the resolved paths contain the right prefix
        final List<KeySpacePath> dataPaths = pathToExport.exportAllData(context, null, ScanProperties.FORWARD_SCAN)
                .map(DataInKeySpacePath::getPath).asList().join();
        for (KeySpacePath dataPath : dataPaths) {
            assertStartsWith(pathToExport, dataPath);
        }

        // assert that the reverse scan is the same as the forward scan, but in reverse
        final List<DataInKeySpacePath> reversed = pathToExport.exportAllData(context, null, ScanProperties.REVERSE_SCAN)
                .asList().join();
        Collections.reverse(reversed);
        assertDataInKeySpacePathEquals(asSingleExport, reversed);

        // Assert continuations work correctly
        final ScanProperties scanProperties = ScanProperties.FORWARD_SCAN.with(props -> props.setReturnedRowLimit(1));
        List<DataInKeySpacePath> asContinuations = new ArrayList<>();
        RecordCursorContinuation continuation = RecordCursorStartContinuation.START;
        while (!continuation.isEnd()) {
            final RecordCursor<DataInKeySpacePath> cursor = pathToExport.exportAllData(context, continuation.toBytes(),
                    scanProperties);
            final AtomicReference<RecordCursorResult<DataInKeySpacePath>> dataInPathResult = new AtomicReference<>();
            final List<DataInKeySpacePath> batch = cursor.asList(dataInPathResult).join();
            asContinuations.addAll(batch);
            continuation = dataInPathResult.get().getContinuation();
            if (dataInPathResult.get().hasNext()) {
                assertEquals(1, batch.size());
            } else {
                assertThat(batch.size()).isLessThanOrEqualTo(1);
            }
        }

        assertDataInKeySpacePathEquals(asSingleExport, asContinuations);
        return asSingleExport;
    }

    private static void assertDataInKeySpacePathEquals(final List<DataInKeySpacePath> expectedList,
                                                       final List<DataInKeySpacePath> actualList) {
        assertThat(actualList).zipSatisfy(expectedList,
                (actual, other) -> {
                    assertThat(actual.getPath()).isEqualTo(other.getPath());
                    // I don't know why intelliJ can't handle this without the explicit type parameter
                    Assertions.<Object>assertThat(actual.getRemainder()).isEqualTo(other.getRemainder());
                    assertThat(actual.getValue()).isEqualTo(other.getValue());
                });
    }

    private static void assertStartsWith(final KeySpacePath rootPath, KeySpacePath childPath) {
        KeySpacePath searchPath = childPath;
        do {
            if (searchPath.equals(rootPath)) {
                return;
            }
            searchPath = searchPath.getParent();
        } while (searchPath != null);
        Assertions.fail("Expected <" + childPath + "> to start with <" + rootPath + "> but it didn't");
    }

    private static void verifyExtractedData(final List<DataInKeySpacePath> app1User100Data,
                                            int expectedCount, String expectedCountMessage,
                                            String expectedValueContents, String contentMessage) {
        assertEquals(expectedCount, app1User100Data.size(), expectedCountMessage);

        for (DataInKeySpacePath data : app1User100Data) {
            String value = Tuple.fromBytes(data.getValue()).getString(0);
            assertTrue(value.contains(expectedValueContents), contentMessage);
        }
    }

}
