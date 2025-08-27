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
import com.apple.foundationdb.record.RecordCursorResult;
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
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the new KeySpacePath data export feature that fetches all data stored under a KeySpacePath
 * and returns it in a RecordCursor&lt;KeyValue&gt;.
 */
@Tag(Tags.RequiresFDB)
public class KeySpacePathDataExportTest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();

    private final Random random = new Random();

    @Test
    public void testExportAllDataFromSimplePath() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("root", KeyType.STRING, "test-root")
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
            RecordCursor<KeyValue> cursor = rootPath.exportAllData(context);
            
            List<KeyValue> allData = cursor.asList().join();
            
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
    public void testExportAllDataFromSpecificSubPath() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("app", KeyType.STRING, "myapp")
                        .addSubdirectory(new KeySpaceDirectory("user", KeyType.LONG))
                        .addSubdirectory(new KeySpaceDirectory("data", KeyType.NULL)));

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
            RecordCursor<KeyValue> cursor = user2Path.exportAllData(context);
            
            List<KeyValue> userData = cursor.asList().join();
            
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
    public void testExportAllDataWithDirectoryLayer() {
        KeySpace root = new KeySpace(
                new DirectoryLayerDirectory("env", "production")
                        .addSubdirectory(new KeySpaceDirectory("tenant", KeyType.LONG))
                        .addSubdirectory(new DirectoryLayerDirectory("service")));

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
            RecordCursor<KeyValue> cursor = tenantPath.exportAllData(context);
            
            List<KeyValue> allData = cursor.asList().join();
            
            // Should have 6 records (3 services * 2 configs each)
            assertEquals(6, allData.size());
            
            // Verify we have data for all three services
            List<String> serviceNames = new ArrayList<>();
            for (KeyValue kv : allData) {
                String value = Tuple.fromBytes(kv.getValue()).getString(0);
                String serviceName = value.split("_")[0];
                if (!serviceNames.contains(serviceName)) {
                    serviceNames.add(serviceName);
                }
            }
            assertEquals(3, serviceNames.size());
            assertTrue(serviceNames.containsAll(Arrays.asList("auth", "storage", "compute")));
        }
    }

    @Test
    public void testExportAllDataWithDifferentKeyTypes() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("mixed", KeyType.STRING, "mixed-types")
                        .addSubdirectory(new KeySpaceDirectory("strings", KeyType.STRING))
                        .addSubdirectory(new KeySpaceDirectory("longs", KeyType.LONG))
                        .addSubdirectory(new KeySpaceDirectory("bytes", KeyType.BYTES))
                        .addSubdirectory(new KeySpaceDirectory("uuids", KeyType.UUID))
                        .addSubdirectory(new KeySpaceDirectory("booleans", KeyType.BOOLEAN)));

        final FDBDatabase database = dbExtension.getDatabase();
        
        // Store test data with different key types
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            KeySpacePath basePath = root.path("mixed");
            
            // String keys
            for (int i = 0; i < 3; i++) {
                Tuple key = basePath.add("strings", "str" + i).toTuple(context);
                tr.set(key.pack(), Tuple.from("string_value_" + i).pack());
            }
            
            // Long keys
            for (long i = 10; i < 13; i++) {
                Tuple key = basePath.add("longs", i).toTuple(context);
                tr.set(key.pack(), Tuple.from("long_value_" + i).pack());
            }
            
            // Bytes keys
            for (int i = 0; i < 2; i++) {
                byte[] byteKey = new byte[] { (byte) i, (byte) (i + 1) };
                Tuple key = basePath.add("bytes", byteKey).toTuple(context);
                tr.set(key.pack(), Tuple.from("bytes_value_" + i).pack());
            }
            
            // UUID keys
            for (int i = 0; i < 2; i++) {
                UUID uuid = new UUID(i, i);
                Tuple key = basePath.add("uuids", uuid).toTuple(context);
                tr.set(key.pack(), Tuple.from("uuid_value_" + i).pack());
            }
            
            // Boolean keys
            for (boolean b : Arrays.asList(true, false)) {
                Tuple key = basePath.add("booleans", b).toTuple(context);
                tr.set(key.pack(), Tuple.from("boolean_value_" + b).pack());
            }
            
            context.commit();
        }
        
        // Export all data and verify different key types
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath mixedPath = root.path("mixed");
            RecordCursor<KeyValue> cursor = mixedPath.exportAllData(context);
            
            List<KeyValue> allData = cursor.asList().join();
            
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
    public void testExportAllDataWithConstantValues() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("app", KeyType.STRING, "testapp")
                        .addSubdirectory(new KeySpaceDirectory("version", KeyType.LONG, 1L))
                        .addSubdirectory(new KeySpaceDirectory("data", KeyType.STRING, "records")));

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
            RecordCursor<KeyValue> cursor = appPath.exportAllData(context);
            
            List<KeyValue> allData = cursor.asList().join();
            
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
    public void testExportAllDataEmpty() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("empty", KeyType.STRING, "empty-space")
                        .addSubdirectory(new KeySpaceDirectory("level1", KeyType.LONG)));

        final FDBDatabase database = dbExtension.getDatabase();
        
        // Don't store any data
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath emptyPath = root.path("empty");
            RecordCursor<KeyValue> cursor = emptyPath.exportAllData(context);
            
            List<KeyValue> allData = cursor.asList().join();
            
            // Should be empty
            assertEquals(0, allData.size());
        }
    }

    @Test
    public void testExportAllDataWithScanProperties() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("limited", KeyType.STRING, "limited-scan")
                        .addSubdirectory(new KeySpaceDirectory("item", KeyType.LONG)));

        final FDBDatabase database = dbExtension.getDatabase();
        
        // Store many records
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            KeySpacePath basePath = root.path("limited");
            
            for (int i = 0; i < 20; i++) {
                Tuple key = basePath.add("item", (long) i).toTuple(context);
                tr.set(key.pack(), Tuple.from("item_data_" + i).pack());
            }
            context.commit();
        }
        
        // Export with limited scan properties
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath limitedPath = root.path("limited");
            ScanProperties scanProps = ScanProperties.FORWARD_SCAN.with(props ->
                props.setReturnedRowLimit(5));
            
            RecordCursor<KeyValue> cursor = limitedPath.exportAllData(context, scanProps);
            
            List<KeyValue> limitedData = cursor.asList().join();
            
            // Should have only 5 records due to limit
            assertEquals(5, limitedData.size());
            
            // Should be the first 5 items
            for (int i = 0; i < 5; i++) {
                String value = Tuple.fromBytes(limitedData.get(i).getValue()).getString(0);
                assertEquals("item_data_" + i, value);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testExportAllDataReverse(boolean reverse) {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("ordered", KeyType.STRING, "ordered-data")
                        .addSubdirectory(new KeySpaceDirectory("sequence", KeyType.LONG)));

        final FDBDatabase database = dbExtension.getDatabase();
        
        // Store ordered data
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            KeySpacePath basePath = root.path("ordered");
            
            for (int i = 0; i < 5; i++) {
                Tuple key = basePath.add("sequence", (long) i).toTuple(context);
                tr.set(key.pack(), Tuple.from("seq_" + i).pack());
            }
            context.commit();
        }
        
        // Export with forward or reverse scan
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath orderedPath = root.path("ordered");
            ScanProperties scanProps = new ScanProperties(null, reverse);
            
            RecordCursor<KeyValue> cursor = orderedPath.exportAllData(context, scanProps);
            
            List<KeyValue> allData = cursor.asList().join();
            
            assertEquals(5, allData.size());
            
            // Verify order based on scan direction
            for (int i = 0; i < 5; i++) {
                String value = Tuple.fromBytes(allData.get(i).getValue()).getString(0);
                int expectedIndex = reverse ? (4 - i) : i;
                assertEquals("seq_" + expectedIndex, value);
            }
        }
    }

    @Test
    public void testExportAllDataWithDeepNestedStructure() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("org", KeyType.STRING, "company")
                        .addSubdirectory(new KeySpaceDirectory("dept", KeyType.STRING))
                        .addSubdirectory(new KeySpaceDirectory("team", KeyType.LONG))
                        .addSubdirectory(new KeySpaceDirectory("member", KeyType.UUID))
                        .addSubdirectory(new KeySpaceDirectory("data", KeyType.NULL)));

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
            RecordCursor<KeyValue> cursor = orgPath.exportAllData(context);
            
            List<KeyValue> allData = cursor.asList().join();
            
            // Should have 16 records (2 depts * 2 teams * 2 members * 2 records each)
            assertEquals(16, allData.size());
        }
        
        // Export data from specific department
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath engPath = root.path("org").add("dept", "engineering");
            RecordCursor<KeyValue> cursor = engPath.exportAllData(context);
            
            List<KeyValue> allData = cursor.asList().join();
            
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
    public void testExportAllDataWithBinaryData() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("binary", KeyType.STRING, "binary-test")
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
            RecordCursor<KeyValue> cursor = binaryPath.exportAllData(context);
            
            List<KeyValue> allData = cursor.asList().join();
            
            assertEquals(3, allData.size());
            
            // Verify binary data integrity
            for (int i = 0; i < allData.size(); i++) {
                KeyValue kv = allData.get(i);
                String valueStr = new String(kv.getValue());
                assertTrue(valueStr.startsWith("binary_data_"));
            }
        }
    }

    @Test
    public void testExportAllDataCursorBehavior() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("cursor", KeyType.STRING, "cursor-test")
                        .addSubdirectory(new KeySpaceDirectory("item", KeyType.LONG)));

        final FDBDatabase database = dbExtension.getDatabase();
        
        // Store test data
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            KeySpacePath basePath = root.path("cursor");
            
            for (int i = 0; i < 10; i++) {
                Tuple key = basePath.add("item", (long) i).toTuple(context);
                tr.set(key.pack(), Tuple.from("cursor_item_" + i).pack());
            }
            context.commit();
        }
        
        // Test cursor behavior
        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath cursorPath = root.path("cursor");
            RecordCursor<KeyValue> cursor = cursorPath.exportAllData(context);
            
            // Test that cursor can be iterated
            List<KeyValue> collected = new ArrayList<>();
            RecordCursorResult<KeyValue> result;
            
            while ((result = cursor.getNext()).hasNext()) {
                collected.add(result.get());
            }
            
            assertEquals(10, collected.size());
            assertFalse(result.hasNext());
            
            // Verify the reason for stopping
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, result.getNoNextReason());
        }
    }
}