/*
 * DataInKeySpacePathTest.java
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
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpaceDirectory.KeyType;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link DataInKeySpacePath}.
 */
@Tag(Tags.RequiresFDB)
class DataInKeySpacePathTest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3, 4, 5})
    void resolution(int depth) {
        // Include some extra children to make sure resolution doesn't get confused
        final String companyUuid = UUID.randomUUID().toString();
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("company", KeyType.STRING, companyUuid)
                        .addSubdirectory(new KeySpaceDirectory("department", KeyType.STRING)
                                .addSubdirectory(new KeySpaceDirectory("team_id", KeyType.LONG)
                                        .addSubdirectory(new KeySpaceDirectory("employee_uuid", KeyType.UUID)
                                                .addSubdirectory(new KeySpaceDirectory("active", KeyType.BOOLEAN)
                                                        .addSubdirectory(new KeySpaceDirectory("data", KeyType.NULL, null))
                                                        .addSubdirectory(new KeySpaceDirectory("metaData", KeyType.LONG, 0))))
                                        .addSubdirectory(new KeySpaceDirectory("buildings", KeyType.STRING)))));

        final FDBDatabase database = dbExtension.getDatabase();

        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();

            UUID employeeId = UUID.randomUUID();

            KeySpacePath employeePath = root.path("company")
                    .add("department", "engineering")
                    .add("team_id", 42L)
                    .add("employee_uuid", employeeId)
                    .add("active", true)
                    .add("data");

            // Add additional tuple elements after the KeySpacePath
            final Tuple remainderTuple = Tuple.from("salary", 75000L, "start_date", "2023-01-15");
            byte[] keyBytes = employeePath.toSubspace(context).pack(remainderTuple);
            byte[] valueBytes = Tuple.from("employee_record").pack();

            tr.set(keyBytes, valueBytes);
            KeyValue keyValue = new KeyValue(keyBytes, valueBytes);

            // Create DataInKeySpacePath from the company-level path
            List<Function<KeySpacePath, KeySpacePath>> pathNavigation = List.of(
                    path -> path.add("department", "engineering"),
                    path -> path.add("team_id", 42L),
                    path -> path.add("employee_uuid", employeeId),
                    path -> path.add("active", true),
                    path -> path.add("data")
            );
            KeySpacePath toResolve = root.path("company");
            for (final Function<KeySpacePath, KeySpacePath> function : pathNavigation.subList(0, depth)) {
                toResolve = function.apply(toResolve);
            }
            DataInKeySpacePath dataInPath = new DataInKeySpacePath(toResolve, keyValue, context);

            ResolvedKeySpacePath resolved = dataInPath.getResolvedPath().join();

            // Verify the path
            ResolvedKeySpacePath activeLevel = assertNameAndValue(resolved, "data", null);
            ResolvedKeySpacePath uuidLevel = assertNameAndValue(activeLevel, "active", true);
            ResolvedKeySpacePath teamLevel = assertNameAndValue(uuidLevel, "employee_uuid", employeeId);
            ResolvedKeySpacePath deptLevel = assertNameAndValue(teamLevel, "team_id", 42L);
            ResolvedKeySpacePath companyLevel = assertNameAndValue(deptLevel, "department", "engineering");
            assertNull(assertNameAndValue(companyLevel, "company", companyUuid));

            // Verify the resolved path recreates the KeySpacePath portion
            assertEquals(TupleHelpers.subTuple(Tuple.fromBytes(keyBytes), 0, 6), resolved.toTuple());

            // Verify that the remainder contains the additional tuple elements
            assertEquals(remainderTuple, resolved.getRemainder());

            context.commit();
        }
    }

    @Test
    void pathWithConstantValues() {
        final String appUuid = UUID.randomUUID().toString();
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("application", KeyType.STRING, appUuid)
                        .addSubdirectory(new KeySpaceDirectory("version", KeyType.LONG, 1L)
                                .addSubdirectory(new KeySpaceDirectory("environment", KeyType.STRING, "production")
                                        .addSubdirectory(new KeySpaceDirectory("data", KeyType.STRING)))));

        final FDBDatabase database = dbExtension.getDatabase();

        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            
            KeySpacePath dataPath = root.path("application")
                    .add("version")      // Uses constant value 1L
                    .add("environment")  // Uses constant value "production"
                    .add("data", "user_records");
            // Add additional tuple elements after the KeySpacePath
            byte[] keyBytes = dataPath.toSubspace(context).pack(
                    Tuple.from("config_id", 1001L, "version", "v2.1"));
            byte[] valueBytes = Tuple.from("constant_test_data").pack();
            
            tr.set(keyBytes, valueBytes);
            KeyValue keyValue = new KeyValue(keyBytes, valueBytes);
            
            // Create DataInKeySpacePath from the application-level path
            KeySpacePath appPath = root.path("application");
            DataInKeySpacePath dataInPath = new DataInKeySpacePath(appPath, keyValue, context);
            
            ResolvedKeySpacePath resolved = dataInPath.getResolvedPath().join();
            
            // Verify the path using assertNameAndValue
            ResolvedKeySpacePath envLevel = assertNameAndValue(resolved, "data", "user_records");
            ResolvedKeySpacePath versionLevel = assertNameAndValue(envLevel, "environment", "production");
            ResolvedKeySpacePath applicationLevel = assertNameAndValue(versionLevel, "version", 1L);
            assertNull(assertNameAndValue(applicationLevel, "application", appUuid));
            
            // Verify the resolved path recreates the KeySpacePath portion
            assertEquals(TupleHelpers.subTuple(Tuple.fromBytes(keyBytes), 0, 4), resolved.toTuple());
            
            // Verify that the remainder contains the additional tuple elements
            Tuple remainder = resolved.getRemainder();
            assertNotNull(remainder);
            assertEquals(4, remainder.size());
            assertEquals("config_id", remainder.getString(0));
            assertEquals(1001L, remainder.getLong(1));
            assertEquals("version", remainder.getString(2));
            assertEquals("v2.1", remainder.getString(3));
            
            context.commit();
        }
    }

    @Test
    void pathWithDirectoryLayer() {
        final String tenantUuid = UUID.randomUUID().toString();
        KeySpace root = new KeySpace(
                new DirectoryLayerDirectory("tenant", tenantUuid)
                        .addSubdirectory(new KeySpaceDirectory("user_id", KeyType.LONG)
                                .addSubdirectory(new DirectoryLayerDirectory("service"))));

        final FDBDatabase database = dbExtension.getDatabase();

        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            
            KeySpacePath servicePath = root.path("tenant")
                    .add("user_id", 999L)
                    .add("service", "analytics");
            
            Tuple keyTuple = servicePath.toTuple(context);
            byte[] keyBytes = keyTuple.pack();
            byte[] valueBytes = Tuple.from("directory_layer_data").pack();
            
            tr.set(keyBytes, valueBytes);
            KeyValue keyValue = new KeyValue(keyBytes, valueBytes);
            
            // Create DataInKeySpacePath from the tenant-level path
            KeySpacePath tenantPath = root.path("tenant");
            DataInKeySpacePath dataInPath = new DataInKeySpacePath(tenantPath, keyValue, context);
            
            ResolvedKeySpacePath serviceLevel = dataInPath.getResolvedPath().join();
            
            final ResolvedKeySpacePath userLevel = assertNameAndDirectoryScopedValue(
                    serviceLevel, "service", "analytics", servicePath, context);
            ResolvedKeySpacePath tenantLevel = assertNameAndValue(userLevel, "user_id", 999L);

            assertNull(assertNameAndDirectoryScopedValue(tenantLevel, "tenant", tenantUuid, tenantPath, context));

            context.commit();
        }
    }

    @Test
    void pathWithBinaryData() {
        final String storeUuid = UUID.randomUUID().toString();
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("binary_store", KeyType.STRING, storeUuid)
                        .addSubdirectory(new KeySpaceDirectory("blob_id", KeyType.BYTES)));

        final FDBDatabase database = dbExtension.getDatabase();

        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            
            byte[] blobId = {0x01, 0x02, 0x03, (byte) 0xFF, (byte) 0xFE};
            KeySpacePath blobPath = root.path("binary_store").add("blob_id", blobId);
            
            Tuple keyTuple = blobPath.toTuple(context);
            byte[] keyBytes = keyTuple.pack();
            byte[] valueBytes = "binary_test_data".getBytes();
            
            tr.set(keyBytes, valueBytes);
            KeyValue keyValue = new KeyValue(keyBytes, valueBytes);
            
            // Create DataInKeySpacePath from the binary_store-level path
            KeySpacePath storePath = root.path("binary_store");
            DataInKeySpacePath dataInPath = new DataInKeySpacePath(storePath, keyValue, context);
            
            ResolvedKeySpacePath resolved = dataInPath.getResolvedPath().join();
            
            // Verify the path using assertNameAndValue
            assertEquals("blob_id", resolved.getDirectoryName());
            byte[] resolvedBytes = (byte[]) resolved.getResolvedValue();
            assertArrayEquals(blobId, resolvedBytes);
            
            ResolvedKeySpacePath storeLevel = assertNameAndValue(resolved.getParent(), "binary_store", storeUuid);
            assertNull(storeLevel);
            
            // Verify the resolved path can recreate the original key
            assertEquals(keyTuple, resolved.toTuple());
            
            context.commit();
        }
    }

    @ParameterizedTest
    @BooleanSource("withRemainder")
    void keyValueAccessors(boolean withRemainder) throws ExecutionException, InterruptedException {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("test", KeyType.STRING, UUID.randomUUID().toString()));

        final FDBDatabase database = dbExtension.getDatabase();

        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath testPath = root.path("test");
            Tuple pathTuple = testPath.toTuple(context);
            byte[] keyBytes = withRemainder ? pathTuple.add("Remainder").pack() : pathTuple.pack();
            byte[] valueBytes = Tuple.from("accessor_test").pack();
            
            KeyValue originalKeyValue = new KeyValue(keyBytes, valueBytes);
            DataInKeySpacePath dataInPath = new DataInKeySpacePath(testPath, originalKeyValue, context);

            // Verify resolved path future is not null
            CompletableFuture<ResolvedKeySpacePath> resolvedFuture = dataInPath.getResolvedPath();
            assertNotNull(resolvedFuture);
            assertTrue(resolvedFuture.isDone() || !resolvedFuture.isCancelled());

            final ResolvedKeySpacePath resolvedPath = resolvedFuture.get();
            assertEquals(pathTuple, resolvedPath.toTuple());
            assertArrayEquals(originalKeyValue.getValue(), dataInPath.getValue());
            if (withRemainder) {
                assertEquals(Tuple.from("Remainder"), resolvedPath.getRemainder());
            } else {
                assertNull(resolvedPath.getRemainder());
            }
        }
    }

    @Test
    void withWrapper() {
        final FDBDatabase database = dbExtension.getDatabase();
        final EnvironmentKeySpace keySpace = EnvironmentKeySpace.setupSampleData(database);

        // Test export at different levels through wrapper methods
        try (FDBRecordContext context = database.openContext()) {
            // Test 4: Export from specific data store level
            final EnvironmentKeySpace.ApplicationPath appPath = keySpace.root().userid(100L).application("app1");
            EnvironmentKeySpace.DataPath dataStore = appPath.dataStore();

            final byte[] key = dataStore.toTuple(context).add("record2").add(0).pack();
            final byte[] value = Tuple.from("data").pack();
            final DataInKeySpacePath dataInKeySpacePath = new DataInKeySpacePath(dataStore, new KeyValue(key, value), context);

            final ResolvedKeySpacePath resolvedPath = dataInKeySpacePath.getResolvedPath().join();
            assertEquals(dataStore.toResolvedPath(context), withoutRemainder(resolvedPath));
            assertEquals(Tuple.from("record2", 0), resolvedPath.getRemainder());

            // Verify the path using assertNameAndValue  
            // Note: We expect the path to be: [environment] -> userid -> application -> data
            ResolvedKeySpacePath appLevel;
            appLevel = assertNameAndValue(resolvedPath, "data", EnvironmentKeySpace.DATA_VALUE);
            ResolvedKeySpacePath userLevel = assertNameAndDirectoryScopedValue(appLevel, "application", "app1",
                    appPath, context);
            ResolvedKeySpacePath envLevel = assertNameAndValue(userLevel, "userid", 100L);
            assertNull(assertNameAndDirectoryScopedValue(envLevel, keySpace.root().getDirectoryName(),
                    keySpace.root().getValue(), keySpace.root(), context));
        }
    }

    private static ResolvedKeySpacePath assertNameAndDirectoryScopedValue(ResolvedKeySpacePath resolved,
                                                                          String name, Object logicalValue,
                                                                          KeySpacePath path, FDBRecordContext context) {
        assertNotNull(resolved);
        assertEquals(name, resolved.getDirectoryName());
        assertEquals(path.toResolvedPath(context).getResolvedValue(), resolved.getResolvedValue());
        assertEquals(logicalValue, resolved.getLogicalValue());
        return resolved.getParent();
    }

    private static ResolvedKeySpacePath assertNameAndValue(ResolvedKeySpacePath resolved, String name, Object value) {
        assertNotNull(resolved);
        assertEquals(name, resolved.getDirectoryName());
        assertEquals(value, resolved.getResolvedValue());
        assertEquals(value, resolved.getLogicalValue());
        return resolved.getParent();
    }

    private ResolvedKeySpacePath withoutRemainder(final ResolvedKeySpacePath path) {
        return new ResolvedKeySpacePath(path.getParent(), path.toPath(), path.getResolvedPathValue(), null);
    }
}
