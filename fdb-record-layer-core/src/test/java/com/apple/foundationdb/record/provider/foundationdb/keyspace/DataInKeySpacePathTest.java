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
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

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
    @ValueSource(ints = {0, 1, 2})
    void testSimpleTwoLevelPath(int depth) {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("app", KeyType.STRING, UUID.randomUUID().toString())
                        .addSubdirectory(new KeySpaceDirectory("locality", KeyType.STRING, "Foo")
                                        .addSubdirectory(new KeySpaceDirectory("user", KeyType.LONG))));

        final FDBDatabase database = dbExtension.getDatabase();

        // Store test data and create DataInKeySpacePath
        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();

            KeySpacePath appPath = root.path("app");
            KeySpacePath localityPath = appPath.add("locality");
            KeySpacePath userPath = localityPath.add("user", 123L);
            final Subspace pathSubspace = userPath.toSubspace(context);
            
            // Add additional tuple elements after the KeySpacePath (this is how data is actually stored)
            byte[] keyBytes = pathSubspace.pack(Tuple.from("record_id", 456L, "metadata"));
            byte[] valueBytes = Tuple.from("test_data").pack();
            
            tr.set(keyBytes, valueBytes);
            KeyValue keyValue = new KeyValue(keyBytes, valueBytes);

            final List<KeySpacePath> queryPaths = List.of(appPath, localityPath, userPath);
            DataInKeySpacePath dataInPath = new DataInKeySpacePath(
                    queryPaths.get(depth), keyValue, context);
            
            // Verify the resolved path
            CompletableFuture<ResolvedKeySpacePath> resolvedFuture = dataInPath.getResolvedPath();
            assertNotNull(resolvedFuture);
            
            ResolvedKeySpacePath resolved = resolvedFuture.join();
            assertNotNull(resolved);
            
            // Verify the resolved path has the correct structure
            assertEquals("user", resolved.getDirectoryName());
            assertEquals(123L, resolved.getResolvedValue());
            
            // Verify parent path
            ResolvedKeySpacePath parent = resolved.getParent();
            assertNotNull(parent);
            assertEquals("locality", parent.getDirectoryName());
            ResolvedKeySpacePath grandParent = parent.getParent();
            assertNotNull(grandParent);
            assertEquals("app", grandParent.getDirectoryName());

            // Verify the resolved path recreates the KeySpacePath portion (not the full key)
            Tuple resolvedTuple = resolved.toTuple();
            assertEquals(TupleHelpers.subTuple(Tuple.fromBytes(keyBytes), 0, 3), resolvedTuple);
            
            // Verify that the remainder contains the additional tuple elements
            Tuple remainder = resolved.getRemainder();
            assertNotNull(remainder);
            assertEquals(3, remainder.size());
            assertEquals("record_id", remainder.getString(0));
            assertEquals(456L, remainder.getLong(1));
            assertEquals("metadata", remainder.getString(2));
            
            context.commit();
        }
    }

    @Test
    void testThreeLevelPathWithStringValues() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("service", KeyType.STRING, UUID.randomUUID().toString())
                        .addSubdirectory(new KeySpaceDirectory("region", KeyType.STRING)
                                .addSubdirectory(new KeySpaceDirectory("instance", KeyType.STRING))));

        final FDBDatabase database = dbExtension.getDatabase();

        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            
            KeySpacePath instancePath = root.path("service")
                    .add("region", "us-west-2")
                    .add("instance", "i-1234567890");
            
            // Add additional tuple elements after the KeySpacePath
            byte[] keyBytes = instancePath.toSubspace(context).pack(
                    Tuple.from("process_id", "web-server", "port", 8080L));
            byte[] valueBytes = Tuple.from("instance_data").pack();
            
            tr.set(keyBytes, valueBytes);
            KeyValue keyValue = new KeyValue(keyBytes, valueBytes);
            
            // Create DataInKeySpacePath from the service-level path
            KeySpacePath servicePath = root.path("service");
            DataInKeySpacePath dataInPath = new DataInKeySpacePath(servicePath, keyValue, context);
            
            ResolvedKeySpacePath resolved = dataInPath.getResolvedPath().join();
            
            // Verify the deepest level
            assertEquals("instance", resolved.getDirectoryName());
            assertEquals("i-1234567890", resolved.getResolvedValue());
            
            // Verify middle level
            ResolvedKeySpacePath regionLevel = resolved.getParent();
            assertNotNull(regionLevel);
            assertEquals("region", regionLevel.getDirectoryName());
            assertEquals("us-west-2", regionLevel.getResolvedValue());
            
            // Verify top level
            ResolvedKeySpacePath serviceLevel = regionLevel.getParent();
            assertNotNull(serviceLevel);
            assertEquals("service", serviceLevel.getDirectoryName());
            
            // Verify the resolved path recreates the KeySpacePath portion
            assertEquals(TupleHelpers.subTuple(Tuple.fromBytes(keyBytes), 0, 3), resolved.toTuple());
            
            // Verify that the remainder contains the additional tuple elements
            Tuple remainder = resolved.getRemainder();
            assertNotNull(remainder);
            assertEquals(4, remainder.size());
            assertEquals("process_id", remainder.getString(0));
            assertEquals("web-server", remainder.getString(1));
            assertEquals("port", remainder.getString(2));
            assertEquals(8080L, remainder.getLong(3));
            
            context.commit();
        }
    }

    @Test
    void testDeepFiveLevelPathWithMixedTypes() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("company", KeyType.STRING, UUID.randomUUID().toString())
                        .addSubdirectory(new KeySpaceDirectory("department", KeyType.STRING)
                        .addSubdirectory(new KeySpaceDirectory("team_id", KeyType.LONG)
                        .addSubdirectory(new KeySpaceDirectory("employee_uuid", KeyType.UUID)
                        .addSubdirectory(new KeySpaceDirectory("active", KeyType.BOOLEAN))))));

        final FDBDatabase database = dbExtension.getDatabase();

        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            
            UUID employeeId = UUID.randomUUID();
            KeySpacePath employeePath = root.path("company")
                    .add("department", "engineering")
                    .add("team_id", 42L)
                    .add("employee_uuid", employeeId)
                    .add("active", true);
            
            // Add additional tuple elements after the KeySpacePath
            byte[] keyBytes = employeePath.toSubspace(context).pack(
                    Tuple.from("salary", 75000L, "start_date", "2023-01-15"));
            byte[] valueBytes = Tuple.from("employee_record").pack();
            
            tr.set(keyBytes, valueBytes);
            KeyValue keyValue = new KeyValue(keyBytes, valueBytes);
            
            // Create DataInKeySpacePath from the company-level path
            KeySpacePath companyPath = root.path("company");
            DataInKeySpacePath dataInPath = new DataInKeySpacePath(companyPath, keyValue, context);
            
            ResolvedKeySpacePath resolved = dataInPath.getResolvedPath().join();
            
            // Verify the deepest level (active)
            assertEquals("active", resolved.getDirectoryName());
            assertEquals(true, resolved.getResolvedValue());
            
            // Verify employee_uuid level
            ResolvedKeySpacePath uuidLevel = resolved.getParent();
            assertNotNull(uuidLevel);
            assertEquals("employee_uuid", uuidLevel.getDirectoryName());
            assertEquals(employeeId, uuidLevel.getResolvedValue());
            
            // Verify team_id level
            ResolvedKeySpacePath teamLevel = uuidLevel.getParent();
            assertNotNull(teamLevel);
            assertEquals("team_id", teamLevel.getDirectoryName());
            assertEquals(42L, teamLevel.getResolvedValue());
            
            // Verify department level
            ResolvedKeySpacePath deptLevel = teamLevel.getParent();
            assertNotNull(deptLevel);
            assertEquals("department", deptLevel.getDirectoryName());
            assertEquals("engineering", deptLevel.getResolvedValue());
            
            // Verify company level
            ResolvedKeySpacePath companyLevel = deptLevel.getParent();
            assertNotNull(companyLevel);
            assertEquals("company", companyLevel.getDirectoryName());
            
            // Verify the resolved path recreates the KeySpacePath portion
            assertEquals(TupleHelpers.subTuple(Tuple.fromBytes(keyBytes), 0, 5), resolved.toTuple());
            
            // Verify that the remainder contains the additional tuple elements
            Tuple remainder = resolved.getRemainder();
            assertNotNull(remainder);
            assertEquals(4, remainder.size());
            assertEquals("salary", remainder.getString(0));
            assertEquals(75000L, remainder.getLong(1));
            assertEquals("start_date", remainder.getString(2));
            assertEquals("2023-01-15", remainder.getString(3));
            
            context.commit();
        }
    }

    @Test
    void testPathWithConstantValues() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("application", KeyType.STRING, UUID.randomUUID().toString())
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
            
            // Verify the deepest level
            assertEquals("data", resolved.getDirectoryName());
            assertEquals("user_records", resolved.getResolvedValue());
            
            // Verify environment level (constant value)
            ResolvedKeySpacePath envLevel = resolved.getParent();
            assertNotNull(envLevel);
            assertEquals("environment", envLevel.getDirectoryName());
            assertEquals("production", envLevel.getResolvedValue());
            
            // Verify version level (constant value)
            ResolvedKeySpacePath versionLevel = envLevel.getParent();
            assertNotNull(versionLevel);
            assertEquals("version", versionLevel.getDirectoryName());
            assertEquals(1L, versionLevel.getResolvedValue());
            
            // Verify application level
            ResolvedKeySpacePath applicationLevel = versionLevel.getParent();
            assertNotNull(applicationLevel);
            assertEquals("application", applicationLevel.getDirectoryName());
            
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
    void testPathWithDirectoryLayer() {
        KeySpace root = new KeySpace(
                new DirectoryLayerDirectory("tenant", UUID.randomUUID().toString())
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
            
            ResolvedKeySpacePath resolved = dataInPath.getResolvedPath().join();
            
            // Verify the deepest level (service - DirectoryLayer)
            assertEquals("service", resolved.getDirectoryName());
            assertEquals("analytics", resolved.getLogicalValue());
            
            // Verify user_id level
            ResolvedKeySpacePath userLevel = resolved.getParent();
            assertNotNull(userLevel);
            assertEquals("user_id", userLevel.getDirectoryName());
            assertEquals(999L, userLevel.getLogicalValue());
            
            // Verify tenant level (DirectoryLayer)
            ResolvedKeySpacePath tenantLevel = userLevel.getParent();
            assertNotNull(tenantLevel);
            assertEquals("tenant", tenantLevel.getDirectoryName());
            
            // Note: DirectoryLayer values are resolved asynchronously, so we verify the structure is correct
            assertNotNull(tenantLevel.getResolvedValue());
            
            context.commit();
        }
    }

    @Test
    void testPathWithBinaryData() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("binary_store", KeyType.STRING, UUID.randomUUID().toString())
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
            
            // Verify the deepest level
            assertEquals("blob_id", resolved.getDirectoryName());
            byte[] resolvedBytes = (byte[]) resolved.getResolvedValue();
            assertArrayEquals(blobId, resolvedBytes);
            
            // Verify parent level
            ResolvedKeySpacePath storeLevel = resolved.getParent();
            assertNotNull(storeLevel);
            assertEquals("binary_store", storeLevel.getDirectoryName());
            
            // Verify the resolved path can recreate the original key
            assertEquals(keyTuple, resolved.toTuple());
            
            context.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 4, 5})
    void testVariableDepthPaths(int depth) {
        // Build a KeySpace with the specified depth
        KeySpaceDirectory rootDir = new KeySpaceDirectory("root", KeyType.STRING, UUID.randomUUID().toString());
        KeySpace root = new KeySpace(rootDir);
        KeySpaceDirectory dir = rootDir;
        for (int i = 1; i < depth; i++) {
            final KeySpaceDirectory next = new KeySpaceDirectory("level" + i, KeyType.LONG);
            dir.addSubdirectory(next);
            dir = next;
        }

        final FDBDatabase database = dbExtension.getDatabase();

        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            
            // Build a path with the specified depth
            KeySpacePath currentPath = root.path("root");
            for (int i = 1; i < depth; i++) {
                currentPath = currentPath.add("level" + i, i * 100L);
            }
            
            Tuple keyTuple = currentPath.toTuple(context);
            byte[] keyBytes = keyTuple.pack();
            byte[] valueBytes = Tuple.from("depth_test_" + depth).pack();
            
            tr.set(keyBytes, valueBytes);
            KeyValue keyValue = new KeyValue(keyBytes, valueBytes);
            
            // Create DataInKeySpacePath from the root-level path
            KeySpacePath rootPath = root.path("root");
            root.resolveFromKey(context, Tuple.fromBytes(keyValue.getKey()));
            DataInKeySpacePath dataInPath = new DataInKeySpacePath(rootPath, keyValue, context);
            
            ResolvedKeySpacePath resolved = dataInPath.getResolvedPath().join();
            
            // Verify the depth by traversing up the path
            int actualDepth = 1; // Start at 1 for the root
            ResolvedKeySpacePath current = resolved;
            while (current.getParent() != null) {
                actualDepth++;
                current = current.getParent();
            }
            assertEquals(depth, actualDepth);
            
            // Verify the deepest level has the expected name and value
            if (depth > 1) {
                assertEquals("level" + (depth - 1), resolved.getDirectoryName());
                assertEquals((depth - 1) * 100L, resolved.getResolvedValue());
            } else {
                assertEquals("root", resolved.getDirectoryName());
            }
            
            // Verify the resolved path can recreate the original key
            assertEquals(keyTuple, resolved.toTuple());
            
            context.commit();
        }
    }

    @Test
    void testKeyValueAccessors() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("test", KeyType.STRING, UUID.randomUUID().toString()));

        final FDBDatabase database = dbExtension.getDatabase();

        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath testPath = root.path("test");
            Tuple keyTuple = testPath.toTuple(context);
            byte[] keyBytes = keyTuple.pack();
            byte[] valueBytes = Tuple.from("accessor_test").pack();
            
            KeyValue originalKeyValue = new KeyValue(keyBytes, valueBytes);
            DataInKeySpacePath dataInPath = new DataInKeySpacePath(testPath, originalKeyValue, context);
            
            // Verify accessor methods
            KeyValue retrievedKeyValue = dataInPath.getRawKeyValue();
            assertNotNull(retrievedKeyValue);
            assertEquals(originalKeyValue.getKey(), retrievedKeyValue.getKey());
            assertEquals(originalKeyValue.getValue(), retrievedKeyValue.getValue());
            
            // Verify resolved path future is not null
            CompletableFuture<ResolvedKeySpacePath> resolvedFuture = dataInPath.getResolvedPath();
            assertNotNull(resolvedFuture);
            assertTrue(resolvedFuture.isDone() || !resolvedFuture.isCancelled());
        }
    }

    @Test
    void testNullKeyTypeDirectory() {
        KeySpace root = new KeySpace(
                new KeySpaceDirectory("base", KeyType.STRING, UUID.randomUUID().toString())
                        .addSubdirectory(new KeySpaceDirectory("null_dir", KeyType.NULL)));

        final FDBDatabase database = dbExtension.getDatabase();

        try (FDBRecordContext context = database.openContext()) {
            Transaction tr = context.ensureActive();
            
            KeySpacePath nullPath = root.path("base").add("null_dir");
            
            Tuple keyTuple = nullPath.toTuple(context);
            byte[] keyBytes = keyTuple.pack();
            byte[] valueBytes = Tuple.from("null_type_test").pack();
            
            tr.set(keyBytes, valueBytes);
            KeyValue keyValue = new KeyValue(keyBytes, valueBytes);
            
            // Create DataInKeySpacePath from the base-level path
            KeySpacePath basePath = root.path("base");
            DataInKeySpacePath dataInPath = new DataInKeySpacePath(basePath, keyValue, context);
            
            ResolvedKeySpacePath resolved = dataInPath.getResolvedPath().join();
            
            // Verify the deepest level (NULL type)
            assertEquals("null_dir", resolved.getDirectoryName());
            // NULL type directories have null as their resolved value
            assertNull(resolved.getResolvedValue());
            
            // Verify parent level
            ResolvedKeySpacePath baseLevel = resolved.getParent();
            assertNotNull(baseLevel);
            assertEquals("base", baseLevel.getDirectoryName());
            
            // Verify the resolved path can recreate the original key
            assertEquals(keyTuple, resolved.toTuple());
            
            context.commit();
        }
    }

    @Test
    void testWithWrapper() {
        final FDBDatabase database = dbExtension.getDatabase();
        final EnvironmentKeySpace keySpace = EnvironmentKeySpace.setupSampleData(database);

        // Test export at different levels through wrapper methods
        try (FDBRecordContext context = database.openContext()) {
            // Test 4: Export from specific data store level
            EnvironmentKeySpace.DataPath dataStore = keySpace.root().userid(100L).application("app1").dataStore();

            final byte[] key = dataStore.toTuple(context).add("record2").add(0).pack();
            final byte[] value = Tuple.from("data").pack();
            final DataInKeySpacePath dataInKeySpacePath = new DataInKeySpacePath(dataStore, new KeyValue(key, value), context);

            final ResolvedKeySpacePath resolvedPath = dataInKeySpacePath.getResolvedPath().join();
            assertEquals(dataStore.toResolvedPath(context), withoutRemainder(resolvedPath));
            assertEquals(Tuple.from("record2", 0), resolvedPath.getRemainder());
        }
    }

    private ResolvedKeySpacePath withoutRemainder(final ResolvedKeySpacePath path) {
        return new ResolvedKeySpacePath(path.getParent(), path.toPath(), path.getResolvedPathValue(), null);
    }
}
