/*
 * KeySpacePathTest.java
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

import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.mockito.Mockito;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link KeySpacePath}.
 * See also {@link KeySpacePathDataExportTest} and {@link ResolvedKeySpacePathTest}.
 */
public class KeySpacePathTest {

    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();

    private static final String ROOT_UUID = UUID.randomUUID().toString();

    /**
     * Creates a KeySpace for testing with a consistent structure.
     * The structure is: root -> branch -> leaf
     * @param useDirectoryLayer if true, leaf uses DirectoryLayerDirectory; otherwise uses KeySpaceDirectory
     * @param useConstantValue if true, adds a constant value to the leaf directory
     * @return a KeySpace with the specified configuration
     */
    private KeySpace createKeySpace(boolean useDirectoryLayer, boolean useConstantValue) {
        KeySpaceDirectory rootDir = new KeySpaceDirectory("test_root", KeySpaceDirectory.KeyType.STRING, ROOT_UUID);
        KeySpaceDirectory branchDir = new KeySpaceDirectory("branch", KeySpaceDirectory.KeyType.STRING, "branch_value");

        KeySpaceDirectory leafDir;
        if (useDirectoryLayer) {
            if (useConstantValue) {
                leafDir = new DirectoryLayerDirectory("leaf", "leaf_constant");
            } else {
                leafDir = new DirectoryLayerDirectory("leaf");
            }
        } else {
            if (useConstantValue) {
                leafDir = new KeySpaceDirectory("leaf", KeySpaceDirectory.KeyType.STRING, "leaf_constant");
            } else {
                leafDir = new KeySpaceDirectory("leaf", KeySpaceDirectory.KeyType.STRING);
            }
        }

        branchDir.addSubdirectory(leafDir);
        rootDir.addSubdirectory(branchDir);
        return new KeySpace(rootDir);
    }

    @ParameterizedTest
    @BooleanSource({"withRemainder", "useDirectoryLayer", "useConstantValue"})
    void testToResolvedPathAsync(boolean withRemainder, boolean useDirectoryLayer, boolean useConstantValue) {
        final FDBDatabase database = dbExtension.getDatabase();
        final KeySpace keySpace = createKeySpace(useDirectoryLayer, useConstantValue);

        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath rootPath = keySpace.path("test_root");
            KeySpacePath branchPath = rootPath.add("branch");

            // Build the full path - add leaf with or without value based on constant
            KeySpacePath fullPath;
            if (useConstantValue) {
                fullPath = branchPath.add("leaf");
            } else {
                fullPath = branchPath.add("leaf", "leaf_value");
            }

            // Create a key with or without remainder
            byte[] keyBytes;
            Tuple expectedRemainder;
            if (withRemainder) {
                expectedRemainder = Tuple.from("extra", "data");
                keyBytes = fullPath.toSubspace(context).pack(expectedRemainder);
            } else {
                expectedRemainder = null;
                keyBytes = fullPath.toSubspace(context).pack();
            }

            // Test toResolvedPathAsync with the key
            ResolvedKeySpacePath resolved = branchPath.toResolvedPathAsync(context, keyBytes).join();

            assertEquals(fullPath.toResolvedPath(context), resolved.withRemainder(null));
            assertEquals(expectedRemainder, resolved.getRemainder());
        }
    }

    @Test
    void testToResolvedPathAsyncWithWrapper() {
        final FDBDatabase database = dbExtension.getDatabase();
        final EnvironmentKeySpace keySpace = EnvironmentKeySpace.setupSampleData(database);

        try (FDBRecordContext context = database.openContext()) {
            // Use the wrapper paths which extend KeySpacePathWrapper
            EnvironmentKeySpace.ApplicationPath appPath = keySpace.root().userid(100L).application("app1");
            EnvironmentKeySpace.DataPath dataPath = appPath.dataStore();

            // Create a key with remainder
            Tuple remainderTuple = Tuple.from("record_id", 42L, "version", 1);
            byte[] keyBytes = dataPath.toSubspace(context).pack(remainderTuple);

            // Test toResolvedPathAsync on the wrapper - should resolve from appPath through dataPath
            ResolvedKeySpacePath resolved = appPath.toResolvedPathAsync(context, keyBytes).join();

            // Verify the resolved path
            assertEquals(EnvironmentKeySpace.DATA_KEY, resolved.getDirectoryName());
            assertEquals(EnvironmentKeySpace.DATA_VALUE, resolved.getResolvedValue());
            assertEquals(remainderTuple, resolved.getRemainder());

            // Verify parent structure
            ResolvedKeySpacePath appLevel = resolved.getParent();
            assertNotNull(appLevel);
            assertEquals(EnvironmentKeySpace.APPLICATION_KEY, appLevel.getDirectoryName());
            assertEquals("app1", appLevel.getLogicalValue());

            ResolvedKeySpacePath userLevel = appLevel.getParent();
            assertNotNull(userLevel);
            assertEquals(EnvironmentKeySpace.USER_KEY, userLevel.getDirectoryName());
            assertEquals(100L, userLevel.getResolvedValue());
        }
    }

    @Test
    void testToResolvedPathAsyncWithKeyNotSubPath() {
        final FDBDatabase database = dbExtension.getDatabase();
        final KeySpace keySpace = createKeySpace(false, false);

        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath rootPath = keySpace.path("test_root");
            KeySpacePath branchPath = rootPath.add("branch");
            KeySpacePath leafPath = branchPath.add("leaf", "leaf_value");

            // Create a key that is shorter than branchPath - it stops at root
            byte[] shorterKeyBytes = rootPath.toSubspace(context).pack();

            // Attempting to resolve a key that is not under branchPath should error
            ExecutionException ex = assertThrows(ExecutionException.class, () -> {
                branchPath.toResolvedPathAsync(context, shorterKeyBytes).get();
            });
            assertEquals(RecordCoreArgumentException.class, ex.getCause().getClass());
        }
    }

    @Test
    void testToResolvedPathAsyncWithInvalidTuple() {
        final FDBDatabase database = dbExtension.getDatabase();
        final KeySpace keySpace = createKeySpace(false, false);

        try (FDBRecordContext context = database.openContext()) {
            KeySpacePath rootPath = keySpace.path("test_root");
            KeySpacePath branchPath = rootPath.add("branch");

            // Create a byte array that is not a valid tuple
            byte[] invalidBytes = new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF};

            // Attempting to resolve invalid tuple bytes should error
            // The exception is thrown synchronously from Tuple.fromBytes, not wrapped in ExecutionException
            assertThrows(IllegalArgumentException.class, () -> {
                branchPath.toResolvedPathAsync(context, invalidBytes);
            });
        }
    }

    /**
     * Test of methods with default implementations to ensure backwards compatibility,
     * in case someone is implementing {@link KeySpacePath}.
     **/
    @Test
    void testDefaultMethods() {
        final KeySpacePath mock = Mockito.mock(KeySpacePath.class);
        final FDBDatabase database = dbExtension.getDatabase();

        try (FDBRecordContext context = database.openContext()) {
            // thenCallReadMethod throws an error if there is not a default implementation
            Mockito.when(mock.toResolvedPathAsync(Mockito.any(), Mockito.any())).thenCallRealMethod();
            assertThrows(UnsupportedOperationException.class,
                    () -> mock.toResolvedPathAsync(context, Tuple.from("foo").pack()));
            Mockito.when(mock.exportAllData(Mockito.any(), Mockito.any(), Mockito.any())).thenCallRealMethod();
            assertThrows(UnsupportedOperationException.class,
                    () -> mock.exportAllData(context, null, ScanProperties.FORWARD_SCAN));
        }
    }
}
