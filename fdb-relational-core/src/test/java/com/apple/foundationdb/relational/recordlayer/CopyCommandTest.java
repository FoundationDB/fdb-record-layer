/*
 * CopyCommandTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.ContextualSQLException;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for COPY command (export and import).
 */
@Tag(Tags.RequiresFDB)
public class CopyCommandTest {

    @RegisterExtension
    public static final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @Test
    void basicExportWithinCluster() throws Exception {
        // Test basic COPY export functionality with unquoted path
        final String pathId = "/TEST/" + UUID.randomUUID().toString().toUpperCase(Locale.ROOT).replace("-", "_");
        // Use the shared KeySpace from RelationalKeyspaceProvider
        final KeySpace keySpace = RelationalKeyspaceProvider.instance().getKeySpace();
        final KeySpacePath testPath = KeySpaceUtils.toKeySpacePath(URI.create(pathId + "/1"), keySpace);


        // Export the data using COPY command (unquoted path)
        try (RelationalConnection conn = DriverManager.getConnection("jdbc:embed:/__SYS").unwrap(RelationalConnection.class)) {
            conn.setSchema("CATALOG");
            conn.setAutoCommit(false);

            // Start a transaction explicitly
            EmbeddedRelationalConnection embeddedConn = conn.unwrap(EmbeddedRelationalConnection.class);
            embeddedConn.createNewTransaction();

            // Write some test data using the connection's FDB context
            writeTestData(conn, testPath, "key1", "value1");
            writeTestData(conn, testPath, "key2", "value2");
            conn.commit();

            embeddedConn.createNewTransaction();

            verifyTestData(conn, testPath, "key1", "value1");
            verifyTestData(conn, testPath, "key2", "value2");

            assertEquals(2, exportData(pathId, false).size());
        }
    }

    @ParameterizedTest
    @BooleanSource("namedParameter")
    void basicImportWithinCluster(boolean namedParameter) throws Exception {
        // Test basic COPY import functionality with quoted paths (allows hyphens)
        final String sourcePath = "/TEST/" + UUID.randomUUID();
        final String destPath = "/TEST/" + UUID.randomUUID();
        final KeySpace keySpace = RelationalKeyspaceProvider.instance().getKeySpace();
        final KeySpacePath sourceTestPath = KeySpaceUtils.toKeySpacePath(URI.create(sourcePath + "/1"), keySpace);
        final KeySpacePath destTestPath = KeySpaceUtils.toKeySpacePath(URI.create(destPath + "/1"), keySpace);

        // Export from source (using quoted path)
        List<byte[]> exportedData;
        try (RelationalConnection conn = DriverManager.getConnection("jdbc:embed:/__SYS").unwrap(RelationalConnection.class)) {
            conn.setSchema("CATALOG");
            conn.setAutoCommit(false);

            // Start a transaction explicitly
            EmbeddedRelationalConnection embeddedConn = conn.unwrap(EmbeddedRelationalConnection.class);
            embeddedConn.createNewTransaction();

            // Write test data to source
            writeTestData(conn, sourceTestPath, "key1", "value1");
            writeTestData(conn, sourceTestPath, "key2", "value2");
            conn.commit();

            exportedData = exportData(sourcePath, true);
        }

        // Import to destination (using quoted path)
        try (RelationalConnection conn = DriverManager.getConnection("jdbc:embed:/__SYS").unwrap(RelationalConnection.class)) {
            conn.setSchema("CATALOG");
            try (RelationalPreparedStatement stmt = conn.prepareStatement("COPY \"" + destPath + "\" FROM " + (namedParameter ? "?data" : "?"))) {
                if (namedParameter) {
                    stmt.setObject("data", exportedData);
                } else {
                    stmt.setObject(1, exportedData);
                }

                assertEquals(2, stmt.executeUpdate(), "Should have imported 2 records");
            }
        }

        // Verify the data was imported
        try (RelationalConnection conn = DriverManager.getConnection("jdbc:embed:/__SYS").unwrap(RelationalConnection.class)) {
            conn.setSchema("CATALOG");
            conn.setAutoCommit(false);

            // Start a transaction explicitly
            EmbeddedRelationalConnection embeddedConn = conn.unwrap(EmbeddedRelationalConnection.class);
            embeddedConn.createNewTransaction();

            verifyTestData(conn, destTestPath, "key1", "value1");
            verifyTestData(conn, destTestPath, "key2", "value2");
        }
    }

    @ParameterizedTest
    @BooleanSource("namedParameter")
    void wrongParameter(boolean namedParameter) throws Exception {
        // Test basic COPY import functionality with quoted paths (allows hyphens)
        final String sourcePath = "/TEST/" + UUID.randomUUID();
        final String destPath = "/TEST/" + UUID.randomUUID();
        final KeySpace keySpace = RelationalKeyspaceProvider.instance().getKeySpace();
        final KeySpacePath sourceTestPath = KeySpaceUtils.toKeySpacePath(URI.create(sourcePath + "/1"), keySpace);
        final KeySpacePath destTestPath = KeySpaceUtils.toKeySpacePath(URI.create(destPath + "/1"), keySpace);

        // Export from source (using quoted path)
        List<byte[]> exportedData = exportData(sourcePath, true);

        // Import to destination (using quoted path)
        try (RelationalConnection conn = DriverManager.getConnection("jdbc:embed:/__SYS").unwrap(RelationalConnection.class)) {
            conn.setSchema("CATALOG");
            try (RelationalPreparedStatement stmt = conn.prepareStatement("COPY \"" + destPath + "\" FROM " + (namedParameter ? "?data" : "?"))) {
                // set the wrong one
                if (namedParameter) {
                    stmt.setObject(1, exportedData);
                } else {
                    stmt.setObject("data", exportedData);
                }

                final ContextualSQLException exception = assertThrows(ContextualSQLException.class, stmt::executeUpdate);
                assertEquals(ErrorCode.UNDEFINED_PARAMETER, ((RelationalException)exception.getCause()).getErrorCode());
            }
        }
    }


    @Test
    void exportEmptyPath() throws Exception {
        // Test exporting from an empty path - should return empty result set (unquoted path)
        final String pathId = "/TEST/" + UUID.randomUUID().toString().replace("-", "_");
        // No need to create KeySpace, just ensure path exists in shared KeySpace

        try (RelationalConnection conn = DriverManager.getConnection("jdbc:embed:/__SYS").unwrap(RelationalConnection.class)) {
            conn.setSchema("CATALOG");
            try (RelationalStatement stmt = conn.createStatement();
                     RelationalResultSet rs = stmt.executeQuery("COPY " + pathId)) {

                // Should have no data
                assertFalse(rs.next(), "Empty path should return empty result set");
            }
        }
    }

    @Test
    void exportInvalidPath() throws Exception {
        // Test exporting from an invalid path - should throw error
        try (RelationalConnection conn = DriverManager.getConnection("jdbc:embed:/__SYS").unwrap(RelationalConnection.class)) {
            conn.setSchema("CATALOG");
            try (RelationalStatement stmt = conn.createStatement()) {
                ContextualSQLException exception = assertThrows(ContextualSQLException.class,
                        () -> stmt.executeQuery("COPY /INVALID/PATH/STRUCTURE"));
                assertEquals(ErrorCode.INVALID_COPY_PATH, ((RelationalException)exception.getCause()).getErrorCode());
            }
        }
    }

    @Test
    void exportWithRowLimit() throws Exception {
        // Test COPY export with Statement.setMaxRows() limiting (unquoted path)
        final String pathId = "/TEST/" + UUID.randomUUID().toString().replace("-", "_").toUpperCase(Locale.ROOT);
        final KeySpace keySpace = RelationalKeyspaceProvider.instance().getKeySpace();
        final KeySpacePath testPath = KeySpaceUtils.toKeySpacePath(URI.create(pathId + "/1"), keySpace);

        // Export with max rows limit
        try (RelationalConnection conn = DriverManager.getConnection("jdbc:embed:/__SYS").unwrap(RelationalConnection.class)) {
            conn.setSchema("CATALOG");
            conn.setAutoCommit(false);

            // Start a transaction explicitly
            EmbeddedRelationalConnection embeddedConn = conn.unwrap(EmbeddedRelationalConnection.class);
            embeddedConn.createNewTransaction();

            // Write 10 records
            for (int i = 0; i < 10; i++) {
                writeTestData(conn, testPath, "key" + i, "value" + i);
            }
            conn.commit();

            try (RelationalStatement stmt = conn.createStatement()) {
                stmt.setMaxRows(5);
                try (RelationalResultSet rs = stmt.executeQuery("COPY " + pathId)) {
                    int count = 0;
                    while (rs.next()) {
                        count++;
                    }
                    assertEquals(5, count, "Should only return 5 rows due to setMaxRows");
                }
            }
        }
    }

    private void writeTestData(@Nonnull RelationalConnection conn, @Nonnull KeySpacePath path, @Nonnull String remainderKey, @Nonnull String value) throws Exception {
        EmbeddedRelationalConnection embeddedConn = conn.unwrap(EmbeddedRelationalConnection.class);
        FDBRecordContext context = embeddedConn.getTransaction().unwrap(RecordContextTransaction.class).getContext();
        byte[] key = path.toSubspace(context).pack(Tuple.from(remainderKey));
        context.ensureActive().set(key, Tuple.from(value).pack());
    }

    private void verifyTestData(@Nonnull RelationalConnection conn, @Nonnull KeySpacePath path, @Nonnull String remainderKey, @Nonnull String expectedValue) throws Exception {
        EmbeddedRelationalConnection embeddedConn = conn.unwrap(EmbeddedRelationalConnection.class);
        FDBRecordContext context = embeddedConn.getTransaction().unwrap(RecordContextTransaction.class).getContext();
        byte[] key = path.toSubspace(context).pack(Tuple.from(remainderKey));
        byte[] actualBytes = context.ensureActive().get(key).join();
        assertNotNull(actualBytes, "Key should exist: " + remainderKey);
        Tuple actualValue = Tuple.fromBytes(actualBytes);
        assertEquals(Tuple.from(expectedValue), actualValue);
    }

    private List<byte[]> exportData(String path, boolean quoted) throws SQLException {
        List<byte[]> exportedData = new ArrayList<>();
        try (RelationalConnection conn = DriverManager.getConnection("jdbc:embed:/__SYS").unwrap(RelationalConnection.class)) {
            conn.setSchema("CATALOG");
            try (RelationalStatement stmt = conn.createStatement();
                     RelationalResultSet rs = stmt.executeQuery("COPY " + (quoted ? "\"" + path + "\"" : path))) {
                while (rs.next()) {
                    exportedData.add(rs.getBytes(1));
                }
            }
        }
        return exportedData;
    }
}
