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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.utils.ConnectionUtils;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for COPY command (export and import).
 */
@Tag(Tags.RequiresFDB)
public class CopyCommandTest {

    @RegisterExtension
    public static final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @ParameterizedTest
    @BooleanSource("quoted")
    void basicExportWithinCluster(boolean quoted) throws Exception {
        // Test basic COPY export functionality
        final String pathId = "/TEST/" + uuidForPath(quoted);
        // Use the shared KeySpace from RelationalKeyspaceProvider
        final KeySpace keySpace = RelationalKeyspaceProvider.instance().getKeySpace();
        final KeySpacePath testPath = KeySpaceUtils.toKeySpacePath(URI.create(pathId + "/1"), keySpace);

        // Write some test data using the connection's FDB context
        writeTestData(testPath, Map.of("key1", "value1", "key2", "value2"));

        // sanity check that the data was written and committed
        verifyTestData(testPath, Map.of("key1", "value1", "key2", "value2"));
        assertEquals(2, exportData(pathId, quoted).size());
    }

    @ParameterizedTest
    @BooleanSource({"namedAndQuoted", "autoCommit"})
    void basicImportWithinCluster(boolean namedAndQuoted, boolean autoCommit) throws Exception {
        // Test basic COPY import functionality with quoted paths (allows hyphens)
        final String sourcePath = "/TEST/" + uuidForPath(namedAndQuoted);
        final String destPath = "/TEST/" + uuidForPath(namedAndQuoted);
        final KeySpace keySpace = RelationalKeyspaceProvider.instance().getKeySpace();
        final KeySpacePath sourceTestPath = KeySpaceUtils.toKeySpacePath(URI.create(sourcePath + "/1"), keySpace);
        final KeySpacePath destTestPath = KeySpaceUtils.toKeySpacePath(URI.create(destPath + "/1"), keySpace);

        writeTestData(sourceTestPath, Map.of("key1", "value1", "key2", "value2"));
        List<byte[]> exportedData = exportData(sourcePath, namedAndQuoted);
        // Clear the source data to ensure import is working correctly
        clearTestData(sourceTestPath);

        // Import to destination (using quoted path)
        importData(namedAndQuoted, autoCommit, destPath, exportedData);

        verifyTestData(destTestPath, Map.of("key1", "value1", "key2", "value2"));
    }

    @ParameterizedTest
    @BooleanSource("namedParameter")
    void wrongParameter(boolean namedParameter) throws Exception {
        // Test if the wrong parameter is set
        final String sourcePath = "/TEST/" + UUID.randomUUID();
        final String destPath = "/TEST/" + UUID.randomUUID();
        List<byte[]> exportedData = exportData(sourcePath, true);

        // Import to destination (using quoted path)
        ConnectionUtils.runAgainstCatalog(conn -> {
            try (RelationalPreparedStatement stmt = conn.prepareStatement("COPY \"" + destPath + "\" FROM " + (namedParameter ? "?data" : "?"))) {
                // set the wrong one
                if (namedParameter) {
                    stmt.setObject(1, exportedData);
                } else {
                    stmt.setObject("data", exportedData);
                }

                RelationalAssertions.assertThrowsSqlException(stmt::executeUpdate)
                        .hasErrorCode(ErrorCode.UNDEFINED_PARAMETER);
            }
        });
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
                RelationalAssertions.assertThrowsSqlException(
                                () -> stmt.executeQuery("COPY /INVALID/PATH/STRUCTURE"))
                        .hasErrorCode(ErrorCode.INVALID_PATH);
            }
        }
    }

    @ParameterizedTest
    @BooleanSource("sourceIsLonger")
    void importInvalidPath(boolean sourceIsLonger) throws RelationalException, SQLException {
        final List<String> source = List.of("TEST", uuidForPath(false), "1");
        final List<String> dest = List.of("TEST", uuidForPath(false), "1");
        final int sourceLength = sourceIsLonger ? 2 : 3;
        final int destLength = sourceIsLonger ? 3 : 2;

        final KeySpace keySpace = RelationalKeyspaceProvider.instance().getKeySpace();
        final KeySpacePath sourceTestPath = KeySpaceUtils.toKeySpacePath(URI.create(String.join("/", source)), keySpace);
        final KeySpacePath destTestPath = KeySpaceUtils.toKeySpacePath(URI.create(String.join("/", dest.subList(0, 2))), keySpace);

        writeTestData(sourceTestPath, Map.of("key1", "value1", "key2", "value2"));
        List<byte[]> exportedData = exportData(String.join("/", source.subList(0, sourceLength)), false);
        // Clear the source data to ensure import is working correctly
        clearTestData(sourceTestPath);

        // Import to destination (using quoted path)
        ConnectionUtils.runAgainstCatalog(conn -> {
            try (RelationalPreparedStatement stmt = conn.prepareStatement("COPY " + maybeQuote(String.join("/", dest.subList(0, destLength)), false) + " FROM ?")) {
                stmt.setObject(1, exportedData);
                RelationalAssertions.assertThrowsSqlException(stmt::executeQuery)
                        .hasErrorCode(ErrorCode.COPY_IMPORT_VALIDATION_ERROR);
            }
        });

        ConnectionUtils.runAgainstCatalog(conn -> {
            conn.setAutoCommit(false);
            final FDBRecordContext context = getRecordContext(conn);
            final List<KeyValue> destData = context.ensureActive().getRange(destTestPath.toSubspace(context).range()).asList().join();
            assertEquals(List.of(), destData);
        });
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 3, 5, 8})
    void exportWithLimitAndContinuation(int limit) throws Exception {
        // Test COPY export with Statement.setMaxRows() limiting (unquoted path)
        final String pathId = "/TEST/" + UUID.randomUUID().toString().replace("-", "_").toUpperCase(Locale.ROOT);
        final KeySpace keySpace = RelationalKeyspaceProvider.instance().getKeySpace();
        final KeySpacePath testPath = KeySpaceUtils.toKeySpacePath(URI.create(pathId + "/1"), keySpace);

        // Export with max rows limit
        // Write 10 records
        Map<String, String> data = new LinkedHashMap<>();
        for (int i = 0; i < 10; i++) {
            data.put("key" + i, "value" + i);
        }
        writeTestData(testPath, data);
        ConnectionUtils.runAgainstCatalog(conn -> {
            try (RelationalStatement stmt = conn.createStatement()) {
                stmt.setMaxRows(limit);
                try (RelationalResultSet rs = stmt.executeQuery("COPY " + pathId)) {
                    final ArrayList<byte[]> results = new ArrayList<>();
                    while (rs.next()) {
                        results.add(rs.getBytes(1));
                    }
                    assertEquals(limit, results.size(), "Should only return " + limit + " rows due to setMaxRows");
                    Continuation continuation = rs.getContinuation();
                    assertFalse(continuation.atEnd());
                    assertFalse(continuation.atBeginning());
                    while (!continuation.atEnd()) {
                        continuation = continueExport(limit, conn, continuation, results);
                    }
                    assertEquals(10, results.size());
                }
            }
        });
    }

    @Test
    void invalidContinuationPlanHash() throws RelationalException, SQLException {
        final String pathId = "/TEST/" + UUID.randomUUID().toString().replace("-", "_").toUpperCase(Locale.ROOT);
        final KeySpace keySpace = RelationalKeyspaceProvider.instance().getKeySpace();
        final KeySpacePath testPath = KeySpaceUtils.toKeySpacePath(URI.create(pathId + "/1"), keySpace);

        // Export with max rows limit
        // Write 10 records
        Map<String, String> data = new LinkedHashMap<>();
        for (int i = 0; i < 10; i++) {
            data.put("key" + i, "value" + i);
        }
        writeTestData(testPath, data);
        ConnectionUtils.runAgainstCatalog(conn -> {
            try (RelationalStatement stmt = conn.createStatement()) {
                final int limit = 3;
                stmt.setMaxRows(limit);
                try (RelationalResultSet rs = stmt.executeQuery("COPY " + pathId)) {
                    final ArrayList<byte[]> results = new ArrayList<>();
                    while (rs.next()) {
                        results.add(rs.getBytes(1));
                    }
                    assertEquals(limit, results.size(), "Should only return 3 rows due to setMaxRows");
                    Continuation continuation = rs.getContinuation();
                    assertFalse(continuation.atEnd());
                    assertFalse(continuation.atBeginning());
                    final ContinuationImpl continuationImpl = (ContinuationImpl)rs.getContinuation();
                    final Continuation corruptedContinuation = continuationImpl.asBuilder()
                            .withPlanHash(continuationImpl.getPlanHash() + 3)
                            .build();
                    RelationalAssertions.assertThrowsSqlException(
                                    () -> continueExport(limit, conn, corruptedContinuation, results))
                            .hasErrorCode(ErrorCode.INVALID_CONTINUATION);
                }
            }
        });
    }

    @Nonnull
    private static Continuation continueExport(final int limit,
                                               final RelationalConnection connection,
                                               Continuation continuation,
                                               final ArrayList<byte[]> results) throws SQLException {
        try (final var preparedStatement = connection.prepareStatement("EXECUTE CONTINUATION ?param")) {
            preparedStatement.setBytes("param", continuation.serialize());
            preparedStatement.setMaxRows(limit);
            try (final RelationalResultSet resultSet = preparedStatement.executeQuery()) {
                int count = 0;
                while (resultSet.next()) {
                    results.add(resultSet.getBytes(1));
                    count++;
                }
                continuation = resultSet.getContinuation();
                if (continuation.atEnd()) {
                    assertThat(count).isLessThan(limit);
                } else {
                    assertEquals(limit, count, "Should only return 5 rows due to setMaxRows" + continuation);
                }
            }
        }
        return continuation;
    }

    @Test
    void useCopyToRestoreDatabase() throws Exception {
        // Test restoring relational data using COPY
        final String uuidName = uuidForPath(false);
        final String databaseName = "/TEST/DB_" + uuidName;
        final String schemaName = "1";
        final String schemaPath = databaseName + "/" + schemaName;
        String templateName = "TEMPLATE_" + uuidName;

        // Export from source (using quoted path)
        // create a schema
        ConnectionUtils.runCatalogStatement(stmt -> {
            stmt.executeUpdate("CREATE SCHEMA TEMPLATE " + templateName +
                    " CREATE TABLE my_table (id bigint, col1 string, PRIMARY KEY(id))");
            stmt.executeUpdate("CREATE DATABASE " + databaseName);
            stmt.executeUpdate("CREATE SCHEMA " + databaseName + "/" + schemaName + " WITH TEMPLATE " + templateName);
        });

        ConnectionUtils.runStatementUpdate(databaseName, "1", "INSERT INTO my_table VALUES (1, 'a'), (2, 'b')");

        List<byte[]> exportedData = exportData(schemaPath, false);

        ConnectionUtils.runStatement(databaseName, schemaName, stmt -> {
            stmt.executeUpdate("DELETE FROM my_table");
            assertFalse(stmt.executeQuery("SELECT * FROM my_table").next(), "There should be no data in the table");
        });

        final int importCount = importData(false, true, schemaPath, exportedData);
        assertThat(importCount).isGreaterThan(2); // we will import at least the rows saved, but also other internal data

        ConnectionUtils.runStatement(databaseName, schemaName, stmt ->
                ResultSetAssert.assertThat(stmt.executeQuery("SELECT * FROM my_table"))
                        .containsRowsExactly(List.of(
                                List.of(1, "a"),
                                List.of(2, "b")
                        )));
    }

    @Nonnull
    private static String uuidForPath(final boolean quoted) {
        if (quoted) {
            return UUID.randomUUID().toString();
        } else {
            return UUID.randomUUID().toString().toUpperCase(Locale.ROOT).replace("-", "_");
        }
    }

    private String maybeQuote(final String path, final boolean quoted) {
        if (quoted) {
            return "\"" + path + "\"";
        } else {
            return path;
        }
    }

    private void writeTestData(@Nonnull KeySpacePath path, @Nonnull Map<String, String> data) throws SQLException, RelationalException {
        ConnectionUtils.runAgainstCatalog(conn -> {
            conn.setAutoCommit(false);
            final FDBRecordContext context = getRecordContext(conn);
            data.forEach((remainder, value) -> {
                byte[] key = path.toSubspace(context).pack(Tuple.from(remainder));
                context.ensureActive().set(key, Tuple.from(value).pack());
            });

            conn.commit();
        });
    }

    private void clearTestData(@Nonnull KeySpacePath path) throws SQLException, RelationalException {
        ConnectionUtils.runAgainstCatalog(conn -> {
            conn.setAutoCommit(false);
            final FDBRecordContext context = getRecordContext(conn);
            context.ensureActive().clear(path.toSubspace(context).range());
            conn.commit();
        });
    }

    private void verifyTestData(@Nonnull KeySpacePath path, @Nonnull Map<String, String> expectedData) throws SQLException, RelationalException {
        ConnectionUtils.runAgainstCatalog(conn -> {
            conn.setAutoCommit(false);
            final FDBRecordContext context = getRecordContext(conn);
            expectedData.forEach((remainder, expectedValue) -> {
                byte[] key = path.toSubspace(context).pack(Tuple.from(remainder));
                byte[] actualBytes = context.ensureActive().get(key).join();
                assertNotNull(actualBytes, "Key should exist: " + remainder);
                Tuple actualValue = Tuple.fromBytes(actualBytes);
                assertEquals(Tuple.from(expectedValue), actualValue);
            });
        });
    }

    private static FDBRecordContext getRecordContext(final @Nonnull RelationalConnection conn) throws SQLException, RelationalException {
        EmbeddedRelationalConnection embeddedConn = conn.unwrap(EmbeddedRelationalConnection.class);
        embeddedConn.createNewTransaction();
        return embeddedConn.getTransaction().unwrap(RecordContextTransaction.class).getContext();
    }

    private List<byte[]> exportData(String path, boolean quoted) throws SQLException, RelationalException {
        return ConnectionUtils.getFromCatalog(conn -> {
            try (RelationalStatement stmt = conn.createStatement();
                     RelationalResultSet rs = stmt.executeQuery("COPY " + maybeQuote(path, quoted))) {
                List<byte[]> exportedData = new ArrayList<>();
                while (rs.next()) {
                    exportedData.add(rs.getBytes(1));
                }
                return exportedData;
            }
        });
    }

    private int importData(final boolean namedAndQuoted, final boolean autoCommit, final String destPath,
                           final List<byte[]> exportedData) throws SQLException, RelationalException {
        return ConnectionUtils.getFromCatalog(conn -> {
            conn.setAutoCommit(autoCommit);
            int resultingCount;
            try (RelationalPreparedStatement stmt = conn.prepareStatement("COPY " + maybeQuote(destPath, namedAndQuoted) + " FROM " + (namedAndQuoted ? "?data" : "?"))) {
                if (namedAndQuoted) {
                    stmt.setObject("data", exportedData);
                } else {
                    stmt.setObject(1, exportedData);
                }

                final RelationalResultSet relationalResultSet = stmt.executeQuery();
                assertTrue(relationalResultSet.next());
                resultingCount = relationalResultSet.getInt("COUNT");
                assertEquals(resultingCount, relationalResultSet.getInt(1));
                assertFalse(relationalResultSet.next());
            }
            if (!autoCommit) {
                conn.commit();
            }
            return resultingCount;
        });
    }
}
