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
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreKeyspace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.DataInKeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePathSerializer;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.copy.CopyData;
import com.apple.foundationdb.relational.recordlayer.query.CopyPlan;
import com.apple.foundationdb.relational.recordlayer.query.MutablePlanGenerationContext;
import com.apple.foundationdb.relational.recordlayer.query.Plan;
import com.apple.foundationdb.relational.recordlayer.query.PreparedParams;
import com.apple.foundationdb.relational.utils.ConnectionUtils;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.test.FDBTestEnvironment;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.protobuf.InvalidProtocolBufferException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    public ConnectionUtils connectionUtils;

    @BeforeEach
    void setUp() {
        connectionUtils = new ConnectionUtils(relationalExtension.getDriver());
    }

    @ParameterizedTest
    @BooleanSource("quoted")
    void basicExportWithinCluster(boolean quoted) throws Exception {
        // Test basic COPY export functionality
        final SchemaInfo schemaInfo = new SchemaInfo("/TEST/" + uuidForPath(quoted), "1", connectionUtils);
        // Use the shared KeySpace from RelationalKeyspaceProvider
        final KeySpace keySpace = RelationalKeyspaceProvider.instance().getKeySpace();
        final KeySpacePath testPath = KeySpaceUtils.toKeySpacePath(URI.create(schemaInfo.schemaPath), keySpace);

        // Write some test data using the connection's FDB context
        writeTestData(connectionUtils, testPath, Map.of("key1", "value1", "key2", "value2"));

        // sanity check that the data was written and committed
        verifyTestData(connectionUtils, testPath, Map.of("key1", "value1", "key2", "value2"));
        assertEquals(2, exportData(quoted, schemaInfo).size());
    }

    @ParameterizedTest
    @BooleanSource({"namedAndQuoted", "autoCommit"})
    void basicImportWithinCluster(boolean namedAndQuoted, boolean autoCommit) throws Exception {
        // Test basic COPY import functionality with quoted paths (allows hyphens)
        final SchemaInfo source = new SchemaInfo("/TEST/" + uuidForPath(namedAndQuoted), "1", connectionUtils);
        final SchemaInfo dest = new SchemaInfo("/TEST/" + uuidForPath(namedAndQuoted), "1", connectionUtils);
        final KeySpace keySpace = RelationalKeyspaceProvider.instance().getKeySpace();
        final KeySpacePath sourceTestPath = KeySpaceUtils.toKeySpacePath(URI.create(source.schemaPath), keySpace);
        final KeySpacePath destTestPath = KeySpaceUtils.toKeySpacePath(URI.create(dest.schemaPath), keySpace);

        writeTestData(connectionUtils, sourceTestPath, Map.of("key1", "value1", "key2", "value2"));
        List<byte[]> exportedData = exportData(namedAndQuoted, source);
        // Clear the source data to ensure import is working correctly
        clearTestData(connectionUtils, sourceTestPath);

        // Import to destination (using quoted path)
        importDatabase(namedAndQuoted, autoCommit, dest, exportedData);

        verifyTestData(connectionUtils, destTestPath, Map.of("key1", "value1", "key2", "value2"));
    }

    @ParameterizedTest
    @BooleanSource("withExecutionContext")
    void withExecutionContext(boolean withExecutionContext) throws RelationalException, SQLException {
        // Test basic COPY import functionality with quoted paths (allows hyphens)
        final SchemaInfo source = new SchemaInfo("/TEST/" + uuidForPath(false), "1", connectionUtils);
        final SchemaInfo dest = new SchemaInfo("/TEST/" + uuidForPath(false), "1", connectionUtils);
        final KeySpace keySpace = RelationalKeyspaceProvider.instance().getKeySpace();
        final KeySpacePath sourceTestPath = KeySpaceUtils.toKeySpacePath(URI.create(source.schemaPath), keySpace);

        writeTestData(connectionUtils, sourceTestPath, Map.of("key1", "value1", "key2", "value2"));
        List<byte[]> exportedData1 = exportData(false, source);
        writeTestData(connectionUtils, sourceTestPath, Map.of("key1", "newValueX", "key3", "value3"));
        List<byte[]> exportedData2 = exportData(false, source);
        // Clear the source data to ensure import is working correctly
        clearTestData(connectionUtils, sourceTestPath);

        // Import to destination (using quoted path)
        int importedCount = connectionUtils.getFromCatalog(conn -> {
            final EmbeddedRelationalConnection embeddedConnection = (EmbeddedRelationalConnection)conn;
            embeddedConnection.createNewTransaction();
            int count;

            final String sql = "COPY " + maybeQuote(dest.databasePath, false) + " FROM ?";
            MutablePlanGenerationContext context = getMutablePlanGenerationContext(sql, exportedData1);
            CopyPlan copyImportAction = CopyPlan.getCopyImportAction(dest.databasePath, context);

            if (withExecutionContext) {
                context = getMutablePlanGenerationContext(sql, exportedData2);
                copyImportAction = copyImportAction.withExecutionContext(context);
            }
            final Plan.ExecutionContext executionContext = Plan.ExecutionContext.of(
                    embeddedConnection.getTransaction(),
                    Options.NONE, conn, embeddedConnection.getMetricCollector());
            try (final RelationalResultSet relationalResultSet = copyImportAction.execute(executionContext)) {
                assertTrue(relationalResultSet.next());
                count = relationalResultSet.getInt("COUNT");
                assertEquals(count, relationalResultSet.getInt(1));
                assertFalse(relationalResultSet.next());
            }

            return count;
        });
        assertEquals(withExecutionContext ? 3 : 2, importedCount);

        final KeySpacePath destTestPath = KeySpaceUtils.toKeySpacePath(URI.create(dest.schemaPath), keySpace);
        if (withExecutionContext) {
            verifyTestData(connectionUtils, destTestPath, Map.of("key1", "newValueX", "key2", "value2",
                    "key3", "value3"));
        } else {
            verifyTestData(connectionUtils, destTestPath, Map.of("key1", "value1", "key2", "value2"));
        }
    }

    @Nonnull
    private static MutablePlanGenerationContext getMutablePlanGenerationContext(final String sql, final List<byte[]> exportedData) {
        final PreparedParams preparedParams = PreparedParams.of(Map.of(1, exportedData), Map.of());
        final MutablePlanGenerationContext context = new MutablePlanGenerationContext(preparedParams,
                PlanHashable.PlanHashMode.VC1, sql, sql, 0);
        context.processUnnamedPreparedParam(1);
        return context;
    }

    @ParameterizedTest
    @BooleanSource("namedParameter")
    void validateWhetherIndexOrNamedParameter(boolean namedParameter) throws Exception {
        // Test if the wrong parameter is set
        final SchemaInfo source = new SchemaInfo("/TEST/" + UUID.randomUUID(), "1", connectionUtils);
        final SchemaInfo dest = new SchemaInfo("/TEST/" + UUID.randomUUID(), "1", connectionUtils);
        List<byte[]> exportedData = exportData(true, source);

        // Import to destination (using quoted path)
        dest.connectionUtils.runAgainstCatalog(conn -> {
            try (RelationalPreparedStatement stmt = conn.prepareStatement("COPY \"" + dest.databasePath + "\" FROM " + (namedParameter ? "?data" : "?"))) {
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

        connectionUtils.runAgainstCatalog(conn -> {
            try (RelationalStatement stmt = conn.createStatement();
                     RelationalResultSet rs = stmt.executeQuery("COPY " + pathId + " PRESERVE INCARNATION")) {
                assertFalse(rs.next(), "Empty path should return empty result set");
            }
        });
    }

    @Test
    void exportInvalidPath() throws Exception {
        // Test exporting from an invalid path - should throw error
        connectionUtils.runAgainstCatalog(conn -> {
            try (RelationalStatement stmt = conn.createStatement()) {
                RelationalAssertions.assertThrowsSqlException(
                                () -> stmt.executeQuery("COPY /INVALID/PATH/STRUCTURE PRESERVE INCARNATION"))
                        .hasErrorCode(ErrorCode.INVALID_PATH);
            }
        });
    }

    @ParameterizedTest
    @BooleanSource("incrementIncarnation")
    void exportWithIncarnationOption(boolean incrementIncarnation) throws Exception {
        final String pathId = "/TEST/" + UUID.randomUUID().toString().replace("-", "_").toUpperCase(Locale.ROOT);
        final KeySpace keySpace = RelationalKeyspaceProvider.instance().getKeySpace();
        final KeySpacePath databasePath = KeySpaceUtils.toKeySpacePath(URI.create(pathId), keySpace);
        final KeySpacePath schemaPath = KeySpaceUtils.toKeySpacePath(URI.create(pathId + "/1"), keySpace);

        // Write a DataStoreInfo with a known incarnation as the STORE_INFO key
        final int originalIncarnation = 42;
        final RecordMetaDataProto.DataStoreInfo storeInfo = RecordMetaDataProto.DataStoreInfo.newBuilder()
                .setFormatVersion(13) // FormatVersion.INCARNATION
                .setIncarnation(originalIncarnation)
                .build();

        connectionUtils.runAgainstCatalog(conn -> {
            conn.setAutoCommit(false);
            final FDBRecordContext context = getRecordContext(conn);
            byte[] key = schemaPath.toSubspace(context).pack(
                    Tuple.from(FDBRecordStoreKeyspace.STORE_INFO.id()));
            context.ensureActive().set(key, storeInfo.toByteArray());
            conn.commit();
        });

        // Export with the incarnation option
        final String incarnationClause = incrementIncarnation ? "INCREMENT INCARNATION" : "PRESERVE INCARNATION";
        final List<byte[]> exportedData = connectionUtils.getFromCatalog(conn -> {
            try (RelationalStatement stmt = conn.createStatement();
                     RelationalResultSet rs = stmt.executeQuery("COPY " + pathId + " " + incarnationClause)) {
                List<byte[]> data = new ArrayList<>();
                while (rs.next()) {
                    data.add(rs.getBytes(1));
                }
                return data;
            }
        });

        assertThat(exportedData).hasSize(1);

        // Parse the exported data and check the incarnation
        final CopyData copyData = CopyData.parseFrom(exportedData.get(0));
        final KeySpacePathSerializer serializer = new KeySpacePathSerializer(databasePath);
        final DataInKeySpacePath dataInKeySpacePath = serializer.deserialize(copyData.getData());
        final RecordMetaDataProto.DataStoreInfo exportedInfo =
                RecordMetaDataProto.DataStoreInfo.parseFrom(dataInKeySpacePath.getValue());

        if (incrementIncarnation) {
            assertEquals(originalIncarnation + 1, exportedInfo.getIncarnation());
        } else {
            assertEquals(originalIncarnation, exportedInfo.getIncarnation());
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

        writeTestData(connectionUtils, sourceTestPath, Map.of("key1", "value1", "key2", "value2"));
        List<byte[]> exportedData = exportData(false, String.join("/", source.subList(0, sourceLength)), connectionUtils);
        // Clear the source data to ensure import is working correctly
        clearTestData(connectionUtils, sourceTestPath);

        // Import to destination (using quoted path)
        connectionUtils.runAgainstCatalog(conn -> {
            final String destUri = maybeQuote(String.join("/", dest.subList(0, destLength)), false);
            try (RelationalPreparedStatement stmt = conn.prepareStatement("COPY " + destUri + " FROM ?")) {
                stmt.setObject(1, exportedData);
                RelationalAssertions.assertThrowsSqlException(stmt::executeQuery)
                        .hasErrorCode(ErrorCode.COPY_IMPORT_VALIDATION_ERROR);
            }
        });

        verifyTestData(connectionUtils, destTestPath, Map.of());
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
        writeTestData(connectionUtils, testPath, tenKeyValueRecords());
        connectionUtils.runAgainstCatalog(conn -> {
            try (RelationalStatement stmt = conn.createStatement()) {
                stmt.setMaxRows(limit);
                try (RelationalResultSet rs = stmt.executeQuery("COPY " + pathId + " PRESERVE INCARNATION")) {
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
        writeTestData(connectionUtils, testPath, tenKeyValueRecords());
        connectionUtils.runAgainstCatalog(conn -> {
            try (RelationalStatement stmt = conn.createStatement()) {
                final int limit = 3;
                stmt.setMaxRows(limit);
                try (RelationalResultSet rs = stmt.executeQuery("COPY " + pathId + " PRESERVE INCARNATION")) {
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
                    assertEquals(limit, count, "Should only return " + limit + " rows due to setMaxRows" + continuation);
                }
            }
        }
        return continuation;
    }

    @Test
    void useCopyToRestoreDatabase() throws Exception {
        // Test restoring relational data using COPY
        final String uuidName = uuidForPath(false);
        final SchemaInfo schema = new SchemaInfo("/TEST/DB_" + uuidName, "1", connectionUtils);
        String templateName = "TEMPLATE_" + uuidName;

        try {
            // Export from source (using quoted path)
            // create a schema
            connectionUtils.runCatalogStatement(stmt -> {
                stmt.executeUpdate("CREATE SCHEMA TEMPLATE " + templateName +
                        " CREATE TABLE my_table (id bigint, col1 string, PRIMARY KEY(id))");
                stmt.executeUpdate("CREATE DATABASE " + schema.databasePath);
                stmt.executeUpdate("CREATE SCHEMA " + schema.schemaPath + " WITH TEMPLATE " + templateName);
            });

            connectionUtils.runStatementUpdate(schema.databasePath, schema.schemaName, "INSERT INTO my_table VALUES (1, 'a'), (2, 'b')");

            List<byte[]> exportedData = exportData(false, schema);

            connectionUtils.runStatement(schema.databasePath, schema.schemaName, stmt -> {
                stmt.executeUpdate("DELETE FROM my_table");
                assertFalse(stmt.executeQuery("SELECT * FROM my_table").next(), "There should be no data in the table");
            });

            final int importCount = importDatabase(false, true, schema, exportedData);
            assertThat(importCount).isGreaterThan(2); // we will import at least the rows saved, but also other internal data

            connectionUtils.runStatement(schema.databasePath, schema.schemaName, stmt ->
                    ResultSetAssert.assertThat(stmt.executeQuery("SELECT * FROM my_table"))
                            .containsRowsExactly(List.of(
                                    List.of(1, "a"),
                                    List.of(2, "b")
                            )));
        } finally {
            connectionUtils.runCatalogStatement(stmt -> {
                stmt.executeUpdate("DROP SCHEMA TEMPLATE " + templateName);
                stmt.executeUpdate("DROP DATABASE " + schema.databasePath);
            });
        }
    }

    @ParameterizedTest
    @BooleanSource("quoted")
    void copyCatalog(boolean quoted) throws RelationalException, SQLException {
        final CatalogTestSetup setup = new CatalogTestSetup(quoted, connectionUtils);
        final List<Pair<Integer, String>> data = List.of(
                Pair.of(1, "a"),
                Pair.of(2, "b"),
                Pair.of(3, "c")
        );
        try {
            // Create a schema template and source database
            connectionUtils.runCatalogStatement(stmt -> {
                createSchemaTemplate(quoted, stmt, setup.templateName);
                createDatabase(quoted, stmt, setup.source.databasePath);
                createSchema(quoted, stmt, setup.source.databasePath, setup.source.schemaName, setup.templateName);
            });

            // Insert some records in the source database using SQL
            insertData(setup.source, data);

            // Export data from source database
            List<byte[]> exportedData = exportData(quoted, setup.source);

            assertSchemaTemplateCount(exportedData, 1);

            // Import to destination database path
            final int importCount = importDatabase(quoted, true, setup.dest, exportedData);
            assertThat(importCount).isGreaterThan(3); // we will import at least the rows saved, but also other internal data

            assertDataExists(setup.dest, data);
        } finally {
            dropTemplateAndDatabase(quoted, List.of(setup.templateName), setup.source);
            dropTemplateAndDatabase(quoted, List.of(setup.templateName), setup.dest);
        }
    }

    @ParameterizedTest
    @BooleanSource("quoted")
    void copyCatalogWithIndexes(boolean quoted) throws RelationalException, SQLException {
        final CatalogTestSetup setup = new CatalogTestSetup(quoted, connectionUtils);
        final List<Pair<Integer, String>> data = List.of(
                Pair.of(3, "charlie"),
                Pair.of(1, "alice"),
                Pair.of(2, "bob")
        );
        try {
            // Create a schema template with an index
            connectionUtils.runCatalogStatement(stmt -> {
                stmt.executeUpdate("CREATE SCHEMA TEMPLATE " + maybeQuote(setup.templateName, quoted) +
                        " CREATE TABLE my_table (id bigint, col1 string, PRIMARY KEY(id))" +
                        " CREATE INDEX idx_col1 AS SELECT col1 FROM my_table");
                createDatabase(quoted, stmt, setup.source.databasePath);
                createSchema(quoted, stmt, setup.source.databasePath, setup.source.schemaName, setup.templateName);
            });

            // Insert some records in the source database using SQL
            insertData(setup.source, data);

            // Export data from source database
            List<byte[]> exportedData = exportData(quoted, setup.source);

            assertSchemaTemplateCount(exportedData, 1);

            // Import to destination database path
            final int importCount = importDatabase(quoted, true, setup.dest, exportedData);
            assertThat(importCount).isGreaterThan(6); // we will import at least the rows saved & index entries, but also other internal data

            // Verify that the records exist in the destination and that the index works
            // by querying with ORDER BY on the indexed column
            setup.dest.connectionUtils.runStatement(setup.dest.databasePath, setup.dest.schemaName, stmt ->
                    ResultSetAssert.assertThat(stmt.executeQuery("SELECT id, col1 FROM my_table ORDER BY col1"))
                            .containsRowsExactly(List.of(
                                    List.of(1, "alice"),
                                    List.of(2, "bob"),
                                    List.of(3, "charlie")
                            )));
        } finally {
            dropTemplateAndDatabase(quoted, List.of(setup.templateName), setup.source);
            dropTemplateAndDatabase(quoted, List.of(setup.templateName), setup.dest);
        }
    }

    @ParameterizedTest
    @BooleanSource({"quoted", "multiCluster"})
    void copyCatalogWithMultipleSchemas(boolean quoted, boolean multiCluster) throws RelationalException, SQLException {
        if (multiCluster) {
            FDBTestEnvironment.assumeClusterCount(2);
        }
        final String uuidName = uuidForPath(quoted);
        final ConnectionUtils sourceConnectionUtils = connectionUtils;
        final String sourceDatabasePath = "/TEST/SOURCE_DB_" + uuidName;
        final String destDatabasePath = multiCluster ? sourceDatabasePath : "/TEST/DEST_DB_" + uuidName;
        String templateName1 = "TEMPLATE1_" + uuidName;
        String templateName2 = "TEMPLATE2_" + uuidName;
        final ConnectionUtils destConnectionUtils = multiCluster ?
                                                    new ConnectionUtils(relationalExtension.extensionForOtherCluster().getDriver()) :
                                                    connectionUtils;
        final List<Moveable> moveables = IntStream.of(0, 1, 2)
                .mapToObj(index -> {
                    String name = "SCHEMA" + index; // all caps so I don't have to worry about quoting
                    return new Moveable(
                            new SchemaInfo(sourceDatabasePath, name, sourceConnectionUtils),
                            new SchemaInfo(destDatabasePath, name, destConnectionUtils),
                            index < 2 ? templateName1 : templateName2,
                            index < 2 ? "my_table" : "other_table",
                            List.of(Pair.of(index, "data" + index))
                        );
                })
                .collect(Collectors.toList());
        try {
            // Create two schema templates and source database with 3 schemas
            sourceConnectionUtils.runCatalogStatement(stmt -> {
                // Template 1 with my_table
                stmt.executeUpdate("CREATE SCHEMA TEMPLATE " + maybeQuote(templateName1, quoted) +
                        " CREATE TABLE my_table (id bigint, col1 string, PRIMARY KEY(id))");
                // Template 2 with other_table
                stmt.executeUpdate("CREATE SCHEMA TEMPLATE " + maybeQuote(templateName2, quoted) +
                        " CREATE TABLE other_table (id bigint, col2 string, PRIMARY KEY(id))");
                createDatabase(quoted, stmt, sourceDatabasePath);
                for (Moveable moveable : moveables) {
                    createSchema(quoted, stmt, sourceDatabasePath, moveable.source.schemaName, moveable.templateName);
                }
            });
            
            // Insert some records in the source database using SQL
            for (final Moveable moveable : moveables) {
                insertData(moveable.source, moveable.tableName, moveable.data);
            }

            // Export data from source database
            List<byte[]> exportedData = exportData(quoted, sourceDatabasePath, sourceConnectionUtils);

            assertSchemaTemplateCount(exportedData, 3);

            // Import to destination database path
            final int importCount = importDatabase(quoted, true, exportedData, destConnectionUtils, destDatabasePath);
            assertThat(importCount).isGreaterThan(3); // we will import at least the rows saved, but also other internal data

            // Verify that the records exist in all three schemas
            for (final Moveable moveable : moveables) {
                assertDataExists(moveable.dest, moveable.tableName, moveable.data);
            }
        } finally {
            dropTemplateAndDatabase(quoted, List.of(templateName1, templateName2), sourceConnectionUtils, sourceDatabasePath);
            dropTemplateAndDatabase(quoted, List.of(templateName1, templateName2), destConnectionUtils, destDatabasePath);
        }
    }

    @ParameterizedTest
    @BooleanSource("quoted")
    void copyCatalogFailsOnTemplateMismatch(boolean quoted) throws RelationalException, SQLException {
        final CatalogTestSetup setup = new CatalogTestSetup(false, connectionUtils);
        String sourceTemplateName = "SOURCE_TEMPLATE_" + setup.uuidName;
        String destTemplateName = "DEST_TEMPLATE_" + setup.uuidName;

        final List<Pair<Integer, String>> data = List.of(
                Pair.of(1, "a")
        );

        try {
            // Create source database with a schema template
            connectionUtils.runCatalogStatement(stmt -> {
                stmt.executeUpdate("CREATE SCHEMA TEMPLATE " + sourceTemplateName +
                        " CREATE TABLE my_table (id bigint, col1 string, PRIMARY KEY(id))");
                stmt.executeUpdate("CREATE DATABASE " + setup.source.databasePath);
                stmt.executeUpdate("CREATE SCHEMA " + setup.source.schemaPath + " WITH TEMPLATE " + sourceTemplateName);
            });

            // Create destination database with a DIFFERENT schema template
            connectionUtils.runCatalogStatement(stmt -> {
                stmt.executeUpdate("CREATE SCHEMA TEMPLATE " + destTemplateName +
                        " CREATE TABLE other_table (id bigint, col2 string, PRIMARY KEY(id))");
                stmt.executeUpdate("CREATE DATABASE " + setup.dest.databasePath);
                stmt.executeUpdate("CREATE SCHEMA " + setup.dest.schemaPath + " WITH TEMPLATE " + destTemplateName);
            });

            // Insert some records in the source database using SQL
            insertData(setup.source, data);

            // Export data from source database
            List<byte[]> exportedData = exportData(quoted, setup.source);

            // Try to import to destination database - should fail because schema exists with different template
            RelationalAssertions.assertThrowsSqlException(() -> importDatabase(quoted, true, setup.dest, exportedData))
                    .hasErrorCode(ErrorCode.INVALID_SCHEMA_TEMPLATE);
        } finally {
            dropTemplateAndDatabase(quoted, List.of(sourceTemplateName), setup.source);
            dropTemplateAndDatabase(quoted, List.of(destTemplateName), setup.dest);
        }
    }

    private static void assertDataExists(final SchemaInfo schemaInfo,
                                  final List<Pair<Integer, String>> data) throws SQLException, RelationalException {
        assertDataExists(schemaInfo, "my_table", data);
    }

    private static void assertDataExists(final SchemaInfo schemaInfo,
                                  final String tableName, final List<Pair<Integer, String>> data) throws SQLException, RelationalException {
        schemaInfo.connectionUtils.runStatement(schemaInfo.databasePath, schemaInfo.schemaName, stmt ->
                ResultSetAssert.assertThat(stmt.executeQuery("SELECT * FROM " + tableName))
                        .containsRowsExactly(data.stream()
                                .map(pair -> List.<Object>of(pair.getLeft(), pair.getRight()))
                                .collect(Collectors.toList())));
    }

    private static void dropTemplateAndDatabase(final boolean quoted, final List<String> templateNames, final SchemaInfo schemaInfo)
            throws SQLException, RelationalException {
        dropTemplateAndDatabase(quoted, templateNames, schemaInfo.connectionUtils, schemaInfo.databasePath);
    }

    private static void dropTemplateAndDatabase(final boolean quoted, final List<String> templateNames,
                                                final ConnectionUtils connectionUtils,
                                                final String path)
            throws SQLException, RelationalException {
        connectionUtils.runCatalogStatement(stmt -> {
            for (final String templateName : templateNames) {
                stmt.executeUpdate("DROP SCHEMA TEMPLATE IF EXISTS " + maybeQuote(templateName, quoted));
            }
            stmt.executeUpdate("DROP DATABASE IF EXISTS " + maybeQuote(path, quoted));
        });
    }

    private static void assertSchemaTemplateCount(final List<byte[]> exportedData, int expectedCount) {
        final List<CopyData> parsedData = exportedData.stream().map(raw -> {
            try {
                return CopyData.parseFrom(raw);
            } catch (InvalidProtocolBufferException e) {
                return Assertions.fail("Export should be parseable");
            }
        }).collect(Collectors.toList());
        // we should have exactly one entry with a schema template, we don't want to be sending it for every record
        assertThat(parsedData).filteredOn(CopyData::hasCatalogInfo).hasSize(expectedCount);
    }

    private static void insertData(final SchemaInfo schemaInfo,
                                   final List<Pair<Integer, String>> data) throws SQLException, RelationalException {
        insertData(schemaInfo, "my_table", data);
    }

    private static void insertData(final SchemaInfo schemaInfo,
                                   final String tableName, final List<Pair<Integer, String>> data) throws SQLException, RelationalException {
        schemaInfo.connectionUtils.runStatementUpdate(schemaInfo.databasePath, schemaInfo.schemaName,
                "INSERT INTO " + tableName + " VALUES "
                + (data.stream().map(pair -> pair.getLeft() + ", '" + pair.getRight() + "'")
                           .collect(Collectors.joining("), (", "(", ")"))));
    }

    private void createSchema(final boolean quoted, final RelationalStatement stmt, final String sourceDatabaseName, final String schemaName, final String templateName) throws SQLException {
        stmt.executeUpdate("CREATE SCHEMA " + maybeQuote(sourceDatabaseName + "/" + schemaName, quoted) +
                " WITH TEMPLATE " + maybeQuote(templateName, quoted));
    }

    private void createDatabase(final boolean quoted, final RelationalStatement stmt, final String sourceDatabaseName) throws SQLException {
        stmt.executeUpdate("CREATE DATABASE " + maybeQuote(sourceDatabaseName, quoted));
    }

    private void createSchemaTemplate(final boolean quoted, final RelationalStatement stmt, final String templateName) throws SQLException {
        stmt.executeUpdate("CREATE SCHEMA TEMPLATE " + maybeQuote(templateName, quoted) +
                " CREATE TABLE my_table (id bigint, col1 string, PRIMARY KEY(id))");
    }

    @Nonnull
    private static String uuidForPath(final boolean quoted) {
        if (quoted) {
            return UUID.randomUUID().toString();
        } else {
            return UUID.randomUUID().toString().toUpperCase(Locale.ROOT).replace("-", "_");
        }
    }

    @Nonnull
    private static Map<String, String> tenKeyValueRecords() {
        final Map<String, String> data = new LinkedHashMap<>();
        for (int i = 0; i < 10; i++) {
            data.put("key" + i, "value" + i);
        }
        return data;
    }

    private static String maybeQuote(final String path, final boolean quoted) {
        if (quoted) {
            return "\"" + path + "\"";
        } else {
            return path;
        }
    }

    private static void writeTestData(final ConnectionUtils connectionUtils, @Nonnull KeySpacePath path,
                                      @Nonnull Map<String, String> data) throws SQLException, RelationalException {
        connectionUtils.runAgainstCatalog(conn -> {
            conn.setAutoCommit(false);
            final FDBRecordContext context = getRecordContext(conn);
            data.forEach((remainder, value) -> {
                byte[] key = path.toSubspace(context).pack(Tuple.from(remainder));
                context.ensureActive().set(key, Tuple.from(value).pack());
            });

            conn.commit();
        });
    }

    private static void clearTestData(final ConnectionUtils connectionUtils, @Nonnull KeySpacePath path) throws SQLException, RelationalException {
        connectionUtils.runAgainstCatalog(conn -> {
            conn.setAutoCommit(false);
            final FDBRecordContext context = getRecordContext(conn);
            context.ensureActive().clear(path.toSubspace(context).range());
            conn.commit();
        });
    }

    private static void verifyTestData(final ConnectionUtils connectionUtils, @Nonnull KeySpacePath path,
                                       @Nonnull Map<String, String> expectedData) throws SQLException, RelationalException {
        connectionUtils.runAgainstCatalog(conn -> {
            conn.setAutoCommit(false);
            final FDBRecordContext context = getRecordContext(conn);
            final List<KeyValue> destData = context.ensureActive().getRange(path.toSubspace(context).range()).asList().join();
            final Map<Tuple, Tuple> actualData = destData.stream().collect(Collectors.toMap(keyValue -> Tuple.fromBytes(keyValue.getKey()),
                    keyValue -> Tuple.fromBytes(keyValue.getValue())));
            final Map<Tuple, Tuple> expectedKeyValues = expectedData.entrySet().stream().collect(Collectors.toMap(
                    entry -> path.toTuple(context).add(entry.getKey()),
                    entry -> Tuple.from(entry.getValue())));
            assertEquals(expectedKeyValues, actualData);
        });
    }

    private static FDBRecordContext getRecordContext(final @Nonnull RelationalConnection conn) throws SQLException, RelationalException {
        EmbeddedRelationalConnection embeddedConn = conn.unwrap(EmbeddedRelationalConnection.class);
        embeddedConn.createNewTransaction();
        return embeddedConn.getTransaction().unwrap(RecordContextTransaction.class).getContext();
    }

    private static List<byte[]> exportData(boolean quoted, SchemaInfo schemaInfo) throws SQLException, RelationalException {
        return exportData(quoted, schemaInfo.databasePath, schemaInfo.connectionUtils);
    }

    private static List<byte[]> exportData(boolean quoted, String path, ConnectionUtils connectionUtils) throws SQLException, RelationalException {
        return connectionUtils.getFromCatalog(conn -> {
            try (RelationalStatement stmt = conn.createStatement();
                     RelationalResultSet rs = stmt.executeQuery("COPY " + maybeQuote(path, quoted) + " PRESERVE INCARNATION")) {
                List<byte[]> exportedData = new ArrayList<>();
                while (rs.next()) {
                    exportedData.add(rs.getBytes(1));
                }
                return exportedData;
            }
        });
    }

    private static int importDatabase(final boolean namedAndQuoted, final boolean autoCommit, final SchemaInfo dest,
                                      final List<byte[]> exportedData) throws SQLException, RelationalException {
        return importDatabase(namedAndQuoted, autoCommit, exportedData, dest.connectionUtils, dest.databasePath);
    }

    private static int importDatabase(final boolean namedAndQuoted, final boolean autoCommit,
                                      final List<byte[]> exportedData, ConnectionUtils targetConnectionUtils, String path) throws SQLException, RelationalException {
        return targetConnectionUtils.getFromCatalog(conn -> {
            conn.setAutoCommit(autoCommit);
            int resultingCount;
            try (RelationalPreparedStatement stmt = conn.prepareStatement("COPY " + maybeQuote(path, namedAndQuoted) + " FROM " + (namedAndQuoted ? "?data" : "?"))) {
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
                assertNotNull(relationalResultSet.getContinuation());
                assertTrue(relationalResultSet.getContinuation().atEnd());
            }
            if (!autoCommit) {
                conn.commit();
            }
            return resultingCount;
        });
    }

    private static class Moveable {
        final SchemaInfo source;
        final SchemaInfo dest;
        final String templateName;
        final String tableName;
        final List<Pair<Integer, String>> data;

        private Moveable(final SchemaInfo source, final SchemaInfo dest,
                         final String templateName, final String tableName,
                         final List<Pair<Integer, String>> data) {
            this.source = source;
            this.dest = dest;
            this.templateName = templateName;
            this.tableName = tableName;
            this.data = data;
        }
    }

    private static class SchemaInfo {
        final String databasePath;
        final String schemaName;
        private final ConnectionUtils connectionUtils;
        final String schemaPath;

        SchemaInfo(String databasePath, String schemaName, final ConnectionUtils connectionUtils) {
            this.databasePath = databasePath;
            this.schemaName = schemaName;
            this.connectionUtils = connectionUtils;
            this.schemaPath = databasePath + "/" + schemaName;
        }
    }

    private static class CatalogTestSetup {
        final String uuidName;
        final SchemaInfo source;
        final SchemaInfo dest;
        final String templateName;

        CatalogTestSetup(boolean quoted, final ConnectionUtils connectionUtils) {
            uuidName = uuidForPath(quoted);
            source = new SchemaInfo("/TEST/SOURCE_DB_" + uuidName, "1", connectionUtils);
            dest = new SchemaInfo("/TEST/DEST_DB_" + uuidName, "1", connectionUtils);
            templateName = "TEMPLATE_" + uuidName;
        }
    }
}
