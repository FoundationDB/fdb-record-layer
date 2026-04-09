/*
 * JDBCParameterizedQueryComparisonTest.java
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

package com.apple.foundationdb.relational.jdbc;

import com.apple.foundationdb.relational.api.RelationalArray;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.recordlayer.RelationalKeyspaceProvider;
import com.apple.foundationdb.relational.server.InProcessRelationalServer;
import com.apple.foundationdb.test.FDBTestEnvironment;
import com.apple.test.ParameterizedTestUtils;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Verify that the JDBC (remote/gRPC) driver and the embedded driver produce identical behavior
 * for parameterized inserts and selects across all SQL types. This catches serialization/deserialization
 * bugs in the gRPC/protobuf layer.
 *
 * <p>The {@link InProcessRelationalServer} creates an {@code FRL} instance which registers both the
 * embedded driver (for {@code jdbc:embed:} URLs) and the gRPC service (for {@code jdbc:relational:}
 * URLs). This allows the test to use both connection types against the same backing database.</p>
 *
 * <p>The test is parameterized on the cartesian product of:
 * <ul>
 *   <li>{@link TypeTestCase} - the SQL type under test</li>
 *   <li>{@code useTypedSetter} - whether to use type-specific setters (e.g. {@code setLong}) or {@code setObject}</li>
 *   <li>{@code insertWithJdbc} - whether the insert goes through JDBC (gRPC) or the embedded driver</li>
 *   <li>{@code readWithJdbc} - whether the read-back goes through JDBC (gRPC) or the embedded driver</li>
 * </ul>
 */
public class JDBCParameterizedQueryComparisonTest {

    @Nonnull
    private static final String SYS_DB_PATH = "/" + RelationalKeyspaceProvider.SYS;
    @Nonnull
    private static final String SCHEMA_NAME = "test_schema";
    private static final long PRIMARY_KEY = 1;

    @Nullable
    private static InProcessRelationalServer server;
    @Nullable
    private static String serverName;

    @Nullable
    private String dbPath;
    @Nullable
    private String templateName;

    /**
     * Enum defining each SQL type test case. The parameterized test iterates over all values.
     * Subclasses override {@link #createValue}, {@link #setTyped}, {@link #readValue}, and
     * optionally {@link #assertEqual} to customize behavior per type.
     */
    enum TypeTestCase {
        BIGINT("bigint", "", "BIGINT", new Object[] {10L, 20L, 30L}) {
            @Nonnull
            @Override
            Object createValue(@Nonnull Connection conn) {
                return 42L;
            }

            @Override
            void setTyped(@Nonnull PreparedStatement statement, int parameterIndex, @Nonnull Object val) throws SQLException {
                statement.setLong(parameterIndex, (Long)val);
            }

            @Nonnull
            @Override
            Object readValue(@Nonnull ResultSet resultSet, int columnIndex) throws SQLException {
                return resultSet.getLong(columnIndex);
            }
        },
        INTEGER("integer", "", "INTEGER", new Object[] {10, 20, 30}) {
            @Nonnull
            @Override
            Object createValue(@Nonnull Connection conn) {
                return 42;
            }

            @Override
            void setTyped(@Nonnull PreparedStatement statement, int parameterIndex, @Nonnull Object val) throws SQLException {
                statement.setInt(parameterIndex, (Integer)val);
            }

            @Nonnull
            @Override
            Object readValue(@Nonnull ResultSet resultSet, int columnIndex) throws SQLException {
                return resultSet.getInt(columnIndex);
            }
        },
        DOUBLE("double", "", "DOUBLE", new Object[] {1.1, 2.2, 3.3}) {
            @Nonnull
            @Override
            Object createValue(@Nonnull Connection conn) {
                return 3.14159;
            }

            @Override
            void setTyped(@Nonnull PreparedStatement statement, int parameterIndex, @Nonnull Object val) throws SQLException {
                statement.setDouble(parameterIndex, (Double)val);
            }

            @Nonnull
            @Override
            Object readValue(@Nonnull ResultSet resultSet, int columnIndex) throws SQLException {
                return resultSet.getDouble(columnIndex);
            }
        },
        FLOAT("float", "", "FLOAT", new Object[] {1.1f, 2.2f, 3.3f}) {
            @Nonnull
            @Override
            Object createValue(@Nonnull Connection conn) {
                return 2.718f;
            }

            @Override
            void setTyped(@Nonnull PreparedStatement statement, int parameterIndex, @Nonnull Object val) throws SQLException {
                statement.setFloat(parameterIndex, (Float)val);
            }

            @Nonnull
            @Override
            Object readValue(@Nonnull ResultSet resultSet, int columnIndex) throws SQLException {
                return resultSet.getFloat(columnIndex);
            }

            @Override
            void assertEqual(@Nonnull String message, @Nonnull Object expected, @Nonnull Object actual) {
                Assertions.assertEquals(((Number)expected).floatValue(),
                        ((Number)actual).floatValue(), 0.001f, message);
            }
        },
        STRING("string", "", "STRING", new Object[] {"a", "b", "c"}) {
            @Nonnull
            @Override
            Object createValue(@Nonnull Connection conn) {
                return "hello world";
            }

            @Override
            void setTyped(@Nonnull PreparedStatement statement, int parameterIndex, @Nonnull Object val) throws SQLException {
                statement.setString(parameterIndex, (String)val);
            }

            @Nonnull
            @Override
            Object readValue(@Nonnull ResultSet resultSet, int columnIndex) throws SQLException {
                return resultSet.getString(columnIndex);
            }
        },
        BOOLEAN("boolean", "", "BOOLEAN", new Object[] {true, false, true}) {
            @Nonnull
            @Override
            Object createValue(@Nonnull Connection conn) {
                return true;
            }

            @Override
            void setTyped(@Nonnull PreparedStatement statement, int parameterIndex, @Nonnull Object val) throws SQLException {
                statement.setBoolean(parameterIndex, (Boolean)val);
            }

            @Nonnull
            @Override
            Object readValue(@Nonnull ResultSet resultSet, int columnIndex) throws SQLException {
                return resultSet.getBoolean(columnIndex);
            }
        },
        BYTES("bytes", "", "BINARY", new Object[] {new byte[] {1, 2, 3, 4, 5}, new byte[] {-1, 0, 1}}) {
            @Nonnull
            @Override
            Object createValue(@Nonnull Connection conn) {
                return new byte[] {1, 2, 3, 4, 5};
            }

            @Override
            void setTyped(@Nonnull PreparedStatement statement, int parameterIndex, @Nonnull Object val) throws SQLException {
                statement.setBytes(parameterIndex, (byte[])val);
            }

            @Nonnull
            @Override
            Object readValue(@Nonnull ResultSet resultSet, int columnIndex) throws SQLException {
                return resultSet.getBytes(columnIndex);
            }

            @Override
            void assertEqual(@Nonnull String message, @Nonnull Object expected, @Nonnull Object actual) {
                Assertions.assertArrayEquals((byte[])expected, (byte[])actual, message);
            }
        },
        INTEGER_ARRAY("integer array", "", null, null) {
            @Nullable
            @Override
            Object createValue(@Nonnull Connection conn) throws SQLException {
                return conn.createArrayOf("INTEGER", new Object[] {10, 20, 30});
            }

            @Override
            void setTyped(@Nonnull PreparedStatement statement, int parameterIndex, @Nonnull Object val) throws SQLException {
                statement.setArray(parameterIndex, (Array)val);
            }

            @Nonnull
            @Override
            Object readValue(@Nonnull ResultSet resultSet, int columnIndex) throws SQLException {
                return resultSet.getArray(columnIndex);
            }

            @Override
            void assertEqual(@Nonnull String message, @Nonnull Object expected, @Nonnull Object actual) {
                try {
                    List<Object> expectedElements = extractArrayElements(expected);
                    List<Object> actualElements = extractArrayElements(actual);
                    Assertions.assertEquals(expectedElements, actualElements, message);
                } catch (SQLException e) {
                    throw new AssertionError(message + ": failed to extract array", e);
                }
            }
        },
        STRUCT("MyStruct", "CREATE TYPE AS STRUCT MyStruct (f0 bigint, f1 string)", null, null) {
            @Nullable
            @Override
            Object createValue(@Nonnull Connection conn) throws SQLException {
                return conn.createStruct("MyStruct", new Object[] {100L, "test_value"});
            }

            @Override
            void setTyped(@Nonnull PreparedStatement statement, int parameterIndex, @Nonnull Object val) throws SQLException {
                statement.setObject(parameterIndex, val);
            }

            @Nonnull
            @Override
            Object readValue(@Nonnull ResultSet resultSet, int columnIndex) throws SQLException {
                RelationalResultSet rrs = resultSet.unwrap(RelationalResultSet.class);
                return rrs.getStruct(columnIndex);
            }

            @Override
            void assertEqual(@Nonnull String message, @Nonnull Object expected, @Nonnull Object actual) {
                try {
                    RelationalStruct expStruct = (RelationalStruct)expected;
                    RelationalStruct actStruct = (RelationalStruct)actual;
                    int expCount = expStruct.getMetaData().getColumnCount();
                    int actCount = actStruct.getMetaData().getColumnCount();
                    Assertions.assertEquals(expCount, actCount, message + " (field count)");
                    for (int i = 1; i <= expCount; i++) {
                        Assertions.assertEquals(expStruct.getObject(i), actStruct.getObject(i),
                                message + " (field " + i + ")");
                    }
                } catch (SQLException e) {
                    throw new AssertionError(message + ": failed to compare structs", e);
                }
            }
        };

        @Nonnull
        private final String columnDdl;
        @Nonnull
        private final String extraTypeDdl;
        /**
         * The SQL type name for use with {@code createArrayOf}, or {@code null} if this type cannot be an array
         * element.
         */
        @Nullable
        private final String arrayTypeName;
        /**
         * Sample values for creating an array of this type, or {@code null} if arrays are not supported.
         */
        @Nullable
        private final Object[] sampleArrayElements;

        TypeTestCase(@Nonnull String columnDdl, @Nonnull String extraTypeDdl,
                     @Nullable String arrayTypeName,
                     @Nullable Object[] sampleArrayElements) {
            this.columnDdl = columnDdl;
            this.extraTypeDdl = extraTypeDdl;
            this.arrayTypeName = arrayTypeName;
            this.sampleArrayElements = sampleArrayElements;
        }

        @Nullable
        abstract Object createValue(@Nonnull Connection conn) throws SQLException;

        abstract void setTyped(@Nonnull PreparedStatement statement, int parameterIndex, @Nonnull Object val) throws SQLException;

        @Nonnull
        abstract Object readValue(@Nonnull ResultSet resultSet, int columnIndex) throws SQLException;

        void assertEqual(@Nonnull String message, @Nonnull Object expected, @Nonnull Object actual) {
            Assertions.assertEquals(expected, actual, message);
        }

        /**
         * Extract elements from an array object for comparison. Uses {@code getResultSet()} on
         * {@link RelationalArray} (returned by result set reads). Falls back to {@code getArray()}
         * for plain {@link Array} instances (e.g. from {@code createArrayOf}) since
         * {@link JDBCArrayImpl#getResultSet()} throws {@code SQLFeatureNotSupportedException}.
         * See <a href="https://github.com/FoundationDB/fdb-record-layer/issues/3665">#3665</a>
         */
        @Nonnull
        static List<Object> extractArrayElements(@Nonnull Object arrayObj) throws SQLException {
            List<Object> elements = new ArrayList<>();
            if (arrayObj instanceof RelationalArray) {
                RelationalResultSet rs = ((RelationalArray)arrayObj).getResultSet();
                while (rs.next()) {
                    final Object obj = rs.getObject(2);
                    elements.add(obj); // column 2 is the value in ARRAY result sets
                }
            } else if (arrayObj instanceof Array) {
                Object[] arr = (Object[])((Array)arrayObj).getArray();
                Collections.addAll(elements, arr);
            } else {
                throw new SQLException("Unexpected array type: " + arrayObj.getClass());
            }
            return elements;
        }
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        // Start in-process gRPC server. This also registers an embedded driver via FRL.
        server = new InProcessRelationalServer(FDBTestEnvironment.randomClusterFile()).start();
        serverName = server.getServerName();

        // Load JDBC driver (via ServiceLoader)
        JDBCRelationalDriverTest.getDriver();
    }

    @AfterAll
    public static void afterAll() throws Exception {
        if (server != null) {
            server.close();
        }
    }

    @BeforeEach
    public void setUp() throws SQLException {
        String uuid = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
        dbPath = "/FRL/parameters_" + uuid;
        templateName = "template_" + uuid;

        try (RelationalConnection conn = getJdbcCatalogConnection()) {
            try (RelationalStatement stmt = conn.createStatement()) {
                stmt.executeUpdate("DROP DATABASE IF EXISTS \"" + dbPath + "\"");
                stmt.executeUpdate("CREATE DATABASE \"" + dbPath + "\"");
            }
        }
    }

    @AfterEach
    public void tearDown() {
        try (RelationalConnection conn = getJdbcCatalogConnection()) {
            try (RelationalStatement stmt = conn.createStatement()) {
                stmt.executeUpdate("DROP DATABASE \"" + dbPath + "\"");
                stmt.executeUpdate("DROP SCHEMA TEMPLATE IF EXISTS \"" + templateName + "\"");
            }
        } catch (Exception e) {
            // best-effort cleanup
        }
    }

    @Nonnull
    static Stream<Arguments> testCases() {
        return ParameterizedTestUtils.cartesianProduct(
                Arrays.stream(TypeTestCase.values()),
                ParameterizedTestUtils.booleans("typedSetter", "setObject"),
                ParameterizedTestUtils.booleans("insertJdbc", "insertEmbedded"),
                ParameterizedTestUtils.booleans("readJdbc", "readEmbedded"));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    void testParameterizedInsertAndSelect(@Nonnull TypeTestCase testCase, boolean useTypedSetter,
                                          boolean insertWithJdbc, boolean readWithJdbc) throws Exception {
        if (insertWithJdbc || readWithJdbc) {
            // JDBC does not support createStruct (always returns null), so inserting with jdbc is not supported.
            // See: https://github.com/FoundationDB/fdb-record-layer/issues/4064
            Assumptions.assumeFalse(testCase == TypeTestCase.STRUCT);
        }

        createSchema(testCase);

        try (Connection insertConn = getConnection(insertWithJdbc)) {
            Object value = testCase.createValue(insertConn);
            Assertions.assertNotNull(value);
            insert(testCase, insertConn, value, useTypedSetter);
        }

        try (Connection readConn = getConnection(readWithJdbc)) {
            Object expectedValue = testCase.createValue(readConn);
            Assertions.assertNotNull(expectedValue);
            readAndAssert(testCase, readConn, expectedValue,
                    testCase + " " + (useTypedSetter ? "typedSetter" : "setObject")
                            + " " + (insertWithJdbc ? "insertJdbc" : "insertEmbedded")
                            + " -> " + (readWithJdbc ? "readJdbc" : "readEmbedded"));
        }
    }

    @ParameterizedTest
    @MethodSource("testCases")
    void testArrayOfTypeInsertAndSelect(@Nonnull TypeTestCase testCase, boolean useTypedSetter,
                                        boolean insertWithJdbc, boolean readWithJdbc) throws Exception {
        Assumptions.assumeFalse(testCase == TypeTestCase.STRUCT,
                "createArrayOf does not support STRUCT in either implementation");
        Assumptions.assumeFalse(testCase == TypeTestCase.INTEGER_ARRAY,
                "array array is not supported by DDL");

        createArraySchema(testCase);

        try (Connection insertConn = getConnection(insertWithJdbc)) {
            Array arrayValue = insertConn.createArrayOf(testCase.arrayTypeName, testCase.sampleArrayElements);
            insertArray(insertConn, arrayValue, useTypedSetter);
        }

        try (Connection readConn = getConnection(readWithJdbc)) {
            Array expectedArray = readConn.createArrayOf(testCase.arrayTypeName, testCase.sampleArrayElements);
            readAndAssertArray(readConn, expectedArray,
                    testCase + "_array " + (useTypedSetter ? "typedSetter" : "setObject")
                            + " " + (insertWithJdbc ? "insertJdbc" : "insertEmbedded")
                            + " -> " + (readWithJdbc ? "readJdbc" : "readEmbedded"));
        }
    }

    private void createArraySchema(@Nonnull TypeTestCase testCase) throws SQLException {
        try (RelationalConnection conn = getJdbcCatalogConnection()) {
            try (RelationalStatement stmt = conn.createStatement()) {
                String createTemplate = "CREATE SCHEMA TEMPLATE \"" + templateName + "\" " +
                        testCase.extraTypeDdl + " " +
                        "CREATE TABLE test_table (pk bigint, val " + testCase.columnDdl + " array, PRIMARY KEY(pk))";
                stmt.executeUpdate(createTemplate);
                stmt.executeUpdate("CREATE SCHEMA \"" + dbPath + "/" + SCHEMA_NAME +
                        "\" WITH TEMPLATE \"" + templateName + "\"");
            }
        }
    }

    private void insertArray(@Nonnull Connection conn, @Nonnull Array value, boolean useTypedSetter) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO test_table (pk, val) VALUES (?, ?)")) {
            ps.setLong(1, PRIMARY_KEY);
            if (useTypedSetter) {
                ps.setArray(2, value);
            } else {
                ps.setObject(2, value);
            }
            ps.executeUpdate();
        }
    }

    private void readAndAssertArray(@Nonnull Connection conn, @Nonnull Array expectedArray, @Nonnull String description) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT val FROM test_table WHERE pk = ?")) {
            ps.setLong(1, PRIMARY_KEY);
            try (ResultSet rs = ps.executeQuery()) {
                Assertions.assertTrue(rs.next(), "Expected row for: " + description);
                Array actualArray = rs.getArray(1);
                List<Object> expectedElements = TypeTestCase.extractArrayElements(expectedArray);
                List<Object> actualElements = TypeTestCase.extractArrayElements(actualArray);
                if (expectedElements.stream().allMatch(obj -> obj instanceof byte[])) {
                    // If we have List<byte[]> assertEquals will fail unless they are the same, so convert to ByteString
                    // first so we have an actual equals method
                    Assertions.assertEquals(asListOfByteStrings(expectedElements), asListOfByteStrings(actualElements));
                } else {
                    Assertions.assertEquals(expectedElements, actualElements, description);
                }
            }
        }
    }

    @Nonnull
    private static List<ByteString> asListOfByteStrings(@Nonnull final List<Object> expectedElements) {
        return expectedElements.stream().map(obj -> ByteString.copyFrom((byte[])obj)).collect(Collectors.toList());
    }

    @Nonnull
    private static RelationalConnection getJdbcCatalogConnection() throws SQLException {
        String uri = "jdbc:relational://" + SYS_DB_PATH + "?schema=" + RelationalKeyspaceProvider.CATALOG
                + "&server=" + serverName;
        return DriverManager.getConnection(uri).unwrap(RelationalConnection.class);
    }

    @Nonnull
    private Connection getConnection(boolean useJdbc) throws SQLException {
        if (useJdbc) {
            String uri = "jdbc:relational://" + dbPath + "?schema=" + SCHEMA_NAME
                    + "&server=" + serverName;
            return DriverManager.getConnection(uri);
        } else {
            return DriverManager.getConnection("jdbc:embed:" + dbPath + "?schema=" + SCHEMA_NAME);
        }
    }

    private void createSchema(@Nonnull TypeTestCase testCase) throws SQLException {
        try (RelationalConnection conn = getJdbcCatalogConnection()) {
            try (RelationalStatement stmt = conn.createStatement()) {
                String createTemplate = "CREATE SCHEMA TEMPLATE \"" + templateName + "\" " +
                        testCase.extraTypeDdl + " " +
                        "CREATE TABLE test_table (pk bigint, val " + testCase.columnDdl + ", PRIMARY KEY(pk))";
                stmt.executeUpdate(createTemplate);
                stmt.executeUpdate("CREATE SCHEMA \"" + dbPath + "/" + SCHEMA_NAME +
                        "\" WITH TEMPLATE \"" + templateName + "\"");
            }
        }
    }

    private void insert(@Nonnull TypeTestCase testCase, @Nonnull Connection conn, @Nonnull Object value, boolean useTypedSetter) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO test_table (pk, val) VALUES (?, ?)")) {
            ps.setLong(1, PRIMARY_KEY);
            if (useTypedSetter) {
                testCase.setTyped(ps, 2, value);
            } else {
                ps.setObject(2, value);
            }
            ps.executeUpdate();
        }
    }

    private void readAndAssert(@Nonnull TypeTestCase testCase, @Nonnull Connection conn, @Nonnull Object expectedValue, @Nonnull String description) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT val FROM test_table WHERE pk = ?")) {
            ps.setLong(1, PRIMARY_KEY);
            try (ResultSet rs = ps.executeQuery()) {
                Assertions.assertTrue(rs.next(), "Expected row for: " + description);
                Object readValue = testCase.readValue(rs, 1);
                testCase.assertEqual(description, expectedValue, readValue);
            }
        }
    }

}
