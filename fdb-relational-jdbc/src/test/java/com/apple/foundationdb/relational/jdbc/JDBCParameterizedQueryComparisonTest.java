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

    private static final String SYS_DB_PATH = "/" + RelationalKeyspaceProvider.SYS;
    private static final String SCHEMA_NAME = "test_schema";
    private static final long PRIMARY_KEY = 1;

    private static InProcessRelationalServer server;
    private static String serverName;

    private String dbPath;
    private String templateName;

    /**
     * Enum defining each SQL type test case. The parameterized test iterates over all values.
     * Subclasses override {@link #createValue}, {@link #setTyped}, {@link #readValue}, and
     * optionally {@link #assertEqual} to customize behavior per type.
     */
    enum TypeTestCase {
        BIGINT("bigint", null, true, "BIGINT", true, new Object[]{10L, 20L, 30L}) {
            @Override
            Object createValue(Connection conn) {
                return 42L;
            }

            @Override
            void setTyped(PreparedStatement statement, int parameterIndex, Object val) throws SQLException {
                statement.setLong(parameterIndex, (Long) val);
            }

            @Override
            Object readValue(ResultSet resultSet, int columnIndex) throws SQLException {
                return resultSet.getLong(columnIndex);
            }
        },
        INTEGER("integer", null, true, "INTEGER", true, new Object[]{10, 20, 30}) {
            @Override
            Object createValue(Connection conn) {
                return 42;
            }

            @Override
            void setTyped(PreparedStatement statement, int parameterIndex, Object val) throws SQLException {
                statement.setInt(parameterIndex, (Integer) val);
            }

            @Override
            Object readValue(ResultSet resultSet, int columnIndex) throws SQLException {
                return resultSet.getInt(columnIndex);
            }
        },
        DOUBLE("double", null, true, "DOUBLE", true, new Object[]{1.1, 2.2, 3.3}) {
            @Override
            Object createValue(Connection conn) {
                return 3.14159;
            }

            @Override
            void setTyped(PreparedStatement statement, int parameterIndex, Object val) throws SQLException {
                statement.setDouble(parameterIndex, (Double) val);
            }

            @Override
            Object readValue(ResultSet resultSet, int columnIndex) throws SQLException {
                return resultSet.getDouble(columnIndex);
            }
        },
        FLOAT("float", null, true, "FLOAT", false, new Object[]{1.1f, 2.2f, 3.3f}) {
            @Override
            Object createValue(Connection conn) {
                return 2.718f;
            }

            @Override
            void setTyped(PreparedStatement statement, int parameterIndex, Object val) throws SQLException {
                statement.setFloat(parameterIndex, (Float) val);
            }

            @Override
            Object readValue(ResultSet resultSet, int columnIndex) throws SQLException {
                return resultSet.getFloat(columnIndex);
            }

            @Override
            void assertEqual(String message, Object expected, Object actual) {
                Assertions.assertEquals(((Number) expected).floatValue(),
                        ((Number) actual).floatValue(), 0.001f, message);
            }
        },
        STRING("string", null, true, "STRING", true, new Object[]{"a", "b", "c"}) {
            @Override
            Object createValue(Connection conn) {
                return "hello world";
            }

            @Override
            void setTyped(PreparedStatement statement, int parameterIndex, Object val) throws SQLException {
                statement.setString(parameterIndex, (String) val);
            }

            @Override
            Object readValue(ResultSet resultSet, int columnIndex) throws SQLException {
                return resultSet.getString(columnIndex);
            }
        },
        BOOLEAN("boolean", null, true, "BOOLEAN", true, new Object[]{true, false, true}) {
            @Override
            Object createValue(Connection conn) {
                return true;
            }

            @Override
            void setTyped(PreparedStatement statement, int parameterIndex, Object val) throws SQLException {
                statement.setBoolean(parameterIndex, (Boolean) val);
            }

            @Override
            Object readValue(ResultSet resultSet, int columnIndex) throws SQLException {
                return resultSet.getBoolean(columnIndex);
            }
        },
        BYTES("bytes", null, true, "BINARY", false, new Object[] {new byte[]{1, 2, 3, 4, 5}, new byte[]{-1, 0, 1}}) {
            @Override
            Object createValue(Connection conn) {
                return new byte[]{1, 2, 3, 4, 5};
            }

            @Override
            void setTyped(PreparedStatement statement, int parameterIndex, Object val) throws SQLException {
                statement.setBytes(parameterIndex, (byte[]) val);
            }

            @Override
            Object readValue(ResultSet resultSet, int columnIndex) throws SQLException {
                return resultSet.getBytes(columnIndex);
            }

            @Override
            void assertEqual(String message, Object expected, Object actual) {
                Assertions.assertArrayEquals((byte[]) expected, (byte[]) actual, message);
            }
        },
        INTEGER_ARRAY("integer array", null, true, null, false, null) {
            @Override
            Object createValue(Connection conn) throws SQLException {
                return conn.createArrayOf("INTEGER", new Object[]{10, 20, 30});
            }

            @Override
            void setTyped(PreparedStatement statement, int parameterIndex, Object val) throws SQLException {
                statement.setArray(parameterIndex, (Array) val);
            }

            @Override
            Object readValue(ResultSet resultSet, int columnIndex) throws SQLException {
                return resultSet.getArray(columnIndex);
            }

            @Override
            void assertEqual(String message, Object expected, Object actual) {
                try {
                    List<Object> expectedElements = extractArrayElements(expected);
                    List<Object> actualElements = extractArrayElements(actual);
                    Assertions.assertEquals(expectedElements, actualElements, message);
                } catch (SQLException e) {
                    throw new AssertionError(message + ": failed to extract array", e);
                }
            }
        },
        STRUCT("MyStruct", "CREATE TYPE AS STRUCT MyStruct (f0 bigint, f1 string)", false, null, false, null) {
            @Override
            Object createValue(Connection conn) throws SQLException {
                return conn.createStruct("MyStruct", new Object[]{100L, "test_value"});
            }

            @Override
            void setTyped(PreparedStatement statement, int parameterIndex, Object val) throws SQLException {
                statement.setObject(parameterIndex, val);
            }

            @Override
            Object readValue(ResultSet resultSet, int columnIndex) throws SQLException {
                RelationalResultSet rrs = resultSet.unwrap(RelationalResultSet.class);
                return rrs.getStruct(columnIndex);
            }

            @Override
            void assertEqual(String message, Object expected, Object actual) {
                try {
                    RelationalStruct expStruct = (RelationalStruct) expected;
                    RelationalStruct actStruct = (RelationalStruct) actual;
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

        private final String columnDdl;
        @Nullable
        private final String extraTypeDdl;
        private final boolean jdbcSetterSupported;
        /** The SQL type name for use with {@code createArrayOf}, or {@code null} if this type cannot be an array element. */
        @Nullable
        private final String arrayTypeName;
        /**
         * Whether JDBC {@code createArrayOf} supports this type. {@code false} when {@code TypeConversion.toColumn}
         * does not handle the JDBC type code (e.g. FLOAT, BINARY).
         */
        private final boolean jdbcArraySupported;
        /** Sample values for creating an array of this type, or {@code null} if arrays are not supported. */
        @Nullable
        private final Object[] sampleArrayElements;

        TypeTestCase(String columnDdl, @Nullable String extraTypeDdl, boolean jdbcSetterSupported,
                     @Nullable String arrayTypeName, boolean jdbcArraySupported,
                     @Nullable Object[] sampleArrayElements) {
            this.columnDdl = columnDdl;
            this.extraTypeDdl = extraTypeDdl;
            this.jdbcSetterSupported = jdbcSetterSupported;
            this.arrayTypeName = arrayTypeName;
            this.jdbcArraySupported = jdbcArraySupported;
            this.sampleArrayElements = sampleArrayElements;
        }

        abstract Object createValue(Connection conn) throws SQLException;

        abstract void setTyped(PreparedStatement statement, int parameterIndex, Object val) throws SQLException;

        abstract Object readValue(ResultSet resultSet, int columnIndex) throws SQLException;

        void assertEqual(String message, Object expected, Object actual) {
            Assertions.assertEquals(expected, actual, message);
        }

        /**
         * Extract elements from an array object for comparison. Uses {@code getResultSet()} on
         * {@link RelationalArray} (returned by result set reads). Falls back to {@code getArray()}
         * for plain {@link Array} instances (e.g. from {@code createArrayOf}) since
         * {@link JDBCArrayImpl#getResultSet()} throws {@code SQLFeatureNotSupportedException}.
         * See <a href="https://github.com/FoundationDB/fdb-record-layer/issues/3665">#3665</a>
         */
        static List<Object> extractArrayElements(Object arrayObj) throws SQLException {
            List<Object> elements = new ArrayList<>();
            if (arrayObj instanceof RelationalArray) {
                RelationalResultSet rs = ((RelationalArray) arrayObj).getResultSet();
                while (rs.next()) {
                    final Object obj = rs.getObject(2);
                    elements.add(obj); // column 2 is the value in ARRAY result sets
                }
            } else if (arrayObj instanceof Array) {
                Object[] arr = (Object[]) ((Array) arrayObj).getArray();
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

    static Stream<Arguments> testCases() {
        return ParameterizedTestUtils.cartesianProduct(
                Arrays.stream(TypeTestCase.values()),
                ParameterizedTestUtils.booleans("typedSetter", "setObject"),
                ParameterizedTestUtils.booleans("insertJdbc", "insertEmbedded"),
                ParameterizedTestUtils.booleans("readJdbc", "readEmbedded"));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    void testParameterizedInsertAndSelect(TypeTestCase testCase, boolean useTypedSetter,
                                          boolean insertWithJdbc, boolean readWithJdbc) throws Exception {
        if (insertWithJdbc || readWithJdbc) {
            // JDBC does not support createStruct (always returns null), so inserting with jdbc is not supported.
            // See: https://github.com/FoundationDB/fdb-record-layer/issues/4064
            Assumptions.assumeFalse(testCase == TypeTestCase.STRUCT);
        }

        createSchema(testCase);

        try (Connection insertConn = getConnection(insertWithJdbc)) {
            Object value = testCase.createValue(insertConn);
            insert(testCase, insertConn, value, useTypedSetter);
        }

        try (Connection readConn = getConnection(readWithJdbc)) {
            Object expectedValue = testCase.createValue(readConn);
            readAndAssert(testCase, readConn, expectedValue,
                    testCase + " " + (useTypedSetter ? "typedSetter" : "setObject")
                            + " " + (insertWithJdbc ? "insertJdbc" : "insertEmbedded")
                            + " -> " + (readWithJdbc ? "readJdbc" : "readEmbedded"));
        }
    }

    @ParameterizedTest
    @MethodSource("testCases")
    void testArrayOfTypeInsertAndSelect(TypeTestCase testCase, boolean useTypedSetter,
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

    private void createArraySchema(TypeTestCase testCase) throws SQLException {
        try (RelationalConnection conn = getJdbcCatalogConnection()) {
            try (RelationalStatement stmt = conn.createStatement()) {
                String createTemplate = "CREATE SCHEMA TEMPLATE \"" + templateName + "\" " +
                        (testCase.extraTypeDdl != null ? testCase.extraTypeDdl + " " : "") +
                        "CREATE TABLE test_table (pk bigint, val " + testCase.columnDdl + " array, PRIMARY KEY(pk))";
                stmt.executeUpdate(createTemplate);
                stmt.executeUpdate("CREATE SCHEMA \"" + dbPath + "/" + SCHEMA_NAME +
                        "\" WITH TEMPLATE \"" + templateName + "\"");
            }
        }
    }

    private void insertArray(Connection conn, Array value, boolean useTypedSetter) throws SQLException {
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

    private void readAndAssertArray(Connection conn, Array expectedArray, String description) throws SQLException {
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
    private static List<ByteString> asListOfByteStrings(final List<Object> expectedElements) {
        return expectedElements.stream().map(obj -> ByteString.copyFrom((byte[])obj)).collect(Collectors.toList());
    }

    private static RelationalConnection getJdbcCatalogConnection() throws SQLException {
        String uri = "jdbc:relational://" + SYS_DB_PATH + "?schema=" + RelationalKeyspaceProvider.CATALOG
                + "&server=" + serverName;
        return DriverManager.getConnection(uri).unwrap(RelationalConnection.class);
    }

    private Connection getConnection(boolean useJdbc) throws SQLException {
        if (useJdbc) {
            String uri = "jdbc:relational://" + dbPath + "?schema=" + SCHEMA_NAME
                    + "&server=" + serverName;
            return DriverManager.getConnection(uri);
        } else {
            return DriverManager.getConnection("jdbc:embed:" + dbPath + "?schema=" + SCHEMA_NAME);
        }
    }

    private void createSchema(TypeTestCase testCase) throws SQLException {
        try (RelationalConnection conn = getJdbcCatalogConnection()) {
            try (RelationalStatement stmt = conn.createStatement()) {
                String createTemplate = "CREATE SCHEMA TEMPLATE \"" + templateName + "\" " +
                        (testCase.extraTypeDdl != null ? testCase.extraTypeDdl + " " : "") +
                        "CREATE TABLE test_table (pk bigint, val " + testCase.columnDdl + ", PRIMARY KEY(pk))";
                stmt.executeUpdate(createTemplate);
                stmt.executeUpdate("CREATE SCHEMA \"" + dbPath + "/" + SCHEMA_NAME +
                        "\" WITH TEMPLATE \"" + templateName + "\"");
            }
        }
    }

    private void insert(TypeTestCase testCase, Connection conn, Object value, boolean useTypedSetter) throws SQLException {
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

    private void readAndAssert(TypeTestCase testCase, Connection conn, Object expectedValue, String description) throws SQLException {
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
