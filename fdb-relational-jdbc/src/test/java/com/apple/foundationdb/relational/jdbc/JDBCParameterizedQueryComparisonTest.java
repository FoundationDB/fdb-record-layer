/*
 * JDBCParameterizedQueryComparisonTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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
import com.apple.test.RandomizedTestUtils;
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
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Verify that the JDBC (remote/gRPC) driver and the embedded driver produce identical behavior
 * for parameterized inserts and selects across all SQL types. This catches serialization/deserialization
 * bugs in the gRPC/protobuf layer.
 *
 * <p>The {@link InProcessRelationalServer} creates an {@code FRL} instance which registers both the
 * embedded driver (for {@code jdbc:embed:} URLs) and the gRPC service (for {@code jdbc:relational:}
 * URLs). This allows the test to use both connection types against the same backing database.</p>
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
     * Subclasses override {@link #createValue}, {@link #setTyped}, {@link #getTyped}, and
     * optionally {@link #assertEquals} to customize behavior per type.
     */
    enum TypeTestCase {
        BIGINT("bigint", "", "BIGINT") {
            @Nonnull
            @Override
            Object createValue(@Nonnull Connection conn, @Nonnull Random random) {
                return random.nextLong();
            }

            @Override
            void setTyped(@Nonnull PreparedStatement statement, int parameterIndex, @Nonnull Object val) throws SQLException {
                statement.setLong(parameterIndex, (Long)val);
            }

            @Nonnull
            @Override
            Object getTyped(@Nonnull ResultSet resultSet, int columnIndex) throws SQLException {
                return resultSet.getLong(columnIndex);
            }
        },
        INTEGER("integer", "", "INTEGER") {
            @Nonnull
            @Override
            Object createValue(@Nonnull Connection conn, @Nonnull Random random) {
                return random.nextInt();
            }

            @Override
            void setTyped(@Nonnull PreparedStatement statement, int parameterIndex, @Nonnull Object val) throws SQLException {
                statement.setInt(parameterIndex, (Integer)val);
            }

            @Nonnull
            @Override
            Object getTyped(@Nonnull ResultSet resultSet, int columnIndex) throws SQLException {
                return resultSet.getInt(columnIndex);
            }
        },
        DOUBLE("double", "", "DOUBLE") {
            @Nonnull
            @Override
            Object createValue(@Nonnull Connection conn, @Nonnull Random random) {
                return random.nextDouble();
            }

            @Override
            void setTyped(@Nonnull PreparedStatement statement, int parameterIndex, @Nonnull Object val) throws SQLException {
                statement.setDouble(parameterIndex, (Double)val);
            }

            @Nonnull
            @Override
            Object getTyped(@Nonnull ResultSet resultSet, int columnIndex) throws SQLException {
                return resultSet.getDouble(columnIndex);
            }

            @Override
            void assertEquals(@Nonnull String message, @Nonnull Object expected, @Nonnull Object actual) {
                Assertions.assertEquals(((Number)expected).doubleValue(),
                        ((Number)actual).doubleValue(), 1e-10, message);
            }
        },
        FLOAT("float", "", "FLOAT") {
            @Nonnull
            @Override
            Object createValue(@Nonnull Connection conn, @Nonnull Random random) {
                return random.nextFloat();
            }

            @Override
            void setTyped(@Nonnull PreparedStatement statement, int parameterIndex, @Nonnull Object val) throws SQLException {
                statement.setFloat(parameterIndex, (Float)val);
            }

            @Nonnull
            @Override
            Object getTyped(@Nonnull ResultSet resultSet, int columnIndex) throws SQLException {
                return resultSet.getFloat(columnIndex);
            }

            @Override
            void assertEquals(@Nonnull String message, @Nonnull Object expected, @Nonnull Object actual) {
                Assertions.assertEquals(((Number)expected).floatValue(),
                        ((Number)actual).floatValue(), 0.001f, message);
            }
        },
        STRING("string", "", "STRING") {
            @Nonnull
            @Override
            Object createValue(@Nonnull Connection conn, @Nonnull Random random) {
                return randomAlphanumericString(random, random.nextInt(20) + 1);
            }

            @Override
            void setTyped(@Nonnull PreparedStatement statement, int parameterIndex, @Nonnull Object val) throws SQLException {
                statement.setString(parameterIndex, (String)val);
            }

            @Nonnull
            @Override
            Object getTyped(@Nonnull ResultSet resultSet, int columnIndex) throws SQLException {
                return resultSet.getString(columnIndex);
            }
        },
        BOOLEAN("boolean", "", "BOOLEAN") {
            @Nonnull
            @Override
            Object createValue(@Nonnull Connection conn, @Nonnull Random random) {
                return random.nextBoolean();
            }

            @Override
            void setTyped(@Nonnull PreparedStatement statement, int parameterIndex, @Nonnull Object val) throws SQLException {
                statement.setBoolean(parameterIndex, (Boolean)val);
            }

            @Nonnull
            @Override
            Object getTyped(@Nonnull ResultSet resultSet, int columnIndex) throws SQLException {
                return resultSet.getBoolean(columnIndex);
            }
        },
        BYTES("bytes", "", "BINARY") {
            @Nonnull
            @Override
            Object createValue(@Nonnull Connection conn, @Nonnull Random random) {
                byte[] bytes = new byte[random.nextInt(20) + 1];
                random.nextBytes(bytes);
                return bytes;
            }

            @Override
            void setTyped(@Nonnull PreparedStatement statement, int parameterIndex, @Nonnull Object val) throws SQLException {
                statement.setBytes(parameterIndex, (byte[])val);
            }

            @Nonnull
            @Override
            Object getTyped(@Nonnull ResultSet resultSet, int columnIndex) throws SQLException {
                return resultSet.getBytes(columnIndex);
            }

            @Override
            void assertEquals(@Nonnull String message, @Nonnull Object expected, @Nonnull Object actual) {
                Assertions.assertArrayEquals((byte[])expected, (byte[])actual, message);
            }
        },
        STRUCT("MyStruct", "CREATE TYPE AS STRUCT MyStruct (f0 bigint, f1 string)", null) {
            @Nonnull
            @Override
            Object createValue(@Nonnull Connection conn, @Nonnull Random random) throws SQLException {
                return conn.createStruct("MyStruct", new Object[] {random.nextLong(),
                        randomAlphanumericString(random, random.nextInt(20) + 1)});
            }

            @Override
            void setTyped(@Nonnull PreparedStatement statement, int parameterIndex, @Nonnull Object val) throws SQLException {
                statement.setObject(parameterIndex, val);
            }

            @Nonnull
            @Override
            Object getTyped(@Nonnull ResultSet resultSet, int columnIndex) throws SQLException {
                RelationalResultSet rrs = resultSet.unwrap(RelationalResultSet.class);
                return rrs.getStruct(columnIndex);
            }

            @Override
            void assertEquals(@Nonnull String message, @Nonnull Object expected, @Nonnull Object actual) {
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

        TypeTestCase(@Nonnull String columnDdl, @Nonnull String extraTypeDdl,
                     @Nullable String arrayTypeName) {
            this.columnDdl = columnDdl;
            this.extraTypeDdl = extraTypeDdl;
            this.arrayTypeName = arrayTypeName;
        }

        /**
         * Create a random value of this type, suitable for insertion into the test table.
         */
        @Nonnull
        abstract Object createValue(@Nonnull Connection conn, @Nonnull Random random) throws SQLException;

        abstract void setTyped(@Nonnull PreparedStatement statement, int parameterIndex, @Nonnull Object val) throws SQLException;

        @Nonnull
        abstract Object getTyped(@Nonnull ResultSet resultSet, int columnIndex) throws SQLException;

        void assertEquals(@Nonnull String message, @Nonnull Object expected, @Nonnull Object actual) {
            Assertions.assertEquals(expected, actual, message);
        }

        @Nonnull
        private static String randomAlphanumericString(@Nonnull Random random, int length) {
            char[] chars = new char[length];
            for (int i = 0; i < length; i++) {
                chars[i] = (char)('a' + random.nextInt(26));
            }
            return new String(chars);
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
                RandomizedTestUtils.randomSeeds(),
                Arrays.stream(TypeTestCase.values()),
                ParameterizedTestUtils.booleans("typedSetter", "setObject"),
                ParameterizedTestUtils.booleans("insertJdbc", "insertEmbedded"),
                ParameterizedTestUtils.booleans("readJdbc", "readEmbedded"));
    }

    @ParameterizedTest
    @MethodSource("testCases")
    void testParameterizedInsertAndSelect(long seed, @Nonnull TypeTestCase testCase, boolean useTypedSetter,
                                          boolean insertWithJdbc, boolean readWithJdbc) throws Exception {
        if (insertWithJdbc || readWithJdbc) {
            // JDBC does not support createStruct (always returns null), so inserting with jdbc is not supported.
            // See: https://github.com/FoundationDB/fdb-record-layer/issues/4064
            Assumptions.assumeFalse(testCase == TypeTestCase.STRUCT);
        }

        final Random random = new Random(seed);
        createSchema(testCase.columnDdl, testCase.extraTypeDdl);

        // Create the value on the insert connection, insert it, then verify the read-back
        // matches the original inserted value (not a freshly created one).
        final Object insertedValue;
        try (Connection insertConn = getConnection(insertWithJdbc)) {
            insertedValue = testCase.createValue(insertConn, random);
            Assertions.assertNotNull(insertedValue);
            insert(insertConn, insertedValue, useTypedSetter
                    ? (statement, value) -> testCase.setTyped(statement, 2, value)
                    : (statement, value) -> statement.setObject(2, value));
        }

        try (Connection readConn = getConnection(readWithJdbc)) {
            readAndAssert(readConn, insertedValue,
                    resultSet -> testCase.getTyped(resultSet, 1),
                    testCase::assertEquals);
        }
    }

    @ParameterizedTest
    @MethodSource("testCases")
    void testArrayOfTypeInsertAndSelect(long seed, @Nonnull TypeTestCase testCase, boolean useTypedSetter,
                                        boolean insertWithJdbc, boolean readWithJdbc) throws Exception {
        Assumptions.assumeFalse(testCase == TypeTestCase.STRUCT,
                "createArrayOf does not support STRUCT in either implementation");
        // "array array" is not supported in the DDL, if it is we should add test coverage of that

        final Random random = new Random(seed);
        createSchema(testCase.columnDdl + " array", testCase.extraTypeDdl);

        // Create the array on the insert connection, insert it, then verify the read-back
        // matches the original inserted array elements.
        final Array insertedArray;
        try (Connection insertConn = getConnection(insertWithJdbc)) {
            Object[] arrayValue = IntStream.range(0, random.nextInt(20) + 1)
                    .mapToObj(i -> {
                        try {
                            return testCase.createValue(insertConn, random);
                        } catch (SQLException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .toArray(Object[]::new);
            insertedArray = insertConn.createArrayOf(testCase.arrayTypeName, arrayValue);
            insert(insertConn, insertedArray, useTypedSetter
                    ? (statement, value) -> statement.setArray(2, (Array) value)
                    : (statement, value) -> statement.setObject(2, value));
        }

        try (Connection readConn = getConnection(readWithJdbc)) {
            readAndAssert(readConn, insertedArray,
                    resultSet -> resultSet.getArray(1),
                    this::assertArrayEquals);
        }
    }

    private void assertArrayEquals(@Nonnull String message, @Nonnull Object expected, @Nonnull Object actual) {
        try {
            List<Object> expectedElements = TypeTestCase.extractArrayElements(expected);
            List<Object> actualElements = TypeTestCase.extractArrayElements(actual);
            if (expectedElements.stream().allMatch(obj -> obj instanceof byte[])) {
                // byte[] uses identity equality, so convert to ByteString for comparison
                Assertions.assertEquals(asListOfByteStrings(expectedElements), asListOfByteStrings(actualElements), message);
            } else {
                Assertions.assertEquals(expectedElements, actualElements, message);
            }
        } catch (SQLException e) {
            throw new AssertionError(message + ": failed to extract array", e);
        }
    }

    @Nonnull
    private static List<ByteString> asListOfByteStrings(@Nonnull List<Object> elements) {
        return elements.stream().map(obj -> ByteString.copyFrom((byte[])obj)).collect(Collectors.toList());
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

    private void createSchema(@Nonnull String columnDdl, @Nonnull String extraTypeDdl) throws SQLException {
        try (RelationalConnection conn = getJdbcCatalogConnection()) {
            try (RelationalStatement stmt = conn.createStatement()) {
                String createTemplate = "CREATE SCHEMA TEMPLATE \"" + templateName + "\" " +
                        extraTypeDdl + " " +
                        "CREATE TABLE test_table (pk bigint, val " + columnDdl + ", PRIMARY KEY(pk))";
                stmt.executeUpdate(createTemplate);
                stmt.executeUpdate("CREATE SCHEMA \"" + dbPath + "/" + SCHEMA_NAME +
                        "\" WITH TEMPLATE \"" + templateName + "\"");
            }
        }
    }

    @FunctionalInterface
    interface ValueSetter {
        void set(@Nonnull PreparedStatement statement, @Nonnull Object value) throws SQLException;
    }

    @FunctionalInterface
    interface ValueReader {
        @Nonnull
        Object read(@Nonnull ResultSet resultSet) throws SQLException;
    }

    @FunctionalInterface
    interface EqualityAssertion {
        void assertEquals(@Nonnull String message, @Nonnull Object expected, @Nonnull Object actual);
    }

    private void insert(@Nonnull Connection conn, @Nonnull Object value,
                        @Nonnull ValueSetter setter) throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement(
                "INSERT INTO test_table (pk, val) VALUES (?, ?)")) {
            statement.setLong(1, PRIMARY_KEY);
            setter.set(statement, value);
            statement.executeUpdate();
        }
    }

    private void readAndAssert(@Nonnull Connection conn, @Nonnull Object expectedValue,
                               @Nonnull ValueReader reader, @Nonnull EqualityAssertion assertion) throws SQLException {
        try (PreparedStatement statement = conn.prepareStatement(
                "SELECT val FROM test_table WHERE pk = ?")) {
            statement.setLong(1, PRIMARY_KEY);
            try (ResultSet resultSet = statement.executeQuery()) {
                Assertions.assertTrue(resultSet.next(), "Expected row");
                Object readValue = reader.read(resultSet);
                assertion.assertEquals("read value should match expected", expectedValue, readValue);
            }
        }
    }

}
