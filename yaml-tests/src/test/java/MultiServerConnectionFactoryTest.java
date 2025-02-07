/*
 * MultiServerFactoryTest.java
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

import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.yamltests.MultiServerConnectionFactory;
import com.apple.foundationdb.relational.yamltests.SimpleYamlConnection;
import com.apple.foundationdb.relational.yamltests.YamlConnection;
import com.apple.foundationdb.relational.yamltests.YamlConnectionFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MultiServerConnectionFactoryTest {
    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void testDefaultPolicy(int initialConnection) throws SQLException {
        String version0 = "0.0.0.0";
        String version1 = "1.1.1.1";
        String defaultVersion = version0;
        MultiServerConnectionFactory classUnderTest = new MultiServerConnectionFactory(
                MultiServerConnectionFactory.ConnectionSelectionPolicy.DEFAULT,
                initialConnection,
                dummyConnectionFactory("Primary", version0),
                List.of(dummyConnectionFactory("Alternate", version1)));

        // It might make sense to just remove ConnectionSelectionPolicy.DEFAULT since it never uses any of the other
        // connections, you don't need the MultiServerConnectionFactory, or it might make sense to have it return
        // just the default for the set of versions
        assertEquals(Set.of("Primary", "Alternate"), classUnderTest.getVersionsUnderTest());

        var connection = classUnderTest.getNewConnection(URI.create("Blah"));
        assertConnection(connection, List.of("Primary"));
        assertStatement(connection.prepareStatement("SQL"), "Primary");
        assertStatement(connection.prepareStatement("SQL"), "Primary");

        connection = classUnderTest.getNewConnection(URI.create("Blah"));
        assertConnection(connection, List.of("Primary"));
        assertStatement(connection.prepareStatement("SQL"), "Primary");
        assertStatement(connection.prepareStatement("SQL"), "Primary");

        connection = classUnderTest.getNewConnection(URI.create("Blah"));
        assertConnection(connection, List.of("Primary"));
        assertStatement(connection.prepareStatement("SQL"), "Primary");
        assertStatement(connection.prepareStatement("SQL"), "Primary");
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void testAlternatePolicy(int initialConnection) throws SQLException {
        final String[] path = new String[] { "Primary", "Alternate" };
        final String initialConnectionName = path[initialConnection];
        final String otherConnectionName = path[(initialConnection + 1) % 2];
        final String version0 = "0.0.0.0";
        final String version1 = "1.1.1.1";
        String[] versions = {version0, version1};

        MultiServerConnectionFactory classUnderTest = new MultiServerConnectionFactory(
                MultiServerConnectionFactory.ConnectionSelectionPolicy.ALTERNATE,
                initialConnection,
                dummyConnectionFactory("Primary", version0),
                List.of(dummyConnectionFactory("Alternate", version1)));
        assertEquals(versions[initialConnection], classUnderTest.getQueryInitialVersion());

        // First run:
        // - Factory current connection: initial connection
        // - connection current connection: initial connection
        // - statement: initial connection (2 statements)
        var connection = classUnderTest.getNewConnection(URI.create("Blah"));
        assertConnection(connection, List.of(initialConnectionName, otherConnectionName));
        assertStatement(connection.prepareStatement("SQL"), initialConnectionName);

        // Second run:
        // - Factory current connection: alternate connection
        // - connection current connection: alternate connection
        // - statement: alternate connection (2 statements)
        connection = classUnderTest.getNewConnection(URI.create("Blah"));
        assertConnection(connection, List.of(otherConnectionName, initialConnectionName));
        assertStatement(connection.prepareStatement("SQL"), otherConnectionName);
        // next statement
        assertStatement(connection.prepareStatement("SQL"), initialConnectionName);

        // Third run:
        // - Factory current connection: initial connection
        // - connection current connection: initial connection
        // - statement: initial connection (1 statement)
        connection = classUnderTest.getNewConnection(URI.create("Blah"));
        assertConnection(connection, List.of(initialConnectionName, otherConnectionName));
        // just one statement for this connection
        assertStatement(connection.prepareStatement("SQL"), initialConnectionName);

        // Fourth run:
        // - Factory current connection: alternate connection
        // - connection current connection: alternate connection
        // - statement: alternate connection (3 statements)
        connection = classUnderTest.getNewConnection(URI.create("Blah"));
        assertConnection(connection, List.of(otherConnectionName, initialConnectionName));
        assertStatement(connection.prepareStatement("SQL"), otherConnectionName);
        // just one statement for this connection
        assertStatement(connection.prepareStatement("SQL"), initialConnectionName);
        assertStatement(connection.prepareStatement("SQL"), otherConnectionName);
    }

    @Test
    void testIllegalInitialConnection() {
        assertThrows(AssertionError.class, () -> new MultiServerConnectionFactory(
                MultiServerConnectionFactory.ConnectionSelectionPolicy.ALTERNATE,
                -1,
                dummyConnectionFactory("A", "0.0.0.0"),
                List.of(dummyConnectionFactory("B", "1.1.1.1"))));
        assertThrows(AssertionError.class, () -> new MultiServerConnectionFactory(
                MultiServerConnectionFactory.ConnectionSelectionPolicy.ALTERNATE,
                7,
                dummyConnectionFactory("A", "0.0.0.0"),
                List.of(dummyConnectionFactory("B", "1.1.1.1"))));
    }

    private void assertStatement(final RelationalPreparedStatement statement, final String query) throws SQLException {
        assertEquals("version=" + query, ((RelationalConnection)statement.getConnection()).getPath().getQuery());
    }

    private static void assertConnection(final YamlConnection connection, final List<String> expectedVersions) {
        assertEquals(expectedVersions, connection.getVersions());
    }

    YamlConnectionFactory dummyConnectionFactory(@Nonnull String version) {
        return new YamlConnectionFactory() {
            @Override
            public YamlConnection getNewConnection(@Nonnull URI connectPath) throws SQLException {
                // Add query string to connection so we can tell where it came from
                URI newPath = URI.create(connectPath + "?version=" + version);
                return new SimpleYamlConnection(dummyConnection(newPath), version);
            }

            @Override
            public Set<String> getVersionsUnderTest() {
                return Set.of(version);
            }
        };
    }

    @Nonnull
    private static RelationalConnection dummyConnection(@Nonnull URI connectPath) throws SQLException {
        final RelationalConnection connection = Mockito.mock(RelationalConnection.class);
        Mockito.when(connection.unwrap(RelationalConnection.class)).thenReturn(connection);
        Mockito.when(connection.getPath()).thenReturn(connectPath);
        final RelationalPreparedStatement statement = dummyPreparedStatement(connection);
        Mockito.when(connection.prepareStatement(Mockito.anyString())).thenReturn(statement);
        return connection;
    }

    private static RelationalPreparedStatement dummyPreparedStatement(final RelationalConnection connection) throws SQLException {
        final RelationalPreparedStatement statement = Mockito.mock(RelationalPreparedStatement.class);
        Mockito.when(statement.getConnection()).thenReturn(connection);
        return statement;
    }
}
