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
import com.apple.foundationdb.relational.yamltests.SimpleYamlConnection;
import com.apple.foundationdb.relational.yamltests.YamlConnection;
import com.apple.foundationdb.relational.yamltests.YamlConnectionFactory;
import com.apple.foundationdb.relational.yamltests.connectionfactory.MultiServerConnectionFactory;
import com.apple.foundationdb.relational.yamltests.server.SemanticVersion;
import com.apple.foundationdb.test.FDBTestEnvironment;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MultiServerConnectionFactoryTest {
    private static final SemanticVersion PRIMARY_VERSION = SemanticVersion.parse("2.2.2.0");
    private static final SemanticVersion ALTERNATE_VERSION = SemanticVersion.parse("1.1.1.0");
    private static final String CLUSTER_FILE = FDBTestEnvironment.randomClusterFile();

    @ParameterizedTest
    @CsvSource({"0", "1"})
    void testDefaultPolicy(int initialConnection) throws SQLException {
        MultiServerConnectionFactory classUnderTest = new MultiServerConnectionFactory(
                MultiServerConnectionFactory.ConnectionSelectionPolicy.DEFAULT,
                initialConnection,
                dummyConnectionFactory(PRIMARY_VERSION),
                List.of(dummyConnectionFactory(ALTERNATE_VERSION)));

        // It might make sense to just remove ConnectionSelectionPolicy.DEFAULT since it never uses any of the other
        // connections, you don't need the MultiServerConnectionFactory, or it might make sense to have it return
        // just the default for the set of versions
        assertEquals(Set.of(PRIMARY_VERSION, ALTERNATE_VERSION), classUnderTest.getVersionsUnderTest());

        var connection = classUnderTest.getNewConnection(URI.create("Blah"));
        assertConnection(connection, PRIMARY_VERSION, List.of(PRIMARY_VERSION));
        assertStatement(connection.prepareStatement("SQL"), PRIMARY_VERSION);
        assertStatement(connection.prepareStatement("SQL"), PRIMARY_VERSION);

        connection = classUnderTest.getNewConnection(URI.create("Blah"));
        assertConnection(connection, PRIMARY_VERSION, List.of(PRIMARY_VERSION));
        assertStatement(connection.prepareStatement("SQL"), PRIMARY_VERSION);
        assertStatement(connection.prepareStatement("SQL"), PRIMARY_VERSION);

        connection = classUnderTest.getNewConnection(URI.create("Blah"));
        assertConnection(connection, PRIMARY_VERSION, List.of(PRIMARY_VERSION));
        assertStatement(connection.prepareStatement("SQL"), PRIMARY_VERSION);
        assertStatement(connection.prepareStatement("SQL"), PRIMARY_VERSION);
    }

    @ParameterizedTest
    @CsvSource({"0", "1"})
    void testAlternatePolicy(int initialConnection) throws SQLException {
        final SemanticVersion[] versions = new SemanticVersion[] { PRIMARY_VERSION, ALTERNATE_VERSION };
        final SemanticVersion initialConnectionVersion = versions[initialConnection];
        final SemanticVersion otherConnectionVersion = versions[(initialConnection + 1) % 2];

        MultiServerConnectionFactory classUnderTest = new MultiServerConnectionFactory(
                MultiServerConnectionFactory.ConnectionSelectionPolicy.ALTERNATE,
                initialConnection,
                dummyConnectionFactory(PRIMARY_VERSION),
                List.of(dummyConnectionFactory(ALTERNATE_VERSION)));

        // First run:
        // - Factory current connection: initial connection
        // - connection current connection: initial connection
        // - statement: initial connection (2 statements)
        var connection = classUnderTest.getNewConnection(URI.create("Blah"));
        assertConnection(connection, initialConnectionVersion, List.of(initialConnectionVersion, otherConnectionVersion));
        assertStatement(connection.prepareStatement("SQL"), initialConnectionVersion);
        // next statement
        assertStatement(connection.prepareStatement("SQL"), otherConnectionVersion);

        // Second run:
        // - Factory current connection: alternate connection
        // - connection current connection: alternate connection
        // - statement: alternate connection (2 statements)
        connection = classUnderTest.getNewConnection(URI.create("Blah"));
        assertConnection(connection, otherConnectionVersion, List.of(otherConnectionVersion, initialConnectionVersion));
        assertStatement(connection.prepareStatement("SQL"), otherConnectionVersion);
        // next statement
        assertStatement(connection.prepareStatement("SQL"), initialConnectionVersion);

        // Third run:
        // - Factory current connection: initial connection
        // - connection current connection: initial connection
        // - statement: initial connection (1 statement)
        connection = classUnderTest.getNewConnection(URI.create("Blah"));
        assertConnection(connection, initialConnectionVersion, List.of(initialConnectionVersion, otherConnectionVersion));
        // just one statement for this connection
        assertStatement(connection.prepareStatement("SQL"), initialConnectionVersion);

        // Fourth run:
        // - Factory current connection: alternate connection
        // - connection current connection: alternate connection
        // - statement: alternate connection (3 statements)
        connection = classUnderTest.getNewConnection(URI.create("Blah"));
        assertConnection(connection, otherConnectionVersion, List.of(otherConnectionVersion, initialConnectionVersion));
        assertStatement(connection.prepareStatement("SQL"), otherConnectionVersion);
        // next statements
        assertStatement(connection.prepareStatement("SQL"), initialConnectionVersion);
        assertStatement(connection.prepareStatement("SQL"), otherConnectionVersion);
    }

    @Test
    void testIllegalInitialConnection() {
        assertThrows(AssertionError.class, () -> new MultiServerConnectionFactory(
                MultiServerConnectionFactory.ConnectionSelectionPolicy.ALTERNATE,
                -1,
                dummyConnectionFactory(PRIMARY_VERSION),
                List.of(dummyConnectionFactory(ALTERNATE_VERSION))));
        assertThrows(AssertionError.class, () -> new MultiServerConnectionFactory(
                MultiServerConnectionFactory.ConnectionSelectionPolicy.ALTERNATE,
                7,
                dummyConnectionFactory(PRIMARY_VERSION),
                List.of(dummyConnectionFactory(ALTERNATE_VERSION))));
    }

    private void assertStatement(final RelationalPreparedStatement statement, final SemanticVersion version) throws SQLException {
        assertEquals("version=" + version, ((RelationalConnection)statement.getConnection()).getPath().getQuery());
    }

    private static void assertConnection(final YamlConnection connection, final SemanticVersion initialVersion, final List<SemanticVersion> expectedVersions) {
        assertEquals(initialVersion, connection.getInitialVersion());
        assertEquals(expectedVersions, connection.getVersions());
    }

    YamlConnectionFactory dummyConnectionFactory(@Nonnull SemanticVersion version) {
        return new YamlConnectionFactory() {
            @Override
            public YamlConnection getNewConnection(@Nonnull URI connectPath) throws SQLException {
                // Add query string to connection so we can tell where it came from
                URI newPath = URI.create(connectPath + "?version=" + version);
                return new SimpleYamlConnection(dummyConnection(newPath), version, CLUSTER_FILE);
            }

            @Override
            public Set<SemanticVersion> getVersionsUnderTest() {
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
