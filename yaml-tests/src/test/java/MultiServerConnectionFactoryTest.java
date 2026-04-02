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
    private static final String CLUSTER_FILE = "cluster0";
    private static final String CLUSTER_FILE_1 = "cluster1";

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

        var connection = classUnderTest.getNewConnection(URI.create("Blah"), 0);
        assertConnection(connection, PRIMARY_VERSION, List.of(PRIMARY_VERSION));
        assertStatement(connection.prepareStatement("SQL"), PRIMARY_VERSION, CLUSTER_FILE);
        assertStatement(connection.prepareStatement("SQL"), PRIMARY_VERSION, CLUSTER_FILE);

        connection = classUnderTest.getNewConnection(URI.create("Blah"), 0);
        assertConnection(connection, PRIMARY_VERSION, List.of(PRIMARY_VERSION));
        assertStatement(connection.prepareStatement("SQL"), PRIMARY_VERSION, CLUSTER_FILE);
        assertStatement(connection.prepareStatement("SQL"), PRIMARY_VERSION, CLUSTER_FILE);

        connection = classUnderTest.getNewConnection(URI.create("Blah"), 0);
        assertConnection(connection, PRIMARY_VERSION, List.of(PRIMARY_VERSION));
        assertStatement(connection.prepareStatement("SQL"), PRIMARY_VERSION, CLUSTER_FILE);
        assertStatement(connection.prepareStatement("SQL"), PRIMARY_VERSION, CLUSTER_FILE);
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
        var connection = classUnderTest.getNewConnection(URI.create("Blah"), 0);
        assertConnection(connection, initialConnectionVersion, List.of(initialConnectionVersion, otherConnectionVersion));
        assertStatement(connection.prepareStatement("SQL"), initialConnectionVersion, CLUSTER_FILE);
        // next statement
        assertStatement(connection.prepareStatement("SQL"), otherConnectionVersion, CLUSTER_FILE);

        // Second run:
        // - Factory current connection: alternate connection
        // - connection current connection: alternate connection
        // - statement: alternate connection (2 statements)
        connection = classUnderTest.getNewConnection(URI.create("Blah"), 0);
        assertConnection(connection, otherConnectionVersion, List.of(otherConnectionVersion, initialConnectionVersion));
        assertStatement(connection.prepareStatement("SQL"), otherConnectionVersion, CLUSTER_FILE);
        // next statement
        assertStatement(connection.prepareStatement("SQL"), initialConnectionVersion, CLUSTER_FILE);

        // Third run:
        // - Factory current connection: initial connection
        // - connection current connection: initial connection
        // - statement: initial connection (1 statement)
        connection = classUnderTest.getNewConnection(URI.create("Blah"), 0);
        assertConnection(connection, initialConnectionVersion, List.of(initialConnectionVersion, otherConnectionVersion));
        // just one statement for this connection
        assertStatement(connection.prepareStatement("SQL"), initialConnectionVersion, CLUSTER_FILE);

        // Fourth run:
        // - Factory current connection: alternate connection
        // - connection current connection: alternate connection
        // - statement: alternate connection (3 statements)
        connection = classUnderTest.getNewConnection(URI.create("Blah"), 0);
        assertConnection(connection, otherConnectionVersion, List.of(otherConnectionVersion, initialConnectionVersion));
        assertStatement(connection.prepareStatement("SQL"), otherConnectionVersion, CLUSTER_FILE);
        // next statements
        assertStatement(connection.prepareStatement("SQL"), initialConnectionVersion, CLUSTER_FILE);
        assertStatement(connection.prepareStatement("SQL"), otherConnectionVersion, CLUSTER_FILE);
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

    @ParameterizedTest
    @CsvSource({"0", "1"})
    void testDefaultPolicyWithCluster(int initialConnection) throws SQLException {
        MultiServerConnectionFactory classUnderTest = new MultiServerConnectionFactory(
                MultiServerConnectionFactory.ConnectionSelectionPolicy.DEFAULT,
                initialConnection,
                dummyMultiClusterConnectionFactory(PRIMARY_VERSION, List.of(CLUSTER_FILE, CLUSTER_FILE_1)),
                List.of(dummyMultiClusterConnectionFactory(ALTERNATE_VERSION, List.of(CLUSTER_FILE, CLUSTER_FILE_1))));

        // Cluster 0 should use the default factory and return CLUSTER_FILE
        var connection = classUnderTest.getNewConnection(URI.create("Blah"), 0);
        assertConnection(connection, PRIMARY_VERSION, List.of(PRIMARY_VERSION));
        assertEquals(CLUSTER_FILE, connection.getClusterFile());
        assertStatement(connection.prepareStatement("SQL"), PRIMARY_VERSION, CLUSTER_FILE);

        // Cluster 1 should use the default factory and return CLUSTER_FILE_1
        connection = classUnderTest.getNewConnection(URI.create("Blah"), 1);
        assertConnection(connection, PRIMARY_VERSION, List.of(PRIMARY_VERSION));
        assertEquals(CLUSTER_FILE_1, connection.getClusterFile());
        assertStatement(connection.prepareStatement("SQL"), PRIMARY_VERSION, CLUSTER_FILE_1);
    }

    @ParameterizedTest
    @CsvSource({"0", "1"})
    void testAlternatePolicyWithCluster(int initialConnection) throws SQLException {
        final SemanticVersion[] versions = new SemanticVersion[] { PRIMARY_VERSION, ALTERNATE_VERSION };
        final SemanticVersion initialConnectionVersion = versions[initialConnection];
        final SemanticVersion otherConnectionVersion = versions[(initialConnection + 1) % 2];

        MultiServerConnectionFactory classUnderTest = new MultiServerConnectionFactory(
                MultiServerConnectionFactory.ConnectionSelectionPolicy.ALTERNATE,
                initialConnection,
                dummyMultiClusterConnectionFactory(PRIMARY_VERSION, List.of(CLUSTER_FILE, CLUSTER_FILE_1)),
                List.of(dummyMultiClusterConnectionFactory(ALTERNATE_VERSION, List.of(CLUSTER_FILE, CLUSTER_FILE_1))));

        // Cluster 0: alternation works and cluster file is CLUSTER_FILE
        var connection = classUnderTest.getNewConnection(URI.create("Blah"), 0);
        assertConnection(connection, initialConnectionVersion, List.of(initialConnectionVersion, otherConnectionVersion));
        assertEquals(CLUSTER_FILE, connection.getClusterFile());
        assertStatement(connection.prepareStatement("SQL"), initialConnectionVersion, CLUSTER_FILE);
        assertStatement(connection.prepareStatement("SQL"), otherConnectionVersion, CLUSTER_FILE);

        // Cluster 1: alternation works and cluster file is CLUSTER_FILE_1
        connection = classUnderTest.getNewConnection(URI.create("Blah"), 1);
        assertConnection(connection, otherConnectionVersion, List.of(otherConnectionVersion, initialConnectionVersion));
        assertEquals(CLUSTER_FILE_1, connection.getClusterFile());
        assertStatement(connection.prepareStatement("SQL"), otherConnectionVersion, CLUSTER_FILE_1);
        assertStatement(connection.prepareStatement("SQL"), initialConnectionVersion, CLUSTER_FILE_1);
    }

    private void assertStatement(final RelationalPreparedStatement statement, final SemanticVersion version, final String clusterFile) throws SQLException {
        assertEquals("version=" + version + "&cluster=" + clusterFile, ((RelationalConnection)statement.getConnection()).getPath().getQuery());
    }

    private static void assertConnection(final YamlConnection connection, final SemanticVersion initialVersion, final List<SemanticVersion> expectedVersions) {
        assertEquals(initialVersion, connection.getInitialVersion());
        assertEquals(expectedVersions, connection.getVersions());
    }

    YamlConnectionFactory dummyConnectionFactory(@Nonnull SemanticVersion version) {
        return dummyMultiClusterConnectionFactory(version, List.of(CLUSTER_FILE));
    }

    YamlConnectionFactory dummyMultiClusterConnectionFactory(@Nonnull SemanticVersion version, @Nonnull List<String> clusterFiles) {
        return new YamlConnectionFactory() {
            @Override
            public YamlConnection getNewConnection(@Nonnull URI connectPath, int clusterIndex) throws SQLException {
                if (clusterIndex < 0 || clusterIndex >= clusterFiles.size()) {
                    throw new SQLException("Cluster index " + clusterIndex + " not available (only " +
                            clusterFiles.size() + " clusters configured)");
                }
                URI newPath = URI.create(connectPath + "?version=" + version + "&cluster=" + clusterFiles.get(clusterIndex));
                return new SimpleYamlConnection(dummyConnection(newPath), version, clusterFiles.get(clusterIndex));
            }

            @Override
            public Set<SemanticVersion> getVersionsUnderTest() {
                return Set.of(version);
            }

            @Override
            public int getAvailableClusterCount() {
                return clusterFiles.size();
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
