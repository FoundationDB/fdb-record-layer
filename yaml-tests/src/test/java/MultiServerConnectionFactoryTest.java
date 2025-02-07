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
import com.apple.foundationdb.relational.yamltests.MultiServerConnectionFactory;
import com.apple.foundationdb.relational.yamltests.YamlRunner;
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
    @ParameterizedTest
    @CsvSource({"0", "1"})
    void testDefaultPolicy(int initialConnection) throws SQLException {
        MultiServerConnectionFactory classUnderTest = new MultiServerConnectionFactory(
                MultiServerConnectionFactory.ConnectionSelectionPolicy.DEFAULT,
                initialConnection,
                dummyConnectionFactory(),
                List.of(dummyConnectionFactory()));
        assertEquals(initialConnection, classUnderTest.getCurrentConnectionSelector());
        MultiServerConnectionFactory.MultiServerRelationalConnection connection = (MultiServerConnectionFactory.MultiServerRelationalConnection)classUnderTest.getNewConnection(URI.create("Blah"));
        assertEquals(0, connection.getCurrentConnectionSelector());
        assertEquals(initialConnection, classUnderTest.getCurrentConnectionSelector());
        connection = (MultiServerConnectionFactory.MultiServerRelationalConnection)classUnderTest.getNewConnection(URI.create("Blah"));
        assertEquals(0, connection.getCurrentConnectionSelector());
        assertEquals(initialConnection, classUnderTest.getCurrentConnectionSelector());
        connection = (MultiServerConnectionFactory.MultiServerRelationalConnection)classUnderTest.getNewConnection(URI.create("Blah"));
        assertEquals(0, connection.getCurrentConnectionSelector());
        assertEquals(initialConnection, classUnderTest.getCurrentConnectionSelector());
    }

    @ParameterizedTest
    @CsvSource({"0", "1"})
    void testAlternatePolicy(int initialConnection) throws SQLException {
        MultiServerConnectionFactory classUnderTest = new MultiServerConnectionFactory(
                MultiServerConnectionFactory.ConnectionSelectionPolicy.ALTERNATE,
                initialConnection,
                dummyConnectionFactory(),
                List.of(dummyConnectionFactory()));
        assertEquals(initialConnection, classUnderTest.getCurrentConnectionSelector());
        MultiServerConnectionFactory.MultiServerRelationalConnection connection = (MultiServerConnectionFactory.MultiServerRelationalConnection)classUnderTest.getNewConnection(URI.create("Blah"));
        assertEquals(initialConnection % 2, connection.getCurrentConnectionSelector());
        assertEquals((initialConnection + 1) % 2, classUnderTest.getCurrentConnectionSelector());
        connection = (MultiServerConnectionFactory.MultiServerRelationalConnection)classUnderTest.getNewConnection(URI.create("Blah"));
        assertEquals((initialConnection + 1) % 2, connection.getCurrentConnectionSelector());
        assertEquals(initialConnection % 2, classUnderTest.getCurrentConnectionSelector());
        connection = (MultiServerConnectionFactory.MultiServerRelationalConnection)classUnderTest.getNewConnection(URI.create("Blah"));
        assertEquals(initialConnection % 2, connection.getCurrentConnectionSelector());
        assertEquals((initialConnection + 1) % 2, classUnderTest.getCurrentConnectionSelector());
    }

    @Test
    void testIllegalInitialConnection() {
        assertThrows(AssertionError.class, () -> new MultiServerConnectionFactory(
                MultiServerConnectionFactory.ConnectionSelectionPolicy.ALTERNATE,
                -1,
                dummyConnectionFactory(),
                List.of(dummyConnectionFactory())));
        assertThrows(AssertionError.class, () -> new MultiServerConnectionFactory(
                MultiServerConnectionFactory.ConnectionSelectionPolicy.ALTERNATE,
                7,
                dummyConnectionFactory(),
                List.of(dummyConnectionFactory())));
    }

    YamlRunner.YamlConnectionFactory dummyConnectionFactory() {
        return new YamlRunner.YamlConnectionFactory() {
            @Override
            public RelationalConnection getNewConnection(@Nonnull URI connectPath) throws SQLException {
                final RelationalConnection connection = Mockito.mock(RelationalConnection.class);
                Mockito.when(connection.unwrap(RelationalConnection.class)).thenReturn(connection);
                return connection;
            }

            @Override
            public Set<String> getVersionsUnderTest() {
                return Set.of("0.0.0.0");
            }
        };
    }
}
