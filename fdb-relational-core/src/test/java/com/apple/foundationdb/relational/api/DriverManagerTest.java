/*
 * DriverManagerTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.api;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

class DriverManagerTest {
    RelationalDriver driver1 = Mockito.mock(RelationalDriver.class);
    RelationalDriver driver2 = Mockito.mock(RelationalDriver.class);

    RelationalConnection connection1 = Mockito.mock(RelationalConnection.class);
    RelationalConnection connection2 = Mockito.mock(RelationalConnection.class);

    @BeforeEach
    public void beforeEach() throws SQLException {
        // Cleanup old drivers, if any,
        final var oldDrivers = DriverManager.drivers().collect(Collectors.toList());
        for (final var driver: oldDrivers) {
            DriverManager.deregisterDriver(driver);
        }
        DriverManager.drivers().forEach(driver -> {
            try {
                DriverManager.deregisterDriver(driver);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
        Assertions.assertEquals(0, DriverManager.drivers().count());
        when(driver1.acceptsURL(ArgumentMatchers.startsWith("jdbc:embed:"))).thenReturn(true);
        when(driver1.connect(ArgumentMatchers.startsWith("jdbc:embed:"), any())).thenReturn(connection1);
        when(driver2.acceptsURL(ArgumentMatchers.startsWith("jdbc:relational:"))).thenReturn(true);
        when(driver2.connect(ArgumentMatchers.startsWith("jdbc:relational:"), any())).thenReturn(connection2);
    }

    @AfterEach
    public void afterEach() throws SQLException {
        DriverManager.deregisterDriver(driver1);
        DriverManager.deregisterDriver(driver2);
    }

    @Test
    public void simpleRegisterTest() throws SQLException {
        DriverManager.registerDriver(driver1);
        Assertions.assertEquals(DriverManager.getDriver("jdbc:embed:/blah/to"), driver1);
        Assertions.assertEquals(DriverManager.getConnection("jdbc:embed:/blah/to"), connection1);
        Assertions.assertThrows(SQLException.class, () -> DriverManager.getDriver("anything"));
        Assertions.assertThrows(SQLException.class, () -> DriverManager.getConnection("anything"));
    }

    @Test
    public void multipleRegisterTest() throws SQLException {
        DriverManager.registerDriver(driver1);
        DriverManager.registerDriver(driver2);
        Assertions.assertEquals(DriverManager.getDriver("jdbc:embed:/blah/to"), driver1);
        Assertions.assertEquals(DriverManager.getConnection("jdbc:embed:/blah/to"), connection1);
        Assertions.assertEquals(DriverManager.getDriver("jdbc:relational:/blah/to"), driver2);
        Assertions.assertEquals(DriverManager.getConnection("jdbc:relational:/blah/to"), connection2);
        Assertions.assertThrows(SQLException.class, () -> DriverManager.getDriver("anything"));
        Assertions.assertThrows(SQLException.class, () -> DriverManager.getConnection("anything"));
    }

    @Test
    public void registerThenDeregisterTest() throws SQLException {
        DriverManager.registerDriver(driver1);
        DriverManager.registerDriver(driver2);
        Assertions.assertEquals(DriverManager.getDriver("jdbc:embed:/blah/to"), driver1);
        Assertions.assertEquals(DriverManager.getConnection("jdbc:embed:/blah/to"), connection1);
        DriverManager.deregisterDriver(driver1);
        Assertions.assertThrows(SQLException.class, () -> DriverManager.getDriver("jdbc:embed:/blah/to"));
        Assertions.assertThrows(SQLException.class, () -> DriverManager.getConnection("jdbc:embed:/blah/to"));
        Assertions.assertEquals(DriverManager.getDriver("jdbc:relational:/blah/to"), driver2);
        Assertions.assertEquals(DriverManager.getConnection("jdbc:relational:/blah/to"), connection2);
    }

    @Test
    public void sameRegisterTwiceTest() throws SQLException {
        DriverManager.registerDriver(driver1);
        DriverManager.registerDriver(driver1);
        Assertions.assertEquals(1, DriverManager.drivers().count());
        DriverManager.registerDriver(driver2);
        Assertions.assertEquals(2, DriverManager.drivers().count());
        DriverManager.registerDriver(driver2);
        Assertions.assertEquals(2, DriverManager.drivers().count());
        DriverManager.deregisterDriver(driver1);
        DriverManager.deregisterDriver(driver2);
        Assertions.assertEquals(0, DriverManager.drivers().count());
    }
}
