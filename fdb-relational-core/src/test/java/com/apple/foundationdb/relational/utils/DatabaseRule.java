/*
 * DatabaseRule.java
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

package com.apple.foundationdb.relational.utils;

import com.apple.foundationdb.relational.recordlayer.RelationalExtension;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseRule implements BeforeEachCallback, BeforeAllCallback, AfterEachCallback, AfterAllCallback {
    private final RelationalExtension relationalExtension;
    private final URI databasePath;

    public DatabaseRule(RelationalExtension relationalExtension, URI databasePath) {
        this.relationalExtension = relationalExtension;
        this.databasePath = databasePath;
    }

    @Override
    public void afterAll(ExtensionContext context) throws SQLException {
        tearDown();
    }

    @Override
    public void afterEach(ExtensionContext context) throws SQLException {
        tearDown();
    }

    @Override
    public void beforeAll(ExtensionContext context) throws SQLException {
        setup();
    }

    @Override
    public void beforeEach(ExtensionContext context) throws SQLException {
        setup();
    }

    private void setup() throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:embed:/__SYS")) {
            connection.setSchema("CATALOG");
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("DROP DATABASE IF EXISTS \"" + databasePath.getPath() + "\"");
                statement.executeUpdate("CREATE DATABASE \"" + databasePath.getPath() + "\"");
            }
        }
    }

    private void tearDown() throws SQLException {
        try (Connection connection = DriverManager.getConnection("jdbc:embed:/__SYS")) {
            connection.setSchema("CATALOG");
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("DROP DATABASE \"" + databasePath.getPath() + "\"");
            }
        }
    }

    public URI getDbUri() {
        return databasePath;
    }
}
