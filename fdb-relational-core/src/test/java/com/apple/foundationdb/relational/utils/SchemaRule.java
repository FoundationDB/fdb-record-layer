/*
 * SchemaRule.java
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

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.recordlayer.RelationalExtension;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.net.URI;
import java.sql.Connection;
import java.sql.Statement;

public class SchemaRule implements BeforeEachCallback, AfterEachCallback {
    private final String schemaName;
    private final URI dbUri;
    private final String templateName;
    private final RelationalExtension relationalExtension;

    public SchemaRule(RelationalExtension relationalExtension, String schemaName, URI dbUri, String templateName) {
        this.relationalExtension = relationalExtension;
        this.schemaName = schemaName;
        this.dbUri = dbUri;
        this.templateName = templateName;
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        tearDown();
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        setup();
    }

    public String getSchemaName() {
        return schemaName;
    }

    private void setup() throws Exception {
        try (Connection connection = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.none())) {
            connection.setSchema("catalog");
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("CREATE SCHEMA '" + dbUri.getPath() + "/" + schemaName + "' WITH TEMPLATE '" + templateName + "'");
            }
        }
    }

    private void tearDown() throws Exception {
        try (Connection connection = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.none())) {
            connection.setSchema("catalog");
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("DROP SCHEMA '" + dbUri.getPath() + "/" + schemaName + "'");
            }
        }
    }

}
