/*
 * SchemaRule.java
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

package com.apple.foundationdb.relational.utils;

import com.apple.foundationdb.relational.api.Options;

import com.apple.foundationdb.relational.recordlayer.RelationalExtension;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.annotation.Nonnull;
import java.net.URI;
import java.sql.SQLException;
import java.util.Objects;

public class SchemaRule implements BeforeEachCallback, AfterEachCallback {

    @Nonnull
    private final RelationalExtension extension;

    @Nonnull
    private final String schemaName;

    @Nonnull
    private final URI dbUri;

    @Nonnull
    private final String templateName;

    @Nonnull
    private final Options connectionOptions;

    public SchemaRule(@Nonnull final RelationalExtension extension,
                      @Nonnull final String schemaName,
                      @Nonnull final URI dbUri,
                      @Nonnull final String templateName,
                      @Nonnull final Options connectionOptions) {
        this.extension = extension;
        this.schemaName = schemaName;
        this.dbUri = dbUri;
        this.templateName = templateName;
        this.connectionOptions = connectionOptions;
    }

    @Override
    public void afterEach(ExtensionContext context) throws SQLException {
        tearDown();
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        setup();
    }

    @Nonnull
    public String getSchemaName() {
        return schemaName;
    }

    private void setup() throws SQLException {
        CatalogOperations.runDdl(
                Objects.requireNonNull(extension.getDriver(), "extension has no active driver"),
                connectionOptions,
                "CREATE SCHEMA \"" + dbUri.getPath() + "/" + schemaName + "\" WITH TEMPLATE \"" + templateName + "\"");
    }

    private void tearDown() throws SQLException {
        CatalogOperations.runDdl(
                Objects.requireNonNull(extension.getDriver(), "extension has no active driver"),
                connectionOptions,
                "DROP SCHEMA IF EXISTS \"" + dbUri.getPath() + "/" + schemaName + "\"");
    }
}
