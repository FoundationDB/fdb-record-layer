/*
 * Ddl.java
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
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalDriver;
import com.apple.foundationdb.relational.recordlayer.RelationalExtension;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.util.Assert;

import org.junit.jupiter.api.extension.ExtensionContext;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public class Ddl implements AutoCloseable {
    @Nonnull
    private final SchemaTemplateRule templateRule;
    @Nonnull
    private final DatabaseRule databaseRule;
    @Nonnull
    private final SchemaRule schemaRule;
    @Nonnull
    private final RelationalExtension relationalExtension;
    @Nullable
    private final ExtensionContext extensionContext;
    @Nonnull
    private final RelationalConnection connection;

    public Ddl(@Nonnull final RelationalExtension relationalExtension,
               @Nonnull final URI dbPath,
               @Nonnull final String schemaName,
               @Nonnull final String templateDefinition,
               @Nonnull final Options options,
               @Nullable SchemaTemplateRule.SchemaTemplateOptions schemaTemplateOptions,
               @Nullable final ExtensionContext extensionContext) throws Exception {
        final String templateName = dbPath.getPath().substring(dbPath.getPath().lastIndexOf("/") + 1);

        this.relationalExtension = relationalExtension;
        this.templateRule = new SchemaTemplateRule(relationalExtension, templateName + "_TEMPLATE", options, schemaTemplateOptions, templateDefinition);
        this.databaseRule = new DatabaseRule(relationalExtension, dbPath, options);
        this.schemaRule = new SchemaRule(relationalExtension, schemaName, dbPath, templateRule.getSchemaTemplateName(), options);
        this.extensionContext = extensionContext;

        try {
            templateRule.beforeEach(extensionContext);
            try {
                databaseRule.beforeEach(extensionContext);
                try {
                    schemaRule.beforeEach(extensionContext);
                } catch (Exception e) {
                    try {
                        schemaRule.afterEach(extensionContext);
                    } catch (Exception ae) {
                        e.addSuppressed(ae);
                    }
                    throw e;
                }
            } catch (Exception e) {
                try {
                    databaseRule.afterEach(extensionContext);
                } catch (Exception ae) {
                    e.addSuppressed(ae);
                }
                throw e;
            }
        } catch (Exception e) {
            try {
                templateRule.afterEach(extensionContext);
            } catch (Exception ae) {
                e.addSuppressed(ae);
            }
            throw e;
        }

        final RelationalDriver driver = Objects.requireNonNull(relationalExtension.getDriver(),
                "RelationalExtension has no active driver — its @BeforeEach must have run first.");
        this.connection = driver
                .connect(URI.create("jdbc:embed://" + databaseRule.getDbUri().toString())).unwrap(RelationalConnection.class);
        Utils.setConnectionOptions(connection, options);
    }

    @Nonnull
    public RelationalConnection getConnection() {
        return connection;
    }

    @Nonnull
    public RelationalConnection setSchemaAndGetConnection() throws SQLException {
        this.connection.setSchema(schemaRule.getSchemaName());
        return getConnection();
    }

    @Nonnull
    public String getSchemaTemplateName() {
        return this.templateRule.getSchemaTemplateName();
    }

    @Override
    public void close() throws SQLException {
        connection.close();
        schemaRule.afterEach(extensionContext);
        databaseRule.afterEach(extensionContext);
        templateRule.afterEach(extensionContext);
    }

    public static final class Builder {

        @Nullable
        private URI database;

        @Nullable
        private String templateDefinition;

        @Nonnull
        private Options options;

        @Nullable
        private SchemaTemplateRule.SchemaTemplateOptions schemaTemplateOptions;

        @Nullable
        private String schemaName;

        @Nullable
        private RelationalExtension extension;

        @Nullable
        private ExtensionContext extensionContext;

        private Builder() {
            options = Options.none();
        }

        @Nonnull
        public Builder database(@Nonnull final URI dbName) throws URISyntaxException {
            database = dbName;
            return this;
        }

        /**
         * Sets the database path to a freshly-generated unique value. Use this when the test
         * doesn't care about the specific database name and just needs an isolated database for
         * its scope. Under parallel JUnit class execution, tests that hard-code the same path
         * (e.g. {@code /TEST/QT}) trip over each other's catalog state; this overload sidesteps
         * that by giving every {@link Ddl} instance a unique path.
         */
        @Nonnull
        public Builder database() {
            // 16 random hex chars = 64 bits of entropy — enough to avoid collisions across a
            // single test run and short enough to keep error messages readable. We deliberately
            // avoid a UUID to keep the path SQL-identifier-safe and human-scannable.
            final String uniqueSuffix = Long.toHexString(ThreadLocalRandom.current().nextLong())
                    + Long.toHexString(ThreadLocalRandom.current().nextLong());
            database = URI.create("/TEST/AUTO_" + uniqueSuffix);
            return this;
        }

        @Nonnull
        public Builder schemaTemplate(@Nonnull final String schemaTemplate) {
            this.templateDefinition = schemaTemplate;
            return this;
        }

        @Nonnull
        public Builder withOptions(@Nonnull final Options options) {
            this.options = options;
            return this;
        }

        @Nonnull
        public Builder withOption(@Nonnull final Options.Name name, Object value) {
            try {
                options = options.withOption(name, value);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
            return this;
        }

        @Nonnull
        public Builder schemaTemplateOptions(@Nullable SchemaTemplateRule.SchemaTemplateOptions options) {
            this.schemaTemplateOptions = options;
            return this;
        }

        @Nonnull
        public Builder schemaName(@Nonnull final String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        @Nonnull
        public Builder relationalExtension(@Nonnull final RelationalExtension extension) {
            this.extension = extension;
            return this;
        }

        @Nonnull
        public Builder extensionContext(@Nonnull final ExtensionContext extensionContext) {
            this.extensionContext = extensionContext;
            return this;
        }

        @Nonnull
        public Ddl build() throws Exception {
            Assert.notNull(database);
            Assert.notNull(templateDefinition);
            if (schemaName == null) {
                schemaName = "testSchema";
            }
            Assert.notNull(extension);
            return new Ddl(extension, database, schemaName, templateDefinition, options, schemaTemplateOptions, extensionContext);
        }
    }

    @Nonnull
    public static Builder builder() {
        return new Builder();
    }
}
