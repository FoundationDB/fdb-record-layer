/*
 * SimpleDatabaseRule.java
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

import com.apple.foundationdb.relational.api.EmbeddedRelationalEngine;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.net.URI;
import java.util.Collection;

import javax.annotation.Nonnull;

/**
 * A JUnit extension that automatically creates all the framework necessary for a unique database and schema.
 *
 * This creates a SchemaTemplate with the specified configuration, then creates a database with the specified name. Once
 * this is done, it automatically creates a schema called 'testSchema' which has the specified template.
 *
 * Use this whenever you want a SQL-style database for testing (i.e. you just want a single database with a single
 * schema format).
 */
public class SimpleDatabaseRule implements BeforeAllCallback, BeforeEachCallback, AfterEachCallback, AfterAllCallback {
    private final SchemaTemplateRule templateRule;
    private final DatabaseRule databaseRule;
    private final SchemaRule schemaRule;

    public SimpleDatabaseRule(@Nonnull EmbeddedRelationalEngine engine,
                              @Nonnull URI dbPath,
                              @Nonnull Collection<TableDefinition> tables,
                              @Nonnull Collection<TypeDefinition> types) {

        String schemaName = "testSchema";
        String templateName = dbPath.getPath().substring(dbPath.getPath().lastIndexOf("/") + 1);

        this.templateRule = new SchemaTemplateRule(templateName + "_TEMPLATE", tables, types, engine::getDdlConnection);
        this.databaseRule = new DatabaseRule(engine, dbPath);
        this.schemaRule = new SchemaRule(schemaName, dbPath, templateRule.getTemplateName(), engine);
    }

    public SimpleDatabaseRule(@Nonnull EmbeddedRelationalEngine engine,
                              @Nonnull URI dbPath,
                              @Nonnull String templateDefinition) {
        String schemaName = "testSchema";
        String templateName = dbPath.getPath().substring(dbPath.getPath().lastIndexOf("/") + 1);

        this.templateRule = new SchemaTemplateRule(templateName + "_TEMPLATE", templateDefinition, engine::getDdlConnection);
        this.databaseRule = new DatabaseRule(engine, dbPath);
        this.schemaRule = new SchemaRule(schemaName, dbPath, templateRule.getTemplateName(), engine);
    }

    public SimpleDatabaseRule(@Nonnull EmbeddedRelationalEngine engine,
                              @Nonnull Class<?> testClass,
                              @Nonnull String templateDefinition) {
        this(engine, URI.create("/" + testClass.getSimpleName()), templateDefinition);
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        schemaRule.afterAll(context);
        databaseRule.afterAll(context);
        templateRule.afterAll(context);
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        schemaRule.afterEach(context);
        databaseRule.afterEach(context);
        templateRule.afterEach(context);
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        templateRule.beforeAll(context);
        databaseRule.beforeAll(context);
        schemaRule.beforeAll(context);
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        templateRule.beforeEach(context);
        databaseRule.beforeEach(context);
        schemaRule.beforeEach(context);
    }

    public URI getDatabasePath() {
        return databaseRule.getDbUri();
    }

    public String getSchemaName() {
        return schemaRule.getSchemaName();
    }

    public URI getConnectionUri() {
        return URI.create("jdbc:embed://" + getDatabasePath().getPath());
    }
}
