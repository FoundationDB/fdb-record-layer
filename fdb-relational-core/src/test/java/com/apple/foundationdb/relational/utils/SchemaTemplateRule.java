/*
 * SchemaTemplateRule.java
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

import com.apple.foundationdb.relational.api.ddl.DdlConnection;
import com.apple.foundationdb.relational.api.ddl.DdlStatement;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Collection;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Manages the lifecycle of a single SchemaTemplate within a unit test.
 */
public class SchemaTemplateRule implements BeforeEachCallback, AfterEachCallback, BeforeAllCallback, AfterAllCallback {
    private final String templateName;
    private final TypeCreator typeCreator;
    private final TypeCreator tableCreator;
    private final Supplier<DdlConnection> connSupplier;

    public SchemaTemplateRule(String templateName,
                              Collection<TableDefinition> tables,
                              Collection<TypeDefinition> types,
                              Supplier<DdlConnection> connSupplier) {
        this.templateName = templateName;
        this.typeCreator = new CreatorFromDefinition("STRUCT", types);
        this.tableCreator = new CreatorFromDefinition("TABLE", tables);
        this.connSupplier = connSupplier;
    }

    public SchemaTemplateRule(String templateName, String templateDefinition, Supplier<DdlConnection> connSupplier) {
        this.templateName = templateName;
        this.typeCreator = new CreatorFromString(templateDefinition);
        this.tableCreator = () -> "";
        this.connSupplier = connSupplier;
    }

    public String getTemplateName() {
        return templateName;
    }

    @Override
    public void afterAll(ExtensionContext context) throws Exception {
        tearDown();
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        tearDown();
    }

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        makeTemplate();
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        makeTemplate();
    }

    private void makeTemplate() throws Exception {
        StringBuilder createStatement = new StringBuilder("CREATE SCHEMA TEMPLATE ").append(templateName).append(" AS {");
        createStatement.append(typeCreator.getTypeDefinition());
        createStatement.append(tableCreator.getTypeDefinition());
        createStatement.append("}");

        try (DdlConnection conn = connSupplier.get();
                DdlStatement statement = conn.createStatement()) {
            statement.execute(createStatement.toString());
            conn.commit();
        }
    }

    private void tearDown() throws Exception {
        StringBuilder dropStatement = new StringBuilder("DROP SCHEMA TEMPLATE ").append(templateName);

        try (DdlConnection conn = connSupplier.get();
                DdlStatement statement = conn.createStatement()) {
            statement.execute(dropStatement.toString());
        }
    }

    private interface TypeCreator {
        String getTypeDefinition();
    }

    private static final class CreatorFromDefinition implements TypeCreator {
        private final String typeName;
        private final Collection<? extends TypeDefinition> typeDefinitions;

        private CreatorFromDefinition(String typeName, Collection<? extends TypeDefinition> typeDefinitions) {
            this.typeName = typeName;
            this.typeDefinitions = typeDefinitions;
        }

        @Override
        public String getTypeDefinition() {
            return typeDefinitions.stream().map(td -> "CREATE " + typeName + " " + td.getDdlDefinition()).collect(Collectors.joining(";")) + ";";
        }
    }

    private static final class CreatorFromString implements TypeCreator {
        private final String creationString;

        private CreatorFromString(String creationString) {
            this.creationString = creationString;
        }

        @Override
        public String getTypeDefinition() {
            return creationString;
        }
    }

}
