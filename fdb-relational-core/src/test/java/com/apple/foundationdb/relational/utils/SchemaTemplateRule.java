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

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.RelationalExtension;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.net.URI;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Manages the lifecycle of a single SchemaTemplate within a unit test.
 */
public class SchemaTemplateRule implements BeforeEachCallback, AfterEachCallback {
    private final RelationalExtension relationalExtension;
    private final String templateName;
    private final TypeCreator typeCreator;
    private final TypeCreator tableCreator;

    public SchemaTemplateRule(EmbeddedRelationalExtension relationalExtension,
                              String templateName,
                              Collection<TableDefinition> tables,
                              Collection<TypeDefinition> types) {
        this.relationalExtension = relationalExtension;
        this.templateName = templateName;
        this.typeCreator = new CreatorFromDefinition("STRUCT", types);
        this.tableCreator = new CreatorFromDefinition("TABLE", tables);
    }

    public SchemaTemplateRule(
            RelationalExtension relationalExtension,
            String templateName,
            String templateDefinition) {
        this.relationalExtension = relationalExtension;
        this.templateName = templateName;
        this.typeCreator = new CreatorFromString(templateDefinition);
        this.tableCreator = () -> "";
    }

    public String getTemplateName() {
        return templateName;
    }

    @Override
    public void afterEach(ExtensionContext context) throws Exception {
        final StringBuilder dropStatement = new StringBuilder("DROP SCHEMA TEMPLATE '").append(templateName).append("'");

        try (Connection connection = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.NONE)) {
            connection.setSchema("catalog");
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate(dropStatement.toString());
            }
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        final StringBuilder createStatement = new StringBuilder("CREATE SCHEMA TEMPLATE '").append(templateName).append("' AS {");
        createStatement.append(typeCreator.getTypeDefinition());
        createStatement.append(tableCreator.getTypeDefinition());
        createStatement.append("}");

        try (Connection connection = Relational.connect(URI.create("jdbc:embed:/__SYS"), Options.NONE)) {
            connection.setSchema("catalog");
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate(createStatement.toString());
            }
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
