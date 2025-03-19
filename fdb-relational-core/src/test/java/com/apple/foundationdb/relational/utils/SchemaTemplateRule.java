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

import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.RelationalExtension;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Manages the lifecycle of a single SchemaTemplate within a unit test.
 */
public class SchemaTemplateRule implements BeforeEachCallback, AfterEachCallback {
    private final RelationalExtension relationalExtension;
    private final String templateName;
    @Nullable
    private final SchemaTemplateOptions options;
    private final TypeCreator typeCreator;
    private final TypeCreator tableCreator;

    private SchemaTemplateRule(RelationalExtension relationalExtension,
                               String templateName,
                               @Nullable SchemaTemplateOptions options,
                               TypeCreator typeCreator,
                               TypeCreator tableCreator) {
        this.relationalExtension = relationalExtension;
        this.templateName = templateName;
        this.options = options;
        this.typeCreator = typeCreator;
        this.tableCreator = tableCreator;
    }

    public SchemaTemplateRule(EmbeddedRelationalExtension relationalExtension,
                              String templateName,
                              SchemaTemplateOptions options,
                              Collection<TableDefinition> tables,
                              Collection<TypeDefinition> types) {
        this(relationalExtension, templateName, options,
                new CreatorFromDefinition("TYPE AS STRUCT", types),
                new CreatorFromDefinition("TABLE", tables));
    }

    public SchemaTemplateRule(
            RelationalExtension relationalExtension,
            String templateName,
            @Nullable SchemaTemplateOptions options,
            String templateDefinition) {
        this(relationalExtension, templateName, options, new CreatorFromString(templateDefinition), () -> "");
    }

    public String getTemplateName() {
        return templateName;
    }

    @Override
    public void afterEach(ExtensionContext context) throws SQLException {
        final StringBuilder dropStatement = new StringBuilder("DROP SCHEMA TEMPLATE \"").append(templateName).append("\"");

        try (Connection connection = DriverManager.getConnection("jdbc:embed:/__SYS")) {
            connection.setSchema("CATALOG");
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate(dropStatement.toString());
            }
        }
    }

    @Override
    public void beforeEach(ExtensionContext context) throws SQLException {
        final StringBuilder dropStatement = new StringBuilder("DROP SCHEMA TEMPLATE IF EXISTS\"").append(templateName).append("\"");

        try (Connection connection = DriverManager.getConnection("jdbc:embed:/__SYS")) {
            connection.setSchema("CATALOG");
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate(dropStatement.toString());
            }
        }
        final StringBuilder createStatement = new StringBuilder("CREATE SCHEMA TEMPLATE \"").append(templateName).append("\" ");
        createStatement.append(typeCreator.getTypeDefinition());
        createStatement.append(tableCreator.getTypeDefinition());

        if (options != null) {
            createStatement.append(options.getOptionsString());
        }

        try (Connection connection = DriverManager.getConnection("jdbc:embed:/__SYS")) {
            connection.setSchema("CATALOG");
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate(createStatement.toString());
            }
        }
    }

    public static final class SchemaTemplateOptions {
        private final boolean enableLongRows;
        private final boolean intermingleTables;

        public SchemaTemplateOptions(boolean enableLongRows, boolean intermingleTables) {
            this.enableLongRows = enableLongRows;
            this.intermingleTables = intermingleTables;
        }

        public String getOptionsString() {
            return String.format(Locale.ROOT, " WITH OPTIONS(ENABLE_LONG_ROWS=%s, INTERMINGLE_TABLES=%s) ",
                    enableLongRows, intermingleTables);
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
            return typeDefinitions.stream().map(td -> "CREATE " + typeName + " " + td.getDdlDefinition()).collect(Collectors.joining(" "));
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
