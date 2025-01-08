/*
 * SchemaDescription.java
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

package com.apple.foundationdb.relational.autotest;

import com.apple.foundationdb.annotation.API;

import java.util.List;

@API(API.Status.EXPERIMENTAL)
public class SchemaDescription {
    private final String templateName;
    private final String templateDescription;
    private final String schemaName;
    private final String displayName;

    private final List<TableDescription> tables;

    public SchemaDescription(String templateName,
                             String templateDescription,
                             String schemaName,
                             String displayName,
                             List<TableDescription> tables) {
        this.templateName = templateName;
        this.templateDescription = templateDescription;
        this.schemaName = schemaName;
        this.displayName = displayName;
        this.tables = tables;
    }

    public List<TableDescription> getTables() {
        return tables;
    }

    public String getTemplateName() {
        return templateName;
    }

    public String getTemplateDescription() {
        return templateDescription;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getDisplayName() {
        return displayName;
    }

    @Override
    public String toString() {
        return "CREATE SCHEMA TEMPLATE " + templateName + " " +
                templateDescription;
    }
}
