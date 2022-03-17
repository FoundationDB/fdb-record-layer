/*
 * SchemaDataImpl.java
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

package com.apple.foundationdb.relational.api.catalog;

import com.apple.foundationdb.relational.api.CatalogData;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SchemaDataImpl implements SchemaData {
    private final Map<String, CatalogData.Table> tableMap;
    private final int version; // the template metadata that this schema is currently known to be at
    private final String templateName; // the name of the template that this schema inherits from
    private final String name; // the name of the schema

    public SchemaDataImpl(Map<String, CatalogData.Table> tableMap, int version, String templateName, String name) {
        this.tableMap = tableMap;
        this.version = version;
        this.templateName = templateName;
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getTemplateName() {
        return templateName;
    }

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public Set<TableInfo> getTables() {
        return tableMap.entrySet().stream().map(entry -> new TableInfo(entry.getKey(), entry.getValue())).collect(Collectors.toSet());
    }

    @Override
    public TableInfo getTable(String name) throws RelationalException {
        final CatalogData.Table table = tableMap.get(name);
        if (table == null) {
            throw new RelationalException("Unknown table: " + name, ErrorCode.UNDEFINED_TABLE);
        }
        return new TableInfo(name, table);
    }
}
