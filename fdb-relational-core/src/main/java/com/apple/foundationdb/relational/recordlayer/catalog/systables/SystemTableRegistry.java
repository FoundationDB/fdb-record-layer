/*
 * SystemTableRegistry.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.catalog.systables;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * A static registry of all available {@link SystemTable}s, the purpose of this interface
 * is to provide a placeholder of these tables that could be consulted e.g. during database
 * bootstrapping.
 */
@ExcludeFromJacocoGeneratedReport //We don't need to test map accesses
@API(API.Status.EXPERIMENTAL)
public final class SystemTableRegistry {

    public static final long SCHEMA_RECORD_TYPE_KEY = 0L;
    public static final long DATABASE_INFO_RECORD_TYPE_KEY = 1L;

    public static final long SCHEMA_TEMPLATE_RECORD_TYPE_KEY = 2L;

    public static final String SCHEMAS_TABLE_NAME = "SCHEMAS";
    public static final String DATABASE_TABLE_NAME = "DATABASES";
    public static final String SCHEMA_TEMPLATE_TABLE_NAME = "TEMPLATES";

    @Nonnull
    private static final Map<String, SystemTable> tablesMap = Map.of(
            SCHEMAS_TABLE_NAME, new SchemaSystemTable(),
            DATABASE_TABLE_NAME, new DatabaseInfoSystemTable(),
            SCHEMA_TEMPLATE_TABLE_NAME, new SchemaTemplateSystemTable()
    );

    @Nonnull
    public static SystemTable getSystemTable(String key) {
        final SystemTable systemTable = tablesMap.get(key);
        if (systemTable == null) {
            throw new IllegalStateException("Programmer error: missing system table named <" + key + ">");
        }
        return systemTable;
    }

    private SystemTableRegistry() {
    }
}
