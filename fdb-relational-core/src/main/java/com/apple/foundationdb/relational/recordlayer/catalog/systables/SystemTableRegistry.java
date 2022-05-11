/*
 * SystemTableRegistry.java
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

package com.apple.foundationdb.relational.recordlayer.catalog.systables;

import java.util.List;

import javax.annotation.Nonnull;

/**
 * A static registry of all available {@link SystemTable}s, the purpose of this interface
 * is to provide a placeholder of these tables that could be consulted e.g. during database
 * bootstrapping.
 */
public final class SystemTableRegistry {

    public static final int SCHEMA_RECORD_TYPE_KEY = 0;
    public static final int DATABASE_INFO_RECORD_TYPE_KEY = 1;
    @Nonnull
    private static final List<SystemTable> tablesMap = List.of(
            new SchemaSystemTable(),
            new DatabaseInfoSystemTable()
    );

    /**
     * Returns a list of all registered system tables.
     * @return a list of all registered system tables.
     */
    @Nonnull
    public static List<SystemTable> getAllTables() {
        return tablesMap;
    }

    private SystemTableRegistry() {
    }
}
