/*
 * SystemTable.java
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

package com.apple.foundationdb.relational.recordlayer.catalog.systables;

import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerTable;

import javax.annotation.Nonnull;

/**
 * This interface provides an abstraction for a database system table (view).
 * It should be implemented by any new system table, and registered accordingly
 * in the {@link SystemTableRegistry}.
 */
public interface SystemTable {
    /**
     * Common table column name used creating, querying, and as foreign key.
     */
    String DATABASE_ID = "DATABASE_ID";
    /**
     * Common table column name used creating, querying, and as foreign key.
     */
    String TEMPLATE_NAME = "TEMPLATE_NAME";
    /**
     * Common table column name used creating, querying, and as foreign key.
     */
    String TEMPLATE_VERSION = "TEMPLATE_VERSION";

    /**
     * Returns the name of the system table.
     * @return The name of the system table.
     */
    @Nonnull
    String getName();

    /**
     * Adds the definition of the system table to the <i>stateful</i> {@link RecordLayerSchemaTemplate.Builder}.
     *
     * During bootstrapping, Relational will populate the {@code __SYS/catalog} with information
     * about system tables such as {@link SchemaSystemTable}, during this process a single {@link RecordLayerSchemaTemplate.Builder}
     * instance is used to gather the structure of each of these tables by calling this method and asking each
     * system table to contribute its definition to the typing context. In other words, it resembles a visitor.
     * Returns the definition of the system table.
     *
     * @param schemaBuilder The current schema builder used to populate schema template information.
     */
    void addDefinition(@Nonnull RecordLayerSchemaTemplate.Builder schemaBuilder);

    RecordLayerTable getType();

    /**
     * Returns the primary key definition of the system table. Each system table must have a primary key.
     * @return The primary key.
     */
    @Nonnull
    KeyExpression getPrimaryKeyDefinition();

}
