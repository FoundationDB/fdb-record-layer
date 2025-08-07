/*
 * RecordLayerSchema.java
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

package com.apple.foundationdb.relational.recordlayer.metadata;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.api.metadata.Schema;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;

import javax.annotation.Nonnull;

@API(API.Status.EXPERIMENTAL)
public class RecordLayerSchema implements Schema {

    @Nonnull
    private final String name;

    @Nonnull
    private final SchemaTemplate schemaTemplate;

    @Nonnull
    private final String databaseName;

    /**
     * Creates a new instance of the {@link Schema}.
     *
     * @param name           The name of the schema.
     * @param databaseName   The database name to which the schema belongs.
     * @param schemaTemplate The name of the originating schema template.
     */
    RecordLayerSchema(@Nonnull final String name,
                      @Nonnull final String databaseName,
                      @Nonnull final SchemaTemplate schemaTemplate) {
        this.name = name;
        this.databaseName = databaseName;
        this.schemaTemplate = schemaTemplate;
    }

    @Nonnull
    @Override
    public String getName() {
        return name;
    }

    @Nonnull
    @Override
    public SchemaTemplate getSchemaTemplate() {
        return schemaTemplate;
    }

    @Nonnull
    @Override
    public String getDatabaseName() {
        return databaseName;
    }
}
