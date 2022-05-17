/*
 * SystemTable.java
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

import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.relational.api.ddl.DdlListener;

import javax.annotation.Nonnull;

/**
 * This interface provides an abstraction for a database system table (view).
 * It should be implemented by any new system table, and registered accordingly
 * in the {@link SystemTableRegistry}.
 */
public interface SystemTable {

    /**
     * Returns the name of the system table.
     * @return The name of the system table.
     */
    @Nonnull
    String getName();

    /**
     * Returns the definition of the system table.
     * @param schemaTemplateBuilder a DdlListener.SchemaTemplateBuilder
     * @return The definition of the system table.
     */
    @Nonnull
    DdlListener.TableBuilder getDefinition(@Nonnull final DdlListener.SchemaTemplateBuilder schemaTemplateBuilder);

    /**
     * Returns the primary key definition of the system table. Each system table must have a primary key.
     * @return The primary key.
     */
    @Nonnull
    KeyExpression getPrimaryKeyDefinition();

    /**
     * Returns the record type key which is used for clustering the records of the same type.
     * @return The record type key.
     */
    int getRecordTypeKey();
}
