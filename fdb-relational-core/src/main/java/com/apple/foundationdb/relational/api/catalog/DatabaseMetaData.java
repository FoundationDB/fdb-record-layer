/*
 * DatabaseMetaData.java
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

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import java.util.Set;

import javax.annotation.Nonnull;

public interface DatabaseMetaData {

    /**
     * Get metadata for the specified schema and table.
     * @param schemaId the identifier for the schema
     * @param tableId the identifier of the table of interest
     * @return metadata about the specified table.
     * @throws RelationalException if something goes wrong. If no table is found, then the thrown exception
     * will have an {@code ErrorCode} of {@link ErrorCode#UNDEFINED_TABLE}.
     * If no schema is found, the error code will be {@link ErrorCode#UNKNOWN_SCHEMA}
     */
    @Nonnull
    TableMetaData getTableMetaData(@Nonnull String schemaId, @Nonnull String tableId) throws RelationalException;

    Set<String> getSchemas() throws RelationalException;
}
