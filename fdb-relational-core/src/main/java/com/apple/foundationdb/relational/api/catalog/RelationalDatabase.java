/*
 * RelationalDatabase.java
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

package com.apple.foundationdb.relational.api.catalog;

import com.apple.foundationdb.relational.api.ConnectionScoped;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.ddl.MetadataOperationsFactory;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * There can only be 1 Database object per Connection instance, and its lifecycle is managed by the connection
 * itself.
 */
@ConnectionScoped
public interface RelationalDatabase extends AutoCloseable {

    RelationalConnection connect(@Nullable Transaction sharedTransaction) throws RelationalException;

    /**
     * Load the specified schema for the database.
     *
     * @param schemaId the unique id of the schema
     * @return a Schema for the specified id.
     * @throws RelationalException if something goes wrong during load
     */
    DatabaseSchema loadSchema(@Nonnull String schemaId) throws RelationalException;

    @Nonnull
    MetadataOperationsFactory getDdlFactory();

    @Override
    void close() throws RelationalException;
}
