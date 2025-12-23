/*
 * HollowStoreCatalog.java
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

package com.apple.foundationdb.relational.transactionbound.catalog;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplateCatalog;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.exceptions.OperationUnsupportedException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.Schema;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;

@API(API.Status.EXPERIMENTAL)
public class HollowStoreCatalog implements StoreCatalog {

    @Nonnull
    public final SchemaTemplate schemaTemplate;
    @Nullable
    private final KeySpace keySpace;

    public HollowStoreCatalog(@Nonnull final SchemaTemplate schemaTemplate, @Nullable final KeySpace keySpace) {
        this.schemaTemplate = schemaTemplate;
        this.keySpace = keySpace;
    }

    @Override
    public SchemaTemplateCatalog getSchemaTemplateCatalog() {
        return null;
    }

    @Nonnull
    @Override
    public Schema loadSchema(@Nonnull Transaction txn, @Nonnull URI databaseId, @Nonnull String schemaName) throws RelationalException {
        return schemaTemplate.generateSchema(databaseId.toString(), schemaName);
    }

    @Override
    public void saveSchema(@Nonnull Transaction txn, @Nonnull Schema dataToWrite, boolean createDatabaseIfNecessary) throws RelationalException {
        throw new OperationUnsupportedException("This store catalog is hollow and does not support calls.");
    }

    @Override
    public void repairSchema(@Nonnull Transaction txn, @Nonnull String databaseId, @Nonnull String schemaName) throws RelationalException {
        throw new OperationUnsupportedException("This store catalog is hollow and does not support calls.");
    }

    @Override
    public void createDatabase(@Nonnull Transaction txn, @Nonnull URI dbUri) throws RelationalException {
        throw new OperationUnsupportedException("This store catalog is hollow and does not support calls.");
    }

    @Override
    public RelationalResultSet listDatabases(@Nonnull Transaction txn, @Nonnull Continuation continuation) throws RelationalException {
        throw new OperationUnsupportedException("This store catalog is hollow and does not support calls.");
    }

    @Override
    public RelationalResultSet listSchemas(@Nonnull Transaction txn, @Nonnull Continuation continuation) throws RelationalException {
        throw new OperationUnsupportedException("This store catalog is hollow and does not support calls.");
    }

    @Override
    public RelationalResultSet listSchemas(@Nonnull Transaction txn, @Nonnull URI databaseId, @Nonnull Continuation continuation) throws RelationalException {
        throw new OperationUnsupportedException("This store catalog is hollow and does not support calls.");
    }

    @Override
    public void deleteSchema(@Nonnull Transaction txn, @Nonnull URI dbUri, @Nonnull String schemaName) throws RelationalException {
        throw new OperationUnsupportedException("This store catalog is hollow and does not support calls.");

    }

    @Override
    public boolean doesDatabaseExist(@Nonnull Transaction txn, @Nonnull URI dbUrl) throws RelationalException {
        throw new OperationUnsupportedException("This store catalog is hollow and does not support calls.");
    }

    @Override
    public boolean doesSchemaExist(@Nonnull Transaction txn, @Nonnull URI dbUri, @Nonnull String schemaName) {
        // We do not check schema existence in this hollow catalog as it should be checked by the caller
        return true;
    }

    @Override
    public boolean deleteDatabase(@Nonnull Transaction txn, @Nonnull URI dbUrl, boolean throwIfDoesNotExist) throws RelationalException {
        throw new OperationUnsupportedException("This store catalog is hollow and does not support calls.");
    }

    @Nonnull
    @Override
    public KeySpace getKeySpace() throws RelationalException {
        if (keySpace == null) {
            throw new OperationUnsupportedException("This store catalog is hollow and does not support calls.");
        } else {
            return keySpace;
        }
    }
}
