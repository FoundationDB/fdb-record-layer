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
import com.apple.foundationdb.relational.api.catalog.SchemaExistsBehavior;
import com.apple.foundationdb.relational.api.catalog.SchemaTemplateCatalog;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.exceptions.OperationUnsupportedException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.Schema;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.util.catalog.KeySpaceProvider;

import org.jspecify.annotations.Nullable;
import java.net.URI;

@API(API.Status.EXPERIMENTAL)
public class HollowStoreCatalog implements StoreCatalog, KeySpaceProvider {

    public final SchemaTemplate schemaTemplate;
    @Nullable
    private final KeySpace keySpace;

    public HollowStoreCatalog(final SchemaTemplate schemaTemplate, @Nullable final KeySpace keySpace) {
        this.schemaTemplate = schemaTemplate;
        this.keySpace = keySpace;
    }

    @Override
    @SuppressWarnings("NullAway") // StoreCatalog#getSchemaTemplateCatalog() declares no `throws`, so unlike this
    // class's other methods we cannot throw OperationUnsupportedException here; this hollow catalog genuinely has
    // no schema template catalog to return, and its only caller in this configuration does not invoke this method.
    public SchemaTemplateCatalog getSchemaTemplateCatalog() {
        return null;
    }

    @Override
    public Schema loadSchema(Transaction txn, URI databaseId, String schemaName) throws RelationalException {
        return schemaTemplate.generateSchema(databaseId.toString(), schemaName);
    }

    @Override
    public void saveSchema(Transaction txn, Schema dataToWrite, boolean createDatabaseIfNecessary,
                           SchemaExistsBehavior existsBehavior) throws RelationalException {
        throw new OperationUnsupportedException("This store catalog is hollow and does not support calls.");
    }

    @Override
    public void repairSchema(Transaction txn, String databaseId, String schemaName) throws RelationalException {
        throw new OperationUnsupportedException("This store catalog is hollow and does not support calls.");
    }

    @Override
    public void createDatabase(Transaction txn, URI dbUri) throws RelationalException {
        throw new OperationUnsupportedException("This store catalog is hollow and does not support calls.");
    }

    @Override
    public RelationalResultSet listDatabases(Transaction txn, Continuation continuation) throws RelationalException {
        throw new OperationUnsupportedException("This store catalog is hollow and does not support calls.");
    }

    @Override
    public RelationalResultSet listSchemas(Transaction txn, Continuation continuation) throws RelationalException {
        throw new OperationUnsupportedException("This store catalog is hollow and does not support calls.");
    }

    @Override
    public RelationalResultSet listSchemas(Transaction txn, URI databaseId, Continuation continuation) throws RelationalException {
        throw new OperationUnsupportedException("This store catalog is hollow and does not support calls.");
    }

    @Override
    public void deleteSchema(Transaction txn, URI dbUri, String schemaName) throws RelationalException {
        throw new OperationUnsupportedException("This store catalog is hollow and does not support calls.");

    }

    @Override
    public boolean doesDatabaseExist(Transaction txn, URI dbUrl) throws RelationalException {
        throw new OperationUnsupportedException("This store catalog is hollow and does not support calls.");
    }

    @Override
    public boolean doesSchemaExist(Transaction txn, URI dbUri, String schemaName) {
        // We do not check schema existence in this hollow catalog as it should be checked by the caller
        return true;
    }

    @Override
    public boolean deleteDatabase(Transaction txn, URI dbUrl, boolean throwIfDoesNotExist) throws RelationalException {
        throw new OperationUnsupportedException("This store catalog is hollow and does not support calls.");
    }

    @Nullable
    @Override
    public KeySpace getKeySpace() {
        return keySpace;
    }
}
