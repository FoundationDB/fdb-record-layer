/*
 * HollowStoreCatalog.java
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

package com.apple.foundationdb.relational.transactionbound.catalog;

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.OperationUnsupportedException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.Schema;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.catalog.StoreCatalog;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import javax.annotation.Nonnull;
import java.net.URI;

@ExcludeFromJacocoGeneratedReport
public class HollowStoreCatalog implements StoreCatalog {

    public static final HollowStoreCatalog INSTANCE = new HollowStoreCatalog();

    @Override
    public Schema loadSchema(@Nonnull Transaction txn, @Nonnull URI databaseId, @Nonnull String schemaName) throws RelationalException {
        throw new OperationUnsupportedException("This store catalog is hollow and does not support calls.");
    }

    @Override
    public SchemaTemplate loadSchemaTemplate(@Nonnull Transaction txn, @Nonnull String templateName, long version) throws RelationalException {
        throw new OperationUnsupportedException("This store catalog is hollow and does not support calls.");
    }

    @Override
    public SchemaTemplate loadSchemaTemplate(@Nonnull Transaction txn, @Nonnull String templateName) throws RelationalException {
        throw new OperationUnsupportedException("This store catalog is hollow and does not support calls.");
    }

    @Override
    public boolean doesSchemaTemplateExist(@Nonnull Transaction txn, @Nonnull String templateName) throws RelationalException {
        throw new OperationUnsupportedException("This store catalog is hollow and does not support calls.");
    }

    @Override
    public void saveSchema(@Nonnull Transaction txn, @Nonnull Schema dataToWrite) throws RelationalException {
        throw new OperationUnsupportedException("This store catalog is hollow and does not support calls.");
    }

    @Override
    public void repairSchema(@Nonnull Transaction txn, @Nonnull String databaseId, @Nonnull String schemaName) throws RelationalException {
        throw new OperationUnsupportedException("This store catalog is hollow and does not support calls.");
    }

    @Override
    public void saveSchemaTemplate(@Nonnull Transaction txn, @Nonnull SchemaTemplate dataToWrite) throws RelationalException {
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
    public void deleteSchema(Transaction txn, URI dbUri, String schemaName) throws RelationalException {
        throw new OperationUnsupportedException("This store catalog is hollow and does not support calls.");

    }

    @Override
    public boolean doesDatabaseExist(Transaction txn, URI dbUrl) throws RelationalException {
        throw new OperationUnsupportedException("This store catalog is hollow and does not support calls.");
    }

    @Override
    public boolean doesSchemaExist(Transaction txn, URI dbUri, String schemaName) throws RelationalException {
        // We do not check schema existence in this hollow catalog as it should be checked by the caller
        return true;
    }

    @Override
    public Continuation deleteDatabase(Transaction txn, URI dbUrl, Continuation continuation) throws RelationalException {
        throw new OperationUnsupportedException("This store catalog is hollow and does not support calls.");
    }
}
