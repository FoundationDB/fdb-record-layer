/*
 * StoreCatalog.java
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

import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.generated.CatalogData;

import java.net.URI;

import javax.annotation.Nonnull;

public interface StoreCatalog {

    /*
    RelationalResultSet listDatabases(@Nonnull Transaction transaction, @Nonnull Continuation continuation);

    RelationalResultSet listSchemas(@Nonnull Transaction transaction, @Nonnull Continuation continuation);

    RelationalResultSet listSchemas(@Nonnull Transaction txn, @Nonnull URI databaseId, @Nonnull Continuation continuation);

     */

    /**
     * Returns a RecordLayerSchemaData object of a table.
     *
     * @param txn        a Transaction
     * @param databaseId id of the database
     * @param schemaName schema name
     * @return the schema
     * @throws RelationalException SchemaNotFoundException if the combination of databaseId and schemaName is not found
     *                           InternalErrorException if txn is incompatible type
     *                           TransactionInactiveException if txn is no longer active
     */
    SchemaData loadSchema(@Nonnull Transaction txn, @Nonnull URI databaseId, @Nonnull String schemaName) throws RelationalException;

    /**
     * Updates schema, returns true if succeeds. Change applied after transaction is committed.
     * When 2 transactions try to update the same schema simultaneously, transaction commit fails with FDBExceptions.FDBStoreTransactionConflictException
     *
     * @param txn         a Transaction
     * @param dataToWrite CatalogData.Schema object that will be stored in FDB
     * @return true if the update succeeds
     * @throws RelationalException InternalErrorException if txn is compatible type
     */
    boolean updateSchema(@Nonnull Transaction txn, @Nonnull CatalogData.Schema dataToWrite) throws RelationalException;
}
