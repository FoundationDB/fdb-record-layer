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

package com.apple.foundationdb.relational.recordlayer.catalog;

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import java.net.URI;

import javax.annotation.Nonnull;

public interface StoreCatalog {
    /**
     * Returns a Schema object of a table.
     *
     * @param txn        a Transaction
     * @param databaseId id of the database
     * @param schemaName schema name
     * @return the schema
     * @throws RelationalException SchemaNotFound if the combination of databaseId and schemaName is not found
     *                           InternalError if txn is incompatible type
     *                           TransactionInactive if txn is no longer active
     */
    @Nonnull
    Schema loadSchema(@Nonnull Transaction txn, @Nonnull URI databaseId, @Nonnull String schemaName) throws RelationalException;

    /**
     * Returns a SchemaTemplate object.
     *
     * @param txn          a Transaction
     * @param templateName name of the schema template
     * @param version      version of the schema template
     * @return the schema template
     * @throws RelationalException UNKNOWN_SCHEMA_TEMPLATE if the combination of templateName and version not found
     */
    @Nonnull
    SchemaTemplate loadSchemaTemplate(@Nonnull Transaction txn, @Nonnull String templateName, long version) throws RelationalException;

    /**
     * Returns the latest version of a schema template.
     *
     * @param txn          a Transaction
     * @param templateName name of the schema template
     * @return the schema template
     * @throws RelationalException UNKNOWN_SCHEMA_TEMPLATE if the templateName not found
     */
    SchemaTemplate loadSchemaTemplate(@Nonnull Transaction txn, @Nonnull String templateName) throws RelationalException;

    /**
     * Updates schema, returns true if succeeds. Change applied after transaction is committed.
     * When 2 transactions try to update the same schema simultaneously, transaction commit fails with FDBExceptions.FDBStoreTransactionConflictException
     *
     * @param txn         a Transaction
     * @param dataToWrite the new Schema
     * @return true if the update succeeds
     * @throws RelationalException InternalError if txn is compatible type
     *                           TransactionInactive if txn is no longer active
     */
    boolean updateSchema(@Nonnull Transaction txn, @Nonnull Schema dataToWrite) throws RelationalException;

    /**
     * Updates schema to the latest template.
     *
     * @param txn        a Transaction
     * @param databaseId database id of the schema to be updated
     * @param schemaName name of the schema to be updated
     * @throws RelationalException InternalError if txn is compatible type
     *                           TransactionInactive if txn is no longer active
     *                           UNDEFINED_SCHEMA if schema not found
     */
    void repairSchema(@Nonnull Transaction txn, @Nonnull String databaseId, @Nonnull String schemaName) throws RelationalException;

    /**
     * Updates a schema template. If version field in the new schema template is unset (=0L), set the version to be lastVersion (=0L if the template doesn't exist) + 1.
     *
     * @param txn         a Transaction
     * @param dataToWrite the new schema template
     * @return true if the update succeeds
     * @throws RelationalException InternalError if txn is compatible type
     *                           TransactionInactive if txn is no longer active
     */
    boolean updateSchemaTemplate(@Nonnull Transaction txn, @Nonnull SchemaTemplate dataToWrite) throws RelationalException;

    boolean doesSchemaTemplateExist(@Nonnull Transaction txn, @Nonnull String templateName) throws RelationalException;

    void createDatabase(@Nonnull Transaction txn, @Nonnull URI dbUri) throws RelationalException;

    /**
     * list databases in the entire Catalog.
     *
     * @param txn          a Transaction
     * @param continuation continuation from a previous execution
     * @return a RelationalResultSet object
     * @throws RelationalException InternalError if txn is compatible type
     *                           TransactionInactive if txn is no longer active
     */
    RelationalResultSet listDatabases(@Nonnull Transaction txn, @Nonnull Continuation continuation) throws RelationalException;

    /**
     * list schemas in entire Catalog.
     *
     * @param txn          a Transaction
     * @param continuation continuation from a previous execution
     * @return a RelationalResultSet object
     * @throws RelationalException InternalError if txn is compatible type
     *                           TransactionInactive if txn is no longer active
     */
    RelationalResultSet listSchemas(@Nonnull Transaction txn, @Nonnull Continuation continuation) throws RelationalException;

    /**
     * list schemas in a database.
     *
     * @param txn          a Transaction
     * @param databaseId   database id
     * @param continuation continuation from a previous execution
     * @return a RelationalResultSet object
     * @throws RelationalException InternalError if txn is compatible type
     *                           TransactionInactive if txn is no longer active
     */
    RelationalResultSet listSchemas(@Nonnull Transaction txn, @Nonnull URI databaseId, @Nonnull Continuation continuation) throws RelationalException;

    /**
     * Delete the schema from the Catalog.
     *
     * @param txn        the transaction to use
     * @param dbUri      the path to the specific database to delete the schema for
     * @param schemaName the name of the schema to delete
     * @throws RelationalException if something goes wrong, with a specific ErrorCode saying what.
     */
    void deleteSchema(Transaction txn, URI dbUri, String schemaName) throws RelationalException;

    boolean doesDatabaseExist(Transaction txn, URI dbUrl) throws RelationalException;

    boolean doesSchemaExist(Transaction txn, URI dbUrl, String schemaName) throws RelationalException;

    void deleteDatabase(Transaction txn, URI dbUrl) throws RelationalException;
}
