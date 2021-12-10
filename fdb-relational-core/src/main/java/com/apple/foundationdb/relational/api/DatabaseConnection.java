/*
 * DatabaseConnection.java
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

package com.apple.foundationdb.relational.api;

import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Represents a connection to a Relational database.
 * <p>
* A connection to a Relational database.
 * <p>
 * Also note that a connection is the transaction manager for the client, so if you have multiple transactions
 * against the same database, you should _also_ expect multiple connection objects to exist. So all in all,
 * lots of Connection objects should be expected on the client's heap at any given point in time.
 */
public interface DatabaseConnection extends AutoCloseable {
    // TODO(bfines) at some point, we will want to extend java.sql.Connection, in order
    // to give ourselves the full power of SQL support. But for now, ease of implementation
    // says to just apply the special interface functions

    /**
     * Create a Statement which can be executed using this DatabaseConnection.
     *
     * If this connection instance is closed, this Statement becomes invalid and should be discarded. Using a
     * Statement object after it's backing Connection is closed is a programmer error.
     *
     * @return A new statement entity to manipulate the database with.
     * @throws RelationalException if something goes wrong while creating a statement.
     */
    Statement createStatement() throws RelationalException;

    //TODO(bfines) We would probably want to implement and support an "AsyncStatement" here, which
    // can capture the asyncronous operations necessary

    void setAutoCommit(boolean autoCommit) throws RelationalException;

    boolean isAutoCommitEnabled();

    /**
     * If no TransactionConfig passed in, a default config is used for the transaction.
     */
    default void beginTransaction() throws RelationalException {
        beginTransaction(null);
    }

    void beginTransaction(@Nullable TransactionConfig config) throws RelationalException;

    void commit() throws RelationalException;

    void rollback() throws RelationalException;

    /**
     * Set the database schema that the commands will operate against.
     * <p>
     * It is possible that this operation does work (i.e. triggering a schema update to get it in line with the
     * schema template).
     *
     * @param schema the schema to set.
     * @throws RelationalException with error code {@link RelationalException.ErrorCode#SCHEMA_NOT_FOUND}
     *                           if no schema of that name exists in the database, and no schema template exists to create one.
     * @throws RelationalException if something goes wrong during execution. This can be anything, so rely
     *                           on the error code to determine exactly what went wrong
     */
    void setSchema(String schema) throws RelationalException;

    /**
     * Get the current schema that this Connection is using.
     *
     * @return the current schema that this Connection is using, or {@code null} if no schema is specified.
     * @throws RelationalException if something goes wrong during execution. This can be anything, so rely
     *                           on the error code to determine exactly what went wrong
     */
    String getSchema() throws RelationalException;

    @Override
    void close() throws RelationalException;

    @Nonnull
    RelationalDatabaseMetaData getMetaData() throws RelationalException;
}
