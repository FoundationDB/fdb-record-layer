/*
 * DdlConnection.java
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

package com.apple.foundationdb.relational.api.ddl;

import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.TransactionManager;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import java.net.URI;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DdlConnection implements AutoCloseable {
    private final TransactionManager txnManager;

    private final ConstantActionFactory constantActionFactory;
    private final DdlQueryFactory queryFactory;

    private Transaction txn;

    /*
     * When you want to do something like SET DATABASE, it will set that value here, and use it as a default
     * database when you don't specify one for e.g. schema commands
     */
    private URI databaseContext;

    public DdlConnection(TransactionManager txnManager,
                         ConstantActionFactory constantActionFactory,
                         DdlQueryFactory queryFactory) {
        this.txnManager = txnManager;
        this.constantActionFactory = constantActionFactory;
        this.queryFactory = queryFactory;
    }

    public DdlStatement createStatement() throws RelationalException {
        begin();
        return new DdlStatement(txn, this, constantActionFactory, queryFactory);
    }

    /**
     * Sets the current database for this connection.
     *
     * (bfines)Note that it does _not_ validate whether such a database exists or not. We may or may not
     * want to add that as a future feature
     * @param database the database to set
     */
    public void setDatabase(@Nonnull URI database) {
        this.databaseContext = database;
    }

    /**
     * Get the current database URI for this connection, or {@code null} if none have been set.
     *
     * @return the current database for this connection, or {@code null} if none have been set.
     */
    @Nullable
    public URI getCurrentDatabase() {
        return databaseContext;
    }

    /**
     * Begin a new transaction.
     *
     * If this connection is currently in an active transaction, this will have no effect.
     * @throws RelationalException if something goes wrong in creating a transaction
     */
    public void begin() throws RelationalException {
        if (txn == null) {
            txn = txnManager.createTransaction();
        }
    }

    public void commit() throws RelationalException {
        if (txn != null) {
            txnManager.commit(txn);
        }
    }

    void abort() throws RelationalException {
        if (txn != null) {
            txnManager.abort(txn);
        }
    }

    @Override
    public void close() throws RelationalException {
        if (txn != null) {
            txn.abort();
        }
    }
}
