/*
 * RecordStoreConnection.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.relational.api.DatabaseConnection;
import com.apple.foundationdb.relational.api.Statement;
import com.apple.foundationdb.relational.api.RelationalException;
import com.apple.foundationdb.relational.api.catalog.RelationalDatabase;

public class RecordStoreConnection implements DatabaseConnection {
    private final FDBDatabase fdbDb;
    final RelationalDatabase frl;

    RecordContextTransaction transaction = null;
    private String currentSchemaLabel;
    private boolean autoCommit = true;

    public RecordStoreConnection(RecordLayerDatabase frl) {
        this.fdbDb = frl.getFDBDatabase();
        this.frl = frl;
    }

    @Override
    public Statement createStatement() throws RelationalException {
        return new RecordStoreStatement(this);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws RelationalException {
        this.autoCommit = autoCommit;
    }

    @Override
    public boolean isAutoCommitEnabled() {
        return this.autoCommit;
    }

    @Override
    public void commit() throws RelationalException {
        RelationalException err = null;
        if(transaction!=null) {
            try {
                transaction.commit();
            } catch (RuntimeException re) {
                err = RelationalException.convert(re);
            }
            try {
                transaction.close();
            } catch (RuntimeException re) {
                if (err != null) {
                    err.addSuppressed(RelationalException.convert(re));
                } else {
                    err = RelationalException.convert(re);
                }
            }
            transaction = null;
        }else{
            err = new RelationalException("No transaction to commit", RelationalException.ErrorCode.TRANSACTION_INACTIVE);
        }
        if (err != null) {
            throw err;
        }
    }

    @Override
    public void rollback() throws RelationalException {
        RelationalException err = null;
        if(transaction!=null) {
            try {
                transaction.close();
            } catch (RuntimeException re) {
                err = RelationalException.convert(re);
            }
            transaction = null;
        }
        if (err != null) {
            throw err;
        }
    }

    @Override
    public void setSchema(String schema) throws RelationalException {
        //open the correct record store
        this.currentSchemaLabel = schema;
        //TODO(bfines) validate that this schema exists
    }

    @Override
    public String getSchema() throws RelationalException {
        return currentSchemaLabel;
    }

    @Override
    public void close() throws RelationalException {
        rollback();
    }

    @Override
    public void beginTransaction() {
        transaction = new RecordContextTransaction(fdbDb.openContext());
    }

    boolean inActiveTransaction() {
        return transaction!=null;
    }
}
