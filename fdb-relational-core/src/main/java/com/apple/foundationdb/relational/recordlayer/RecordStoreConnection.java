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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContextConfig;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBTransactionPriority;
import com.apple.foundationdb.relational.api.TransactionConfig;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalDatabaseMetaData;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import java.sql.SQLException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class RecordStoreConnection implements RelationalConnection {
    private boolean isClosed;
    private final FDBDatabase fdbDb;
    final RecordLayerDatabase frl;

    RecordContextTransaction transaction;
    private String currentSchemaLabel;
    private boolean autoCommit = true;
    private boolean usingAnExistingTransaction;
    private TransactionConfig transactionConfig;

    public RecordStoreConnection(@Nonnull RecordLayerDatabase frl, @Nonnull TransactionConfig transactionConfig,
                                 @Nullable RecordContextTransaction existingTransaction) {
        this.fdbDb = frl.getFDBDatabase();
        this.frl = frl;
        this.transaction = existingTransaction;
        this.transactionConfig = transactionConfig;
        this.usingAnExistingTransaction = existingTransaction != null;
    }

    @Override
    public RelationalStatement createStatement() throws SQLException {
        return new RecordStoreStatement(this);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        this.autoCommit = autoCommit;
    }

    @Override
    public boolean getAutoCommit() {
        return !usingAnExistingTransaction && this.autoCommit;
    }

    @Override
    public void commit() throws SQLException {
        RelationalException err = null;
        if (transaction != null) {
            try {
                transaction.commit();
            } catch (RuntimeException | RelationalException re) {
                err = ExceptionUtil.toRelationalException(re);
            }
            try {
                transaction.close();
            } catch (RuntimeException | RelationalException re) {
                if (err != null) {
                    err.addSuppressed(ExceptionUtil.toRelationalException(re));
                } else {
                    err = ExceptionUtil.toRelationalException(re);
                }
            }
            transaction = null;
            usingAnExistingTransaction = false;
        } else {
            err = new RelationalException("No transaction to commit", ErrorCode.TRANSACTION_INACTIVE);
        }
        if (err != null) {
            throw err.toSqlException();
        }
    }

    @Override
    public void rollback() throws SQLException {
        RelationalException err = null;
        if (transaction != null) {
            try {
                transaction.close();
            } catch (RuntimeException | RelationalException re) {
                err = ExceptionUtil.toRelationalException(re);
            }
            transaction = null;
            usingAnExistingTransaction = false;
        }
        if (err != null) {
            throw err.toSqlException();
        }
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        //open the correct record store
        this.currentSchemaLabel = schema;
        //TODO(bfines) validate that this schema exists
    }

    @Override
    public String getSchema() {
        return currentSchemaLabel;
    }

    @Override
    public void close() throws SQLException {
        rollback();
        try {
            frl.close();
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
        isClosed = true;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }

    @Override
    @Nonnull
    public RelationalDatabaseMetaData getMetaData() throws SQLException {
        return new RecordLayerMetaData(this, frl.getKeySpace());
    }

    @Override
    public void beginTransaction(@Nullable TransactionConfig config) throws RelationalException {
        try {
            if (!inActiveTransaction()) {
                transaction = new RecordContextTransaction(
                        fdbDb.openContext(getFDBRecordContextConfig(
                                config == null ? this.transactionConfig : config, frl.getStoreTimer())));
            }
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex);
        }
    }

    boolean inActiveTransaction() {
        return transaction != null;
    }

    private FDBRecordContextConfig getFDBRecordContextConfig(@Nullable TransactionConfig config, FDBStoreTimer storeTimer) throws RelationalException {
        if (config != null) {
            TransactionConfig.WeakReadSemantics weakReadSemantics = config.getWeakReadSemantics();
            return FDBRecordContextConfig.newBuilder()
                    .setTransactionId(config.getTransactionId())
                    .setMdcContext(config.getLoggingContext())
                    .setPriority(getPriorityForFDB(config.getTransactionPriority()))
                    .setWeakReadSemantics(weakReadSemantics == null ? null :
                            new FDBDatabase.WeakReadSemantics(weakReadSemantics.getMinVersion(),
                                    weakReadSemantics.getStalenessBoundMillis(), weakReadSemantics.isCausalReadRisky()))
                    .setTransactionTimeoutMillis(config.getTransactionTimeoutMillis())
                    .setEnableAssertions(config.isEnableAssertions())
                    .setLogTransaction(config.isLogTransaction())
                    .setTrackOpen(config.isTrackOpen())
                    .setSaveOpenStackTrace(config.isSaveOpenStackTrace())
                    .setTimer(storeTimer)
                    .build();
        } else {
            return FDBRecordContextConfig.newBuilder()
                    .setTimer(storeTimer)
                    .setPriority(FDBTransactionPriority.DEFAULT)
                    .build();
        }
    }

    private FDBTransactionPriority getPriorityForFDB(TransactionConfig.Priority priority) throws RelationalException {
        switch (priority) {
            case DEFAULT:
                return FDBTransactionPriority.DEFAULT;
            case BATCH:
                return FDBTransactionPriority.BATCH;
            case SYSTEM_IMMEDIATE:
                return FDBTransactionPriority.SYSTEM_IMMEDIATE;
            default:
                throw new RelationalException("Invalid transaction priority in the config: <" + priority.name() + ">",
                        ErrorCode.INVALID_PARAMETER);
        }
    }
}
