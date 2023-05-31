/*
 * EmbeddedRelationalConnection.java
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
import com.apple.foundationdb.relational.api.FieldDescription;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RowArray;
import com.apple.foundationdb.relational.api.SqlTypeNamesSupport;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.TransactionManager;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalDatabaseMetaData;
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.RelationalStructMetaData;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.InternalErrorException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.api.metrics.MetricCollector;
import com.apple.foundationdb.relational.recordlayer.metric.RecordLayerMetricCollector;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.sql.Array;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.stream.Collectors;

public class EmbeddedRelationalConnection implements RelationalConnection {
    private boolean isClosed;
    final AbstractDatabase frl;
    final StoreCatalog backingCatalog;
    MetricCollector metricCollector;
    Transaction transaction;
    private String currentSchemaLabel;
    private boolean autoCommit = true;
    private boolean usingAnExistingTransaction;
    private final TransactionManager txnManager;
    @Nonnull
    private Options options;

    public EmbeddedRelationalConnection(@Nonnull AbstractDatabase frl,
                                      @Nonnull StoreCatalog backingCatalog,
                                      @Nullable Transaction transaction,
                                      @Nonnull Options options) throws InternalErrorException {
        this.frl = frl;
        this.txnManager = frl.getTransactionManager();
        this.transaction = transaction;
        this.usingAnExistingTransaction = transaction != null;
        if (usingAnExistingTransaction) {
            this.metricCollector = new RecordLayerMetricCollector(transaction.unwrap(RecordContextTransaction.class).getContext());
        }
        this.backingCatalog = backingCatalog;
        this.options = options;
    }

    @Override
    public RelationalStatement createStatement() throws SQLException {
        return new ErrorCapturingStatement(new EmbeddedRelationalStatement(this));
    }

    @Override
    public RelationalPreparedStatement prepareStatement(String sql) throws SQLException {
        return new EmbeddedRelationalPreparedStatement(sql, this);
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
        setSchema(schema, true);
    }

    void setSchema(@Nullable String schema, boolean checkSchemaExists) throws SQLException {
        if (schema == null) {
            this.currentSchemaLabel = null;
            return;
        }
        if (checkSchemaExists) {
            boolean newTransaction = !inActiveTransaction();
            try {
                if (newTransaction) {
                    beginTransaction();
                }
                if (!this.backingCatalog.doesSchemaExist(transaction, frl.getURI(), schema)) {
                    throw new RelationalException(String.format("Schema %s does not exist in %s", schema, frl.getURI()), ErrorCode.UNDEFINED_SCHEMA);
                }
                this.currentSchemaLabel = schema;
            } catch (RelationalException e) {
                throw e.toSqlException();
            } finally {
                if (newTransaction) {
                    rollback();
                }
            }
        } else {
            this.currentSchemaLabel = schema;
        }
    }

    @Nonnull
    public SchemaTemplate getSchemaTemplate() throws RelationalException {
        return backingCatalog.loadSchema(transaction, getPath(), getSchema()).getSchemaTemplate();
    }

    @Nonnull
    public MetricCollector getMetricCollector() {
        return metricCollector;
    }

    @Override
    public String getSchema() {
        return currentSchemaLabel;
    }

    @Override
    public void close() throws SQLException {
        SQLException se = null;
        try {
            rollback();
        } catch (SQLException e) {
            se = e;
        }
        try {
            frl.close();
        } catch (RelationalException e) {
            if (se != null) {
                se.addSuppressed(e.toSqlException());
            } else {
                se = e.toSqlException();
            }
        }
        if (se != null) {
            throw se;
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
        return new CatalogMetaData(this, backingCatalog);
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        int typeCode = SqlTypeNamesSupport.getSqlTypeCode(typeName);
        return new RowArray(
                Arrays.stream(elements).map(ArrayRow::new).collect(Collectors.toList()),
                new RelationalStructMetaData(
                        FieldDescription.primitive("na", typeCode, DatabaseMetaData.columnNoNulls)));
    }

    @Override
    public void beginTransaction() throws SQLException {
        try {
            if (!inActiveTransaction()) {
                transaction = txnManager.createTransaction(options);
                metricCollector = new RecordLayerMetricCollector(transaction.unwrap(RecordContextTransaction.class).getContext());
                addCloseListener(() -> {
                    if (metricCollector != null) {
                        metricCollector.flush();
                        metricCollector = null;
                    }
                });
            }
        } catch (RecordCoreException ex) {
            throw ExceptionUtil.toRelationalException(ex).toSqlException();
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
    }

    @Override
    @Nonnull
    public Options getOptions() {
        return options;
    }

    @Override
    public void setOption(Options.Name name, Object value) throws SQLException {
        options = Options.builder().fromOptions(options).withOption(name, value).build();
    }

    @Override
    public URI getPath() {
        return this.frl.getURI();
    }

    boolean inActiveTransaction() {
        return transaction != null;
    }

    void addCloseListener(@Nonnull Runnable closeListener) throws RelationalException {
        this.transaction.unwrap(RecordContextTransaction.class).addTerminationListener(closeListener);
    }

    @Nonnull
    public AbstractDatabase getRecordLayerDatabase() {
        return frl;
    }

    public void ensureTransactionActive() throws RelationalException {
        if (!inActiveTransaction()) {
            if (getAutoCommit()) {
                try {
                    beginTransaction();
                } catch (SQLException e) {
                    throw ExceptionUtil.toRelationalException(e);
                }
            } else {
                throw new RelationalException("Transaction not begun", ErrorCode.TRANSACTION_INACTIVE);
            }
        }
    }

    @Override
    @Nonnull
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return iface.cast(this);
    }
}
