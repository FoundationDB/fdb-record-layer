/*
 * EmbeddedRelationalConnection.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.relational.api.EmbeddedRelationalDriver;
import com.apple.foundationdb.relational.api.EmbeddedRelationalStruct;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalArrayMetaData;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalDatabaseMetaData;
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.RowArray;
import com.apple.foundationdb.relational.api.SqlTypeNamesSupport;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.TransactionManager;
import com.apple.foundationdb.relational.api.catalog.StoreCatalog;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.InternalErrorException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.fluentsql.expression.ExpressionFactory;
import com.apple.foundationdb.relational.api.fluentsql.statement.StatementBuilderFactory;
import com.apple.foundationdb.relational.api.metadata.DataType;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.api.metrics.MetricCollector;
import com.apple.foundationdb.relational.recordlayer.metric.RecordLayerMetricCollector;
import com.apple.foundationdb.relational.recordlayer.structuredsql.expression.ExpressionFactoryImpl;
import com.apple.foundationdb.relational.recordlayer.structuredsql.statement.StatementBuilderFactoryImpl;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;
import com.apple.foundationdb.relational.util.Supplier;
import com.google.common.annotations.VisibleForTesting;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Struct;
import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Implementation of {@link RelationalConnection} for connecting to a database in Relational when running as an embedded library.
 * It can itself function in 2 modes:
 * <ul>
 *     <li>Default: The transaction is not supplied to the connection. Its start and end is guided by the JDBC
 *     specification given the state of {@code autoCommit}</li>
 *     <li>With External transaction: In this scenario, an already opened {@link Transaction} is supplied to the connection
 *     to be used to execute all statements and procedure. For consumer perspective, this is equivalent to {@code autoCommit}
 *     being set to {@code true} since the {@link Connection#commit()} and {@link Connection#rollback()} wont be applicable.
 *     However, all statements run within the external transaction. For internal usage, the consumer should check
 *     {@link EmbeddedRelationalConnection#canCommit()} to see if they are allowed to manage a transaction.</li>
 * </ul>
 */
@API(API.Status.EXPERIMENTAL)
public class EmbeddedRelationalConnection implements RelationalConnection {

    /**
     * Chose a transaction level that we *pretend* to support (Rather than throw an exception that stops
     * all processing).
     * TODO: Implement.
     */
    private static final int DEFAULT_TRANSACTION_LEVEL = Connection.TRANSACTION_SERIALIZABLE;

    private boolean isClosed;
    @Nonnull
    private final AbstractDatabase frl;
    @Nonnull
    private final StoreCatalog backingCatalog;
    @Nullable
    private MetricCollector metricCollector;
    @Nullable
    private Transaction transaction;
    ExecuteProperties executeProperties;
    private String currentSchemaLabel;
    private boolean autoCommit = true;
    private final boolean usingAnExternalTransaction;
    private final TransactionManager txnManager;

    @Nonnull
    private Options options;

    private int transactionIsolation;

    @SpotBugsSuppressWarnings(value = "CT_CONSTRUCTOR_THROW", justification = "May be refactored as embedded takes over transaction lifetime")
    public EmbeddedRelationalConnection(@Nonnull AbstractDatabase frl,
                                      @Nonnull StoreCatalog backingCatalog,
                                      @Nullable Transaction transaction,
                                      @Nonnull Options options) throws InternalErrorException {
        this.frl = frl;
        this.txnManager = frl.getTransactionManager();
        this.transaction = transaction;
        this.usingAnExternalTransaction = transaction != null;
        if (usingAnExternalTransaction) {
            this.metricCollector = new RecordLayerMetricCollector(transaction.unwrap(RecordContextTransaction.class).getContext());
        }
        this.backingCatalog = backingCatalog;
        this.options = options;
        transactionIsolation = Connection.TRANSACTION_SERIALIZABLE;
        executeProperties = newExecuteProperties();
    }

    @Override
    public RelationalStatement createStatement() throws SQLException {
        checkOpen();
        return new EmbeddedRelationalStatement(this);
    }

    @Override
    public RelationalPreparedStatement prepareStatement(String sql) throws SQLException {
        checkOpen();
        return new EmbeddedRelationalPreparedStatement(sql, this);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        // Do nothing. Swallow rather than return useless complaint about not being implemented
        // which stops all processing.
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkOpen();
        if (usingAnExternalTransaction) {
            throw new RelationalException("Cannot set autoCommit when using an external transaction!", ErrorCode.INVALID_TRANSACTION_STATE).toSqlException();
        }
        // NOTE: If this method is called during a transaction and the auto-commit mode is changed, the transaction is
        // committed. If setAutoCommit is called and the auto-commit mode is not changed, the call is a no-op.
        // ref. https://docs.oracle.com/javase/8/docs/api/java/sql/Connection.html#setAutoCommit-boolean-
        if (this.autoCommit == autoCommit) {
            return;
        }
        if (inActiveTransaction()) {
            commitInternal();
        }
        this.autoCommit = autoCommit;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkOpen();
        return usingAnExternalTransaction || this.autoCommit;
    }

    /**
     * Internal API that returns if the stakeholder of the transaction can commit or rollback an active transaction.
     * This is different from {@link RelationalConnection#getAutoCommit()} which tells the consumer of the
     * {@link RelationalConnection} to know if the autoCommit is switch on or off for the connection.
     *
     * @return {@code true} if the transaction can be committed by internal stakeholders, else {@code false}.
     * @throws SQLException if the connection is closed.
     */
    boolean canCommit() throws SQLException {
        checkOpen();
        return !usingAnExternalTransaction && this.autoCommit;
    }

    @Override
    public void commit() throws SQLException {
        checkOpen();
        if (getAutoCommit()) {
            throw new RelationalException("commit called when the Connection is in auto-commit mode!", ErrorCode.CANNOT_COMMIT_ROLLBACK_WITH_AUTOCOMMIT).toSqlException();
        }
        if (!inActiveTransaction()) {
            throw new RelationalException("No transaction to commit", ErrorCode.TRANSACTION_INACTIVE).toSqlException();
        }
        commitInternal();
    }

    void commitInternal() throws SQLException {
        RelationalException err = null;
        try {
            getTransaction().commit();
        } catch (RuntimeException | RelationalException re) {
            err = ExceptionUtil.toRelationalException(re);
        }
        try {
            getTransaction().close();
        } catch (RuntimeException | RelationalException re) {
            if (err != null) {
                err.addSuppressed(ExceptionUtil.toRelationalException(re));
            } else {
                err = ExceptionUtil.toRelationalException(re);
            }
        }
        transaction = null;
        if (err != null) {
            throw err.toSqlException();
        }
    }

    @Override
    public void rollback() throws SQLException {
        checkOpen();
        if (getAutoCommit()) {
            throw new RelationalException("rollback called when the Connection is in auto-commit mode!", ErrorCode.CANNOT_COMMIT_ROLLBACK_WITH_AUTOCOMMIT).toSqlException();
        }
        if (!inActiveTransaction()) {
            throw new RelationalException("No transaction to rollback!", ErrorCode.TRANSACTION_INACTIVE).toSqlException();
        }
        rollbackInternal();
    }

    void rollbackInternal() throws SQLException {
        RelationalException err = null;
        try {
            getTransaction().close();
        } catch (RuntimeException | RelationalException re) {
            err = ExceptionUtil.toRelationalException(re);
        }
        transaction = null;
        if (err != null) {
            throw err.toSqlException();
        }
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        setSchema(schema, true);
    }

    void setSchema(@Nullable String schema, boolean checkSchemaExists) throws SQLException {
        checkOpen();
        if (schema == null) {
            this.currentSchemaLabel = null;
            return;
        }
        if (checkSchemaExists) {
            checkSchemaExists(schema);
        }
        this.currentSchemaLabel = schema;
    }

    private void checkSchemaExists(@Nonnull String schema) throws SQLException {
        runIsolatedInTransactionIfPossible(() -> {
            if (!this.backingCatalog.doesSchemaExist(getTransaction(), getRecordLayerDatabase().getURI(), schema)) {
                throw new RelationalException(String.format(Locale.ROOT, "Schema %s does not exist in %s", schema, getPath()), ErrorCode.UNDEFINED_SCHEMA);
            }
            return null;
        });
    }

    @Nonnull
    public SchemaTemplate getSchemaTemplate() throws RelationalException {
        try {
            return backingCatalog.loadSchema(getTransaction(), getPath(), getSchema()).getSchemaTemplate();
        } catch (SQLException sqle) {
            throw new RelationalException(sqle);
        }
    }

    @Nullable
    public MetricCollector getMetricCollector() {
        return metricCollector;
    }

    @Override
    public String getSchema() throws SQLException {
        checkOpen();
        return currentSchemaLabel;
    }

    @Override
    public void close() throws SQLException {
        SQLException se = null;
        try {
            if (inActiveTransaction()) {
                rollbackInternal();
            }
        } catch (SQLException e) {
            se = e;
        }
        try {
            getRecordLayerDatabase().close();
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

    @Nonnull
    @Override
    public RelationalDatabaseMetaData getMetaData() throws SQLException {
        return new CatalogMetaData(this, this.backingCatalog) {
            @Override
            public String getDriverName() {
                return EmbeddedRelationalDriver.DRIVER_NAME;
            }

            @Override
            public int getDefaultTransactionIsolation() {
                return DEFAULT_TRANSACTION_LEVEL;
            }

            @Override
            public boolean supportsTransactionIsolationLevel(int level) {
                return getDefaultTransactionIsolation() == level;
            }

            @Override
            public String getCatalogTerm() {
                return RelationalKeyspaceProvider.CATALOG;
            }
        };
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        transactionIsolation = level;
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return transactionIsolation;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        // Return null for now until we implement Warnings. Returning an exception breaks processing.
        return null;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        final var dataType = SqlTypeNamesSupport.getDataTypeFromSqlTypeName(typeName);
        if (dataType != null) {
            return new RowArray(Arrays.stream(elements).collect(Collectors.toList()), RelationalArrayMetaData.of(DataType.ArrayType.from(dataType, false)));
        } else if (elements.length == 0) {
            throw new RelationalException("Cannot determine the complete component type of array of struct since it has no elements!", ErrorCode.INTERNAL_ERROR).toSqlException();
        } else {
            final var elementType = DataType.getDataTypeFromObject(elements[0]);
            if (elementType instanceof DataType.ArrayType) {
                throw new RelationalException("Nested arrays are not supported yet!", ErrorCode.UNSUPPORTED_OPERATION).toSqlException();
            } else if (elementType.getJdbcSqlCode() != SqlTypeNamesSupport.getSqlTypeCode(typeName)) {
                throw new RelationalException("Element of the array is expected to be of type " + typeName, ErrorCode.DATATYPE_MISMATCH).toSqlException();
            }
            return new RowArray(Arrays.stream(elements).collect(Collectors.toList()), RelationalArrayMetaData.of(DataType.ArrayType.from(elementType, false)));
        }
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        // We do not preserve the typeName, as there is no validation around that currently.
        final var builder = EmbeddedRelationalStruct.newBuilder();
        int nextFieldIndex = 0;
        for (var atr: attributes) {
            builder.addObject("f" + nextFieldIndex++, atr);
        }
        return builder.build();
    }

    private void startTransaction() throws SQLException {
        try {
            if (!inActiveTransaction()) {
                transaction = txnManager.createTransaction(options);
                executeProperties = newExecuteProperties();
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

    @Nonnull
    @Override
    public Options getOptions() {
        return options;
    }

    @Override
    public void setOption(Options.Name name, Object value) throws SQLException {
        options = options.withOption(name, value);
        frl.setOption(name, value);
    }

    @Override
    public URI getPath() {
        return getRecordLayerDatabase().getURI();
    }

    @Nonnull
    public StoreCatalog getBackingCatalog() {
        return backingCatalog;
    }

    /**
     * Returns the {@link com.apple.foundationdb.Transaction} object if there is one active.
     *
     * @return the current transaction.
     * @throws RelationalException if there is no active transaction.
     */
    @Nonnull
    public Transaction getTransaction() throws RelationalException {
        if (transaction == null) {
            throw new RelationalException("No Active Transaction!", ErrorCode.INVALID_TRANSACTION_STATE);
        }
        return transaction;
    }

    /**
     * Returns if there is an ongoing transaction. This does not differentiate if the transaction is an external one
     * (Connection stated with external transaction mode) or managed by this connection itself (default mode). If needs
     * to be checked if the transaction is the former, use {@link EmbeddedRelationalConnection#usingAnExternalTransaction}.
     *
     * @return {@code true} if there is a transaction, else {@code false}.
     */
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

    /**
     * Ensures that the connection has an active transaction. If the connection does not have one, it will start a new
     * transaction irrespective of the state of {@link EmbeddedRelationalConnection#getAutoCommit()}. This method returns
     * a boolean indicative of the fact that a new transaction has been started to fulfill the request.
     *
     * NOTE: This is an internal API not to be used by holders of the {@link Connection}.
     *
     * @return {@code true} if calling this method starts a new transaction, else {@code false}.
     * @throws RelationalException if there is no active transaction and one cannot be started.
     */
    boolean ensureTransactionActive() throws RelationalException, SQLException {
        if (inActiveTransaction()) {
            if (canCommit()) {
                // canCommit() = true means that there is no external transaction and autoCommit is enabled, meaning
                // that the internal stakeholders can manage the transactions when required to.
                // As this implies that autoCommit is enabled, if there is an existing transaction in such a case, that
                // should be dropped and a fresh transaction should be started.
                rollbackInternal();
                // call again with no active transaction.
                return ensureTransactionActive();
            }
            return false;
        }
        try {
            startTransaction();
            return true;
        } catch (SQLException e) {
            throw ExceptionUtil.toRelationalException(e);
        }
    }

    @VisibleForTesting
    public void createNewTransaction() throws RelationalException, SQLException {
        if (inActiveTransaction()) {
            throw new RelationalException("There is already an opened transaction!", ErrorCode.INVALID_TRANSACTION_STATE);
        }
        ensureTransactionActive();
    }

    @Nonnull
    public ExecuteProperties getExecuteProperties() {
        return executeProperties;
    }

    private static IsolationLevel toExecutePropertiesIsolationLevel(int jdbcTransactionIsolation) {
        if (jdbcTransactionIsolation == Connection.TRANSACTION_SERIALIZABLE) {
            return IsolationLevel.SERIALIZABLE;
        } else {
            return IsolationLevel.SNAPSHOT;
        }
    }

    // todo: remove this.
    private ExecuteProperties newExecuteProperties() {
        return ExecuteProperties.newBuilder()
                .setIsolationLevel(toExecutePropertiesIsolationLevel(transactionIsolation))
                .setTimeLimit(options.getOption(Options.Name.EXECUTION_TIME_LIMIT))
                .setScannedBytesLimit(options.getOption(Options.Name.EXECUTION_SCANNED_BYTES_LIMIT))
                .setScannedRecordsLimit(options.getOption(Options.Name.EXECUTION_SCANNED_ROWS_LIMIT))
                .setFailOnScanLimitReached(false)
                .build();
    }

    private void checkOpen() throws SQLException {
        if (isClosed()) {
            throw new RelationalException("Connection is closed!", ErrorCode.INTERNAL_ERROR).toSqlException();
        }
    }

    @Nonnull
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return iface.cast(this);
    }

    @Nonnull
    @Override
    public StatementBuilderFactory createStatementBuilderFactory() throws SQLException {
        return runIsolatedInTransactionIfPossible(() -> new StatementBuilderFactoryImpl(getSchemaTemplate(), this));
    }

    @Nonnull
    @Override
    public ExpressionFactory createExpressionBuilderFactory() throws SQLException {
        return runIsolatedInTransactionIfPossible(() -> new ExpressionFactoryImpl(getSchemaTemplate(), getOptions()));
    }

    /**
     * This method runs an operation isolated within a transaction. This means that if there is no active transaction,
     * then one is created to perform the operation. If a transaction is exclusively created for the operation, then
     * it is discarded after the operation is done. Since the transaction is discarded (not committed, rolled back),
     * the operation should be strictly read only.
     *
     * @param operation task to be performed within a transaction.
     * @return the value returned by the operation.
     * @param <T> return type of the operation.
     * @throws SQLException if the operation completes exceptionally, or if the transaction is not properly managed.
     */
    <T> T runIsolatedInTransactionIfPossible(Supplier<T> operation) throws SQLException {
        boolean newTransaction = false;
        SQLException exception = null;
        T result = null;
        try {
            newTransaction = ensureTransactionActive();
            result = operation.get();
        } catch (RelationalException e) {
            exception = e.toSqlException();
        }
        if (newTransaction) {
            try {
                rollbackInternal();
            } catch (SQLException sqle) {
                if (exception != null) {
                    exception.addSuppressed(sqle);
                } else {
                    exception = sqle;
                }
            }
        }
        if (exception != null) {
            throw exception;
        } else {
            return result;
        }
    }
}
