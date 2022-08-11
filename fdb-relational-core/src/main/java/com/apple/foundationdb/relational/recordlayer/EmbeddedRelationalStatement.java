/*
 * EmbeddedRelationalStatement.java
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

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.relational.api.DynamicMessageBuilder;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.TableScan;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.query.Plan;
import com.apple.foundationdb.relational.recordlayer.query.PlanContext;
import com.apple.foundationdb.relational.recordlayer.query.QueryPlan;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;
import com.apple.foundationdb.relational.recordlayer.utils.Assert;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

public class EmbeddedRelationalStatement implements RelationalStatement {

    @Nonnull
    private final EmbeddedRelationalConnection conn;

    @Nullable
    private RelationalResultSet currentResultSet;

    public EmbeddedRelationalStatement(@Nonnull final EmbeddedRelationalConnection conn) {
        this.conn = conn;
    }

    private Optional<RelationalResultSet> executeQueryInternal(@Nonnull String query,
                                                             @Nonnull Options options) throws RelationalException, SQLException {
        ensureTransactionActive();
        if (conn.getSchema() == null) {
            throw new RelationalException("No Schema specified", ErrorCode.UNDEFINED_SCHEMA);
        }
        final FDBRecordStore store = conn.getRecordLayerDatabase().loadSchema(conn.getSchema()).loadStore();
        final var planContext = PlanContext.Builder.create().fromRecordStore(store).fromDatabase(conn.getRecordLayerDatabase()).build();
        final Plan<?> plan = Plan.generate(query, planContext);
        final var executionContext = Plan.ExecutionContext.of(conn.transaction, options, conn);
        if (plan instanceof QueryPlan) {
            return Optional.of(((QueryPlan) plan).execute(executionContext));
        } else {
            plan.execute(executionContext);
            return Optional.empty();
        }
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        try {
            Assert.notNull(sql);
            Optional<RelationalResultSet> resultSet = executeQueryInternal(sql, Options.NONE);
            if (resultSet.isPresent()) {
                currentResultSet = resultSet.get();
                return true;
            } else {
                currentResultSet = null;
                if (getConnection().getAutoCommit()) {
                    getConnection().commit();
                }
                return false;
            }
        } catch (RelationalException ve) {
            throw ve.toSqlException();
        }
    }

    @Override
    public RelationalResultSet executeQuery(String sql) throws SQLException {
        try {
            Assert.notNull(sql);
            Optional<RelationalResultSet> resultSet = executeQueryInternal(sql, Options.NONE);
            if (resultSet.isPresent()) {
                return resultSet.get();
            } else {
                throw new SQLException(String.format("query '%s' does not return result set, use JDBC executeUpdate method instead", sql), ErrorCode.INVALID_PARAMETER.getErrorCode());
            }
        } catch (RelationalException ve) {
            throw ve.toSqlException();
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        try {
            Assert.notNull(sql);
            Optional<RelationalResultSet> resultSet = executeQueryInternal(sql, Options.NONE);
            if (resultSet.isEmpty()) {
                if (getConnection().getAutoCommit()) {
                    getConnection().commit();
                }
                return 0; // todo improve
            } else {
                throw new SQLException(String.format("query '%s' returns a result set, use JDBC executeQuery method instead", sql));
            }
        } catch (RelationalException ve) {
            throw ve.toSqlException();
        }
    }

    @Override
    public RelationalResultSet getResultSet() throws SQLException {
        if (currentResultSet != null /*&& !currentResultSet.isClosed()  todo implement this*/) {
            return currentResultSet;
        }
        throw new SQLException("no open result set available");
    }

    @Override
    public int getUpdateCount() throws SQLException {
        if (currentResultSet != null) {
            return -1;
        } else {
            return 0; // current spec.
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        return conn;
    }

    @Override
    public @Nonnull
    RelationalResultSet executeScan(@Nonnull TableScan scan, @Nonnull Options options) throws RelationalException {
        ensureTransactionActive();
        options = Options.combine(conn.getOptions(), options);

        String[] schemaAndTable = getSchemaAndTable(conn.getSchema(), scan.getTableName());
        RecordLayerSchema schema = conn.frl.loadSchema(schemaAndTable[0]);

        Table table = schema.loadTable(schemaAndTable[1]);

        String indexName = options.getOption(Options.Name.INDEX_HINT);
        DirectScannable source = getSourceScannable(indexName, table);

        final KeyBuilder keyBuilder = source.getKeyBuilder();
        Row start = scan.getStartKey().isEmpty() ? null : keyBuilder.buildKey(scan.getStartKey(), false);
        Row end = scan.getEndKey().isEmpty() ? null : keyBuilder.buildKey(scan.getEndKey(), false);

        StructMetaData sourceMetaData = source.getMetaData();
        return new RecordLayerResultSet(sourceMetaData,
                source.openScan(conn.transaction, start, end, options), conn);
    }

    @Override
    public @Nonnull
    RelationalResultSet executeGet(@Nonnull String tableName, @Nonnull KeySet key, @Nonnull Options options) throws RelationalException {
        options = Options.combine(conn.getOptions(), options);

        ensureTransactionActive();

        String[] schemaAndTable = getSchemaAndTable(conn.getSchema(), tableName);
        RecordLayerSchema schema = conn.frl.loadSchema(schemaAndTable[0]);

        Table table = schema.loadTable(schemaAndTable[1]);

        String indexName = options.getOption(Options.Name.INDEX_HINT);
        DirectScannable source = getSourceScannable(indexName, table);
        source.validate(options);

        Row tuple = source.getKeyBuilder().buildKey(key.toMap(), true);

        final Row row = source.get(conn.transaction, tuple, options);

        final Iterator<Row> rowIter = row == null ? Collections.emptyIterator() : Collections.singleton(row).iterator();
        return new IteratorResultSet(table.getMetaData(), rowIter, 0);
    }

    @Override
    public DynamicMessageBuilder getDataBuilder(@Nonnull String typeName) throws RelationalException {
        ensureTransactionActive();
        String[] schemaAndTable = getSchemaAndTable(conn.getSchema(), typeName);
        RecordLayerSchema schema = conn.frl.loadSchema(schemaAndTable[0]);
        return schema.getDataBuilder(schemaAndTable[1]);
    }

    @Override
    public int executeInsert(@Nonnull String tableName, @Nonnull Iterator<? extends Message> data, @Nonnull Options options) throws RelationalException {
        options = Options.combine(conn.getOptions(), options);
        //do this check first because otherwise we might start an expensive transaction that does nothing
        if (!data.hasNext()) {
            return 0;
        }

        ensureTransactionActive();

        String[] schemaAndTable = getSchemaAndTable(conn.getSchema(), tableName);
        RecordLayerSchema schema = conn.frl.loadSchema(schemaAndTable[0]);

        Table table = schema.loadTable(schemaAndTable[1]);
        table.validateTable(options);
        final Boolean replaceOnDuplicate = options.getOption(Options.Name.REPLACE_ON_DUPLICATE_PK);

        return executeMutation(() -> {
            int rowCount = 0;
            while (data.hasNext()) {
                Message message = data.next();
                if (table.insertRecord(message, replaceOnDuplicate != null && replaceOnDuplicate)) {
                    rowCount++;
                }
            }
            return rowCount;
        });
    }

    @Override
    public int executeDelete(@Nonnull String tableName, @Nonnull Iterator<KeySet> keys, @Nonnull Options options) throws RelationalException {
        options = Options.combine(conn.getOptions(), options);
        if (!keys.hasNext()) {
            return 0;
        }

        ensureTransactionActive();
        String[] schemaAndTable = getSchemaAndTable(conn.getSchema(), tableName);
        RecordLayerSchema schema = conn.frl.loadSchema(schemaAndTable[0]);

        Table table = schema.loadTable(schemaAndTable[1]);
        table.validateTable(options);

        return executeMutation(() -> {
            int count = 0;
            Row toDelete = table.getKeyBuilder().buildKey(keys.next().toMap(), true);
            while (toDelete != null) {
                if (table.deleteRecord(toDelete)) {
                    count++;
                }
                toDelete = null;
                if (keys.hasNext()) {
                    toDelete = table.getKeyBuilder().buildKey(keys.next().toMap(), true);
                }
            }
            return count;
        });
    }

    private interface Mutation {
        int execute() throws SQLException, RelationalException;
    }

    private int executeMutation(Mutation mutation) throws RelationalException {
        int count = 0;
        RelationalException err = null;
        try {
            count = mutation.execute();
            if (conn.getAutoCommit()) {
                conn.commit();
            }
        } catch (RuntimeException | RelationalException | SQLException re) {
            err = ExceptionUtil.toRelationalException(re);
            if (conn.getAutoCommit()) {
                try {
                    conn.rollback();
                } catch (SQLException ve) {
                    err.addSuppressed(ve);
                }
            }
        }
        if (err != null) {
            throw err;
        }
        return count;
    }

    @Override
    public void close() throws SQLException {
        //TODO(bfines) implement
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void ensureTransactionActive() throws RelationalException {
        if (!conn.inActiveTransaction()) {
            if (conn.getAutoCommit()) {
                conn.beginTransaction();
            } else {
                throw new RelationalException("Transaction not begun", ErrorCode.TRANSACTION_INACTIVE);
            }
        }
    }

    private String[] getSchemaAndTable(@Nullable String schemaName, @Nonnull String tableName) throws RelationalException {
        String schema = schemaName;
        String tableN = tableName;
        if (tableName.contains(".")) {
            String[] t = tableName.split("\\.");
            schema = t[0];
            tableN = t[1];
        }
        if (schema == null) {
            throw new RelationalException("Invalid table format", ErrorCode.INVALID_PARAMETER);
        }

        return new String[]{schema, tableN};
    }

    private @Nonnull DirectScannable getSourceScannable(String indexName, @Nonnull Table table) throws RelationalException {
        if (indexName != null) {
            Index index = null;
            final Set<Index> readableIndexes = table.getAvailableIndexes();
            for (Index idx : readableIndexes) {
                if (idx.getName().equals(indexName)) {
                    index = idx;
                    break;
                }
            }
            if (index == null) {
                throw new RelationalException("Unknown index: <" + indexName + "> on type <" + table.getName() + ">", ErrorCode.UNDEFINED_INDEX);
            }
            return index;
        } else {
            return table;
        }
    }

}
