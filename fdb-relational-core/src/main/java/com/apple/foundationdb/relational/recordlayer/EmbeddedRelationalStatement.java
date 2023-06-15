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

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.DynamicMessageBuilder;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.RelationalStruct;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metrics.RelationalMetric;
import com.apple.foundationdb.relational.recordlayer.query.Plan;
import com.apple.foundationdb.relational.recordlayer.query.PlanContext;
import com.apple.foundationdb.relational.recordlayer.query.PlanGenerator;
import com.apple.foundationdb.relational.recordlayer.query.QueryPlan;
import com.apple.foundationdb.relational.recordlayer.util.Assert;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class EmbeddedRelationalStatement implements RelationalStatement {

    @Nonnull
    private final EmbeddedRelationalConnection conn;

    @Nullable
    private RelationalResultSet currentResultSet;
    private boolean closed;

    private int currentRowCount;

    public EmbeddedRelationalStatement(@Nonnull final EmbeddedRelationalConnection conn) {
        this.conn = conn;
    }

    @Nonnull
    private Optional<Pair<RelationalResultSet, Boolean>> executeQueryInternal(@Nonnull String query) throws RelationalException {
        conn.ensureTransactionActive();
        Options options = conn.getOptions();
        if (conn.getSchema() == null) {
            throw new RelationalException("No Schema specified", ErrorCode.UNDEFINED_SCHEMA);
        }
        try (var schema = conn.getRecordLayerDatabase().loadSchema(conn.getSchema())) {
            final var store = schema.loadStore().unwrap(FDBRecordStoreBase.class);
            final var planGenerator = PlanGenerator.of(conn.frl.getPlanCache() == null ? Optional.empty() : Optional.of(conn.frl.getPlanCache()),
                    store.getRecordMetaData(), store.getRecordStoreState(), options);
            final var planContext = PlanContext.Builder.create()
                    .fromRecordStore(store)
                    .fromDatabase(conn.getRecordLayerDatabase())
                    .withMetricsCollector(conn.metricCollector)
                    .withSchemaTemplate(conn.getSchemaTemplate())
                    .build();
            final Plan<?> plan = planGenerator.getPlan(query, planContext);
            final var executionContext = Plan.ExecutionContext.of(conn.transaction, options, conn, conn.metricCollector);
            if (plan instanceof QueryPlan) {
                final RelationalResultSet executeResultSet = ((QueryPlan) plan).execute(executionContext);
                return Optional.of(Pair.of(executeResultSet, plan.isUpdatePlan()));
            } else {
                plan.execute(executionContext);
                return Optional.empty();
            }
        }
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        try {
            checkOpen();
            Assert.notNull(sql);
            conn.ensureTransactionActive();
            final var resultSet = conn.metricCollector.clock(RelationalMetric.RelationalEvent.TOTAL_PROCESS_QUERY, () -> executeQueryInternal(sql));
            if (resultSet.isPresent()) {
                var pair = resultSet.get();
                if (!pair.getRight()) {
                    //result set statements get a -1 for update count
                    currentRowCount = -1;
                    currentResultSet = new ErrorCapturingResultSet(pair.getLeft());
                    return true;
                } else {
                    //this is an update statement, so generate the row count and set the update clause
                    currentResultSet = null;
                    currentRowCount = countUpdates(pair.getLeft());
                    pair.getLeft().close();
                    return false;
                }
            } else {
                currentResultSet = null;
                //ddl statements are updates that don't return results, so they get 0 for row count
                currentRowCount = 0;
                if (getConnection().getAutoCommit()) {
                    getConnection().commit();
                }
                return false;
            }
        } catch (RelationalException ve) {
            if (getConnection().getAutoCommit()) {
                try {
                    getConnection().rollback();
                } catch (SQLException se) {
                    ve.addSuppressed(se);
                }
            }
            throw ve.toSqlException();
        }
    }

    @Override
    public RelationalResultSet executeQuery(String sql) throws SQLException {
        if (execute(sql)) {
            return currentResultSet;
        } else {
            throw new SQLException(String.format("query '%s' does not return result set, use JDBC executeUpdate method instead", sql), ErrorCode.INVALID_PARAMETER.getErrorCode());
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkOpen();
        if (execute(sql)) {
            throw new SQLException(String.format("query '%s' returns a result set, use JDBC executeQuery method instead", sql));
        }
        return currentRowCount;
    }

    @Override
    public RelationalResultSet getResultSet() throws SQLException {
        checkOpen();
        if (currentResultSet != null && !currentResultSet.isClosed()) {
            var resultSet = currentResultSet;
            currentResultSet = null;
            return resultSet;
        }
        throw new SQLException("no open result set available");
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkOpen();
        if (currentResultSet != null) {
            return -1;
        } else {
            return currentRowCount; // current spec.
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        checkOpen();
        return conn;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public @Nonnull RelationalResultSet executeScan(@Nonnull String tableName, @Nonnull KeySet prefix, @Nonnull Options options) throws SQLException {
        try {
            checkOpen();
            conn.ensureTransactionActive();
            options = Options.combine(conn.getOptions(), options);

            String[] schemaAndTable = getSchemaAndTable(conn.getSchema(), tableName);
            RecordLayerSchema schema = conn.frl.loadSchema(schemaAndTable[0]);

            Table table = schema.loadTable(schemaAndTable[1]);

            String indexName = options.getOption(Options.Name.INDEX_HINT);
            DirectScannable source = getSourceScannable(indexName, table);

            KeyBuilder keyBuilder = source.getKeyBuilder();
            Row row = keyBuilder.buildKey(prefix.toMap(), false);

            StructMetaData sourceMetaData = source.getMetaData();
            return new ErrorCapturingResultSet(new RecordLayerResultSet(sourceMetaData,
                    source.openScan(row, options), conn));
        } catch (RelationalException e) {
            if (getConnection().getAutoCommit()) {
                conn.rollback();
            }
            throw e.toSqlException();
        }
    }

    @Override
    public @Nonnull
    RelationalResultSet executeGet(@Nonnull String tableName, @Nonnull KeySet key, @Nonnull Options options) throws SQLException {
        try {
            checkOpen();
            options = Options.combine(conn.getOptions(), options);

            conn.ensureTransactionActive();

            String[] schemaAndTable = getSchemaAndTable(conn.getSchema(), tableName);
            RecordLayerSchema schema = conn.frl.loadSchema(schemaAndTable[0]);

            Table table = schema.loadTable(schemaAndTable[1]);

            String indexName = options.getOption(Options.Name.INDEX_HINT);
            DirectScannable source = getSourceScannable(indexName, table);
            source.validate(options);

            Row tuple = source.getKeyBuilder().buildKey(key.toMap(), true);

            final Row row = source.get(conn.transaction, tuple, options);

            final Iterator<Row> rowIter = row == null ? Collections.emptyIterator() : Collections.singleton(row).iterator();
            return new ErrorCapturingResultSet(new IteratorResultSet(table.getMetaData(), rowIter, 0));
        } catch (RelationalException e) {
            if (getConnection().getAutoCommit()) {
                conn.rollback();
            }
            throw e.toSqlException();
        }
    }

    @Override
    public DynamicMessageBuilder getDataBuilder(@Nonnull String tableName) throws SQLException {
        try {
            checkOpen();
            conn.ensureTransactionActive();
            String[] schemaAndTable = getSchemaAndTable(conn.getSchema(), tableName);
            RecordLayerSchema schema = conn.frl.loadSchema(schemaAndTable[0]);
            return schema.getDataBuilder(schemaAndTable[1]);
        } catch (RelationalException e) {
            if (getConnection().getAutoCommit()) {
                conn.rollback();
            }
            throw e.toSqlException();
        }
    }

    @Override
    @Nonnull
    public DynamicMessageBuilder getDataBuilder(@Nonnull String maybeQualifiedTableName, @Nonnull final List<String> nestedFields) throws SQLException {
        try {
            checkOpen();
            conn.ensureTransactionActive();
            String[] schemaAndTable = getSchemaAndTable(conn.getSchema(), maybeQualifiedTableName);
            RecordLayerSchema schema = conn.frl.loadSchema(schemaAndTable[0]);
            final var typeAccessor = new java.util.ArrayList<>(List.of(schemaAndTable[1]));
            typeAccessor.addAll(nestedFields);
            return schema.getDataBuilder(String.join(".", typeAccessor));
        } catch (RelationalException e) {
            if (getConnection().getAutoCommit()) {
                conn.rollback();
            }
            throw e.toSqlException();
        }
    }

    @Override
    public int executeInsert(@Nonnull String tableName, @Nonnull Iterator<? extends Message> data, @Nonnull Options options) throws SQLException {
        try {
            checkOpen();
            options = Options.combine(conn.getOptions(), options);
            //do this check first because otherwise we might start an expensive transaction that does nothing
            if (!data.hasNext()) {
                return 0;
            }

            conn.ensureTransactionActive();

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
        } catch (RelationalException e) {
            if (getConnection().getAutoCommit()) {
                conn.rollback();
            }
            throw e.toSqlException();
        }
    }

    @Override
    public int executeInsert(@Nonnull String tableName, @Nonnull List<RelationalStruct> data, @Nonnull Options options)
            throws SQLException {
        try {
            checkOpen();
            options = Options.combine(conn.getOptions(), options);
            //do this check first because otherwise we might start an expensive transaction that does nothing
            if (data.isEmpty()) {
                return 0;
            }

            conn.ensureTransactionActive();

            String[] schemaAndTable = getSchemaAndTable(conn.getSchema(), tableName);
            RecordLayerSchema schema = conn.frl.loadSchema(schemaAndTable[0]);

            Table table = schema.loadTable(schemaAndTable[1]);
            table.validateTable(options);
            final Boolean replaceOnDuplicate = options.getOption(Options.Name.REPLACE_ON_DUPLICATE_PK);

            return executeMutation(() -> {
                int rowCount = 0;
                for (RelationalStruct struct : data) {
                    if (table.insertRecord(struct, replaceOnDuplicate != null && replaceOnDuplicate)) {
                        rowCount++;
                    }
                }
                return rowCount;
            });
        } catch (RelationalException e) {
            if (getConnection().getAutoCommit()) {
                conn.rollback();
            }
            throw e.toSqlException();
        }
    }

    @Override
    public int executeDelete(@Nonnull String tableName, @Nonnull Iterator<KeySet> keys, @Nonnull Options options) throws SQLException {
        try {
            checkOpen();
            options = Options.combine(conn.getOptions(), options);
            if (!keys.hasNext()) {
                return 0;
            }

            conn.ensureTransactionActive();
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
        } catch (RelationalException e) {
            if (getConnection().getAutoCommit()) {
                conn.rollback();
            }
            throw e.toSqlException();
        }
    }

    @Override
    @SuppressWarnings("PMD.PreserveStackTrace") // intentional - Fall back for Invalid Range Exception from Record Layer
    public void executeDeleteRange(@Nonnull String tableName, @Nonnull KeySet prefix, @Nonnull Options options) throws SQLException {
        try {
            checkOpen();
            conn.ensureTransactionActive();
            options = Options.combine(conn.getOptions(), options);

            String[] schemaAndTable = getSchemaAndTable(conn.getSchema(), tableName);
            RecordLayerSchema schema = conn.frl.loadSchema(schemaAndTable[0]);
            Table table = schema.loadTable(schemaAndTable[1]);
            table.validateTable(options);

            Map<String, Object> deletePrefixColumns = prefix.toMap();
            KeyBuilder keyBuilder = table.getKeyBuilder();
            Row row = keyBuilder.buildKey(deletePrefixColumns, false);
            int keyLength = row.getNumFields();
            if (row.getNumFields() == keyBuilder.getKeySize()) {
                if (row.getObject(keyLength - 1) != null) {
                    // We have a complete key. Delete only the one record
                    table.deleteRecord(row);
                    return;
                }
            }
            try {
                table.deleteRange(deletePrefixColumns);
            } catch (Query.InvalidExpressionException ex) {
                // To work around a record layer limitation, we execute point deletes at this point if we cannot execute a range delete
                // This may be caused by the fact that an index does not share the same prefix as the table we're trying to range delete from
                Continuation continuation = ContinuationImpl.BEGIN;
                ResumableIterator<Row> scannedRows;
                do {
                    Options newOptions = Options.combine(options, Options.builder().withOption(Options.Name.CONTINUATION, continuation).build());
                    scannedRows = table.openScan(row, newOptions);
                    while (scannedRows.hasNext()) {
                        Row scannedRow = scannedRows.next();
                        if (!table.deleteRecord(keyBuilder.buildKey(scannedRow))) {
                            throw new RelationalException("Cannot delete record during fallback deleteRange", ErrorCode.INTERNAL_ERROR);
                        }
                    }
                    continuation = scannedRows.getContinuation();
                } while (scannedRows.terminatedEarly());
            }
        } catch (RelationalException e) {
            if (getConnection().getAutoCommit()) {
                conn.rollback();
            }
            throw e.toSqlException();
        }
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
        if (currentResultSet != null) {
            currentResultSet.close();
        }
        closed = true;
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/

    // TODO (yhatem) this should be refactored and cleaned up, ideally consumers should work with structured metadata API
    //               instead of this string processing since that is error-prone and somewhat very low-level.
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

    private void checkOpen() throws SQLException {
        if (closed) {
            throw new RelationalException("Statement closed", ErrorCode.STATEMENT_CLOSED).toSqlException();
        }
    }

    private int countUpdates(@Nonnull RelationalResultSet resultSet) throws RelationalException, SQLException {
        /*
         * This is a bit of a temporary hack both to address a bug(TODO), and also to get around the
         * way that RecordLayer DML plans are executed.
         *
         * The return of a record layer plan is _always_ a ResultSet, even when it's a straight insert operation.
         * For DML operations the result set contains the rows that were written, which makes sense from a planning
         * and execution perspective but is nearly useless to us at this stage, where all we want to know
         * is the number of records mutated. To get that result, we have to quickly process the returned result
         * set and count the rows returned. It's a tad expensive, but (typically) should be easy enough since
         * they'll be held in memory.
         *
         * The returned value is an int because that's what JDBC expects, but also because getting more than
         * Integer.MAX_VALUE results into FDB is currently(as of Spring 2023) impossible.
         *
         */
        int count = 0;
        while (resultSet.next()) {
            count++;
        }
        return count;
    }

}
