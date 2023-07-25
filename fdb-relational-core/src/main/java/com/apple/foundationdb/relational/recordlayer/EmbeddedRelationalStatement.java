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
import com.apple.foundationdb.relational.recordlayer.query.PlanContext;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EmbeddedRelationalStatement extends AbstractEmbeddedStatement implements RelationalStatement {

    public EmbeddedRelationalStatement(@Nonnull EmbeddedRelationalConnection conn) {
        super(conn);
    }

    @Override
    PlanContext buildPlanContext(FDBRecordStoreBase<Message> store) throws RelationalException {
        return PlanContext.Builder.create()
                .fromRecordStore(store)
                .fromDatabase(conn.getRecordLayerDatabase())
                .withMetricsCollector(conn.metricCollector)
                .withSchemaTemplate(conn.getSchemaTemplate())
                .build();
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        try {
            return executeInternal(sql);
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
    }

    @Override
    public RelationalResultSet executeQuery(String sql) throws SQLException {
        if (execute(sql)) {
            return currentResultSet;
        } else {
            throw new SQLException(String.format("query '%s' does not return result set, use JDBC executeUpdate method instead", sql), ErrorCode.NO_RESULT_SET.getErrorCode());
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkOpen();
        if (execute(sql)) {
            throw new SQLException(String.format("query '%s' returns a result set, use JDBC executeQuery method instead", sql), ErrorCode.EXECUTE_UPDATE_RETURNED_RESULT_SET.getErrorCode());
        }
        return currentRowCount;
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
        } catch (RelationalException | SQLException | RuntimeException ex) {
            if (conn.getAutoCommit()) {
                try {
                    conn.rollback();
                } catch (SQLException e) {
                    e.addSuppressed(ex);
                    throw e;
                }
            }
            throw ExceptionUtil.toRelationalException(ex).toSqlException();
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
        } catch (RelationalException | SQLException | RuntimeException ex) {
            if (conn.getAutoCommit()) {
                try {
                    conn.rollback();
                } catch (SQLException e) {
                    e.addSuppressed(ex);
                    throw e;
                }
            }
            throw ExceptionUtil.toRelationalException(ex).toSqlException();
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
        } catch (RelationalException | RuntimeException ex) {
            throw ExceptionUtil.toRelationalException(ex).toSqlException();
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
        } catch (RelationalException | RuntimeException e) {
            throw ExceptionUtil.toRelationalException(e).toSqlException();
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

            int rowCount = 0;
            while (data.hasNext()) {
                Message message = data.next();
                if (table.insertRecord(message, replaceOnDuplicate != null && replaceOnDuplicate)) {
                    rowCount++;
                }
            }
            if (conn.getAutoCommit()) {
                conn.commit();
            }
            return rowCount;
        } catch (RelationalException | SQLException | RuntimeException ex) {
            if (conn.getAutoCommit()) {
                try {
                    conn.rollback();
                } catch (SQLException e) {
                    e.addSuppressed(ex);
                    throw e;
                }
            }
            throw ExceptionUtil.toRelationalException(ex).toSqlException();
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

            int rowCount = 0;
            for (RelationalStruct struct : data) {
                if (table.insertRecord(struct, replaceOnDuplicate != null && replaceOnDuplicate)) {
                    rowCount++;
                }
            }
            if (conn.getAutoCommit()) {
                conn.commit();
            }
            return rowCount;
        } catch (RelationalException | SQLException | RuntimeException ex) {
            if (conn.getAutoCommit()) {
                try {
                    conn.rollback();
                } catch (SQLException e) {
                    e.addSuppressed(ex);
                    throw e;
                }
            }
            throw ExceptionUtil.toRelationalException(ex).toSqlException();
        }
    }

    @Override
    public int executeDelete(@Nonnull String tableName, @Nonnull Iterator<KeySet> keys, @Nonnull Options options) throws SQLException {
        try {
            checkOpen();
            conn.ensureTransactionActive();
            options = Options.combine(conn.getOptions(), options);
            if (!keys.hasNext()) {
                return 0;
            }

            String[] schemaAndTable = getSchemaAndTable(conn.getSchema(), tableName);
            RecordLayerSchema schema = conn.frl.loadSchema(schemaAndTable[0]);

            Table table = schema.loadTable(schemaAndTable[1]);
            table.validateTable(options);

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
            if (conn.getAutoCommit()) {
                conn.commit();
            }
            return count;
        } catch (RelationalException | SQLException | RuntimeException ex) {
            if (conn.getAutoCommit()) {
                try {
                    conn.rollback();
                } catch (SQLException e) {
                    e.addSuppressed(ex);
                    throw e;
                }
            }
            throw ExceptionUtil.toRelationalException(ex).toSqlException();
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
                    if (conn.getAutoCommit()) {
                        conn.commit();
                    }
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
                if (conn.getAutoCommit()) {
                    conn.commit();
                }
            }
        } catch (RelationalException | SQLException | RuntimeException ex) {
            if (conn.getAutoCommit()) {
                try {
                    conn.rollback();
                } catch (SQLException e) {
                    e.addSuppressed(ex);
                    throw e;
                }
            }
            throw ExceptionUtil.toRelationalException(ex).toSqlException();
        }
    }

    @Override
    public void close() throws SQLException {
        try {
            if (currentResultSet != null) {
                currentResultSet.close();
            }
            closed = true;
        } catch (RuntimeException ex) {
            if (conn.getAutoCommit()) {
                try {
                    conn.rollback();
                } catch (SQLException e) {
                    e.addSuppressed(ex);
                    throw e;
                }
            }
            throw ExceptionUtil.toRelationalException(ex).toSqlException();
        }
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
}
