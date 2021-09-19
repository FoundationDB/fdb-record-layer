/*
 * RecordStoreStatement.java
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

import com.apple.foundationdb.relational.api.*;
import com.apple.foundationdb.relational.api.exceptions.OperationUnsupportedException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.google.common.base.Preconditions;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Set;

public class RecordStoreStatement implements Statement {
    private final RecordStoreConnection conn;

    public RecordStoreStatement(RecordStoreConnection conn) {
        this.conn = conn;
    }

    @Override
    public RelationalResultSet executeQuery(String query, Options options, QueryProperties queryProperties) throws RelationalException {
        throw new OperationUnsupportedException("No language is currently supported");
    }

    @Override
    public RelationalResultSet executeQuery(Queryable query, Options options, QueryProperties queryProperties) throws RelationalException {
        throw new OperationUnsupportedException("Not Implemented in the Relational layer");
    }

    @Override
    public @Nonnull
    RelationalResultSet executeScan(@Nonnull TableScan scan, @Nonnull Options options) throws RelationalException {
        ensureTransactionActive();

        String[] schemaAndTable = getSchemaAndTable(conn.getSchema(), scan.getTableName());
        RecordLayerSchema schema = conn.frl.loadSchema(schemaAndTable[0], options);

        Table table = schema.loadTable(schemaAndTable[1], options);

        Scannable source = getSourceScannable(options, table);

        final KeyBuilder keyBuilder = source.getKeyBuilder();
        NestableTuple start = keyBuilder.buildKey(scan.getStartKey(),true);
        NestableTuple end = keyBuilder.buildKey(scan.getEndKey(),true);

        return new RecordLayerResultSet(source, start, end, conn, scan.getScanProperties());
    }

    @Override
    public @Nonnull
    RelationalResultSet executeGet(@Nonnull String tableName, @Nonnull KeySet key, @Nonnull Options options, @Nonnull QueryProperties queryProperties) throws RelationalException {
        //check that the key is valid
        Preconditions.checkArgument(key.toMap().size() != 0, "Cannot perform a GET without specifying a key");
        ensureTransactionActive();

        String[] schemaAndTable = getSchemaAndTable(conn.getSchema(), tableName);
        RecordLayerSchema schema = conn.frl.loadSchema(schemaAndTable[0], options);

        Table table = schema.loadTable(schemaAndTable[1], options);

        Scannable source = getSourceScannable(options, table);

        NestableTuple tuple = source.getKeyBuilder().buildKey(key.toMap(),true);

        final KeyValue keyValue = source.get(conn.transaction, tuple, queryProperties);
        return new KeyValueResultSet(keyValue, table.getFieldNames(), true);
    }

    @Override
    public int executeInsert(@Nonnull String tableName, @Nonnull Iterator<Message> data, Options options) throws RelationalException {
        //do this check first because otherwise we might start an expensive transaction that does nothing
        if (!data.hasNext()) {
            return 0;
        }

        ensureTransactionActive();

        String[] schemaAndTable = getSchemaAndTable(conn.getSchema(), tableName);
        RecordLayerSchema schema = conn.frl.loadSchema(schemaAndTable[0], options);

        Table table = schema.loadTable(schemaAndTable[1], options);

        int rowCount = 0;
        RelationalException err = null;
        try {
            while (data.hasNext()) {
                Message message = data.next();
                if (table.insertRecord(message)) {
                    rowCount++;
                }
            }
            if (conn.isAutoCommitEnabled()) {
                conn.commit();
            }

        } catch (RuntimeException re) {
            err = RelationalException.convert(re);
            if (conn.isAutoCommitEnabled()) {
                try {
                    conn.rollback();
                } catch (RelationalException ve) {
                    err.addSuppressed(ve);
                }
            }
        }

        if (err != null) {
            throw err;
        }
        return rowCount;
    }

    @Override
    public int executeDelete(@Nonnull String tableName, @Nonnull Iterator<KeySet> keys, Options options) throws RelationalException {
        if (!keys.hasNext()) {
            return 0;
        }

        ensureTransactionActive();
        String[] schemaAndTable = getSchemaAndTable(conn.getSchema(), tableName);
        RecordLayerSchema schema = conn.frl.loadSchema(schemaAndTable[0], options);

        Table table = schema.loadTable(schemaAndTable[1], options);

        Scannable source = getSourceScannable(options, table);
        NestableTuple toDelete = source.getKeyBuilder().buildKey(keys.next().toMap(),true);
        int count = 0;
        RelationalException err = null;
        try {
            while (toDelete != null) {
                if (table.deleteRecord(toDelete)) {
                    count++;
                }
                toDelete = null;
                if (keys.hasNext()) {
                    toDelete = source.getKeyBuilder().buildKey(keys.next().toMap(),true);
                }
            }
            if (conn.isAutoCommitEnabled()) {
                conn.commit();
            }
        } catch (RuntimeException re) {
            err = RelationalException.convert(re);
            if (conn.isAutoCommitEnabled()) {
                try {
                    conn.rollback();
                } catch (RelationalException ve) {
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
    public Continuation getContinuation() {
        throw new OperationUnsupportedException("Not Implemented in the Relational layer");
    }

    @Override
    public void close() throws RelationalException {
        //TODO(bfines) implement
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void ensureTransactionActive() {
        if (!conn.inActiveTransaction()) {
            if (conn.isAutoCommitEnabled()) {
                conn.beginTransaction();
            } else {
                throw new RelationalException("Transaction not begun", RelationalException.ErrorCode.TRANSACTION_INACTIVE);
            }
        }
    }

    private String[] getSchemaAndTable(@Nullable String schemaName, @Nonnull String tableName) {
        String schema = schemaName;
        String tableN = tableName;
        if (schema == null) {
            //look for the schema in the table name
            String[] t = tableName.split("\\.");
            if (t.length != 2) {
                throw new RelationalException("Invalid table format", RelationalException.ErrorCode.CANNOT_CONVERT_TYPE);
            }
            schema = t[0];
            tableN = t[1];
        }

        return new String[]{schema, tableN};
    }


    private @Nonnull Scannable getSourceScannable(@Nonnull Options options, @Nonnull Table table) {
        Scannable source;
        String indexName = options.getOption(OperationOption.INDEX_HINT_NAME, null);
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
                throw new RelationalException("Unknown index: <" + indexName + "> on type <" + table.getName() + ">", RelationalException.ErrorCode.UNKNOWN_INDEX);
            }
            source = index;
        } else {
            source = table;
        }
        return source;
    }

}
