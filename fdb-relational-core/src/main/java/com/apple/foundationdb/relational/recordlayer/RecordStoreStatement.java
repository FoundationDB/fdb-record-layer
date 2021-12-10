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

import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.relational.api.*;
import com.apple.foundationdb.relational.api.exceptions.OperationUnsupportedException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.RecordQuery;
import com.google.common.base.Converter;
import com.google.common.base.Preconditions;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Field;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

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
    @Nonnull
    public RelationalResultSet executeQuery(@Nonnull Queryable query, @Nonnull Options options) throws RelationalException {
        ensureTransactionActive();

        String schem = query.getSchema();
        if (schem == null) {
            schem = conn.getSchema();
        }
        String[] schemaAndTable = getSchemaAndTable(schem, query.getTable());
        RecordLayerSchema schema = conn.frl.loadSchema(schemaAndTable[0], options);

        Table table = schema.loadTable(schemaAndTable[1], options);
        final Descriptors.Descriptor tableTypeDescriptor = table.getMetaData().getTableTypeDescriptor();
        final List<Descriptors.FieldDescriptor> fields = tableTypeDescriptor.getFields();
        List<String> queryColumns = query.getColumns();
        List<KeyExpression> queryCols = new ArrayList<>(queryColumns.size());

        for (String queryColumn : queryColumns) {
            Descriptors.FieldDescriptor fieldToUse = null;
            for (Descriptors.FieldDescriptor field : fields) {
                if (field.getName().equalsIgnoreCase(queryColumn)) {
                    fieldToUse = field;
                    break;
                }
            }
            if (fieldToUse == null) {
                throw new RelationalException("Invalid column for table. Table: <" + query.getTable() + ">,column: <" + queryColumn + ">", RelationalException.ErrorCode.INVALID_PARAMETER);
            }
            if (fieldToUse.getJavaType() == Descriptors.FieldDescriptor.JavaType.MESSAGE) {
                final Descriptors.Descriptor messageType = fieldToUse.getMessageType();
                List<Descriptors.FieldDescriptor> nestedFields = messageType.getFields();
                KeyExpression kE = Key.Expressions.concatenateFields(nestedFields.stream().map(Descriptors.FieldDescriptor::getName).collect(Collectors.toList()));
                queryCols.add(Key.Expressions.field(queryColumn).nest(kE));
            } else {
                queryCols.add(Key.Expressions.field(queryColumn));
            }
        }

        RecordQuery.Builder recQueryBuilder = RecordQuery.newBuilder().setRecordType(table.getName());
        if (queryCols.size() > 0) {
            recQueryBuilder.setRequiredResults(queryCols);
        }
        if (query.getWhereClause() != null) {
            recQueryBuilder.setFilter(WhereClauseUtils.convertClause(query.getWhereClause()));
        }

        final QueryScannable scannable = new QueryScannable(conn, conn.frl.loadSchema(query.getSchema(), options), recQueryBuilder.build(), queryColumns.toArray(new String[0]), query.isExplain());
        return new RecordLayerResultSet(scannable, null, null, conn, query.getQueryOptions());
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
        NestableTuple start = scan.getStartKey().isEmpty() ? null : keyBuilder.buildKey(scan.getStartKey(), true, true);
        NestableTuple end = scan.getEndKey().isEmpty() ? null : keyBuilder.buildKey(scan.getEndKey(), true, true);

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

        NestableTuple tuple = source.getKeyBuilder().buildKey(key.toMap(), true, true);

        final KeyValue keyValue = source.get(conn.transaction, tuple, queryProperties);
        return new KeyValueResultSet(keyValue, table.getFieldNames(), true);
    }

    @Override
    public int executeInsert(@Nonnull String tableName, @Nonnull Iterator<? extends Message> data, Options options) throws RelationalException {
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
        NestableTuple toDelete = source.getKeyBuilder().buildKey(keys.next().toMap(), true, true);
        int count = 0;
        RelationalException err = null;
        try {
            while (toDelete != null) {
                if (table.deleteRecord(toDelete)) {
                    count++;
                }
                toDelete = null;
                if (keys.hasNext()) {
                    toDelete = source.getKeyBuilder().buildKey(keys.next().toMap(), true, true);
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
