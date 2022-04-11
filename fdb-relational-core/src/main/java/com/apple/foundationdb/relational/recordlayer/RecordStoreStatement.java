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

import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.OperationOption;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.QueryProperties;
import com.apple.foundationdb.relational.api.Queryable;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.TableScan;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

import com.google.common.base.Preconditions;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class RecordStoreStatement implements RelationalStatement {
    private final RecordStoreConnection conn;

    public RecordStoreStatement(RecordStoreConnection conn) {
        this.conn = conn;
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
                throw new RelationalException("Invalid column for table. Table: <" + query.getTable() + ">,column: <" + queryColumn + ">", ErrorCode.INVALID_PARAMETER);
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
        if (!queryCols.isEmpty()) {
            recQueryBuilder.setRequiredResults(queryCols);
        }
        if (query.getWhereClause() != null) {
            recQueryBuilder.setFilter(WhereClauseUtils.convertClause(query.getWhereClause()));
        }

        final QueryScannable scannable = new QueryScannable(conn.frl.loadSchema(schemaAndTable[0], options), recQueryBuilder.build(), queryColumns.toArray(new String[0]), query.isExplain());
        return new RecordLayerResultSet(scannable, null, null, conn, query.getQueryOptions(), options.getOption(OperationOption.CONTINUATION_NAME, null));
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
        Row start = scan.getStartKey().isEmpty() ? null : keyBuilder.buildKey(scan.getStartKey(), true, true);
        Row end = scan.getEndKey().isEmpty() ? null : keyBuilder.buildKey(scan.getEndKey(), true, true);

        return new RecordLayerResultSet(source, start, end, conn, scan.getScanProperties(),
                options.getOption(OperationOption.CONTINUATION_NAME, null));
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

        Row tuple = source.getKeyBuilder().buildKey(key.toMap(), true, true);

        final Row row = source.get(conn.transaction, tuple, queryProperties);

        Iterable<Row> iterable = toIterable(row == null ? Collections.emptyIterator() : List.of(row).iterator());
        Scannable scannable = new IterableScannable<>(iterable, Function.identity(), new String[]{}, table.getFieldNames());
        return new RecordLayerResultSet(
                scannable,
                new EmptyTuple(),
                new EmptyTuple(),
                conn,
                queryProperties,
                options.getOption(OperationOption.CONTINUATION_NAME, null));
    }

    @Override
    public int executeInsert(@Nonnull String tableName, @Nonnull Iterator<? extends Message> data, @Nonnull Options options) throws RelationalException {
        //do this check first because otherwise we might start an expensive transaction that does nothing
        if (!data.hasNext()) {
            return 0;
        }

        ensureTransactionActive();

        String[] schemaAndTable = getSchemaAndTable(conn.getSchema(), tableName);
        RecordLayerSchema schema = conn.frl.loadSchema(schemaAndTable[0], options);

        Table table = schema.loadTable(schemaAndTable[1], options);

        return executeMutation(() -> {
            int rowCount = 0;
            while (data.hasNext()) {
                Message message = data.next();
                if (table.insertRecord(message)) {
                    rowCount++;
                }
            }
            return rowCount;
        });
    }

    @Override
    public int executeDelete(@Nonnull String tableName, @Nonnull Iterator<KeySet> keys, @Nonnull Options options) throws RelationalException {
        if (!keys.hasNext()) {
            return 0;
        }

        ensureTransactionActive();
        String[] schemaAndTable = getSchemaAndTable(conn.getSchema(), tableName);
        RecordLayerSchema schema = conn.frl.loadSchema(schemaAndTable[0], options);

        Table table = schema.loadTable(schemaAndTable[1], options);

        Scannable source = getSourceScannable(options, table);
        return executeMutation(() -> {
            int count = 0;
            Row toDelete = source.getKeyBuilder().buildKey(keys.next().toMap(), true, true);
            while (toDelete != null) {
                if (table.deleteRecord(toDelete)) {
                    count++;
                }
                toDelete = null;
                if (keys.hasNext()) {
                    toDelete = source.getKeyBuilder().buildKey(keys.next().toMap(), true, true);
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
        if (schema == null) {
            //look for the schema in the table name
            String[] t = tableName.split("\\.");
            if (t.length != 2) {
                throw new RelationalException("Invalid table format", ErrorCode.CANNOT_CONVERT_TYPE);
            }
            schema = t[0];
            tableN = t[1];
        }

        return new String[]{schema, tableN};
    }

    private @Nonnull Scannable getSourceScannable(@Nonnull Options options, @Nonnull Table table) throws RelationalException {
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
                throw new RelationalException("Unknown index: <" + indexName + "> on type <" + table.getName() + ">", ErrorCode.UNKNOWN_INDEX);
            }
            source = index;
        } else {
            source = table;
        }
        return source;
    }

    private static <T> Iterable<T> toIterable(Iterator<T> it) {
        return () -> it;
    }

}
