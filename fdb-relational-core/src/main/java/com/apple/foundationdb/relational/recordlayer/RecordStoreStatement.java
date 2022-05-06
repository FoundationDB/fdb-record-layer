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

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.query.plan.cascades.RelationalExpression;
import com.apple.foundationdb.record.query.plan.cascades.properties.UsedTypesProperty;
import com.apple.foundationdb.record.query.plan.cascades.typing.Type;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.relational.api.DynamicMessageBuilder;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.OperationOption;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.QueryProperties;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.TableScan;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.query.PlanGenerator;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;
import com.apple.foundationdb.relational.recordlayer.utils.Assert;

import com.google.common.base.Preconditions;
import com.google.protobuf.Message;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class RecordStoreStatement implements RelationalStatement {

    @Nonnull
    private final RecordStoreConnection conn;

    public RecordStoreStatement(@Nonnull final RecordStoreConnection conn) {
        this.conn = conn;
    }

    @Override
    public RelationalResultSet executeQuery(@Nonnull String query, @Nonnull Options options, @Nonnull QueryProperties queryProperties) throws RelationalException {
        ensureTransactionActive();
        final String schemaName = conn.getSchema();
        final RecordLayerSchema schema = conn.frl.loadSchema(schemaName, options);
        final FDBRecordStore store = schema.loadStore();
        final RelationalExpression relationalExpression = PlanGenerator.generateLogicalPlan(query, store.getRecordMetaData(), store.getRecordStoreState(), ignore -> {
        });
        final Type innerType = relationalExpression.getResultType().getInnerType();
        Assert.notNull(innerType);
        Assert.that(innerType instanceof Type.Record, String.format("unexpected plan returning top-level result of type %s", innerType.getTypeCode()));
        final Set<Type> usedTypes = UsedTypesProperty.evaluate(relationalExpression);
        final TypeRepository.Builder builder = TypeRepository.newBuilder();
        usedTypes.forEach(builder::addTypeIfNeeded);
        final RecordQueryPlan recordQueryPlan = PlanGenerator.generatePlan(query, store.getRecordMetaData(), store.getRecordStoreState());
        final String[] fieldNames = Objects.requireNonNull(((Type.Record) innerType).getFields()).stream().sorted(Comparator.comparingInt(Type.Record.Field::getFieldIndex)).map(Type.Record.Field::getFieldName).collect(Collectors.toUnmodifiableList()).toArray(String[]::new);
        final QueryExecutor queryExecutor = new QueryExecutor(recordQueryPlan, fieldNames, EvaluationContext.forTypeRepository(builder.build()), schema, false /* get this information from the query plan */);
        return new RecordLayerResultSet(queryExecutor.getFieldNames(),
                queryExecutor.execute(options.getOption(OperationOption.CONTINUATION_NAME, null)),
                conn);
    }

    @Override
    public @Nonnull
    RelationalResultSet executeScan(@Nonnull TableScan scan, @Nonnull Options options) throws RelationalException {
        ensureTransactionActive();

        String[] schemaAndTable = getSchemaAndTable(conn.getSchema(), scan.getTableName());
        RecordLayerSchema schema = conn.frl.loadSchema(schemaAndTable[0], options);

        Table table = schema.loadTable(schemaAndTable[1], options);

        DirectScannable source = getSourceScannable(options, table);

        final KeyBuilder keyBuilder = source.getKeyBuilder();
        Row start = scan.getStartKey().isEmpty() ? null : keyBuilder.buildKey(scan.getStartKey(), true, true);
        Row end = scan.getEndKey().isEmpty() ? null : keyBuilder.buildKey(scan.getEndKey(), true, true);

        return new RecordLayerResultSet(source.getFieldNames(),
                source.openScan(conn.transaction, start, end, options.getOption(OperationOption.CONTINUATION_NAME, null), scan.getScanProperties()),
                conn);
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

        DirectScannable source = getSourceScannable(options, table);

        Row tuple = source.getKeyBuilder().buildKey(key.toMap(), true, true);

        final Row row = source.get(conn.transaction, tuple, queryProperties);

        return new IteratorResultSet(table.getFieldNames(), row == null ? Collections.emptyIterator() : List.of(row).iterator(), 0);
    }

    @Override
    public DynamicMessageBuilder getDataBuilder(@Nonnull String typeName) throws RelationalException {
        ensureTransactionActive();
        String[] schemaAndTable = getSchemaAndTable(conn.getSchema(), typeName);
        RecordLayerSchema schema = conn.frl.loadSchema(schemaAndTable[0], Options.create());
        return schema.getDataBuilder(typeName);
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

        DirectScannable source = getSourceScannable(options, table);
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

    private @Nonnull DirectScannable getSourceScannable(@Nonnull Options options, @Nonnull Table table) throws RelationalException {
        DirectScannable source;
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

}
