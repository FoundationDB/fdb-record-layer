/*
 * InMemoryRelationalStatement.java
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

package com.apple.foundationdb.relational.memory;

import com.apple.foundationdb.record.RecordStoreState;
import com.apple.foundationdb.relational.api.DynamicMessageBuilder;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.ProtobufDataBuilder;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.TableScan;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.IteratorResultSet;
import com.apple.foundationdb.relational.recordlayer.MessageTuple;
import com.apple.foundationdb.relational.recordlayer.query.Plan;
import com.apple.foundationdb.relational.recordlayer.query.PlanContext;
import com.apple.foundationdb.relational.recordlayer.query.QueryPlan;
import com.apple.foundationdb.relational.utils.InMemoryTransactionManager;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Collections;
import java.util.Iterator;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

public class InMemoryRelationalStatement implements RelationalStatement {

    final InMemoryTransactionManager inMemoryTransactionManager = new InMemoryTransactionManager();
    private final InMemoryRelationalConnection relationalConn;

    public InMemoryRelationalStatement(InMemoryRelationalConnection relationalConn) {
        this.relationalConn = relationalConn;
    }

    @Override
    public RelationalResultSet executeQuery(String sql) throws SQLException {
        if (execute(sql)) {
            return getResultSet();
        } else {
            throw new SQLException(String.format("query '%s' does not return result set, use JDBC executeUpdate method instead", sql), ErrorCode.INVALID_PARAMETER.getErrorCode());
        }
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        try {
            PlanContext ctx = PlanContext.Builder.create()
                    .withConstantActionFactory(relationalConn.getConstantActionFactory())
                    .withDdlQueryFactory(relationalConn.getDdlQueryFactory())
                    .withDbUri(relationalConn.getDatabaseUri())
                    .withMetadata(relationalConn.getRecordMetaData())
                    .withStoreState(new RecordStoreState(null, null))
                    .build();

            final Plan<?> plan = Plan.generate(sql, ctx);
            if (plan instanceof QueryPlan) {
                throw new SQLFeatureNotSupportedException("Cannot execute queries in the InMemory Relational version, it's only good for Direct Access API");
            }

            Plan.ExecutionContext executionCtx = Plan.ExecutionContext.of(inMemoryTransactionManager.createTransaction(), Options.NONE, relationalConn);
            plan.execute(executionCtx);
            return true;
        } catch (RelationalException e) {
            throw e.toSqlException();
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new UnsupportedOperationException("Not Implemented in the Relational layer for query " + sql);
    }

    @Nonnull
    @Override
    public RelationalResultSet executeScan(@Nonnull TableScan scan, @Nonnull Options options) throws RelationalException {
        final InMemoryTable inMemoryTable = relationalConn.loadTable(scan.getTableName());
        if (inMemoryTable == null) {
            throw new RelationalException("Unknown table <" + scan.getTableName() + ">", ErrorCode.UNKNOWN_TYPE);
        }
        Stream<Message> m = inMemoryTable.scan(scan.getStartKey(), scan.getEndKey());
        Iterator<? extends Row> iterator = m.map(MessageTuple::new).iterator();
        return new IteratorResultSet(inMemoryTable.getMetaData(), iterator, 0);
    }

    @Nonnull
    @Override
    public RelationalResultSet executeGet(@Nonnull String tableName, @Nonnull KeySet key, @Nonnull Options options) throws RelationalException {
        final InMemoryTable inMemoryTable = relationalConn.loadTable(tableName);
        if (inMemoryTable == null) {
            throw new RelationalException("Unknown table <" + tableName + ">", ErrorCode.UNKNOWN_TYPE);
        }
        Message m = inMemoryTable.get(key);
        String[] columns = new String[inMemoryTable.getDescriptor().getFields().size()];
        for (Descriptors.FieldDescriptor fd : inMemoryTable.getDescriptor().getFields()) {
            columns[fd.getIndex()] = fd.getName();
        }
        Iterator<Row> iterator = m != null ? Collections.<Row>singleton(new MessageTuple(m)).iterator() : Collections.emptyIterator();
        return new IteratorResultSet(inMemoryTable.getMetaData(), iterator, 0);
    }

    @Override
    public int executeInsert(@Nonnull String tableName, @Nonnull Iterator<? extends Message> data, @Nonnull Options options) throws RelationalException {
        final InMemoryTable inMemoryTable = relationalConn.loadTable(tableName);
        if (inMemoryTable == null) {
            throw new RelationalException("Unknown table <" + tableName + ">", ErrorCode.UNKNOWN_TYPE);
        }
        return inMemoryTable.add(data);
    }

    @Nonnull
    @Override
    public DynamicMessageBuilder getDataBuilder(@Nonnull String typeName) throws RelationalException {
        final InMemoryTable inMemoryTable = relationalConn.loadTable(typeName);
        if (inMemoryTable == null) {
            throw new RelationalException("Unknown table <" + typeName + ">", ErrorCode.UNKNOWN_TYPE);
        }
        return new ProtobufDataBuilder(inMemoryTable.getDescriptor());
    }

    @Override
    public int executeDelete(@Nonnull String tableName, @Nonnull Iterator<KeySet> keys, @Nonnull Options options) {
        return 0;
    }

    @Override
    public void close() throws SQLException {

    }
}
