/*
 * EmbeddedRelationalPreparedStatement.java
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
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.query.Plan;
import com.apple.foundationdb.relational.recordlayer.query.PlanContext;
import com.apple.foundationdb.relational.recordlayer.query.QueryPlan;
import com.apple.foundationdb.relational.recordlayer.util.Assert;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.Optional;

public class EmbeddedRelationalPreparedStatement implements RelationalPreparedStatement {
    @Nonnull
    private final String sql;

    @Nonnull
    private final EmbeddedRelationalConnection conn;

    public EmbeddedRelationalPreparedStatement(@Nonnull String sql, @Nonnull EmbeddedRelationalConnection conn) {
        this.sql = sql;
        this.conn = conn;
    }

    @Override
    public RelationalResultSet executeQuery() throws SQLException {
        try {
            Assert.notNull(sql);
            Optional<RelationalResultSet> resultSet = executeQueryInternal(sql, Options.NONE);
            if (resultSet.isPresent()) {
                return new ErrorCapturingResultSet(resultSet.get());
            } else {
                throw new RelationalException("PreparedStatement.executeQuery must return a result set but was executed on a query that doesn't: " + sql,
                        ErrorCode.NO_RESULT_SET);
            }
        } catch (RelationalException ve) {
            throw ve.toSqlException();
        }
    }

    private Optional<RelationalResultSet> executeQueryInternal(@Nonnull String query,
                                                             @Nonnull Options options) throws RelationalException, SQLException {
        conn.ensureTransactionActive();
        if (conn.getSchema() == null) {
            throw new RelationalException("No Schema specified", ErrorCode.UNDEFINED_SCHEMA);
        }
        try (var schema = conn.getRecordLayerDatabase().loadSchema(conn.getSchema())) {
            final FDBRecordStore store = schema.loadStore();
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
    }

    @Override
    public void close() throws SQLException {
        // Nothing to do
    }
}
