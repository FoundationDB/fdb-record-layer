/*
 * IncarnationTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query.functions;

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FormatVersion;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalPreparedStatement;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.AbstractDatabase;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.RecordLayerSchema;
import com.apple.foundationdb.relational.recordlayer.RelationalConnectionRule;
import com.apple.foundationdb.relational.recordlayer.RelationalStatementRule;
import com.apple.foundationdb.relational.recordlayer.storage.BackingRecordStore;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.List;
import java.util.function.IntFunction;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests of get_versionstamp_incarnation, and eventually the writing version of that.
 * See also {@code FDBIncarnationQueryTest} in fdb-record-layer-core
 */
public class IncarnationTest {
    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(
            IncarnationTest.class,
            "CREATE TABLE my_record (key string, incarnation integer, data string, PRIMARY KEY(key))");

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule connection = new RelationalConnectionRule(
            () -> relationalExtension.getDriver(FormatVersion.INCARNATION), database::getConnectionUri)
            .withOptions(Options.NONE)
            .withSchema("TEST_SCHEMA");

    @RegisterExtension
    @Order(3)
    public final RelationalStatementRule statement = new RelationalStatementRule(connection);


    @Test
    void insertWithIncarnation() throws SQLException, RelationalException {
        assertThat(statement.executeUpdate("INSERT INTO my_record (key, incarnation, data) VALUES " +
                "('r1', get_versionstamp_incarnation(), 'something0')"))
                .isEqualTo(1);
        ResultSetAssert.assertThat(statement.executeQuery("SELECT incarnation FROM my_record"))
                .containsRowsExactly(List.<Object[]>of(new Object[] {0}));

        updateIncarnation(current -> 100);
        assertThat(statement.executeUpdate("INSERT INTO my_record (key, incarnation, data) VALUES " +
                "('r2', get_versionstamp_incarnation(), 'something1')"))
                .isEqualTo(1);
        ResultSetAssert.assertThat(statement.executeQuery("SELECT incarnation FROM my_record"))
                .containsRowsExactly(List.of(new Object[] {0}, new Object[] {100}));
    }

    @Test
    void updateWithIncarnation() throws SQLException, RelationalException {
        assertThat(statement.executeUpdate("INSERT INTO my_record (key, incarnation, data) VALUES " +
                "('r1', get_versionstamp_incarnation(), 'something0')," +
                "('r2', get_versionstamp_incarnation(), 'something1')"))
                .isEqualTo(2);
        ResultSetAssert.assertThat(statement.executeQuery("SELECT incarnation FROM my_record"))
                .containsRowsExactly(List.of(new Object[] {0}, new Object[] {0}));

        updateIncarnation(current -> 57);
        assertThat(statement.executeUpdate("UPDATE my_record set incarnation=get_versionstamp_incarnation(), data='banana' WHERE key='r1'"))
                .isEqualTo(1);
        ResultSetAssert.assertThat(statement.executeQuery("SELECT incarnation, data FROM my_record"))
                .containsRowsExactly(List.of(new Object[] {57, "banana"}, new Object[] {0, "something1"}));
    }

    @Test
    void incarnationWithContinuations() throws SQLException, RelationalException {
        updateIncarnation(current -> 12);
        assertThat(statement.executeUpdate("INSERT INTO my_record (key, incarnation, data) VALUES " +
                "('r1', get_versionstamp_incarnation(), 'something0')," +
                "('r2', get_versionstamp_incarnation(), 'something1')"))
                .isEqualTo(2);
        statement.setMaxRows(1);

        final RelationalResultSet resultSet = statement.executeQuery("SELECT get_versionstamp_incarnation(), key FROM my_record");
        ResultSetAssert.assertThat(resultSet)
                .containsRowsExactly(List.<Object[]>of(new Object[] {12, "r1"}));
        updateIncarnation(current -> 57);
        try (RelationalPreparedStatement preparedStatement = connection.prepareStatement("EXECUTE CONTINUATION ?")) {
            preparedStatement.setBytes(1, resultSet.getContinuation().serialize());
            ResultSetAssert.assertThat(preparedStatement.executeQuery())
                    .containsRowsExactly(List.<Object[]>of(new Object[] {57, "r2"}));
        }
    }

    private void updateIncarnation(@Nonnull final IntFunction<Integer> updater) throws SQLException, RelationalException {
        connection.setAutoCommit(false);
        // force transaction to start, because at the time of writing, we don't support BEGIN TRANSACTION
        statement.executeQuery("SELECT key FROM my_record");
        final AbstractDatabase recordLayerDatabase = ((EmbeddedRelationalConnection)statement.getConnection()).getRecordLayerDatabase();
        final RecordLayerSchema recordLayerSchema = recordLayerDatabase.loadSchema(statement.getConnection().getSchema());
        final BackingRecordStore backingRecordStore = (BackingRecordStore)recordLayerSchema.loadStore();
        final FDBRecordStoreBase<?> store = backingRecordStore.unwrap(FDBRecordStoreBase.class);
        store.updateIncarnation(updater).join();
        connection.setAutoCommit(true);
    }
}
