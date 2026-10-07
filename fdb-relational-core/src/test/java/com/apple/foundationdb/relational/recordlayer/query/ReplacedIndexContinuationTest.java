/*
 * ReplacedIndexContinuationTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer.query;

import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.EmbeddedRelationalDriver;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.AbstractDatabase;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalConnection;
import com.apple.foundationdb.relational.recordlayer.EmbeddedRelationalExtension;
import com.apple.foundationdb.relational.recordlayer.RecordStoreAndRecordContextTransaction;
import com.apple.foundationdb.relational.recordlayer.Utils;
import com.apple.foundationdb.relational.recordlayer.metadata.RecordLayerSchemaTemplate;
import com.apple.foundationdb.relational.recordlayer.storage.BackingStore;
import com.apple.foundationdb.relational.transactionbound.TransactionBoundEmbeddedRelationalEngine;
import com.apple.foundationdb.relational.utils.RelationalAssertions;
import com.apple.foundationdb.relational.utils.ResultSetAssert;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;

/**
 * Tests what happens to an in-flight SQL query when the index it was planned against is
 * {@linkplain IndexOptions#REPLACED_BY_OPTION_PREFIX replaced}.
 *
 * <p>
 * The scenario: a query is hinted onto value index {@code X}, interrupted part way through so that a continuation is
 * handed back, and then the meta-data is evolved to say {@code X} is {@code replacedBy} {@code Y}. Because {@code Y}
 * is already readable, opening the store disables {@code X} and deletes its data, so the continuation now refers to a
 * scan of an index that no longer holds anything. Resuming it should be rejected.
 * </p>
 *
 * <p>
 * Note that {@code replacedBy} cannot be expressed in the relational DDL — it is a Record Layer index option only — so
 * the second half of the test drives a transaction-bound connection over hand-built meta-data, in the style of
 * {@link TransactionBoundQueryTest}.
 * </p>
 */
public class ReplacedIndexContinuationTest {
    @Nonnull
    private static final String SCHEMA_TEMPLATE =
            """
            CREATE TABLE t1(id bigint, a bigint, b string, PRIMARY KEY(id))
            CREATE INDEX x ON t1(a)
            CREATE INDEX y ON t1(a, b)
            """;

    @RegisterExtension
    @Order(0)
    @Nonnull
    final EmbeddedRelationalExtension embeddedExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    final SimpleDatabaseRule databaseRule = new SimpleDatabaseRule(ReplacedIndexContinuationTest.class, SCHEMA_TEMPLATE);

    /** The store most recently opened by {@link #connectTransactionBound}, so the test can inspect its index states. */
    @Nullable
    private FDBRecordStore lastOpenedStore;

    public ReplacedIndexContinuationTest() {
        Utils.enableCascadesDebugger();
    }

    @Test
    void continuationOnReplacedIndexIsRejected() throws SQLException, RelationalException {
        final Continuation continuation;
        try (EmbeddedRelationalConnection connection = connectEmbedded()) {
            insertData(connection);

            // Sanity check that the hint actually put us on X, otherwise the rest of the test proves nothing.
            try (RelationalStatement statement = connection.createStatement();
                    RelationalResultSet resultSet = statement.executeQuery("EXPLAIN SELECT id, a FROM t1 USE INDEX (x)")) {
                ResultSetAssert.assertThat(resultSet).hasNextRow();
                Assertions.assertThat(resultSet.getString("PLAN"))
                        .as("query should be planned on the hinted index X")
                        .contains("X");
            }

            // Interrupt the scan after two rows so that we get a continuation into the middle of X.
            try (RelationalStatement statement = connection.createStatement()) {
                statement.setMaxRows(2);
                try (RelationalResultSet resultSet = statement.executeQuery("SELECT id, a FROM t1 USE INDEX (x)")) {
                    ResultSetAssert.assertThat(resultSet)
                            .hasNextRow().hasColumn("ID", 1L)
                            .hasNextRow().hasColumn("ID", 2L)
                            .hasNoNextRow();
                    continuation = resultSet.getContinuation();
                    Assertions.assertThat(continuation.atEnd()).isFalse();
                }
            }
        }

        // Mark X as replaced by Y, leaving X's own versions alone so that only the replacement itself can be what
        // invalidates the continuation.
        final RecordMetaData metaDataWithReplacement = metaDataWithReplacedIndex("X", "Y");

        try (FDBRecordContext context = openContext()) {
            try (EmbeddedRelationalConnection connection = connectTransactionBound(context, metaDataWithReplacement)) {
                // Opening the store with the new meta-data should have dropped X, since its replacement is readable.
                Assertions.assertThat(Objects.requireNonNull(lastOpenedStore).getRecordStoreState().getState("X"))
                        .as("X should have been disabled once its replacement became readable")
                        .isEqualTo(IndexState.DISABLED);

                try (var statement = connection.prepareStatement("EXECUTE CONTINUATION ?continuation")) {
                    statement.setMaxRows(2);
                    statement.setBytes("continuation", continuation.serialize());

                    // The continuation is indeed refused, but only because the record layer refuses to scan a
                    // disabled index at execution time. Nothing on the relational side recognises that the
                    // continuation has been invalidated, so the failure arrives as a raw, unmapped error rather
                    // than as INVALID_CONTINUATION, which is what a caller resuming a query would expect to see.
                    RelationalAssertions.assertThrowsSqlException(statement::executeQuery)
                            .containsInMessage("Cannot scan non-readable index")
                            .hasErrorCode(ErrorCode.UNKNOWN);
                }
            }
        }
    }

    /**
     * Rebuild the database's meta-data with {@code replacedBy} set on {@code indexName}. The index proto is edited in
     * place so that {@code addedVersion} and {@code lastModifiedVersion} are untouched — adding a {@code replacedBy}
     * option is explicitly a version-preserving change, so the plan constraint's index-version check will still pass
     * and the only thing left that can reject the continuation is {@code X} no longer being readable.
     */
    @Nonnull
    private RecordMetaData metaDataWithReplacedIndex(@Nonnull String indexName, @Nonnull String replacementName)
            throws SQLException, RelationalException {
        try (EmbeddedRelationalConnection connection = connectEmbedded()) {
            connection.createNewTransaction();
            final SchemaTemplate schemaTemplate = connection.getSchemaTemplate();
            final RecordMetaDataProto.MetaData proto =
                    schemaTemplate.unwrap(RecordLayerSchemaTemplate.class).toRecordMetadata().toProto();

            final RecordMetaDataProto.MetaData.Builder builder = proto.toBuilder()
                    .setVersion(proto.getVersion() + 1);
            boolean found = false;
            for (int i = 0; i < proto.getIndexesCount(); i++) {
                final RecordMetaDataProto.Index index = proto.getIndexes(i);
                if (index.getName().equals(indexName)) {
                    builder.setIndexes(i, index.toBuilder()
                            .addOptions(RecordMetaDataProto.Index.Option.newBuilder()
                                    .setKey(IndexOptions.REPLACED_BY_OPTION_PREFIX)
                                    .setValue(replacementName))
                            .build());
                    found = true;
                }
            }
            Assertions.assertThat(found).as("index %s should exist in the meta-data", indexName).isTrue();
            return RecordMetaData.build(builder.build());
        }
    }

    private void insertData(@Nonnull EmbeddedRelationalConnection connection) throws SQLException {
        try (RelationalStatement statement = connection.createStatement()) {
            statement.executeUpdate("INSERT INTO t1 VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c'), (4, 40, 'd'), (5, 50, 'e')");
        }
    }

    @Nonnull
    private EmbeddedRelationalConnection connectEmbedded() throws SQLException {
        final Connection connection = DriverManager.getConnection(databaseRule.getConnectionUri().toString());
        connection.setSchema(databaseRule.getSchemaName());
        return connection.unwrap(EmbeddedRelationalConnection.class);
    }

    @Nonnull
    private FDBRecordContext openContext() throws SQLException, RelationalException {
        try (EmbeddedRelationalConnection connection = connectEmbedded()) {
            connection.createNewTransaction();
            final FDBRecordContext context = connection.getTransaction().unwrap(FDBRecordContext.class);
            // The connection's own context dies with the connection, so hand back a fresh one on the same database.
            return context.getDatabase().openContext();
        }
    }

    @Nonnull
    private EmbeddedRelationalConnection connectTransactionBound(@Nonnull FDBRecordContext context,
                                                                 @Nonnull RecordMetaData metaData) throws SQLException, RelationalException {
        try (EmbeddedRelationalConnection embeddedConnection = connectEmbedded()) {
            embeddedConnection.createNewTransaction();
            final AbstractDatabase db = embeddedConnection.getRecordLayerDatabase();
            final BackingStore store = db.loadRecordStore(databaseRule.getSchemaName(),
                    FDBRecordStoreBase.StoreExistenceCheck.ERROR_IF_NO_INFO_AND_NOT_EMPTY);

            // Opening with the new meta-data runs checkVersion, which is what drops the replaced index.
            final FDBRecordStore newStore = store.unwrap(FDBRecordStore.class).asBuilder()
                    .setMetaDataProvider(metaData)
                    .setContext(context)
                    .open();
            lastOpenedStore = newStore;

            final var originalDriver = embeddedExtension.getDriver();
            DriverManager.deregisterDriver(originalDriver);
            final var newDriver = new EmbeddedRelationalDriver(new TransactionBoundEmbeddedRelationalEngine(Options.none()));
            DriverManager.registerDriver(newDriver);
            try {
                final var driver = (EmbeddedRelationalDriver) DriverManager.getDriver(databaseRule.getConnectionUri().toString());
                final var schemaTemplate = RecordLayerSchemaTemplate.fromRecordMetadata(metaData,
                        databaseRule.getSchemaTemplateName(), metaData.getVersion());
                final RelationalConnection transactionBoundConnection = driver.connect(databaseRule.getConnectionUri(),
                        new RecordStoreAndRecordContextTransaction(newStore, context, schemaTemplate), Options.none());
                transactionBoundConnection.setSchema(databaseRule.getSchemaName());
                return transactionBoundConnection.unwrap(EmbeddedRelationalConnection.class);
            } finally {
                DriverManager.deregisterDriver(newDriver);
                DriverManager.registerDriver(originalDriver);
            }
        }
    }
}
