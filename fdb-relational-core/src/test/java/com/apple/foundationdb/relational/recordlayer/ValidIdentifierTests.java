/*
 * TransactionBoundDatabaseTest.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.relational.api.EmbeddedRelationalDriver;
import com.apple.foundationdb.relational.api.EmbeddedRelationalEngine;
import com.apple.foundationdb.relational.api.EmbeddedRelationalStruct;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.transactionbound.TransactionBoundEmbeddedRelationalEngine;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.sql.SQLException;

public class ValidIdentifierTests {
    @RegisterExtension
    @Order(0)
    public static final RelationalExtension relational = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule dbRule = new SimpleDatabaseRule(ValidIdentifierTests.class, TestSchemas.restaurant());

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule connRule = new RelationalConnectionRule(dbRule::getConnectionUri)
            .withOptions(Options.NONE)
            .withSchema("TEST_SCHEMA");

    @Test
    void simpleSelect() throws RelationalException, SQLException {
        final var embeddedConnection = connRule.getUnderlyingEmbeddedConnection();
        final var store = getStore(embeddedConnection);
        final var schemaTemplate = getSchemaTemplate(embeddedConnection);

        try (FDBRecordContext context = createNewContext(embeddedConnection)) {
            final var newStore = store.asBuilder().setMetaDataProvider(store.getMetaDataProvider()).setContext(context).open();
            try (Transaction transaction = new RecordStoreAndRecordContextTransaction(newStore, context, schemaTemplate)) {

                // Then, once we have a transaction that contains both an FDBRecordStoreBase<Message> and an FDBRecordContext,
                // connect to a TransactionBoundDatabase
                EmbeddedRelationalEngine engine = new TransactionBoundEmbeddedRelationalEngine();
                EmbeddedRelationalDriver driver = new EmbeddedRelationalDriver(engine);
                try (RelationalConnection conn = driver.connect(dbRule.getConnectionUri(), transaction, Options.NONE)) {
                    conn.setSchema("TEST_SCHEMA");
                    try (RelationalStatement statement = conn.createStatement()) {
                        var record = EmbeddedRelationalStruct.newBuilder()
                                .addLong("REST_NO", 42)
                                .addString("NAME", "FOO")
                                .build();
                        statement.executeInsert("RESTAURANT", record);
                    }

                    try (RelationalStatement statement = conn.createStatement()) {
                        try (RelationalResultSet resultSet = statement.executeScan("RESTAURANT", new KeySet(), Options.NONE)) {
                            Assertions.assertThat(resultSet.next()).isTrue();
                            Assertions.assertThat(resultSet.getString("NAME")).isEqualTo("FOO");
                            Assertions.assertThat(resultSet.getLong("REST_NO")).isEqualTo(42L);
                        }
                    }
                }
            }
        }
    }

    @Test
    void selectWithIncludedPlanCache() throws RelationalException, SQLException {
        final var embeddedConnection = connRule.getUnderlyingEmbeddedConnection();
        final var store = getStore(embeddedConnection);
        final var schemaTemplate = getSchemaTemplate(embeddedConnection);

        try (FDBRecordContext context = createNewContext(embeddedConnection)) {
            final var newStore = store.asBuilder().setMetaDataProvider(store.getMetaDataProvider()).setContext(context).open();
            try (Transaction transaction = new RecordStoreAndRecordContextTransaction(newStore, context, schemaTemplate)) {

                // Then, once we have a transaction that contains both an FDBRecordStoreBase<Message> and an FDBRecordContext,
                // connect to a TransactionBoundDatabase
                TransactionBoundEmbeddedRelationalEngine engine = new TransactionBoundEmbeddedRelationalEngine(Options.builder().withOption(Options.Name.PLAN_CACHE_PRIMARY_TIME_TO_LIVE_MILLIS, 10L).build());
                Assertions.assertThat(engine.getPlanCache()).isNotNull()
                        .extracting(planCache -> planCache.getStats().numEntries()).isEqualTo(0L);
                EmbeddedRelationalDriver driver = new EmbeddedRelationalDriver(engine);
                try (RelationalConnection conn = driver.connect(dbRule.getConnectionUri(), transaction, Options.NONE)) {
                    conn.setSchema("TEST_SCHEMA");
                    try (RelationalStatement statement = conn.createStatement()) {
                        var record = EmbeddedRelationalStruct.newBuilder()
                                .addLong("REST_NO", 42)
                                .addString("NAME", "FOO")
                                .build();
                        statement.executeInsert("RESTAURANT", record);
                    }

                    try (RelationalStatement statement = conn.createStatement()) {
                        try (RelationalResultSet resultSet = statement.executeQuery("select * from RESTAURANT")) {
                            Assertions.assertThat(resultSet.next()).isTrue();
                            Assertions.assertThat(resultSet.getString("NAME")).isEqualTo("FOO");
                            Assertions.assertThat(resultSet.getLong("REST_NO")).isEqualTo(42L);
                        }
                    }
                }

                //we should have populated the plan cache;
                // Assertions.assertThat(engine.getPlanCache().getStats().numEntries()).isEqualTo(1L);
            }
        }
    }

    @Test
    void createTemporaryFunction() throws RelationalException, SQLException {
        final var embeddedConnection = connRule.getUnderlyingEmbeddedConnection();
        final var store = getStore(embeddedConnection);
        final var schemaTemplate = getSchemaTemplate(embeddedConnection);

        try (FDBRecordContext context = createNewContext(embeddedConnection)) {
            final var newStore = store.asBuilder().setMetaDataProvider(store.getMetaDataProvider()).setContext(context).open();
            try (Transaction transaction = new RecordStoreAndRecordContextTransaction(newStore, context, schemaTemplate)) {
                EmbeddedRelationalDriver driver = new EmbeddedRelationalDriver(new TransactionBoundEmbeddedRelationalEngine());
                try (RelationalConnection conn = driver.connect(dbRule.getConnectionUri(), transaction, Options.NONE)) {
                    conn.setSchema("TEST_SCHEMA");
                    try (RelationalStatement statement = conn.createStatement()) {
                        statement.executeUpdate("CREATE TEMPORARY FUNCTION REST_FUNC() ON COMMIT DROP FUNCTION AS SELECT * FROM RESTAURANT WHERE REST_NO > 1000");
                    }
                    Assertions.assertThat(transaction.getBoundSchemaTemplateMaybe()).isPresent();
                    final var routinesInBoundSchemaTemplates = transaction.getBoundSchemaTemplateMaybe().get().getInvokedRoutines();
                    Assertions.assertThat(routinesInBoundSchemaTemplates)
                            .hasSize(1)
                            .anyMatch(routine -> routine.getName().equals("REST_FUNC"));
                }
            }
        }
    }

    @Test
    void dropTemporaryFunction() throws RelationalException, SQLException {
        final var embeddedConnection = connRule.getUnderlyingEmbeddedConnection();
        final var store = getStore(embeddedConnection);
        final var schemaTemplate = getSchemaTemplate(embeddedConnection);

        try (FDBRecordContext context = createNewContext(embeddedConnection)) {
            final var newStore = store.asBuilder().setMetaDataProvider(store.getMetaDataProvider()).setContext(context).open();
            try (Transaction transaction = new RecordStoreAndRecordContextTransaction(newStore, context, schemaTemplate)) {
                EmbeddedRelationalDriver driver = new EmbeddedRelationalDriver(new TransactionBoundEmbeddedRelationalEngine());
                try (RelationalConnection conn = driver.connect(dbRule.getConnectionUri(), transaction, Options.NONE)) {
                    conn.setSchema("TEST_SCHEMA");
                    try (RelationalStatement statement = conn.createStatement()) {
                        statement.executeUpdate("CREATE TEMPORARY FUNCTION REST_FUNC() ON COMMIT DROP FUNCTION AS SELECT * FROM RESTAURANT WHERE REST_NO > 1000");
                    }
                    Assertions.assertThat(transaction.getBoundSchemaTemplateMaybe()).isPresent();
                    Assertions.assertThat(transaction.getBoundSchemaTemplateMaybe().get().getInvokedRoutines())
                            .hasSize(1)
                            .anyMatch(routine -> routine.getName().equals("REST_FUNC"));
                    try (RelationalStatement statement = conn.createStatement()) {
                        statement.executeUpdate("DROP TEMPORARY FUNCTION REST_FUNC");
                    }
                    Assertions.assertThat(transaction.getBoundSchemaTemplateMaybe()).isPresent();
                    Assertions.assertThat(transaction.getBoundSchemaTemplateMaybe().get().getInvokedRoutines())
                            .isEmpty();
                }
            }
        }
    }

    @Test
    void dropTemporaryIfExistFunction() throws RelationalException, SQLException {
        final var embeddedConnection = connRule.getUnderlyingEmbeddedConnection();
        final var store = getStore(embeddedConnection);
        final var schemaTemplate = getSchemaTemplate(embeddedConnection);

        try (FDBRecordContext context = createNewContext(embeddedConnection)) {
            final var newStore = store.asBuilder().setMetaDataProvider(store.getMetaDataProvider()).setContext(context).open();
            try (Transaction transaction = new RecordStoreAndRecordContextTransaction(newStore, context, schemaTemplate)) {
                EmbeddedRelationalDriver driver = new EmbeddedRelationalDriver(new TransactionBoundEmbeddedRelationalEngine());
                try (RelationalConnection conn = driver.connect(dbRule.getConnectionUri(), transaction, Options.NONE)) {
                    conn.setSchema("TEST_SCHEMA");
                    try (RelationalStatement statement = conn.createStatement()) {
                        statement.executeUpdate("CREATE TEMPORARY FUNCTION REST_FUNC() ON COMMIT DROP FUNCTION AS SELECT * FROM RESTAURANT WHERE REST_NO > 1000");
                    }
                    Assertions.assertThat(transaction.getBoundSchemaTemplateMaybe()).isPresent();
                    Assertions.assertThat(transaction.getBoundSchemaTemplateMaybe().get().getInvokedRoutines())
                            .hasSize(1)
                            .anyMatch(routine -> routine.getName().equals("REST_FUNC"));
                    try (RelationalStatement statement = conn.createStatement()) {
                        statement.executeUpdate("DROP TEMPORARY FUNCTION IF EXISTS REST_FUNC");
                    }
                    Assertions.assertThat(transaction.getBoundSchemaTemplateMaybe()).isPresent();
                    Assertions.assertThat(transaction.getBoundSchemaTemplateMaybe().get().getInvokedRoutines())
                            .isEmpty();
                    try (RelationalStatement statement = conn.createStatement()) {
                        statement.executeUpdate("DROP TEMPORARY FUNCTION IF EXISTS DOES_NOT_EXISTS");
                    }
                }
            }
        }
    }

    @Test
    void unsetTransactionBoundSchemaTemplate() throws RelationalException, SQLException {
        final var embeddedConnection = connRule.getUnderlyingEmbeddedConnection();
        final var store = getStore(embeddedConnection);
        final var schemaTemplate = getSchemaTemplate(embeddedConnection);

        try (FDBRecordContext context = createNewContext(embeddedConnection)) {
            final var newStore = store.asBuilder().setMetaDataProvider(store.getMetaDataProvider()).setContext(context).open();
            try (Transaction transaction = new RecordStoreAndRecordContextTransaction(newStore, context, schemaTemplate)) {
                EmbeddedRelationalDriver driver = new EmbeddedRelationalDriver(new TransactionBoundEmbeddedRelationalEngine());
                try (RelationalConnection conn = driver.connect(dbRule.getConnectionUri(), transaction, Options.NONE)) {
                    conn.setSchema("TEST_SCHEMA");
                    try (RelationalStatement statement = conn.createStatement()) {
                        statement.executeUpdate("CREATE TEMPORARY FUNCTION REST_FUNC() ON COMMIT DROP FUNCTION AS SELECT * FROM RESTAURANT WHERE REST_NO > 1000");
                    }
                    Assertions.assertThat(transaction.getBoundSchemaTemplateMaybe()).isPresent();
                    Assertions.assertThat(transaction.getBoundSchemaTemplateMaybe().get().getInvokedRoutines())
                            .hasSize(1)
                            .anyMatch(routine -> routine.getName().equals("REST_FUNC"));
                    transaction.unsetBoundSchemaTemplate();
                    Assertions.assertThat(transaction.getBoundSchemaTemplateMaybe()).isEmpty();
                }
            }
        }
    }

    static FDBRecordStore getStore(EmbeddedRelationalConnection connection) throws RelationalException, SQLException {
        connection.setAutoCommit(false);
        connection.createNewTransaction();
        RecordLayerSchema schema = connection.getRecordLayerDatabase().loadSchema("TEST_SCHEMA");
        final var store = schema.loadStore().unwrap(FDBRecordStore.class);
        connection.rollback();
        connection.setAutoCommit(true);
        return store;
    }

    static SchemaTemplate getSchemaTemplate(EmbeddedRelationalConnection connection) throws RelationalException, SQLException {
        connection.setAutoCommit(false);
        connection.createNewTransaction();
        final var schemaTemplate = connection.getSchemaTemplate();
        connection.rollback();
        connection.setAutoCommit(true);
        return schemaTemplate;
    }

    static FDBRecordContext createNewContext(@Nonnull EmbeddedRelationalConnection connection) throws RelationalException {
        return connection.getRecordLayerDatabase().getTransactionManager().createTransaction(Options.NONE).unwrap(FDBRecordContext.class);
    }
}
