/*
 * RecordContextTransactionLocalVariablesTest.java
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.sql.SQLException;

/**
 * Direct unit tests for the transaction-scoped variable storage on {@link Transaction} /
 * {@link RecordContextTransaction}, independent of any SQL surface.
 */
public class RecordContextTransactionLocalVariablesTest {

    @RegisterExtension
    @Order(0)
    public static final EmbeddedRelationalExtension relational = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule dbRule = new SimpleDatabaseRule(RecordContextTransactionLocalVariablesTest.class, TestSchemas.restaurant());

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule connRule = new RelationalConnectionRule(dbRule::getConnectionUri)
            .withOptions(Options.NONE)
            .withSchema("TEST_SCHEMA");

    @Test
    void setThenGetReturnsTheBoundValue() throws RelationalException, SQLException {
        try (Transaction transaction = createTransaction()) {
            transaction.setLocalVariable("x", 42L);
            Assertions.assertThat(transaction.getLocalVariables()).containsEntry("x", 42L);
        }
    }

    @Test
    void overwritingAVariableReplacesThePreviousValue() throws RelationalException, SQLException {
        try (Transaction transaction = createTransaction()) {
            transaction.setLocalVariable("x", 1L);
            transaction.setLocalVariable("x", 2L);
            Assertions.assertThat(transaction.getLocalVariables()).containsEntry("x", 2L);
        }
    }

    @Test
    void nullIsALegitimateValueDistinctFromUnset() throws RelationalException, SQLException {
        try (Transaction transaction = createTransaction()) {
            transaction.setLocalVariable("x", null);
            Assertions.assertThat(transaction.getLocalVariables())
                    .containsKey("x")
                    .doesNotContainKey("never_set");
            Assertions.assertThat(transaction.getLocalVariables().get("x")).isNull();
        }
    }

    @Test
    void variablesAreNotVisibleInANewTransactionAfterCommit() throws RelationalException, SQLException {
        final FDBRecordContext firstContext = createNewContext();
        try (Transaction transaction = createTransaction(firstContext)) {
            transaction.setLocalVariable("x", 42L);
            transaction.commit();
        }
        try (Transaction transaction = createTransaction(createNewContext())) {
            Assertions.assertThat(transaction.getLocalVariables()).doesNotContainKey("x");
        }
    }

    @Test
    void variablesAreClearedOnAbort() throws RelationalException, SQLException {
        try (Transaction transaction = createTransaction()) {
            transaction.setLocalVariable("x", 42L);
            transaction.abort();
        }
        try (Transaction transaction = createTransaction(createNewContext())) {
            Assertions.assertThat(transaction.getLocalVariables()).doesNotContainKey("x");
        }
    }

    @Nonnull
    private Transaction createTransaction() throws RelationalException, SQLException {
        return createTransaction(createNewContext());
    }

    @Nonnull
    private Transaction createTransaction(@Nonnull final FDBRecordContext context) throws RelationalException, SQLException {
        final EmbeddedRelationalConnection embeddedConnection = connRule.getUnderlyingEmbeddedConnection();
        final FDBRecordStore store = getStore(embeddedConnection);
        final SchemaTemplate schemaTemplate = getSchemaTemplate(embeddedConnection);
        final FDBRecordStore newStore = store.asBuilder().setContext(context).open();
        return new RecordStoreAndRecordContextTransaction(newStore, context, schemaTemplate);
    }

    @Nonnull
    private FDBRecordContext createNewContext() throws RelationalException, SQLException {
        return connRule.getUnderlyingEmbeddedConnection().getRecordLayerDatabase().getTransactionManager()
                .createTransaction(Options.NONE).unwrap(FDBRecordContext.class);
    }

    private static FDBRecordStore getStore(EmbeddedRelationalConnection connection) throws RelationalException, SQLException {
        connection.setAutoCommit(false);
        connection.createNewTransaction();
        RecordLayerSchema schema = connection.getRecordLayerDatabase().loadSchema("TEST_SCHEMA");
        final var store = schema.loadStore().unwrap(FDBRecordStore.class);
        connection.rollback();
        connection.setAutoCommit(true);
        return store;
    }

    private static SchemaTemplate getSchemaTemplate(EmbeddedRelationalConnection connection) throws RelationalException, SQLException {
        connection.setAutoCommit(false);
        connection.createNewTransaction();
        final var schemaTemplate = connection.getSchemaTemplate();
        connection.rollback();
        connection.setAutoCommit(true);
        return schemaTemplate;
    }
}
