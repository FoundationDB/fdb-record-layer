/*
 * TransactionBoundDatabaseTest.java
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

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.relational.api.EmbeddedRelationalDriver;
import com.apple.foundationdb.relational.api.EmbeddedRelationalEngine;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.transactionbound.TransactionBoundEmbeddedRelationalEngine;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;

import com.google.protobuf.Message;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.SQLException;

public class TransactionBoundDatabaseTest {
    @RegisterExtension
    @Order(0)
    public static final RelationalExtension relational = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule dbRule = new SimpleDatabaseRule(relational, TransactionBoundDatabaseTest.class, TestSchemas.restaurant());

    @RegisterExtension
    @Order(2)
    public final RelationalConnectionRule connRule = new RelationalConnectionRule(dbRule::getConnectionUri)
            .withOptions(Options.NONE)
            .withSchema("TEST_SCHEMA");

    @Test
    void simpleSelect() throws RelationalException, SQLException {
        // First create a transaction object out of the connection and the statement
        RecordLayerSchema schema = ((EmbeddedRelationalConnection) connRule.getUnderlying()).frl.loadSchema("TEST_SCHEMA");
        FDBRecordStore store = schema.loadStore().unwrap(FDBRecordStore.class);
        try (FDBRecordContext context = ((EmbeddedRelationalConnection) connRule.getUnderlying()).frl.getTransactionManager().createTransaction(Options.NONE).unwrap(FDBRecordContext.class)) {
            try (Transaction transaction = new RecordStoreAndRecordContextTransaction(store, context)) {

                // Then, once we have a transaction that contains both an FDBRecordStore and an FDBRecordContext,
                // connect to a TransactionBoundDatabase
                EmbeddedRelationalEngine engine = new TransactionBoundEmbeddedRelationalEngine();
                EmbeddedRelationalDriver driver = new EmbeddedRelationalDriver(engine);
                try (RelationalConnection conn = driver.connect(dbRule.getConnectionUri(), transaction, Options.NONE)) {
                    conn.setSchema("TEST_SCHEMA");
                    try (RelationalStatement statement = conn.createStatement()) {
                        Message record = statement.getDataBuilder("RESTAURANT")
                                .setField("REST_NO", 42)
                                .setField("NAME", "FOO")
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
        // First create a transaction object out of the connection and the statement
        RecordLayerSchema schema = ((EmbeddedRelationalConnection) connRule.getUnderlying()).frl.loadSchema("TEST_SCHEMA");
        FDBRecordStore store = schema.loadStore().unwrap(FDBRecordStore.class);
        try (FDBRecordContext context = ((EmbeddedRelationalConnection) connRule.getUnderlying()).frl.getTransactionManager().createTransaction(Options.NONE).unwrap(FDBRecordContext.class)) {
            try (Transaction transaction = new RecordStoreAndRecordContextTransaction(store, context)) {

                // Then, once we have a transaction that contains both an FDBRecordStore and an FDBRecordContext,
                // connect to a TransactionBoundDatabase
                TransactionBoundEmbeddedRelationalEngine engine = new TransactionBoundEmbeddedRelationalEngine(Options.builder().withOption(Options.Name.PLAN_CACHE_MAX_ENTRIES, 1).build());
                Assertions.assertThat(engine.getPlanCache()).isNotNull()
                        .extracting(planCache -> planCache.getStats().numEntries()).isEqualTo(0L);
                EmbeddedRelationalDriver driver = new EmbeddedRelationalDriver(engine);
                try (RelationalConnection conn = driver.connect(dbRule.getConnectionUri(), transaction, Options.NONE)) {
                    conn.setSchema("TEST_SCHEMA");
                    try (RelationalStatement statement = conn.createStatement()) {
                        Message record = statement.getDataBuilder("RESTAURANT")
                                .setField("REST_NO", 42)
                                .setField("NAME", "FOO")
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
                Assertions.assertThat(engine.getPlanCache().getStats().numEntries()).isEqualTo(1L);
            }
        }
    }
}
