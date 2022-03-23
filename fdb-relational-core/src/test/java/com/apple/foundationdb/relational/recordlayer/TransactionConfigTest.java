/*
 * TransactionConfigTest.java
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

import com.apple.foundationdb.record.Restaurant;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.relational.api.OperationOption;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.TransactionConfig;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.common.collect.Iterators;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

public class TransactionConfigTest {
    @RegisterExtension
    @Order(0)
    public final RecordLayerCatalogRule catalog = new RecordLayerCatalogRule();

    @RegisterExtension
    @Order(1)
    public final RecordLayerTemplateRule template = new RecordLayerTemplateRule("RestaurantRecord", catalog)
            .setRecordFile(Restaurant.getDescriptor())
            .configureTable("RestaurantRecord", table -> table.setPrimaryKey(Key.Expressions.field("rest_no")));

    @RegisterExtension
    @Order(2)
    public final DatabaseRule database = new DatabaseRule("record_layer_transaction_config_test", catalog)
            .withSchema("test", template);

    @Test
    void testRecordInsertionWithTimeOutInConfig() throws RelationalException, SQLException {
        try (RelationalConnection conn = Relational.connect(database.getConnectionUri(), Options.create().withOption(OperationOption.forceVerifyDdl()))) {
            conn.beginTransaction(testTransactionConfig());
            conn.setSchema("test");
            try (RelationalStatement s = conn.createStatement()) {
                long id = System.currentTimeMillis();
                Restaurant.RestaurantRecord r = Restaurant.RestaurantRecord.newBuilder().setName("testRest" + id).setRestNo(id).build();
                s.executeInsert("RestaurantRecord", Iterators.singletonIterator(r), Options.create());
            } catch (RelationalException | SQLException e) {
                Throwable throwable = e.getCause();
                String errorMsg = throwable.getMessage();
                Assertions.assertEquals("Operation aborted because the transaction timed out", errorMsg);
            }
            Map<String, Object> metrics = catalog.getMetrics();
            Assertions.assertTrue(metrics.containsKey("CHECK_VERSION"));
        }
    }

    private TransactionConfig testTransactionConfig() {
        return TransactionConfig.newBuilder()
                .setTransactionId("testTransaction" + UUID.randomUUID())
                .setTransactionTimeoutMillis(1L)
                .build();
    }
}
