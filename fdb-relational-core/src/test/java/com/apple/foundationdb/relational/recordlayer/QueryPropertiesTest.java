/*
 * QueryPropertiesTest.java
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

import com.apple.foundationdb.record.ByteScanLimiterFactory;
import com.apple.foundationdb.record.CursorStreamingMode;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.ExecuteState;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordScanLimiterFactory;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.relational.api.EmbeddedRelationalStruct;
import com.apple.foundationdb.relational.api.KeySet;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.RelationalConnection;
import com.apple.foundationdb.relational.api.RelationalDriver;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.RelationalStatement;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.utils.SimpleDatabaseRule;
import com.apple.foundationdb.relational.utils.TestSchemas;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class QueryPropertiesTest {
    @RegisterExtension
    @Order(0)
    public final EmbeddedRelationalExtension relationalExtension = new EmbeddedRelationalExtension();

    @RegisterExtension
    @Order(1)
    public final SimpleDatabaseRule database = new SimpleDatabaseRule(QueryPropertiesTest.class, TestSchemas.restaurant());

    @Test
    void verifyExecuteAndScanPropertiesGivenQueryProperties() throws SQLException {
        Options options = Options.NONE;

        final ExecuteState trackingExecuteState = new ExecuteState(RecordScanLimiterFactory.tracking(), ByteScanLimiterFactory.tracking());
        final ExecuteProperties executeProperties1 = QueryPropertiesUtils.getExecuteProperties(options);
        Assertions.assertEquals(IsolationLevel.SERIALIZABLE, executeProperties1.getIsolationLevel());
        Assertions.assertEquals(0, executeProperties1.getSkip());
        Assertions.assertEquals(0, executeProperties1.getReturnedRowLimit());
        Assertions.assertEquals(0L, executeProperties1.getTimeLimit());
        Assertions.assertEquals(Integer.MAX_VALUE, executeProperties1.getScannedRecordsLimit());
        Assertions.assertEquals(Long.MAX_VALUE, executeProperties1.getScannedBytesLimit());
        Assertions.assertEquals(trackingExecuteState.toString(), executeProperties1.getState().toString());
        Assertions.assertFalse(executeProperties1.isFailOnScanLimitReached());
        Assertions.assertEquals(CursorStreamingMode.ITERATOR, executeProperties1.getDefaultCursorStreamingMode());

        options = Options.builder().withOption(Options.Name.MAX_ROWS, 2).build();
        final ExecuteProperties executeProperties4 = QueryPropertiesUtils.getExecuteProperties(options);
        Assertions.assertEquals(2, executeProperties4.getReturnedRowLimit());

        final ScanProperties scanProperties = QueryPropertiesUtils.getScanProperties(options);
        Assertions.assertFalse(scanProperties.isReverse());
        Assertions.assertEquals(CursorStreamingMode.ITERATOR, scanProperties.getCursorStreamingMode());
    }

    @Test
    void scanWithLimit() throws RelationalException, SQLException {
        final long firstRestNo = System.currentTimeMillis();
        final List<Long> restNoList = testScan(Options.builder().withOption(Options.Name.MAX_ROWS, 1).build(), firstRestNo);
        // Only 1 of the 2 saved records is read, due to the limit == 1
        Assertions.assertEquals(ImmutableList.of(firstRestNo), restNoList);
    }

    List<Long> testScan(Options options, long firstRestNo) throws RelationalException, SQLException {
        final var driver = (RelationalDriver) DriverManager.getDriver(database.getConnectionUri().toString());
        try (RelationalConnection conn = driver.connect(database.getConnectionUri(), options)) {
            conn.setSchema("TEST_SCHEMA");
            try (RelationalStatement s = conn.createStatement()) {
                for (long i = 0; i < 2; i++) {
                    long id = firstRestNo + i;
                    var restaurant = EmbeddedRelationalStruct.newBuilder()
                            .addString("NAME", "testRest" + id)
                            .addLong("REST_NO", id)
                            .build();
                    s.executeInsert("RESTAURANT", restaurant);
                }

                KeySet keySet = new KeySet().setKeyColumn("REST_NO", firstRestNo);
                final RelationalResultSet resultSet = s.executeScan("RESTAURANT", keySet, Options.NONE);
                return getRestNoList(resultSet);
            } catch (Throwable t) {
                try {
                    conn.rollback();
                } catch (Throwable suppressable) {
                    t.addSuppressed(suppressable);
                }
                throw t;
            }
        }
    }

    List<Long> getRestNoList(@Nonnull RelationalResultSet resultSet) throws SQLException {
        List<Long> numbers = new ArrayList<>();
        while (resultSet.next()) {
            numbers.add(resultSet.getLong("REST_NO"));
        }
        return numbers;
    }
}
