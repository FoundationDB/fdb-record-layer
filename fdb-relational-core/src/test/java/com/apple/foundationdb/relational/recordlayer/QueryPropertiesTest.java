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
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordScanLimiterFactory;
import com.apple.foundationdb.record.Restaurant;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.relational.api.DatabaseConnection;
import com.apple.foundationdb.relational.api.OperationOption;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.QueryProperties;
import com.apple.foundationdb.relational.api.Statement;
import com.apple.foundationdb.relational.api.TableScan;
import com.apple.foundationdb.relational.api.Relational;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.RelationalResultSet;
import com.apple.foundationdb.relational.api.catalog.DatabaseTemplate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class QueryPropertiesTest {

    @RegisterExtension
    public final RecordLayerCatalogRule catalog = new RecordLayerCatalogRule();

    @BeforeEach
    public final void setupCatalog(){
        final RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(Restaurant.getDescriptor());
        builder.getRecordType("RestaurantRecord").setPrimaryKey(Key.Expressions.field("rest_no"));
        catalog.createSchemaTemplate(new RecordLayerTemplate("RestaurantRecord", builder.build()));

        catalog.createDatabase(URI.create("/query_properties_test"),
                DatabaseTemplate.newBuilder()
                        .withSchema("test","RestaurantRecord")
                        .build());
    }

    @AfterEach
    void tearDown() {
        catalog.deleteDatabase(URI.create("/query_properties_test"));
    }

    @Test
    void verifyExecuteAndScanPropertiesGivenQueryProperties() {
        final QueryProperties.Builder builder = QueryProperties.newBuilder();

        final ExecuteState trackingExecuteState = new ExecuteState(RecordScanLimiterFactory.tracking(), ByteScanLimiterFactory.tracking());
        final ExecuteProperties executeProperties1 = QueryPropertiesUtils.getExecuteProperties(builder.build());
        Assertions.assertEquals(IsolationLevel.SERIALIZABLE, executeProperties1.getIsolationLevel());
        Assertions.assertEquals(0, executeProperties1.getSkip());
        Assertions.assertEquals(0, executeProperties1.getReturnedRowLimit());
        Assertions.assertEquals(0L, executeProperties1.getTimeLimit());
        Assertions.assertEquals(Integer.MAX_VALUE, executeProperties1.getScannedRecordsLimit());
        Assertions.assertEquals(Long.MAX_VALUE, executeProperties1.getScannedBytesLimit());
        Assertions.assertEquals(trackingExecuteState.toString(), executeProperties1.getState().toString());
        Assertions.assertEquals(false, executeProperties1.isFailOnScanLimitReached());
        Assertions.assertEquals(CursorStreamingMode.ITERATOR, executeProperties1.getDefaultCursorStreamingMode());

        builder.setIsSnapshotIsolation(true);
        final ExecuteProperties executeProperties2 = QueryPropertiesUtils.getExecuteProperties(builder.build());
        Assertions.assertEquals(IsolationLevel.SNAPSHOT, executeProperties2.getIsolationLevel());

        builder.setSkip(2);
        final ExecuteProperties executeProperties3 = QueryPropertiesUtils.getExecuteProperties(builder.build());
        Assertions.assertEquals(2, executeProperties3.getSkip());

        builder.setRowLimit(2);
        final ExecuteProperties executeProperties4 = QueryPropertiesUtils.getExecuteProperties(builder.build());
        Assertions.assertEquals(2, executeProperties4.getReturnedRowLimit());

        builder.setTimeLimit(2L);
        final ExecuteProperties executeProperties5 = QueryPropertiesUtils.getExecuteProperties(builder.build());
        Assertions.assertEquals(2L, executeProperties5.getTimeLimit());

        builder.setScannedRecordsLimit(2);
        final ExecuteProperties executeProperties6 = QueryPropertiesUtils.getExecuteProperties(builder.build());
        final ExecuteState enforceRecordLimitExecuteState = new ExecuteState(RecordScanLimiterFactory.enforce(2), ByteScanLimiterFactory.tracking());
        Assertions.assertEquals(enforceRecordLimitExecuteState.toString(), executeProperties6.getState().toString());

        builder.setScannedBytesLimit(2L);
        final ExecuteProperties executeProperties7 = QueryPropertiesUtils.getExecuteProperties(builder.build());
        final ExecuteState enforceBothExecuteState = new ExecuteState(RecordScanLimiterFactory.enforce(2), ByteScanLimiterFactory.enforce(2L));
        Assertions.assertEquals(enforceBothExecuteState.toString(), executeProperties7.getState().toString());

        builder.setFailOnScanLimitReached(true);
        final ExecuteProperties executeProperties8 = QueryPropertiesUtils.getExecuteProperties(builder.build());
        Assertions.assertEquals(true, executeProperties8.isFailOnScanLimitReached());

        builder.setLoadAllRecordsImmediately(true);
        final ExecuteProperties executeProperties9 = QueryPropertiesUtils.getExecuteProperties(builder.build());
        Assertions.assertEquals(CursorStreamingMode.WANT_ALL, executeProperties9.getDefaultCursorStreamingMode());

        final ScanProperties scanProperties = QueryPropertiesUtils.getScanProperties(builder.build());
        Assertions.assertEquals(false, scanProperties.isReverse());
        Assertions.assertEquals(CursorStreamingMode.WANT_ALL, scanProperties.getCursorStreamingMode());

        builder.setReverse(true);
        ScanProperties scanProperties2 = QueryPropertiesUtils.getScanProperties(builder.build());
        Assertions.assertEquals(true, scanProperties2.isReverse());
    }

    @Test
    void scanWithLimit() {
        final QueryProperties queryProperties = QueryProperties.newBuilder()
                .setRowLimit(1)
                .build();
        final long firstRestNo = System.currentTimeMillis();
        final List<Long> restNoList = testScan(queryProperties, firstRestNo);
        // Only 1 of the 2 saved records is read, due to the limit == 1
        Assertions.assertEquals(ImmutableList.of(firstRestNo), restNoList);
    }

    @Test
    void scanReverse() {
        final QueryProperties queryProperties = QueryProperties.newBuilder()
                .setReverse(true)
                .build();
        final long firstRestNo = System.currentTimeMillis();
        final List<Long> restNoList = testScan(queryProperties, firstRestNo);
        // The 2 records are read in reverse order, due to reverse == true
        Assertions.assertEquals(ImmutableList.of(firstRestNo + 1, firstRestNo), restNoList);
    }

    List<Long> testScan(QueryProperties queryProperties, long firstRestNo) throws RelationalException {
        final URI dbUrl = URI.create("rlsc:embed:/query_properties_test");
        try(DatabaseConnection conn = Relational.connect(dbUrl, Options.create().withOption(OperationOption.forceVerifyDdl()))){
            conn.beginTransaction();
            conn.setSchema("test");
            try(Statement s = conn.createStatement()){
                long id1 = firstRestNo;
                Restaurant.RestaurantRecord r1 = Restaurant.RestaurantRecord.newBuilder().setName("testRest" + id1).setRestNo(id1).build();
                s.executeInsert("RestaurantRecord", Iterators.singletonIterator(r1), Options.create());

                long id2 = id1 + 1;
                Restaurant.RestaurantRecord r2 = Restaurant.RestaurantRecord.newBuilder().setName("testRest" + id2).setRestNo(id2).build();
                s.executeInsert("RestaurantRecord", Iterators.singletonIterator(r2), Options.create());

                TableScan scan = TableScan.newBuilder()
                        .withTableName("RestaurantRecord")
                        .setStartKey("rest_no", id1)
                        .setEndKey("rest_no",id2 + 1)
                        .setScanProperties(queryProperties)
                        .build();
                final RelationalResultSet resultSet = s.executeScan(scan, Options.create());
                return getRestNoList(resultSet);
            } catch (Throwable t) {
                try {
                    conn.rollback();
                }catch(Throwable suppressable){
                    t.addSuppressed(suppressable);
                }
                throw t;
            }
        }
    }

    List<Long> getRestNoList(@Nonnull RelationalResultSet resultSet) {
        List<Long> numbers = new ArrayList<>();
        while (resultSet.next()) {
            numbers.add(resultSet.getLong("rest_no"));
        }
        return numbers;
    }
}
