/*
 * FDBRecordStoreDeleteWhereTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsWithHeaderProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.metadata.Key.Expressions.keyWithValue;
import static com.apple.foundationdb.record.metadata.Key.Expressions.recordType;
import static com.apple.foundationdb.record.metadata.Key.Expressions.value;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.scan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.scoreForRank;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests related to {@link FDBRecordStore#deleteRecordsWhere(QueryComponent)} and its variants.
 */
@Tag(Tags.RequiresFDB)
public class FDBRecordStoreDeleteWhereTest extends FDBRecordStoreTestBase {

    @ParameterizedTest(name = "testDeleteWhere[useCountIndex={0}]" )
    @BooleanSource
    void testDeleteWhere(boolean useCountIndex) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeaderPrimaryKey(context, useCountIndex);

            saveHeaderRecord(1, "a", 0, "lynx");
            saveHeaderRecord(1, "b", 1, "bobcat");
            saveHeaderRecord(1, "c", 2, "panther");

            saveHeaderRecord(2, "a", 3, "jaguar");
            saveHeaderRecord(2, "b", 4, "leopard");
            saveHeaderRecord(2, "c", 5, "lion");
            saveHeaderRecord(2, "d", 6, "tiger");
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            final KeyExpression groupExpr = openRecordWithHeaderPrimaryKey(context, useCountIndex);

            assertEquals(3, recordStore.getSnapshotRecordCount(groupExpr, Key.Evaluated.scalar(1)).join().longValue());

            recordStore.deleteRecordsWhere(Query.field("header").matches(Query.field("rec_no").equalsValue(1)));
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            final KeyExpression groupExpr = openRecordWithHeaderPrimaryKey(context, useCountIndex);

            assertEquals(0, recordStore.getSnapshotRecordCount(groupExpr, Key.Evaluated.scalar(1)).join().longValue());

            int expectedNum = 3;
            for (FDBStoredRecord<Message> storedRecord : recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asList().join()) {
                TestRecordsWithHeaderProto.MyRecord rec = parseMyRecord(storedRecord.getRecord());
                assertEquals(2, rec.getHeader().getRecNo());
                assertEquals(expectedNum++, rec.getHeader().getNum());
            }
            assertEquals(7, expectedNum);
            expectedNum = 3;
            for (FDBIndexedRecord<Message> indexedRecord : recordStore.scanIndexRecords("MyRecord$str_value").asList().join()) {
                TestRecordsWithHeaderProto.MyRecord rec = parseMyRecord(indexedRecord.getRecord());
                assertEquals(2, rec.getHeader().getRecNo());
                assertEquals(expectedNum++, rec.getHeader().getNum());
            }
            assertEquals(7, expectedNum);
            commit(context);
        }
    }

    @Test
    void testDeleteWhereGroupedCount() throws Exception {
        KeyExpression groupExpr = concat(
                field("header").nest(field("rec_no")),
                field("header").nest(field("path")));
        RecordMetaDataHook hook = metaData -> {
            metaData.getRecordType("MyRecord")
                    .setPrimaryKey(concat(
                            field("header").nest(field("rec_no")),
                            field("header").nest(field("path")),
                            field("header").nest(field("num"))));

            metaData.addUniversalIndex(new Index("MyRecord$groupedCount", new GroupingKeyExpression(groupExpr, 0), IndexTypes.COUNT));
            metaData.addUniversalIndex(new Index("MyRecord$groupedUpdateCount", new GroupingKeyExpression(groupExpr, 0), IndexTypes.COUNT_UPDATES));
        };
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);

            saveHeaderRecord(1, "a", 0, "lynx");
            saveHeaderRecord(1, "a", 1, "bobcat");
            saveHeaderRecord(1, "b", 2, "panther");

            saveHeaderRecord(2, "a", 3, "jaguar");
            saveHeaderRecord(2, "b", 4, "leopard");
            saveHeaderRecord(2, "c", 5, "lion");
            saveHeaderRecord(2, "d", 6, "tiger");
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);

            // Number of records where first component of primary key is 1
            assertEquals(3, recordStore.getSnapshotRecordCount(groupExpr, Key.Evaluated.scalar(1)).join().longValue());

            // Number of updates to such records
            assertEquals(3, recordStore.getSnapshotRecordUpdateCount(groupExpr, Key.Evaluated.scalar(1)).join().longValue());

            recordStore.deleteRecordsWhere(Query.and(
                    Query.field("header").matches(Query.field("rec_no").equalsValue(1)),
                    Query.field("header").matches(Query.field("path").equalsValue("a"))));

            assertEquals(1, recordStore.getSnapshotRecordCount(groupExpr, Key.Evaluated.scalar(1)).join().longValue());

            // Deleting by group prefix resets the update counter for the group(s)
            assertEquals(1, recordStore.getSnapshotRecordUpdateCount(groupExpr, Key.Evaluated.scalar(1)).join().longValue());

            commit(context);
        }
    }

    private long getGroupedSum(Index sumIndex, Key.Evaluated evaluated) {
        IndexAggregateFunction sumAggregate = new IndexAggregateFunction(FunctionNames.SUM, sumIndex.getRootExpression(), sumIndex.getName());
        Tuple sumTuple = recordStore.evaluateAggregateFunction(List.of("MyRecord"), sumAggregate, evaluated, IsolationLevel.SERIALIZABLE)
                .join();
        assertNotNull(sumTuple, "sum aggregate result should not be null");
        return sumTuple.getLong(0);
    }

    private long getGroupedSumByPath(Index sumIndex, String path) {
        return getGroupedSum(sumIndex, Key.Evaluated.scalar(path));
    }

    @ParameterizedTest(name = "testDeleteWhereUngroupedSum[recordTypePrefix={0}]")
    @BooleanSource
    void testDeleteWhereUngroupedSum(boolean recordTypePrefix) throws Exception {
        final Random random = new Random();
        final Index sumIndex = new Index("MyRecord$sum_num",
                field("header").nest(field("num")).ungrouped(),
                IndexTypes.SUM);
        RecordMetaDataHook hook = metaData -> {
            final RecordTypeBuilder recordType = metaData.getRecordType("MyRecord");
            final KeyExpression basePrimaryKey = field("header").nest(concatenateFields("num", "rec_no"));
            if (recordTypePrefix) {
                recordType.setPrimaryKey(concat(recordType(), basePrimaryKey));
            } else {
                recordType.setPrimaryKey(basePrimaryKey);
            }
            metaData.addIndex(recordType, sumIndex);
        };
        int sum = 0;
        List<Integer> nums = new ArrayList<>();
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);
            for (long recNo = 0; recNo < 20; recNo++) {
                int num = random.nextInt(10);
                saveHeaderRecord(recNo, "path", num, "unused");
                sum += num;
                nums.add(num);
            }
            assertEquals(sum, getGroupedSum(sumIndex, Key.Evaluated.EMPTY));
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);
            int numToDelete = nums.get(random.nextInt(nums.size()));
            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MyRecord")
                    .setFilter(Query.field("header").matches(Query.field("num").equalsValue(numToDelete)))
                    .build();
            List<TestRecordsWithHeaderProto.MyRecord> existingRecords = queryMyRecords(query);
            assertThat(existingRecords, not(empty()));

            // Try deleting without restricting the record type
            QueryComponent predicate = Query.field("header").matches(Query.field("num").equalsValue(numToDelete));
            Query.InvalidExpressionException err1 = assertThrows(Query.InvalidExpressionException.class, () -> recordStore.deleteRecordsWhere(predicate));
            if (recordTypePrefix) {
                assertThat(err1.getMessage(), containsString("deleteRecordsWhere not matching primary key MyRecord"));
            } else {
                assertThat(err1.getMessage(), indexDoesNotSupportDeleteWhere(sumIndex));
            }
            // Try deleting while restricting to just the one record type
            Exception err2 = assertThrows(Exception.class, () -> recordStore.deleteRecordsWhere("MyRecord", predicate));
            if (recordTypePrefix) {
                assertThat(err2, instanceOf(Query.InvalidExpressionException.class));
                assertThat(err2.getMessage(), indexDoesNotSupportDeleteWhere(sumIndex));
            } else {
                assertThat(err2, instanceOf(RecordCoreException.class));
                assertThat(err2.getMessage(), containsString("record type version of deleteRecordsWhere can only be used when all record types have a type prefix"));
            }

            // Assert that the sum and records where not affected by the failed deletes
            assertEquals(sum, getGroupedSum(sumIndex, Key.Evaluated.EMPTY));
            assertEquals(existingRecords, queryMyRecords(query));

            if (recordTypePrefix) {
                recordStore.deleteRecordsWhere("MyRecord", null);
                assertEquals(0, getGroupedSum(sumIndex, Key.Evaluated.EMPTY));
                assertThat(queryMyRecords(RecordQuery.newBuilder().setRecordType("MyRecord").build()), empty());
            } else {
                // In theory, because there's only one record type in the meta-data, this should work. If this is changed in
                // the future, then the assertions from the recordTypePrefix case can be collapsed with this case
                RecordCoreException err3 = assertThrows(RecordCoreException.class, () -> recordStore.deleteRecordsWhere("MyRecord", null));
                assertThat(err3.getMessage(), containsString("record type version of deleteRecordsWhere can only be used when all record types have a type prefix"));
            }
        }
    }

    @Test
    void testDeleteWhereGroupedSum() throws Exception {
        final Random random = new Random();
        final Index sumIndex = new Index("MyRecord$sum_num_by_path",
                new GroupingKeyExpression(field("header").nest(concatenateFields("path", "num")), 1),
                IndexTypes.SUM);
        RecordMetaDataHook hook = metaData -> {
            final RecordTypeBuilder recordType = metaData.getRecordType("MyRecord");
            recordType.setPrimaryKey(field("header").nest(concatenateFields("path", "num", "rec_no")));
            metaData.addIndex(recordType, sumIndex);
        };

        final List<String> paths = List.of("a", "b", "c", "d");
        final List<TestRecordsWithHeaderProto.MyRecord> saved = new ArrayList<>();
        final Map<String, Integer> sumForPath = new HashMap<>();
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);
            final long maxRecNo = 20L;
            for (String path : paths) {
                for (int recNo = 0; recNo < maxRecNo; recNo++) {
                    saved.add(saveHeaderRecord(recNo, path, random.nextInt(20), "str_value"));
                }
            }
            for (String path : paths) {
                int expectedSum = saved.stream()
                        .filter(rec -> rec.getHeader().getPath().equals(path))
                        .mapToInt(rec -> rec.getHeader().getNum())
                        .sum();
                assertEquals(expectedSum, getGroupedSumByPath(sumIndex, path));
                sumForPath.put(path, expectedSum);
            }
            commit(context);
        }

        // Delete single path
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);

            final String pathToDelete = paths.get(random.nextInt(paths.size()));
            recordStore.deleteRecordsWhere(Query.field("header").matches(Query.field("path").equalsValue(pathToDelete)));
            List<TestRecordsWithHeaderProto.MyRecord> remaining = queryMyRecords(RecordQuery.newBuilder().setRecordType("MyRecord").build());
            remaining.forEach(remainingRecord -> assertNotEquals(pathToDelete, remainingRecord.getHeader().getPath(),
                    () -> "record with path " + pathToDelete + " should have been deleted: " + remainingRecord));
            assertThat(remaining, containsInAnyOrder(saved.stream()
                    .filter(rec -> !rec.getHeader().getPath().equals(pathToDelete))
                    .map(Matchers::equalTo)
                    .collect(Collectors.toList())));

            for (String path : paths) {
                int expectedSum = path.equals(pathToDelete) ? 0 : sumForPath.get(path);
                assertEquals(expectedSum, getGroupedSumByPath(sumIndex, path), "sum should be unaffected by delete where except for deleted range");
            }

            // do not commit
        }

        // Attempt to delete in a way that crosses grouping key of count index
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);

            final String pathToDelete = paths.get(random.nextInt(paths.size()));
            TestRecordsWithHeaderProto.MyRecord myRecord = saved.stream()
                    .filter(savedRecord -> savedRecord.getHeader().getPath().equals(pathToDelete))
                    .findAny()
                    .orElseGet(() -> fail("should have saved header records for every path including " + pathToDelete));
            TestRecordsWithHeaderProto.HeaderRecord header = myRecord.getHeader();
            final Tuple primaryKey = Tuple.from(header.getPath(), header.getNum(), header.getRecNo());
            assertNotNull(recordStore.loadRecord(primaryKey), "record should be present before delete where");
            Query.InvalidExpressionException err = assertThrows(Query.InvalidExpressionException.class,
                    () -> recordStore.deleteRecordsWhere(Query.field("header").matches(Query.and(
                            Query.field("path").equalsValue(pathToDelete),
                            Query.field("num").equalsValue(myRecord.getHeader().getNum())))));
            assertThat(err.getMessage(), indexDoesNotSupportDeleteWhere(sumIndex));
            assertNotNull(recordStore.loadRecord(primaryKey), "record should not have been deleted by unsuccessful delete where");
        }
    }

    @ParameterizedTest(name = "testDeleteWherePermutedMinMax[max={0}]")
    @BooleanSource
    void testDeleteWherePermutedMinMax(boolean max) throws Exception {
        final Random random = new Random();
        // Equivalent of to an index on:
        //    path, min/max(rec_no), num
        // That is, the aggregate is grouped by (path, num) but ordered first by path, then the aggregate, then num
        final Index extremumIndex = new Index("MyRecord$extremum_recno_by_num_by_path",
                new GroupingKeyExpression(field("header").nest(concatenateFields("path", "num", "rec_no")), 1),
                max ? IndexTypes.PERMUTED_MAX : IndexTypes.PERMUTED_MIN,
                Map.of(IndexOptions.PERMUTED_SIZE_OPTION, "1"));
        RecordMetaDataHook hook = metaData -> {
            final RecordTypeBuilder recordType = metaData.getRecordType("MyRecord");
            recordType.setPrimaryKey(field("header").nest(concatenateFields("path", "num", "rec_no")));
            metaData.addIndex(recordType, extremumIndex);
        };

        final List<String> paths = List.of("a", "b", "c", "d");
        final List<TestRecordsWithHeaderProto.MyRecord> saved = new ArrayList<>();
        final Map<String, Map<Integer, Long>> extremeRecNosForPath = new HashMap<>();
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);
            final long maxRecNo = 20L;
            for (String path : paths) {
                for (int recNo = 0; recNo < maxRecNo; recNo++) {
                    saved.add(saveHeaderRecord(recNo, path, random.nextInt(5), "str_value"));
                }
            }
            for (String path : paths) {
                Map<Integer, Long> forPath = new HashMap<>();
                saved.stream()
                        .filter(rec -> rec.getHeader().getPath().equals(path))
                        .forEach(rec -> {
                            int num = rec.getHeader().getNum();
                            long recNo = rec.getHeader().getRecNo();
                            forPath.compute(num, (key, existing) -> existing == null ? recNo : (max ? Math.max(recNo, existing) : Math.min(recNo, existing)));
                        });
                extremeRecNosForPath.put(path, forPath);
            }
            commit(context);
        }

        final String pathToDelete = paths.get(random.nextInt(paths.size()));

        // Attempt to delete a path and num. This should fail, as it does not align with the permuted min/max index (because of the permuted size option)
        // Note that it _would_ align if we didn't permute the values here
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);

            final QueryComponent component = Query.field("header").matches(Query.and(
                    Query.field("path").equalsValue(pathToDelete),
                    Query.field("num").equalsValue(2)
            ));
            Query.InvalidExpressionException err = assertThrows(Query.InvalidExpressionException.class, () -> recordStore.deleteRecordsWhere(component));
            assertThat(err.getMessage(), indexDoesNotSupportDeleteWhere(extremumIndex));

            commit(context);
        }

        // Delete single path
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);

            recordStore.deleteRecordsWhere(Query.field("header").matches(Query.field("path").equalsValue(pathToDelete)));

            List<TestRecordsWithHeaderProto.MyRecord> remaining = queryMyRecords(RecordQuery.newBuilder().setRecordType("MyRecord").build());
            remaining.forEach(remainingRecord -> assertNotEquals(pathToDelete, remainingRecord.getHeader().getPath(),
                    () -> "record with path " + pathToDelete + " should have been deleted: " + remainingRecord));
            assertThat(remaining, containsInAnyOrder(saved.stream()
                    .filter(rec -> !rec.getHeader().getPath().equals(pathToDelete))
                    .map(Matchers::equalTo)
                    .collect(Collectors.toList())));

            for (String path : paths) {
                // When scanned BY_GROUP, the index keeps one entry (containing the extremum rec_no for each group). Make sure the ones for the deleted path are gone
                try (RecordCursor<IndexEntry> entryCursor = recordStore.scanIndex(extremumIndex, IndexScanType.BY_GROUP, TupleRange.allOf(Tuple.from(path)), null, ScanProperties.FORWARD_SCAN)) {
                    Map<Integer, Long> extremeByNum = new HashMap<>();
                    for (RecordCursorResult<IndexEntry> result = entryCursor.getNext(); result.hasNext(); result = entryCursor.getNext()) {
                        IndexEntry entry = Objects.requireNonNull(result.get());
                        assertNull(extremeByNum.put((int)entry.getKey().getLong(2), entry.getKey().getLong(1)));
                    }
                    if (path.equals(pathToDelete)) {
                        assertThat(extremeByNum.entrySet(), empty());
                    } else {
                        assertEquals(extremeRecNosForPath.get(path), extremeByNum);
                    }
                }
                // When scanned BY_VALUE, the index keeps one entry per record. Make sure the ones for the deleted path are gone
                try (RecordCursor<FDBIndexedRecord<Message>> recordCursor = recordStore.scanIndexRecords(extremumIndex, IndexScanType.BY_VALUE, TupleRange.allOf(Tuple.from(path)), null, IndexOrphanBehavior.ERROR, ScanProperties.FORWARD_SCAN)) {
                    List<TestRecordsWithHeaderProto.MyRecord> recordsFromIndex = recordCursor
                            .map(rec -> TestRecordsWithHeaderProto.MyRecord.newBuilder().mergeFrom(rec.getRecord()).build())
                            .asList()
                            .get();
                    if (path.equals(pathToDelete)) {
                        assertThat(recordsFromIndex, empty());
                    } else {
                        assertThat(recordsFromIndex, containsInAnyOrder(saved.stream()
                                .filter(rec -> rec.getHeader().getPath().equals(path))
                                .map(Matchers::equalTo)
                                .collect(Collectors.toList())));
                    }
                }
            }

            commit(context);
        }
    }

    @Test
    void testDeleteWhereGroupSumSplitsFunction() throws Exception {
        final Random random = new Random();
        // The "transpose" function flips its arguments, so this index is effectively "sum the rec_no field, grouped by path and num
        final Index sumIndex = new Index("MyRecord$sum_rec_no_by_path_and_num",
                new GroupingKeyExpression(concat(field("header").nest(field("path")), function("transpose", field("header").nest(concatenateFields("rec_no", "num")))), 1),
                IndexTypes.SUM);
        RecordMetaDataHook hook = metaData -> {
            final RecordTypeBuilder recordType = metaData.getRecordType("MyRecord");
            recordType.setPrimaryKey(field("header").nest(concatenateFields("path", "rec_no")));
            metaData.addIndex(recordType, sumIndex);
        };

        final List<String> paths = List.of("gravel", "dirt", "bike", "garden", "brick");
        final List<TestRecordsWithHeaderProto.MyRecord> saved = new ArrayList<>();
        final Map<String, Long> sumsByPath = new HashMap<>();
        final Map<String, Map<Integer, Long>> sumsByPathAndNum = new HashMap<>();
        final int maxNum = 5;
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);
            for (String path : paths) {
                for (int recNo = 0; recNo < 30; recNo++) {
                    saved.add(saveHeaderRecord(recNo, path, random.nextInt(maxNum), "unread"));
                }
            }

            for (String path : paths) {
                Map<Integer, Long> sumsByNum = new HashMap<>();
                for (int i = 0; i < maxNum; i++) {
                    final int num = i;
                    long expectedSum = saved.stream()
                            .filter(savedRecord -> savedRecord.getHeader().getPath().equals(path) && savedRecord.getHeader().getNum() == num)
                            .mapToLong(savedRecord -> savedRecord.getHeader().getRecNo())
                            .sum();
                    assertEquals(expectedSum, getGroupedSum(sumIndex, Key.Evaluated.concatenate(path, num)));
                    sumsByNum.put(num, expectedSum);
                }
                sumsByPathAndNum.put(path, sumsByNum);

                long expectedSum = sumsByNum.values().stream()
                        .mapToLong(Long::longValue)
                        .sum();
                assertEquals(expectedSum, getGroupedSumByPath(sumIndex, path));
                sumsByPath.put(path, expectedSum);

            }

            commit(context);
        }

        // Delete a single path. This should work because header.path is a prefix of (though not the full prefix of)
        // the key expression on which the index is defined
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);

            final String pathToDelete = paths.get(random.nextInt(paths.size()));
            recordStore.deleteRecordsWhere(Query.field("header").matches(Query.field("path").equalsValue(pathToDelete)));

            // Assert that the deleted path is gone, but all other path sums should be the same
            for (String path : paths) {
                // Get the sum for each value of num
                Map<Integer, Long> sumByNum = sumsByPathAndNum.get(path);
                for (Map.Entry<Integer, Long> numAndSum : sumByNum.entrySet()) {
                    long expectedSum = path.equals(pathToDelete) ? 0L : numAndSum.getValue();
                    assertEquals(expectedSum, getGroupedSum(sumIndex, Key.Evaluated.concatenate(path, numAndSum.getKey())));
                }

                // Get the aggregate sum for an entire path
                long expectedSum = path.equals(pathToDelete) ? 0L : sumsByPath.get(path);
                assertEquals(expectedSum, getGroupedSumByPath(sumIndex, path));
            }

            // do not commit
        }

        // Try to delete in a way that would be inconsistent with the sum index
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);
            final String pathToDelete = paths.get(random.nextInt(paths.size()));
            Query.InvalidExpressionException err = assertThrows(Query.InvalidExpressionException.class,
                    () -> recordStore.deleteRecordsWhere(Query.field("header").matches(Query.and(
                            Query.field("path").equalsValue(pathToDelete),
                            Query.field("rec_no").equalsValue(3L)))));

            assertThat(err.getMessage(), indexDoesNotSupportDeleteWhere(sumIndex));
        }
    }

    @Test
    void testDeleteWhereRank() throws Exception {
        final Random random = new Random();
        final Index rankIndex = new Index(
                "MyRecord$num_by_path",
                new GroupingKeyExpression(field("header").nest(concatenateFields("path", "num")), 1),
                IndexTypes.RANK);
        RecordMetaDataHook hook = metaData -> {
            final RecordTypeBuilder recordType = metaData.getRecordType("MyRecord");
            recordType.setPrimaryKey(field("header").nest(concatenateFields("path", "num", "rec_no")));
            metaData.addIndex(recordType, rankIndex);
        };

        final List<String> paths = List.of("newark", "hoboken", "journal_square", "33rd_street");
        final RecordQueryPlan plan;
        final Map<String, List<TestRecordsWithHeaderProto.MyRecord>> queryResultsByPath = new HashMap<>();

        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);
            final List<TestRecordsWithHeaderProto.MyRecord> savedRecords = new ArrayList<>();
            for (String path : paths) {
                for (long recNo = 0; recNo < 15; recNo++) {
                    savedRecords.add(saveHeaderRecord(recNo, path, random.nextInt(10), "unused"));
                }
            }

            GroupingKeyExpression rankGroup = (GroupingKeyExpression) rankIndex.getRootExpression();
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MyRecord")
                    .setFilter(Query.and(
                            Query.field("header").matches(Query.field("path").equalsParameter("path_value")),
                            Query.rank(rankGroup).greaterThan(2),
                            Query.rank(rankGroup).lessThanOrEquals(6)))
                    .build();
            plan = recordStore.planQuery(query);
            // Plan should use the index to get ranks for the endpoints, and then it can scan the main record index between
            // the bounds to resolve the records
            assertThat(plan, scoreForRank(containsInAnyOrder(
                    hasToString("__rank_0 = " + rankIndex.getName() +  ".score_for_rank($path_value, 2)"),
                    hasToString("__rank_1 = " + rankIndex.getName() + ".score_for_rank_else_skip($path_value, 6)")
            ), scan(bounds(hasTupleString("[EQUALS $path_value, [GREATER_THAN $__rank_0 && LESS_THAN_OR_EQUALS $__rank_1]]")))));

            for (String path : paths) {
                List<TestRecordsWithHeaderProto.MyRecord> sortedForPath = savedRecords.stream()
                        .filter(myRecord -> myRecord.getHeader().getPath().equals(path))
                        .sorted((rec1, rec2) -> {
                            int numComparison = Integer.compare(rec1.getHeader().getNum(), rec2.getHeader().getNum());
                            if (numComparison != 0) {
                                return numComparison;
                            } else {
                                return Long.compare(rec1.getHeader().getRecNo(), rec2.getHeader().getRecNo());
                            }
                        })
                        .collect(Collectors.toList());
                Set<Integer> numsInRankRange = sortedForPath.stream()
                        .map(myRecord -> myRecord.getHeader().getNum())
                        .distinct()
                        .skip(3)
                        .limit(4)
                        .collect(Collectors.toSet());
                List<TestRecordsWithHeaderProto.MyRecord> expectedResults = sortedForPath.stream()
                        .filter(myRecord -> numsInRankRange.contains(myRecord.getHeader().getNum()))
                        .collect(Collectors.toList());

                assertEquals(expectedResults, queryMyRecords(plan, EvaluationContext.forBinding("path_value", path)));
                queryResultsByPath.put(path, expectedResults);
            }

            commit(context);
        }

        // Try to delete a single path
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);

            String pathToDelete = paths.get(random.nextInt(paths.size()));
            recordStore.deleteRecordsWhere(Query.field("header").matches(Query.field("path").equalsValue(pathToDelete)));

            for (String path : paths) {
                final List<TestRecordsWithHeaderProto.MyRecord> recordsAfterDelete = queryMyRecords(plan, EvaluationContext.forBinding("path_value", path));
                assertEquals(path.equals(pathToDelete) ? Collections.emptyList() : queryResultsByPath.get(path), recordsAfterDelete);
            }

            // do not commit
        }

        // Attempt to delete with a group that would cross the group boundary of the rank index
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);
            String pathToDelete = paths.get(random.nextInt(paths.size()));
            Query.InvalidExpressionException err = assertThrows(Query.InvalidExpressionException.class,
                    () -> recordStore.deleteRecordsWhere(Query.field("header").matches(Query.and(
                            Query.field("path").equalsValue(pathToDelete),
                            Query.field("num").equalsValue(4)))));
            assertThat(err.getMessage(), indexDoesNotSupportDeleteWhere(rankIndex));
        }
    }

    @Test
    void testDeleteWhereMissingPrimaryKey() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeaderPrimaryKey(context, false);
            assertThrows(Query.InvalidExpressionException.class, () ->
                    recordStore.deleteRecordsWhere(Query.field("header").matches(Query.field("path").equalsValue(1))));
        }
    }

    @Test
    void testDeleteWhereMissingIndex() {
        try (FDBRecordContext context = openContext()) {
            RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestRecordsWithHeaderProto.getDescriptor());
            builder.getRecordType("MyRecord")
                    .setPrimaryKey(field("header").nest(concatenateFields("rec_no", "path")));
            builder.addIndex("MyRecord", "MyRecord$str_value", concat(field("header").nest("path"),
                    field("str_value")));
            RecordMetaData metaData = builder.getRecordMetaData();
            createOrOpenRecordStore(context, metaData);
            assertThrows(Query.InvalidExpressionException.class, () ->
                    recordStore.deleteRecordsWhere(Query.field("header").matches(Query.field("rec_no").equalsValue(1))));
        }
    }

    @Test
    void testDeleteWhereNullPredicate() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeaderPrimaryKey(context, false);
            // Ensure that the null predicate doesn't match records. In theory, we could choose to interpret this
            // as the same thing as recordStore.deleteAllData. If we do, we should change this test the new behavior,
            // but for now, this tests what the current behavior is so that we can notice if it changes
            assertThrows(Query.InvalidExpressionException.class,
                    () -> recordStore.deleteRecordsWhere(null));
        }
    }

    @Test
    void testDeleteWhereWithFunctionIndexSplit() throws Exception {
        // Index key is (header.path, first three characters of str_value)
        // Value is remaining suffix of str_value
        Index splitStringIndex = new Index(
                "split_string_index",
                keyWithValue(concat(field("header").nest("path"), function("split_string", concat(field("str_value"), value(3L)))), 2)
        );
        final RecordMetaDataHook hook = metaData -> {
            RecordTypeBuilder typeBuilder = metaData.getRecordType("MyRecord");
            typeBuilder.setPrimaryKey(field("header").nest(concatenateFields("path", "rec_no")));
            metaData.addIndex(typeBuilder, splitStringIndex);
        };

        final Map<String, TestRecordsWithHeaderProto.MyRecord> recordsByPath = insertRecordsByPath(hook);
        final String path = deleteByPath(hook, recordsByPath,
                pathToDelete -> recordStore.deleteRecordsWhere(Query.field("header").matches(Query.field("path").equalsValue(pathToDelete))));
        recordsByPath.remove(path);

        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);
            assertThat("index should have no entries for deleted path",
                    recordStore.scanIndex(splitStringIndex, IndexScanType.BY_VALUE, TupleRange.allOf(Tuple.from(path)), null, ScanProperties.FORWARD_SCAN).asList().get(),
                    empty());
            assertThat("index should have one entry for each non-deleted record",
                    recordStore.scanIndexRecords(splitStringIndex.getName()).map(indexedRecord -> TestRecordsWithHeaderProto.MyRecord.newBuilder().mergeFrom(indexedRecord.getRecord()).build()).asList().get(),
                    containsInAnyOrder(recordsByPath.values().stream().map(Matchers::equalTo).collect(Collectors.toList())));
        }
    }

    @Test
    void testDeleteWhereSingleTypeWithFunctionIndexSplit() throws Exception {
        // Index key is (header.path, first three characters of str_value)
        // Value is remaining suffix of str_value
        Index splitStringIndex = new Index(
                "split_string_index",
                keyWithValue(concat(field("header").nest("path"), function("split_string", concat(field("str_value"), value(3L)))), 2)
        );
        final RecordMetaDataHook hook = metaData -> {
            RecordTypeBuilder typeBuilder = metaData.getRecordType("MyRecord");
            typeBuilder.setPrimaryKey(concat(recordType(), field("header").nest(concatenateFields("path", "rec_no"))));
            metaData.addIndex(typeBuilder, splitStringIndex);
        };

        final Map<String, TestRecordsWithHeaderProto.MyRecord> recordsByPath = insertRecordsByPath(hook);
        final String path = deleteByPath(hook, recordsByPath,
                pathToDelete -> recordStore.deleteRecordsWhere(
                        TestRecordsWithHeaderProto.MyRecord.getDescriptor().getName(),
                        Query.field("header").matches(Query.field("path").equalsValue(pathToDelete))));
        recordsByPath.remove(path);

        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);
            assertThat("index should have no entries for deleted path",
                    recordStore.scanIndex(splitStringIndex, IndexScanType.BY_VALUE, TupleRange.allOf(Tuple.from(path)), null, ScanProperties.FORWARD_SCAN).asList().get(),
                    empty());
            assertThat("index should have one entry for each non-deleted record",
                    recordStore.scanIndexRecords(splitStringIndex.getName()).map(indexedRecord -> TestRecordsWithHeaderProto.MyRecord.newBuilder().mergeFrom(indexedRecord.getRecord()).build()).asList().get(),
                    containsInAnyOrder(recordsByPath.values().stream().map(Matchers::equalTo).collect(Collectors.toList())));
        }
    }

    @Test
    void testDeleteWhereSingleTypeEmptyPredicate() throws Exception {
        // Index on a single type that puts only the first 3 characters of str_value into the key of the index,
        // the rest of the suffix going into the value.
        Index splitStringIndex = new Index(
                "split_string_index",
                keyWithValue(function("split_string", concat(field("str_value"), value(3L))), 1)
        );
        final RecordMetaDataHook hook = metaData -> {
            RecordTypeBuilder typeBuilder = metaData.getRecordType("MyRecord");
            typeBuilder.setPrimaryKey(concat(recordType(), field("header").nest(field("rec_no"))));
            metaData.addIndex(typeBuilder, splitStringIndex);
        };
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);

            TestRecordsWithHeaderProto.MyRecord rec1 = saveHeaderRecord(1066L, "unused_path", 42, "string");
            TestRecordsWithHeaderProto.MyRecord rec2 = saveHeaderRecord(1623L, "unused_path", 42, "stripe");
            TestRecordsWithHeaderProto.MyRecord rec3 = saveHeaderRecord(1412L, "unused_path", 42, "stale");
            List<TestRecordsWithHeaderProto.MyRecord> results = asMyRecordList(recordStore.scanIndexRecords(splitStringIndex.getName(), IndexScanType.BY_VALUE, TupleRange.allOf(Tuple.from("str")), null, ScanProperties.FORWARD_SCAN));
            assertThat(results, containsInAnyOrder(rec1, rec2));
            assertThat(results, not(contains(rec3)));

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);

            // Predicate on rec_no should not be matched
            Query.InvalidExpressionException err = assertThrows(Query.InvalidExpressionException.class,
                    () -> recordStore.deleteRecordsWhere("MyRecord", Query.field("header").matches(Query.field("rec_no").equalsValue(1066L))));
            assertThat(err.getMessage(), indexDoesNotSupportDeleteWhere(splitStringIndex));
            assertNotNull(recordStore.loadRecord(Tuple.from(recordStore.getRecordMetaData().getRecordType("MyRecord").getRecordTypeKey(), 1066L)));

            // Single type delete should be satisfied by clearing out the index
            recordStore.deleteRecordsWhere("MyRecord", null);
            assertThat(recordStore.scanIndexRecords(splitStringIndex.getName()).asList().get(), empty());
            assertThat(recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).asList().get(), empty());
        }
    }

    @Test
    void testDeleteWhereSingleTypeWithRecordTypePrefixedFunctionIndexSplit() throws Exception {
        // Index key is (recordType, header.path, first three characters of str_value)
        // Value is remaining suffix of str_value
        Index splitStringIndex = new Index(
                "split_string_index",
                keyWithValue(concat(recordType(), field("header").nest("path"), function("split_string", concat(field("str_value"), value(3L)))), 3)
        );
        final RecordMetaDataHook hook = metaData -> {
            RecordTypeBuilder typeBuilder = metaData.getRecordType("MyRecord");
            typeBuilder.setPrimaryKey(concat(recordType(), field("header").nest(concatenateFields("path", "rec_no"))));
            metaData.addUniversalIndex(splitStringIndex);
        };

        final Map<String, TestRecordsWithHeaderProto.MyRecord> recordsByPath = insertRecordsByPath(hook);
        final String path = deleteByPath(hook, recordsByPath,
                pathToDelete -> recordStore.deleteRecordsWhere(
                        TestRecordsWithHeaderProto.MyRecord.getDescriptor().getName(),
                        Query.field("header").matches(Query.field("path").equalsValue(pathToDelete))));
        recordsByPath.remove(path);

        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);
            Object typeKey = recordStore.getRecordMetaData().getRecordType(TestRecordsWithHeaderProto.MyRecord.getDescriptor().getName())
                    .getRecordTypeKey();
            assertThat("index should have no entries for deleted path",
                    recordStore.scanIndex(splitStringIndex, IndexScanType.BY_VALUE, TupleRange.allOf(Tuple.from(typeKey, path)), null, ScanProperties.FORWARD_SCAN).asList().get(),
                    empty());
            assertThat("index should have one entry for each non-deleted record",
                    recordStore.scanIndexRecords(splitStringIndex.getName()).map(indexedRecord -> TestRecordsWithHeaderProto.MyRecord.newBuilder().mergeFrom(indexedRecord.getRecord()).build()).asList().get(),
                    containsInAnyOrder(recordsByPath.values().stream().map(Matchers::equalTo).collect(Collectors.toList())));
        }
    }

    @Test
    void testDeleteWhereSkipsDisabledIndexes() throws Exception {
        final Index strValueIndex = new Index("MyRecord$path+str_value", concat(field("header").nest("path"), field("str_value")));
        final RecordMetaDataHook hook = metaDataBuilder -> {
            final RecordTypeBuilder typeBuilder = metaDataBuilder.getRecordType("MyRecord");
            typeBuilder.setPrimaryKey(field("header").nest(concatenateFields("path", "num", "rec_no")));
            metaDataBuilder.addIndex(typeBuilder, strValueIndex);
        };

        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, hook);
            final String path = "foo";
            Map<Integer, List<FDBStoredRecord<Message>>> recordsByNum = new HashMap<>();
            for (int num = 0; num < 5; num++) {
                List<FDBStoredRecord<Message>> records = new ArrayList<>();
                for (int i = 0; i < 4; i++) {
                    TestRecordsWithHeaderProto.MyRecord rec = TestRecordsWithHeaderProto.MyRecord.newBuilder()
                            .setHeader(TestRecordsWithHeaderProto.HeaderRecord.newBuilder()
                                    .setNum(num)
                                    .setPath(path)
                                    .setRecNo(num * 100 + i)
                                    .build())
                            .setStrValue(i % 2 == 0 ? "even" : "odd")
                            .build();
                    records.add(recordStore.saveRecord(rec));
                }
                recordsByNum.put(num, records);
            }

            for (Map.Entry<Integer, List<FDBStoredRecord<Message>>> entry : recordsByNum.entrySet()) {
                List<FDBStoredRecord<Message>> readRecords = recordStore.scanRecords(TupleRange.allOf(Tuple.from(path, entry.getKey())), null, ScanProperties.FORWARD_SCAN)
                        .asList()
                        .join();
                assertThat(readRecords, hasSize(entry.getValue().size()));
            }

            final QueryComponent filter = Query.field("header").matches(
                    Query.and(
                            Query.field("path").equalsValue(path),
                            Query.field("num").equalsValue(2)
                    )
            );
            Query.InvalidExpressionException err = assertThrows(Query.InvalidExpressionException.class, () -> recordStore.deleteRecordsWhere(filter));
            assertThat(err.getMessage(), indexDoesNotSupportDeleteWhere(strValueIndex));

            // Mark the index as write-only. The index still needs to be maintained, so the deleteRecordsWhere should still fail
            recordStore.markIndexWriteOnly(strValueIndex).join();
            err = assertThrows(Query.InvalidExpressionException.class, () -> recordStore.deleteRecordsWhere(filter));
            assertThat(err.getMessage(), indexDoesNotSupportDeleteWhere(strValueIndex));

            // Disable the index. The index is no longer maintained, so it's safe to skip this index when considering a deletion
            recordStore.markIndexDisabled(strValueIndex).join();

            // Now that the index is no longer maintained, the range delete should succeed
            recordStore.deleteRecordsWhere(filter);
            for (Map.Entry<Integer, List<FDBStoredRecord<Message>>> entry : recordsByNum.entrySet()) {
                List<FDBStoredRecord<Message>> readRecords = recordStore.scanRecords(TupleRange.allOf(Tuple.from(path, entry.getKey())), null, ScanProperties.FORWARD_SCAN)
                        .asList()
                        .join();
                if (entry.getKey() == 2) {
                    assertThat(readRecords, empty());
                } else {
                    assertThat(readRecords, hasSize(entry.getValue().size()));
                }
            }

            commit(context);
        }
    }

    private Map<String, TestRecordsWithHeaderProto.MyRecord> insertRecordsByPath(RecordMetaDataHook metaDataHook) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, metaDataHook);

            final Map<String, TestRecordsWithHeaderProto.MyRecord> recordsByPath = new HashMap<>();
            for (String path : List.of("foo", "bar", "baz")) {
                TestRecordsWithHeaderProto.MyRecord rec = TestRecordsWithHeaderProto.MyRecord.newBuilder()
                        .setHeader(TestRecordsWithHeaderProto.HeaderRecord.newBuilder()
                                .setPath(path)
                                .setRecNo(42)
                        )
                        .setStrValue("abcdefg")
                        .build();
                recordStore.saveRecord(rec);
                recordsByPath.put(path, rec);
            }

            commit(context);
            return recordsByPath;
        }
    }

    private String deleteByPath(RecordMetaDataHook metaDataHook, Map<String, TestRecordsWithHeaderProto.MyRecord> recordsByPath,
                                Consumer<String> deleteByPathOperation) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, metaDataHook);
            final String path = recordsByPath.keySet().iterator().next();
            deleteByPathOperation.accept(path);

            List<TestRecordsWithHeaderProto.MyRecord> readRecords = recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN)
                    .map(storedRecord -> TestRecordsWithHeaderProto.MyRecord.newBuilder().mergeFrom(storedRecord.getRecord()).build())
                    .asList()
                    .get();
            assertThat(readRecords, hasSize(recordsByPath.size() - 1));
            assertThat(readRecords, containsInAnyOrder(recordsByPath.values().stream()
                    .filter(myRecord -> !myRecord.getHeader().getPath().equals(path))
                    .map(Matchers::equalTo)
                    .collect(Collectors.toList())
            ));

            commit(context);
            return path;
        }
    }

    @SuppressWarnings("deprecation")
    private KeyExpression openRecordWithHeaderPrimaryKey(FDBRecordContext context, boolean useCountIndex) throws Exception {
        final KeyExpression groupExpr = field("header").nest("rec_no");
        openRecordWithHeader(context, metaData -> {
            metaData.getRecordType("MyRecord")
                    .setPrimaryKey(field("header").nest(concatenateFields("rec_no", "path")));
            metaData.addIndex("MyRecord", "MyRecord$str_value", concat(groupExpr, field("str_value")));
            if (useCountIndex) {
                metaData.addUniversalIndex(new Index("MyRecord$count", new GroupingKeyExpression(groupExpr, 0), IndexTypes.COUNT));
            } else {
                metaData.setRecordCountKey(groupExpr);
            }
        });
        return groupExpr;
    }

    private List<TestRecordsWithHeaderProto.MyRecord> queryMyRecords(RecordQuery query) {
        RecordQueryPlan plan = recordStore.planQuery(query);
        return queryMyRecords(plan, EvaluationContext.EMPTY);
    }

    private List<TestRecordsWithHeaderProto.MyRecord> queryMyRecords(RecordQueryPlan plan, EvaluationContext evaluationContext) {
        return asMyRecordList(plan.execute(recordStore, evaluationContext));
    }

    private List<TestRecordsWithHeaderProto.MyRecord> asMyRecordList(RecordCursor<? extends FDBRecord<?>> cursor) {
        return cursor.map(FDBRecord::getRecord)
                .map(this::parseMyRecord)
                .asList()
                .join();
    }

    private static Matcher<String> indexDoesNotSupportDeleteWhere(Index index) {
        return containsString("deleteRecordsWhere not supported by index " + index.getName());
    }
}
