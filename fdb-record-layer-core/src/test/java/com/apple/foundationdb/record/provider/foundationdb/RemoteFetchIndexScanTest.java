/*
 * FDBRecordStoreIndexPrefetchTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.IndexFetchMethod;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer.Events.SCAN_REMOTE_FETCH_ENTRY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.params.ParameterizedTest.ARGUMENTS_WITH_NAMES_PLACEHOLDER;

/**
 * A test for the remote fetch index scan wrapper.
 */
@Tag(Tags.RequiresFDB)
class RemoteFetchIndexScanTest extends RemoteFetchTestBase {
    protected static final RecordQuery IN_VALUE = RecordQuery.newBuilder()
            .setRecordType("MySimpleRecord")
            .setFilter(Query.field("num_value_unique").in(List.of(1000, 990, 980, 970, 960)))
            .build();

    protected static final RecordQuery OR_AND_VALUE = RecordQuery.newBuilder()
            .setRecordType("MySimpleRecord")
            .setFilter(Query.or(
                    Query.field("num_value_unique").equalsValue(1000),
                    Query.and(
                            Query.field("num_value_unique").greaterThanOrEquals(900),
                            Query.field("num_value_unique").lessThan(910))))
            .build();

    private boolean useSplitRecords = true;

    @BeforeEach
    void setup() throws Exception {
        complexQuerySetup(splitRecordsHook);
    }

    @ParameterizedTest(name = "indexPrefetchSimpleIndexTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @MethodSource("testedParams")
    void indexPrefetchSimpleIndexTest(IndexFetchMethod fetchMethod, IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        scanAndVerifyData("MySimpleRecord$num_value_unique", fetchMethod, indexEntryReturnPolicy, scanBounds(), ScanProperties.FORWARD_SCAN,
                null, 100,
                (rec, i) -> {
                    int primaryKey = 99 - i;
                    String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
                    int numValue = 1000 - primaryKey;
                    assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, fetchMethod, indexEntryReturnPolicy);
                }, splitRecordsHook);
        assertCounters(fetchMethod, 1, 101);
    }

    @ParameterizedTest(name = "indexPrefetchSimpleIndexReverseTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @MethodSource("testedParams")
    void indexPrefetchSimpleIndexReverseTest(IndexFetchMethod fetchMethod, IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        scanAndVerifyData("MySimpleRecord$num_value_unique", fetchMethod, indexEntryReturnPolicy, scanBounds(), ScanProperties.REVERSE_SCAN,
                null, 100,
                (rec, i) -> {
                    int primaryKey = i;
                    String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
                    int numValue = 1000 - primaryKey;
                    assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, fetchMethod, indexEntryReturnPolicy);
                }, splitRecordsHook);
        assertCounters(fetchMethod, 1, 101);
    }

    @ParameterizedTest(name = "indexPrefetchComplexIndexTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @MethodSource("testedParams")
    void indexPrefetchComplexIndexTest(IndexFetchMethod fetchMethod, IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        scanAndVerifyData("MySimpleRecord$str_value_indexed", fetchMethod, indexEntryReturnPolicy, scanBounds(), ScanProperties.FORWARD_SCAN,
                null, 100,
                (rec, i) -> {
                    int primaryKey = (i < 50) ? (i * 2) : ((i - 50) * 2) + 1;
                    String strValue = (i < 50) ? "even" : "odd";
                    int numValue = 1000 - primaryKey;
                    assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$str_value_indexed", strValue, fetchMethod, indexEntryReturnPolicy);
                }, splitRecordsHook);
        assertCounters(fetchMethod, 1, 101);
    }

    @ParameterizedTest(name = "indexPrefetchWithContinuationTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @MethodSource("testedParams")
    void indexPrefetchWithContinuationTest(IndexFetchMethod fetchMethod, IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                .setReturnedRowLimit(5)
                .build();
        ScanProperties scanProperties = new ScanProperties(executeProperties, false);

        // First iteration - first 5 records
        byte[] continuation = scanAndVerifyData("MySimpleRecord$num_value_unique", fetchMethod, indexEntryReturnPolicy, scanBounds(), scanProperties,
                null, 5,
                (rec, i) -> {
                    int primaryKey = 99 - i;
                    String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
                    int numValue = 1000 - primaryKey;
                    assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, fetchMethod, indexEntryReturnPolicy);
                }, splitRecordsHook);
        assertCounters(fetchMethod, 1, 6);
        // Second iteration - next 5 records
        continuation = scanAndVerifyData("MySimpleRecord$num_value_unique", fetchMethod, indexEntryReturnPolicy, scanBounds(), scanProperties,
                continuation, 5,
                (rec, i) -> {
                    int primaryKey = 94 - i;
                    String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
                    int numValue = 1000 - primaryKey;
                    assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, fetchMethod, indexEntryReturnPolicy);
                }, splitRecordsHook);
        // Third iteration - final 90 records
        continuation = scanAndVerifyData("MySimpleRecord$num_value_unique", fetchMethod, indexEntryReturnPolicy, scanBounds(), ScanProperties.FORWARD_SCAN,
                continuation, 90,
                (rec, i) -> {
                    int primaryKey = 89 - i;
                    String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
                    int numValue = 1000 - primaryKey;
                    assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, fetchMethod, indexEntryReturnPolicy);
                }, splitRecordsHook);

        assertNull(continuation);
        assertCounters(fetchMethod, 3, 103);
    }

    /*
     * Test continuation where the continued plan uses a different prefetch mode than the original plan.
     */
    @ParameterizedTest(name = "indexPrefetchWithMixedContinuationTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource()
    void indexPrefetchWithMixedContinuationTest(IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                .setReturnedRowLimit(4)
                .build();
        ScanProperties scanProperties = new ScanProperties(executeProperties, false);

        // First iteration - first 4 records
        byte[] continuation = scanAndVerifyData("MySimpleRecord$num_value_unique", IndexFetchMethod.USE_REMOTE_FETCH, indexEntryReturnPolicy,
                scanBounds(), scanProperties, null, 4,
                (rec, i) -> {
                    int primaryKey = 99 - i;
                    String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
                    int numValue = 1000 - primaryKey;
                    assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, IndexFetchMethod.USE_REMOTE_FETCH, indexEntryReturnPolicy);
                }, splitRecordsHook);

        // Second iteration - second 4 records
        continuation = scanAndVerifyData("MySimpleRecord$num_value_unique", IndexFetchMethod.SCAN_AND_FETCH, indexEntryReturnPolicy,
                scanBounds(), scanProperties, continuation, 4,
                (rec, i) -> {
                    int primaryKey = 95 - i;
                    String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
                    int numValue = 1000 - primaryKey;
                    assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, IndexFetchMethod.SCAN_AND_FETCH, indexEntryReturnPolicy);
                }, splitRecordsHook);

        // Third iteration - last 92 records
        continuation = scanAndVerifyData("MySimpleRecord$num_value_unique", IndexFetchMethod.USE_REMOTE_FETCH, indexEntryReturnPolicy,
                scanBounds(), ScanProperties.FORWARD_SCAN, continuation, 92,
                (rec, i) -> {
                    int primaryKey = 91 - i;
                    String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
                    int numValue = 1000 - primaryKey;
                    assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, IndexFetchMethod.USE_REMOTE_FETCH, indexEntryReturnPolicy);
                }, splitRecordsHook);

        assertNull(continuation);
        assertCounters(IndexFetchMethod.USE_REMOTE_FETCH, 2, 98);
    }

    @ParameterizedTest(name = "testScanLimit(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @MethodSource("testedParams")
    void testScanLimit(IndexFetchMethod fetchMethod, IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        ExecuteProperties executeProperties = ExecuteProperties.newBuilder()
                .setReturnedRowLimit(3)
                .build();
        ScanProperties scanProperties = new ScanProperties(executeProperties, false);

        byte[] continuation = scanAndVerifyData("MySimpleRecord$num_value_unique", fetchMethod, indexEntryReturnPolicy,
                scanBounds(), scanProperties, null, 3,
                (rec, i) -> {
                    int primaryKey = 99 - i;
                    String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
                    int numValue = 1000 - primaryKey;
                    assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, fetchMethod, indexEntryReturnPolicy);
                }, splitRecordsHook);

        executeProperties = ExecuteProperties.newBuilder()
                .setReturnedRowLimit(1)
                .build();
        scanProperties = new ScanProperties(executeProperties, false);

        continuation = scanAndVerifyData("MySimpleRecord$num_value_unique", fetchMethod, indexEntryReturnPolicy,
                scanBounds(), scanProperties, continuation, 1,
                (rec, i) -> {
                    int primaryKey = 96 - i;
                    String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
                    int numValue = 1000 - primaryKey;
                    assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, fetchMethod, indexEntryReturnPolicy);
                }, splitRecordsHook);

        executeProperties = ExecuteProperties.newBuilder()
                .setReturnedRowLimit(100)
                .build();
        scanProperties = new ScanProperties(executeProperties, false);

        continuation = scanAndVerifyData("MySimpleRecord$num_value_unique", fetchMethod, indexEntryReturnPolicy,
                scanBounds(), scanProperties, continuation, 96,
                (rec, i) -> {
                    int primaryKey = 95 - i;
                    String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
                    int numValue = 1000 - primaryKey;
                    assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, fetchMethod, indexEntryReturnPolicy);
                }, splitRecordsHook);

        assertNull(continuation);
    }

    /**
     * This test writes a value to the store within the range of the scan.
     */
    @ParameterizedTest(name = "testReadYourWriteInRange(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @MethodSource("testedParams")
    void testReadYourWriteInRange(IndexFetchMethod fetchMethod, IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        assumeTrue(recordStore.getContext().isAPIVersionAtLeast(APIVersion.API_VERSION_7_1));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, splitRecordsHook);
            // Save record in range (don't commit)
            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            recBuilder.setRecNo(1);
            recBuilder.setNumValueUnique(999);
            recBuilder.setStrValueIndexed("blah");
            recordStore.saveRecord(recBuilder.build());

            if (fetchMethod == IndexFetchMethod.USE_REMOTE_FETCH) {
                assertThrows(ExecutionException.class, () -> scanToList(context, "MySimpleRecord$num_value_unique", fetchMethod, indexEntryReturnPolicy, scanBounds(), ScanProperties.FORWARD_SCAN, primaryKey(), null));
            } else {
                scanAndVerifyData(context, "MySimpleRecord$num_value_unique", fetchMethod, indexEntryReturnPolicy,
                        scanBounds(), ScanProperties.FORWARD_SCAN, null, 100,
                        (rec, i) -> {
                            int primaryKey = 99 - i;
                            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
                            if (primaryKey == 1) {
                                strValue = "blah";
                            }
                            int numValue = 1000 - primaryKey;
                            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, fetchMethod, indexEntryReturnPolicy);
                        });
                assertCounters(fetchMethod, 1, 60);
            }
        }
    }

    /**
     * This test captures the case that FDB fails the scan after it has already returned several records. A record gets
     * modified in the transaction at a point that would allow FDB to fetch a few pages of payload before encountering
     * the modified range conflict. This should be recovered by the fallback mode.
     */
    @ParameterizedTest(name = "failAfterRecordsReturnedTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @MethodSource("testedParams")
    void failAfterRecordsReturnedTest(IndexFetchMethod fetchMethod, IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        assumeTrue(recordStore.getContext().isAPIVersionAtLeast(APIVersion.API_VERSION_7_1));

        List<TestRecords1Proto.MySimpleRecord> created = saveManyRecords();

        // Modify record and then scan unmodified index
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, splitRecordsHook);

            // Update a record. This record will eventually be returned in a query, but we do *not* modify
            // a field in the index being scanned, so that FDB does not detect the range conflict until later
            TestRecords1Proto.MySimpleRecord lastRecord = created.get(created.size() - 1);
            recordStore.saveRecord(lastRecord.toBuilder()
                    .setStrValueIndexed("foo")
                    .build());

            // Use remote fetch to scan the num_value_unique index. The first few results should return
            // data (essentially the first few pages of data from scanning the index), but once it
            // gets to the final page, it should fail because it sees a modified range when trying to look
            // up the record
            if (fetchMethod == IndexFetchMethod.USE_REMOTE_FETCH) {
                assertThrows(ExecutionException.class, () -> scanToList(context, "MySimpleRecord$num_value_unique", fetchMethod, indexEntryReturnPolicy, scanBounds(), ScanProperties.FORWARD_SCAN, primaryKey(), null));
            } else {
                scanAndVerifyData(context, "MySimpleRecord$num_value_unique", fetchMethod, indexEntryReturnPolicy,
                        scanBounds(), ScanProperties.FORWARD_SCAN, null, 500,
                        (rec, i) -> {
                            int primaryKey = i;
                            int numValue = i;
                            String strValue = (i == created.size() - 1) ? "foo" : "";
                            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, fetchMethod, indexEntryReturnPolicy);
                        });
            }
        }
    }

    @ParameterizedTest(name = "failAfterRecordsReturnedReverseTest(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @MethodSource("testedParams")
    void failAfterRecordsReturnedReverseTest(IndexFetchMethod fetchMethod, IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        assumeTrue(recordStore.getContext().isAPIVersionAtLeast(APIVersion.API_VERSION_7_1));

        List<TestRecords1Proto.MySimpleRecord> created = saveManyRecords();

        // Modify record and then scan unmodified index
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, splitRecordsHook);

            // Update a record. This record will eventually be returned in a query, but we do *not* modify
            // a field in the index being scanned, so that FDB does not detect the range conflict until later
            TestRecords1Proto.MySimpleRecord lastRecord = created.get(0);
            recordStore.saveRecord(lastRecord.toBuilder()
                    .setStrValueIndexed("foo")
                    .build());

            // Use remote fetch to scan the num_value_unique index. The first few results should return
            // data (essentially the first few pages of data from scanning the index), but once it
            // gets to the final page, it should fail because it sees a modified range when trying to look
            // up the record
            if (fetchMethod == IndexFetchMethod.USE_REMOTE_FETCH) {
                assertThrows(ExecutionException.class, () -> scanToList(context, "MySimpleRecord$num_value_unique", fetchMethod, indexEntryReturnPolicy, scanBounds(), ScanProperties.REVERSE_SCAN, primaryKey(), null));
            } else {
                scanAndVerifyData(context, "MySimpleRecord$num_value_unique", fetchMethod, indexEntryReturnPolicy,
                        scanBounds(), ScanProperties.REVERSE_SCAN, null, 500,
                        (rec, i) -> {
                            int primaryKey = 499 - i;
                            int numValue = primaryKey;
                            String strValue = (i == (created.size() - 1)) ? "foo" : "";
                            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, fetchMethod, indexEntryReturnPolicy);
                        });
            }
        }
    }

    /**
     * This tests the case where the call to scanIndexRecords fails immediately (and may fallback).
     */
    @ParameterizedTest(name = "testScanFailsImmediately(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @MethodSource("testedParams")
    void testScanFailsImmediately(IndexFetchMethod fetchMethod, IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        assumeTrue(recordStore.getContext().isAPIVersionAtLeast(APIVersion.API_VERSION_7_1));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, splitRecordsHook);

            // These scan properties will throw an exception in remote fetch since SNAPSHOT is not supported
            ScanProperties scanProperties = new ScanProperties(ExecuteProperties.newBuilder()
                    .setReturnedRowLimit(Integer.MAX_VALUE)
                    .setIsolationLevel(IsolationLevel.SNAPSHOT)
                    .build());

            IndexScanRange scanBounds = scanBounds();
            if (fetchMethod == IndexFetchMethod.USE_REMOTE_FETCH) {
                assertThrows(UnsupportedOperationException.class,
                        () -> scanToList(context, "MySimpleRecord$num_value_unique", fetchMethod, indexEntryReturnPolicy, scanBounds, scanProperties, primaryKey(), null));
            } else {
                scanAndVerifyData(context, "MySimpleRecord$num_value_unique", fetchMethod, indexEntryReturnPolicy,
                        scanBounds, scanProperties, null, 100,
                        (rec, i) -> {
                            int primaryKey = 99 - i;
                            String strValue = ((primaryKey % 2) == 0) ? "even" : "odd";
                            int numValue = 1000 - primaryKey;
                            assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$num_value_unique", (long)numValue, fetchMethod, indexEntryReturnPolicy);
                        });
                assertNull(recordStore.getTimer().getCounter(SCAN_REMOTE_FETCH_ENTRY));
            }
        }
    }

    /**
     * This test uses an index type that does not support remote fetch (BITMAP).
     */
    @ParameterizedTest(name = "testScanUnsupportedIndex(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @MethodSource("testedParams")
    void testScanUnsupportedIndex(IndexFetchMethod fetchMethod, IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        assumeTrue(recordStore.getContext().isAPIVersionAtLeast(APIVersion.API_VERSION_7_1));

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, splitRecordsHook);

            IndexScanRange scanBounds = new IndexScanRange(IndexScanType.BY_VALUE, TupleRange.ALL);
            if (fetchMethod == IndexFetchMethod.USE_REMOTE_FETCH) {
                // USE_REMOTE_FETCH should fail with UnsupportedRemoteFetchIndexException since the BitmapValueIndexMaintainer
                // does not support remote fetch
                assertThrows(UnsupportedRemoteFetchIndexException.class,
                        () -> scanToList(context, "bitmap_index", fetchMethod, indexEntryReturnPolicy, scanBounds, ScanProperties.FORWARD_SCAN, primaryKey(), null));
            } else {
                // SCAN_AND_FETCH and USE_REMOTE_FETCH_WITH_FALLBACK should fail with RecordCoreException
                // since the type of scan is wrong (BY_VALUE - should be BY_GROUP). The point though is that the failure
                // happens in the fallback path
                assertThrows(RecordCoreException.class,
                        () -> scanToList(context, "bitmap_index", fetchMethod, indexEntryReturnPolicy, scanBounds, ScanProperties.FORWARD_SCAN, primaryKey(), null));
            }
        }
    }

    @ParameterizedTest(name = "testOrphanPolicyError(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource
    void testOrphanPolicyError(IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        assumeTrue(recordStore.getContext().isAPIVersionAtLeast(APIVersion.API_VERSION_7_1));

        createOrphanEntry();

        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context, splitRecordsHook);
            Exception ex = assertThrows(ExecutionException.class, () -> scanIndex(IndexFetchMethod.USE_REMOTE_FETCH, indexEntryReturnPolicy, IndexOrphanBehavior.ERROR, ScanProperties.FORWARD_SCAN));
            assertTrue(ex.getCause() instanceof RecordCoreStorageException);
        }
    }

    @ParameterizedTest(name = "testOrphanPolicySkip(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource
    void testOrphanPolicySkip(IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        assumeTrue(recordStore.getContext().isAPIVersionAtLeast(APIVersion.API_VERSION_7_1));

        createOrphanEntry();

        List<FDBIndexedRecord<Message>> records;
        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context, splitRecordsHook);
            records = scanIndex(IndexFetchMethod.USE_REMOTE_FETCH, indexEntryReturnPolicy, IndexOrphanBehavior.SKIP, ScanProperties.FORWARD_SCAN);
        }
        assertEquals(99, records.size());
        long c = 99;
        for (FDBIndexedRecord<Message> rec : records) {
            if (c != 2) {
                assertEquals(c, rec.getStoredRecord().getPrimaryKey().get(0));
            } else {
                // skip the missing record
                c--;
            }
            c--;
        }
        assertCounters(IndexFetchMethod.USE_REMOTE_FETCH, 1, 100);
    }

    @ParameterizedTest(name = "testOrphanPolicyReturn(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource
    void testOrphanPolicyReturn(IndexEntryReturnPolicy indexEntryReturnPolicy) throws Exception {
        assumeTrue(recordStore.getContext().isAPIVersionAtLeast(APIVersion.API_VERSION_7_1));

        createOrphanEntry();

        List<FDBIndexedRecord<Message>> records;
        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context, splitRecordsHook);
            records = scanIndex(IndexFetchMethod.USE_REMOTE_FETCH, indexEntryReturnPolicy, IndexOrphanBehavior.RETURN, ScanProperties.FORWARD_SCAN);
        }
        if ((indexEntryReturnPolicy == IndexEntryReturnPolicy.ALL) || (indexEntryReturnPolicy == IndexEntryReturnPolicy.UNMATCHED)) {
            assertEquals(100, records.size());
            for (long i = 0 ; i < 100 ; i++) {
                if (i != 97) {
                    assertEquals(99 - i, records.get((int)i) .getStoredRecord().getPrimaryKey().get(0));
                } else {
                    assertFalse(records.get((int)i).hasStoredRecord());
                }
            }
            assertCounters(IndexFetchMethod.USE_REMOTE_FETCH, 1, 101);
        } else {
            // NONE and MATCHED will omit the orphan index entry in FDB, so we have no (null->null) entry, and only 99 entries overall
            assertEquals(99, records.size());
            int c = 0;
            for (long i = 99 ; i >= 0 ; i--) {
                if (i != 2) {
                    assertEquals(i, records.get(c) .getStoredRecord().getPrimaryKey().get(0));
                    c++;
                }
            }
            assertCounters(IndexFetchMethod.USE_REMOTE_FETCH, 1, 100);
        }
    }

    /**
     * Tests the scanIndexRecords method that takes a commonPrimaryKeyLength - this is the same as the other tests for index scans
     * except that it provides a pre-calculated primary key length.
     */
    @ParameterizedTest(name = "testIntegerPkLength(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource()
    void testIntegerPkLength(IndexFetchMethod fetchMethod) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, splitRecordsHook);
            try (RecordCursorIterator<FDBQueriedRecord<Message>> iterator = recordStore.scanIndexRecords(
                            recordStore.getRecordMetaData().getIndex("MySimpleRecord$str_value_indexed"), fetchMethod, scanBounds(),
                            1, null, IndexOrphanBehavior.ERROR, ScanProperties.FORWARD_SCAN, IndexEntryReturnPolicy.ALL)
                    .map(FDBQueriedRecord::indexed)
                    .asIterator()) {
                verifyData(100, (rec, i) -> {
                    int primaryKey = (i < 50) ? (i * 2) : ((i - 50) * 2) + 1;
                    String strValue = (i < 50) ? "even" : "odd";
                    int numValue = 1000 - primaryKey;
                    assertRecord(rec, primaryKey, strValue, numValue, "MySimpleRecord$str_value_indexed", strValue, fetchMethod, IndexEntryReturnPolicy.ALL);
                }, iterator);
            }
        }
        assertCounters(fetchMethod, 1, 101);
    }

    @ParameterizedTest(name = "testIntegerPkLength(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource()
    void testInvalidIntegerPkLength(IndexFetchMethod fetchMethod) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, splitRecordsHook);
            Index index = recordStore.getRecordMetaData().getIndex("MySimpleRecord$str_value_indexed");
            IndexScanRange scanBounds = scanBounds();

            if (fetchMethod == IndexFetchMethod.USE_REMOTE_FETCH) {
                assertThrows(RecordCoreArgumentException.class, () -> {
                    recordStore.scanIndexRecords(
                            index, fetchMethod, scanBounds,
                            -1,
                            null, IndexOrphanBehavior.ERROR, ScanProperties.FORWARD_SCAN, IndexEntryReturnPolicy.ALL);
                });
            } else {
                List<FDBIndexedRecord<Message>> records = recordStore.scanIndexRecords(
                                index, fetchMethod, scanBounds,
                                -1,
                                null, IndexOrphanBehavior.ERROR, ScanProperties.FORWARD_SCAN, IndexEntryReturnPolicy.ALL)
                        .asList().get();
                assertEquals(100, records.size());
            }
        }
    }

    @ParameterizedTest(name = "testIntegerPkLength(" + ARGUMENTS_WITH_NAMES_PLACEHOLDER + ")")
    @EnumSource()
    void testTooLargeIntegerPkLength(IndexFetchMethod fetchMethod) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, splitRecordsHook);
            Index index = recordStore.getRecordMetaData().getIndex("MySimpleRecord$str_value_indexed");
            IndexScanRange scanBounds = scanBounds();

            if (fetchMethod == IndexFetchMethod.USE_REMOTE_FETCH) {
                // In this case, the failure comes from FDB once it sees that the request contains key elements that do not exist ({K[7]} ... {K[85]})
                Exception ex = assertThrows(ExecutionException.class, () -> recordStore.scanIndexRecords(
                                index, fetchMethod, scanBounds,
                                86,
                                null, IndexOrphanBehavior.ERROR, ScanProperties.FORWARD_SCAN, IndexEntryReturnPolicy.ALL)
                        .asList().get());
                assertTrue(ex.getCause() instanceof FDBException);
            } else {
                List<FDBIndexedRecord<Message>> records = recordStore.scanIndexRecords(
                                index, fetchMethod, scanBounds,
                                86,
                                null, IndexOrphanBehavior.ERROR, ScanProperties.FORWARD_SCAN, IndexEntryReturnPolicy.ALL)
                        .asList().get();
                assertEquals(100, records.size());
            }
        }
    }

    private List<FDBIndexedRecord<Message>> scanIndex(final IndexFetchMethod fetchMethod,
                                                      final IndexEntryReturnPolicy indexEntryReturnPolicy,
                                                      final IndexOrphanBehavior orphanBehavior,
                                                      final ScanProperties scanProperties) throws InterruptedException, ExecutionException {
        return recordStore.scanIndexRecords(recordStore.getRecordMetaData().getIndex("MySimpleRecord$num_value_unique"), fetchMethod, scanBounds(),
                null, orphanBehavior, scanProperties, indexEntryReturnPolicy).asList().get();
    }

    private void createOrphanEntry() throws Exception {
        // Unchecked open the store, remove the index and then delete a record. This will create an orphan entry in the index.
        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context, builder -> {
                builder.removeIndex("MySimpleRecord$num_value_unique");
            });
            recordStore.deleteRecord(Tuple.from(2L));
            commit(context);
        }
    }

    public boolean isUseSplitRecords() {
        return useSplitRecords;
    }

    public void setUseSplitRecords(final boolean useSplitRecords) {
        this.useSplitRecords = useSplitRecords;
    }

    private KeyExpression primaryKey() {
        return recordStore.getRecordMetaData().getRecordType("MySimpleRecord").getPrimaryKey();
    }

    @Nonnull
    private IndexScanRange scanBounds() {
        return new IndexScanRange(IndexScanType.BY_VALUE, TupleRange.ALL);
    }

    @Nonnull
    private final RecordMetaDataHook splitRecordsHook = metaDataBuilder -> {
        // UseSplitRecords can be set to different values to impact the way the store is opened
        metaDataBuilder.setSplitLongRecords(isUseSplitRecords());
        metaDataBuilder.addIndex("MySimpleRecord", "PrimaryKeyIndex", "rec_no");
        metaDataBuilder.addIndex("MySimpleRecord", new Index("bitmap_index", concatenateFields("str_value_indexed", "num_value_2", "rec_no").group(1), IndexTypes.BITMAP_VALUE, Collections.singletonMap(IndexOptions.BITMAP_VALUE_ENTRY_SIZE_OPTION, "16")));
    };

    private void assertRecordWithPrimaryKeyIndex(final FDBQueriedRecord<Message> rec, final long primaryKey, final String strValue, final int numValue, final String indexName, final Object indexedValue) {
        IndexEntry indexEntry = rec.getIndexEntry();
        assertThat(indexEntry.getIndex().getName(), equalTo(indexName));
        List<Object> indexElements = indexEntry.getKey().getItems();
        assertThat(indexElements.size(), equalTo(1));
        assertThat(indexElements.get(0), equalTo(primaryKey));
        List<Object> indexPrimaryKey = indexEntry.getPrimaryKey().getItems();
        assertThat(indexPrimaryKey.size(), equalTo(1));
        assertThat(indexPrimaryKey.get(0), equalTo(primaryKey));

        FDBStoredRecord<Message> storedRecord = rec.getStoredRecord();
        assertThat(storedRecord.getPrimaryKey().get(0), equalTo(primaryKey));
        assertThat(storedRecord.getRecordType().getName(), equalTo("MySimpleRecord"));

        TestRecords1Proto.MySimpleRecord.Builder myrec = TestRecords1Proto.MySimpleRecord.newBuilder();
        myrec.mergeFrom(Objects.requireNonNull(rec).getRecord());
        assertThat(myrec.getRecNo(), equalTo(primaryKey));
        assertThat(myrec.getStrValueIndexed(), equalTo(strValue));
        assertThat(myrec.getNumValueUnique(), equalTo(numValue));
    }

    private List<TestRecords1Proto.MySimpleRecord> saveManyRecords() {
        List<TestRecords1Proto.MySimpleRecord> created = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            created.add(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(i)
                    .setNumValue3Indexed(i % 3)
                    .setNumValueUnique(i)
                    .build());
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, splitRecordsHook);
            recordStore.deleteAllRecords();
            commit(context);
        }
        Iterator<TestRecords1Proto.MySimpleRecord> createdIterator = created.iterator();
        while (createdIterator.hasNext()) {
            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context, splitRecordsHook);
                int i = 0;
                while (i < 50 && createdIterator.hasNext()) {
                    recordStore.saveRecord(createdIterator.next());
                    i++;
                }
                commit(context);
            }
        }

        return created;
    }
}
