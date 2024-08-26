/*
 * FDBRecordStoreTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.FDBError;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.RecordMetaDataOptionsProto;
import com.apple.foundationdb.record.RecordMetaDataProto;
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestNoUnionProto;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecords7Proto;
import com.apple.foundationdb.record.TestRecordsDuplicateUnionFields;
import com.apple.foundationdb.record.TestRecordsImportProto;
import com.apple.foundationdb.record.TestRecordsWithHeaderProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.RecordSerializationException;
import com.apple.foundationdb.record.provider.common.RecordSerializer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer.Events.DELETE_INDEX_ENTRY;
import static com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer.Events.SAVE_INDEX_ENTRY;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Basic tests for {@link FDBRecordStore}.
 */
@Tag(Tags.RequiresFDB)
public class FDBRecordStoreTest extends FDBRecordStoreTestBase {

    /**
     * If there are two primary keys that overlap in the just the right way, then one can have the representation
     * of one record's primary key be a strict prefix of the other record's. With older versions of the Record Layer,
     * this could lead to problems where attempting to load the record with the shorter key would also read data for
     * the record with the longer key. See <a href="https://github.com/FoundationDB/fdb-record-layer/issues/782">Issue #782</a>.
     *
     * <p>
     * This test is parameterized by format version because the fix involves being more particular about the range that
     * is scanned. In particular, the scan range is now only over those keys which are strict prefixes of the primary
     * key. This is fine as long as there aren't any data stored at that key. Prior to {@link FDBRecordStore#SAVE_UNSPLIT_WITH_SUFFIX_FORMAT_VERSION},
     * there could be data in that key was not true if {@code splitLongRecords} was {@code false}. Parameterizing here
     * tests those older configurations.
     * </p>
     *
     * @param formatVersion format version to use when running the test
     * @param splitLongRecords whether the test should split long records or not
     */
    @ParameterizedTest(name = "prefixPrimaryKeysWithNullByteAfterPrefix [formatVersion = {0}, splitLongRecords = {1}]")
    @MethodSource("formatVersionAndSplitArgs")
    public void prefixPrimaryKeysWithNullByteAfterPrefix(int formatVersion, boolean splitLongRecords) throws Exception {
        final RecordMetaDataHook hook = metaData -> {
            metaData.setSplitLongRecords(splitLongRecords);
            metaData.getRecordType("MySimpleRecord").setPrimaryKey(field("str_value_indexed"));
        };
        final FDBRecordStore.Builder storeBuilder;
        // The primary key for record1 is a prefix of the primary key for record2, and the first byte in record2's primary
        // key is a null byte. Because FDB's Tuple layer null terminates its representation of strings, this means that
        // the keys used to store record1 forms a prefix of the keys storing record2. However, the null byte should be
        // escaped in such a way that it is possible to read only the keys for record1. This is necessary to properly load
        // the record by primary key.
        final TestRecords1Proto.MySimpleRecord record1 = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setStrValueIndexed("foo")
                .setNumValue3Indexed(1066)
                .build();
        final TestRecords1Proto.MySimpleRecord record2 = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setStrValueIndexed("foo\0bar")
                .setNumValue3Indexed(1415)
                .build();
        // Save the two records
        try (FDBRecordContext context = openContext()) {
            uncheckedOpenSimpleRecordStore(context, hook);
            storeBuilder = recordStore.asBuilder().setFormatVersion(formatVersion);
            final FDBRecordStore store = storeBuilder.create();
            store.saveRecord(record1);
            store.saveRecord(record2);
            commit(context);
        }
        // Load by scanning records
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = storeBuilder.setContext(context).open();
            final List<FDBStoredRecord<Message>> records = store.scanRecords(null, ScanProperties.FORWARD_SCAN).asList().get();
            assertThat(records, hasSize(2));
            assertEquals(record1, records.get(0).getRecord());
            assertEquals(record2, records.get(1).getRecord());
        }
        // Load by primary key
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = storeBuilder.setContext(context).open();
            FDBStoredRecord<Message> storedRecord1 = store.loadRecord(Tuple.from(record1.getStrValueIndexed()));
            assertNotNull(storedRecord1, "record1 was missing");
            assertEquals(record1, storedRecord1.getRecord());

            FDBStoredRecord<Message> storedRecord2 = store.loadRecord(Tuple.from(record2.getStrValueIndexed()));
            assertNotNull(storedRecord2, "record2 was missing");
            assertEquals(record2, storedRecord2.getRecord());
        }
        // Load by query
        try (FDBRecordContext context = openContext()) {
            final FDBRecordStore store = storeBuilder.setContext(context).open();
            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("MySimpleRecord")
                    .setFilter(Query.field("num_value_3_indexed").equalsParameter("num"))
                    .build();
            final RecordQueryPlan plan = store.planQuery(query);
            assertThat(plan, indexScan(allOf(indexName("MySimpleRecord$num_value_3_indexed"), bounds(hasTupleString("[EQUALS $num]")))));

            // Record 1
            List<FDBQueriedRecord<Message>> record1List = plan.execute(store, EvaluationContext.forBinding("num", 1066)).asList().get();
            assertThat(record1List, hasSize(1));
            FDBQueriedRecord<Message> queriedRecord1 = record1List.get(0);
            assertEquals(record1, queriedRecord1.getRecord());

            // Record 2
            List<FDBQueriedRecord<Message>> record2List = plan.execute(store, EvaluationContext.forBinding("num", 1415)).asList().get();
            assertThat(record2List, hasSize(1));
            FDBQueriedRecord<Message> queriedRecord2 = record2List.get(0);
            assertEquals(record2, queriedRecord2.getRecord());
        }
    }

    @Test
    public void scanContinuations() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            for (int i = 0; i < 100; i++) {
                recBuilder.setRecNo(i);
                recordStore.saveRecord(recBuilder.build());
            }
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            for (Boolean builtInLimit : new Boolean[] { Boolean.TRUE, Boolean.FALSE }) {
                for (int limit = 1; limit <= 5; limit++) {
                    int i = 0;
                    byte[] continuation = null;
                    do {
                        try (RecordCursorIterator<FDBStoredRecord<Message>> cursor = scanContinuationsCursor(continuation, limit, false, builtInLimit).asIterator()) {
                            while (cursor.hasNext()) {
                                assertEquals(i, cursor.next().getPrimaryKey().getLong(0));
                                i++;
                            }
                            continuation = cursor.getContinuation();
                        }
                    } while (continuation != null);
                    assertEquals(100, i);
                    do {
                        try (RecordCursorIterator<FDBStoredRecord<Message>> cursor = scanContinuationsCursor(continuation, limit, true, builtInLimit).asIterator()) {
                            while (cursor.hasNext()) {
                                i--;
                                assertEquals(i, cursor.next().getPrimaryKey().getLong(0));
                            }
                            continuation = cursor.getContinuation();
                        }
                    } while (continuation != null);
                    assertEquals(0, i);
                }
            }
            commit(context);
        }
    }

    private RecordCursor<FDBStoredRecord<Message>> scanContinuationsCursor(byte[] continuation, int limit, boolean reverse, boolean builtInLimit) {
        if (builtInLimit) {
            return recordStore.scanRecords(continuation, new ScanProperties(ExecuteProperties.newBuilder()
                    .setReturnedRowLimit(limit)
                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                    .build(), reverse));
        } else {
            // Using a separate limit cursor will mean calling getContinuation on the inner cursor in the middle of its stream.
            return recordStore.scanRecords(continuation, new ScanProperties(ExecuteProperties.newBuilder()
                    .setReturnedRowLimit(Integer.MAX_VALUE)
                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                    .build(), reverse)).limitRowsTo(limit);
        }
    }

    @Test
    public void testOverlappingPrimaryKey() {
        try (FDBRecordContext context = openContext()) {
            RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestRecordsWithHeaderProto.getDescriptor());
            builder.getRecordType("MyRecord")
                    .setPrimaryKey(field("header").nest(concatenateFields("path", "rec_no")));
            builder.addIndex("MyRecord", "MyRecord$path_str", concat(field("header").nest("path"),
                    field("str_value")));
            RecordMetaData metaData = builder.getRecordMetaData();
            createOrOpenRecordStore(context, metaData);

            TestRecordsWithHeaderProto.MyRecord.Builder recBuilder = TestRecordsWithHeaderProto.MyRecord.newBuilder();
            TestRecordsWithHeaderProto.HeaderRecord.Builder headerBuilder = recBuilder.getHeaderBuilder();
            headerBuilder.setPath("aaa");
            headerBuilder.setRecNo(1);
            recBuilder.setStrValue("hello");
            recordStore.saveRecord(recBuilder.build());

            headerBuilder.setPath("aaa");
            headerBuilder.setRecNo(2);
            recBuilder.setStrValue("goodbye");
            recordStore.saveRecord(recBuilder.build());

            headerBuilder.setPath("zzz");
            headerBuilder.setRecNo(3);
            recBuilder.setStrValue("end");
            recordStore.saveRecord(recBuilder.build());

            List<List<Object>> rows = new ArrayList<>();
            Index index = metaData.getIndex("MyRecord$path_str");
            ScanComparisons comparisons = ScanComparisons.from(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, "aaa"));
            TupleRange range = comparisons.toTupleRange();
            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(index, IndexScanType.BY_VALUE, range,
                                                                            null, ScanProperties.FORWARD_SCAN)) {
                cursor.forEach(row -> rows.add(row.getKey().getItems())).join();
            }
            assertEquals(Arrays.asList(Arrays.asList("aaa", "goodbye", 2L),
                                       Arrays.asList("aaa", "hello", 1L)),
                         rows);
        }
    }

    @Test
    public void testIndexKeyTooLarge() {
        assertThrows(FDBExceptions.FDBStoreKeySizeException.class, () -> {
            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context);

                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(1);
                recBuilder.setStrValueIndexed(Strings.repeat("x", 10000));
                recordStore.saveRecord(recBuilder.build());
                fail("exception should have been thrown before commit");
                commit(context);
            }
        });
    }

    @Test
    public void testIndexValueTooLarge() {
        assertThrows(FDBExceptions.FDBStoreValueSizeException.class, () -> {
            try (FDBRecordContext context = openContext()) {
                openSimpleRecordStore(context, md -> {
                    md.setSplitLongRecords(true);
                    md.removeIndex("MySimpleRecord$str_value_indexed");
                    md.addIndex("MySimpleRecord", new Index(
                            "valueIndex",
                            field("num_value_2"),
                            field("str_value_indexed"),
                            IndexTypes.VALUE,
                            Collections.emptyMap()));
                });

                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(1);
                recBuilder.setStrValueIndexed(Strings.repeat("x", 100000));
                recordStore.saveRecord(recBuilder.build());
                fail("exception should have been thrown before commit");
                commit(context);
            }
        });
    }

    @Test
    public void testStoredRecordSizeIsPlausible() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);
            final int recordSubspaceSize = recordStore.recordsSubspace().pack().length;

            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();

            recBuilder.setRecNo(1);
            recBuilder.setStrValueIndexed(Strings.repeat("x", 10));
            FDBStoredRecord<Message> rec1 = recordStore.saveRecord(recBuilder.build());
            assertEquals(1, rec1.getKeyCount(), "small record should only need one key-value pair");
            assertThat("small record should only need few key bytes", rec1.getKeySize(), allOf(greaterThan(recordSubspaceSize), lessThan(recordSubspaceSize + 10)));
            assertThat("small record should only need few value bytes", rec1.getValueSize(), allOf(greaterThan(10), lessThan(100)));

            recBuilder.setRecNo(2);
            recBuilder.setStrValueIndexed(Strings.repeat("x", 100000));
            FDBStoredRecord<Message> rec2 = recordStore.saveRecord(recBuilder.build());
            assertThat("large record should only need several key-value pairs", rec2.getKeyCount(), allOf(greaterThan(1), lessThan(10)));
            assertThat("large record should only need few key bytes", rec2.getKeySize(), allOf(greaterThan(rec2.getKeyCount() * recordSubspaceSize), lessThan(rec2.getKeyCount() * (recordSubspaceSize + 10))));
            assertThat("large record should only need many value bytes", rec2.getValueSize(), allOf(greaterThan(100000), lessThan(101000)));

            FDBStoredRecord<Message> rec1x = recordStore.loadRecord(rec1.getPrimaryKey());
            assertEquals(rec1.getKeyCount(), rec1x.getKeyCount(), "small record loaded key count should match");
            assertEquals(rec1.getKeySize(), rec1x.getKeySize(), "small record loaded key size should match");
            assertEquals(rec1.getValueSize(), rec1x.getValueSize(), "small record loaded value size should match");

            FDBStoredRecord<Message> rec2x = recordStore.loadRecord(rec2.getPrimaryKey());
            assertEquals(rec2.getKeyCount(), rec2x.getKeyCount(), "large record loaded key count should match");
            assertEquals(rec2.getKeySize(), rec2x.getKeySize(), "large record loaded key size should match");
            assertEquals(rec2.getValueSize(), rec2x.getValueSize(), "large record loaded value size should match");

            commit(context);
        }
    }

    @Test
    public void testStoredRecordSizeIsConsistent() {
        final RecordMetaDataHook hook = md -> md.setSplitLongRecords(true);

        Set<FDBStoredRecord<Message>> saved;
        try (FDBRecordContext context = openContext()) {
            List<FDBStoredRecord<Message>> saving = new ArrayList<>();
            openSimpleRecordStore(context, hook);

            for (int i = 0; i < 10; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i + 1);
                recBuilder.setStrValueIndexed(Strings.repeat("z", i * 10));
                saving.add(recordStore.saveRecord(recBuilder.build()));
            }
            commit(context);

            final byte[] commitVersionstamp = context.getVersionStamp();
            assertNotNull(commitVersionstamp);
            saved = saving.stream().map(rec -> rec.withCommittedVersion(context.getVersionStamp())).collect(Collectors.toSet());
        }

        Set<FDBStoredRecord<Message>> scanned = new HashSet<>();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).forEach(scanned::add).join();
            commit(context);
        }

        Set<FDBStoredRecord<Message>> indexed = new HashSet<>();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);
            recordStore.scanIndexRecords("MySimpleRecord$str_value_indexed").forEach(i -> indexed.add(i.getStoredRecord())).join();
            commit(context);
        }

        assertEquals(saved, scanned);
        assertEquals(saved, indexed);

    }

    @Test
    public void testStoreTimersIncrement() throws Exception {
        final int recordCount = 10;
        final int recordKeyCount = 2 * recordCount;
        final int minRecordKeyBytes = 8;
        final int minTotalRecordKeyBytes = minRecordKeyBytes * recordKeyCount;
        final int minRecordValueBytes = 15;
        final int minTotalRecordValueBytes = recordCount * minRecordValueBytes;

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.deleteAllRecords();

            for (int i = 0; i < recordCount; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i + 1);
                recordStore.saveRecord(recBuilder.build());
            }

            assertThat(timer.getCount(FDBStoreTimer.Events.SAVE_RECORD), equalTo(recordCount));
            assertThat(timer.getCount(FDBStoreTimer.Events.SAVE_INDEX_ENTRY), equalTo(recordCount * 3));
            assertThat(timer.getCount(FDBStoreTimer.Counts.SAVE_RECORD_KEY), equalTo(recordKeyCount));
            assertThat(timer.getCount(FDBStoreTimer.Counts.SAVE_RECORD_KEY_BYTES), greaterThan(minTotalRecordKeyBytes));
            assertThat(timer.getCount(FDBStoreTimer.Counts.SAVE_RECORD_VALUE_BYTES), greaterThan(minTotalRecordValueBytes));
            assertThat(timer.getCount(FDBStoreTimer.Counts.SAVE_INDEX_KEY), equalTo(recordCount * 3));
            assertThat(timer.getCount(FDBStoreTimer.Counts.SAVE_INDEX_KEY_BYTES), greaterThan(minTotalRecordKeyBytes * 3));
            assertThat(timer.getCount(FDBStoreTimer.Counts.SAVE_INDEX_VALUE_BYTES), equalTo(0));
            assertThat(timer.getCount(RecordSerializer.Events.SERIALIZE_PROTOBUF_RECORD), equalTo(recordCount));


            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            
            recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).getCount().join();

            assertThat(timer.getCount(FDBStoreTimer.Events.SCAN_RECORDS), equalTo( recordCount + 1));
            assertThat(timer.getCount(FDBStoreTimer.Counts.LOAD_RECORD_KEY), equalTo(recordKeyCount));
            assertThat(timer.getCount(FDBStoreTimer.Counts.LOAD_RECORD_KEY_BYTES), greaterThan(minTotalRecordKeyBytes));
            assertThat(timer.getCount(FDBStoreTimer.Counts.LOAD_RECORD_VALUE_BYTES), greaterThan(minTotalRecordValueBytes));
            assertThat(timer.getCount(RecordSerializer.Events.DESERIALIZE_PROTOBUF_RECORD), equalTo(recordCount));

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            recordStore.scanIndexRecords("MySimpleRecord$str_value_indexed").getCount().join();

            assertThat(timer.getCount(FDBStoreTimer.Events.SCAN_INDEX_KEYS), equalTo(recordCount + 1));
            assertThat(timer.getCount(FDBStoreTimer.Counts.LOAD_RECORD_KEY), equalTo(recordKeyCount));
            assertThat(timer.getCount(FDBStoreTimer.Counts.LOAD_RECORD_KEY_BYTES), greaterThan(minTotalRecordKeyBytes));
            assertThat(timer.getCount(FDBStoreTimer.Counts.LOAD_RECORD_VALUE_BYTES), greaterThan(minTotalRecordValueBytes));
            assertThat(timer.getCount(FDBStoreTimer.Counts.LOAD_INDEX_KEY), equalTo(recordCount));
            assertThat(timer.getCount(FDBStoreTimer.Counts.LOAD_INDEX_KEY_BYTES), greaterThan(minTotalRecordKeyBytes));
            assertThat(timer.getCount(FDBStoreTimer.Counts.LOAD_INDEX_VALUE_BYTES), equalTo(0));

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            for (int i = 0; i < recordCount; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i + 1);
                recBuilder.setStrValueIndexed("abcxyz");
                recordStore.saveRecord(recBuilder.build());
            }
            assertThat(timer.getCount(FDBStoreTimer.Counts.REPLACE_RECORD_VALUE_BYTES), greaterThan(minTotalRecordValueBytes));

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.deleteAllRecords();

            for (int i = 0; i < recordCount; i++) {
                TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
                recBuilder.setRecNo(i + 1);
                recordStore.saveRecord(recBuilder.build());
            }
            for (int i = 0; i < recordCount; i++) {
                recordStore.deleteRecord(Tuple.from(i + 1));
            }
            assertThat(timer.getCount(FDBStoreTimer.Events.SAVE_RECORD), equalTo(recordCount));
            assertEquals(timer.getCount(FDBStoreTimer.Events.SAVE_RECORD), timer.getCount(FDBStoreTimer.Events.DELETE_RECORD));
            assertEquals(timer.getCount(FDBStoreTimer.Events.SAVE_INDEX_ENTRY), timer.getCount(FDBStoreTimer.Events.DELETE_INDEX_ENTRY));
            assertEquals(timer.getCount(FDBStoreTimer.Counts.SAVE_RECORD_KEY), timer.getCount(FDBStoreTimer.Counts.DELETE_RECORD_KEY));
            assertEquals(timer.getCount(FDBStoreTimer.Counts.SAVE_RECORD_KEY_BYTES), timer.getCount(FDBStoreTimer.Counts.DELETE_RECORD_KEY_BYTES));
            assertEquals(timer.getCount(FDBStoreTimer.Counts.SAVE_RECORD_VALUE_BYTES), timer.getCount(FDBStoreTimer.Counts.DELETE_RECORD_VALUE_BYTES));
            assertEquals(timer.getCount(FDBStoreTimer.Counts.SAVE_INDEX_KEY), timer.getCount(FDBStoreTimer.Counts.DELETE_INDEX_KEY));
            assertEquals(timer.getCount(FDBStoreTimer.Counts.SAVE_INDEX_KEY_BYTES), timer.getCount(FDBStoreTimer.Counts.DELETE_INDEX_KEY_BYTES));

            commit(context);
        }
    }

    @Test
    public void updateUnchanged() throws Exception {
        TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
        recBuilder.setRecNo(1);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            recBuilder.setStrValueIndexed("abc");
            recordStore.saveRecord(recBuilder.build());
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recBuilder.setStrValueIndexed("xyz");
            recordStore.saveRecord(recBuilder.build());
            assertEquals(2, timer.getCount(DELETE_INDEX_ENTRY) + timer.getCount(SAVE_INDEX_ENTRY), "should update one index");
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.saveRecord(recBuilder.build());
            assertEquals(0, timer.getCount(DELETE_INDEX_ENTRY) + timer.getCount(SAVE_INDEX_ENTRY), "should not update any index");
            commit(context);
        }
    }

    @Test
    public void typeChange() throws Exception {
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, RecordMetaData.build(TestRecords7Proto.getDescriptor()));

            TestRecords7Proto.MyRecord1.Builder rec1Builder = TestRecords7Proto.MyRecord1.newBuilder();
            rec1Builder.setRecNo(1);
            rec1Builder.setStrValue("one");
            recordStore.saveRecord(rec1Builder.build());

            assertEquals(1L, recordStore.scanIndexRecords("MyRecord1$str_value").getCount().get().longValue(), "should have one record in index");

            TestRecords7Proto.MyRecord2.Builder rec2Builder = TestRecords7Proto.MyRecord2.newBuilder();
            rec2Builder.setRecNo(1); // Same primary key
            rec2Builder.setStrValue("two");
            recordStore.saveRecord(rec2Builder.build());

            FDBStoredRecord<Message> rec2 = recordStore.loadRecord(Tuple.from(1L));
            assertNotNull(rec2);
            TestRecords7Proto.MyRecord2.Builder myrec = TestRecords7Proto.MyRecord2.newBuilder();
            myrec.mergeFrom(rec2.getRecord());
            assertEquals("two", myrec.getStrValue(), "should load second record");

            assertEquals(0L, recordStore.scanIndexRecords("MyRecord1$str_value").getCount().get().longValue(), "should have no records in index");
        }
    }

    @Test
    public void testCommittedVersion() throws Exception {
        try (FDBRecordContext context = openContext()) {
            final long readVersion = context.getReadVersion();

            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            recBuilder.setRecNo(1);
            recordStore.saveRecord(recBuilder.build());
            commit(context);
            assertThat(context.getCommittedVersion(), greaterThan(readVersion));
        }
    }

    @Test
    public void testCommittedVersionReadOnly() {
        try (FDBRecordContext context = openContext()) {
            commit(context);
            assertThat(context.getCommittedVersion(), equalTo(-1L));
        }
    }

    @Test
    public void testVersionStamp() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            recBuilder.setRecNo(1);
            recordStore.saveRecord(recBuilder.build());
            commit(context);
            assertNotNull(context.getVersionStamp());

            long committedVersion = context.getCommittedVersion();
            assertThat(committedVersion, greaterThan(0L));
            assertEquals(committedVersion, ByteBuffer.wrap(context.getVersionStamp()).getLong());
            // TODO: When have version stamp operations in tuples, etc. check them.
        }
    }

    @Test
    public void testVersionStampReadOnly() {
        try (FDBRecordContext context = openContext()) {
            commit(context);
            assertNull(context.getVersionStamp());
        }
    }

    @Test
    public void testCancelWhileCommitVersionStamp() throws Exception {
        FDBRecordContext context = openContext();
        openSimpleRecordStore(context);
        recordStore.addConflictForSubspace(true); // so that we are not a read-only transaction
        CompletableFuture<Void> commitFuture = context.commitAsync();
        context.close();
        try {
            commitFuture.get();
        } catch (ExecutionException e) {
            // Ignore. Only waiting to make sure it's completed.
        }

        // Depending on who wins the race, we might hit either assert. However, the behavior
        // should match the result of getCommittedVersion.
        boolean shouldFail;
        long committedVersion;
        try {
            committedVersion = context.getCommittedVersion();
            assertThat(committedVersion, greaterThan(0L));
            shouldFail = false;
        } catch (RecordCoreStorageException e) {
            committedVersion = -1L;
            shouldFail = true;
        }
        try {
            byte[] versionStamp = context.getVersionStamp();
            assertThat(shouldFail, is(false));
            assertNotNull(versionStamp);
            assertEquals(committedVersion, ByteBuffer.wrap(versionStamp).getLong());
        } catch (RecordCoreStorageException e) {
            assertEquals("Transaction has not been committed yet.", e.getMessage());
            assertThat(shouldFail, is(true));
        }
    }

    @Test
    public void testSubspaceWriteConflict() throws Exception {
        // Double check that it works to have two contexts on same space without conflict.
        FDBRecordContext context1 = openContext();
        uncheckedOpenSimpleRecordStore(context1);
        try (FDBRecordContext context2 = openContext()) {
            uncheckedOpenSimpleRecordStore(context2);
            commit(context2);
        }
        commit(context1);

        // Again with conflict.
        FDBRecordContext context3 = openContext();
        uncheckedOpenSimpleRecordStore(context3);
        FDBRecordStore recordStore3 = recordStore;
        recordStore3.loadRecord(Tuple.from(0L)); // Need to read something as write-only transactions never conflict
        recordStore3.addConflictForSubspace(true);
        try (FDBRecordContext context4 = openContext()) {
            uncheckedOpenSimpleRecordStore(context4);
            recordStore.addConflictForSubspace(true);
            commit(context4);
        }
        try {
            commit(context3);
            fail("should have gotten failure");
        } catch (FDBExceptions.FDBStoreRetriableException ex) {
            assertTrue(ex.getCause() instanceof FDBException);
            assertThat(((FDBException)ex.getCause()).getCode(), equalTo(FDBError.NOT_COMMITTED.code()));
        }
    }

    @Test
    public void testSubspaceReadWriteConflict() throws Exception {
        // create the store, so it won't conflict on that action
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            context.commit();
        }
        // Double check that it works to have two contexts on same space writing different records without conflict.
        try (FDBRecordContext context1 = openContext()) {
            openSimpleRecordStore(context1);
            FDBRecordStore recordStore1 = recordStore;
            recordStore1.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1).build());
            try (FDBRecordContext context2 = openContext()) {
                openSimpleRecordStore(context2);
                recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(2).build());
                commit(context2);
            }
            commit(context1);
        }

        // Again with requested conflict.
        try (FDBRecordContext context3 = openContext()) {
            openSimpleRecordStore(context3);
            FDBRecordStore recordStore3 = recordStore;
            recordStore3.addConflictForSubspace(false);
            recordStore3.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(3).build());
            try (FDBRecordContext context4 = openContext()) {
                uncheckedOpenSimpleRecordStore(context4);
                recordStore.addConflictForSubspace(false);
                recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(4).build());
                commit(context4);
            }
            assertThrows(FDBExceptions.FDBStoreTransactionConflictException.class, () -> commit(context3));
        }
    }

    /**
     * Test that the user field can be set.
     */
    @Test
    public void testAccessUserField() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertNull(recordStore.getHeaderUserField("foo"));
            recordStore.setHeaderUserField("foo", "bar".getBytes(Charsets.UTF_8));
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertEquals("bar", recordStore.getHeaderUserField("foo").toStringUtf8());
            RecordMetaDataProto.DataStoreInfo storeHeader = recordStore.getRecordStoreState().getStoreHeader();
            assertEquals(1, storeHeader.getUserFieldCount());

            // Validate that one can overwrite an existing value
            recordStore.setHeaderUserField("foo", "µs".getBytes(Charsets.UTF_8));
            storeHeader = recordStore.getRecordStoreState().getStoreHeader();
            assertEquals(1, storeHeader.getUserFieldCount());

            // Validate that one can add a new value
            recordStore.setHeaderUserField("baz", field("baz").toKeyExpression().toByteString());
            storeHeader = recordStore.getRecordStoreState().getStoreHeader();
            assertEquals(2, storeHeader.getUserFieldCount());

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            // Read back the stored values
            assertEquals("µs", recordStore.getHeaderUserField("foo").toStringUtf8());
            ByteString bazValue = recordStore.getHeaderUserField("baz");
            assertNotNull(bazValue);
            KeyExpression expr = KeyExpression.fromProto(RecordMetaDataProto.KeyExpression.parseFrom(bazValue));
            assertEquals(field("baz"), expr);

            // Add in a new field
            recordStore.setHeaderUserField("qwop", Tuple.from(1066L).pack());

            // Delete the middle field
            recordStore.clearHeaderUserField("baz");
            assertNull(recordStore.getHeaderUserField("baz"));
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertEquals("µs", recordStore.getHeaderUserField("foo").toStringUtf8());
            assertNull(recordStore.getHeaderUserField("baz"));
            assertEquals(Tuple.from(1066L), Tuple.fromBytes(recordStore.getHeaderUserField("qwop").toByteArray()));
            RecordMetaDataProto.DataStoreInfo storeHeader = recordStore.getRecordStoreState().getStoreHeader();
            assertEquals(2, storeHeader.getUserFieldCount());
            commit(context);
        }
    }

    /**
     * Test that when the store header user fields are updated, all other concurrent operations fail with a conflict.
     */
    @Test
    public void testConflictIfUpdateUserHeaderField() throws Exception {
        final String userField = "my_important_user_header_field";
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.setHeaderUserField(userField, ByteArrayUtil.encodeInt(1066L));
            commit(context);
        }

        try (FDBRecordContext context1 = openContext(); FDBRecordContext context2 = openContext()) {
            openSimpleRecordStore(context1);
            FDBRecordStore recordStore1 = recordStore;
            openSimpleRecordStore(context2);
            FDBRecordStore recordStore2 = recordStore;

            recordStore1.setHeaderUserField(userField, ByteArrayUtil.encodeInt(1459L));
            commit(context1);

            // Need to save a record to make the conflict resolver actually check for conflicts
            recordStore2.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1776L)
                    .build());
            assertThrows(FDBExceptions.FDBStoreTransactionConflictException.class, context2::commit);
        }
    }

    @Test
    public void testUserVersionMonotonic() {
        final FDBRecordStoreBase.UserVersionChecker userVersion1 = (oldUserVersion, oldMetaDataVersion, metaData) -> CompletableFuture.completedFuture(101);
        final FDBRecordStoreBase.UserVersionChecker userVersion2 = (oldUserVersion, oldMetaDataVersion, metaData) -> CompletableFuture.completedFuture(102);

        final RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        try (FDBRecordContext context = openContext()) {
            recordStore = FDBRecordStore.newBuilder()
                .setContext(context).setKeySpacePath(path).setMetaDataProvider(builder).setUserVersionChecker(userVersion1)
                .create();
            assertEquals(101, recordStore.getUserVersion());
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            recordStore = FDBRecordStore.newBuilder()
                    .setContext(context).setKeySpacePath(path).setMetaDataProvider(builder).setUserVersionChecker(userVersion2)
                    .open();
            assertEquals(102, recordStore.getUserVersion());
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore.Builder storeBuilder = FDBRecordStore.newBuilder()
                    .setContext(context).setKeySpacePath(path).setMetaDataProvider(builder).setUserVersionChecker(userVersion1);
            RecordCoreException ex = assertThrows(RecordCoreException.class, storeBuilder::open);
            assertThat(ex.getMessage(), containsString("Stale user version"));
        }
    }

    static class SwitchingProvider implements RecordMetaDataProvider, FDBRecordStoreBase.UserVersionChecker {
        private boolean needOld = false;
        private final Integer defaultVersion;
        private final RecordMetaData metaData1;
        private final RecordMetaData metaData2;

        SwitchingProvider(int defaultVersion, RecordMetaData metaData1, RecordMetaData metaData2) {
            this.defaultVersion = defaultVersion;
            this.metaData1 = metaData1;
            this.metaData2 = metaData2;
        }

        @Nonnull
        @Override
        public RecordMetaData getRecordMetaData() {
            return needOld ? metaData1 : metaData2;
        }

        @Override
        public CompletableFuture<Integer> checkUserVersion(@Nonnull final RecordMetaDataProto.DataStoreInfo storeHeader, final RecordMetaDataProvider metaData) {
            if (storeHeader.getFormatVersion() == 0) {
                return CompletableFuture.completedFuture(defaultVersion);
            }
            int oldUserVersion = storeHeader.getUserVersion();
            if (oldUserVersion == 101) {
                needOld = true;
            }
            return CompletableFuture.completedFuture(oldUserVersion);
        }

        @Deprecated
        @Override
        public CompletableFuture<Integer> checkUserVersion(int oldUserVersion, int oldMetaDataVersion, RecordMetaDataProvider metaData) {
            throw new RecordCoreException("deprecated checkUserVersion called");
        }
    }

    @Test
    public void testUserVersionDeterminesMetaData() {
        final RecordMetaDataBuilder builder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        builder.setVersion(101);
        final RecordMetaData metaData1 = builder.getRecordMetaData();
        builder.setVersion(102);
        final RecordMetaData metaData2 = builder.getRecordMetaData();
        final SwitchingProvider oldProvider = new SwitchingProvider(101, metaData1, metaData1);
        final SwitchingProvider newProvider = new SwitchingProvider(102, metaData1, metaData2);

        try (FDBRecordContext context = openContext()) {
            recordStore = FDBRecordStore.newBuilder()
                    .setContext(context).setKeySpacePath(path).setMetaDataProvider(oldProvider).setUserVersionChecker(oldProvider)
                    .create();
            assertEquals(101, recordStore.getUserVersion());
            assertEquals(metaData1, recordStore.getRecordMetaData());
            assertEquals(metaData1.getVersion(), recordStore.getRecordStoreState().getStoreHeader().getMetaDataversion());
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            recordStore = FDBRecordStore.newBuilder()
                    .setContext(context).setKeySpacePath(path).setMetaDataProvider(newProvider).setUserVersionChecker(newProvider)
                    .open();
            assertEquals(101, recordStore.getUserVersion());
            assertEquals(metaData1, recordStore.getRecordMetaData());
            assertEquals(metaData1.getVersion(), recordStore.getRecordStoreState().getStoreHeader().getMetaDataversion());
            context.commit();
        }

        final SwitchingProvider newProvider2 = new SwitchingProvider(102, metaData1, metaData2);
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore.deleteStore(context, path);
            recordStore = FDBRecordStore.newBuilder()
                    .setContext(context).setKeySpacePath(path).setMetaDataProvider(newProvider2).setUserVersionChecker(newProvider2)
                    .create();
            assertEquals(102, recordStore.getUserVersion());
            assertEquals(metaData2, recordStore.getRecordMetaData());
            assertEquals(metaData2.getVersion(), recordStore.getRecordStoreState().getStoreHeader().getMetaDataversion());
            context.commit();
        }
    }

    @Test
    public void unionFieldUpdateCompatibility() throws Exception {
        final TestRecords1Proto.MySimpleRecord record1 = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1066L)
                .setNumValue2(42)
                .build();
        final TestRecords1Proto.MySimpleRecord record2 = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1415L)
                .setStrValueIndexed("second_record")
                .build();
        final TestRecords1Proto.MySimpleRecord record3 = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(800L)
                .setNumValue2(14)
                .build();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            RecordMetaData metaData = recordStore.getRecordMetaData();
            assertSame(TestRecords1Proto.RecordTypeUnion.getDescriptor().findFieldByName("_MySimpleRecord"),
                    metaData.getUnionFieldForRecordType(metaData.getRecordType("MySimpleRecord")));

            // Save a record using the field using the old meta-data
            recordStore.saveRecord(record1);

            // Save a record using the new union descriptor but in the old location
            context.ensureActive().set(
                    recordStore.recordsSubspace().pack(Tuple.from(record2.getRecNo(), SplitHelper.UNSPLIT_RECORD)),
                    TestRecordsDuplicateUnionFields.RecordTypeUnion.newBuilder()
                        .setMySimpleRecordOld(record2)
                        .build()
                        .toByteArray()
            );

            // Save a record using the new union descriptor in the new location
            context.ensureActive().set(
                    recordStore.recordsSubspace().pack(Tuple.from(record3.getRecNo(), SplitHelper.UNSPLIT_RECORD)),
                    TestRecordsDuplicateUnionFields.RecordTypeUnion.newBuilder()
                            .setMySimpleRecordNew(record3)
                            .build()
                            .toByteArray()
            );

            assertEquals(record1, recordStore.loadRecord(Tuple.from(record1.getRecNo())).getRecord());
            assertEquals(record2, recordStore.loadRecord(Tuple.from(record2.getRecNo())).getRecord());

            RecordCoreException e = assertThrows(RecordCoreException.class,
                    () -> recordStore.loadRecord(Tuple.from(record3.getRecNo())).getRecord());
            assertNotNull(e.getCause());
            assertThat(e.getCause(), instanceOf(RecordSerializationException.class));
            assertThat(e.getCause().getMessage(), containsString("because there are unknown fields"));

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, metaDataBuilder -> metaDataBuilder.updateRecords(TestRecordsDuplicateUnionFields.getDescriptor()));
            RecordMetaData metaData = recordStore.getRecordMetaData();
            assertSame(TestRecordsDuplicateUnionFields.RecordTypeUnion.getDescriptor().findFieldByName("_MySimpleRecord_new"),
                    metaData.getUnionFieldForRecordType(metaData.getRecordType("MySimpleRecord")));

            // All three records should be readable even though written by the previous store
            for (TestRecords1Proto.MySimpleRecord record : Arrays.asList(record1, record2, record3)) {
                FDBStoredRecord<Message> storedRecord = recordStore.loadRecord(Tuple.from(record.getRecNo()));
                assertNotNull(storedRecord);
                assertSame(metaData.getRecordType("MySimpleRecord"), storedRecord.getRecordType());
                assertEquals(record, storedRecord.getRecord());
            }

            commit(context);
        }
    }

    @Test
    public void importedRecordType() throws Exception {
        final RecordMetaDataHook hook = md -> md.addIndex("MySimpleRecord", "added_index", "num_value_2");

        try (FDBRecordContext context = openContext()) {
            openAnyRecordStore(TestRecordsImportProto.getDescriptor(), context, hook);

            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            recBuilder.setRecNo(1);
            recBuilder.setStrValueIndexed("abc");
            recBuilder.setNumValueUnique(123);
            recBuilder.setNumValue2(456);
            recordStore.saveRecord(recBuilder.build());
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openAnyRecordStore(TestRecordsImportProto.getDescriptor(), context, hook);
            FDBStoredRecord<Message> rec1 = recordStore.loadRecord(Tuple.from(1L));
            assertNotNull(rec1);
            TestRecords1Proto.MySimpleRecord.Builder myrec1 = TestRecords1Proto.MySimpleRecord.newBuilder();
            myrec1.mergeFrom(rec1.getRecord());
            assertEquals(123, myrec1.getNumValueUnique());
            assertEquals(Collections.singletonList(Tuple.from("abc", 1)),
                    recordStore.scanIndex(recordStore.getRecordMetaData().getIndex("MySimpleRecord$str_value_indexed"),
                            IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN).map(IndexEntry::getKey).asList().join());
            assertEquals(Collections.singletonList(Tuple.from(456, 1)),
                    recordStore.scanIndex(recordStore.getRecordMetaData().getIndex("added_index"),
                            IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN).map(IndexEntry::getKey).asList().join());
            commit(context);
        }
    }

    @Test
    public void commitChecks() throws Exception {
        // Start check now; fails even if added.
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            context.addCommitCheck(checkRec1Exists());

            TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1L)
                    .build();
            recordStore.saveRecord(rec);
            assertThrows(RecordDoesNotExistException.class, () -> commit(context));
        }
        // Deferred check; fails if not added.
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            context.addCommitCheck(this::checkRec1Exists);

            assertThrows(RecordDoesNotExistException.class, () -> commit(context));
        }
        // Succeeds if added.
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            context.addCommitCheck(this::checkRec1Exists);

            TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1L)
                    .build();
            recordStore.saveRecord(rec);
            commit(context);
        }
        // Immediate succeeds too now.
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            context.addCommitCheck(checkRec1Exists());
            commit(context);
        }
    }

    private CompletableFuture<Void> checkRec1Exists() {
        return recordStore.recordExistsAsync(Tuple.from(1)).thenAccept(exists -> {
            if (!exists) {
                throw new RecordDoesNotExistException("required record does not exist");
            }
        });
    }

    @Nonnull
    private FDBMetaDataStore openMetaDataStore(@Nonnull FDBRecordContext context, @Nonnull KeySpacePath metaDataPath, boolean clear) {
        FDBMetaDataStore metaDataStore = new FDBMetaDataStore(context, metaDataPath);
        metaDataStore.setDependencies(new Descriptors.FileDescriptor[] {
                RecordMetaDataOptionsProto.getDescriptor()
        });
        metaDataStore.setMaintainHistory(false);
        if (clear) {
            context.clear(Range.startsWith(metaDataPath.toSubspace(context).pack()));
        }
        return metaDataStore;
    }

    @Test
    public void testNoUnion() {
        final KeySpacePath metaDataPath = pathManager.createPath(TestKeySpace.META_DATA_STORE);
        int version;
        try (FDBRecordContext context = fdb.openContext()) {
            FDBMetaDataStore metaDataStore = openMetaDataStore(context, metaDataPath, true);
            metaDataStore.saveRecordMetaData(TestNoUnionProto.getDescriptor());
            context.commit();
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            version = metaDataStore.getRecordMetaData().getVersion();
        }

        // Store a MySimpleRecord record.
        try (FDBRecordContext context = fdb.openContext()) {
            FDBMetaDataStore metaDataStore = openMetaDataStore(context, metaDataPath, false);
            FDBRecordStore recordStore = FDBRecordStore.newBuilder().setContext(context).setKeySpacePath(path)
                    .setMetaDataStore(metaDataStore)
                    .createOrOpen();
            assertEquals(version, recordStore.getRecordMetaData().getVersion());
            Descriptors.Descriptor mySimpleRecordDescriptor = recordStore.getRecordMetaData().getRecordType("MySimpleRecord").getDescriptor();
            final Descriptors.FieldDescriptor recNo = mySimpleRecordDescriptor.findFieldByName("rec_no");
            final Descriptors.FieldDescriptor numValue2 = mySimpleRecordDescriptor.findFieldByName("num_value_2");
            final Descriptors.FieldDescriptor strValueIndexed = mySimpleRecordDescriptor.findFieldByName("str_value_indexed");
            final Descriptors.FieldDescriptor numValue3Indexed = mySimpleRecordDescriptor.findFieldByName("num_value_3_indexed");
            Message.Builder messageBuilder = DynamicMessage.newBuilder(mySimpleRecordDescriptor);
            messageBuilder.setField(recNo, 1066L);
            messageBuilder.setField(numValue2, 42);
            messageBuilder.setField(strValueIndexed, "value");
            messageBuilder.setField(numValue3Indexed, 1729);
            recordStore.saveRecord(messageBuilder.build());
        }

        // Store another MySimpleRecord record with local file descriptor.
        try (FDBRecordContext context = fdb.openContext()) {
            FDBMetaDataStore metaDataStore = openMetaDataStore(context, metaDataPath, false);
            metaDataStore.setLocalFileDescriptor(TestNoUnionProto.getDescriptor());
            FDBRecordStore recordStore = FDBRecordStore.newBuilder().setContext(context).setKeySpacePath(path)
                    .setMetaDataStore(metaDataStore)
                    .createOrOpen();
            assertEquals(version, recordStore.getRecordMetaData().getVersion());
            TestNoUnionProto.MySimpleRecord record = TestNoUnionProto.MySimpleRecord.newBuilder()
                    .setRecNo(1067L)
                    .setNumValue2(43)
                    .setStrValueIndexed("value2")
                    .setNumValue3Indexed(1730)
                    .build();
            recordStore.saveRecord(record);
        }

        // Add a record type.
        try (FDBRecordContext context = fdb.openContext()) {
            FDBMetaDataStore metaDataStore = openMetaDataStore(context, metaDataPath, false);
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertEquals(version , metaDataStore.getRecordMetaData().getVersion());
            DescriptorProtos.DescriptorProto newRecordType = DescriptorProtos.DescriptorProto.newBuilder()
                    .setName("MyNewRecord")
                    .addField(DescriptorProtos.FieldDescriptorProto.newBuilder()
                            .setLabel(DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL)
                            .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32)
                            .setName("rec_no")
                            .setNumber(1))
                    .build();
            metaDataStore.mutateMetaData(metaDataProto -> MetaDataProtoEditor.addRecordType(metaDataProto, newRecordType, field("rec_no")));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MySimpleRecord"));
            assertNotNull(metaDataStore.getRecordMetaData().getRecordType("MyNewRecord"));
            assertEquals(version + 1 , metaDataStore.getRecordMetaData().getVersion());
            assertEquals(version + 1 , metaDataStore.getRecordMetaData().getRecordType("MyNewRecord").getSinceVersion().intValue());
            context.commit();
        }

        // Store a MyNewRecord record.
        try (FDBRecordContext context = fdb.openContext()) {
            FDBMetaDataStore metaDataStore = openMetaDataStore(context, metaDataPath, false);
            FDBRecordStore recordStore = FDBRecordStore.newBuilder().setContext(context).setKeySpacePath(path)
                    .setMetaDataStore(metaDataStore)
                    .createOrOpen();
            assertEquals(version + 1, recordStore.getRecordMetaData().getVersion());
            Descriptors.Descriptor myNewRecordDescriptor = recordStore.getRecordMetaData().getRecordType("MyNewRecord").getDescriptor();
            final Descriptors.FieldDescriptor recNo = myNewRecordDescriptor.findFieldByName("rec_no");
            Message.Builder messageBuilder = DynamicMessage.newBuilder(myNewRecordDescriptor);
            messageBuilder.setField(recNo, 2345);
            recordStore.saveRecord(messageBuilder.build());
        }
    }

    @Test
    public void testDryRunSaveRecord() throws Exception {
        TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
        TestRecords1Proto.MySimpleRecord record1 = recBuilder.setRecNo(1).setStrValueIndexed("abc").build();
        FDBStoredRecord<Message> result1;
        FDBStoredRecord<Message> result2;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            result1 = recordStore.dryRunSaveRecordAsync(record1, FDBRecordStoreBase.RecordExistenceCheck.ERROR_IF_EXISTS).get();
            // assert returns the message to be saved
            assertEquals(record1, result1.getRecord());
            commit(context);
        }
        // assert dryRun doesn't save the record and doesn't save the index
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, metaDataBuilder -> metaDataBuilder.updateRecords(TestRecords1Proto.getDescriptor()));
            assertNull(recordStore.loadRecord(Tuple.from(1)));
            assertEquals(Collections.emptyList(),
                    recordStore.scanIndex(recordStore.getRecordMetaData().getIndex("MySimpleRecord$str_value_indexed"),
                            IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN).map(IndexEntry::getKey).asList().join());
            commit(context);
        }
        // assert normal save returns same sizeInfo
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, metaDataBuilder -> metaDataBuilder.updateRecords(TestRecords1Proto.getDescriptor()));
            result2 = recordStore.saveRecordAsync(record1, FDBRecordStoreBase.RecordExistenceCheck.ERROR_IF_EXISTS).get();
            // assert returns the message to be saved
            assertEquals(record1, result2.getRecord());
            // assert dry run and normal save returns same sizeInfo
            assertEquals(result1.getKeyCount(), result2.getKeyCount());
            assertEquals(result1.getKeySize(), result2.getKeySize());
            assertEquals(result1.getValueSize(), result2.getValueSize());
            assertEquals(result1.isSplit(), result2.isSplit());
            assertEquals(result1.isVersionedInline(), result2.isVersionedInline());
            commit(context);
        }
        // assert normal save does save the record and change index
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, metaDataBuilder -> metaDataBuilder.updateRecords(TestRecords1Proto.getDescriptor()));
            assertNotNull(result2.getRecord(), "record2 was missing");
            assertEquals(record1, result2.getRecord());
            assertEquals(Collections.singletonList(Tuple.from("abc", 1)),
                    recordStore.scanIndex(recordStore.getRecordMetaData().getIndex("MySimpleRecord$str_value_indexed"),
                            IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN).map(IndexEntry::getKey).asList().join());

            commit(context);
        }
    }

    private long checkLastUpdateTimeUpdated(long previousUpdateTime, @Nullable RecordMetaDataHook metaDataHook, @Nonnull Consumer<FDBRecordStore> updateOperation) {
        long updateTime;
        try (FDBRecordContext context = openContext()) {
            final long beforeOpenTime = System.currentTimeMillis();
            assumeTrue(previousUpdateTime < beforeOpenTime, "time has not advanced since first update");
            openSimpleRecordStore(context, metaDataHook);
            updateOperation.accept(recordStore);
            updateTime = recordStore.getRecordStoreState().getStoreHeader().getLastUpdateTime();
            assertThat(updateTime, greaterThan(previousUpdateTime));
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, metaDataHook);
            assertEquals(updateTime, recordStore.getRecordStoreState().getStoreHeader().getLastUpdateTime());
        }
        return updateTime;
    }

    /**
     * Test that the various ways of updating a the store header all update the last updated time.
     */
    @Test
    public void updateLastUpdatedTime() {
        RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.addIndex("MySimpleRecord", "num_value_2");

        AtomicInteger metaDataVersion = new AtomicInteger();
        long firstUpdateTime = checkLastUpdateTimeUpdated(0L, null, store -> metaDataVersion.set(store.getRecordMetaData().getVersion()));

        // Update the meta-data to a new version
        long secondUpdateTime = checkLastUpdateTimeUpdated(firstUpdateTime, hook,
                store -> assertEquals(metaDataVersion.get() + 1, store.getRecordMetaData().getVersion()));

        // Change the store state cacheability
        long thirdUpdateTime = checkLastUpdateTimeUpdated(secondUpdateTime, hook,
                store -> store.setStateCacheability(true));

        // Set a header user field
        long fourthUpdateTime = checkLastUpdateTimeUpdated(thirdUpdateTime, hook,
                store -> store.setHeaderUserField("field_name", new byte[]{0x01, 0x2}));

        // Clear a header user field
        checkLastUpdateTimeUpdated(fourthUpdateTime, hook,
                store -> store.clearHeaderUserField("field_name"));
    }

}
