/*
 * FDBRecordStoreAutoQATest.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecordsWithUnionProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.base.Strings;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Additional tests of {@link FDBRecordStore}, focused on deleteRecordsWhere, split records and existence checks.
 */
@Tag(Tags.RequiresFDB)
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class FDBRecordStoreAutoQATest extends FDBRecordStoreTestBase {
    private static final String PATH_VALUE_INDEX = "MyRecord$path_str";
    private static final String PATH_COUNT_INDEX = "MyRecord$path_count";
    private static final KeyExpression PATH = field("header").nest("path");

    private static final RecordMetaDataHook PATH_PREFIX_HOOK = metaData -> {
        metaData.getRecordType("MyRecord").setPrimaryKey(concat(PATH, field("header").nest("rec_no")));
        metaData.addIndex("MyRecord", new Index(PATH_VALUE_INDEX, concat(PATH, field("str_value"))));
        metaData.addIndex("MyRecord", new Index(PATH_COUNT_INDEX, new GroupingKeyExpression(PATH, 0), IndexTypes.COUNT));
    };

    @Test
    void deleteRecordsWhereStringPrefixKeepsIndexEntriesOfNulSuffixedPrefix() throws Exception {
        // BUG AQA-31: StandardIndexMaintainer.deleteWhere clears [pack(prefix), strinc(pack(prefix))), which also covers
        // the index entries of a sibling prefix that extends the value after a NUL ("a\u0000b" for "a"). Spec:
        // FDBRecordStoreBase.deleteRecordsWhere javadoc, "Delete records and associated index entries matching".
        // deleting path "a" must not remove index entries of records with path "a\u0000b", whose records survive
        final String sibling = "a\u0000b";
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, PATH_PREFIX_HOOK);
            saveHeaderRecord(1, "a", 1, "deleted");
            saveHeaderRecord(2, sibling, 2, "kept");
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, PATH_PREFIX_HOOK);
            recordStore.deleteRecordsWhere(Query.field("header").matches(Query.field("path").equalsValue("a")));
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, PATH_PREFIX_HOOK);
            assertThat(recordStore.loadRecord(Tuple.from("a", 1L))).isNull();
            assertThat(recordStore.loadRecord(Tuple.from(sibling, 2L))).isNotNull();
            final List<IndexEntry> entries = recordStore.scanIndex(recordStore.getRecordMetaData().getIndex(PATH_VALUE_INDEX),
                    IndexScanType.BY_VALUE, TupleRange.allOf(Tuple.from(sibling)), null, ScanProperties.FORWARD_SCAN)
                    .asList().join();
            assertThat(entries).extracting(IndexEntry::getKey).containsExactly(Tuple.from(sibling, "kept", 2L));
        }
    }

    @Test
    void deleteRecordsWhereStringPrefixKeepsCountOfNulSuffixedPrefix() throws Exception {
        // BUG AQA-31: StandardIndexMaintainer.deleteWhere clears [pack(prefix), strinc(pack(prefix))), which also covers
        // the index entries of a sibling prefix that extends the value after a NUL ("a\u0000b" for "a"). Spec:
        // FDBRecordStoreBase.deleteRecordsWhere javadoc, "Delete records and associated index entries matching".
        // deleting path "a" must not reset the grouped COUNT of the surviving group "a\u0000b"
        final String sibling = "a\u0000b";
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, PATH_PREFIX_HOOK);
            saveHeaderRecord(1, "a", 1, "deleted");
            saveHeaderRecord(2, sibling, 2, "kept");
            saveHeaderRecord(3, sibling, 3, "kept too");
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, PATH_PREFIX_HOOK);
            recordStore.deleteRecordsWhere(Query.field("header").matches(Query.field("path").equalsValue("a")));
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openRecordWithHeader(context, PATH_PREFIX_HOOK);
            assertThat(recordStore.scanRecords(TupleRange.allOf(Tuple.from(sibling)), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join()).isEqualTo(2);
            final Index countIndex = recordStore.getRecordMetaData().getIndex(PATH_COUNT_INDEX);
            final IndexAggregateFunction count = new IndexAggregateFunction(FunctionNames.COUNT,
                    countIndex.getRootExpression(), countIndex.getName());
            final Tuple result = recordStore.evaluateAggregateFunction(List.of("MyRecord"), count,
                    Key.Evaluated.scalar(sibling), IsolationLevel.SERIALIZABLE).join();
            assertThat(result.getLong(0)).isEqualTo(2L);
        }
    }

    @Test
    void saveRecordOverwritingSplitRecordWithSmallRecordLeavesNoStaleSplits() throws Exception {
        // a small record saved over a split record must occupy the same keys as a freshly saved small record
        saveAndSplitSimpleRecord(1L, Strings.repeat("x", SplitHelper.SPLIT_RECORD_SIZE * 2 + 10), 1);
        final TestRecords1Proto.MySimpleRecord small1 = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1L).setStrValueIndexed("small").setNumValueUnique(1).build();
        final TestRecords1Proto.MySimpleRecord small2 = small1.toBuilder().setRecNo(2L).setNumValueUnique(2).build();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);
            recordStore.saveRecord(small1);
            recordStore.saveRecord(small2);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, TEST_SPLIT_HOOK);
            final List<KeyValue> keys1 = context.ensureActive().getRange(recordStore.recordsSubspace().range(Tuple.from(1L))).asList().join();
            final List<KeyValue> keys2 = context.ensureActive().getRange(recordStore.recordsSubspace().range(Tuple.from(2L))).asList().join();
            assertThat(keys1).hasSameSizeAs(keys2);
            final FDBStoredRecord<Message> loaded = recordStore.loadRecord(Tuple.from(1L));
            assertThat(loaded).isNotNull();
            assertThat(loaded.getRecord()).isEqualTo(small1);
            assertThat(loaded.getKeyCount()).isEqualTo(1);
        }
    }

    @Test
    void saveRecordAsyncConcurrentInsertsOfSamePrimaryKeyFailOnce() throws Exception {
        // two concurrent ERROR_IF_EXISTS saves of the same primary key in one transaction must not both succeed
        final TestRecords1Proto.MySimpleRecord first = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(1L).setStrValueIndexed("first").setNumValueUnique(1).build();
        final TestRecords1Proto.MySimpleRecord second = first.toBuilder().setStrValueIndexed("second").setNumValueUnique(2).build();
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            final CompletableFuture<FDBStoredRecord<Message>> f1 =
                    recordStore.saveRecordAsync(first, FDBRecordStoreBase.RecordExistenceCheck.ERROR_IF_EXISTS);
            final CompletableFuture<FDBStoredRecord<Message>> f2 =
                    recordStore.saveRecordAsync(second, FDBRecordStoreBase.RecordExistenceCheck.ERROR_IF_EXISTS);
            final long failures = List.of(f1, f2).stream()
                    .filter(f -> f.handle((r, e) -> e != null).join())
                    .count();
            assertThat(failures).isEqualTo(1L);
            assertThatThrownBy(() -> CompletableFuture.allOf(f1, f2).join())
                    .isInstanceOf(CompletionException.class)
                    .hasCauseInstanceOf(RecordAlreadyExistsException.class);
        }
    }

    @Test
    void saveRecordErrorIfRecordTypeChangedRejectsOtherTypeWithSamePrimaryKey() throws Exception {
        // ERROR_IF_RECORD_TYPE_CHANGED must throw and keep the old record when the existing record has another type
        try (FDBRecordContext context = openContext()) {
            openUnionRecordStore(context);
            recordStore.saveRecord(TestRecordsWithUnionProto.MySimpleRecord2.newBuilder()
                    .setStrValueIndexed("key").setEtag(1).build());
            assertThatThrownBy(() -> recordStore.saveRecord(
                    TestRecordsWithUnionProto.MySimpleRecord3.newBuilder().setStrValueIndexed("key").build(),
                    FDBRecordStoreBase.RecordExistenceCheck.ERROR_IF_RECORD_TYPE_CHANGED))
                    .isInstanceOf(RecordTypeChangedException.class);
            final FDBStoredRecord<Message> loaded = recordStore.loadRecord(Tuple.from("key"));
            assertThat(loaded).isNotNull();
            assertThat(loaded.getRecordType().getName()).isEqualTo("MySimpleRecord2");
        }
    }
}
