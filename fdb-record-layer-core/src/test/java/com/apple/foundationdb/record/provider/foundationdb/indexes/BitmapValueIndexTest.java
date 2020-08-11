/*
 * BitmapValueIndexTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2020 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for {@code BITMAP_VALUE} type indexes.
 */
@Tag(Tags.RequiresFDB)
public class BitmapValueIndexTest extends FDBRecordStoreTestBase {

    @Test
    public void basic() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, REC_NO_BY_STR_NUM3_HOOK);
            saveRecords(100, 200);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, REC_NO_BY_STR_NUM3_HOOK);
            assertThat(
                    collectOnBits(recordStore.scanIndex(
                            recordStore.getRecordMetaData().getIndex("rec_no_by_str_num3"), IndexScanType.BY_GROUP,
                            TupleRange.allOf(Tuple.from("odd", 1)),
                            null, ScanProperties.FORWARD_SCAN)),
                    equalTo(IntStream.range(100, 200).boxed()
                            .filter(i -> (i & 1) == 1)
                            .filter(i -> (i % 5) == 1)
                            .collect(Collectors.toList())));
            assertThat(
                    collectOnBits(recordStore.scanIndex(
                            recordStore.getRecordMetaData().getIndex("rec_no_by_str_num3"), IndexScanType.BY_GROUP,
                            TupleRange.between(Tuple.from("odd", 1, 150), Tuple.from("odd", 1, 175)),
                            null, ScanProperties.FORWARD_SCAN)),
                    equalTo(IntStream.range(150, 175).boxed()
                            .filter(i -> (i & 1) == 1)
                            .filter(i -> (i % 5) == 1)
                            .collect(Collectors.toList())));
        }
    }

    @Test
    public void aggregateFunction() {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, REC_NO_BY_STR_NUM3_HOOK);
            saveRecords(100, 200);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, REC_NO_BY_STR_NUM3_HOOK);
            final IndexAggregateFunction aggregateFunction = new IndexAggregateFunction(FunctionNames.BITMAP_VALUE, REC_NO_BY_STR_NUM3, null);
            assertThat(
                    collectOnBits(recordStore.evaluateAggregateFunction(
                            Collections.singletonList("MySimpleRecord"), aggregateFunction,
                            TupleRange.allOf(Tuple.from("odd", 3)),
                            IsolationLevel.SERIALIZABLE).join().getBytes(0), 0),
                    equalTo(IntStream.range(100, 200).boxed()
                            .filter(i -> (i & 1) == 1)
                            .filter(i -> (i % 5) == 3)
                            .collect(Collectors.toList())));
            assertThat(
                    collectOnBits(recordStore.evaluateAggregateFunction(
                            Collections.singletonList("MySimpleRecord"), aggregateFunction,
                            TupleRange.between(Tuple.from("odd", 3, 160), Tuple.from("odd", 3, 180)),
                            IsolationLevel.SERIALIZABLE).join().getBytes(0), 160),
                    equalTo(IntStream.range(160, 180).boxed()
                            .filter(i -> (i & 1) == 1)
                            .filter(i -> (i % 5) == 3)
                            .collect(Collectors.toList())));
        }
    }

    @Test
    public void nonPrimaryKey() {
        final RecordMetaDataHook num_by_num3_hook = metadata -> {
            metadata.addIndex(metadata.getRecordType("MySimpleRecord"),
                              new Index("num_by_num3",
                                        concatenateFields("num_value_3_indexed", "num_value_unique").group(1),
                                        IndexTypes.BITMAP_VALUE, SMALL_BITMAP_OPTIONS));
        };
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, num_by_num3_hook);
            saveRecords(0, 100);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, num_by_num3_hook);
            assertThat(
                    collectOnBits(recordStore.scanIndex(
                            recordStore.getRecordMetaData().getIndex("num_by_num3"), IndexScanType.BY_GROUP,
                            TupleRange.allOf(Tuple.from(2)),
                            null, ScanProperties.FORWARD_SCAN)),
                    equalTo(IntStream.range(1000, 1100).boxed()
                            .filter(i -> (i % 5) == 2)
                            .collect(Collectors.toList())));
        }
    }

    protected static final KeyExpression REC_NO_BY_STR_NUM3 = concatenateFields("str_value_indexed", "num_value_3_indexed", "rec_no").group(1);
    protected static final Map<String, String> SMALL_BITMAP_OPTIONS = Collections.singletonMap(IndexOptions.BITMAP_VALUE_ENTRY_SIZE_OPTION, "16");
    protected static final RecordMetaDataHook REC_NO_BY_STR_NUM3_HOOK = metadata -> {
        metadata.addIndex(metadata.getRecordType("MySimpleRecord"), new Index("rec_no_by_str_num3", REC_NO_BY_STR_NUM3, IndexTypes.BITMAP_VALUE, SMALL_BITMAP_OPTIONS));
    };

    protected void saveRecords(int start, int end) {
        for (int recNo = start; recNo < end; recNo++) {
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(recNo)
                    .setStrValueIndexed((recNo & 1) == 1 ? "odd" : "even")
                    .setNumValueUnique(recNo + 1000)
                    .setNumValue3Indexed(recNo % 5)
                    .build());
        }
    }

    protected List<Integer> collectOnBits(@Nonnull RecordCursor<IndexEntry> indexEntries) {
        return indexEntries.reduce(new ArrayList<Integer>(), (list, entries) -> {
            list.addAll(collectOnBits(entries));
            return list;
        }).join();
    }

    protected List<Integer> collectOnBits(@Nonnull IndexEntry indexEntry) {
        return collectOnBits(indexEntry.getValue().getBytes(0), (int)indexEntry.getKey().getLong(indexEntry.getKeySize() - 1));
    }

    protected List<Integer> collectOnBits(@Nonnull byte[] bitmap, int offset) {
        final List<Integer> result = new ArrayList<>();
        for (int i = 0; i < bitmap.length; i++) {
            if (bitmap[i] != 0) {
                for (int j = 0; j < 8; j++) {
                    if ((bitmap[i] & (1 << j)) != 0) {
                        result.add(offset + i * 8 + j);
                    }
                }
            }
        }
        return result;
    }

}
