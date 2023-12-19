/*
 * PermutedMinMaxIndexTest.java
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

import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;


/**
 * Tests for permuted min / max type indexes.
 */
@Tag(Tags.RequiresFDB)
public class MinMaxIndexTest extends FDBRecordStoreTestBase {

    protected static final String INDEX_NAME = "permuted";

    protected static RecordMetaDataHook hook(boolean min) {
        return md -> {
            md.addIndex("MySimpleRecord", new Index(INDEX_NAME,
                    Key.Expressions.concatenateFields("str_value_indexed", "num_value_2", "num_value_3_indexed").group(1),
                    min ? IndexTypes.MIN : IndexTypes.MAX,
                    Collections.singletonMap(IndexOptions.PERMUTED_SIZE_OPTION, "1")));
        };
    }

    @Test
    public void min() {
        final RecordMetaDataHook hook = hook(true);
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            saveRecord(1, "yes", 111, 100);
            saveRecord(2, "yes", 222, 200);
            saveRecord(99, "no", 66, 0);

            assertEquals(Arrays.asList(
                    Tuple.from(100, 111),
                    Tuple.from(200, 222)
            ), scanGroup(Tuple.from("yes"), false));

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            saveRecord(3, "yes", 111, 50);
            assertEquals(Arrays.asList(
                    Tuple.from(50, 111),
                    Tuple.from(200, 222)
            ), scanGroup(Tuple.from("yes"), false));

            saveRecord(3, "yes", 111, 150);
            assertEquals(Arrays.asList(
                    Tuple.from(100, 111),
                    Tuple.from(200, 222)
            ), scanGroup(Tuple.from("yes"), false));

            recordStore.deleteRecord(Tuple.from(1));
            assertEquals(Arrays.asList(
                    Tuple.from(150, 111),
                    Tuple.from(200, 222)
            ), scanGroup(Tuple.from("yes"), false));

            recordStore.deleteRecord(Tuple.from(3));
            assertEquals(Arrays.asList(
                    Tuple.from(200, 222)
            ), scanGroup(Tuple.from("yes"), false));
        }
    }

    @Test
    public void max() {
        final RecordMetaDataHook hook = hook(false);
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            saveRecord(1, "yes", 111, 100);
            saveRecord(2, "yes", 222, 200);
            saveRecord(99, "no", 666, 0);

            assertEquals(Arrays.asList(
                    Tuple.from(200, 222),
                    Tuple.from(100, 111)
            ), scanGroup(Tuple.from("yes"), true));

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            saveRecord(3, "yes", 111, 250);
            assertEquals(Arrays.asList(
                    Tuple.from(250, 111),
                    Tuple.from(200, 222)
            ), scanGroup(Tuple.from("yes"), true));

            saveRecord(3, "yes", 111, 50);
            assertEquals(Arrays.asList(
                    Tuple.from(200, 222),
                    Tuple.from(100, 111)
            ), scanGroup(Tuple.from("yes"), true));

            recordStore.deleteRecord(Tuple.from(1));
            assertEquals(Arrays.asList(
                    Tuple.from(200, 222),
                    Tuple.from(50, 111)
            ), scanGroup(Tuple.from("yes"), true));

            recordStore.deleteRecord(Tuple.from(3));
            assertEquals(Arrays.asList(
                    Tuple.from(200, 222)
            ), scanGroup(Tuple.from("yes"), true));
        }
    }

    @Test
    public void tie() {
        final RecordMetaDataHook hook = hook(false);
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            saveRecord(1, "yes", 111, 100);
            saveRecord(2, "yes", 111, 50);
            saveRecord(3, "yes", 111, 100);

            assertEquals(Arrays.asList(
                    Tuple.from(100, 111)
                    ),
                    scanGroup(Tuple.from("yes"), true));

            recordStore.deleteRecord(Tuple.from(1));
            assertEquals(Arrays.asList(
                    Tuple.from(100, 111)
                    ),
                    scanGroup(Tuple.from("yes"), true));

            recordStore.deleteRecord(Tuple.from(3));
            assertEquals(Arrays.asList(
                    Tuple.from(50, 111)
                    ),
                    scanGroup(Tuple.from("yes"), true));
        }
    }

    @Test
    public void deleteWhere() {
        final RecordMetaDataHook hook = md -> {
            final KeyExpression pkey = Key.Expressions.concatenateFields("num_value_2", "num_value_3_indexed", "rec_no");
            md.getRecordType("MySimpleRecord").setPrimaryKey(pkey);
            md.getRecordType("MyOtherRecord").setPrimaryKey(pkey);
            md.removeIndex("MySimpleRecord$str_value_indexed");
            md.removeIndex("MySimpleRecord$num_value_3_indexed");
            md.removeIndex("MySimpleRecord$num_value_unique");
            md.removeIndex(COUNT_INDEX.getName());
            md.removeIndex(COUNT_UPDATES_INDEX.getName());
            md.addIndex("MySimpleRecord", new Index(INDEX_NAME,
                    Key.Expressions.concatenateFields("num_value_2", "num_value_3_indexed", "str_value_indexed", "num_value_unique").group(1),
                    IndexTypes.MAX, Collections.singletonMap(IndexOptions.PERMUTED_SIZE_OPTION, "2")));
        };
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            saveRecord(100, "yes", 1, 1);
            saveRecord(150, "yes", 1, 1);
            saveRecord(200, "no", 1, 1);
            saveRecord(300, "yes", 1, 2);
            saveRecord(400, "no", 1, 2);
            saveRecord(500, "maybe", 2, 1);

            assertEquals(Arrays.asList(
                    Tuple.from(1, 150, 1, "yes"),
                    Tuple.from(1, 200, 1, "no"),
                    Tuple.from(1, 300, 2, "yes"),
                    Tuple.from(1, 400, 2, "no"),
                    Tuple.from(2, 500, 1, "maybe")
            ), scanGroup(Tuple.from(), false));

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            recordStore.deleteRecordsWhere(Query.field("num_value_2").equalsValue(2));

            assertEquals(Arrays.asList(
                    Tuple.from(1, 150, 1, "yes"),
                    Tuple.from(1, 200, 1, "no"),
                    Tuple.from(1, 300, 2, "yes"),
                    Tuple.from(1, 400, 2, "no")
            ), scanGroup(Tuple.from(), false));

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, hook);

            assertThrows(Query.InvalidExpressionException.class, () -> {
                recordStore.deleteRecordsWhere(Query.and(
                        Query.field("num_value_2").equalsValue(2),
                        Query.field("num_value_3_indexed").equalsValue(1)));
            });
        }
    }

    private void saveRecord(int recNo, String strValue, int value2, int value3) {
        recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(recNo)
                .setStrValueIndexed(strValue)
                .setNumValue2(value2)
                .setNumValue3Indexed(value3)
                .setNumValueUnique(recNo)
                .build());
    }

    private List<Tuple> scanGroup(Tuple group, boolean reverse) {
        return recordStore.scanIndex(recordStore.getRecordMetaData().getIndex(INDEX_NAME), IndexScanType.BY_GROUP,
                TupleRange.allOf(group), null, reverse ? ScanProperties.REVERSE_SCAN : ScanProperties.FORWARD_SCAN)
                .map(entry -> TupleHelpers.subTuple(entry.getKey(), group.size(), entry.getKeySize()))
                .asList()
                .join();
    }

}
