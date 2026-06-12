/*
 * SerializedSizeKeyExpressionTest.java
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

package com.apple.foundationdb.record.metadata.expressions;

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexableRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SerializedSizeFunctionKeyExpressionTest extends FDBRecordStoreTestBase {
    private static final KeyExpression expression = FunctionKeyExpression.create(SerializedSizeFunctionKeyExpression.FUNCTION_NAME, Key.Expressions.empty());

    static {
        assertThat(expression)
                .isInstanceOf(SerializedSizeFunctionKeyExpression.class);
        assertThat(expression.getColumnSize())
                .isOne();
    }

    private static <M extends Message> FDBIndexableRecord<M> createIndexable(@Nonnull M message) {
        final RecordMetaData metaData = RecordMetaData.build(message.getDescriptorForType().getFile());
        return FDBStoredRecord.<M>newBuilder()
                .setRecordType(metaData.getRecordType(message.getDescriptorForType().getName()))
                .setRecord(message)
                .setPrimaryKey(TupleHelpers.EMPTY)
                .setValueSize(message.getSerializedSize())
                .build();
    }

    @Test
    void evalOnNull() {
        assertThat(expression.evaluateSingleton(null))
                .isEqualTo(Key.Evaluated.scalar(0L));
    }

    @Test
    void evalOnEmpty() {
        assertThat(expression.evaluateSingleton(createIndexable(TestRecords1Proto.MyOtherRecord.getDefaultInstance())))
                .isEqualTo(Key.Evaluated.scalar(0L));
    }

    @Test
    void evalOnRecord() {
        final Message message = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setStrValueIndexed("str_value")
                .setNumValue2(42)
                .build();
        assertThat(expression.evaluateSingleton(createIndexable(message)))
                .isEqualTo(Key.Evaluated.scalar((long) message.getSerializedSize()));
    }

    @Test
    void maintainValueIndexOnSize() throws Exception {
        final Index serializedSizeIndex = new Index("serializedSize", expression);

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, metaData -> metaData.addUniversalIndex(serializedSizeIndex));

            final List<IndexEntry> beforeInsert = recordStore.scanIndex(serializedSizeIndex, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                    .asList()
                    .get();
            assertThat(beforeInsert)
                    .isEmpty();

            TestRecords1Proto.MySimpleRecord record1 = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .setStrValueIndexed("hello")
                    .setNumValue3Indexed(42)
                    .build();
            recordStore.saveRecord(record1);

            TestRecords1Proto.MyOtherRecord record2 = TestRecords1Proto.MyOtherRecord.newBuilder()
                    .setRecNo(1415L)
                    .setNumValue3Indexed(42)
                    .build();
            recordStore.saveRecord(record2);

            final List<IndexEntry> afterInsert = recordStore.scanIndex(serializedSizeIndex, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                    .asList()
                    .get();
            assertThat(afterInsert)
                    .hasSize(2);
            assertThat(afterInsert.get(0).getKey().getLong(0))
                    .isEqualTo(TestRecords1Proto.RecordTypeUnion.newBuilder().setMyOtherRecord(record2).build().getSerializedSize() + 13L);
            assertThat(afterInsert.get(1).getKey().getLong(0))
                    .isEqualTo(TestRecords1Proto.RecordTypeUnion.newBuilder().setMySimpleRecord(record1).build().getSerializedSize() + 13L);

            recordStore.deleteRecord(Tuple.from(record2.getRecNo()));

            final List<IndexEntry> afterDelete = recordStore.scanIndex(serializedSizeIndex, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                    .asList()
                    .get();
            assertThat(afterDelete)
                    .hasSize(1);
            assertThat(afterDelete.get(0).getKey().getLong(0))
                    .isEqualTo(TestRecords1Proto.RecordTypeUnion.newBuilder().setMySimpleRecord(record1).build().getSerializedSize() + 13L);

            commit(context);
        }
    }
}
