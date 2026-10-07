/*
 * RecordChangesIndexTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto.MySimpleRecord;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecordBuilder;
import com.apple.foundationdb.record.provider.foundationdb.SplitHelper;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Function;

/**
 * Tests for {@code RECORD_CHANGES} type indexes.
 */
@Tag(Tags.RequiresFDB)
class RecordChangesIndexTest extends FDBRecordStoreTestBase {

    @Nonnull
    private final RecordMetaDataHook globalVersionIndex =
            metaDataBuilder -> {
                metaDataBuilder.setSplitLongRecords(true);
                metaDataBuilder.addUniversalIndex(new Index("globalChanges", VersionKeyExpression.VERSION, IndexTypes.RECORD_CHANGES));
            };

    @Test
    void basicGlobalIndex() {
        MySimpleRecord.Builder record1 = MySimpleRecord.newBuilder().setRecNo(1066L).setNumValue2(1);
        MySimpleRecord.Builder record2 = MySimpleRecord.newBuilder().setRecNo(1067L).setNumValue2(2);

        FDBStoredRecord<Message> record1_1;
        FDBStoredRecord<Message> record2_1;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, globalVersionIndex);
            record1_1 = recordStore.saveRecord(record1.build());
            record2_1 = recordStore.saveRecord(record2.build());
            context.commit();
            record1_1 = clearSize(record1_1.withCommittedVersion(context.getVersionStamp()));
            record2_1 = clearSize(record2_1.withCommittedVersion(context.getVersionStamp()));
        }

        Tuple lastKey = null;

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, globalVersionIndex);
            List<RecordChangesIndexMaintainer.RecordChangesEntry<MySimpleRecord>> changes = getSimpleChanges(lastKey);
            Assertions.assertThat(changes).hasSize(2);
            Assertions.assertThat(changes.get(0).oldRecord()).isNull();
            Assertions.assertThat(changes.get(0).newRecord()).isEqualTo(record1_1);
            Assertions.assertThat(changes.get(1).oldRecord()).isNull();
            Assertions.assertThat(changes.get(1).newRecord()).isEqualTo(record2_1);
            lastKey = changes.get(1).key();
        }

        record1.setNumValue2(3).setStrValueIndexed("hello");

        FDBStoredRecord<Message> record1_2;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, globalVersionIndex);
            record1_2 = recordStore.saveRecord(record1.build());
            context.commit();
            record1_2 = clearSize(record1_2.withCommittedVersion(context.getVersionStamp()));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, globalVersionIndex);
            List<RecordChangesIndexMaintainer.RecordChangesEntry<MySimpleRecord>> changes = getSimpleChanges(lastKey);
            Assertions.assertThat(changes).hasSize(1);
            Assertions.assertThat(changes.get(0).oldRecord()).isEqualTo(record1_1);
            Assertions.assertThat(changes.get(0).newRecord()).isEqualTo(record1_2);
            lastKey = changes.get(0).key();
        }

        record2.clearNumValue2();

        FDBStoredRecord<Message> record2_2;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, globalVersionIndex);
            record2_2 = recordStore.saveRecord(record2.build());
            context.commit();
            record2_2 = clearSize(record2_2.withCommittedVersion(context.getVersionStamp()));
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, globalVersionIndex);
            List<RecordChangesIndexMaintainer.RecordChangesEntry<MySimpleRecord>> changes = getSimpleChanges(lastKey);
            Assertions.assertThat(changes).hasSize(1);
            Assertions.assertThat(changes.get(0).oldRecord()).isEqualTo(record2_1);
            Assertions.assertThat(changes.get(0).newRecord()).isEqualTo(record2_2);
            lastKey = changes.get(0).key();
        }

    }

    @Test
    void multipleChangesSameRecord() {
        MySimpleRecord.Builder record1 = MySimpleRecord.newBuilder().setRecNo(1066L).setNumValue2(1);

        FDBStoredRecord<Message> record1_1;
        FDBStoredRecord<Message> record1_2;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, globalVersionIndex);
            record1_1 = recordStore.saveRecord(record1.build());
            record1.setStrValueIndexed("hello");
            // This will make a change with an incomplete old record version.
            record1_2 = recordStore.saveRecord(record1.build());
            context.commit();
            record1_1 = clearSize(record1_1.withCommittedVersion(context.getVersionStamp()));
            record1_2 = clearSize(record1_2.withCommittedVersion(context.getVersionStamp()));
        }

        Tuple lastKey = null;

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, globalVersionIndex);
            List<RecordChangesIndexMaintainer.RecordChangesEntry<MySimpleRecord>> changes = getSimpleChanges(lastKey);
            Assertions.assertThat(changes).hasSize(2);
            Assertions.assertThat(changes.get(0).oldRecord()).isNull();
            Assertions.assertThat(changes.get(0).newRecord()).isEqualTo(record1_1);
            Assertions.assertThat(changes.get(1).oldRecord()).isEqualTo(record1_1);
            Assertions.assertThat(changes.get(1).newRecord()).isEqualTo(record1_2);
            lastKey = changes.get(1).key();
        }

    }

    private <M extends Message> FDBStoredRecord<M> clearSize(@Nonnull FDBStoredRecord<M> storedRecord) {
        return storedRecord.asBuilder().setSize(new SplitHelper.SizeInfo()).build();
    }

    private List<RecordChangesIndexMaintainer.RecordChangesEntry<MySimpleRecord>> getSimpleChanges(@Nullable Tuple lastKey) {
        final Index index = recordStore.getRecordMetaData().getIndex("globalChanges");
        RecordChangesIndexMaintainer indexMaintainer = (RecordChangesIndexMaintainer)recordStore.getIndexMaintainer(index);
        return indexMaintainer.scanChanges(TupleRange.ALL, lastKey, ScanProperties.FORWARD_SCAN)
                .map(entry -> narrowChange(entry, r -> MySimpleRecord.newBuilder().mergeFrom(r).build()))
                .asList().join();
    }

    private <M extends Message> RecordChangesIndexMaintainer.RecordChangesEntry<M> narrowChange(@Nonnull RecordChangesIndexMaintainer.RecordChangesEntry<Message> raw,
                                                                                                @Nonnull Function<Message, M> narrowFunction) {
        return new RecordChangesIndexMaintainer.RecordChangesEntry<>(raw.key(),
                narrowStoredRecord(raw.oldRecord(), narrowFunction),
                narrowStoredRecord(raw.newRecord(), narrowFunction));
    }

    @SuppressWarnings("unchecked")
    @Nullable
    private <M extends Message>  FDBStoredRecord<M> narrowStoredRecord(@Nullable FDBStoredRecord<Message> storedRecord,
                                                                       @Nonnull Function<Message, M> narrowFunction) {
        if (storedRecord == null) {
            return null;
        }
        FDBStoredRecordBuilder<Message> builder = storedRecord.asBuilder();
        FDBStoredRecordBuilder<M> narrowBuilder = (FDBStoredRecordBuilder<M>)builder;
        narrowBuilder.setRecord(narrowFunction.apply(builder.getRecord()));
        return narrowBuilder.build();
    }

}
