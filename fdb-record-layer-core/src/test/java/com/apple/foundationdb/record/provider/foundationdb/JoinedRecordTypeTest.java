/*
 * JoinedRecordTypeTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsJoinIndexProto;
import com.apple.foundationdb.record.metadata.JoinedRecordType;
import com.apple.foundationdb.record.metadata.JoinedRecordTypeBuilder;
import com.apple.foundationdb.record.provider.foundationdb.query.FDBRecordStoreQueryTestBase;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests of the {@link JoinedRecordType} class. Some of these tests may require access to an underlying FDB record
 * store.
 */
@Tag(Tags.RequiresFDB)
public class JoinedRecordTypeTest extends FDBRecordStoreQueryTestBase {

    public static final String JOINED_RECORD_NAME = "JoinedRecord";
    public static final String SIMPLE_RECORD = "simple_record";
    public static final String OTHER_RECORD = "other_record";

    @Nonnull
    private static RecordMetaData baseMetaData(@Nonnull RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsJoinIndexProto.getDescriptor());
        hook.apply(metaDataBuilder);
        return metaDataBuilder.build();
    }

    @Nonnull
    private static RecordMetaDataHook addJoinedType() {
        return metaDataBuilder -> {
            JoinedRecordTypeBuilder typeBuilder = metaDataBuilder.addJoinedRecordType(JOINED_RECORD_NAME);
            typeBuilder.addConstituent(SIMPLE_RECORD, "MySimpleRecord");
            typeBuilder.addConstituent(OTHER_RECORD, "MyOtherRecord");
            typeBuilder.addJoin(SIMPLE_RECORD, field("other_rec_no"), OTHER_RECORD, field("rec_no"));
        };
    }

    @ParameterizedTest
    @EnumSource(IndexOrphanBehavior.class)
    void loadSyntheticRecord(IndexOrphanBehavior orphanBehavior) throws ExecutionException, InterruptedException {
        final Tuple joinedPrimaryKey;
        final FDBStoredRecord<Message> simpleRecord;
        final FDBStoredRecord<Message> otherRecord;
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, baseMetaData(addJoinedType()));

            Tuple syntheticRecordTypeKey = recordStore.getRecordMetaData()
                    .getSyntheticRecordType(JOINED_RECORD_NAME)
                    .getRecordTypeKeyTuple();
            simpleRecord = recordStore.saveRecord(createSimpleRecord(100, 10));
            otherRecord = recordStore.saveRecord(createOtherRecord(101, 11));
            joinedPrimaryKey = Tuple.from(
                    syntheticRecordTypeKey.getItems().get(0),
                    simpleRecord.getPrimaryKey().getItems(),
                    otherRecord.getPrimaryKey().getItems());

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, baseMetaData(addJoinedType()));

            final FDBSyntheticRecord rec = recordStore.loadSyntheticRecord(joinedPrimaryKey, orphanBehavior).get();
            assertEquals(2, rec.getConstituents().size());

            assertEquals(simpleRecord, rec.getConstituent(SIMPLE_RECORD));
            assertEquals(otherRecord, rec.getConstituent(OTHER_RECORD));

            context.commit();
        }
    }

    static Stream<Arguments> twoBooleans() {
        return Stream.of(Arguments.of(true, false), Arguments.of(false, true), Arguments.of(true, true));
    }

    @ParameterizedTest
    @MethodSource("twoBooleans")
    void loadSyntheticRecordFailsMissingConstituent(boolean missingSimple, boolean missingOther) throws ExecutionException, InterruptedException {
        final Tuple joinedPrimaryKey;
        final FDBStoredRecord<Message> simpleRecord;
        final FDBStoredRecord<Message> otherRecord;
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, baseMetaData(addJoinedType()));

            Tuple syntheticRecordTypeKey = recordStore.getRecordMetaData()
                    .getSyntheticRecordType(JOINED_RECORD_NAME)
                    .getRecordTypeKeyTuple();
            simpleRecord = recordStore.saveRecord(createSimpleRecord(100, 10));
            otherRecord = recordStore.saveRecord(createOtherRecord(101, 11));
            joinedPrimaryKey = Tuple.from(
                    syntheticRecordTypeKey.getItems().get(0),
                    simpleRecord.getPrimaryKey().getItems(),
                    otherRecord.getPrimaryKey().getItems());

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, baseMetaData(addJoinedType()));
            if (missingOther) {
                recordStore.deleteRecord(otherRecord.getPrimaryKey());
            }
            if (missingSimple) {
                recordStore.deleteRecord(simpleRecord.getPrimaryKey());
            }

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, baseMetaData(addJoinedType()));
            // Default policy (ERROR) should fail
            final ExecutionException exception = assertThrows(ExecutionException.class, () -> recordStore.loadSyntheticRecord(joinedPrimaryKey, IndexOrphanBehavior.ERROR).get());
            assertEquals(RecordDoesNotExistException.class, exception.getCause().getClass());
            // RETURN policy returns the shell of the synthetic record with no constituents
            final FDBSyntheticRecord syntheticRecord = recordStore.loadSyntheticRecord(joinedPrimaryKey, IndexOrphanBehavior.RETURN).get();
            assertEquals(0, syntheticRecord.getConstituents().size());
            // SKIP policy returns null in case of missing constituents
            assertNull(recordStore.loadSyntheticRecord(joinedPrimaryKey, IndexOrphanBehavior.SKIP).get());

            context.commit();
        }
    }

    private Message createSimpleRecord(final int recNo, final int numValue) {
        return TestRecordsJoinIndexProto.MySimpleRecord.newBuilder()
                .setRecNo(recNo)
                .setNumValue(numValue)
                .build();
    }

    private Message createOtherRecord(final int recNo, final int numValue) {
        return TestRecordsJoinIndexProto.MyOtherRecord.newBuilder()
                .setRecNo(recNo)
                .setNumValue(numValue)
                .build();
    }
}
