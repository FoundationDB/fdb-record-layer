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
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexComparison;
import com.apple.foundationdb.record.metadata.IndexPredicate;
import com.apple.foundationdb.tuple.TupleHelpers;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nonnull;
import java.util.concurrent.ExecutionException;

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
    private static final String FILTERED_JOINED_INDEX = "filteredJoinedNumValue";
    /** Only joined records whose {@code other_record.num_value} exceeds this are indexed. */
    private static final int FILTERED_NUM_VALUE_THRESHOLD = 5;

    @Nonnull
    private static RecordMetaData baseMetaData(@Nonnull RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsJoinIndexProto.getDescriptor());
        hook.apply(metaDataBuilder);
        return metaDataBuilder.build();
    }

    /**
     * The joined type with a filtered (sparse) index on it, keyed on one constituent and filtered on the other. The
     * predicate can only be evaluated against the joined record, whose type is not among the stored record types.
     */
    @Nonnull
    private static RecordMetaDataHook addFilteredJoinedIndex() {
        return addJoinedType().andThen(metaDataBuilder -> {
            final IndexPredicate predicate = new IndexPredicate.ValuePredicate(
                    List.of(OTHER_RECORD, "num_value"),
                    new IndexComparison.SimpleComparison(
                            IndexComparison.SimpleComparison.ComparisonType.GREATER_THAN,
                            FILTERED_NUM_VALUE_THRESHOLD));
            metaDataBuilder.addIndex(JOINED_RECORD_NAME,
                    new Index(new Index(FILTERED_JOINED_INDEX, field(SIMPLE_RECORD).nest("num_value")), predicate));
        });
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

    @ParameterizedTest
    @CsvSource({"true,true", "true,false", "false,true"})
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
            ExecutionException exception = assertThrows(ExecutionException.class, () -> recordStore.loadSyntheticRecord(joinedPrimaryKey).get());
            assertEquals(RecordDoesNotExistException.class, exception.getCause().getClass());
            exception = assertThrows(ExecutionException.class, () -> recordStore.loadSyntheticRecord(joinedPrimaryKey, IndexOrphanBehavior.ERROR).get());
            assertEquals(RecordDoesNotExistException.class, exception.getCause().getClass());
            // RETURN policy returns the shell of the synthetic record with no constituents
            final FDBSyntheticRecord syntheticRecord = recordStore.loadSyntheticRecord(joinedPrimaryKey, IndexOrphanBehavior.RETURN).get();
            assertEquals(0, syntheticRecord.getConstituents().size());
            // SKIP policy returns null in case of missing constituents
            assertNull(recordStore.loadSyntheticRecord(joinedPrimaryKey, IndexOrphanBehavior.SKIP).get());

            context.commit();
        }
    }

    /**
     * A filtered (sparse) index over a joined type indexes only the joined records its predicate accepts, and the
     * predicate is evaluated against the joined record rather than either stored one.
     */
    @Test
    void filteredIndexOnJoinedType() {
        final RecordMetaData metaData = baseMetaData(addFilteredJoinedIndex());
        final Index index = metaData.getIndex(FILTERED_JOINED_INDEX);
        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, metaData);
            final Tuple syntheticTypeKey = recordStore.getRecordMetaData()
                    .getSyntheticRecordType(JOINED_RECORD_NAME)
                    .getRecordTypeKeyTuple();

            // accepted by the predicate: other_record.num_value is above the threshold
            final FDBStoredRecord<Message> keptSimple = recordStore.saveRecord(joinableSimpleRecord(100, 10, 101));
            final FDBStoredRecord<Message> keptOther = recordStore.saveRecord(createOtherRecord(101, 11));
            // filtered out: other_record.num_value is below the threshold
            recordStore.saveRecord(joinableSimpleRecord(200, 20, 201));
            recordStore.saveRecord(createOtherRecord(201, 1));

            final Tuple keptSyntheticKey = Tuple.from(syntheticTypeKey.getItems().get(0),
                    keptSimple.getPrimaryKey().getItems(), keptOther.getPrimaryKey().getItems());
            final List<IndexEntry> expected = List.of(new IndexEntry(index,
                    Tuple.from(10L).addAll(keptSyntheticKey), TupleHelpers.EMPTY, keptSyntheticKey));

            final List<IndexEntry> scanned = recordStore
                    .scanIndex(index, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                    .asList()
                    .join();
            assertEquals(expected, scanned);

            context.commit();
        }
    }

    /** A simple record whose {@code other_rec_no} points at the other record, so the join matches. */
    private Message joinableSimpleRecord(final int recNo, final int numValue, final int otherRecNo) {
        return TestRecordsJoinIndexProto.MySimpleRecord.newBuilder()
                .setRecNo(recNo)
                .setNumValue(numValue)
                .setOtherRecNo(otherRecNo)
                .build();
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
