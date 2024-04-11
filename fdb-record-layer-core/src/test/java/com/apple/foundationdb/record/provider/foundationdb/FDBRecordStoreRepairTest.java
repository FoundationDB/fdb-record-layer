/*
 * FDBRecordStoreRepairTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.test.Tags;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for logic that repairs a record store.
 */
@Tag(Tags.RequiresFDB)
public class FDBRecordStoreRepairTest extends FDBRecordStoreTestBase {

    @Test
    public void cannotRepairInSnapshotIsolation() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestHelpers.assertThrows(RecordCoreArgumentException.class, () ->
                    recordStore.repairRecordKeys(null,
                            new ScanProperties(ExecuteProperties.newBuilder().setIsolationLevel(IsolationLevel.SNAPSHOT).build()))
            );
        }
    }

    @Test
    public void repairMissingUnsplitSuffixes() throws Exception {
        doRepairMissingUnsplitSuffixes(false);
    }

    @Test
    public void repairMissingUnsplitSuffixesDryRun() throws Exception {
        doRepairMissingUnsplitSuffixes(true);
    }

    public void doRepairMissingUnsplitSuffixes(boolean isDryRun) throws Exception {
        List<Message> records = createRecordsToCorrupt(10);

        // Validate that there are no problem entries
        doKeyRepair(0, 0, 0);

        AtomicInteger count = new AtomicInteger();
        mutateRecordKeys(recordKey -> {
            long suffix = recordKey.getLong(recordKey.size() - 1);
            assertEquals(SplitHelper.UNSPLIT_RECORD, suffix);

            if (count.incrementAndGet() % 2 == 0) {
                return TupleHelpers.subTuple(recordKey, 0, recordKey.size() - 1);
            }
            return recordKey;
        });

        // This will throw because of the missing split record suffix
        TestHelpers.assertThrows(RecordCoreException.class, () -> {
            validateRecords(records);
            return null;
        });

        doKeyRepair(5, 0, 0, isDryRun);

        if (isDryRun) {
            TestHelpers.assertThrows(RecordCoreException.class, () -> {
                validateRecords(records);
                return null;
            });
        } else {
            validateRecords(records);
        }
    }


    @Test
    public void identifyLongKeys() throws Exception {
        createRecordsToCorrupt(10);
        doKeyRepair(0, 0, 0);

        AtomicInteger count = new AtomicInteger();
        mutateRecordKeys(recordKey -> {
            long suffix = recordKey.getLong(recordKey.size() - 1);
            assertEquals(SplitHelper.UNSPLIT_RECORD, suffix);

            if (count.incrementAndGet() % 2 == 0) {
                // Make the record key longer than it should be by thrown on an extra unsplit record marker
                return recordKey.add(SplitHelper.UNSPLIT_RECORD);
            }
            return recordKey;
        });

        doKeyRepair(0, 5, 0);
    }

    @Test
    public void identifyShortKeys() throws Exception {
        RecordMetaDataHook hook = metaData -> {
            metaData.getRecordType("MySimpleRecord").setPrimaryKey(
                    Key.Expressions.concat(
                            Key.Expressions.field("rec_no"),
                            Key.Expressions.field("str_value_indexed"),
                            Key.Expressions.field("num_value_unique")
                    )
            );
        };

        createRecordsToCorrupt(10, hook);
        doKeyRepair(0, 0, 0, hook);

        // No need to corrupt anything, we can just use the original primary key
        doKeyRepair(0, 10, 0);
    }

    @Test
    public void invalidSplitSuffixType() throws Exception {
        createRecordsToCorrupt(10);
        doKeyRepair(0, 0, 0);

        AtomicInteger count = new AtomicInteger();
        mutateRecordKeys(recordKey -> {
            long suffix = recordKey.getLong(recordKey.size() - 1);
            assertEquals(SplitHelper.UNSPLIT_RECORD, suffix);

            if (count.incrementAndGet() % 2 == 0) {
                return TupleHelpers.subTuple(recordKey, 0, recordKey.size() - 1).add("foo");
            }
            return recordKey;
        });

        doKeyRepair(0, 0, 5);
    }

    @Test
    public void invalidSplitSuffixValue() throws Exception {
        createRecordsToCorrupt(10);
        doKeyRepair(0, 0, 0);

        AtomicInteger count = new AtomicInteger();
        mutateRecordKeys(recordKey -> {
            long suffix = recordKey.getLong(recordKey.size() - 1);
            assertEquals(SplitHelper.UNSPLIT_RECORD, suffix);

            if (count.incrementAndGet() % 2 == 0) {
                return TupleHelpers.subTuple(recordKey, 0, recordKey.size() - 1).add(99L);
            }
            return recordKey;
        });

        doKeyRepair(0, 0, 5);
    }

    private void mutateRecordKeys(Function<Tuple, Tuple> mutator) throws Exception {
        try (FDBRecordContext context = openContext()) {
            final Transaction tr = context.ensureActive();
            openUnsplitRecordStore(context);
            RecordCursor<KeyValue> cursor = RecordCursor.fromIterator(tr.getRange(recordStore.recordsSubspace().range()).iterator());
            cursor.forEach(keyValue -> {
                Tuple keyTuple = Tuple.fromBytes(keyValue.getKey());
                long suffix = keyTuple.getLong(keyTuple.size() - 1);

                // Skip record versions
                if (suffix != SplitHelper.RECORD_VERSION) {
                    Tuple mutatedKey = mutator.apply(keyTuple);
                    if (!mutatedKey.equals(keyTuple)) {
                        tr.clear(keyValue.getKey());
                        tr.set(mutatedKey.pack(), keyValue.getValue());
                    }
                }
            }).get();
            commit(context);
        }
    }

    private List<Message> createRecordsToCorrupt(int nRecords) throws Exception {
        return createRecordsToCorrupt(nRecords, null);
    }

    private List<Message> createRecordsToCorrupt(int nRecords, RecordMetaDataHook hook) throws Exception {
        List<Message> records = new ArrayList<>();
        try (FDBRecordContext context = openContext()) {
            openUnsplitRecordStore(context, hook);
            for (int i = 0; i < nRecords; i++) {
                TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(i)
                        .setStrValueIndexed("a_" + 1)
                        .setNumValueUnique(i)
                        .build();
                recordStore.saveRecord(rec);
                records.add(rec);
            }
            commit(context);
        }
        return records;
    }

    private void validateRecords(List<Message> expectedRecords) throws Exception {
        validateRecords(expectedRecords, null);
    }

    private void validateRecords(List<Message> expectedRecords, RecordMetaDataHook hook) throws Exception {
        final List<Message> readRecords;
        try (FDBRecordContext context = openContext()) {
            openUnsplitRecordStore(context, hook);
            readRecords = recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN)
                    .map(FDBStoredRecord::getRecord)
                    .asList().get();
        }
        assertEquals(expectedRecords.size(), readRecords.size());
        expectedRecords.stream().forEach(message -> assertTrue(readRecords.contains(message), () -> "Failed to read " + message));
    }


    private void doKeyRepair(int repairedKeys, int invalidKeyLengths, int invalidSplitSuffixes) throws Exception {
        doKeyRepair(repairedKeys, invalidKeyLengths, invalidSplitSuffixes, null, false);
    }

    private void doKeyRepair(int repairedKeys, int invalidKeyLengths, int invalidSplitSuffixes, boolean isDryRun) throws Exception {
        doKeyRepair(repairedKeys, invalidKeyLengths, invalidSplitSuffixes, null, isDryRun);
    }

    private void doKeyRepair(int repairedKeys, int invalidKeyLengths, int invalidSplitSuffixes,
                             RecordMetaDataHook hook) throws Exception {
        doKeyRepair(repairedKeys, invalidKeyLengths, invalidSplitSuffixes, hook, false);
    }

    private void doKeyRepair(int repairedKeys, int invalidKeyLengths, int invalidSplitSuffixes,
                             RecordMetaDataHook hook,
                             boolean isDryRun) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openUnsplitRecordStore(context, hook);
            assertNull(recordStore.repairRecordKeys(null, ScanProperties.FORWARD_SCAN, isDryRun).get(),
                    "Did not expect a continuation!");
            context.commit();

            final FDBStoreTimer timer = Objects.requireNonNull(context.getTimer());
            assertEquals(repairedKeys, timer.getCount(FDBStoreTimer.Counts.REPAIR_RECORD_KEY));
            assertEquals(invalidKeyLengths, timer.getCount(FDBStoreTimer.Counts.INVALID_KEY_LENGTH));
            assertEquals(invalidSplitSuffixes, timer.getCount(FDBStoreTimer.Counts.INVALID_SPLIT_SUFFIX));
            timer.reset();
        }
    }

    public void openUnsplitRecordStore(FDBRecordContext context) throws Exception {
        openUnsplitRecordStore(context, null);
    }

    public void openUnsplitRecordStore(FDBRecordContext context, RecordMetaDataHook hook) throws Exception {
        openSimpleRecordStore(context, metaData -> {
            metaData.setSplitLongRecords(false);
            if (hook != null) {
                hook.apply(metaData);
            }
        });
    }
}
