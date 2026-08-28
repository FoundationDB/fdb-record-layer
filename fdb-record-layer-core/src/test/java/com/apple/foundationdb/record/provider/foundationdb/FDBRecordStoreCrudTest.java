/*
 * FDBRecordStoreCRUDTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecordsBytesProto;
import com.apple.foundationdb.record.TestRecordsUuidProto;
import com.apple.foundationdb.record.TestRecordsWithUnionProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.expressions.TupleFieldsHelper;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.RandomSeedSource;
import com.apple.test.Tags;
import com.google.common.base.Strings;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Basic CRUD operation tests on {@link FDBRecordStore}.
 */
@Tag(Tags.RequiresFDB)
@Execution(ExecutionMode.CONCURRENT)
class FDBRecordStoreCrudTest extends FDBRecordStoreTestBase {
    @Nonnull
    private final String longString = Strings.repeat("x", 101_000);

    /**
     * Helper method to run index scrubbing validation on all indexes. This method
     * assumes that the store has already been opened (see, e.g., {@link #openSimpleRecordStore(FDBRecordContext)}).
     * It will ignore any scrubbing failures that happen because the index does not
     * support scrubbing, but will assert that all other scrubbing jobs find no
     * inconsistencies.
     */
    private void scrubAllIndexes() {
        for (Index index : recordStore.getRecordMetaData().getAllIndexes()) {
            try (OnlineIndexScrubber scrubber =  OnlineIndexScrubber.newBuilder()
                    .setRecordStore(recordStore)
                    .setIndex(index)
                    .setScrubbingPolicy(OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                            .setAllowRepair(false)
                    )
                    .build()) {
                assertEquals(0L, scrubber.scrubDanglingIndexEntries());
                assertEquals(0L, scrubber.scrubMissingIndexEntries());
            } catch (UnsupportedOperationException e) {
                // Not all indexes support scrubbing. Ignore the ones where this fails
                if (e.getMessage().contains("This index does not support scrubbing")) {
                    continue;
                }
                throw e;
            }
        }
    }

    @Test
    void writeRead() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1L)
                    .setStrValueIndexed("abc")
                    .setNumValueUnique(123)
                    .build();
            recordStore.saveRecord(rec);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            FDBStoredRecord<Message> rec1 = recordStore.loadRecord(Tuple.from(1L));
            assertNotNull(rec1);
            TestRecords1Proto.MySimpleRecord.Builder myrec1 = TestRecords1Proto.MySimpleRecord.newBuilder();
            myrec1.mergeFrom(rec1.getRecord());
            assertEquals(123, myrec1.getNumValueUnique());
            commit(context);
        }
    }

    @Test
    void writeCheckExists() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1L)
                    .setStrValueIndexed("abc")
                    .setNumValueUnique(123)
                    .build();
            recordStore.saveRecord(rec);
            assertThat(recordStore.recordExists(Tuple.from(1L)), is(true));
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertThat(recordStore.recordExists(Tuple.from(1L)), is(true));
            assertThat(recordStore.recordExists(Tuple.from(2L)), is(false));
            commit(context);
        }
    }

    @Test
    void writeCheckExistsConcurrently() throws Exception {
        try (FDBRecordContext context1 = openContext(); FDBRecordContext context2 = openContext()) {
            openSimpleRecordStore(context1);

            TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .build();
            recordStore.saveRecord(rec);

            openSimpleRecordStore(context2);
            assertThat(recordStore.recordExists(Tuple.from(1066L)), is(false));
            TestRecords1Proto.MySimpleRecord rec2 = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1415L)
                    .build();
            recordStore.saveRecord(rec2);

            commit(context1);
            assertThrows(FDBExceptions.FDBStoreTransactionConflictException.class, context2::commit);
        }
        try (FDBRecordContext context1 = openContext(); FDBRecordContext context2 = openContext()) {
            openSimpleRecordStore(context1);
            recordStore.deleteRecord(Tuple.from(1066L));

            openSimpleRecordStore(context2);
            assertThat(recordStore.recordExists(Tuple.from(1066L), IsolationLevel.SNAPSHOT), is(true));
            TestRecords1Proto.MySimpleRecord rec2 = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1415L)
                    .build();
            recordStore.saveRecord(rec2);

            commit(context1);
            commit(context2);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            assertThat(recordStore.recordExists(Tuple.from(1066L)), is(false));
            assertThat(recordStore.recordExists(Tuple.from(1415L)), is(true));
            scrubAllIndexes();
            commit(context);
        }
    }

    @Test
    void writeByteString() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openBytesRecordStore(context);

            recordStore.saveRecord(TestRecordsBytesProto.ByteStringRecord.newBuilder()
                    .setPkey(byteString(0, 1, 2)).setSecondary(byteString(0, 1, 2)).setName("foo").build());
            recordStore.saveRecord(TestRecordsBytesProto.ByteStringRecord.newBuilder()
                    .setPkey(byteString(0, 1, 3)).setSecondary(byteString(0, 1, 3)).setName("foo").build());
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openBytesRecordStore(context);
            FDBStoredRecord<Message> rec1 = recordStore.loadRecord(Tuple.from(byteString(0, 1, 2).toByteArray()));
            assertNotNull(rec1);
            TestRecordsBytesProto.ByteStringRecord.Builder myrec1 = TestRecordsBytesProto.ByteStringRecord.newBuilder();
            myrec1.mergeFrom(rec1.getRecord());
            assertEquals(byteString(0, 1, 2), myrec1.getPkey());
            assertEquals("foo", myrec1.getName());
            scrubAllIndexes();
            commit(context);
        }
    }

    @Test
    void writeUuid() {
        UUID uuid1 = UUID.fromString("710730ce-d9fd-417a-bb6e-27bcfefe3d4d");
        UUID uuid2 = UUID.fromString("03b9221a-e61b-4bee-8c47-34e1248ed273");

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, RecordMetaData.build(TestRecordsUuidProto.getDescriptor()));
            recordStore.saveRecord(TestRecordsUuidProto.UuidRecord.newBuilder()
                    .setSecondary(TupleFieldsHelper.toProto(UUID.randomUUID())).setPkey(TupleFieldsHelper.toProto(uuid1)).setName("foo").build());
            recordStore.saveRecord(TestRecordsUuidProto.UuidRecord.newBuilder()
                    .setSecondary(TupleFieldsHelper.toProto(UUID.randomUUID())).setPkey(TupleFieldsHelper.toProto(uuid2)).setName("foo").build());
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            createOrOpenRecordStore(context, RecordMetaData.build(TestRecordsUuidProto.getDescriptor()));
            FDBStoredRecord<Message> rec1 = recordStore.loadRecord(Tuple.from(uuid1));
            assertNotNull(rec1);
            TestRecordsUuidProto.UuidRecord.Builder myrec1 = TestRecordsUuidProto.UuidRecord.newBuilder();
            myrec1.mergeFrom(rec1.getRecord());
            assertEquals(uuid1, TupleFieldsHelper.fromProto(myrec1.getPkey()));
            assertEquals("foo", myrec1.getName());
            commit(context);
        }
    }

    @Test
    void writeNotUnionType() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openUnionRecordStore(context);

            assertThrows(MetaDataException.class, () -> {
                TestRecordsWithUnionProto.NotInUnion.Builder recBuilder = TestRecordsWithUnionProto.NotInUnion.newBuilder();
                recBuilder.setNumValueUnique(3);
                recBuilder.setStrValueIndexed("boxes");
                recordStore.saveRecord(recBuilder.build());
                commit(context);
            });
        }
    }

    @ParameterizedTest(name = "saveRecordsConcurrently[{0}]")
    @BooleanSource
    void saveRecordsConcurrently(boolean disableConcurrencyManagement) throws Exception {
        final List<FDBStoredRecord<Message>> saved;
        final FDBRecordStore.Builder storeBuilder;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            storeBuilder = recordStore.asBuilder().setDisableConcurrencyManagement(disableConcurrencyManagement);
            recordStore = storeBuilder.open();

            // Create 100 futures, each one saving a different record, and then run them concurrently.
            // As they are each touching a different record, the operations should succeed regardless
            // of whether the concurrency manager is disabled.
            final List<CompletableFuture<FDBStoredRecord<Message>>> futures = IntStream.range(0, 100)
                    .mapToObj(id -> TestRecords1Proto.MySimpleRecord.newBuilder()
                            .setRecNo(id + 1000L)
                            .setNumValue3Indexed(id % 3)
                            .setNumValue2(id % 4)
                            .setStrValueIndexed((id % 2L == 0L) ? "even" : "odd")
                            .setNumValueUnique(id + 100)
                            .build()
                    )
                    .map(recordStore::saveRecordAsync)
                    .toList();
            saved = AsyncUtil.getAll(futures).get();

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            recordStore = storeBuilder.setContext(context).open();
            final List<FDBStoredRecord<Message>> loaded = AsyncUtil.getAll(saved.stream()
                    .map(FDBStoredRecord::getPrimaryKey)
                    .map(recordStore::loadRecordAsync)
                    .toList()
            ).get();
            assertEquals(saved.size(), loaded.size(), "saved and loaded lists should have the same size");
            for (int i = 0; i < saved.size(); i++) {
                FDBStoredRecord<Message> savedRecord = saved.get(i);
                FDBStoredRecord<Message> loadedRecord = loaded.get(i);
                assertEquals(savedRecord.getRecord(), loadedRecord.getRecord());
            }
            assertEquals(saved.size(), recordStore.getSnapshotRecordCount().get());
            assertEquals(saved.size(), recordStore.getSnapshotRecordUpdateCount().get());

            scrubAllIndexes();
        }
    }

    @Test
    void saveSameRecordConcurrently() throws Exception {
        final FDBRecordStore.Builder storeBuilder;
        final List<FDBStoredRecord<Message>> saved;
        byte[] commitVersionstamp;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, metaDataBuilder -> metaDataBuilder.setStoreRecordVersions(true));
            storeBuilder = recordStore.asBuilder().setDisableConcurrencyManagement(false);
            recordStore = storeBuilder.open();

            // Create 100 futures, each one saving the same record (i.e., the same primary key), but with
            // different values. Only one of these will succeed at the end, so 
            final List<CompletableFuture<FDBStoredRecord<Message>>> futures = IntStream.range(0, 100)
                    .mapToObj(id -> TestRecords1Proto.MySimpleRecord.newBuilder()
                            .setRecNo(1000L)
                            .setNumValue3Indexed(id % 3)
                            .setNumValue2(id)
                            .setStrValueIndexed((id % 2L == 0L) ? "even" : "odd")
                            .setNumValueUnique(id)
                            .build()
                    )
                    .map(recordStore::saveRecordAsync)
                    .toList();
            saved = AsyncUtil.getAll(futures).get();

            commit(context);
            commitVersionstamp = Objects.requireNonNull(context.getVersionStamp());
        }

        try (FDBRecordContext context = openContext()) {
            recordStore = storeBuilder.setContext(context).open();

            final List<FDBStoredRecord<Message>> loaded = AsyncUtil.getAll(saved.stream()
                    .map(FDBStoredRecord::getPrimaryKey)
                    .map(recordStore::loadRecordAsync)
                    .toList()
            ).get();
            assertEquals(saved.size(), loaded.size(), "saved and loaded lists should have the same size");
            boolean found = false;
            for (int i = 0; i < saved.size(); i++) {
                FDBStoredRecord<Message> savedRecord = saved.get(i);
                FDBStoredRecord<Message> loadedRecord = loaded.get(i);
                if (savedRecord.getRecord().equals(loadedRecord.getRecord())) {
                    found = true;
                    assertNotNull(loadedRecord.getVersion());
                    assertNotNull(savedRecord.getVersion());
                    assertEquals(savedRecord.getVersion().withCommittedVersion(commitVersionstamp), loadedRecord.getVersion());
                }
            }
            assertTrue(found, "no record found that matched original set");
            assertEquals(1L, recordStore.getSnapshotRecordCount().get());
            assertEquals(saved.size(), recordStore.getSnapshotRecordUpdateCount().get());

            scrubAllIndexes();
        }
    }

    @Test
    void onlyOneConcurrentInsertSucceeds() throws Exception {
        final FDBRecordStore.Builder storeBuilder;
        final List<FDBStoredRecord<Message>> inserted;
        byte[] commitVersionstamp;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, metaDataBuilder -> metaDataBuilder.setStoreRecordVersions(true));
            storeBuilder = recordStore.asBuilder().setDisableConcurrencyManagement(false);
            recordStore = storeBuilder.open();

            final List<CompletableFuture<FDBStoredRecord<Message>>> futures = IntStream.range(0, 100)
                    .mapToObj(id -> TestRecords1Proto.MySimpleRecord.newBuilder()
                            .setRecNo(1000L + (id % 10))
                            .setNumValue3Indexed(id % 3)
                            .setNumValue2(id)
                            .setStrValueIndexed((id % 2L == 0L) ? "even" : "odd")
                            .build())
                    .map(rec -> recordStore.insertRecordAsync(rec).handle((saved, err) -> {
                        if (err != null) {
                            if (err instanceof CompletionException) {
                                err = err.getCause();
                            }
                            assertInstanceOf(RecordAlreadyExistsException.class, err);
                            return null;
                        }
                        return saved;
                    }))
                    .toList();
            final List<FDBStoredRecord<Message>> savedRecords = AsyncUtil.getAll(futures).get();
            inserted = savedRecords.stream().filter(Objects::nonNull).toList();
            assertEquals(10, inserted.size());
            final Set<Tuple> savedPrimaryKeys = inserted.stream().map(FDBStoredRecord::getPrimaryKey).collect(Collectors.toSet());
            assertEquals(10, savedPrimaryKeys.size());

            assertEquals(10, recordStore.getSnapshotRecordCount().get());
            assertEquals(10, recordStore.getSnapshotRecordUpdateCount().get());

            commit(context);
            commitVersionstamp = Objects.requireNonNull(context.getVersionStamp());
        }

        try (FDBRecordContext context = openContext()) {
            recordStore = storeBuilder.setContext(context).open();

            inserted.forEach(insertedRecord -> {
                final FDBStoredRecord<Message> stored = recordStore.loadRecord(insertedRecord.getPrimaryKey());
                assertNotNull(stored);
                assertEquals(insertedRecord.getRecord(), stored.getRecord());
                assertEquals(Objects.requireNonNull(insertedRecord.getVersion()).withCommittedVersion(commitVersionstamp), stored.getVersion());
            });

            scrubAllIndexes();
        }
    }

    @Test
    void deleteSameRecordConcurrently() throws Exception {
        // Save a single record
        final FDBRecordStore.Builder storeBuilder;
        final FDBStoredRecord<Message> saved;
        byte[] commitVersionstamp;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, metaDataBuilder -> metaDataBuilder.setStoreRecordVersions(true));
            storeBuilder = recordStore.asBuilder().setDisableConcurrencyManagement(false);
            recordStore = storeBuilder.open();

            saved = recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1805L)
                    .setNumValue2(3)
                    .setStrValueIndexed("blah")
                    .setNumValue3Indexed(4)
                    .build());
            commit(context);
            commitVersionstamp = Objects.requireNonNull(context.getVersionStamp());
        }

        // Attempt to delete that record from multiple places, interspersed with concurrent reads.
        // Exactly one delete should succeed, and all the reads should occur either strictly before
        // or strictly after the delete
        try (FDBRecordContext context = openContext()) {
            recordStore = storeBuilder.setContext(context).open();

            // Fire off some reads before the first delete call
            final List<CompletableFuture<FDBStoredRecord<Message>>> readFutures = new ArrayList<>();
            Stream.generate(() -> recordStore.loadRecordAsync(saved.getPrimaryKey()))
                    .limit(30)
                    .forEach(readFutures::add);

            // Now, issue multiple deletes
            final List<CompletableFuture<Boolean>> deleteFutures = Stream.generate(() -> recordStore.deleteRecordAsync(saved.getPrimaryKey()))
                    .limit(30)
                    .toList();

            // Add additional reads after the first delete
            Stream.generate(() -> recordStore.loadRecordAsync(saved.getPrimaryKey()))
                    .limit(30)
                    .forEach(readFutures::add);

            final List<FDBStoredRecord<Message>> readResults = AsyncUtil.getAll(readFutures).get();
            final List<Boolean> deleteResults = AsyncUtil.getAll(deleteFutures).get();

            // Exactly one of the deletes should return true
            assertEquals(1, deleteResults.stream()
                    .filter(deleted -> deleted)
                    .count());

            // All the read results should either be null (if they happened after the delete) or they should match the original record
            readResults.stream()
                    .filter(Objects::nonNull)
                    .forEach(readRecord -> {
                        assertEquals(saved.getRecord(), readRecord.getRecord());
                        assertEquals(Objects.requireNonNull(saved.getVersion()).withCommittedVersion(commitVersionstamp), readRecord.getVersion());
                    });

            assertEquals(0L, recordStore.getSnapshotRecordCount().get());

            scrubAllIndexes();
            commit(context);
        }
    }

    private static class TaskState {
        private int taskNumber;
        private int updates;
        private int completed;
        @Nonnull
        private final Map<Tuple, Deque<Pair<Integer, Message>>> historyByRecord = new ConcurrentHashMap<>();
    }

    /**
     * Create a random sequence of single-record operations and run them with concurrency. Each
     * operation can do some kind of single-record operation (e.g., read or update one record).
     * Those are spread across a small number of record primary keys. With each operation, we
     * check to make sure (1) there are no exceptions hit executing the operation and (2) we
     * the value is consistent with the operation history. Because of the concurrency, we
     * are not able to fix the reads to a single possible value, but we should be able to assert
     * that the value is one of a consistent set of values.
     *
     * @param seed a seed to use in the random number generator used to create test cases
     * @throws Exception any problem hit while running the test
     */
    @ParameterizedTest
    @RandomSeedSource
    void concurrentRecordOperationStressTest(long seed) throws Exception {
        final int concurrentTasks = 100;
        final int totalTasks = 1000;
        final TaskState taskState = new TaskState();

        final FDBRecordStore.Builder storeBuilder;

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.setStoreRecordVersions(true);
                metaDataBuilder.setSplitLongRecords(true);
                metaDataBuilder.removeIndex("MySimpleRecord$str_value_indexed");
            });
            // These tests fail if concurrence management is disabled. For this test, we also
            // assert on the default behavior (that the store starts with concurrency management
            // enabled). If this is changed, then this assert can be updated, but we still need
            // to override the feature for this test
            storeBuilder = recordStore.asBuilder();
            assertFalse(storeBuilder.isConcurrencyManagementDisabled());
            storeBuilder.setDisableConcurrencyManagement(false);
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            recordStore = storeBuilder.setContext(context).open();

            // These tests will fail if we don't have the concurrency manager enabled.
            // In theory, this could be an assumption, but making it an assert means that
            // we are notified if we somehow lose coverage, and then we can decide if that's
            // desirable or not
            assertFalse(recordStore.asBuilder().isConcurrencyManagementDisabled());

            final Random random = new Random(seed);
            final Queue<CompletableFuture<Void>> tasks = new ArrayDeque<>();
            while (taskState.completed < totalTasks) {
                while (tasks.size() < concurrentTasks && taskState.taskNumber < totalTasks) {
                    taskState.taskNumber++;
                    tasks.add(createRandomRecordOperation(random, taskState));
                }
                // Wait for the head of the queue to complete, then remove the leading head of tasks that have already completed
                tasks.peek().get();
                while (!tasks.isEmpty() && tasks.peek().isDone()) {
                    tasks.remove().get();
                    taskState.completed++;
                }
            }
            assertTrue(tasks.isEmpty());
            validateRecordsAfterRun(taskState);
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            recordStore = storeBuilder.setContext(context).open();
            validateRecordsAfterRun(taskState);
            scrubAllIndexes();
        }
    }

    private void validateRecordsAfterRun(@Nonnull TaskState taskState) throws Exception {
        // The updates index should contain one udpate for every update started during the test
        assertEquals(taskState.updates, recordStore.getSnapshotRecordUpdateCount().get());

        // Make sure the most recent update is persisted for each record
        int expectedCount = 0;
        for (Map.Entry<Tuple, Deque<Pair<Integer, Message>>> entry :  taskState.historyByRecord.entrySet()) {
            final Tuple primaryKey = entry.getKey();
            final Pair<Integer, Message> mostRecentUpdate = entry.getValue().peekFirst();
            @Nullable final Message expectedMessage = mostRecentUpdate == null ? null : mostRecentUpdate.getRight();
            FDBStoredRecord<Message> loaded = recordStore.loadRecord(primaryKey);
            @Nullable final Message readMessage = loaded == null ? null : loaded.getRecord();
            assertEquals(expectedMessage, readMessage);
            if (expectedMessage != null) {
                expectedCount++;
            }
        }
        assertEquals(expectedCount, recordStore.getSnapshotRecordCount().get());
    }

    @Nonnull
    private CompletableFuture<Void> createRandomRecordOperation(@Nonnull Random random, @Nonnull TaskState taskState) {
        final int taskNumber = taskState.taskNumber;
        final int completed = taskState.completed;
        double choice = random.nextDouble();
        final long recNo = random.nextLong(20);
        final Tuple primaryKey = Tuple.from(recNo);
        Deque<Pair<Integer, Message>> recordHistory = taskState.historyByRecord.computeIfAbsent(primaryKey, ignored -> new ConcurrentLinkedDeque<>());
        if (choice < 0.3) {
            // Read the record at this primary key.
            return recordStore.loadRecordAsync(primaryKey).thenApply(stored -> {
                Set<Message> possibleValues = possibleValuesForRecord(recordHistory, completed);
                Message storedRec = stored == null ? null : stored.getRecord();
                assertThat(storedRec, in(possibleValues));
                if (stored != null) {
                    assertEquals(primaryKey, stored.getPrimaryKey());
                }
                return null;
            });
        } else if (choice < 0.5) {
            // Check if the record at this primary key exists
            return recordStore.recordExistsAsync(primaryKey).thenApply(exists -> {
                Set<Message> possibleValues = possibleValuesForRecord(recordHistory, completed);
                if (exists) {
                    assertTrue(anyNonNull(possibleValues), "record exists, so there should be at least one non-null possible value");
                } else {
                    assertTrue(canBeNull(possibleValues), "record does not exist, so null must be a possible value");
                }
                return null;
            });
        } else if (choice < 0.6) {
            // Preload the record from the database. This will populate the preload cache with a value
            // read from the database. Later reads from the database will use this preloaded value if
            // it is set, so this is important for making sure that we later clean up that state. That is,
            // the presence of "preload" events in the history should not change the behavior of the other
            // operations
            return recordStore.preloadRecordAsync(primaryKey);
        } else if (choice < 0.9) {
            // Insert a new value for a record
            TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(recNo)
                    .setNumValueUnique(taskNumber)
                    .setStrValueIndexed(random.nextDouble() < 0.05 ? longString : "blah")
                    .setNumValue3Indexed(random.nextInt(3))
                    .build();
            recordHistory.addFirst(Pair.of(taskNumber, rec));
            taskState.updates++;
            return recordStore.saveRecordAsync(rec).thenApply(ignored -> null);
        } else {
            // Delete the record.
            recordHistory.addFirst(Pair.of(taskNumber, null));
            return recordStore.deleteRecordAsync(primaryKey).thenApply(deleted -> {
                final Set<Message> possibleValues = possibleValuesForRecord(recordHistory, completed);
                if (deleted) {
                    assertTrue(anyNonNull(possibleValues), "record previously existed, so some non-null value must be possible");
                }
                return null;
            });
        }
    }

    /**
     * Compute the set of possible values for a record given its history. The history array should be sorted
     * in reverse chronological order with the latest writes to the history coming at the front. When a
     * task is started, some tail of the history is already completed. Any read must therefore be some
     * value that was either (1) begun after all the completed tasks or (2) was the final write completed.
     * Note that we're relying here on the way that write locks are managed in the {@link com.apple.foundationdb.record.locking.LockRegistry}.
     * That is to say, that we always enqueue later writes on top of older writes, so they end up executing
     * in the order in which they are created.
     *
     * @param history the history of values for the record in descending version order
     * @param completedAtStart the final update that is guaranteed to have completed after the read started
     * @return a set of possible values the read could be
     */
    @Nonnull
    private static Set<Message> possibleValuesForRecord(@Nonnull Deque<Pair<Integer, Message>> history, int completedAtStart) {
        Set<Message> possibleValues = new HashSet<>();
        boolean foundOldest = false;
        for (final Pair<Integer, Message> pair : history) {
            possibleValues.add(pair.getRight());
            if (Objects.requireNonNull(pair.getLeft()) < completedAtStart) {
                foundOldest = true;
                break;
            }
        }
        if (!foundOldest) {
            possibleValues.add(null);
        }
        return possibleValues;
    }

    private static boolean canBeNull(@Nonnull Set<?> possibleValues) {
        return possibleValues.contains(null);
    }

    private static boolean anyNonNull(@Nonnull Set<?> possibleValues) {
        return possibleValues.stream().anyMatch(Objects::nonNull);
    }

    @Test
    void readPreloaded() throws Exception {
        byte[] commitVersionstamp;
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .build();
            recordStore.saveRecord(rec);

            commit(context);
            commitVersionstamp = context.getVersionStamp();
            assertNotNull(commitVersionstamp);
        }

        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.preloadRecordAsync(Tuple.from(1066L)).get();  // ensure loaded in context
            context.ensureActive().cancel(); // ensure no more I/O done through the transaction
            FDBStoredRecord<Message> record = recordStore.loadRecord(Tuple.from(1066L));
            assertNotNull(record);
            assertSame(TestRecords1Proto.MySimpleRecord.getDescriptor(), record.getRecordType().getDescriptor());
            assertEquals(1066L, record.getRecord().getField(TestRecords1Proto.MySimpleRecord.getDescriptor().findFieldByNumber(TestRecords1Proto.MySimpleRecord.REC_NO_FIELD_NUMBER)));
            assertEquals(FDBRecordVersion.complete(commitVersionstamp, 0), record.getVersion());

            FDBExceptions.FDBStoreException e = assertThrows(FDBExceptions.FDBStoreException.class, context::commit);
            assertNotNull(e.getCause());
            assertThat(e.getCause(), instanceOf(FDBException.class));
            FDBException fdbE = (FDBException)e.getCause();
            assertEquals(FDBError.TRANSACTION_CANCELLED.code(), fdbE.getCode());
        }
    }

    @Test
    void readMissingPreloaded() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            // 4488 does not exist
            recordStore.preloadRecordAsync(Tuple.from(4488L)).get();  // ensure loaded in context
            context.ensureActive().cancel(); // ensure no more I/O done through the transaction

            FDBStoredRecord<Message> record = recordStore.loadRecord(Tuple.from(4488L));
            assertNull(record);

            FDBExceptions.FDBStoreException e = assertThrows(FDBExceptions.FDBStoreException.class, context::commit);
            assertNotNull(e.getCause());
            assertThat(e.getCause(), instanceOf(FDBException.class));
            FDBException fdbE = (FDBException)e.getCause();
            assertEquals(FDBError.TRANSACTION_CANCELLED.code(), fdbE.getCode());
        }
    }

    @Test
    void readYourWritesPreloaded() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .build();
            recordStore.saveRecord(rec);

            recordStore.preloadRecordAsync(Tuple.from(1066L)).get();
            context.ensureActive().cancel(); // ensure no more I/O done through the transaction
            FDBStoredRecord<Message> record = recordStore.loadRecord(Tuple.from(1066L));
            assertNotNull(record);
            assertSame(TestRecords1Proto.MySimpleRecord.getDescriptor(), record.getRecordType().getDescriptor());
            assertEquals(rec.toByteString(), record.getRecord().toByteString());
            assertEquals(FDBRecordVersion.incomplete(0), record.getVersion());

            FDBExceptions.FDBStoreException e = assertThrows(FDBExceptions.FDBStoreException.class, context::commit);
            assertNotNull(e.getCause());
            assertThat(e.getCause(), instanceOf(FDBException.class));
            FDBException fdbE = (FDBException)e.getCause();
            assertEquals(FDBError.TRANSACTION_CANCELLED.code(), fdbE.getCode());
        }
    }

    @Test
    void deletePreloaded() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .build();
            recordStore.saveRecord(rec);

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.preloadRecordAsync(Tuple.from(1066L)).get();  // ensure loaded in context
            recordStore.deleteRecord(Tuple.from(1066L));
            FDBStoredRecord<Message> record = recordStore.loadRecord(Tuple.from(1066L));
            assertNull(record);
        }
    }

    @Test
    void deleteAllPreloaded() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .build();
            recordStore.saveRecord(rec);

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.preloadRecordAsync(Tuple.from(1066L)).get();  // ensure loaded in context
            recordStore.deleteAllRecords();
            FDBStoredRecord<Message> record = recordStore.loadRecord(Tuple.from(1066L));
            assertNull(record);
        }
    }

    @Test
    void saveOverPreloaded() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .setStrValueIndexed("first_value")
                    .build();
            recordStore.saveRecord(rec);

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.preloadRecordAsync(Tuple.from(1066L)).get();  // ensure loaded in context

            TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder()
                    .setRecNo(1066L)
                    .setStrValueIndexed("second_value")
                    .build();
            recordStore.saveRecord(rec);

            FDBStoredRecord<Message> record = recordStore.loadRecord(Tuple.from(1066L));
            assertNotNull(record);
            assertSame(TestRecords1Proto.MySimpleRecord.getDescriptor(), record.getRecordType().getDescriptor());
            assertEquals(1066L, record.getRecord().getField(TestRecords1Proto.MySimpleRecord.getDescriptor().findFieldByNumber(TestRecords1Proto.MySimpleRecord.REC_NO_FIELD_NUMBER)));
            assertEquals("second_value", record.getRecord().getField(TestRecords1Proto.MySimpleRecord.getDescriptor().findFieldByNumber(TestRecords1Proto.MySimpleRecord.STR_VALUE_INDEXED_FIELD_NUMBER)));
            assertEquals(FDBRecordVersion.incomplete(0), record.getVersion());
        }
    }

    @Test
    void preloadNonExisting() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            // Make sure pre-loading a non-existing record doesn't fail
            recordStore.preloadRecordAsync(Tuple.from(1L, 2L, 3L, 4L));
        }
    }

    @Test
    void delete() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);

            TestRecords1Proto.MySimpleRecord.Builder recBuilder = TestRecords1Proto.MySimpleRecord.newBuilder();
            recBuilder.setRecNo(1);
            recBuilder.setStrValueIndexed("abc");
            recBuilder.setNumValueUnique(123);
            recordStore.saveRecord(recBuilder.build());
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            recordStore.deleteRecord(Tuple.from(1L));
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openSimpleRecordStore(context);
            FDBStoredRecord<Message> rec1 = recordStore.loadRecord(Tuple.from(1L));
            assertNull(rec1);
            commit(context);
        }
    }

}
