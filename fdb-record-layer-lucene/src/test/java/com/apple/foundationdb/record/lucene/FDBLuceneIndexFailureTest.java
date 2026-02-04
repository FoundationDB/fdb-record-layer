/*
 * FDBLuceneIndexFailureTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository;
import com.apple.foundationdb.record.lucene.directory.MockedLuceneIndexMaintainerFactory;
import com.apple.foundationdb.record.lucene.directory.TestingIndexMaintainerRegistry;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.OnlineIndexer;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.util.LoggableException;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.createComplexDocument;
import static com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository.Methods.LUCENE_GET_ALL_FIELDS_INFO_STREAM;
import static com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository.Methods.LUCENE_GET_FDB_LUCENE_FILE_REFERENCE_ASYNC;
import static com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository.Methods.LUCENE_GET_PRIMARY_KEY_SEGMENT_INDEX;
import static com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository.Methods.LUCENE_LIST_ALL;
import static com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository.Methods.LUCENE_READ_BLOCK;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.COMPLEX_DOC;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A test that uses a few of the tests from {@link LuceneIndexTest} under a fault-injection scenario.
 */
@Tag(Tags.RequiresFDB)
public class FDBLuceneIndexFailureTest extends FDBLuceneTestBase {
    private TestingIndexMaintainerRegistry registry;
    private InjectedFailureRepository injectedFailures;

    @BeforeEach
    public void setup() {
        this.registry = new TestingIndexMaintainerRegistry();
        this.injectedFailures = new InjectedFailureRepository();
        // This registry is used in openContext
        registry.overrideFactory(new MockedLuceneIndexMaintainerFactory(injectedFailures));
    }

    @ParameterizedTest
    @BooleanSource
    void basicGroupedPartitionedTest(boolean useLegacyAsyncToSync) {
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_USE_LEGACY_ASYNC_TO_SYNC, useLegacyAsyncToSync)
                .build();
        try (FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, COMPLEX_DOC, COMPLEX_PARTITIONED);
            final LuceneScanBounds scanBounds = groupedTextSearch(COMPLEX_PARTITIONED, "text:propose", 2);
            injectedFailures.addFailure(LUCENE_GET_FDB_LUCENE_FILE_REFERENCE_ASYNC,
                            new FDBExceptions.FDBStoreTransactionIsTooOldException("Blah", new FDBException("Blah", 7)),
                            0);

            recordStore.saveRecord(createComplexDocument(6666L, ENGINEER_JOKE, 1, Instant.now().toEpochMilli()));
            recordStore.saveRecord(createComplexDocument(7777L, ENGINEER_JOKE, 2, Instant.now().toEpochMilli()));
            recordStore.saveRecord(createComplexDocument(8888L, WAYLON, 2, Instant.now().plus(1, ChronoUnit.DAYS).toEpochMilli()));
            recordStore.saveRecord(createComplexDocument(9999L, "hello world!", 1, Instant.now().plus(2, ChronoUnit.DAYS).toEpochMilli()));

            // This fails with the mock exception
            assertThrows(FDBExceptions.FDBStoreTransactionIsTooOldException.class,
                    () -> LuceneConcurrency.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_GET_FILE_REFERENCE,
                            recordStore.scanIndex(COMPLEX_PARTITIONED, scanBounds, null, ScanProperties.FORWARD_SCAN).asList(),
                            context));
            assertNull(getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY));
        }
    }

    @ParameterizedTest
    @BooleanSource
    void basicNonGroupedPartitionedTest(boolean useLegacyAsyncToSync) {
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_USE_LEGACY_ASYNC_TO_SYNC, useLegacyAsyncToSync)
                .build();
        try (FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, COMPLEX_DOC, COMPLEX_PARTITIONED_NOGROUP);
            final LuceneScanBounds scanBounds = fullTextSearch(COMPLEX_PARTITIONED_NOGROUP, "text:propose");
            injectedFailures.addFailure(LUCENE_GET_FDB_LUCENE_FILE_REFERENCE_ASYNC,
                            new LuceneConcurrency.AsyncToSyncTimeoutException("Blah", new TimeoutException("Blah")),
                            0);

            recordStore.saveRecord(createComplexDocument(6666L, ENGINEER_JOKE, 1, Instant.now().toEpochMilli()));
            recordStore.saveRecord(createComplexDocument(7777L, ENGINEER_JOKE, 2, Instant.now().toEpochMilli()));
            recordStore.saveRecord(createComplexDocument(8888L, WAYLON, 2, Instant.now().plus(1, ChronoUnit.DAYS).toEpochMilli()));
            recordStore.saveRecord(createComplexDocument(9999L, "hello world!", 1, Instant.now().plus(2, ChronoUnit.DAYS).toEpochMilli()));

            // This should fail because the mock exception is thrown
            assertThrows(LuceneConcurrency.AsyncToSyncTimeoutException.class,
                    () -> LuceneConcurrency.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_GET_FILE_REFERENCE,
                            recordStore.scanIndex(COMPLEX_PARTITIONED_NOGROUP, scanBounds, null, ScanProperties.FORWARD_SCAN).asList(),
                            context));
        }
    }

    public static Stream<Arguments> legacySyncAndMapping() {
        return Stream.of(true, false)
                .flatMap(useLegacyAsyncToSync -> Stream.of(true, false)
                        .map(useExceptionMapping -> Arguments.of(useLegacyAsyncToSync, useExceptionMapping)));
    }

    @ParameterizedTest
    @MethodSource("legacySyncAndMapping")
    void repartitionGroupedTestWithExceptionMapping(boolean useLegacyAsyncToSync, boolean useExceptionMapping) throws IOException {
        Index index = COMPLEX_PARTITIONED;
        Tuple groupingKey = Tuple.from(1L);
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 6)
                .addProp(LuceneRecordContextProperties.LUCENE_USE_LEGACY_ASYNC_TO_SYNC, useLegacyAsyncToSync)
                .build();
        setupExceptionMapping(useExceptionMapping);

        final int totalDocCount = 20;
        Consumer<FDBRecordContext> schemaSetup = context -> rebuildIndexMetaData(context, COMPLEX_DOC, index);
        long docGroupFieldValue = groupingKey.isEmpty() ? 0L : groupingKey.getLong(0);

        // create/save documents
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            long start = Instant.now().toEpochMilli();
            for (int i = 0; i < totalDocCount; i++) {
                recordStore.saveRecord(createComplexDocument(1000L + i, ENGINEER_JOKE, docGroupFieldValue, start + i * 100));
            }
            commit(context);
        }

        // initially, all documents are saved into one partition
        List<LucenePartitionInfoProto.LucenePartitionInfo> partitionInfos = getPartitionMeta(index, groupingKey, contextProps, schemaSetup);
        assertEquals(1, partitionInfos.size());
        assertEquals(totalDocCount, partitionInfos.get(0).getCount());

        // run re-partitioning with failure
        // When exception mapping is in place, the thrown exception is UnknownLoggableException
        // When no exception mapping is taking place, the thrown exception is the injected one (AsyncToSyncTimeout)
        if (useExceptionMapping) {
            assertThrows(UnknownLoggableException.class,
                    () -> explicitMergeIndex(index, contextProps, schemaSetup, true, 0));
        } else {
            assertThrows(LuceneConcurrency.AsyncToSyncTimeoutException.class,
                    () -> explicitMergeIndex(index, contextProps, schemaSetup, true, 0));
        }

        // run partitioning without failure - make sure the index is still in good shape
        timer.reset();
        explicitMergeIndex(index, contextProps, schemaSetup, false, 0);
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            assertEquals(1, getCounter(context, LuceneEvents.Counts.LUCENE_REPARTITION_CALLS).getCount());
        }
        partitionInfos = getPartitionMeta(index, groupingKey, contextProps, schemaSetup);
        // It should first move 6 from the most-recent to a new, older partition, then move 6 again into a partition
        // in between the two
        assertEquals(List.of(6, 6, 8),
                partitionInfos.stream()
                        .sorted(Comparator.comparing(partitionInfo -> Tuple.fromBytes(partitionInfo.getFrom().toByteArray())))
                        .map(LucenePartitionInfoProto.LucenePartitionInfo::getCount)
                        .collect(Collectors.toList()));
        assertEquals(List.of(1, 2, 0),
                partitionInfos.stream()
                        .sorted(Comparator.comparing(partitionInfo -> Tuple.fromBytes(partitionInfo.getFrom().toByteArray())))
                        .map(LucenePartitionInfoProto.LucenePartitionInfo::getId)
                        .collect(Collectors.toList()));
    }

    @Tag(Tags.Slow)
    @ParameterizedTest
    @BooleanSource
    void repartitionAndMerge(boolean useLegacyAsyncToSync) throws IOException {
        Index index = COMPLEX_PARTITIONED;
        Tuple groupingKey = Tuple.from(1);
        int mergeSegmentsPerTier = 2;

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 2)
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, (double)mergeSegmentsPerTier)
                .addProp(LuceneRecordContextProperties.LUCENE_USE_LEGACY_ASYNC_TO_SYNC, useLegacyAsyncToSync)
                .build();

        Consumer<FDBRecordContext> schemaSetup = context -> rebuildIndexMetaData(context, COMPLEX_DOC, index);
        long docGroupFieldValue = groupingKey.isEmpty() ? 0L : groupingKey.getLong(0);

        int transactionCount = 100;
        int docsPerTransaction = 2;
        // create/save documents
        long id = 0;
        List<Long> allIds = new ArrayList<>();
        for (int i = 0; i < transactionCount; i++) {
            try (FDBRecordContext context = openContext(contextProps)) {
                schemaSetup.accept(context);
                recordStore.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(false);
                long start = Instant.now().toEpochMilli();
                for (int j = 0; j < docsPerTransaction; j++) {
                    id++;
                    recordStore.saveRecord(createComplexDocument(id, ENGINEER_JOKE, docGroupFieldValue, start + id));
                    allIds.add(id);
                }
                commit(context);
            }
        }

        // we haven't done any merges yet, or repartitioning, so each transaction should be one new segment
        assertEquals(Map.of(0, transactionCount),
                getSegmentCounts(index, groupingKey, contextProps, schemaSetup));

        // Run a few merges with different failure points
        assertThrows(LuceneConcurrency.AsyncToSyncTimeoutException.class,
                () -> explicitMergeIndex(index, contextProps, schemaSetup, true, 0));
        assertThrows(LuceneConcurrency.AsyncToSyncTimeoutException.class,
                () -> explicitMergeIndex(index, contextProps, schemaSetup, true, 5));
        assertThrows(LuceneConcurrency.AsyncToSyncTimeoutException.class,
                () -> explicitMergeIndex(index, contextProps, schemaSetup, true, 10));

        // Continue with the regular merge and verify that index is still valid
        timer.reset();
        explicitMergeIndex(index, contextProps, schemaSetup, false, 0);
        final Map<Integer, Integer> segmentCounts = getSegmentCounts(index, groupingKey, contextProps, schemaSetup);
        final int partitionSize = 10;
        final int partitionCount;
        partitionCount = allIds.size() / partitionSize;
        assertThat(segmentCounts, Matchers.aMapWithSize(partitionCount));
        assertEquals(IntStream.range(0, partitionCount).boxed()
                        .collect(Collectors.toMap(Function.identity(), partitionId -> 2)),
                segmentCounts);

        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            validateDocsInPartition(index, 0, groupingKey,
                    allIds.stream()
                            .skip(190)
                            .map(idLong -> Tuple.from(docGroupFieldValue, idLong))
                            .collect(Collectors.toSet()),
                    "text:propose");
            for (int i = 1; i < 20; i++) {
                // 0 should have the newest
                // everyone else should increase
                validateDocsInPartition(index, i, groupingKey,
                        allIds.stream().skip((i - 1) * partitionSize)
                                .limit(partitionSize)
                                .map(idLong -> Tuple.from(docGroupFieldValue, idLong))
                                .collect(Collectors.toSet()),
                        "text:propose");
            }
            List<LucenePartitionInfoProto.LucenePartitionInfo> partitionInfos = getPartitionMeta(index,
                    groupingKey, contextProps, schemaSetup);
            assertEquals(partitionCount, partitionInfos.size());
        }
    }

    @ParameterizedTest
    @BooleanSource
    void optimizedPartitionInsertionTest(boolean useLegacyAsyncToSync) throws IOException {
        Index index = COMPLEX_PARTITIONED;
        Tuple groupingKey = Tuple.from(1L);
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 6)
                .addProp(LuceneRecordContextProperties.LUCENE_USE_LEGACY_ASYNC_TO_SYNC, useLegacyAsyncToSync)
                .build();

        final int totalDocCount = 10; // configured index's highwater mark
        Consumer<FDBRecordContext> schemaSetup = context -> rebuildIndexMetaData(context, COMPLEX_DOC, index);
        long docGroupFieldValue = groupingKey.isEmpty() ? 0L : groupingKey.getLong(0);

        // create/save documents
        long start = Instant.now().toEpochMilli();
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            for (int i = 0; i < totalDocCount; i++) {
                recordStore.saveRecord(createComplexDocument(1000L + i, ENGINEER_JOKE, docGroupFieldValue, start + i * 100));
            }
            commit(context);
        }

        // partition 0 should be at capacity now
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            validateDocsInPartition(index, 0, groupingKey, makeKeyTuples(docGroupFieldValue, 1000, 1009), "text:propose");
        }

        // Add docs and fail the insertion
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            // Inject failures (using partition 1)
            injectedFailures.addFailure(LUCENE_GET_PRIMARY_KEY_SEGMENT_INDEX,
                            new LuceneConcurrency.AsyncToSyncTimeoutException("Blah", new TimeoutException("Blah")),
                            0);
            // Save more docs - this should fail with injected exception
            assertThrows(LuceneConcurrency.AsyncToSyncTimeoutException.class,
                    () -> recordStore.saveRecord(createComplexDocument(1000L + totalDocCount, ENGINEER_JOKE, docGroupFieldValue, start - 1)));
        }

        // now add 20 documents older than the oldest document in partition 0
        // they should go into partitions 1 and 2
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            injectedFailures.clear();
            for (int i = 0; i < 20; i++) {
                recordStore.saveRecord(createComplexDocument(1000L + totalDocCount + i, ENGINEER_JOKE, docGroupFieldValue, start - i - 1));
            }
            validateDocsInPartition(index, 1, groupingKey, makeKeyTuples(docGroupFieldValue, 1010, 1019), "text:propose");
            validateDocsInPartition(index, 2, groupingKey, makeKeyTuples(docGroupFieldValue, 1020, 1029), "text:propose");
        }
    }

    @ParameterizedTest
    @BooleanSource
    void addDocumentTest(boolean useLegacyAsyncToSync) throws IOException {
        Index index = COMPLEX_PARTITIONED;
        Tuple groupingKey = Tuple.from(1L);
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_USE_LEGACY_ASYNC_TO_SYNC, useLegacyAsyncToSync)
                .build();

        Consumer<FDBRecordContext> schemaSetup = context -> rebuildIndexMetaData(context, COMPLEX_DOC, index);
        long docGroupFieldValue = groupingKey.isEmpty() ? 0L : groupingKey.getLong(0);

        // create/save documents
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            // Inject failures (using partition 0)
            injectedFailures.addFailure(LUCENE_GET_PRIMARY_KEY_SEGMENT_INDEX,
                    new LuceneConcurrency.AsyncToSyncTimeoutException("Blah", new TimeoutException("Blah")),
                    0);
            // this should fail with injected exception
            assertThrows(LuceneConcurrency.AsyncToSyncTimeoutException.class,
                    () -> recordStore.saveRecord(createComplexDocument(1000L , ENGINEER_JOKE, docGroupFieldValue, 1)));
        }
    }

    @ParameterizedTest
    @BooleanSource
    void updateDocumentFailedTest(boolean useLegacyAsyncToSync) throws IOException {
        // This test injects a failure late in the update process, into the commit part of the update, where the updated
        // index is written to DB
        Index index = COMPLEX_PARTITIONED;
        Tuple groupingKey = Tuple.from(1L);
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_USE_LEGACY_ASYNC_TO_SYNC, useLegacyAsyncToSync)
                .build();

        Consumer<FDBRecordContext> schemaSetup = context -> rebuildIndexMetaData(context, COMPLEX_DOC, index);
        long docGroupFieldValue = groupingKey.isEmpty() ? 0L : groupingKey.getLong(0);

        // create documents
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            // this should fail with injected exception
            recordStore.saveRecord(createComplexDocument(1000L , ENGINEER_JOKE, docGroupFieldValue, 1));
            context.commit();
        }
        // Update documents with failure
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            // Inject failures (using partition 0)
            injectedFailures.addFailure(LUCENE_READ_BLOCK,
                    new LuceneConcurrency.AsyncToSyncTimeoutException("Blah", new TimeoutException("Blah")),
                    5);
            // this should fail with injected exception
            recordStore.saveRecord(createComplexDocument(1000L , ENGINEER_JOKE, docGroupFieldValue, 2));
            assertThrows(LuceneConcurrency.AsyncToSyncTimeoutException.class,
                    () -> context.commit());
        }
    }

    @ParameterizedTest
    @BooleanSource
    void addDocumentWithUnknownExceptionTest(boolean useLegacyAsyncToSync) throws IOException {
        Index index = COMPLEX_PARTITIONED;
        Tuple groupingKey = Tuple.from(1L);
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_USE_LEGACY_ASYNC_TO_SYNC, useLegacyAsyncToSync)
                .build();

        Consumer<FDBRecordContext> schemaSetup = context -> rebuildIndexMetaData(context, COMPLEX_DOC, index);
        long docGroupFieldValue = groupingKey.isEmpty() ? 0L : groupingKey.getLong(0);

        // Unknown RecordCoreException (rethrown as-is)
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            // Inject failures (using partition 0)
            injectedFailures.addFailure(LUCENE_GET_PRIMARY_KEY_SEGMENT_INDEX,
                            new UnknownRecordCoreException("Blah"),
                            0);
            // this should fail with injected exception
            assertThrows(UnknownRecordCoreException.class,
                    () -> recordStore.saveRecord(createComplexDocument(1000L , ENGINEER_JOKE, docGroupFieldValue, 1)));
        }

        // Unknown IOException with RecordCoreException (cause rethrown)
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            // Inject failures (using partition 0)
            injectedFailures.addFailure(LUCENE_LIST_ALL,
                            new IOException((new UnknownRecordCoreException("Blah"))),
                            0);
            // this should fail with injected exception
            assertThrows(UnknownRecordCoreException.class,
                    () -> recordStore.saveRecord(createComplexDocument(1000L , ENGINEER_JOKE, docGroupFieldValue, 1)));
        }

        // Unknown IOException with RuntimeException (cause rethrown)
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            // Inject failures (using partition 0)
            injectedFailures.addFailure(LUCENE_LIST_ALL,
                            new IOException((new UnknownRuntimeException("Blah"))),
                            0);
            // this should fail with injected exception
            assertThrows(UnknownRuntimeException.class,
                    () -> recordStore.saveRecord(createComplexDocument(1000L , ENGINEER_JOKE, docGroupFieldValue, 1)));
        }
    }

    /**
     * Simulate the case where the Exception injection is active.
     * This test inserts an exception mapper into the store to verify that exception handling is working when unknown
     * exceptions
     * are thrown from the legacy async-to-sync mechanism
     *
     * @param useLegacyAsyncToSync whether to set the legacy-async-to-sync property
     */
    @ParameterizedTest
    @MethodSource("legacySyncAndMapping")
    void saveDocumentWithMappedInjectedException(boolean useLegacyAsyncToSync, boolean useExceptionMapping) {
        Index index = COMPLEX_PARTITIONED;
        Tuple groupingKey = Tuple.from(1L);
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_USE_LEGACY_ASYNC_TO_SYNC, useLegacyAsyncToSync)
                .build();
        setupExceptionMapping(useExceptionMapping);

        Consumer<FDBRecordContext> schemaSetup = context -> rebuildIndexMetaData(context, COMPLEX_DOC, index);
        long docGroupFieldValue = groupingKey.isEmpty() ? 0L : groupingKey.getLong(0);

        // Save a document
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            recordStore.saveRecord(createComplexDocument(1000L, ENGINEER_JOKE, docGroupFieldValue, 100));
            commit(context);
        }

        // Save another, with injected failure
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            // Inject failures (using partition 0)
            injectedFailures.addFailure(LUCENE_GET_ALL_FIELDS_INFO_STREAM,
                            new FDBExceptions.FDBStoreTransactionIsTooOldException("Blah", new FDBException("Blah", 7)),
                            0);
            // For the legacy asyncToSync case, the mapper creates the instance of the UnknownLoggableException when the original
            // error is thrown from the MockedFDBDirectory.
            // For the non-legacy case, the MockedFDBDirectory throws the injected exception, but the call to saveRecord()
            // calls the non-lucene asyncToSync and that gets wrapped by the mapper.
            if (useExceptionMapping) {
                assertThrows(UnknownLoggableException.class,
                        () -> recordStore.saveRecord(createComplexDocument(1000L, ENGINEER_JOKE, docGroupFieldValue, 1)));
            } else {
                assertThrows(FDBExceptions.FDBStoreTransactionIsTooOldException.class,
                        () -> LuceneConcurrency.asyncToSync(FDBStoreTimer.Waits.WAIT_SAVE_RECORD,
                                recordStore.saveRecordAsync(createComplexDocument(1000L, ENGINEER_JOKE, docGroupFieldValue, 1)),
                                context));
            }
        }

        // Save using saveAsync with Lucene asyncToSync, with injected failure
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            // Inject failures (using partition 0)
            injectedFailures.addFailure(LUCENE_GET_ALL_FIELDS_INFO_STREAM,
                            new FDBExceptions.FDBStoreTransactionIsTooOldException("Blah", new FDBException("Blah", 7)),
                            0);
            // For the legacy case, the mapper creates the instance of the UnknownLoggableException when the original
            // error is thrown from the MockedFDBDirectory.
            // For the non-legacy case, the injected exception is thrown since no mapping takes place anymore
            if (useLegacyAsyncToSync && useExceptionMapping) {
                assertThrows(UnknownLoggableException.class,
                        () -> LuceneConcurrency.asyncToSync(FDBStoreTimer.Waits.WAIT_SAVE_RECORD,
                                recordStore.saveRecordAsync(createComplexDocument(1000L, ENGINEER_JOKE, docGroupFieldValue, 1)),
                                context));
            } else {
                assertThrows(FDBExceptions.FDBStoreTransactionIsTooOldException.class,
                        () -> LuceneConcurrency.asyncToSync(FDBStoreTimer.Waits.WAIT_SAVE_RECORD,
                                recordStore.saveRecordAsync(createComplexDocument(1000L, ENGINEER_JOKE, docGroupFieldValue, 1)),
                                context));
            }
        }
    }

    private RuntimeException mapExceptions(Throwable throwable, StoreTimer.Event event) {
        if (throwable instanceof ExecutionException) {
            throwable = throwable.getCause();
        }
        return new UnknownLoggableException(throwable);
    }

    /**
     * Private utility to set up the test environment.
     * This method creates the {@link TestingIndexMaintainerRegistry} and the {@link MockedLuceneIndexMaintainerFactory}
     * that create the MockedDirectory* classes that allow failure injection to take place.
     *
     * @param context the context for the store
     * @param document the record type
     * @param index the index to use
     */
    private void rebuildIndexMetaData(final FDBRecordContext context, final String document, final Index index) {
        Pair<FDBRecordStore, QueryPlanner> pair = LuceneIndexTestUtils.rebuildIndexMetaData(context, path, document, index, isUseCascadesPlanner(), registry);
        this.recordStore = pair.getLeft();
        this.planner = pair.getRight();
        this.recordStore.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(true);
    }

    private void setupExceptionMapping(boolean useExceptionMapping) {
        try (FDBRecordContext context = openContext()) {
            if (useExceptionMapping) {
                context.getDatabase().setAsyncToSyncExceptionMapper(this::mapExceptions);
            } else {
                context.getDatabase().setAsyncToSyncExceptionMapper((ex, ev) -> FDBExceptions.wrapException(ex));
            }
        }
    }

    private void explicitMergeIndex(Index index, RecordLayerPropertyStorage contextProps, Consumer<FDBRecordContext> schemaSetup, boolean injectFailure, final int failureAtCount) {
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            if (injectFailure) {
                injectedFailures.addFailure(LUCENE_GET_FDB_LUCENE_FILE_REFERENCE_ASYNC,
                                new LuceneConcurrency.AsyncToSyncTimeoutException("Blah", new TimeoutException("Blah")),
                        failureAtCount); // Since the merge creates a new directory, we need global scope for the failure
            } else {
                injectedFailures.removeFailure(LUCENE_GET_FDB_LUCENE_FILE_REFERENCE_ASYNC);
            }

            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                    .setRecordStore(recordStore)
                    .setIndex(index)
                    .setTimer(timer)
                    .build()) {
                indexBuilder.mergeIndex();
            }
        }
    }

    private class UnknownRecordCoreException extends RecordCoreException {
        private static final long serialVersionUID = 0L;

        public UnknownRecordCoreException(@Nonnull final String msg) {
            super(msg, new Exception(msg));
        }
    }

    private class UnknownRuntimeException extends RuntimeException {
        private static final long serialVersionUID = 0L;

        public UnknownRuntimeException(@Nonnull final String msg) {
            super(msg);
        }
    }

    private class UnknownLoggableException extends LoggableException {
        private static final long serialVersionUID = 0L;

        public UnknownLoggableException(@Nonnull final Throwable cause) {
            super(cause);
        }
    }

}
