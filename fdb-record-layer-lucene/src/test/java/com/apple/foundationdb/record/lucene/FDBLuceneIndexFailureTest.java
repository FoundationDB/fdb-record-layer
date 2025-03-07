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
import com.apple.foundationdb.record.RecordMetaDataProvider;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository;
import com.apple.foundationdb.record.lucene.directory.MockedLuceneIndexMaintainerFactory;
import com.apple.foundationdb.record.lucene.directory.TestingIndexMaintainerRegistry;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.OnlineIndexer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository.Methods.LUCENE_GET_ALL_FIELDS_INFO_STREAM;
import static com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository.Methods.LUCENE_GET_FDB_LUCENE_FILE_REFERENCE_ASYNC;
import static com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository.Methods.LUCENE_GET_FILE_REFERENCE_CACHE_ASYNC;
import static com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository.Methods.LUCENE_GET_PRIMARY_KEY_SEGMENT_INDEX;
import static com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository.Methods.LUCENE_LIST_ALL;
import static com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository.Methods.LUCENE_READ_BLOCK;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A test that uses a few of the tests from {@link LuceneIndexTest} under a fault-injection scenario.
 */
@Tag(Tags.RequiresFDB)
public class FDBLuceneIndexFailureTest extends FDBRecordStoreTestBase {
    private TestingIndexMaintainerRegistry registry;
    private InjectedFailureRepository injectedFailures;

    @BeforeEach
    public void setup() {
        this.registry = new TestingIndexMaintainerRegistry();
        this.injectedFailures = new InjectedFailureRepository();
        // This registry is used in openContext
        registry.overrideFactory(new MockedLuceneIndexMaintainerFactory(injectedFailures));
    }

    public static Stream<Arguments> legacySyncGrouping() {
        return Stream.of(true, false)
                .flatMap(useLegacyAsyncToSync -> Stream.of(true, false)
                        .map(isGrouped -> Arguments.of(useLegacyAsyncToSync, isGrouped)));
    }

    @ParameterizedTest
    @MethodSource("legacySyncGrouping")
    void basicPartitionedTest(boolean useLegacyAsyncToSync, boolean isGrouped) {
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_USE_LEGACY_ASYNC_TO_SYNC, useLegacyAsyncToSync)
                .build();
        final long seed = 6474737L;
        final long start = Instant.now().toEpochMilli();
        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(seed, this::getStoreBuilderWithRegistry, pathManager)
                .setIsGrouped(isGrouped)
                .setPartitionHighWatermark(10)
                .build();

        try (FDBRecordContext context = openContext(contextProps)) {
            FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
            dataModel.saveRecords(5, start, context, 1);

            injectedFailures.addFailure(LUCENE_GET_FILE_REFERENCE_CACHE_ASYNC,
                    new FDBExceptions.FDBStoreTransactionIsTooOldException("Blah", new FDBException("Blah", 7)),
                    0);

            // This fails with the mock exception
            final LuceneScanBounds scanBounds = isGrouped
                                                ? LuceneIndexTestValidator.groupedSortedTextSearch(store, dataModel.index, "text:blah", null, 1)
                                                : LuceneIndexTestUtils.fullTextSearch(store, dataModel.index, "text:blah", false);
            assertThrows(FDBExceptions.FDBStoreTransactionIsTooOldException.class,
                    () -> LuceneConcurrency.asyncToSync(LuceneEvents.Waits.WAIT_LUCENE_GET_FILE_REFERENCE,
                            store.scanIndex(dataModel.index, scanBounds, null, ScanProperties.FORWARD_SCAN).asList(),
                            context));
            assertNull(LuceneIndexTestUtils.getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY));
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
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 6)
                .addProp(LuceneRecordContextProperties.LUCENE_USE_LEGACY_ASYNC_TO_SYNC, useLegacyAsyncToSync)
                .build();

        final long seed = 6474737L;
        Tuple groupingKey = Tuple.from(1L);
        final int totalDocCount = 20;
        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(seed, this::getStoreBuilderWithRegistry, pathManager)
                .setIsGrouped(true)
                .setPartitionHighWatermark(10)
                .build();

        // create/save documents
        long docGroupFieldValue = groupingKey.isEmpty() ? 0L : groupingKey.getLong(0);
        final long start = Instant.now().toEpochMilli();
        try (FDBRecordContext context = openContext(contextProps)) {
            setupExceptionMapping(context, useExceptionMapping);
            dataModel.saveRecords(totalDocCount, start, context, (int)docGroupFieldValue);
            commit(context);
        }

        List<LucenePartitionInfoProto.LucenePartitionInfo> partitionInfos;
        // initially, all documents are saved into one partition
        try (FDBRecordContext context = openContext(contextProps)) {
            FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
            partitionInfos = LuceneIndexTestUtils.getPartitionMeta(dataModel.index, groupingKey, store);
            assertEquals(1, partitionInfos.size());
            assertEquals(totalDocCount, partitionInfos.get(0).getCount());
        }

        // run re-partitioning with failure
        // When exception mapping is in place, the thrown exception is UnknownLoggableException
        // When no exception mapping is taking place, the thrown exception is the injected one (AsyncToSyncTimeout)
        if (useExceptionMapping) {
            assertThrows(UnknownLoggableException.class,
                    () -> explicitMergeIndex(true, 0, contextProps, dataModel));
        } else {
            assertThrows(LuceneConcurrency.AsyncToSyncTimeoutException.class,
                    () -> explicitMergeIndex(true, 0, contextProps, dataModel));
        }

        // run partitioning without failure - make sure the index is still in good shape
        explicitMergeIndex(false, 0, contextProps, dataModel);
        try (FDBRecordContext context = openContext(contextProps)) {
            FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
            assertEquals(2, LuceneIndexTestUtils.getCounter(context, LuceneEvents.Counts.LUCENE_REPARTITION_CALLS).getCount());
            partitionInfos = LuceneIndexTestUtils.getPartitionMeta(dataModel.index, groupingKey, store);
        }
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
        final long seed = 67826354623L;
        Tuple groupingKey = Tuple.from(1);
        int mergeSegmentsPerTier = 2;

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 2)
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, (double)mergeSegmentsPerTier)
                .addProp(LuceneRecordContextProperties.LUCENE_USE_LEGACY_ASYNC_TO_SYNC, useLegacyAsyncToSync)
                .build();

        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(seed, this::getStoreBuilderWithRegistry, pathManager)
                .setIsGrouped(true)
                .setPartitionHighWatermark(10)
                .build();

        long docGroupFieldValue = groupingKey.isEmpty() ? 0L : groupingKey.getLong(0);

        int transactionCount = 100;
        int docsPerTransaction = 2;
        // create/save documents
        long id = 0;
        List<Tuple> allIds = new ArrayList<>();
        for (int i = 0; i < transactionCount; i++) {
            try (FDBRecordContext context = openContext(contextProps)) {
                FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
                long start = Instant.now().toEpochMilli();
                for (int j = 0; j < docsPerTransaction; j++) {
                    id++;
                    Tuple pk = dataModel.saveRecord(start + id, store, (int)docGroupFieldValue);
                    allIds.add(pk);
                }
                commit(context);
            }
        }

        // we haven't done any merges yet, or repartitioning, so each transaction should be one new segment
        try (FDBRecordContext context = openContext(contextProps)) {
            FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
            assertEquals(Map.of(0, transactionCount),
                    LuceneIndexTestUtils.getSegmentCounts(dataModel.index, groupingKey, store));
        }

        // Run a few merges with different failure points
        assertThrows(LuceneConcurrency.AsyncToSyncTimeoutException.class,
                () -> explicitMergeIndex(true, 0, contextProps, dataModel));
        assertThrows(LuceneConcurrency.AsyncToSyncTimeoutException.class,
                () -> explicitMergeIndex(true, 5, contextProps, dataModel));
        assertThrows(LuceneConcurrency.AsyncToSyncTimeoutException.class,
                () -> explicitMergeIndex(true, 10, contextProps, dataModel));

        // Continue with the regular merge and verify that index is still valid
        timer.reset();
        explicitMergeIndex(false, 0, contextProps, dataModel);
        final Map<Integer, Integer> segmentCounts;
        try (FDBRecordContext context = openContext(contextProps)) {
            FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
            segmentCounts = LuceneIndexTestUtils.getSegmentCounts(dataModel.index, groupingKey, store);
        }
        final int partitionSize = 10;
        final int partitionCount;
        partitionCount = allIds.size() / partitionSize;
        assertThat(segmentCounts, Matchers.aMapWithSize(partitionCount));
        assertEquals(IntStream.range(0, partitionCount).boxed()
                        .collect(Collectors.toMap(Function.identity(), partitionId -> 2)),
                segmentCounts);

        try (FDBRecordContext context = openContext(contextProps)) {
            FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
            dataModel.validateDocsInPartition(dataModel.index, 0, groupingKey,
                    allIds.stream()
                            .skip(190)
                            .collect(Collectors.toSet()),
                    LuceneIndexTestDataModel.PARENT_SEARCH_TERM,
                    store);
            for (int i = 1; i < 20; i++) {
                // 0 should have the newest
                // everyone else should increase
                dataModel.validateDocsInPartition(dataModel.index, i, groupingKey,
                        allIds.stream().skip((i - 1) * partitionSize)
                                .limit(partitionSize)
                                .collect(Collectors.toSet()),
                        LuceneIndexTestDataModel.PARENT_SEARCH_TERM,
                        store);
            }
        }
        try (FDBRecordContext context = openContext(contextProps)) {
            FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
            List<LucenePartitionInfoProto.LucenePartitionInfo> partitionInfos = LuceneIndexTestUtils.getPartitionMeta(dataModel.index, groupingKey, store);
            assertEquals(partitionCount, partitionInfos.size());
        }
    }

    @ParameterizedTest
    @BooleanSource
    void optimizedPartitionInsertionTest(boolean useLegacyAsyncToSync) throws IOException {
        long seed = 657368233L;
        Tuple groupingKey = Tuple.from(1L);
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 10)
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, (double)2)
                .addProp(LuceneRecordContextProperties.LUCENE_USE_LEGACY_ASYNC_TO_SYNC, useLegacyAsyncToSync)
                .build();

        final int totalDocCount = 10; // configured index's highwater mark

        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(seed, this::getStoreBuilderWithRegistry, pathManager)
                .setIsGrouped(true)
                .setPartitionHighWatermark(10)
                .build();

        int docGroupFieldValue = groupingKey.isEmpty() ? 0 : (int)groupingKey.getLong(0);

        // create/save documents
        long start = Instant.now().toEpochMilli();
        Set<Tuple> allIds = new LinkedHashSet<>();
        try (FDBRecordContext context = openContext(contextProps)) {
            FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
            for (int i = 0; i < totalDocCount; i++) {
                allIds.add(dataModel.saveRecord(start + i * 100, store, docGroupFieldValue));
            }
            commit(context);
        }

        // partition 0 should be at capacity now
        try (FDBRecordContext context = openContext(contextProps)) {
            FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
            dataModel.validateDocsInPartition(dataModel.index, 0, groupingKey, allIds, LuceneIndexTestDataModel.PARENT_SEARCH_TERM, store);
        }

        // Add docs and fail the insertion
        try (FDBRecordContext context = openContext(contextProps)) {
            FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
            // Inject failures (using partition 1)
            injectedFailures.addFailure(LUCENE_GET_PRIMARY_KEY_SEGMENT_INDEX,
                            new LuceneConcurrency.AsyncToSyncTimeoutException("Blah", new TimeoutException("Blah")),
                            0);
            // Save more docs - this should fail with injected exception
            assertThrows(LuceneConcurrency.AsyncToSyncTimeoutException.class,
                    () -> dataModel.saveRecord(start, store, docGroupFieldValue));
        }

        // now add 20 documents, repartition and make sure the index is valid
        try (FDBRecordContext context = openContext(contextProps)) {
            FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
            injectedFailures.clear();
            for (int i = 0; i < 10; i++) {
                dataModel.saveRecord(start - i - 1, store, docGroupFieldValue);
            }
            for (int i = 10; i < 20; i++) {
                dataModel.saveRecord(start - i - 1, store, docGroupFieldValue);
            }
            commit(context);
        }
        explicitMergeIndex(false, 0, contextProps, dataModel);
        dataModel.validate(() -> openContext(contextProps));
    }

    @ParameterizedTest
    @BooleanSource
    void addDocumentTest(boolean useLegacyAsyncToSync) throws IOException {
        long seed = 4652783648726L;
        Tuple groupingKey = Tuple.from(1L);
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_USE_LEGACY_ASYNC_TO_SYNC, useLegacyAsyncToSync)
                .build();

        int docGroupFieldValue = groupingKey.isEmpty() ? 0 : (int)groupingKey.getLong(0);

        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(seed, this::getStoreBuilderWithRegistry, pathManager)
                .setIsGrouped(true)
                .setPartitionHighWatermark(10)
                .build();

        // create/save documents
        try (FDBRecordContext context = openContext(contextProps)) {
            FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
            // Inject failures (using partition 0)
            injectedFailures.addFailure(LUCENE_GET_PRIMARY_KEY_SEGMENT_INDEX,
                    new LuceneConcurrency.AsyncToSyncTimeoutException("Blah", new TimeoutException("Blah")),
                    0);
            // this should fail with injected exception
            assertThrows(LuceneConcurrency.AsyncToSyncTimeoutException.class,
                    () -> dataModel.saveRecord(1, store, docGroupFieldValue));
        }
    }

    @ParameterizedTest
    @BooleanSource
    void updateDocumentFailedTest(boolean useLegacyAsyncToSync) throws IOException {
        // This test injects a failure late in the update process, into the commit part of the update, where the updated
        // index is written to DB
        long seed = 828737L;
        Tuple groupingKey = Tuple.from(1L);
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_USE_LEGACY_ASYNC_TO_SYNC, useLegacyAsyncToSync)
                .build();

        int docGroupFieldValue = groupingKey.isEmpty() ? 0 : (int)groupingKey.getLong(0);

        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(seed, this::getStoreBuilderWithRegistry, pathManager)
                .setIsGrouped(true)
                .setPartitionHighWatermark(10)
                .build();

        // create documents
        try (FDBRecordContext context = openContext(contextProps)) {
            FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
            dataModel.saveRecord(1, store, docGroupFieldValue);
            context.commit();
        }
        // Update documents with failure
        try (FDBRecordContext context = openContext(contextProps)) {
            FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
            // Inject failures (using partition 0)
            injectedFailures.addFailure(LUCENE_READ_BLOCK,
                    new LuceneConcurrency.AsyncToSyncTimeoutException("Blah", new TimeoutException("Blah")),
                    5);
            // this should fail with injected exception
            dataModel.saveRecord(2, store, docGroupFieldValue);
            assertThrows(LuceneConcurrency.AsyncToSyncTimeoutException.class,
                    () -> context.commit());
        }
    }

    @ParameterizedTest
    @BooleanSource
    void addDocumentWithUnknownExceptionTest(boolean useLegacyAsyncToSync) throws IOException {
        long seed = 66477848;
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_USE_LEGACY_ASYNC_TO_SYNC, useLegacyAsyncToSync)
                .build();

        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(seed, this::getStoreBuilderWithRegistry, pathManager)
                .setIsGrouped(true)
                .setPartitionHighWatermark(10)
                .build();

        // Unknown RecordCoreException (rethrown as-is)
        try (FDBRecordContext context = openContext(contextProps)) {
            FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
            // Inject failures (using partition 0)
            injectedFailures.addFailure(LUCENE_GET_PRIMARY_KEY_SEGMENT_INDEX,
                            new UnknownRecordCoreException("Blah"),
                            0);
            // this should fail with injected exception
            assertThrows(UnknownRecordCoreException.class,
                    () -> dataModel.saveRecord(1, store, 1));
        }

        // Unknown IOException with RecordCoreException (cause rethrown)
        try (FDBRecordContext context = openContext(contextProps)) {
            FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
            // Inject failures (using partition 0)
            injectedFailures.addFailure(LUCENE_LIST_ALL,
                            new IOException((new UnknownRecordCoreException("Blah"))),
                            0);
            // this should fail with injected exception
            assertThrows(UnknownRecordCoreException.class,
                    () -> dataModel.saveRecord(1, store, 1));
        }

        // Unknown IOException with RuntimeException (cause rethrown)
        try (FDBRecordContext context = openContext(contextProps)) {
            FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
            // Inject failures (using partition 0)
            injectedFailures.addFailure(LUCENE_LIST_ALL,
                            new IOException((new UnknownRuntimeException("Blah"))),
                            0);
            // this should fail with injected exception
            assertThrows(UnknownRuntimeException.class,
                    () -> dataModel.saveRecord(1, store, 1));
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
        long seed = 5524442;
        Tuple groupingKey = Tuple.from(1L);
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_USE_LEGACY_ASYNC_TO_SYNC, useLegacyAsyncToSync)
                .build();
        setupExceptionMapping(useExceptionMapping);
        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(seed, this::getStoreBuilderWithRegistry, pathManager)
                .setIsGrouped(true)
                .setPartitionHighWatermark(10)
                .build();


        long docGroupFieldValue = groupingKey.isEmpty() ? 0L : groupingKey.getLong(0);

        // Save a document
        try (FDBRecordContext context = openContext(contextProps)) {
            FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
            dataModel.saveRecord(1, store, 1);
            commit(context);
        }

        // Save another, with injected failure
        try (FDBRecordContext context = openContext(contextProps)) {
            FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
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
                        () -> dataModel.saveRecord(2, store, 1));
            } else {
                assertThrows(FDBExceptions.FDBStoreTransactionIsTooOldException.class,
                        () -> LuceneConcurrency.asyncToSync(FDBStoreTimer.Waits.WAIT_SAVE_RECORD,
                                dataModel.saveRecordAsync(true, 3, store, 1),
                                context));
            }
        }

        // Save using saveAsync with Lucene asyncToSync, with injected failure
        try (FDBRecordContext context = openContext(contextProps)) {
            FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
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
                                dataModel.saveRecordAsync(true, 5, store, 1),
                                context));
            } else {
                assertThrows(FDBExceptions.FDBStoreTransactionIsTooOldException.class,
                        () -> LuceneConcurrency.asyncToSync(FDBStoreTimer.Waits.WAIT_SAVE_RECORD,
                                dataModel.saveRecordAsync(true, 6, store, 1),
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

    private void setupExceptionMapping(FDBRecordContext context, boolean useExceptionMapping) {
        if (useExceptionMapping) {
            context.getDatabase().setAsyncToSyncExceptionMapper(this::mapExceptions);
        } else {
            context.getDatabase().setAsyncToSyncExceptionMapper((ex, ev) -> FDBExceptions.wrapException(ex));
        }
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

    private void explicitMergeIndex(boolean injectFailure, final int failureAtCount, RecordLayerPropertyStorage contextProps, LuceneIndexTestDataModel dataModel) {
        try (FDBRecordContext context = openContext(contextProps)) {
            FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
            if (injectFailure) {
                injectedFailures.addFailure(LUCENE_GET_FDB_LUCENE_FILE_REFERENCE_ASYNC,
                        new LuceneConcurrency.AsyncToSyncTimeoutException("Blah", new TimeoutException("Blah")),
                        failureAtCount); // Since the merge creates a new directory, we need global scope for the failure
            } else {
                injectedFailures.removeFailure(LUCENE_GET_FDB_LUCENE_FILE_REFERENCE_ASYNC);
            }

            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                    .setRecordStore(store)
                    .setIndex(dataModel.index)
                    .setTimer(timer)
                    .build()) {
                indexBuilder.mergeIndex();
            }
        }
    }

    @Nonnull
    protected FDBRecordStore.Builder getStoreBuilderWithRegistry(@Nonnull FDBRecordContext context,
                                                                 @Nonnull RecordMetaDataProvider metaData,
                                                                 @Nonnull final KeySpacePath path) {
        return super.getStoreBuilder(context, metaData, path).setIndexMaintainerRegistry(registry);
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
