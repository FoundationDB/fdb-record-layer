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
import com.apple.foundationdb.record.lucene.directory.FDBDirectoryManager;
import com.apple.foundationdb.record.lucene.directory.MockedFDBDirectory;
import com.apple.foundationdb.record.lucene.directory.MockedFDBDirectoryManager;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.common.text.AllSuffixesTextTokenizer;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintenanceFilter;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.util.pair.Pair;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.Tags;
import com.google.common.base.Verify;
import org.apache.lucene.search.Sort;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static com.apple.foundationdb.record.lucene.LuceneIndexOptions.INDEX_PARTITION_BY_FIELD_NAME;
import static com.apple.foundationdb.record.lucene.LuceneIndexOptions.INDEX_PARTITION_HIGH_WATERMARK;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.createComplexDocument;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.COMPLEX_DOC;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * A test that uses a few of the tests from {@link LuceneIndexTest} under a fault-injection scenario.
 */
@Tag(Tags.RequiresFDB)
public class FDBLuceneIndexFailureTest extends FDBRecordStoreTestBase {

    protected static final String ENGINEER_JOKE = "A software engineer, a hardware engineer, and a departmental manager were driving down a steep mountain road when suddenly the brakes on their car failed. The car careened out of control down the road, bouncing off the crash barriers, ground to a halt scraping along the mountainside. The occupants were stuck halfway down a mountain in a car with no brakes. What were they to do?" +
            "'I know,' said the departmental manager. 'Let's have a meeting, propose a Vision, formulate a Mission Statement, define some Goals, and by a process of Continuous Improvement find a solution to the Critical Problems, and we can be on our way.'" +
            "'No, no,' said the hardware engineer. 'That will take far too long, and that method has never worked before. In no time at all, I can strip down the car's braking system, isolate the fault, fix it, and we can be on our way.'" +
            "'Wait, said the software engineer. 'Before we do anything, I think we should push the car back up the road and see if it happens again.'";
    protected static final String WAYLON = "There's always one more way to do things and that's your way, and you have a right to try it at least once.";

    protected static final Index COMPLEX_PARTITIONED = complexPartitionedIndex(Map.of(
            IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME,
            INDEX_PARTITION_BY_FIELD_NAME, "timestamp",
            INDEX_PARTITION_HIGH_WATERMARK, "10"));

    protected static final Index COMPLEX_PARTITIONED_NOGROUP = complexPartitionedIndexNoGroup(Map.of(
            IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME,
            INDEX_PARTITION_BY_FIELD_NAME, "timestamp",
            INDEX_PARTITION_HIGH_WATERMARK, "10"));

    @ParameterizedTest
    @BooleanSource
    void basicGroupedPartitionedTest(boolean useLegacyAsyncToSync) {
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_USE_LEGACY_ASYNC_TO_SYNC, useLegacyAsyncToSync)
                .build();
        try (FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, COMPLEX_DOC, COMPLEX_PARTITIONED);
            final LuceneScanBounds scanBounds = groupedTextSearch(COMPLEX_PARTITIONED, "text:propose", 2);
            final MockedFDBDirectory mockedDirectory = (MockedFDBDirectory)injectMockDirectoryManager(context, COMPLEX_PARTITIONED).getDirectory(scanBounds.groupKey, 0);
            mockedDirectory.addFailure(MockedFDBDirectory.Methods.GET_FDB_LUCENE_FILE_REFERENCE_ASYNC,
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
            getMockedDirectory(scanBounds.getGroupKey(), COMPLEX_PARTITIONED_NOGROUP, 0)
                    .addFailure(MockedFDBDirectory.Methods.GET_FDB_LUCENE_FILE_REFERENCE_ASYNC,
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
            getMockedDirectory(groupingKey, index, 1)
                    .addFailure(MockedFDBDirectory.Methods.GET_PRIMARY_KEY_SEGMENT_INDEX,
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
            getMockedDirectory(groupingKey, index, 0).addFailure(MockedFDBDirectory.Methods.GET_PRIMARY_KEY_SEGMENT_INDEX,
                    new LuceneConcurrency.AsyncToSyncTimeoutException("Blah", new TimeoutException("Blah")),
                    0);
            // this should fail with injected exception
            assertThrows(LuceneConcurrency.AsyncToSyncTimeoutException.class,
                    () -> recordStore.saveRecord(createComplexDocument(1000L , ENGINEER_JOKE, docGroupFieldValue, 1)));
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
            getMockedDirectory(groupingKey, index, 0)
                    .addFailure(MockedFDBDirectory.Methods.GET_PRIMARY_KEY_SEGMENT_INDEX,
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
            getMockedDirectory(groupingKey, index, 0)
                    .addFailure(MockedFDBDirectory.Methods.LIST_ALL,
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
            getMockedDirectory(groupingKey, index, 0)
                    .addFailure(MockedFDBDirectory.Methods.LIST_ALL,
                            new IOException((new UnknownRuntimeException("Blah"))),
                            0);
            // this should fail with injected exception
            assertThrows(UnknownRuntimeException.class,
                    () -> recordStore.saveRecord(createComplexDocument(1000L , ENGINEER_JOKE, docGroupFieldValue, 1)));
        }
    }

    private MockedFDBDirectory getMockedDirectory(final Tuple groupKey, final Index index, final int partitionId) {
        LuceneIndexMaintainer indexMaintainer = (LuceneIndexMaintainer)recordStore.getIndexMaintainer(index);
        MockedFDBDirectory mockedDirectory = (MockedFDBDirectory)indexMaintainer.getDirectory(groupKey, partitionId);
        return mockedDirectory;
    }

    @Nonnull
    static Index complexPartitionedIndex(final Map<String, String> options) {
        return new Index("Complex$partitioned",
                concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
                        function(LuceneFunctionNames.LUCENE_SORTED, field("timestamp"))).groupBy(field("group")),
                LuceneIndexTypes.LUCENE,
                options);
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
        Pair<FDBRecordStore, QueryPlanner> pair = LuceneIndexTestUtils.rebuildIndexMetaData(context, path, document, index, isUseCascadesPlanner());
        this.recordStore = pair.getLeft();
        this.planner = pair.getRight();
        this.recordStore.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(true);
    }

    private StoreTimer.Counter getCounter(@Nonnull final FDBRecordContext recordContext, @Nonnull final StoreTimer.Event event) {
        return Verify.verifyNotNull(recordContext.getTimer()).getCounter(event);
    }

    private LuceneScanBounds groupedTextSearch(Index index, String search, Object group) {
        return groupedSortedTextSearch(index, search, null, group);
    }

    private LuceneScanBounds groupedSortedTextSearch(Index index, String search, Sort sort, Object group) {
        return LuceneIndexTestValidator.groupedSortedTextSearch(recordStore, index, search, sort, group);
    }

    private LuceneScanBounds fullTextSearch(Index index, String search) {
        return LuceneIndexTestUtils.fullTextSearch(recordStore, index, search, false);
    }

    @Nonnull
    private static Index complexPartitionedIndexNoGroup(final Map<String, String> options) {
        return new Index("Complex$partitioned_noGroup",
                concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
                        function(LuceneFunctionNames.LUCENE_SORTED, field("timestamp"))),
                LuceneIndexTypes.LUCENE,
                options);
    }

    private void validateDocsInPartition(Index index, int partitionId, Tuple groupingKey,
                                         Set<Tuple> expectedPrimaryKeys, final String universalSearch) throws IOException {
        LuceneIndexTestValidator.validateDocsInPartition(recordStore, index, partitionId, groupingKey, expectedPrimaryKeys, universalSearch);
    }

    private Set<Tuple> makeKeyTuples(long group, int... ranges) {
        int[] rangeList = Arrays.stream(ranges).toArray();
        if (rangeList.length == 0 || rangeList.length % 2 == 1) {
            throw new IllegalArgumentException("specify ranges as pairs of (from, to)");
        }
        Set<Tuple> tuples = new HashSet<>();
        for (int i = 0; i < rangeList.length - 1; i += 2) {
            for (int j = rangeList[i]; j <= rangeList[i + 1]; j++) {
                tuples.add(Tuple.from(group, j));
            }
        }
        return tuples;
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

}
