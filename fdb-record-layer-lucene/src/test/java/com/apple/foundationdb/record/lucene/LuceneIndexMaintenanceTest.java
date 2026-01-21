/*
 * LuceneIndexValidationTest.java
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
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.LoggableTimeoutException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.lucene.codec.LazyCloseable;
import com.apple.foundationdb.record.lucene.directory.AgilityContext;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.lucene.directory.FDBDirectoryLockFactory;
import com.apple.foundationdb.record.lucene.directory.FDBDirectoryManager;
import com.apple.foundationdb.record.lucene.directory.FDBDirectoryWrapper;
import com.apple.foundationdb.record.lucene.directory.PendingWriteQueue;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.common.FixedZeroKeyManager;
import com.apple.foundationdb.record.provider.common.RollingTestKeyManager;
import com.apple.foundationdb.record.provider.common.SerializationKeyManager;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreConcurrentTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainerState;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintenanceFilter;
import com.apple.foundationdb.record.provider.foundationdb.OnlineIndexer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.util.RandomSecretUtil;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.util.LoggableKeysAndValues;
import com.apple.test.ParameterizedTestUtils;
import com.apple.test.RandomizedTestUtils;
import com.apple.test.SuperSlow;
import com.apple.test.Tags;
import com.apple.test.TestConfigurationUtils;
import com.google.common.collect.Streams;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Lock;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.crypto.SecretKey;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.lucene.LuceneIndexOptions.INDEX_PARTITION_BY_FIELD_NAME;
import static com.apple.foundationdb.record.lucene.LuceneIndexOptions.INDEX_PARTITION_HIGH_WATERMARK;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.SIMPLE_TEXT_SUFFIXES;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests of the consistency of the Lucene Index.
 */
@Tag(Tags.RequiresFDB)
public class LuceneIndexMaintenanceTest extends FDBRecordStoreConcurrentTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(LuceneIndexMaintenanceTest.class);

    @BeforeEach
    void setUp() {
        fdb.setAsyncToSyncTimeout(waitEvent -> {
            if (waitEvent == FDBStoreTimer.Waits.WAIT_ONLINE_MERGE_INDEX) {
                return Duration.ofSeconds(60L);
            } else {
                return Duration.ofSeconds(7L);
            }
        });
    }

    static Stream<Arguments> configurationArguments() {
        // This has found situations that should have explicit tests:
        //      1. Multiple groups
        //      2. When the size of first partition is exactly highWatermark+repartitionCount
        return Stream.concat(
                Stream.of(
                        // there's not much special about which flags are enabled and the numbers are used, it's just
                        // to make sure we have some variety, and make sure we have a test with each boolean true, and
                        // false.
                        // For partitionHighWatermark vs repartitionCount it is important to have both an even factor,
                        // and not.
                        Arguments.of(true, false, false, 13, 3, 20, 9237590782644L),
                        Arguments.of(true, true, true, 10, 2, 23, -644766138635622644L),
                        Arguments.of(false, true, true, 11, 4, 20, -1089113174774589435L),
                        Arguments.of(false, false, false, 5, 1, 18, 6223372946177329440L),
                        Arguments.of(true, false, false, 14, 6, 0, 2451719304283565963L)),
                RandomizedTestUtils.randomArguments(random ->
                        Arguments.of(random.nextBoolean(),
                                random.nextBoolean(),
                                random.nextBoolean(),
                                random.nextInt(20) + 2,
                                random.nextInt(10) + 1,
                                0,
                                random.nextLong())));
    }

    @ParameterizedTest(name = "randomizedRepartitionTest({argumentsWithNames})")
    @MethodSource("configurationArguments")
    void randomizedRepartitionTest(boolean isGrouped,
                                   boolean isSynthetic,
                                   boolean primaryKeySegmentIndexEnabled,
                                   int partitionHighWatermark,
                                   int repartitionCount,
                                   int minDocumentCount,
                                   long seed) throws IOException {
        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(seed, this::getStoreBuilder, pathManager)
                .setIsGrouped(isGrouped)
                .setIsSynthetic(isSynthetic)
                .setPrimaryKeySegmentIndexEnabled(primaryKeySegmentIndexEnabled)
                .setPartitionHighWatermark(partitionHighWatermark)
                .build();

        LOGGER.info(KeyValueLogMessage.of("Running randomizedRepartitionTest",
                "dataModel", dataModel,
                "repartitionCount", repartitionCount,
                "seed", seed));

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, repartitionCount)
                .addProp(LuceneRecordContextProperties.LUCENE_MAX_DOCUMENTS_TO_MOVE_DURING_REPARTITIONING, dataModel.nextInt(1000) + repartitionCount)
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, (double)dataModel.nextInt(10) + 2) // it must be at least 2.0
                .build();

        dataModel.saveManyRecords(minDocumentCount, () -> openContext(contextProps), dataModel.nextInt(15) + 1);

        explicitMergeIndex(dataModel.index, contextProps, dataModel.schemaSetup);

        dataModel.validate(() -> openContext(contextProps));

        if (isGrouped) {
            validateDeleteWhere(isSynthetic, dataModel.groupingKeyToPrimaryKeyToPartitionKey, contextProps, dataModel.schemaSetup, dataModel.index);
        }
    }

    public static Stream<Arguments> savingInReverseDoesNotRequireRepartitioning() {
        return Stream.concat(Stream.of(true, false).flatMap(isGrouped ->
                        Stream.of(true, false).map(isSynthetic ->
                                Arguments.of(isGrouped, isSynthetic, 8, 1234098))),
                RandomizedTestUtils.randomArguments(random ->
                        Arguments.of(
                                random.nextBoolean(),
                                random.nextBoolean(),
                                random.nextInt(30) + 2,
                                random.nextLong()
                        )));
    }

    @ParameterizedTest
    @MethodSource
    void savingInReverseDoesNotRequireRepartitioning(boolean isGrouped,
                                                     boolean isSynthetic,
                                                     int partitionHighWatermark,
                                                     long seed) throws IOException {
        // We should create older partitions when the newer one is full, if adding a record that is older
        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(seed, this::getStoreBuilder, pathManager)
                .setIsGrouped(isGrouped)
                .setIsSynthetic(isSynthetic)
                .setPrimaryKeySegmentIndexEnabled(true)
                .setPartitionHighWatermark(partitionHighWatermark)
                .build();

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, (double)dataModel.nextInt(10) + 2) // it must be at least 2.0
                .build();

        try (FDBRecordContext context = openContext(contextProps)) {
            dataModel.saveRecordsToAllGroups(partitionHighWatermark, context);
            context.commit();
        }
        dataModel.validate(() -> openContext(contextProps));

        dataModel.getPartitionCounts(() -> openContext(contextProps)).forEach((groupingKey, partitionCounts) -> {
            assertThat(partitionCounts, Matchers.contains(partitionHighWatermark));
        });

        dataModel.setReverseSaveOrder(true);

        try (FDBRecordContext context = openContext(contextProps)) {
            dataModel.saveRecordsToAllGroups(partitionHighWatermark - 1, context);
            context.commit();
        }
        dataModel.validate(() -> openContext(contextProps));

        dataModel.getPartitionCounts(() -> openContext(contextProps)).forEach((groupingKey, partitionCounts) -> {
            assertThat(partitionCounts, Matchers.contains(partitionHighWatermark - 1, partitionHighWatermark));
        });

        if (isGrouped) {
            validateDeleteWhere(isSynthetic, dataModel.groupingKeyToPrimaryKeyToPartitionKey, contextProps, dataModel.schemaSetup, dataModel.index);
        }
    }

    static Stream<Arguments> manyDocumentsArgumentsSlow() {
        return Stream.concat(
                Stream.of(Arguments.of(true, true, true, true, false, 80, 2, 200, 234809),
                // I don't know why, but this took over an hour, I'm hoping my laptop slept, but I don't see it
                Arguments.of(false, true, false, true, false, 50, 8, 212, 3125111852333110588L)),
                RandomizedTestUtils.randomArguments(random ->
                        Arguments.of(random.nextBoolean(),
                                random.nextBoolean(),
                                random.nextBoolean(),
                                random.nextBoolean(),
                                random.nextBoolean(),
                                // We want to have a high partitionHighWatermark so that the underlying lucene indexes
                                // actually end up with many records, and so that we don't end up with a ton of partitions
                                random.nextInt(300) + 50,
                                random.nextInt(10) + 1,
                                random.nextInt(200) + 100,
                                random.nextLong())));
    }

    @ParameterizedTest
    @MethodSource("manyDocumentsArgumentsSlow")
    @SuperSlow
    void manyDocumentSlow(boolean isGrouped,
                          boolean isSynthetic,
                          boolean primaryKeySegmentIndexEnabled,
                          boolean compressed,
                          boolean encrypted,
                          int partitionHighWatermark,
                          int repartitionCount,
                          int loopCount,
                          long seed) throws IOException {
        manyDocuments(isGrouped, isSynthetic, primaryKeySegmentIndexEnabled, compressed, encrypted, partitionHighWatermark,
                repartitionCount, loopCount, 10, seed);
    }


    static Stream<Arguments> manyDocumentsArguments() {
        return Stream.concat(
                Stream.concat(
                        Stream.of(Arguments.of(true,  true,  true,  true, false, 20, 4, 50, 3, -644766138635622644L)),
                        TestConfigurationUtils.onlyNightly(
                                Stream.of(
                                        Arguments.of(true,  false, false, true, false, 21, 3, 55, 3, 9237590782644L),
                                        Arguments.of(false, true,  true,  true, false, 18, 3, 46, 3, -1089113174774589435L),
                                        Arguments.of(false, false, false, true, false, 24, 6, 59, 3, 6223372946177329440L),
                                        Arguments.of(true,  false, false, true, false, 27, 9, 48, 3, 2451719304283565963L)))),
                RandomizedTestUtils.randomArguments(random ->
                        Arguments.of(random.nextBoolean(),
                                random.nextBoolean(),
                                random.nextBoolean(),
                                random.nextBoolean(),
                                random.nextBoolean(),
                                // We want to have a high partitionHighWatermark so that the underlying lucene indexes
                                // actually end up with many records
                                random.nextInt(150) + 2,
                                random.nextInt(10) + 1,
                                random.nextInt(100) + 50,
                                3,
                                random.nextLong())));
    }

    @ParameterizedTest
    @MethodSource("manyDocumentsArguments")
    void manyDocuments(boolean isGrouped,
                       boolean isSynthetic,
                       boolean primaryKeySegmentIndexEnabled,
                       boolean compressed,
                       boolean encrypted,
                       int partitionHighWatermark,
                       int repartitionCount,
                       int loopCount,
                       int maxTransactionsPerLoop,
                       long seed) throws IOException {
        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(seed, this::getStoreBuilder, pathManager)
                .setIsGrouped(isGrouped)
                .setIsSynthetic(isSynthetic)
                .setPrimaryKeySegmentIndexEnabled(primaryKeySegmentIndexEnabled)
                .setPartitionHighWatermark(partitionHighWatermark)
                .build();

        LOGGER.info(KeyValueLogMessage.of("Running manyDocument",
                "dataModel", dataModel,
                "repartitionCount", repartitionCount,
                "seed", seed,
                "loopCount", loopCount));

        final RecordLayerPropertyStorage.Builder contextPropsBuilder = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, repartitionCount)
                .addProp(LuceneRecordContextProperties.LUCENE_MAX_DOCUMENTS_TO_MOVE_DURING_REPARTITIONING, dataModel.nextInt(1000) + repartitionCount)
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, (double)dataModel.nextInt(10) + 2) // it must be at least 2.0
                .addProp(LuceneRecordContextProperties.LUCENE_INDEX_COMPRESSION_ENABLED, compressed)
                .addProp(LuceneRecordContextProperties.LUCENE_INDEX_ENCRYPTION_ENABLED, encrypted)
                .addProp(LuceneRecordContextProperties.LUCENE_FIELD_PROTOBUF_PREFIX_ENABLED, encrypted);
        if (encrypted) {
            contextPropsBuilder.addProp(LuceneRecordContextProperties.LUCENE_INDEX_KEY_MANAGER, new RollingTestKeyManager(seed));
        }
        final RecordLayerPropertyStorage contextProps = contextPropsBuilder.build();
        for (int i = 0; i < loopCount; i++) {
            LOGGER.info(KeyValueLogMessage.of("ManyDocument loop",
                    "iteration", i,
                    "groupCount", dataModel.groupingKeyToPrimaryKeyToPartitionKey.size(),
                    "docCount", dataModel.groupingKeyToPrimaryKeyToPartitionKey.values().stream().mapToInt(Map::size).sum(),
                    "docMinPerGroup", dataModel.groupingKeyToPrimaryKeyToPartitionKey.values().stream().mapToInt(Map::size).min(),
                    "docMaxPerGroup", dataModel.groupingKeyToPrimaryKeyToPartitionKey.values().stream().mapToInt(Map::size).max()));
            dataModel.saveManyRecords(1, () -> openContext(contextProps), dataModel.nextInt(maxTransactionsPerLoop - 1) + 1);

            explicitMergeIndex(dataModel.index, contextProps, dataModel.schemaSetup);
        }

        dataModel.validate(() -> openContext(contextProps));

        if (isGrouped) {
            validateDeleteWhere(isSynthetic, dataModel.groupingKeyToPrimaryKeyToPartitionKey, contextProps, dataModel.schemaSetup, dataModel.index);
        }
    }


    static Stream<Arguments> flakyMergeArguments() {
        // because some of these require @SuperSlow, flakyMerge quick covers an example that we can comfortably put in
        // PRB
        return Stream.concat(
                // all of these permutations take multiple minutes, but probably are not all needed as part of
                // PRB, so put 3 fixed configuration that we know will fail merges in a variety of places into
                // the nightly build
                TestConfigurationUtils.onlyNightly(
                        Stream.of(
                                Arguments.of(true, false, false, 50, 9237590782644L, true),
                                Arguments.of(false, true, true, 33, -1089113174774589435L, true),
                                Arguments.of(false, false, false, 35, 6223372946177329440L, true))
                ),
                RandomizedTestUtils.randomArguments(random ->
                        Arguments.of(random.nextBoolean(), // isGrouped
                                random.nextBoolean(), // isSynthetic
                                random.nextBoolean(), // primaryKeySegmentIndexEnabled
                                random.nextInt(40) + 2, // minDocumentCount
                                random.nextLong(), // seed for other randomness
                                false))); // require failure
    }

    @Test
    @Tag(Tags.Slow)
    void flakyMergeQuick() throws IOException {
        flakyMerge(true, true, true, 31, -644766138635622644L, true);
    }

    /**
     * Test that the index is in a good state if the merge operation has errors.
     * The test will validate that if Lucene merge fails randomly in the middle it will still be usable, and
     * not corrupted. Having retries in the IndexingMerger means that it could heal, but we want to make
     * sure that requests coming in while the merge is ongoing don't get a corrupted view.
     *
     * @param isGrouped whether the index has a grouping key
     * @param isSynthetic whether the index is on a synthetic type
     * @param primaryKeySegmentIndexEnabled whether to enable the primaryKeySegmentIndex
     * @param minDocumentCount the minimum document count required for each group
     * @param seed seed used for extra, less important randomness
     * @param requireFailure whether it is expected that the merge will fail. Useful for ensuring tha PRBs actually
     * reproduce the issue, but hard to guarantee for randomly generated parameters
     * @throws IOException from lucene, probably
     */
    @ParameterizedTest(name = "flakyMerge({argumentsWithNames})")
    @MethodSource("flakyMergeArguments")
    @SuperSlow
    void flakyMerge(boolean isGrouped,
                    boolean isSynthetic,
                    boolean primaryKeySegmentIndexEnabled,
                    int minDocumentCount,
                    long seed,
                    boolean requireFailure) throws IOException {

        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(seed, this::getStoreBuilder, pathManager)
                .setIsGrouped(isGrouped)
                .setIsSynthetic(isSynthetic)
                .setPrimaryKeySegmentIndexEnabled(primaryKeySegmentIndexEnabled)
                .setPartitionHighWatermark(Integer.MAX_VALUE) // no partitioning
                .build();
        LOGGER.info(KeyValueLogMessage.of("Running flakyMerge test",
                "dataModel", dataModel,
                "seed", seed));

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, 2.0)
                .addProp(LuceneRecordContextProperties.LUCENE_AGILE_COMMIT_TIME_QUOTA, 1) // commit as often as possible
                .addProp(LuceneRecordContextProperties.LUCENE_AGILE_COMMIT_SIZE_QUOTA, 1) // commit as often as possible
                .addProp(LuceneRecordContextProperties.LUCENE_FILE_LOCK_TIME_WINDOW_MILLISECONDS, (int)TimeUnit.SECONDS.toMillis(10) + 1) // TODO figure out how to fix this
                .build();

        // Generate random documents
        final int transactionCount = dataModel.nextInt(15) + 10;
        dataModel.saveManyRecords(minDocumentCount, () -> openContext(contextProps), transactionCount);

        final Function<StoreTimer.Wait, Duration> oldAsyncToSyncTimeout = fdb.getAsyncToSyncTimeout();
        AtomicInteger waitCounts = new AtomicInteger();
        try {
            final Function<StoreTimer.Wait, Duration> asyncToSyncTimeout = (wait) -> {
                if (wait.getClass().equals(LuceneEvents.Waits.class) &&
                        // don't have the timeout on FILE_LOCK_CLEAR because that will leave the file lock around,
                        // and the next iteration will fail on that.
                        wait != LuceneEvents.Waits.WAIT_LUCENE_FILE_LOCK_CLEAR &&
                        // if we timeout on setting, AgilityContext may commit in the background, but Lucene won't have
                        // the Lock reference to close, and clear the lock.
                        wait != LuceneEvents.Waits.WAIT_LUCENE_FILE_LOCK_SET &&
                        waitCounts.getAndDecrement() == 0) {

                    return Duration.ofNanos(1L);
                } else {
                    return oldAsyncToSyncTimeout == null ? Duration.ofDays(1L) : oldAsyncToSyncTimeout.apply(wait);
                }
            };
            for (int i = 0; i < 20; i++) {
                fdb.setAsyncToSyncTimeout(asyncToSyncTimeout);
                waitCounts.set(i);
                boolean success = false;
                try {
                    LOGGER.info(KeyValueLogMessage.of("Merge started",
                            "iteration", i));
                    // To avoid merge retries, use the low level merge from the index maintainer
                    try (FDBRecordContext context = openContext(contextProps)) {
                        FDBRecordStore recordStore = Objects.requireNonNull(dataModel.schemaSetup.apply(context));
                        final CompletableFuture<Void> future = recordStore.getIndexMaintainer(Objects.requireNonNull(dataModel.index)).mergeIndex();
                        fdb.asyncToSync(timer, FDBStoreTimer.Waits.WAIT_ONLINE_MERGE_INDEX, future);
                    }
                    LOGGER.info(KeyValueLogMessage.of("Merge completed",
                            "iteration", i));
                    assertFalse(requireFailure && i < 15, i + " merge should have failed");
                    success = true;
                } catch (RecordCoreException e) {
                    final LoggableKeysAndValues<? extends Exception> timeoutException = findTimeoutException(e);
                    LOGGER.info(KeyValueLogMessage.of("Merge failed",
                            "iteration", i,
                            "cause", e.getClass(),
                            "message", e.getMessage(),
                            "timeout", timeoutException != null));
                    if (timeoutException == null) {
                        throw e;
                    }
                    assertEquals(1L, timeoutException.getLogInfo().get(LogMessageKeys.TIME_LIMIT.toString()), i + " " + e.getMessage());
                    assertEquals(TimeUnit.NANOSECONDS, timeoutException.getLogInfo().get(LogMessageKeys.TIME_UNIT.toString()), i + " " + e.getMessage());
                }
                fdb.setAsyncToSyncTimeout(oldAsyncToSyncTimeout);
                dbExtension.checkForOpenContexts(); // validate after every loop that we didn't leave any contexts open
                LOGGER.debug(KeyValueLogMessage.of("Validating",
                        "iteration", i));
                new LuceneIndexTestValidator(() -> openContext(contextProps), context -> Objects.requireNonNull(dataModel.schemaSetup.apply(context)))
                        .validate(dataModel.index, dataModel.groupingKeyToPrimaryKeyToPartitionKey, isSynthetic ? "child_str_value:forth" : "text_value:about", !success);
                LOGGER.debug(KeyValueLogMessage.of("Done Validating",
                        "iteration", i));
                dbExtension.checkForOpenContexts(); // just in case the validation code leaks a context
            }
        } finally {
            fdb.setAsyncToSyncTimeout(oldAsyncToSyncTimeout);
            if (LOGGER.isDebugEnabled()) {
                dataModel.groupingKeyToPrimaryKeyToPartitionKey.entrySet().stream().sorted(Map.Entry.comparingByKey())
                        .forEach(entry -> LOGGER.debug(entry.getKey() + ": " + entry.getValue().keySet()));
            }
        }
    }

    // Lock a directory, and commit, then try to update a record, or save a record, or do a search, and assert that nothing
    // is corrupted (or that the user request fails)
    @Test
    void lockCommitThenValidateTest() throws IOException {
        final Map<String, String> options = Map.of(
                INDEX_PARTITION_BY_FIELD_NAME, "timestamp",
                INDEX_PARTITION_HIGH_WATERMARK, String.valueOf(8));
        Index index = complexPartitionedIndex(options);
        final KeySpacePath path = pathManager.createPath(TestKeySpace.RECORD_STORE);
        Function<FDBRecordContext, FDBRecordStore> schemaSetup = context ->
                LuceneIndexTestUtils.rebuildIndexMetaData(context, path, TestRecordsTextProto.ComplexDocument.getDescriptor().getName(), index, useCascadesPlanner).getLeft();

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 8)
                .build();

        long timestamp = System.currentTimeMillis();
        Map<Tuple, Map<Tuple, Tuple>> insertedDocs = new HashMap<>();
        // save a record
        createComplexRecords(1, insertedDocs, contextProps, schemaSetup);

        // explicitly lock directory then commit
        try (FDBRecordContext context = openContext(contextProps)) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));

            // low-level lock the directory:
            // subspace for (group 1, partition data subspace, partition 0, file lock subspace)
            Subspace subspace = recordStore.indexSubspace(index).subspace(Tuple.from(1, LucenePartitioner.PARTITION_DATA_SUBSPACE, 0, FDBDirectory.FILE_LOCK_SUBSPACE));
            byte[] fileLockKey = subspace.pack(Tuple.from("write.lock"));
            FDBDirectoryLockFactory lockFactory = new FDBDirectoryLockFactory(null, 10_000);

            Lock testLock = lockFactory.obtainLock(new AgilityContext.NonAgile(context), fileLockKey, "write.lock");
            testLock.ensureValid();
            commit(context);
        }

        // search should work
        try (FDBRecordContext context = openContext(contextProps)) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));

            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(
                    index,
                    LuceneIndexTestValidator.groupedSortedTextSearch(recordStore, index, "text:word", null, 1), null, ScanProperties.FORWARD_SCAN)) {
                List<Tuple> primaryKeys = LuceneConcurrency.asyncToSync(FDBStoreTimer.Waits.WAIT_ADVANCE_CURSOR, cursor.asList(), context)
                        .stream()
                        .map(IndexEntry::getPrimaryKey)
                        .collect(Collectors.toList());
                assertEquals(1, primaryKeys.size());
                assertEquals(Tuple.from(1, 1000L), primaryKeys.get(0));
            }
        }

        // create another record, this should fail
        try (FDBRecordContext context = openContext(contextProps)) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));

            TestRecordsTextProto.ComplexDocument cd = TestRecordsTextProto.ComplexDocument.newBuilder()
                    .setGroup(1)
                    .setDocId(2000L)
                    .setIsSeen(true)
                    .setText("A word about what I want to say")
                    .setTimestamp(timestamp + 2000)
                    .setHeader(TestRecordsTextProto.ComplexDocument.Header.newBuilder().setHeaderId(1999L))
                    .build();
            assertThrows(RecordCoreException.class, () -> recordStore.saveRecord(cd), "Lock failed: already locked by another entity");
        }

        // validate index
        new LuceneIndexTestValidator(() -> openContext(contextProps), context -> Objects.requireNonNull(schemaSetup.apply(context)))
                .validate(index, insertedDocs, "text:about", false);
    }


    // A chaos test of two threads, one constantly trying to merge, and one trying to update a record, or save a new record or do a search.
    // At the end the index should be validated for consistency.
    @Test
    void chaosMergeAndUpdateTest() throws InterruptedException, IOException {
        final Map<String, String> options = Map.of(
                INDEX_PARTITION_BY_FIELD_NAME, "timestamp",
                INDEX_PARTITION_HIGH_WATERMARK, String.valueOf(100));
        Index index = complexPartitionedIndex(options);
        final KeySpacePath path = pathManager.createPath(TestKeySpace.RECORD_STORE);
        Function<FDBRecordContext, FDBRecordStore> schemaSetup = context ->
                LuceneIndexTestUtils.rebuildIndexMetaData(context, path, TestRecordsTextProto.ComplexDocument.getDescriptor().getName(), index, useCascadesPlanner).getLeft();

        assertNotNull(index);

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 8)
                .build();

        long timestamp = System.currentTimeMillis();
        Map<Tuple, Map<Tuple, Tuple>> insertedDocs = new HashMap<>();
        // Use a queue with a capacity of 1 to help ensure that saves are actually occurring between merge attempts
        // and it's not just a loop of merges concluding that there is no merge to be done
        BlockingQueue<Boolean> mergeQueue = new ArrayBlockingQueue<>(1);
        AtomicInteger successfulMerges = new AtomicInteger();
        AtomicInteger merges = new AtomicInteger();
        AtomicInteger docCount = new AtomicInteger();
        AtomicInteger conflicts = new AtomicInteger();
        AtomicReference<Throwable> failedInsert = new AtomicReference<>();
        AtomicReference<Throwable> failedMerge = new AtomicReference<>();
        Thread inserter = new Thread(() -> {
            try {
                int i = 0;
                while (docCount.get() < 200) {
                    // create a record then query
                    try (FDBRecordContext context = openContext(contextProps)) {
                        FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
                        recordStore.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(false);
                        TestRecordsTextProto.ComplexDocument cd = TestRecordsTextProto.ComplexDocument.newBuilder()
                                .setGroup(1)
                                .setDocId(i + 1000L)
                                .setIsSeen(true)
                                .setText("A word about what I want to say")
                                .setTimestamp(timestamp + i)
                                .setHeader(TestRecordsTextProto.ComplexDocument.Header.newBuilder().setHeaderId(1000L - i))
                                .build();
                        try {
                            final Tuple primaryKey = recordStore.saveRecord(cd).getPrimaryKey();

                            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(
                                    index,
                                    LuceneIndexTestValidator.groupedSortedTextSearch(recordStore, index, "text:word", null, 1), null, ScanProperties.FORWARD_SCAN)) {
                                List<IndexEntry> matches = LuceneConcurrency.asyncToSync(FDBStoreTimer.Waits.WAIT_ADVANCE_CURSOR, cursor.asList(), context);
                                assertFalse(matches.isEmpty());
                            }

                            commit(context);
                            i++;
                            insertedDocs.computeIfAbsent(Tuple.from(1), k -> new HashMap<>()).put(primaryKey, Tuple.from(timestamp + i));
                            docCount.incrementAndGet();
                            mergeQueue.offer(true);
                        } catch (Exception e) {
                            if (Thread.currentThread().isInterrupted()) {
                                return;
                            }
                            if (e instanceof FDBExceptions.FDBStoreTransactionConflictException) {
                                conflicts.incrementAndGet();
                            } else if (e instanceof FDBExceptions.FDBStoreLockTakenException) {
                                // do nothing
                            } else {
                                LOGGER.debug("Failing: couldn't commit for key {}", (1000L + i), e);
                                failedInsert.set(e);
                                return;
                            }
                            // commit failed due to conflict with other thread. Continue trying to create docs.
                            LOGGER.debug("Ignoring: couldn't commit for key {} due to {}", (1000L + i), e.getClass().getSimpleName());
                        }
                    }
                }
            } finally {
                try {
                    mergeQueue.put(false);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        Thread merger = new Thread(() -> {
            // busy merge
            int i = 0;
            // start a merge after each save completes
            try {
                while (mergeQueue.take()) {
                    i++;
                    try {
                        merges.incrementAndGet();
                        explicitMergeIndex(index, contextProps, schemaSetup);
                        successfulMerges.incrementAndGet();
                    } catch (Exception e) {
                        LOGGER.debug("Merging: failed {}", i);
                        // Only record the first failure, but keeping processing merges, and, importantly, popping off
                        // the queue
                        failedMerge.compareAndSet(null, e);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        inserter.start();
        merger.start();

        inserter.join();
        assertNull(failedInsert.get());
        // This could happen if the cluster is down, or something that otherwise prevents us from inserting any documents
        // by asserting that there are documents we prevent the `join` below from waiting forever.
        if (insertedDocs.isEmpty()) {
            merger.interrupt();
        }
        assertThat(insertedDocs, Matchers.not(Matchers.anEmptyMap()));
        merger.join();
        assertNull(failedMerge.get());
        assertThat(successfulMerges.get(), Matchers.greaterThan(10));
        assertThat(conflicts.get(), Matchers.greaterThan(10));
        assertThat(docCount.get(), Matchers.greaterThanOrEqualTo(200));

        // validate index is sane
        new LuceneIndexTestValidator(() -> openContext(contextProps), context -> Objects.requireNonNull(schemaSetup.apply(context)))
                .validate(index, insertedDocs, "text:about", false);
    }

    @Test
    void testPendingQueueSimple() {
        // Test simple non-partitioned ongoing merge indicator life cycle
        final Index index = SIMPLE_TEXT_SUFFIXES;
        final KeySpacePath path = pathManager.createPath(TestKeySpace.RECORD_STORE);
        final Function<FDBRecordContext, FDBRecordStore> schemaSetup = context ->
                LuceneIndexTestUtils.rebuildIndexMetaData(context, path,
                        TestRecordsTextProto.SimpleDocument.getDescriptor().getName(),
                        index, useCascadesPlanner).getLeft();

        // Set merge indicator
        pendingQueueSetPendingQueueIndicator(schemaSetup, index, null, null);

        // Write a record
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            recordStore.saveRecord(
                    LuceneIndexTestUtils.createSimpleDocument(1001L, "test document", 1)
            );
            commit(context);
        }

        // Verify record is in queue
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));

            // Verify entry is in pending queue
            IndexMaintainerState state = new IndexMaintainerState(recordStore, index,
                    recordStore.getIndexMaintenanceFilter());
            FDBDirectoryManager directoryManager = FDBDirectoryManager.getManager(state);
            FDBDirectory directory = directoryManager.getDirectory(null, null);

            assertTrue(directory.shouldUseQueue(),
                    "Merge indicator should still be true");

            PendingWriteQueue queue = directory.createPendingWritesQueue();

            List<PendingWriteQueue.QueueEntry> queueEntries = new ArrayList<>();
            RecordCursor<PendingWriteQueue.QueueEntry> queueCursor = queue.getQueueCursor(
                    context, ScanProperties.FORWARD_SCAN, null);
            queueCursor.forEach(queueEntries::add).join();

            assertEquals(1, queueEntries.size(), "Queue should have exactly 1 entry");

            PendingWriteQueue.QueueEntry entry = queueEntries.get(0);
            assertEquals(LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT,
                    entry.getOperationType(), "Entry should be INSERT operation");
            commit(context);
        }

        // call merge - this should call drain and remove the ongoing merge indicator
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            final LuceneIndexMaintainer indexMaintainer = getIndexMaintainer(recordStore, index);
            indexMaintainer.mergeIndex().join();
            commit(context);
        }

        // Verify empty queue
        pendingQueueVerifyClearQueueAndIndicator(schemaSetup, index, null, null);
    }

    @Test
    void testPendingQueueSimpleWithDelete() {
        // Test pending queue with both INSERT and DELETE operations
        final Index index = SIMPLE_TEXT_SUFFIXES;
        final KeySpacePath path = pathManager.createPath(TestKeySpace.RECORD_STORE);
        final Function<FDBRecordContext, FDBRecordStore> schemaSetup = context ->
                LuceneIndexTestUtils.rebuildIndexMetaData(context, path,
                        TestRecordsTextProto.SimpleDocument.getDescriptor().getName(),
                        index, useCascadesPlanner).getLeft();

        // Set queue indicator
        pendingQueueSetPendingQueueIndicator(schemaSetup, index, null, null);

        // Write multiple records
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            recordStore.saveRecord(
                    LuceneIndexTestUtils.createSimpleDocument(1001L, "first document", 1)
            );
            recordStore.saveRecord(
                    LuceneIndexTestUtils.createSimpleDocument(1002L, "second document", 1)
            );
            commit(context);
        }

        // Delete one of the records
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            recordStore.deleteRecord(Tuple.from(1001L));
            commit(context);
        }

        // Verify records are in queue (2 INSERTs + 1 DELETE)
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));

            IndexMaintainerState state = new IndexMaintainerState(recordStore, index,
                    recordStore.getIndexMaintenanceFilter());
            FDBDirectoryManager directoryManager = FDBDirectoryManager.getManager(state);
            FDBDirectory directory = directoryManager.getDirectory(null, null);

            assertTrue(directory.shouldUseQueue(),
                    "Merge indicator should still be true");

            PendingWriteQueue queue = directory.createPendingWritesQueue();

            List<PendingWriteQueue.QueueEntry> queueEntries = new ArrayList<>();
            RecordCursor<PendingWriteQueue.QueueEntry> queueCursor = queue.getQueueCursor(
                    context, ScanProperties.FORWARD_SCAN, null);
            queueCursor.forEach(queueEntries::add).join();

            assertEquals(3, queueEntries.size(), "Queue should have exactly 3 entries");

            // Verify operation types - first two should be INSERTs
            assertEquals(LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT,
                    queueEntries.get(0).getOperationType(),
                    "First entry should be INSERT operation");
            assertEquals(LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT,
                    queueEntries.get(1).getOperationType(),
                    "Second entry should be INSERT operation");

            // Third should be DELETE
            assertEquals(LucenePendingWriteQueueProto.PendingWriteItem.OperationType.DELETE,
                    queueEntries.get(2).getOperationType(),
                    "Third entry should be DELETE operation");

            commit(context);
        }

        // Call merge - this should drain the queue and remove the queue indicator
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            final LuceneIndexMaintainer indexMaintainer = getIndexMaintainer(recordStore, index);
            indexMaintainer.mergeIndex().join();
            commit(context);
        }

        // Verify empty queue and correct final state
        pendingQueueVerifyClearQueueAndIndicator(schemaSetup, index, null, null);
    }

    @Test
    void pendingQueueTestMultiplePartitions() {
        // Test pending queue with partitioned index - documents in different partitions
        final Map<String, String> options = Map.of(
                INDEX_PARTITION_BY_FIELD_NAME, "timestamp",
                INDEX_PARTITION_HIGH_WATERMARK, String.valueOf(100));

        final Index index = complexPartitionedIndex(options);
        final KeySpacePath path = pathManager.createPath(TestKeySpace.RECORD_STORE);
        final Function<FDBRecordContext, FDBRecordStore> schemaSetup = context ->
                LuceneIndexTestUtils.rebuildIndexMetaData(context, path,
                        TestRecordsTextProto.ComplexDocument.getDescriptor().getName(),
                        index, useCascadesPlanner).getLeft();

        // Insert a few documents when "ongoing merge indicator" is clear.
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));

            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(2001L, "document foo", 1L, 30L));
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(2002L, "document bar", 1L, 130L));
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(2003L, "document zoo", 1L, 35L));
            commit(context);
        }

        final Tuple groupingKey = Tuple.from(1L);  // All documents in group 1
        final Integer partition0 = 0;  // timestamp < 100
        final Integer partition1 = 1;  // timestamp 100-199

        // Enable queue mode for both partitions
        pendingQueueSetPendingQueueIndicator(schemaSetup, index, groupingKey, partition0);

        // Store primary keys for later deletion
        Tuple primaryKey1;

        // Insert documents when "ongoing merge indicator" is set.
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));

            primaryKey1 = recordStore.saveRecord(
                    LuceneIndexTestUtils.createComplexDocument(1001L, "first document", 1L, 50L)
            ).getPrimaryKey();
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1002L, "second document", 1L, 150L));
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1003L, "third document", 1L, 75L));
            commit(context);
        }

        // Delete one document from partition 0
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            recordStore.deleteRecord(primaryKey1);
            commit(context);
        }

        // Verify queues in both partitions
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            IndexMaintainerState state = new IndexMaintainerState(recordStore, index,
                    recordStore.getIndexMaintenanceFilter());
            FDBDirectoryManager directoryManager = FDBDirectoryManager.getManager(state);

            // Verify partition 1 queue: empty
            FDBDirectory dir1 = directoryManager.getDirectory(groupingKey, partition1);
            assertFalse(dir1.shouldUseQueue(), "Partition 1 should not use queue");

            PendingWriteQueue queue1 = dir1.createPendingWritesQueue();
            List<PendingWriteQueue.QueueEntry> entries1 = new ArrayList<>();
            queue1.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null)
                    .forEach(entries1::add).join();

            assertEquals(0, entries1.size(), "Partition 1 should have no entries");

            // Verify partition 0 queue: 3 INSERTs + 1 DELETE
            FDBDirectory dir0 = directoryManager.getDirectory(groupingKey, partition0);
            assertTrue(dir0.shouldUseQueue(), "Partition 0 should use queue");

            PendingWriteQueue queue0 = dir0.createPendingWritesQueue();
            List<PendingWriteQueue.QueueEntry> entries0 = new ArrayList<>();
            queue0.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null)
                    .forEach(entries0::add).join();

            assertEquals(4, entries0.size(), "Partition 0 should have 4 entries");
            assertEquals(LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT,
                    entries0.get(0).getOperationType());
            assertEquals(LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT,
                    entries0.get(1).getOperationType());
            assertEquals(LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT,
                    entries0.get(2).getOperationType());
            assertEquals(LucenePendingWriteQueueProto.PendingWriteItem.OperationType.DELETE,
                    entries0.get(3).getOperationType());

            commit(context);
        }

        // Merge index - drains all partition queues
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            final LuceneIndexMaintainer indexMaintainer = getIndexMaintainer(recordStore, index);
            // unlike real life, this test sets the "ongoing merge indicator" ahead of the real merge. Merge allows
            // the indicator being set as a way to recover from a leftover set indicator - which also enables this test.
            indexMaintainer.mergeIndex().join();
            commit(context);
        }

        // Verify both queues are empty and indicators are cleared
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            IndexMaintainerState state = new IndexMaintainerState(recordStore, index,
                    recordStore.getIndexMaintenanceFilter());
            FDBDirectoryManager directoryManager = FDBDirectoryManager.getManager(state);

            // Check partition 0
            FDBDirectory dir0 = directoryManager.getDirectory(groupingKey, partition0);
            assertFalse(dir0.shouldUseQueue(), "Partition 0 queue indicator should be false");

            PendingWriteQueue queue0 = dir0.createPendingWritesQueue();
            List<PendingWriteQueue.QueueEntry> entries0 = new ArrayList<>();
            queue0.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null)
                    .forEach(entries0::add).join();
            assertEquals(0, entries0.size(), "Partition 0 queue should be empty");

            // Check partition 1
            FDBDirectory dir1 = directoryManager.getDirectory(groupingKey, partition1);
            assertFalse(dir1.shouldUseQueue(), "Partition 1 queue indicator should be false");

            PendingWriteQueue queue1 = dir1.createPendingWritesQueue();
            List<PendingWriteQueue.QueueEntry> entries1 = new ArrayList<>();
            queue1.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null)
                    .forEach(entries1::add).join();
            assertEquals(0, entries1.size(), "Partition 1 queue should be empty");

            commit(context);
        }
    }

    @Test
    void testPendingQueueWithUpdate() {
        // Test UPDATE operation in pending queue
        final Index index = SIMPLE_TEXT_SUFFIXES;
        final KeySpacePath path = pathManager.createPath(TestKeySpace.RECORD_STORE);
        final Function<FDBRecordContext, FDBRecordStore> schemaSetup = context ->
                LuceneIndexTestUtils.rebuildIndexMetaData(context, path,
                        TestRecordsTextProto.SimpleDocument.getDescriptor().getName(),
                        index, useCascadesPlanner).getLeft();

        // Insert document before queue is enabled
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            recordStore.saveRecord(
                    LuceneIndexTestUtils.createSimpleDocument(1001L, "original text", 1)
            );
            commit(context);
        }

        // Mark ongoing merge
        pendingQueueSetPendingQueueIndicator(schemaSetup, index, null, null);

        // Update the document
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            recordStore.saveRecord(
                    LuceneIndexTestUtils.createSimpleDocument(1001L, "updated text", 1)
            );
            commit(context);
        }

        // Verify UPDATE and DELETE in queue
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            IndexMaintainerState state = new IndexMaintainerState(recordStore, index,
                    recordStore.getIndexMaintenanceFilter());
            FDBDirectoryManager directoryManager = FDBDirectoryManager.getManager(state);
            FDBDirectory directory = directoryManager.getDirectory(null, null);

            PendingWriteQueue queue = directory.createPendingWritesQueue();
            List<PendingWriteQueue.QueueEntry> entries = new ArrayList<>();
            queue.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null)
                    .forEach(entries::add).join();

            assertEquals(2, entries.size(), "Queue should have 1 UPDATE entry");
            assertEquals(LucenePendingWriteQueueProto.PendingWriteItem.OperationType.DELETE,
                    entries.get(0).getOperationType());
            assertEquals(LucenePendingWriteQueueProto.PendingWriteItem.OperationType.UPDATE,
                    entries.get(1).getOperationType());
            commit(context);
        }

        // Merge and verify
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            final LuceneIndexMaintainer indexMaintainer = getIndexMaintainer(recordStore, index);
            indexMaintainer.mergeIndex().join();
            commit(context);
        }

        // Verify queue cleared
        pendingQueueVerifyClearQueueAndIndicator(schemaSetup, index, null, null);
    }

    @Test
    void testPendingQueueMixedOperations() {
        // Test INSERT, UPDATE, DELETE sequence on same document
        final Index index = SIMPLE_TEXT_SUFFIXES;
        final KeySpacePath path = pathManager.createPath(TestKeySpace.RECORD_STORE);
        final Function<FDBRecordContext, FDBRecordStore> schemaSetup = context ->
                LuceneIndexTestUtils.rebuildIndexMetaData(context, path,
                        TestRecordsTextProto.SimpleDocument.getDescriptor().getName(),
                        index, useCascadesPlanner).getLeft();

        // Mark ongoing merge
        pendingQueueSetPendingQueueIndicator(schemaSetup, index, null, null);

        // INSERT
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            recordStore.saveRecord(LuceneIndexTestUtils.createSimpleDocument(1001L, "version 1", 1));
            commit(context);
        }

        // UPDATE (generates DELETE + INSERT since doc was inserted while queue was active)
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            recordStore.saveRecord(LuceneIndexTestUtils.createSimpleDocument(1001L, "version 2", 1));
            commit(context);
        }

        // DELETE
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            recordStore.deleteRecord(Tuple.from(1001L));
            commit(context);
        }

        // Verify INSERT, DELETE, INSERT, DELETE in queue
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            IndexMaintainerState state = new IndexMaintainerState(recordStore, index,
                    recordStore.getIndexMaintenanceFilter());
            FDBDirectoryManager directoryManager = FDBDirectoryManager.getManager(state);
            FDBDirectory directory = directoryManager.getDirectory(null, null);

            PendingWriteQueue queue = directory.createPendingWritesQueue();
            List<PendingWriteQueue.QueueEntry> entries = new ArrayList<>();
            queue.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null)
                    .forEach(entries::add).join();

            assertEquals(4, entries.size(), "Queue should have 4 entries");
            assertEquals(LucenePendingWriteQueueProto.PendingWriteItem.OperationType.INSERT,
                    entries.get(0).getOperationType(), "Entry 0: INSERT");
            assertEquals(LucenePendingWriteQueueProto.PendingWriteItem.OperationType.DELETE,
                    entries.get(1).getOperationType(), "Entry 1: DELETE from update");
            assertEquals(LucenePendingWriteQueueProto.PendingWriteItem.OperationType.UPDATE,
                    entries.get(2).getOperationType(), "Entry 2: INSERT from update");
            assertEquals(LucenePendingWriteQueueProto.PendingWriteItem.OperationType.DELETE,
                    entries.get(3).getOperationType(), "Entry 3: DELETE");
            commit(context);
        }

        // Merge
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            final LuceneIndexMaintainer indexMaintainer = getIndexMaintainer(recordStore, index);
            indexMaintainer.mergeIndex().join();
            commit(context);
        }

        // Verify queue cleared
        pendingQueueVerifyClearQueueAndIndicator(schemaSetup, index, null, null);
    }

    private void pendingQueueVerifyClearQueueAndIndicator(Function<FDBRecordContext, FDBRecordStore> schemaSetup, Index index, @Nullable Tuple groupingKey, @Nullable Integer partitionId) {
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            IndexMaintainerState state = new IndexMaintainerState(recordStore, index,
                    recordStore.getIndexMaintenanceFilter());
            FDBDirectoryManager directoryManager = FDBDirectoryManager.getManager(state);
            FDBDirectory directory = directoryManager.getDirectory(groupingKey, partitionId);

            assertFalse(directory.shouldUseQueue());

            PendingWriteQueue queue = directory.createPendingWritesQueue();
            List<PendingWriteQueue.QueueEntry> entries = new ArrayList<>();
            queue.getQueueCursor(context, ScanProperties.FORWARD_SCAN, null)
                    .forEach(entries::add).join();
            assertEquals(0, entries.size());
            commit(context);
        }
    }

    private void pendingQueueSetPendingQueueIndicator(Function<FDBRecordContext, FDBRecordStore> schemaSetup, Index index, @Nullable Tuple groupingKey, @Nullable Integer partitionId) {
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));

            // Get directory and set queue indicator
            IndexMaintainerState state = new IndexMaintainerState(recordStore, index,
                    recordStore.getIndexMaintenanceFilter());
            FDBDirectoryManager directoryManager = FDBDirectoryManager.getManager(state);
            FDBDirectory directory = directoryManager.getDirectory(groupingKey, partitionId);
            directory.setPendingQueueIndicator();
            commit(context);
        }
    }

    // A test where there are multiple threads trying to do merges. At the end the index should be validated for consistency.
    @Test
    void multipleConcurrentMergesTest() throws IOException, InterruptedException {
        final Map<String, String> options = Map.of(
                INDEX_PARTITION_BY_FIELD_NAME, "timestamp",
                INDEX_PARTITION_HIGH_WATERMARK, String.valueOf(100));

        Index index = complexPartitionedIndex(options);
        final KeySpacePath path = pathManager.createPath(TestKeySpace.RECORD_STORE);
        Function<FDBRecordContext, FDBRecordStore> schemaSetup = context ->
                LuceneIndexTestUtils.rebuildIndexMetaData(context, path, TestRecordsTextProto.ComplexDocument.getDescriptor().getName(), index, useCascadesPlanner).getLeft();
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 8)
                .build();

        final int countReps = 20;
        final int threadCount = 10;

        Map<Tuple, Map<Tuple, Tuple>> insertedDocs = new HashMap<>();

        // create a bunch of docs
        createComplexRecords(countReps, insertedDocs, contextProps, schemaSetup);

        final CountDownLatch readyToMerge = new CountDownLatch(1);
        final CountDownLatch doneMerging = new CountDownLatch(threadCount);

        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    readyToMerge.await();
                    explicitMergeIndex(index, contextProps, schemaSetup);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneMerging.countDown();
                }
            }).start();
        }
        readyToMerge.countDown();
        doneMerging.await();

        new LuceneIndexTestValidator(() -> openContext(contextProps), context -> Objects.requireNonNull(schemaSetup.apply(context)))
                .validate(index, insertedDocs, "text:about", false);
    }

    static Stream<Arguments> mergeLosesLockTest() {
        return Stream.concat(
                Stream.of( 65).map(Arguments::of), // fixed 65% lock failure rate
                RandomizedTestUtils.randomArguments(random -> Arguments.of(random.nextInt(100) + 1))); //  1-100%
    }

    // A test of what lucene does when a merge loses its lock
    @ParameterizedTest
    @MethodSource
    void mergeLosesLockTest(int failurePercentage) throws IOException {
        final Map<String, String> options = Map.of(
                INDEX_PARTITION_BY_FIELD_NAME, "timestamp",
                INDEX_PARTITION_HIGH_WATERMARK, String.valueOf(200));
        Index index = complexPartitionedIndex(options);
        final KeySpacePath path = pathManager.createPath(TestKeySpace.RECORD_STORE);
        Function<FDBRecordContext, FDBRecordStore> schemaSetup = context ->
                LuceneIndexTestUtils.rebuildIndexMetaData(context, path, TestRecordsTextProto.ComplexDocument.getDescriptor().getName(), index, useCascadesPlanner).getLeft();

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 8)
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, 2.0)
                .addProp(LuceneRecordContextProperties.LUCENE_AGILE_COMMIT_TIME_QUOTA, 1) // 1ms
                .build();

        final int docCount = 100;
        Map<Tuple, Map<Tuple, Tuple>> insertedDocs = new HashMap<>();
        // create a bunch of docs
        createComplexRecords(docCount, insertedDocs, contextProps, schemaSetup);

        // try a couple of times
        for (int l = 0; l < 2; l++) {
            try (FDBRecordContext context = openContext(contextProps)) {
                FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));

                // directory key for group 1/partition 0
                Tuple directoryKey = Tuple.from(1, LucenePartitioner.PARTITION_DATA_SUBSPACE, 0);
                IndexMaintainerState state = new IndexMaintainerState(recordStore, index, IndexMaintenanceFilter.NORMAL);

                final var fieldInfos = LuceneIndexExpressions.getDocumentFieldDerivations(state.index, state.store.getRecordMetaData());
                LuceneAnalyzerCombinationProvider indexAnalyzerSelector = LuceneAnalyzerRegistryImpl.instance()
                        .getLuceneAnalyzerCombinationProvider(state.index, LuceneAnalyzerType.FULL_TEXT, fieldInfos);

                // custom test directory that returns a lucene lock that's never valid (Lock.ensureValid() throws IOException)
                FDBDirectory fdbDirectory = new InvalidLockTestFDBDirectory(recordStore.indexSubspace(index).subspace(directoryKey), context, options, failurePercentage);
                FDBDirectoryWrapper fdbDirectoryWrapper = new FDBDirectoryWrapper(state, fdbDirectory, directoryKey,
                        1, AgilityContext.agile(context, 1L, 1L),
                        indexAnalyzerSelector.provideIndexAnalyzer(), new Exception());

                recordStore.getIndexDeferredMaintenanceControl().setExplicitMergePath(true);
                assertThrows(IOException.class, () -> fdbDirectoryWrapper.mergeIndex(), "invalid lock");
                commit(context);
            }
        }

        // validate that the index is still sane
        new LuceneIndexTestValidator(() -> openContext(contextProps), context -> Objects.requireNonNull(schemaSetup.apply(context)))
                .validate(index, insertedDocs, "text:about", false);
    }


    /**
     * a test FDBDirectory class that returns a {@link Lock} that is not valid.
     */
    static class InvalidLockTestFDBDirectory extends FDBDirectory {
        private final int percentFailure;

        public InvalidLockTestFDBDirectory(@Nonnull Subspace subspace,
                                           @Nonnull FDBRecordContext context,
                                           @Nullable Map<String, String> indexOptions,
                                           final int percentFailure) {
            super(subspace, context, indexOptions);
            this.percentFailure = percentFailure;
        }

        @Override
        @Nonnull
        public Lock obtainLock(@Nonnull final String lockName) throws IOException {
            final Lock lock = super.obtainLock(lockName);
            return new Lock() {
                @Override
                public void close() throws IOException {
                    lock.close();
                }

                @Override
                public void ensureValid() throws IOException {
                    // 0 <= 0-99 < 100
                    if (ThreadLocalRandom.current().nextInt(100) < percentFailure) {
                        throw new IOException("invalid lock");
                    } else {
                        lock.ensureValid();
                    }
                }
            };
        }
    }

    static Stream<Arguments> sampledDelete() {
        return Stream.concat(Stream.of(Arguments.of(true, true, 230498),
                Arguments.of(false, false, 43790)),
                RandomizedTestUtils.randomArguments(random ->
                        Arguments.of(random.nextBoolean(), random.nextBoolean(), random.nextLong())));
    }

    @ParameterizedTest
    @MethodSource
    void sampledDelete(boolean isSynthetic, boolean isGrouped, long seed) throws IOException {
        // Test to make sure that if we delete half the records from every partition, their counts go down appropriately.
        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(seed, this::getStoreBuilder, pathManager)
                .setIsGrouped(isGrouped)
                .setIsSynthetic(isSynthetic)
                .setPrimaryKeySegmentIndexEnabled(true)
                .setPartitionHighWatermark(10)
                .build();


        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 2)
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, (double)dataModel.nextInt(10) + 2) // it must be at least 2.0
                .build();

        try (FDBRecordContext context = openContext(contextProps)) {
            dataModel.saveRecordsToAllGroups(25, context);
            commit(context);
        }

        explicitMergeIndex(dataModel.index, contextProps, dataModel.schemaSetup);

        dataModel.getPartitionCounts(() -> openContext(contextProps)).forEach((groupingKey, partitionCounts) ->
                assertThat(partitionCounts, Matchers.contains(10, 6, 9)));

        dataModel.validate(() -> openContext(contextProps));

        try (FDBRecordContext context = openContext(contextProps)) {
            final FDBRecordStore recordStore = dataModel.createOrOpenRecordStore(context);
            dataModel.sampleRecordsUnderTest().forEach(record -> record.deleteRecord(recordStore).join());
            context.commit();
        }

        // We won't re-partition here
        dataModel.validate(() -> openContext(contextProps));
        dataModel.getPartitionCounts(() -> openContext(contextProps)).forEach((groupingKey, partitionCounts) ->
                assertThat(partitionCounts, Matchers.contains(5, 3, 4)));
    }

    static Stream<Arguments> removeEmptyPartitions() {
        return Stream.of(
                // In the following, (5, 20) means we delete records 5-20, emptying the second partition,
                // (15, 25) means we empty the last partition, (0, 10) means we empty the first partition
                // and (0, 20) means removing the first 2 partitions
                // (there are 25 records total in each partition and the high watermark is 10).
                Arguments.of(true, true, 987654, 5, 20, 2),
                Arguments.of(true, true, 987654, 15, 25, 2),
                Arguments.of(true, true, 987654, 0, 10, 2),
                Arguments.of(true, true, 987654, 0, 20, 1),
                Arguments.of(false, false, 543210, 5, 20, 2),
                Arguments.of(false, false, 543210, 15, 25, 2),
                Arguments.of(false, false, 543210, 0, 10, 2),
                Arguments.of(false, false, 543210, 0, 20, 1));
    }

    /**
     * clear a partition and ensure it gets removed.
     *
     * @param isSynthetic whether to use synthetic records
     * @param isGrouped whether to use grouped index
     * @param seed the random seed
     * @param start the first record to delete from each group
     * @param end the last record to delete from each group
     * @param expectedCount the expected number of remaining partitions
     */
    @ParameterizedTest
    @MethodSource
    void removeEmptyPartitions(boolean isSynthetic, boolean isGrouped, long seed, int start, int end, int expectedCount) throws IOException {
        // Test that empty partitions are removed during repartitioning
        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(seed, this::getStoreBuilder, pathManager)
                .setIsGrouped(isGrouped)
                .setIsSynthetic(isSynthetic)
                .setPrimaryKeySegmentIndexEnabled(true)
                .setPartitionHighWatermark(10)
                .build();

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 2)
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, (double)dataModel.nextInt(10) + 2) // it must be at least 2.0
                .build();

        // Create multiple partitions with documents
        try (FDBRecordContext context = openContext(contextProps)) {
            dataModel.saveRecordsToAllGroups(25, context);
            commit(context);
        }

        explicitMergeIndex(dataModel.index, contextProps, dataModel.schemaSetup);

        // Verify we have 3 partitions (25 docs) initially
        dataModel.getPartitionCounts(() -> openContext(contextProps)).forEach((groupingKey, partitionCounts) ->
                assertThat(partitionCounts, hasSize(3)));

        // Delete documents
        try (FDBRecordContext context = openContext(contextProps)) {
            final FDBRecordStore recordStore = dataModel.createOrOpenRecordStore(context);
            dataModel.groupingKeys().forEach(groupingKey -> {
                List<Tuple> sortedPartitionKeys = dataModel.groupingKeyToPrimaryKeyToPartitionKey.get(groupingKey)
                        .values().stream().sorted().collect(Collectors.toList());
                // delete enough docs to empty partition
                Set<Tuple> partitionKeysToDelete = new HashSet<>(sortedPartitionKeys.subList(start, end));
                dataModel.recordsUnderTest().stream()
                        .filter(rec -> partitionKeysToDelete.contains(rec.getPartitioningKey()))
                        .forEach(rec -> rec.deleteRecord(recordStore).join());
            });
            context.commit();
        }

        // Before repartitioning, we should still have 3 partitions (including the empty one)
        dataModel.getPartitionCounts(() -> openContext(contextProps)).forEach((groupingKey, partitionCounts) -> {
            assertThat(partitionCounts, hasSize(3));
            assertThat(partitionCounts, hasItem(0));
        });
        // Trigger repartitioning - this should remove the empty partition
        explicitMergeIndex(dataModel.index, contextProps, dataModel.schemaSetup);

        // After repartitioning, we should have only expectedCount partitions (empty one removed)
        dataModel.getPartitionCounts(() -> openContext(contextProps)).forEach((groupingKey, partitionCounts) ->
                assertThat(partitionCounts, hasSize(expectedCount)));

        // Validate the index is still consistent
        dataModel.validate(() -> openContext(contextProps));
    }

    @ParameterizedTest
    @MethodSource("sampledDelete")
    void emptyAllPartitions(boolean isSynthetic, boolean isGrouped, long seed) throws IOException {
        // Test that empty partitions are removed during repartitioning
        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(seed, this::getStoreBuilder, pathManager)
                .setIsGrouped(isGrouped)
                .setIsSynthetic(isSynthetic)
                .setPrimaryKeySegmentIndexEnabled(true)
                .setPartitionHighWatermark(10)
                .build();

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 2)
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, (double)dataModel.nextInt(10) + 2) // it must be at least 2.0
                .build();

        // Create multiple partitions with documents
        try (FDBRecordContext context = openContext(contextProps)) {
            dataModel.saveRecordsToAllGroups(25, context);
            commit(context);
        }

        explicitMergeIndex(dataModel.index, contextProps, dataModel.schemaSetup);

        // Verify we have 3 partitions (25 docs) initially
        dataModel.getPartitionCounts(() -> openContext(contextProps)).forEach((groupingKey, partitionCounts) ->
                assertThat(partitionCounts, hasSize(3)));

        // Delete all documents
        try (FDBRecordContext context = openContext(contextProps)) {
            final FDBRecordStore recordStore = dataModel.createOrOpenRecordStore(context);
            dataModel.recordsUnderTest()
                    .forEach(rec -> rec.deleteRecord(recordStore).join());
            context.commit();
        }

        // Before repartitioning, we should still have 3 partitions (including the empty one)
        dataModel.getPartitionCounts(() -> openContext(contextProps)).forEach((groupingKey, partitionCounts) ->
                assertThat(partitionCounts, hasSize(3)));

        // Trigger repartitioning - this should remove the empty partition
        explicitMergeIndex(dataModel.index, contextProps, dataModel.schemaSetup);

        // After repartitioning, we should have only 1 partition (empty ones removed)
        dataModel.getPartitionCounts(() -> openContext(contextProps)).forEach((groupingKey, partitionCounts) ->
                assertThat(partitionCounts, hasSize(1)));

        // Validate the index is still consistent
        dataModel.validate(() -> openContext(contextProps));
    }

    /**
     * Remove some random set of records, commit and merge, ensure that once all records are
     * removed we have one empty partition.
     */
    @Test
    void randomlyRemoveAllRecords() throws IOException {
        // Test that empty partitions are removed during repartitioning
        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(3663, this::getStoreBuilder, pathManager)
                .setIsGrouped(true)
                .setIsSynthetic(true)
                .setPrimaryKeySegmentIndexEnabled(true)
                .setPartitionHighWatermark(10)
                .build();

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 2)
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, (double)dataModel.nextInt(10) + 2) // it must be at least 2.0
                .build();

        // Create multiple partitions with documents
        try (FDBRecordContext context = openContext(contextProps)) {
            dataModel.saveRecordsToAllGroups(25, context);
            commit(context);
        }

        explicitMergeIndex(dataModel.index, contextProps, dataModel.schemaSetup);

        // Iterate and delete 1/2 of records each time
        boolean done = false;
        int maxLoop = 100;
        AtomicInteger currentLoopCount = new AtomicInteger(1);
        while (!done) {
            try (FDBRecordContext context = openContext(contextProps)) {
                final FDBRecordStore recordStore = dataModel.createOrOpenRecordStore(context);
                dataModel.recordsUnderTest()
                        .forEach(rec -> {
                            if ((dataModel.nextInt(2) == 0) || (currentLoopCount.get() >= maxLoop)) {
                                rec.deleteRecord(recordStore).join();
                            }
                        });
                context.commit();
            }
            explicitMergeIndex(dataModel.index, contextProps, dataModel.schemaSetup);
            dataModel.validate(() -> openContext(contextProps));
            currentLoopCount.incrementAndGet();
            // done once all documents have been deleted
            done = dataModel.recordsUnderTest().isEmpty();
        }

        // Ensure we only have one empty partition
        dataModel.getPartitionCounts(() -> openContext(contextProps))
                .forEach((groupingKey, partitionCounts) ->
                    assertThat(partitionCounts, contains(0)));
    }

    static Stream<Arguments> multiUpdate() {
        Stream<Long> seeds = Streams.concat(
                Stream.of(5365L),
                TestConfigurationUtils.onlyNightly(RandomizedTestUtils.randomSeeds(6664, 76778)));
        return ParameterizedTestUtils.cartesianProduct(
                ParameterizedTestUtils.booleans("isSynthetic"),
                ParameterizedTestUtils.booleans("isGrouped"),
                Stream.of(0, 10),
                Stream.of(0, 1, 4),
                seeds);
    }

    @ParameterizedTest
    @MethodSource("multiUpdate")
    void multipleUpdatesInTransaction(boolean isSynthetic, boolean isGrouped, int highWatermark, int updateCount, long seed) throws IOException {
        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(seed, this::getStoreBuilder, pathManager)
                .setIsGrouped(isGrouped)
                .setIsSynthetic(isSynthetic)
                .setPrimaryKeySegmentIndexEnabled(true)
                .setPartitionHighWatermark(highWatermark)
                .build();

        final int documentCount = 10 + dataModel.getRandom().nextInt(10);

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 2)
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, (double)dataModel.nextInt(10) + 2) // it must be at least 2.0
                .build();

        // save records
        try (FDBRecordContext context = openContext(contextProps)) {
            dataModel.saveRecordsToAllGroups(documentCount, context);
            commit(context);
        }

        try (FDBRecordContext context = openContext(contextProps)) {
            final FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
            dataModel.sampleRecordsUnderTest().forEach(rec -> {
                for (int i = 0; i < updateCount; i++) {
                    // update some documents multiple times
                    rec.updateOtherValue(store).join();
                }
            });
            commit(context);
        }
        explicitMergeIndex(dataModel.index, contextProps, dataModel.schemaSetup);

        if (highWatermark > 0) {
            // ensure each partition has all records
            dataModel.getPartitionCounts(() -> openContext(contextProps)).forEach((groupingKey, partitionCounts) ->
                    assertThat(partitionCounts.stream().mapToInt(i -> i).sum(), Matchers.equalTo(documentCount)));
        }

        explicitMergeIndex(dataModel.index, contextProps, dataModel.schemaSetup);
        dataModel.validate(() -> openContext(contextProps));
    }

    @Test
    void testCreatePendingWritesQueue() throws IOException {
        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(1, this::getStoreBuilder, pathManager)
                .setIsGrouped(true)
                .setIsSynthetic(true)
                .setPrimaryKeySegmentIndexEnabled(true)
                .setPartitionHighWatermark(0) // no partitioning, so we only have one partition to work with
                .build();
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 2)
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, (double)dataModel.nextInt(10) + 2) // it must be at least 2.0
                .build();

        Tuple groupingKey = Tuple.from(1);
        Integer partitionId = null;
        try (FDBRecordContext context = openContext(contextProps)) {
            final FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
            final LuceneIndexMaintainer indexMaintainer = getIndexMaintainer(store, dataModel.index);
            final FDBDirectoryManager directoryManager = indexMaintainer.getDirectoryManager();
            final FDBDirectoryWrapper directoryWrapper = directoryManager.getDirectoryWrapper(groupingKey, partitionId);
            final PendingWriteQueue pendingWriteQueue = directoryWrapper.getPendingWriteQueue();
            assertNotNull(pendingWriteQueue);
        }
    }

    /**
     * Test that the DirectoryWrapper accounts for all the created WriterReaders.
     * Create multiple ReaderWriters in concurrent threads and ensure they are all closed.
     */
    @Test
    void testCreateReadersConcurrently() throws IOException {
        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(1, this::getStoreBuilder, pathManager)
                .setIsGrouped(true)
                .setIsSynthetic(true)
                .setPrimaryKeySegmentIndexEnabled(true)
                .setPartitionHighWatermark(0) // no partitioning, so we only have one partition to work with
                .build();
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 2)
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, (double)dataModel.nextInt(10) + 2) // it must be at least 2.0
                .build();

        Queue<IndexReader> createdReaders = new ConcurrentLinkedQueue<>();
        Set<DirectoryReader> actualReaders;
        Tuple groupingKey = Tuple.from(1);
        Integer partitionId = null;
        int threads = 10;
        int loops = 20;
        try (FDBRecordContext context = openContext(contextProps)) {
            final FDBRecordStore store = dataModel.createOrOpenRecordStore(context);
            final LuceneIndexMaintainer indexMaintainer = getIndexMaintainer(store, dataModel.index);
            final FDBDirectoryManager directoryManager = indexMaintainer.getDirectoryManager();

            List<CompletableFuture<Void>> futures = new ArrayList<>(threads);
            for (int i = 0; i < threads; i++) {
                futures.add(CompletableFuture.runAsync(() -> {
                    try {
                        for (int j = 0; j < loops; j++) {
                            // Cause the reader to become stale
                            dataModel.saveRecord(store, 1);
                            // Refresh the reader - this should create a new one
                            DirectoryReader writerReader = directoryManager.getWriterReader(groupingKey, partitionId, true);
                            // Store the created reader for later
                            createdReaders.add(writerReader);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, context.getExecutor()));
            }
            AsyncUtil.getAll(futures).join();
            Stream<DirectoryReader> stream = directoryManager.getDirectoryWrapper(groupingKey, partitionId)
                    .getReadersToClose()
                    .stream().map(LazyCloseable::getUnchecked);
            actualReaders = stream.collect(Collectors.toSet());
            commit(context);
        }

        // assert that each captured reader is included in the directory wrapper's list
        createdReaders.forEach(createdReader ->
                assertTrue(actualReaders.contains(createdReader)));
    }

    static Stream<Arguments> changingEncryptionKey() {
        return Stream.concat(Stream.of(Arguments.of(true, true, 288513),
                Arguments.of(false, false, 792025)),
                RandomizedTestUtils.randomArguments(random ->
                        Arguments.of(random.nextBoolean(), random.nextBoolean(), random.nextLong())));
    }

    @ParameterizedTest
    @MethodSource
    void changingEncryptionKey(boolean isSynthetic, boolean isGrouped, long seed) {
        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(seed, this::getStoreBuilder, pathManager)
                .setIsGrouped(isGrouped)
                .setIsSynthetic(isSynthetic)
                .setPrimaryKeySegmentIndexEnabled(true)
                .build();

        final RecordLayerPropertyStorage.Builder contextPropsBuilder = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_INDEX_COMPRESSION_ENABLED, true)
                .addProp(LuceneRecordContextProperties.LUCENE_INDEX_ENCRYPTION_ENABLED, true)
                .addProp(LuceneRecordContextProperties.LUCENE_FIELD_PROTOBUF_PREFIX_ENABLED, true);
        final SecretKey key1 = RandomSecretUtil.randomSecretKey(dataModel.getRandom());
        final SerializationKeyManager keyManager1 = new FixedZeroKeyManager(key1, null, dataModel.getRandom());
        contextPropsBuilder.addProp(LuceneRecordContextProperties.LUCENE_INDEX_KEY_MANAGER, keyManager1);
        final RecordLayerPropertyStorage contextProps1 = contextPropsBuilder.build();

        try (FDBRecordContext context = openContext(contextProps1)) {
            dataModel.saveRecordsToAllGroups(20, context);
            commit(context);
        }

        explicitMergeIndex(dataModel.index, contextProps1, dataModel.schemaSetup);

        final SecretKey key2 = RandomSecretUtil.randomSecretKey(dataModel.getRandom());
        final SerializationKeyManager keyManager2 = new FixedZeroKeyManager(key2, null, dataModel.getRandom());
        contextPropsBuilder.removeProp(LuceneRecordContextProperties.LUCENE_INDEX_KEY_MANAGER);
        contextPropsBuilder.addProp(LuceneRecordContextProperties.LUCENE_INDEX_KEY_MANAGER, keyManager2);
        final RecordLayerPropertyStorage contextProps2 = contextPropsBuilder.build();
        IOException ioException = assertThrows(IOException.class,
                () -> dataModel.validate(() -> openContext(contextProps2)));
        assertThat(ioException.getCause(), instanceOf(RecordCoreException.class));
        // Possible failures: (1) does not decrypt; (2) decrypts to garbage with bad compression level; (3) does not decompress.
        assertThat(ioException.getCause().getMessage(), anyOf(
                containsString("Lucene data decoding failure"),
                containsString("Un-supported compression version")));
    }

    private enum PartitionCount { NONE, ONE, MULTIPLE }

    private static Stream<Arguments> concurrentParameters() {
        // only run the individual tests with synthetic during nightly, the mix runs both
        return Stream.concat(Stream.of(false),
                TestConfigurationUtils.onlyNightly(
                        IntStream.range(0, 3).boxed().flatMap(i -> Stream.of(true, false))))
                .flatMap(isSynthetic ->
                        Arrays.stream(PartitionCount.values())
                            .map(partitionHighWatermark -> Arguments.of(isSynthetic, partitionHighWatermark)));
    }

    /**
     * Test that updating records in the same transaction does not cause issues with the executor, and doesn't result in
     * a corrupted index.
     * <p>
     *     See issues: <a href="https://github.com/FoundationDB/fdb-record-layer/issues/2989">#2989</a> and
     *     <a href="https://github.com/FoundationDB/fdb-record-layer/issues/2990">#2990</a>.
     * </p>
     * @throws IOException if there's an issue with Lucene
     */
    @ParameterizedTest
    @MethodSource("concurrentParameters")
    void concurrentUpdate(final boolean isSynthetic, PartitionCount partitionCount) throws IOException {
        concurrentTestWithinTransaction(isSynthetic, (dataModel, recordStore) ->
                RecordCursor.fromList(dataModel.recordsUnderTest())
                        .mapPipelined(record -> record.updateOtherValue(recordStore), 10)
                        .asList().join(),
                Assertions::assertEquals, noopConsumer(), partitionCount);
    }

    /**
     * Test that deleting records in the same transaction does not cause issues with the executor, and doesn't result in
     * a corrupted index.
     * <p>
     *     See issues: <a href="https://github.com/FoundationDB/fdb-record-layer/issues/2989">#2989</a> and
     *     <a href="https://github.com/FoundationDB/fdb-record-layer/issues/2990">#2990</a>.
     * </p>
     * @throws IOException if there's an issue with Lucene
     */
    @ParameterizedTest
    @MethodSource("concurrentParameters")
    void concurrentDelete(final boolean isSynthetic, PartitionCount partitionCount) throws IOException {
        concurrentTestWithinTransaction(isSynthetic, (dataModel, recordStore) ->
                RecordCursor.fromList(dataModel.recordsUnderTest())
                        .mapPipelined(record -> record.deleteRecord(recordStore), 10)
                        .asList().join(),
                (inserted, actual) -> assertEquals(0, actual),
                // Assert that there is only one partition left, and that it has 0 documents
                partitionCounts -> partitionCounts.values().forEach(counts -> assertThat(counts, contains(0))),
                partitionCount);
    }

    /**
     * Test that inserting records in the same transaction does not cause issues with the executor, and doesn't result in
     * a corrupted index.
     * <p>
     *     See issues: <a href="https://github.com/FoundationDB/fdb-record-layer/issues/2989">#2989</a> and
     *     <a href="https://github.com/FoundationDB/fdb-record-layer/issues/2990">#2990</a>.
     * </p>
     * @throws IOException if there's an issue with Lucene
     */
    @ParameterizedTest
    @MethodSource("concurrentParameters")
    void concurrentInsert(final boolean isSynthetic, final PartitionCount partitionCount) throws IOException {
        concurrentTestWithinTransaction(isSynthetic, (dataModel, recordStore) ->
                        RecordCursor.fromList(dataModel.recordsUnderTest())
                                .mapPipelined(record -> { // ignore the record, we're just using that as a count
                                    return dataModel.saveRecordAsync(true, recordStore, 1);
                                }, 10)
                                .asList().join(),
                (inserted, actual) -> assertEquals(inserted * 2, actual), noopConsumer(), partitionCount);
    }

    private static Stream<Arguments> concurrentMixParameters() {
        return ParameterizedTestUtils.cartesianProduct(
                ParameterizedTestUtils.booleans("isSynthetic"),
                Arrays.stream(PartitionCount.values()));
    }

    /**
     * Test that mutating records in the same transaction does not cause issues with the executor, and doesn't result in
     * a corrupted index.
     * <p>
     *     See issues: <a href="https://github.com/FoundationDB/fdb-record-layer/issues/2989">#2989</a> and
     *     <a href="https://github.com/FoundationDB/fdb-record-layer/issues/2990">#2990</a>.
     * </p>
     * @throws IOException if there's an issue with Lucene
     */
    @ParameterizedTest
    @MethodSource("concurrentMixParameters")
    void concurrentMix(final boolean isSynthetic, final PartitionCount partitionCount) throws IOException {
        // We never touch the same record twice.
        AtomicInteger step = new AtomicInteger(0);
        AtomicInteger updates = new AtomicInteger(0);
        AtomicInteger deletes = new AtomicInteger(0);
        AtomicInteger saves = new AtomicInteger(0);
        concurrentTestWithinTransaction(isSynthetic, (dataModel, recordStore) ->
                        RecordCursor.fromList(dataModel.recordsUnderTest())
                                .mapPipelined(record -> {
                                    switch (step.incrementAndGet() % 3) {
                                        case 0:
                                            updates.incrementAndGet();
                                            return record.updateOtherValue(recordStore);
                                        case 1:
                                            deletes.incrementAndGet();
                                            return record.deleteRecord(recordStore);
                                        default:
                                            saves.incrementAndGet();
                                            return dataModel.saveRecordAsync(true, recordStore, 1)
                                                    .thenAccept(vignore -> { });
                                    }
                                }, 10)
                                .asList().join(),
                (inserted, actual) -> assertEquals(inserted + saves.get() - deletes.get(), actual),
                noopConsumer(),
                partitionCount);
    }

    private void concurrentTestWithinTransaction(boolean isSynthetic,
                                                 final BiConsumer<LuceneIndexTestDataModel, FDBRecordStore> applyChangeConcurrently,
                                                 final BiConsumer<Integer, Integer> assertDataModelCount,
                                                 final Consumer<Map<Tuple, List<Integer>>> assertPartitionCounts,
                                                 final PartitionCount partitionCountToPopulate) throws IOException {
        // Once the two issues noted below are fixed, we should make this parameterized, and run with additional random
        // configurations.
        AtomicInteger threadCounter = new AtomicInteger();
        // Synchronization blocks in FDBDirectoryWrapper used to cause thread starvation
        // see https://github.com/FoundationDB/fdb-record-layer/issues/2989
        // So set the pool small to make sure we cover that
        // But, the test is still flaky if we set it to 1: https://github.com/FoundationDB/fdb-record-layer/issues/3501
        this.dbExtension.getDatabaseFactory().setExecutor(new ForkJoinPool(3,
                pool -> {
                    final ForkJoinWorkerThread thread = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
                    thread.setName("ConcurrentUpdatePool-" + threadCounter.getAndIncrement());
                    return thread;
                },
                null, false));
        final long seed = 320947L;
        final boolean isGrouped = true;
        final boolean primaryKeySegmentIndexEnabled = true;
        int partitionHighWatermark;

        switch (partitionCountToPopulate) {
            case NONE:
                partitionHighWatermark = 0;
                break;
            case ONE:
                partitionHighWatermark = 100_000;
                break;
            case MULTIPLE:
                partitionHighWatermark = 100;
                break;
            default:
                throw new IllegalArgumentException("Unknown value for partitionCountToPopulate:" + partitionCountToPopulate);
        }

        final LuceneIndexTestDataModel dataModel = new LuceneIndexTestDataModel.Builder(seed, this::getStoreBuilder, pathManager)
                .setIsGrouped(isGrouped)
                .setIsSynthetic(isSynthetic)
                .setPrimaryKeySegmentIndexEnabled(primaryKeySegmentIndexEnabled)
                .setPartitionHighWatermark(partitionHighWatermark)
                .build();

        final int repartitionCount = 10;
        final int recordsPerIteration = 10;
        final int loopCount = 40;

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, repartitionCount)
                .addProp(LuceneRecordContextProperties.LUCENE_MAX_DOCUMENTS_TO_MOVE_DURING_REPARTITIONING, dataModel.nextInt(1000) + repartitionCount)
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, (double)dataModel.nextInt(10) + 2) // it must be at least 2.0
                .build();
        for (int i = 0; i < loopCount; i++) {
            LOGGER.info(KeyValueLogMessage.of("concurrentUpdate loop",
                    "iteration", i,
                    "groupCount", dataModel.groupingKeyToPrimaryKeyToPartitionKey.size(),
                    "docCount", dataModel.groupingKeyToPrimaryKeyToPartitionKey.values().stream().mapToInt(Map::size).sum(),
                    "docMinPerGroup", dataModel.groupingKeyToPrimaryKeyToPartitionKey.values().stream().mapToInt(Map::size).min(),
                    "docMaxPerGroup", dataModel.groupingKeyToPrimaryKeyToPartitionKey.values().stream().mapToInt(Map::size).max()));

            try (FDBRecordContext context = openContext(contextProps)) {
                dataModel.saveRecords(recordsPerIteration, context, 1);
                commit(context);
            }
            explicitMergeIndex(dataModel.index, contextProps, dataModel.schemaSetup);
        }

        dataModel.validate(() -> openContext(contextProps));

        try (FDBRecordContext context = openContext(contextProps)) {
            FDBRecordStore recordStore = Objects.requireNonNull(dataModel.schemaSetup.apply(context));
            recordStore.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(false);
            assertThat(dataModel.recordsUnderTest(), Matchers.hasSize(Matchers.greaterThan(30)));
            LOGGER.info("concurrentTestWithinTransaction: Starting applyChanges");
            applyChangeConcurrently.accept(dataModel, recordStore);
            LOGGER.info("concurrentTestWithinTransaction: Done applyChanges");
            commit(context);
        }

        explicitMergeIndex(dataModel.index, contextProps, dataModel.schemaSetup);

        assertDataModelCount.accept(recordsPerIteration * loopCount,
                dataModel.groupingKeyToPrimaryKeyToPartitionKey.values().stream().mapToInt(Map::size).sum());
        if (dataModel.partitionHighWatermark > 0) {
            assertPartitionCounts.accept(dataModel.getPartitionCounts(() -> openContext(contextProps)));
        }

        dataModel.validate(() -> openContext(contextProps));

        if (isGrouped) {
            validateDeleteWhere(isSynthetic, dataModel.groupingKeyToPrimaryKeyToPartitionKey, contextProps, dataModel.schemaSetup, dataModel.index);
        }
    }

    static Stream<Arguments> concurrentStoreTest() {
        return Stream.concat(
                Stream.of(
                        Arguments.of(true, true, true, 10, 9237590782644L)),
                RandomizedTestUtils.randomArguments(random ->
                        Arguments.of(random.nextBoolean(),
                                random.nextBoolean(),
                                random.nextBoolean(),
                                random.nextInt(20) + 3,
                                random.nextLong())));
    }

    @ParameterizedTest
    @MethodSource
    @SuperSlow
    void concurrentStoreTest(boolean isGrouped,
                             boolean isSynthetic,
                             boolean primaryKeySegmentIndexEnabled,
                             int storeCount,
                             long seed) {
        // create a soft timeout of 5 minutes because the randomness can cause values that may not appear to be something
        // that should take long to take 30+ minutes.... But we're probably getting pretty good coverage if it is just
        // running in a loop for 5 minutes even if it doesn't hit the requested loop count
        final long end = System.nanoTime() + TimeUnit.MINUTES.toNanos(5);
        Random random = new Random(seed);
        final int repartitionCount = 2;
        final RandomTextGenerator outerTextGenerator = new RandomTextGenerator(random);
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, repartitionCount)
                .addProp(LuceneRecordContextProperties.LUCENE_MAX_DOCUMENTS_TO_MOVE_DURING_REPARTITIONING, random.nextInt(1000) + repartitionCount)
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, (double)random.nextInt(10) + 2) // it must be at least 2.0
                .build();
        // We create one builder here, and reuse it so that the dataModel for each thread has the same metadata,
        // but different paths.
        final LuceneIndexTestDataModel.Builder dataModelBuilder = new LuceneIndexTestDataModel
                .Builder(random.nextLong(), LuceneIndexMaintenanceTest.this::getStoreBuilder, pathManager)
                .setIsGrouped(isGrouped)
                .setIsSynthetic(isSynthetic)
                .setPrimaryKeySegmentIndexEnabled(primaryKeySegmentIndexEnabled)
                .setPartitionHighWatermark(1000)
                .setTextGeneratorWithNewRandom(outerTextGenerator);

        final List<ConcurrentStoreTestRunner> runners = IntStream.range(0, storeCount)
                .mapToObj(i -> new ConcurrentStoreTestRunner(contextProps, end, dataModelBuilder))
                .collect(Collectors.toList());

        final List<ConcurrentMap<Tuple, ConcurrentMap<Tuple, Tuple>>> allIds = AsyncUtil.getAll(runners.stream()
                        .map(CompletableFuture::supplyAsync)
                        .collect(Collectors.toList()))
                .join();
        LOGGER.info(KeyValueLogMessage.of("Completed concurrentStoreTest successfully",
                "ids", allIds.stream()
                        .map(storeIds -> storeIds.values().stream().mapToInt(Map::size).sum())
                        .collect(Collectors.toList())));
        for (final ConcurrentMap<Tuple, ConcurrentMap<Tuple, Tuple>> storeIds : allIds) {
            assertThat("All of the stores should have generated a fair amount of documents",
                    storeIds.values().stream().mapToInt(Map::size).sum(), Matchers.greaterThan(200));
        }
    }

    private class ConcurrentStoreTestRunner implements Supplier<ConcurrentMap<Tuple, ConcurrentMap<Tuple, Tuple>>> {
        private final RecordLayerPropertyStorage contextProps;
        private final LuceneIndexTestDataModel dataModel;
        private final long endTime;

        public ConcurrentStoreTestRunner(final RecordLayerPropertyStorage contextProps,
                                         final long endTime, final LuceneIndexTestDataModel.Builder dataModelBuilder) {
            this.contextProps = contextProps;
            this.endTime = endTime;
            this.dataModel = dataModelBuilder.build();
        }

        @Override
        public ConcurrentMap<Tuple, ConcurrentMap<Tuple, Tuple>> get() {
            int maxTransactionsPerLoop = 5;
            final LuceneIndexTestValidator luceneIndexTestValidator = new LuceneIndexTestValidator(() -> openContext(contextProps),
                    dataModel::createOrOpenRecordStore);
            int currentLoop = 0;
            while (System.nanoTime() < endTime) {
                currentLoop++;
                try {
                    dataModel.saveManyRecords(1, () -> openContext(contextProps), dataModel.nextInt(maxTransactionsPerLoop - 1) + 1);
                } catch (RuntimeException e) {
                    throw new RuntimeException("Failed to generate documents at iteration " + currentLoop, e);
                }

                boolean mergeFailed = mergeIndex(currentLoop);
                try {
                    luceneIndexTestValidator.validate(dataModel.index, dataModel.groupingKeyToPrimaryKeyToPartitionKey,
                            dataModel.isSynthetic ? "child_str_value:forth" : "text_value:about", mergeFailed);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            return dataModel.groupingKeyToPrimaryKeyToPartitionKey;
        }

        private boolean mergeIndex(final int currentLoop) {
            try {
                explicitMergeIndex(dataModel.index, contextProps, dataModel.schemaSetup);
            } catch (FDBExceptions.FDBStoreRetriableException e) {
                if (e.getCause() instanceof FDBException) {
                    final FDBException fe = (FDBException)e.getCause();
                    if (fe.getCode() == 1051) { // Batch GRV request rate limit exceeded
                        LOGGER.info("Batch GRV exceeded at iteration " + currentLoop, e);
                        try {
                            Thread.sleep(50);
                            return true;
                        } catch (InterruptedException ex) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(ex);
                        }
                    }
                }
                throw new RuntimeException("Failed merge at iteration " + currentLoop, e);
            } catch (RuntimeException e) {
                throw new RuntimeException("Failed merge at iteration " + currentLoop, e);
            }
            return false;
        }
    }

    private void createComplexRecords(int count,
                                      Map<Tuple, Map<Tuple, Tuple>> insertedKeys,
                                      RecordLayerPropertyStorage contextProps,
                                      Function<FDBRecordContext, FDBRecordStore> schemaSetup) {
        long timestamp = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            try (FDBRecordContext context = openContext(contextProps)) {
                FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
                recordStore.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(false);
                TestRecordsTextProto.ComplexDocument cd = TestRecordsTextProto.ComplexDocument.newBuilder()
                        .setGroup(1)
                        .setDocId(i + 1000L)
                        .setIsSeen(true)
                        .setText("A word about what I want to say")
                        .setTimestamp(timestamp + i)
                        .setHeader(TestRecordsTextProto.ComplexDocument.Header.newBuilder().setHeaderId(1000L - i))
                        .build();
                Tuple primaryKey = recordStore.saveRecord(cd).getPrimaryKey();
                insertedKeys.computeIfAbsent(Tuple.from(1), k -> new HashMap<>()).put(primaryKey, Tuple.from(timestamp));
                commit(context);
            }
        }
    }

    private static LoggableKeysAndValues<? extends Exception> findTimeoutException(final RecordCoreException e) {
        Map<Throwable, String> visited = new IdentityHashMap<>();
        ArrayDeque<Throwable> toVisit = new ArrayDeque<>();
        toVisit.push(e);
        while (!toVisit.isEmpty()) {
            Throwable cause = toVisit.removeFirst();
            if (!visited.containsKey(cause)) {
                // This will get throws when the legacy asyncToSync is called
                if (cause instanceof LoggableTimeoutException) {
                    return (LoggableTimeoutException) cause;
                } else if (cause instanceof LuceneConcurrency.AsyncToSyncTimeoutException) {
                    return (LuceneConcurrency.AsyncToSyncTimeoutException) cause;
                }
                if (cause.getCause() != null) {
                    toVisit.addLast(cause.getCause());
                }
                for (final Throwable suppressed : cause.getSuppressed()) {
                    toVisit.addLast(suppressed);
                }
                visited.put(cause, "");
            }
        }
        return null;
    }

    @Nonnull
    public static Index complexPartitionedIndex(final Map<String, String> options) {
        return new Index("Complex$partitioned",
                concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
                        function(LuceneFunctionNames.LUCENE_SORTED, field("timestamp"))).groupBy(field("group")),
                LuceneIndexTypes.LUCENE,
                options);
    }

    private void validateDeleteWhere(final boolean isSynthetic,
                                     final Map<Tuple, ? extends Map<Tuple, Tuple>> ids,
                                     final RecordLayerPropertyStorage contextProps,
                                     final Function<FDBRecordContext, FDBRecordStore> schemaSetup,
                                     final Index index) throws IOException {
        final List<Tuple> groups = List.copyOf(ids.keySet());
        for (final Tuple group : groups) {
            try (FDBRecordContext context = openContext(contextProps)) {
                FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
                recordStore.deleteRecordsWhere(Query.field("group").equalsValue(group.getLong(0)));
                context.commit();
            }
            ids.remove(group);
            new LuceneIndexTestValidator(() -> openContext(contextProps), context -> Objects.requireNonNull(schemaSetup.apply(context)))
                    .validate(index, ids, isSynthetic ? "child_str_value:forth" : "text_value:about");
        }
    }


    private void explicitMergeIndex(Index index,
                                    RecordLayerPropertyStorage contextProps,
                                    Function<FDBRecordContext, FDBRecordStore> schemaSetup) {
        try (FDBRecordContext context = openContext(contextProps)) {
            FDBRecordStore recordStore = Objects.requireNonNull(schemaSetup.apply(context));
            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                    .setRecordStore(recordStore)
                    .setIndex(index)
                    .setTimer(new FDBStoreTimer())
                    .build()) {
                indexBuilder.mergeIndex();
            }
        }
    }

    protected RecordLayerPropertyStorage.Builder addDefaultProps(final RecordLayerPropertyStorage.Builder props) {
        if (props.hasProp(LuceneRecordContextProperties.LUCENE_INDEX_COMPRESSION_ENABLED)) {
            return props;
        }
        return super.addDefaultProps(props).addProp(LuceneRecordContextProperties.LUCENE_INDEX_COMPRESSION_ENABLED, true);
    }

    private <T> Consumer<T> noopConsumer() {
        return ignored -> { };
    }

    @Nonnull
    protected LuceneIndexMaintainer getIndexMaintainer(FDBRecordStore store, Index index) {
        return (LuceneIndexMaintainer)store.getIndexMaintainer(index);
    }
}
