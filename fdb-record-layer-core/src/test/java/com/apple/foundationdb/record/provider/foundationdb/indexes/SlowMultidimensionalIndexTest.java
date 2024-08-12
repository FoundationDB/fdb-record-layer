/*
 * SlowMultidimensionalIndexTest.java
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryIndexPlan;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static com.apple.foundationdb.async.rtree.RTree.Storage.BY_NODE;
import static com.apple.foundationdb.async.rtree.RTree.Storage.BY_SLOT;

/**
 * Additional tests for Multidimensional Index around concurrency and large data-sets.
 */
@Tag(Tags.Slow)
class SlowMultidimensionalIndexTest extends MultidimensionalIndexTestBase {

    static Stream<Arguments> argumentsForBasicReads() {
        return SimpleMultidimensionalIndexTest.argumentsForBasicReads();
    }

    /**
     * Arguments for index reads. Note that for each run we run the tests with a different seed. That is intentionally
     * done in a way that the seed itself is handed over to the test case which causes that seed to be reported.
     * If a testcase fails, the particular seed reported can be used here to recreate the exact conditions of the
     * failure.
     *
     * @return a stream of arguments
     */
    static Stream<Arguments> argumentsForIndexReads() {
        final Random random = new Random(System.currentTimeMillis());
        return Stream.concat(
                SimpleMultidimensionalIndexTest.argumentsForIndexReads(),
                Stream.of(
                    Arguments.of(random.nextLong(), 1000, BY_SLOT.toString(), false, false),
                    Arguments.of(random.nextLong(), 1000, BY_SLOT.toString(), false, true),
                    Arguments.of(random.nextLong(), 1000, BY_SLOT.toString(), true, false),
                    Arguments.of(random.nextLong(), 1000, BY_SLOT.toString(), true, true),
                    Arguments.of(random.nextLong(), 1000, BY_NODE.toString(), false, false),
                    Arguments.of(random.nextLong(), 1000, BY_NODE.toString(), false, true),
                    Arguments.of(random.nextLong(), 1000, BY_NODE.toString(), true, false),
                    Arguments.of(random.nextLong(), 1000, BY_NODE.toString(), true, true),
                    Arguments.of(random.nextLong(), 5000, BY_SLOT.toString(), false, false),
                    Arguments.of(random.nextLong(), 5000, BY_SLOT.toString(), false, true),
                    Arguments.of(random.nextLong(), 5000, BY_SLOT.toString(), true, false),
                    Arguments.of(random.nextLong(), 5000, BY_SLOT.toString(), true, true),
                    Arguments.of(random.nextLong(), 5000, BY_NODE.toString(), false, false),
                    Arguments.of(random.nextLong(), 5000, BY_NODE.toString(), false, true),
                    Arguments.of(random.nextLong(), 5000, BY_NODE.toString(), true, false),
                    Arguments.of(random.nextLong(), 5000, BY_NODE.toString(), true, true)
        ));
    }

    static Stream<Arguments> argumentsForIndexReadsAfterDeletes() {
        final Random random = new Random(System.currentTimeMillis());
        return Stream.concat(
                SimpleMultidimensionalIndexTest.argumentsForIndexReadsAfterDeletes(),
                Stream.of(
                    Arguments.of(random.nextLong(), 1000, random.nextInt(1000) + 1, BY_SLOT.toString(), false, false),
                    Arguments.of(random.nextLong(), 1000, random.nextInt(1000) + 1, BY_SLOT.toString(), false, true),
                    Arguments.of(random.nextLong(), 1000, random.nextInt(1000) + 1, BY_SLOT.toString(), true, false),
                    Arguments.of(random.nextLong(), 1000, random.nextInt(1000) + 1, BY_SLOT.toString(), true, true),
                    Arguments.of(random.nextLong(), 5000, random.nextInt(5000) + 1, BY_SLOT.toString(), false, false),
                    Arguments.of(random.nextLong(), 5000, random.nextInt(5000) + 1, BY_SLOT.toString(), false, true),
                    Arguments.of(random.nextLong(), 5000, random.nextInt(5000) + 1, BY_SLOT.toString(), true, false),
                    Arguments.of(random.nextLong(), 5000, random.nextInt(5000) + 1, BY_SLOT.toString(), true, true),
                    Arguments.of(random.nextLong(), 1000, random.nextInt(1000) + 1, BY_NODE.toString(), false, false),
                    Arguments.of(random.nextLong(), 1000, random.nextInt(1000) + 1, BY_NODE.toString(), false, true),
                    Arguments.of(random.nextLong(), 1000, random.nextInt(1000) + 1, BY_NODE.toString(), true, false),
                    Arguments.of(random.nextLong(), 1000, random.nextInt(1000) + 1, BY_NODE.toString(), true, true),
                    Arguments.of(random.nextLong(), 5000, random.nextInt(5000) + 1, BY_NODE.toString(), false, false),
                    Arguments.of(random.nextLong(), 5000, random.nextInt(5000) + 1, BY_NODE.toString(), false, true),
                    Arguments.of(random.nextLong(), 5000, random.nextInt(5000) + 1, BY_NODE.toString(), true, false),
                    Arguments.of(random.nextLong(), 5000, random.nextInt(5000) + 1, BY_NODE.toString(), true, true)
                ));
    }

    static Stream<Arguments> argumentsForIndexReadsWithDuplicates() {
        return Stream.concat(
                SimpleMultidimensionalIndexTest.argumentsForIndexReadsWithDuplicates(),
                Stream.of(
                    Arguments.of(1000, BY_SLOT.toString(), false, false),
                    Arguments.of(1000, BY_SLOT.toString(), false, true),
                    Arguments.of(1000, BY_SLOT.toString(), true, false),
                    Arguments.of(1000, BY_SLOT.toString(), true, true),
                    Arguments.of(5000, BY_SLOT.toString(), false, false),
                    Arguments.of(5000, BY_SLOT.toString(), false, true),
                    Arguments.of(5000, BY_SLOT.toString(), true, false),
                    Arguments.of(5000, BY_SLOT.toString(), true, true),
                    Arguments.of(1000, BY_NODE.toString(), false, false),
                    Arguments.of(1000, BY_NODE.toString(), false, true),
                    Arguments.of(1000, BY_NODE.toString(), true, false),
                    Arguments.of(1000, BY_NODE.toString(), true, true),
                    Arguments.of(5000, BY_NODE.toString(), false, false),
                    Arguments.of(5000, BY_NODE.toString(), false, true),
                    Arguments.of(5000, BY_NODE.toString(), true, false),
                    Arguments.of(5000, BY_NODE.toString(), true, true))
        );
    }

    /**
     * Arguments for index reads using an IN clause. Note that for each run we run the tests with a different seed.
     * That is intentionally done in a way that the seed itself is handed over to the test case which causes that seed
     * to be reported. If a testcase fails, the particular seed reported can be used here to recreate the exact
     * conditions of the failure.
     * @return a stream of arguments
     */
    static Stream<Arguments> argumentsForIndexReadWithIn() {
        final Random random = new Random(System.currentTimeMillis());
        return Stream.concat(
                SimpleMultidimensionalIndexTest.argumentsForIndexReadWithIn(),
                Stream.of(
                        Arguments.of(random.nextLong(), 1000, 100),
                        Arguments.of(random.nextLong(), 1000, 1000),
                        Arguments.of(random.nextLong(), 1000, 5000),
                        Arguments.of(random.nextLong(), 5000, 100),
                        Arguments.of(random.nextLong(), 5000, 5000),
                        Arguments.of(random.nextLong(), 5000, 10000)
                ));
    }

    @ParameterizedTest
    @MethodSource("argumentsForBasicReads")
    void basicRead(@Nonnull final String storage, final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        super.basicReadTest(true, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @ParameterizedTest
    @MethodSource("argumentsForBasicReads")
    void basicReadWithNulls(@Nonnull final String storage, final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        super.basicReadWithNullsTest(true, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @ParameterizedTest
    @MethodSource("argumentsForBasicReads")
    void deleteWhereTest(@Nonnull final String storage, final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        super.deleteWhereTest(true, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @ParameterizedTest
    @MethodSource("argumentsForBasicReads")
    void coveringIndexScanWithFetchTest(@Nonnull final String storage, final boolean storeHilbertValues,
                                        final boolean useNodeSlotIndex) throws Exception {
        super.coveringIndexScanWithFetchTest(true, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @ParameterizedTest
    @MethodSource("argumentsForIndexReads")
    void indexReadTest(final long seed, final int numRecords, @Nonnull final String storage,
                       final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        super.indexReadTest(true, seed, numRecords, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @ParameterizedTest
    @MethodSource("argumentsForIndexReads")
    void indexReadWithNullsTest(final long seed, final int numRecords, @Nonnull final String storage,
                                final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        super.indexReadWithNullsTest(true, seed, numRecords, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @ParameterizedTest
    @MethodSource("argumentsForIndexReads")
    void indexReadIsNullTest(final long seed, final int numRecords, @Nonnull final String storage,
                             final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        super.indexReadIsNullTest(true, seed, numRecords, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @ParameterizedTest
    @MethodSource("argumentsForIndexReads")
    void coveringIndexReadTest(final long seed, final int numRecords, @Nonnull final String storage,
                               final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        super.coveringIndexReadTest(true, seed, numRecords, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @ParameterizedTest
    @MethodSource("argumentsForIndexReads")
    void indexScan3DTest(final long seed, final int numRecords, @Nonnull final String storage,
                         final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        super.indexScan3DTest(true, seed, numRecords, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @ParameterizedTest
    @MethodSource("argumentsForIndexReads")
    void unprefixedIndexReadTest(final long seed, final int numRecords, @Nonnull final String storage,
                                 final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        super.unprefixedIndexReadTest(true, seed, numRecords, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @ParameterizedTest
    @MethodSource("argumentsForIndexReads")
    void unprefixedSuffixedIndexReadTest(final long seed, final int numRecords, @Nonnull final String storage,
                                         final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        super.unprefixedSuffixedIndexReadTest(true, seed, numRecords, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @ParameterizedTest
    @MethodSource("argumentsForIndexReads")
    void indexReadWithAdditionalValueTest(final long seed, final int numRecords, @Nonnull final String storage,
                                          final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        super.indexReadWithAdditionalValueTest(true, seed, numRecords, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @ParameterizedTest
    @MethodSource("argumentsForIndexReads")
    void unprefixedSuffixedIndexReadWithResidualsTest(final long seed, final int numRecords, @Nonnull final String storage,
                                                      final boolean storeHilbertValues, final Boolean useNodeSlotIndex) throws Exception {
        super.unprefixedSuffixedIndexReadWithResidualsTest(true, seed, numRecords, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @ParameterizedTest
    @MethodSource("argumentsForIndexReads")
    void indexSkipScanTest(final long seed, final int numRecords, @Nonnull final String storage,
                           final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        super.indexSkipScanTest(true, seed, numRecords, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @ParameterizedTest
    @MethodSource("argumentsForIndexReadsAfterDeletes")
    void indexReadsAfterDeletesTest(final long seed, final int numRecords, final int numDeletes,
                                    @Nonnull final String storage, final boolean storeHilbertValues,
                                    final boolean useNodeSlotIndex) throws Exception {
        super.indexReadsAfterDeletesTest(true, seed, numRecords, numDeletes, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @ParameterizedTest
    @MethodSource("argumentsForIndexReadsWithDuplicates")
    void indexReadWithDuplicatesTest(final int numRecords, @Nonnull final String storage,
                                     final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        super.indexReadWithDuplicatesTest(true, numRecords, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @Test
    void continuationTest() throws Exception {
        super.continuationTest(true);
    }

    @Test
    void wrongDimensionTypes() {
        super.wrongDimensionTypes(true);
    }

    @Test
    void indexReadWithNullsAndMinsTest1() throws Exception {
        super.indexReadWithNullsAndMinsTest1(true);
    }

    @Test
    void indexReadWithNullsAndMinsTest2() throws Exception {
        super.indexReadWithNullsAndMinsTest2(true);
    }

    @ParameterizedTest
    @MethodSource("argumentsForIndexReadWithIn")
    void indexReadWithIn(final long seed, final int numRecords, final int numIns) throws Exception {
        super.indexReadWithIn(false, seed, numRecords, numIns);
    }

    @ParameterizedTest
    @MethodSource("argumentsForBasicReads")
    void concurrentReadsAndWrites(@Nonnull final String storage, final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        final RecordMetaDataHook additionalIndex = metaDataBuilder -> addMultidimensionalIndex(metaDataBuilder, storage,
                storeHilbertValues, useNodeSlotIndex);
        final RecordQueryIndexPlan indexPlan =
                new RecordQueryIndexPlan("EventIntervals",
                        new HypercubeScanParameters("business",
                                (Long)null, null,
                                null, null),
                        false);
        final var random = new Random(System.currentTimeMillis());
        final var expectedMessages = new HashSet<Message>();
        var writeNum = 0;
        try (final var context = openContext()) {
            final var writeFutures = new ArrayList<CompletableFuture<FDBStoredRecord<Message>>>();
            final var readFutures = new ArrayList<CompletableFuture<Void>>();
            openRecordStore(context, additionalIndex);
            for (int i = 0; i < 500; i++) {
                if (random.nextBoolean()) {
                    // write single record.
                    writeFutures.add(recordStore.saveRecordAsync(getRecordGenerator(random, ImmutableList.of("business")).apply(writeNum++)));
                } else {
                    // read all records inserted.
                    readFutures.add(CompletableFuture.runAsync(() -> {
                        try (var cursor = indexPlan.executePlan(recordStore, EvaluationContext.empty(), null, ExecuteProperties.SERIAL_EXECUTE)) {
                            while (true) {
                                var result = cursor.onNext().get();
                                if (!result.hasNext()) {
                                    return;
                                }
                            }
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }, context.getExecutor()));
                }
            }
            // Assert no failures in any issued writes and reads.
            Assertions.assertDoesNotThrow(() -> AsyncUtil.whenAll(writeFutures).get());
            Assertions.assertDoesNotThrow(() -> AsyncUtil.whenAll(readFutures).get());
            context.commit();
            for (var future: writeFutures) {
                expectedMessages.add(future.get().getRecord());
            }
        }
        // Assert that the index have all written records intact.
        final var actualMessages = new HashSet<Message>();
        try (final var context = openContext()) {
            openRecordStore(context, additionalIndex);
            try (var cursor = indexPlan.executePlan(recordStore, EvaluationContext.empty(), null, ExecuteProperties.SERIAL_EXECUTE)) {
                cursor.asStream().forEach(result -> actualMessages.add(result.getMessage()));
            }
        }
        Assertions.assertEquals(expectedMessages, actualMessages);
    }

}
