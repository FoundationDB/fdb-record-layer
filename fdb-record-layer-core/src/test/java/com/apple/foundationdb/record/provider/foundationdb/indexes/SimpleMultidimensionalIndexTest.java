/*
 * SimpleMultidimensionalIndexTest.java
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.Random;
import java.util.stream.Stream;

import static com.apple.foundationdb.async.rtree.RTree.Storage.BY_NODE;
import static com.apple.foundationdb.async.rtree.RTree.Storage.BY_SLOT;

/**
 * Simple tests for Multidimensional Index.
 */
class SimpleMultidimensionalIndexTest extends MultidimensionalIndexTestBase {

    static Stream<Arguments> argumentsForBasicReads() {
        return Stream.of(
                Arguments.of(BY_NODE.toString(), false, false),
                Arguments.of(BY_NODE.toString(), false, true),
                Arguments.of(BY_NODE.toString(), true, false),
                Arguments.of(BY_NODE.toString(), true, true),
                Arguments.of(BY_SLOT.toString(), false, false),
                Arguments.of(BY_SLOT.toString(), false, true),
                Arguments.of(BY_SLOT.toString(), true, false),
                Arguments.of(BY_SLOT.toString(), true, true));
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
        return Stream.of(
                Arguments.of(random.nextLong(), 100, BY_SLOT.toString(), false, false),
                Arguments.of(random.nextLong(), 100, BY_SLOT.toString(), false, true),
                Arguments.of(random.nextLong(), 100, BY_SLOT.toString(), true, false),
                Arguments.of(random.nextLong(), 100, BY_SLOT.toString(), true, true),
                Arguments.of(random.nextLong(), 100, BY_NODE.toString(), false, false),
                Arguments.of(random.nextLong(), 100, BY_NODE.toString(), false, true),
                Arguments.of(random.nextLong(), 100, BY_NODE.toString(), true, false),
                Arguments.of(random.nextLong(), 100, BY_NODE.toString(), true, true),
                Arguments.of(random.nextLong(), 500, BY_SLOT.toString(), false, false),
                Arguments.of(random.nextLong(), 500, BY_SLOT.toString(), false, true),
                Arguments.of(random.nextLong(), 500, BY_SLOT.toString(), true, false),
                Arguments.of(random.nextLong(), 500, BY_SLOT.toString(), true, true),
                Arguments.of(random.nextLong(), 500, BY_NODE.toString(), false, false),
                Arguments.of(random.nextLong(), 500, BY_NODE.toString(), false, true),
                Arguments.of(random.nextLong(), 500, BY_NODE.toString(), true, false),
                Arguments.of(random.nextLong(), 500, BY_NODE.toString(), true, true),
                // large values only for default config
                Arguments.of(random.nextLong(), 1000, BY_NODE.toString(), true, false),
                Arguments.of(random.nextLong(), 5000, BY_NODE.toString(), true, false)
        );
    }

    static Stream<Arguments> argumentsForIndexReadsAfterDeletes() {
        final Random random = new Random(System.currentTimeMillis());
        return Stream.of(
                Arguments.of(random.nextLong(), 10, random.nextInt(10) + 1, BY_SLOT.toString(), false, false),
                Arguments.of(random.nextLong(), 10, random.nextInt(10) + 1, BY_SLOT.toString(), false, true),
                Arguments.of(random.nextLong(), 10, random.nextInt(10) + 1, BY_SLOT.toString(), true, false),
                Arguments.of(random.nextLong(), 10, random.nextInt(10) + 1, BY_SLOT.toString(), true, true),
                Arguments.of(random.nextLong(), 100, random.nextInt(100) + 1, BY_SLOT.toString(), false, false),
                Arguments.of(random.nextLong(), 100, random.nextInt(100) + 1, BY_SLOT.toString(), false, true),
                Arguments.of(random.nextLong(), 100, random.nextInt(100) + 1, BY_SLOT.toString(), true, false),
                Arguments.of(random.nextLong(), 100, random.nextInt(100) + 1, BY_SLOT.toString(), true, true),
                Arguments.of(random.nextLong(), 300, random.nextInt(300) + 1, BY_SLOT.toString(), false, false),
                Arguments.of(random.nextLong(), 300, random.nextInt(300) + 1, BY_SLOT.toString(), false, true),
                Arguments.of(random.nextLong(), 300, random.nextInt(300) + 1, BY_SLOT.toString(), true, false),
                Arguments.of(random.nextLong(), 300, random.nextInt(300) + 1, BY_SLOT.toString(), true, true),
                Arguments.of(random.nextLong(), 10, random.nextInt(10) + 1, BY_NODE.toString(), false, false),
                Arguments.of(random.nextLong(), 10, random.nextInt(10) + 1, BY_NODE.toString(), false, true),
                Arguments.of(random.nextLong(), 10, random.nextInt(10) + 1, BY_NODE.toString(), true, false),
                Arguments.of(random.nextLong(), 10, random.nextInt(10) + 1, BY_NODE.toString(), true, true),
                Arguments.of(random.nextLong(), 100, random.nextInt(100) + 1, BY_NODE.toString(), false, false),
                Arguments.of(random.nextLong(), 100, random.nextInt(100) + 1, BY_NODE.toString(), false, true),
                Arguments.of(random.nextLong(), 100, random.nextInt(100) + 1, BY_NODE.toString(), true, false),
                Arguments.of(random.nextLong(), 100, random.nextInt(100) + 1, BY_NODE.toString(), true, true),
                Arguments.of(random.nextLong(), 300, random.nextInt(300) + 1, BY_NODE.toString(), false, false),
                Arguments.of(random.nextLong(), 300, random.nextInt(300) + 1, BY_NODE.toString(), false, true),
                Arguments.of(random.nextLong(), 300, random.nextInt(300) + 1, BY_NODE.toString(), true, false),
                Arguments.of(random.nextLong(), 300, random.nextInt(300) + 1, BY_NODE.toString(), true, true),
                // large values only for default config
                Arguments.of(random.nextLong(), 1000, random.nextInt(1000) + 1, BY_NODE.toString(), true, false),
                Arguments.of(random.nextLong(), 5000, random.nextInt(1000) + 1, BY_NODE.toString(), true, false)
        );
    }

    /**
     * Arguments for index reads using an IN clause. Note that for each run we run the tests with a different seed.
     * That is intentionally done in a way that the seed itself is handed over to the test case which causes that seed
     * to be reported. If a testcase fails, the particular seed reported can be used here to recreate the exact
     * conditions of the failure.
     *
     * @return a stream of arguments
     */
    static Stream<Arguments> argumentsForIndexReadWithIn() {
        final Random random = new Random(System.currentTimeMillis());
        return Stream.of(
                Arguments.of(random.nextLong(), 100, 10),
                Arguments.of(random.nextLong(), 100, 100),
                Arguments.of(random.nextLong(), 100, 1000),
                Arguments.of(random.nextLong(), 500, 200),
                Arguments.of(random.nextLong(), 500, 500),
                Arguments.of(random.nextLong(), 500, 1000)
        );
    }

    static Stream<Arguments> argumentsForIndexReadsWithDuplicates() {
        return Stream.of(
                Arguments.of(100, BY_SLOT.toString(), false, false),
                Arguments.of(100, BY_SLOT.toString(), false, true),
                Arguments.of(100, BY_SLOT.toString(), true, false),
                Arguments.of(100, BY_SLOT.toString(), true, true),
                Arguments.of(500, BY_SLOT.toString(), false, false),
                Arguments.of(500, BY_SLOT.toString(), false, true),
                Arguments.of(500, BY_SLOT.toString(), true, false),
                Arguments.of(500, BY_SLOT.toString(), true, true),
                Arguments.of(100, BY_NODE.toString(), false, false),
                Arguments.of(100, BY_NODE.toString(), false, true),
                Arguments.of(100, BY_NODE.toString(), true, false),
                Arguments.of(100, BY_NODE.toString(), true, true),
                Arguments.of(500, BY_NODE.toString(), false, false),
                Arguments.of(500, BY_NODE.toString(), false, true),
                Arguments.of(500, BY_NODE.toString(), true, false),
                Arguments.of(500, BY_NODE.toString(), true, true),
                // large values only for default config
                Arguments.of(1000, BY_NODE.toString(), true, false),
                Arguments.of(5000, BY_NODE.toString(), true, false)
        );
    }


    @ParameterizedTest
    @MethodSource("argumentsForBasicReads")
    void basicReadTest(@Nonnull final String storage, final boolean storeHilbertValues, final boolean useNodeSlotIndex)
            throws Exception {
        super.basicReadTest(false, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @ParameterizedTest
    @MethodSource("argumentsForBasicReads")
    void basicReadWithNullsTest(@Nonnull final String storage, final boolean storeHilbertValues,
                                final boolean useNodeSlotIndex) throws Exception {
        super.basicReadWithNullsTest(false, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @ParameterizedTest
    @MethodSource("argumentsForBasicReads")
    void deleteWhereTest(@Nonnull final String storage, final boolean storeHilbertValues, final boolean useNodeSlotIndex)
            throws Exception {
        super.deleteWhereTest(false, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @ParameterizedTest
    @MethodSource("argumentsForBasicReads")
    void coveringIndexScanWithFetchTest(@Nonnull final String storage, final boolean storeHilbertValues,
                                        final boolean useNodeSlotIndex) throws Exception {
        super.coveringIndexScanWithFetchTest(false, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @ParameterizedTest
    @MethodSource("argumentsForIndexReads")
    void indexReadTest(final long seed, final int numRecords, @Nonnull final String storage,
                       final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        super.indexReadTest(false, seed, numRecords, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @ParameterizedTest
    @MethodSource("argumentsForIndexReads")
    void indexReadWithNullsTest(final long seed, final int numRecords, @Nonnull final String storage,
                                final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        super.indexReadWithNullsTest(false, seed, numRecords, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @ParameterizedTest
    @MethodSource("argumentsForIndexReads")
    void indexReadIsNullTest(final long seed, final int numRecords, @Nonnull final String storage,
                             final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        super.indexReadIsNullTest(false, seed, numRecords, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @ParameterizedTest
    @MethodSource("argumentsForIndexReads")
    void coveringIndexReadTest(final long seed, final int numRecords, @Nonnull final String storage,
                               final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        super.coveringIndexReadTest(false, seed, numRecords, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @ParameterizedTest
    @MethodSource("argumentsForIndexReads")
    void indexScan3DTest(final long seed, final int numRecords, @Nonnull final String storage,
                         final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        super.indexScan3DTest(false, seed, numRecords, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @ParameterizedTest
    @MethodSource("argumentsForIndexReads")
    void unprefixedIndexReadTest(final long seed, final int numRecords, @Nonnull final String storage,
                                 final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        super.unprefixedIndexReadTest(false, seed, numRecords, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @ParameterizedTest
    @MethodSource("argumentsForIndexReads")
    void unprefixedSuffixedIndexReadTest(final long seed, final int numRecords, @Nonnull final String storage,
                                         final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        super.unprefixedSuffixedIndexReadTest(false, seed, numRecords, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @ParameterizedTest
    @MethodSource("argumentsForIndexReads")
    void indexReadWithAdditionalValueTest(final long seed, final int numRecords, @Nonnull final String storage,
                                          final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        super.indexReadWithAdditionalValueTest(false, seed, numRecords, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @ParameterizedTest
    @MethodSource("argumentsForIndexReads")
    void unprefixedSuffixedIndexReadWithResidualsTest(final long seed, final int numRecords, @Nonnull final String storage,
                                                      final boolean storeHilbertValues, final Boolean useNodeSlotIndex) throws Exception {
        super.unprefixedSuffixedIndexReadWithResidualsTest(false, seed, numRecords, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @ParameterizedTest
    @MethodSource("argumentsForIndexReads")
    void indexSkipScanTest(final long seed, final int numRecords, @Nonnull final String storage,
                           final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        super.indexSkipScanTest(false, seed, numRecords, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @ParameterizedTest
    @MethodSource("argumentsForIndexReadsAfterDeletes")
    void indexReadsAfterDeletesTest(final long seed, final int numRecords, final int numDeletes,
                                    @Nonnull final String storage, final boolean storeHilbertValues,
                                    final boolean useNodeSlotIndex) throws Exception {
        super.indexReadsAfterDeletesTest(false, seed, numRecords, numDeletes, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @ParameterizedTest
    @MethodSource("argumentsForIndexReadsWithDuplicates")
    void indexReadWithDuplicatesTest(final int numRecords, @Nonnull final String storage,
                                     final boolean storeHilbertValues, final boolean useNodeSlotIndex) throws Exception {
        super.indexReadWithDuplicatesTest(false, numRecords, storage, storeHilbertValues, useNodeSlotIndex);
    }

    @Test
    void continuationTest() throws Exception {
        super.continuationTest(false);
    }

    @Test
    void wrongDimensionTypes() {
        super.wrongDimensionTypes(false);
    }

    @Test
    void indexReadWithNullsAndMinsTest1() throws Exception {
        super.indexReadWithNullsAndMinsTest1(false);
    }

    @Test
    void indexReadWithNullsAndMinsTest2() throws Exception {
        super.indexReadWithNullsAndMinsTest2(false);
    }

    @ParameterizedTest
    @MethodSource("argumentsForIndexReadWithIn")
    void indexReadWithIn(final long seed, final int numRecords, final int numIns) throws Exception {
        super.indexReadWithIn(false, seed, numRecords, numIns);
    }
}
