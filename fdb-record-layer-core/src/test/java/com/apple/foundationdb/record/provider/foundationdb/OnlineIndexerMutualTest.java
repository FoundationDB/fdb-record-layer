/*
 * OnlineIndexerMutualTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.Range;
import com.apple.foundationdb.record.IndexBuildProto;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordIndexUniquenessViolation;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for mutually building indexes {@link OnlineIndexer}.
 */
class OnlineIndexerMutualTest extends OnlineIndexerTest  {
    private static final Logger LOGGER = LoggerFactory.getLogger(OnlineIndexerMutualTest.class);

    private void populateData(final long numRecords) {
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, numRecords).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(val)
                        .setNumValue2((int)val * 19)
                        .setNumValue3Indexed((int) val * 77)
                        .setNumValueUnique((int)val * 1139)
                        .build()
        ).collect(Collectors.toList());

        try (FDBRecordContext context = openContext())  {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }
    }

    private void populateOtherData(final long numRecords, final long start) {
        List<TestRecords1Proto.MyOtherRecord> records = LongStream.range(0, numRecords).mapToObj(val ->
                TestRecords1Proto.MyOtherRecord.newBuilder()
                        .setRecNo(val + start)
                        .setNumValue2((int) val * 1033)
                        .setNumValue3Indexed((int)val * 11111)
                        .build()
        ).collect(Collectors.toList());

        try (FDBRecordContext context = openContext())  {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }
    }

    private void assertAllReadable(List<Index> indexes) {
        openSimpleMetaData(allIndexesHook(indexes));
        try (FDBRecordContext context = openContext()) {
            for (Index index : indexes) {
                assertTrue(recordStore.isIndexReadable(index));
            }
            context.commit();
        }
    }

    private List<Tuple> getBoundariesList(final long numRecords, final long step) {
        List<Tuple> boundaries = new ArrayList<>();
        boundaries.add(null);
        for (long i = step; i < numRecords; i += step) {
            final TestRecords1Proto.MySimpleRecord rec = TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(i).build();
            final RecordType recordType = metaData.getRecordTypeForDescriptor(rec.getDescriptorForType());
            final KeyExpression primaryKeyExpression = recordType.getPrimaryKey();
            final FDBStoredRecordBuilder<TestRecords1Proto.MySimpleRecord> recordBuilder = FDBStoredRecord.newBuilder(rec).setRecordType(recordType);
            final Tuple primaryKey = primaryKeyExpression.evaluateSingleton(recordBuilder).toTuple();
            boundaries.add(primaryKey);
        }
        boundaries.add(null);
        return boundaries;
    }

    private static FDBRecordStoreTestBase.RecordMetaDataHook allIndexesHook(List<Index> indexes) {
        return metaDataBuilder -> {
            for (Index index: indexes) {
                metaDataBuilder.addIndex("MySimpleRecord", index);
            }
        } ;
    }

    @Test
    void testMutualIndexingNoBoundaries() {
        // Let a single thread build all the indexes - boundaries will be detected automatically - which means (null, null) because the data set will be too small to have multiple shards in fdb
        final FDBStoreTimer timer = new FDBStoreTimer();

        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));

        openSimpleMetaData();
        long numRecords = 80;
        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                .setTargetIndexes(indexes)
                .setTimer(timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setMutualIndexing() // no boundaries mean self detection - which will be no boundaries
                        .build())
                .build()) {

            indexBuilder.buildIndex(true);
        }
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
        assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        assertAllReadable(indexes);
        validateIndexes(indexes);
    }

    @ParameterizedTest
    @CsvSource({
            // single threads:
            "0, 103, 10",
            "0, 417, 17",
            "0, 1417, 157",
            "0, 40, 2", // small fragments
            "0, 30, 1", // smaller fragments
            // multi threads:
            "4, 103, 17",
            "40, 773, 14",
            "20, 299, 19",
            "3, 40, 2", // small fragments
            "3, 30, 1", // smaller fragments
    })
    void testMutualIndexing(int numThreads, long numRecords, long boundarySize) {
        // build indexing by boundaries.
        // If numThreads < 2 - do it in a single thread.
        // Else, perform it in parallel by multiple threads
        List<Index> indexes = new ArrayList<>();
        // Here: Value indexes only
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        // Here: Add a non-value index
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));

        openSimpleMetaData();
        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);
        final FDBStoreTimer timer = new FDBStoreTimer();
        final List<Tuple> boundariesList = getBoundariesList(numRecords, boundarySize);
        if (numThreads < 2) {
            oneThreadIndexing(indexes, timer, boundariesList);
        } else {
            IntStream range = IntStream.rangeClosed(0, numThreads);
            final AtomicInteger nonIdle = new AtomicInteger(0);
            range.parallel().forEach(ignore -> {
                int numScanned = oneThreadIndexing(indexes, null, boundariesList);
                if (numScanned > 0) {
                    nonIdle.addAndGet(1);
                }
            });
            // require at least two parallel indexing processes to perform real work
            assertTrue(nonIdle.get() > 1);
        }

        if (numThreads < 2) {
            assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED));
            assertEquals(numRecords, timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED));
        }
        assertAllReadable(indexes);
        validateIndexes(indexes);
    }

    int oneThreadIndexing(List<Index> indexes, FDBStoreTimer callerTimer, List<Tuple> boundaries) {
        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        final FDBStoreTimer timer = callerTimer != null ? callerTimer : new FDBStoreTimer();
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                .setTargetIndexes(indexes)
                .setTimer(timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setMutualIndexingBoundaries(boundaries)
                        .build())
                .build()) {
            indexBuilder.buildIndex(true);
        }
        int numScanned = timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_SCANNED);
        if (callerTimer == null && LOGGER.isInfoEnabled()) {
            LOGGER.info(KeyValueLogMessage.of("oneThreadIndexing",
                    LogMessageKeys.RECORDS_SCANNED, numScanned,
                    "tid", Thread.currentThread().getId()
            ));
        }
        return numScanned;
    }

    void oneThreadIndexingCrashHalfway(List<Index> indexes, FDBStoreTimer timer, List<Tuple> boundaries, int after) {
        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        final String testThrowMsg = "Intentionally crash during test";
        final AtomicLong counter = new AtomicLong(0);

        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                .setTargetIndexes(indexes)
                .setTimer(timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setMutualIndexingBoundaries(boundaries)
                        .build())
                .setConfigLoader(old -> {
                    if (counter.incrementAndGet() > after) {
                        throw new RecordCoreException(testThrowMsg);
                    }
                    return old;
                })
                .build()) {
            RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains(testThrowMsg));
        }
    }

    @Test
    @Tag(Tags.Slow)
    void testMutualIndexingCrashFewThreads() {
        // Force few of the threads to crash during indexing. Note that this is not a stable test - as there
        // is a small probability that "building" threads will build all or most of the index before a "crashing"
        // thread will get a chance to crash - and then fail assertion.
        // If that ever happens, we'll handle it (or disable this test).
        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));
        Index unusedIndex = new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE);

        int numRecords = 543;
        openSimpleMetaData();
        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);
        final FDBStoreTimer timer = new FDBStoreTimer();
        int boundarySize = 20;
        final List<Tuple> boundariesList = getBoundariesList(numRecords, boundarySize);

        // Crash the odd ones
        IntStream.rangeClosed(1, 9).parallel().forEach(i -> {
            if (0 == (i & 1)) {
                oneThreadIndexing(indexes, timer, boundariesList);
            } else {
                oneThreadIndexingCrashHalfway(indexes, timer, boundariesList, 1);
            }
        });

        // validate
        assertAllReadable(indexes);
        validateIndexes(indexes);
    }

    @Test
    void testMutualIndexingCrashAndContinue() {
        // Start building with multi threads, crash all
        // Continue with other threads, crash them too
        // Successfully finish indexing with other threads
        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));

        int numRecords = 412;
        openSimpleMetaData();
        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);
        final FDBStoreTimer timer = new FDBStoreTimer();
        int boundarySize = 10;
        final List<Tuple> boundariesList = getBoundariesList(numRecords, boundarySize);

        // First crash, 8 threads, crash after 1:
        IntStream.rangeClosed(0, 8).parallel().forEach(ignore ->
                oneThreadIndexingCrashHalfway(indexes, timer, boundariesList, 1));

        // Second crash: 3 threads, crash after i
        IntStream.rangeClosed(0, 3).parallel().forEach(i ->
                oneThreadIndexingCrashHalfway(indexes, timer, boundariesList, i));

        // Now succeed: 10 threads
        IntStream.rangeClosed(0, 10).parallel().forEach(ignore ->
                oneThreadIndexing(indexes, timer, boundariesList));

        // validate
        assertAllReadable(indexes);
        validateIndexes(indexes);
    }

    @Test
    void testMutualIndexingCrashAndRefuseContinueNonMutually() {
        // Start building with multi threads, crash all
        // Make sure that the regular indexing is blocked
        // Finish indexing, just for fun.
        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));

        int numRecords = 232;
        openSimpleMetaData();
        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);
        final FDBStoreTimer timer = new FDBStoreTimer();
        int boundarySize = 10;
        final List<Tuple> boundariesList = getBoundariesList(numRecords, boundarySize);

        // First crash, 10 threads, crash after 1:
        IntStream.rangeClosed(0, 10).parallel().forEach(ignore ->
                oneThreadIndexingCrashHalfway(indexes, timer, boundariesList, 1));

        // Fail to build with a regular indexer
        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                .setTargetIndexes(indexes)
                .setTimer(timer)
                .build()) {

            RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("This index was partly built by another method"));
        }

        // Successfully build
        IntStream.rangeClosed(0, 30).parallel().forEach(ignore ->
                oneThreadIndexing(indexes, timer, boundariesList));

        // validate
        assertAllReadable(indexes);
        validateIndexes(indexes);
    }

    @Test
    void testMutualIndexingCrashAndAllowContinueNonMutually() {
        // Start building with multi threads, crash all
        // Make sure that the regular indexing is unblocked
        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));

        int numRecords = 132;
        openSimpleMetaData();
        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);
        final FDBStoreTimer timer = new FDBStoreTimer();
        int boundarySize = 11;
        final List<Tuple> boundariesList = getBoundariesList(numRecords, boundarySize);

        // First crash, 5 threads, crash after 1:
        IntStream.rangeClosed(0, 5).parallel().forEach(ignore ->
                oneThreadIndexingCrashHalfway(indexes, timer, boundariesList, 1));

        // Build with a regular indexer, allow takeover
        openSimpleMetaData(hook);
        for (Index index: indexes) {
            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                    .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                    .setIndex(index)
                    .setTimer(timer)
                    .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                            .allowTakeoverContinue()
                            .build())
                    .build()) {
                indexBuilder.buildIndex();
            }
        }

        // validate
        assertAllReadable(indexes);
        validateIndexes(indexes);
    }

    @Test
    void testMutualIndexingWeirdBoundaries() {
        // test some boundaries end cases
        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));

        int numRecords = 100;
        openSimpleMetaData();
        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);
        final FDBStoreTimer timer = new FDBStoreTimer();
        int boundarySize = 10;
        final List<Tuple> boundariesList = getBoundariesList(numRecords, boundarySize);
        assertEquals(11, boundariesList.size());

        // Add null in the middle, causing fragments to overlap
        boundariesList.add(0, null);
        // Build and validate
        IntStream.rangeClosed(0, 8).parallel().forEach(ignore ->
                oneThreadIndexing(indexes, timer, boundariesList));
        assertAllReadable(indexes);
        validateIndexes(indexes);

        disableAll(indexes);
        // Duplicate entry, causing empty fragments
        boundariesList.add(7, boundariesList.get(7));
        boundariesList.add(10, boundariesList.get(10));
        boundariesList.add(10, boundariesList.get(10));

        // Build and validate
        IntStream.rangeClosed(0, 3).parallel().forEach(ignore ->
                oneThreadIndexing(indexes, timer, boundariesList));
        assertAllReadable(indexes);
        validateIndexes(indexes);

        // pad with nulls, causing more empty fragments
        disableAll(indexes);
        boundariesList.add(0, null);
        boundariesList.add(boundariesList.size() - 1, null);

        // Build and validate
        IntStream.rangeClosed(0, 18).parallel().forEach(ignore ->
                oneThreadIndexing(indexes, timer, boundariesList));
        assertAllReadable(indexes);
        validateIndexes(indexes);
    }

    @Test
    @Tag(Tags.Slow)
    void testMutualIndexingWithEmptyFragments() {
        // repeat testing boundaries end cases, but when most boundaries (well, fragments) contain no actual records
        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));

        openSimpleMetaData();
        List<TestRecords1Proto.MySimpleRecord> headRecords = LongStream.range(0, 100).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(val)
                        .setNumValue2((int)val * 19)
                        .setNumValue3Indexed((int) val * 77)
                        .setNumValueUnique((int)val * 1139)
                        .build()
        ).collect(Collectors.toList());
        List<TestRecords1Proto.MySimpleRecord> tailRecords = LongStream.range(938, 1000).mapToObj(val ->
                TestRecords1Proto.MySimpleRecord.newBuilder()
                        .setRecNo(val)
                        .setNumValue2((int)val * 19)
                        .setNumValue3Indexed((int) val * 77)
                        .setNumValueUnique((int)val * 1139)
                        .build()
        ).collect(Collectors.toList());

        try (FDBRecordContext context = openContext())  {
            headRecords.forEach(recordStore::saveRecord);
            tailRecords.forEach(recordStore::saveRecord);
            context.commit();
        }

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);
        final FDBStoreTimer timer = new FDBStoreTimer();
        int boundarySize = 10;
        int pseudoNumRecords = 1000;
        final List<Tuple> boundariesList = getBoundariesList(pseudoNumRecords, boundarySize);
        assertEquals(101, boundariesList.size());

        // Add null in the middle, causing fragments to overlap
        boundariesList.add(0, null);
        // Build and validate
        IntStream.rangeClosed(0, 8).parallel().forEach(ignore ->
                oneThreadIndexing(indexes, timer, boundariesList));
        assertAllReadable(indexes);
        validateIndexes(indexes);

        disableAll(indexes);
        // Duplicate entry, causing empty fragments
        boundariesList.add(7, boundariesList.get(7));
        boundariesList.add(10, boundariesList.get(9));
        boundariesList.add(10, boundariesList.get(9));

        // Build and validate
        IntStream.rangeClosed(0, 3).parallel().forEach(ignore ->
                oneThreadIndexing(indexes, timer, boundariesList));
        assertAllReadable(indexes);
        validateIndexes(indexes);

        // pad with nulls, causing more empty fragments
        disableAll(indexes);
        boundariesList.add(0, null);
        boundariesList.add(boundariesList.size() - 1, null);

        // Build and validate
        IntStream.rangeClosed(0, 18).parallel().forEach(ignore ->
                oneThreadIndexing(indexes, timer, boundariesList));
        assertAllReadable(indexes);
        validateIndexes(indexes);
    }

    @Test
    void testSortAndSquash() {
        List<byte[]> points = new ArrayList<>();
        points.add(byteEmpty());
        for (int i = 2; i < 0xff; i += 3) {
            points.add(byteOf(i));
        }
        points.add(byteOf(0xff));
        List<Range> ranges = new ArrayList<>();
        for (int i = 0; i < points.size() - 1; i++) {
            ranges.add(new Range(points.get(i), points.get(i + 1)));
        }
        List<Range> partial = new ArrayList<>();
        for (int i = 1 ; i < ranges.size(); i += 4) {
            partial.add(ranges.get(i));
        }
        // test squash
        Collections.shuffle(ranges);
        List<Range> squashed = IndexingMutuallyByRecords.sortAndSquash(ranges);
        assertEquals(1, squashed.size());
        assertEquals(points.get(0), squashed.get(0).begin);
        assertEquals(points.get(points.size() - 1), squashed.get(0).end);

        // test sort without squash
        List<Range> partialShuffled = new ArrayList<>(partial);
        Collections.shuffle(partialShuffled);
        squashed = IndexingMutuallyByRecords.sortAndSquash(partialShuffled);
        assertEquals(partial, squashed);

        // test sparse overlaps
        List<Range> sparse = new ArrayList<>(Arrays.asList(rangeOf(0, 9), rangeOf(20, 29), rangeOf(100, 110)));
        List<Range> sparseShuffled = new ArrayList<>(Arrays.asList(
                rangeOf(0, 7), rangeOf(3, 4), rangeOf(1, 8), rangeOf(8, 9),
                rangeOf(20, 21), rangeOf(21, 29),
                rangeOf(100, 110), rangeOf(100, 101), rangeOf(101, 108)));
        Collections.shuffle(sparseShuffled);
        squashed = IndexingMutuallyByRecords.sortAndSquash(sparseShuffled);
        assertEquals(sparse, squashed);
    }

    @Test
    void testFullyUnBuiltRange() {
        List<Range> ranges = new ArrayList<>();
        ranges.add(rangeOf(0, 9));
        ranges.add(rangeOf(20, 29));
        ranges.add(rangeOf(40, 49));
        // fully unbuilt
        checkFully(ranges, 0, 9);
        checkFully(ranges, 0, 8);
        checkFully(ranges, 41, 49);
        checkFully(ranges, 23, 24);
        // not fully unbuilt
        assertNull(IndexingMutuallyByRecords.fullyUnBuiltRange(ranges, rangeOf(0, 10)));
        assertNull(IndexingMutuallyByRecords.fullyUnBuiltRange(ranges, rangeOf(8, 10)));
        assertNull(IndexingMutuallyByRecords.fullyUnBuiltRange(ranges, rangeOf(9, 10)));
        assertNull(IndexingMutuallyByRecords.fullyUnBuiltRange(ranges, rangeOf(100, 110)));
        assertNull(IndexingMutuallyByRecords.fullyUnBuiltRange(ranges, rangeOf(0, 20)));
        assertNull(IndexingMutuallyByRecords.fullyUnBuiltRange(ranges, rangeOf(20, 300)));
        assertNull(IndexingMutuallyByRecords.fullyUnBuiltRange(ranges, rangeOf(41, 50)));
        assertNull(IndexingMutuallyByRecords.fullyUnBuiltRange(ranges, rangeOf(20, 44)));
    }

    @Test
    void testPartlyUnBuiltRange() {
        List<Range> ranges = new ArrayList<>();
        ranges.add(rangeOf(0, 9));
        ranges.add(rangeOf(20, 29));
        ranges.add(rangeOf(40, 49));
        // fully unbuilt
        checkPartial(ranges, 0, 9, 0, 9);
        checkPartial(ranges, 0, 8, 0, 8);
        checkPartial(ranges, 41, 49, 41, 49);
        checkPartial(ranges, 23, 24, 23, 24);
        // partly unbuilt
        checkPartial(ranges, 14, 24, 20, 24);
        checkPartial(ranges, 0, 12, 0, 9);
        checkPartial(ranges, 0, 100, 0, 9);
        checkPartial(ranges, 40, 100, 40, 49);
        // no overlap
        assertNull(IndexingMutuallyByRecords.partlyUnBuiltRange(ranges, rangeOf(10, 11)));
        assertNull(IndexingMutuallyByRecords.partlyUnBuiltRange(ranges, rangeOf(100, 200)));
        assertNull(IndexingMutuallyByRecords.partlyUnBuiltRange(ranges, rangeOf(33, 40)));
    }

    private static void checkFully(List<Range> ranges, int rangeStart, int rangeEnd) {
        Range res = IndexingMutuallyByRecords.fullyUnBuiltRange(ranges, rangeOf(rangeStart, rangeEnd));
        assertNotNull(res);
        assertEquals(res.begin[0], byteOf(rangeStart)[0]);
        assertEquals(res.end[0], byteOf(rangeEnd)[0]);
    }

    private static void checkPartial(List<Range> ranges, int rangeStart, int rangeEnd, int expectStart, int expectEnd) {
        Range res = IndexingMutuallyByRecords.partlyUnBuiltRange(ranges, rangeOf(rangeStart, rangeEnd));
        assertNotNull(res);
        assertEquals(res.begin[0], byteOf(expectStart)[0]);
        assertEquals(res.end[0], byteOf(expectEnd)[0]);
    }

    private static Range rangeOf(int start, int end) {
        assertTrue(start < end);
        return new Range(byteOf(start), byteOf(end));
    }

    private static byte[] byteOf(int i) {
        return new byte[]{(byte) i};
    }

    private static byte[] byteEmpty() {
        return new byte[0];
    }

    @ParameterizedTest
    @CsvSource({
            // numRecords must be even
            "false, 3, 100, 7",
            "true, 12, 150, 9",
            "true, 5, 140, 14",
            "false, 5, 78, 14",
            "true, 20, 202, 14",
    })
    void testUniquenessMultiTarget(boolean allowUniquePending, int numThreads, int numRecords, int boundarySize) {
        assertEquals(0, (numRecords & 1)); // must be an even number
        List<TestRecords1Proto.MySimpleRecord> records = LongStream.range(0, numRecords).mapToObj( val ->
                TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(val)
                        .setNumValue2(((int)val) % (numRecords / 2))
                        .setNumValue3Indexed((int) val * 7)
                        .setNumValueUnique((int)val * 119)
                        .build()
        ).collect(Collectors.toList());

        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);

        openSimpleMetaData();
        try (FDBRecordContext context = openContext())  {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }
        // build indexes, "indexA" should have a uniqueness violation
        openSimpleMetaData(hook);
        disableAll(indexes);
        final List<Tuple> boundaries = getBoundariesList(numRecords, boundarySize);
        IntStream range = IntStream.rangeClosed(0, numThreads);
        range.parallel().forEach(ignore -> {
            openSimpleMetaData(allIndexesHook(indexes));
            try (OnlineIndexer indexBuilder = newIndexerBuilder()
                    .setTargetIndexes(indexes)
                    .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                            .setMutualIndexingBoundaries(boundaries)
                            .allowUniquePendingState(allowUniquePending))
                    .build()) {
                if (allowUniquePending) {
                    indexBuilder.buildIndex();
                } else {
                    buildIndexAssertThrowUniquenessViolationOrValidation(indexBuilder);
                }
            }
        });

        try (FDBRecordContext context = openContext()) {
            // unique index with uniqueness violation:
            assertEquals(numRecords, (int)recordStore.scanUniquenessViolations(indexes.get(0)).getCount().join());
            if (allowUniquePending) {
                assertTrue(recordStore.isIndexReadableUniquePending(indexes.get(0)));
                final List<IndexEntry> scanned = recordStore.scanIndex(indexes.get(0), IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                        .asList().join();
                assertEquals(scanned.size(), records.size());
                List<Long> numValues = records.stream().map(TestRecords1Proto.MySimpleRecord::getNumValue2).map(Integer::longValue).collect(Collectors.toList());
                List<Long> scannedValues = scanned.stream().map(IndexEntry::getKey).map(tuple -> tuple.getLong(0)).collect(Collectors.toList());
                assertTrue(numValues.containsAll(scannedValues));
                assertTrue(scannedValues.containsAll(numValues));
            } else {
                assertTrue(recordStore.isIndexWriteOnly(indexes.get(0)));
                RecordCoreException e = assertThrows(ScanNonReadableIndexException.class,
                        () -> recordStore.scanIndex(indexes.get(0), IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN));
                assertTrue(e.getMessage().contains("Cannot scan non-readable index"));
            }
            // non-unique index:
            assertTrue(recordStore.isIndexReadable(indexes.get(1)));
            // unique index of unique numbers:
            assertTrue(recordStore.isIndexReadable(indexes.get(2)));
            context.commit();
        }

        // now try resolving the duplications, and marking readable with another build
        final Index index = indexes.get(0);
        try (FDBRecordContext context = openContext()) {
            Set<Tuple> indexEntries = new HashSet<>(recordStore.scanUniquenessViolations(index)
                    .map( v -> v.getIndexEntry().getKey() )
                    .asList().join());

            for (Tuple indexKey : indexEntries) {
                List<Tuple> primaryKeys = recordStore.scanUniquenessViolations(index, indexKey).map(RecordIndexUniquenessViolation::getPrimaryKey).asList().join();
                assertEquals(2, primaryKeys.size());
                recordStore.resolveUniquenessViolation(index, indexKey, primaryKeys.get(0)).join();
                assertEquals(0, (int)recordStore.scanUniquenessViolations(index, indexKey).getCount().join());
            }

            for (int i = 0; i < numRecords / 2; i++) {
                assertNotNull(recordStore.loadRecord(Tuple.from(i)));
            }
            for (int i = numRecords / 2; i < records.size(); i++) {
                assertNull(recordStore.loadRecord(Tuple.from(i)));
            }
            context.commit();
        }
        openSimpleMetaData(hook);
        openSimpleMetaData(allIndexesHook(indexes));
        try (OnlineIndexer indexBuilder = newIndexerBuilder()
                .setIndex(index)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .allowTakeoverContinue()
                        .allowUniquePendingState(allowUniquePending))
                .build()) {
            indexBuilder.buildIndex();
        }
        assertAllReadable(indexes);
    }

    private void buildIndexAssertThrowUniquenessViolationOrValidation(OnlineIndexer indexer) {
        indexer.buildIndexAsync().handle((ignore, e) -> {
            assertNotNull(e);
            RuntimeException runE = FDBExceptions.wrapException(e);
            assertNotNull(runE);
            assertTrue(runE instanceof RecordIndexUniquenessViolation ||
                       runE instanceof IndexingBase.ValidationException);
            return null;
        }).join();
    }

    @Test
    void testMultiTargetMismatchStateFailure() {
        //Throw when one index has a different status
        final long numRecords = 40;

        List<Index> indexes = new ArrayList<>();
        // Here: Value indexes only
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));

        openSimpleMetaData();
        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);

        // built one index
        try (OnlineIndexer indexer = newIndexerBuilder()
                .setIndex(indexes.get(1))
                .build()) {
            indexer.buildIndex(false);
        }

        // assert multi target failures
        final List<Tuple> boundaries = getBoundariesList(numRecords, 4);
        IntStream range = IntStream.rangeClosed(0, 10);
        range.parallel().forEach(ignore -> {
            openSimpleMetaData(allIndexesHook(indexes));
            try (OnlineIndexer indexBuilder = newIndexerBuilder()
                    .setTargetIndexes(indexes)
                    .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                            .setMutualIndexingBoundaries(boundaries))
                    .build()) {
                RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
                assertTrue(e.getMessage().contains("A target index state doesn't match the primary index state"));
            }
        });
    }

    @Test
    void testMultiTargetPartlyBuildFailure() {
        // Throw when one index has a different type stamp
        final int numRecords = 107;

        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));

        openSimpleMetaData();
        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);

        final List<Tuple> boundaries = getBoundariesList(numRecords, 4);
        // 1. partly build multi
        IntStream.rangeClosed(0, 10).parallel().forEach(ignore -> buildIndexAndCrashHalfway(indexes, boundaries, indexes));

        // 2. let one index continue ahead
        buildIndexAndCrashHalfway(indexes.subList(0, 1), null, indexes); // null do no imply mutual indexing

        // 3. assert mismatch type stamp
        IntStream.rangeClosed(0, 10).parallel().forEach(ignore -> {
            openSimpleMetaData(allIndexesHook(indexes));
            try (OnlineIndexer indexBuilder = newIndexerBuilder()
                    .setTargetIndexes(indexes)
                    .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                            .setIfMismatchPrevious(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                            .setMutualIndexingBoundaries(boundaries))
                    .build()) {
                RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
                assertTrue(e.getMessage().contains("This index was partly built by another method"));
            }
        });
    }

    @Test
    void testMultiTargetPartlyBuildChangeTargets() {
        // Throw when the index list changes
        final int numRecords = 107;

        List<Index> indexes = new ArrayList<>();
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));

        openSimpleMetaData();
        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);
        final List<Tuple> boundaries = getBoundariesList(numRecords, 4);

        // 1. partly build multi
        IntStream.rangeClosed(0, 8).parallel().forEach(ignore -> buildIndexAndCrashHalfway(indexes, boundaries, indexes));

        // 2. Change indexes set
        indexes.remove(1);

        // 3. assert mismatch type stamp
        IntStream.rangeClosed(0, 12).parallel().forEach(ignore -> {
            openSimpleMetaData(allIndexesHook(indexes));
            try (OnlineIndexer indexBuilder = newIndexerBuilder()
                    .setTargetIndexes(indexes)
                    .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                            .setIfMismatchPrevious(OnlineIndexer.IndexingPolicy.DesiredAction.ERROR)
                            .setMutualIndexingBoundaries(boundaries))
                    .build()) {
                RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
                assertTrue(e.getMessage().contains("This index was partly built by another method"));
            }
        });
    }

    private void buildIndexAndCrashHalfway(List<Index> indexes, List<Tuple> boundaries, List<Index> indexesHook) {
        // Force a RecordCoreException failure
        final String throwMsg = "Intentionally crash during test";
        openSimpleMetaData(allIndexesHook(indexesHook));
        try (OnlineIndexer indexBuilder = newIndexerBuilder()
                .setTargetIndexes(indexes)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setMutualIndexingBoundaries(boundaries))
                .setLimit(1)
                .setConfigLoader(old -> {
                    throw new RecordCoreException(throwMsg);
                })
                .build()) {

            RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains(throwMsg));
            // The index should be partially built
        }
    }

    @Test
    void testMultiTargetMultiType() {
        // Use different record types
        final int numRecords = 32;
        final int numRecordsOther = 124;
        final long start = 100;

        openSimpleMetaData();
        populateData(numRecords);
        populateOtherData(numRecordsOther, start);

        Index indexMyA = new Index("indexMyA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index indexMyB = new Index("indexMyB", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        Index indexOtherA = new Index("indexOtherA", field("num_value_2"), IndexTypes.VALUE);
        Index indexOtherB = new Index("indexOtherB", field("num_value_2"), IndexTypes.VALUE);
        final List<Index> indexes = Arrays.asList(indexMyA, indexMyB, indexOtherA, indexOtherB);

        // build indexes
        final List<Tuple> boundaries = getBoundariesList(start + numRecordsOther, 30);
        IntStream.rangeClosed(0, 8).parallel().forEach(ignore -> {
            FDBRecordStoreTestBase.RecordMetaDataHook hook = metaDataBuilder -> {
                metaDataBuilder.addIndex("MySimpleRecord", indexMyA);
                metaDataBuilder.addIndex("MySimpleRecord", indexMyB);
                metaDataBuilder.addIndex("MyOtherRecord", indexOtherA);
                metaDataBuilder.addIndex("MyOtherRecord", indexOtherB);
            };
            openSimpleMetaData(hook);
            try (OnlineIndexer indexBuilder = newIndexerBuilder()
                    .setTargetIndexes(indexes)
                    .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                            .setMutualIndexingBoundaries(boundaries))
                    .build()) {
                indexBuilder.buildIndex(true);
            }
        });
        validateIndexes(indexes);
    }

    @Test
    void testMutualIndexingBlocker() {
        // build indexes, pause by indexing stamp blocker, release unblock
        List<Index> indexes = new ArrayList<>();
        // Here: Value indexes only
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        // Here: Add a non-value index
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));

        openSimpleMetaData();
        int numRecords = 333;
        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);

        // Start indexing and crash, block indexing
        final List<Tuple> boundariesList = getBoundariesList(numRecords, 4);
        final FDBStoreTimer timer = new FDBStoreTimer();
        IntStream.rangeClosed(0, 4).parallel().forEach(id -> {
            if (id == 3) {
                FDBRecordStoreTestBase.RecordMetaDataHook localHook = allIndexesHook(indexes);
                openSimpleMetaData(localHook);
                try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                        .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                        .setTargetIndexes(indexes)
                        .build()) {
                    Thread.sleep(500); // this sleep is required to make sure that indexing had started and the indexes contain type stamps
                    indexer.indexingStamp(OnlineIndexer.IndexingStampOperation.BLOCK, null, 0);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } else {
                oneThreadIndexingCrashHalfway(indexes, timer, boundariesList, 1);
            }
        });

        // Test query, ensure blocked
        openSimpleMetaData(hook);
        try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                .setTargetIndexes(indexes)
                .build()) {
            final AbstractMap<String, IndexBuildProto.IndexBuildIndexingStamp> stampMap =
                    indexer.indexingStamp(OnlineIndexer.IndexingStampOperation.QUERY, null, 0);
            System.out.println(stampMap);
            final List<String> indexNames = indexes.stream().map(Index::getName).collect(Collectors.toList());
            assertTrue(stampMap.keySet().containsAll(indexNames));
            for (String indexName : indexNames) {
                final IndexBuildProto.IndexBuildIndexingStamp stamp = stampMap.get(indexName);
                assertTrue(stamp.getTargetIndexList().containsAll(indexNames));
                assertEquals(IndexBuildProto.IndexBuildIndexingStamp.Method.MUTUAL_BY_RECORDS, stamp.getMethod());
                assertTrue(stamp.getBlock());
            }
        }

        // Attempt continue while blocked, ensure failure
        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                .setTargetIndexes(indexes)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setMutualIndexingBoundaries(boundariesList)
                        .build())
                .build()) {
            RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("This index was partly built, and blocked"));
        }

        // Unblock, the return value is the old stamp - validate it
        openSimpleMetaData(hook);
        try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                .setTargetIndexes(indexes)
                .build()) {
            final AbstractMap<String, IndexBuildProto.IndexBuildIndexingStamp> stampMap =
                    indexer.indexingStamp(OnlineIndexer.IndexingStampOperation.UNBLOCK, null, 0);
            System.out.println(stampMap);
            final List<String> indexNames = indexes.stream().map(Index::getName).collect(Collectors.toList());
            assertTrue(stampMap.keySet().containsAll(indexNames));
            for (String indexName : indexNames) {
                final IndexBuildProto.IndexBuildIndexingStamp stamp = stampMap.get(indexName);
                assertTrue(stamp.getTargetIndexList().containsAll(indexNames));
                assertEquals(IndexBuildProto.IndexBuildIndexingStamp.Method.MUTUAL_BY_RECORDS, stamp.getMethod());
                assertTrue(stamp.getBlock());
            }
        }

        // After unblocking, finish building
        IntStream.rangeClosed(0, 3).parallel()
                .forEach(ignore -> oneThreadIndexing(indexes, timer, boundariesList));

        // Validate
        assertAllReadable(indexes);
        validateIndexes(indexes);

    }

    @Test
    void testMutualIndexingBlockerIndexingUnblock() {
        // build indexes, pause by indexing stamp blocker, continue with allowUnblock + id
        List<Index> indexes = new ArrayList<>();
        // Here: Value indexes only
        indexes.add(new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        indexes.add(new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE));
        indexes.add(new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS));
        // Here: Add a non-value index
        indexes.add(new Index("indexD", new GroupingKeyExpression(EmptyKeyExpression.EMPTY, 0), IndexTypes.COUNT));

        openSimpleMetaData();
        int numRecords = 333;
        populateData(numRecords);

        FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(indexes);
        openSimpleMetaData(hook);
        disableAll(indexes);

        // Start indexing, crash halfway
        final List<Tuple> boundariesList = getBoundariesList(numRecords, 4);
        final FDBStoreTimer timer = new FDBStoreTimer();
        IntStream.rangeClosed(0, 4).parallel().forEach(id -> {
            oneThreadIndexingCrashHalfway(indexes, timer, boundariesList, 3);
        });

        // Block with block-id
        FDBRecordStoreTestBase.RecordMetaDataHook localHook = allIndexesHook(indexes);
        openSimpleMetaData(localHook);
        String luka = "my name is Luka";
        try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                .setTargetIndexes(indexes)
                .build()) {
            indexer.indexingStamp(OnlineIndexer.IndexingStampOperation.BLOCK, luka, 10);
        }

        // Test query, ensure blocked
        openSimpleMetaData(hook);
        try (OnlineIndexer indexer = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                .setTargetIndexes(indexes)
                .build()) {
            final AbstractMap<String, IndexBuildProto.IndexBuildIndexingStamp> stampMap =
                    indexer.indexingStamp(OnlineIndexer.IndexingStampOperation.QUERY, null, 0);
            System.out.println(stampMap);
            final List<String> indexNames = indexes.stream().map(Index::getName).collect(Collectors.toList());
            assertTrue(stampMap.keySet().containsAll(indexNames));
            for (String indexName : indexNames) {
                final IndexBuildProto.IndexBuildIndexingStamp stamp = stampMap.get(indexName);
                assertTrue(stamp.getTargetIndexList().containsAll(indexNames));
                assertEquals(IndexBuildProto.IndexBuildIndexingStamp.Method.MUTUAL_BY_RECORDS, stamp.getMethod());
                assertTrue(stamp.getBlock());
                assertEquals(luka, stamp.getBlockID());
                assertTrue(stamp.getBlockExpireEpochSeconds() > (System.currentTimeMillis() / 1000));
                assertTrue(stamp.getBlockExpireEpochSeconds() < 20 + (System.currentTimeMillis() / 1000));
            }
        }

        // Attempt continue while blocked with the wrong id, ensure failure
        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                .setTargetIndexes(indexes)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setMutualIndexingBoundaries(boundariesList)
                        .setAllowUnblock(true, "my name is not Luka")
                        .build())
                .build()) {
            RecordCoreException e = assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
            assertTrue(e.getMessage().contains("This index was partly built, and blocked"));
        }

        // Continue with unblock, correct id
        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setDatabase(fdb).setMetaData(metaData).setSubspace(subspace)
                .setTargetIndexes(indexes)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setMutualIndexingBoundaries(boundariesList)
                        .setAllowUnblock(true, luka)
                        .build())
                .build()) {
            indexBuilder.buildIndex();
        }

        // Validate
        assertAllReadable(indexes);
        validateIndexes(indexes);
    }
}
