/*
 * OnlineIndexerIndexFromIndexAutoQATest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.keyWithValue;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Additional tests for building indexes from other indexes with {@link OnlineIndexer}.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class OnlineIndexerIndexFromIndexAutoQATest extends OnlineIndexerTest {

    private static final int NUM_RECORDS = 40;
    private static final int CHUNK_SIZE = 10;

    private final Index tgtSumIndex = new Index("tgt_index", field("num_value_3_indexed").ungrouped(), IndexTypes.SUM);
    private Index srcIndex = new Index("src_index", field("num_value_2"), IndexTypes.VALUE);
    private FDBRecordStoreTestBase.RecordMetaDataHook hook = allIndexesHook(List.of(srcIndex, tgtSumIndex));

    private static TestRecords1Proto.MySimpleRecord simpleRecord(long recNo, @Nullable Integer numValue2, int numValue3) {
        final TestRecords1Proto.MySimpleRecord.Builder builder = TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(recNo)
                .setNumValue3Indexed(numValue3);
        if (numValue2 != null) {
            builder.setNumValue2(numValue2);
        }
        return builder.build();
    }

    private void populateWithSomeNullSourceValues() {
        openSimpleMetaData();
        saveRecordsWithSomeNullSourceValues();
    }

    private void saveRecordsWithSomeNullSourceValues() {
        // num_value_2 (the source index's field) is unset for every 7th record
        try (FDBRecordContext context = openContext()) {
            for (int i = 0; i < NUM_RECORDS; i++) {
                recordStore.saveRecord(simpleRecord(i, i % 7 == 3 ? null : i * 10, i + 1));
            }
            context.commit();
        }
    }

    private void buildIndexAndCrashHalfway(Index tgtIndex, int count, OnlineIndexer.IndexingPolicy policy) {
        final AtomicLong counter = new AtomicLong(0);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex)
                .setIndexingPolicy(policy)
                .setLimit(CHUNK_SIZE)
                .setConfigLoader(old -> {
                    if (counter.incrementAndGet() > count) {
                        throw new RecordCoreException("Intentionally crash during test");
                    }
                    return old;
                })
                .build()) {
            assertThrows(RecordCoreException.class, indexBuilder::buildIndex);
        }
    }

    private void updateRecordsAtBothEnds() {
        // Move records across the built/unbuilt boundary of the source index order (at both ends, to cover reverse scans),
        // change values in place, delete, and insert
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.saveRecord(simpleRecord(0, 100_000, 500));   // low end -> high end
            recordStore.saveRecord(simpleRecord(39, -5, 600));       // high end -> low end
            recordStore.saveRecord(simpleRecord(3, 5000, 700));      // null -> high end
            recordStore.saveRecord(simpleRecord(5, null, 750));      // low end -> null
            recordStore.saveRecord(simpleRecord(2, 20, 1000));       // low end, same source key
            recordStore.saveRecord(simpleRecord(36, 360, 1100));     // high end, same source key
            recordStore.deleteRecord(Tuple.from(1L));
            recordStore.deleteRecord(Tuple.from(37L));
            recordStore.saveRecord(simpleRecord(1000, null, 800));
            recordStore.saveRecord(simpleRecord(1001, 99_999, 900));
            recordStore.saveRecord(simpleRecord(1002, -100, 950));
            context.commit();
        }
    }

    private void assertSumMatchesRecords() {
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            assertThat(recordStore.getIndexState(tgtSumIndex)).isEqualTo(IndexState.READABLE);
            final long expected = recordStore.scanRecords(TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                    .map(rec -> (long)TestRecords1Proto.MySimpleRecord.newBuilder().mergeFrom(rec.getRecord()).getNumValue3Indexed())
                    .asList().join()
                    .stream().mapToLong(Long::longValue).sum();
            final IndexAggregateFunction sumFunction = new IndexAggregateFunction(FunctionNames.SUM,
                    tgtSumIndex.getRootExpression(), tgtSumIndex.getName());
            final long actual = recordStore.evaluateAggregateFunction(List.of("MySimpleRecord"), sumFunction,
                    TupleRange.ALL, IsolationLevel.SERIALIZABLE).join().getLong(0);
            assertThat(actual).isEqualTo(expected);
            context.commit();
        }
    }

    private OnlineIndexer.IndexingPolicy byIndexPolicy(boolean reverse) {
        return OnlineIndexer.IndexingPolicy.newBuilder()
                .setSourceIndex(srcIndex.getName())
                .forbidRecordScan()
                .setReverseScanOrder(reverse)
                .build();
    }

    @ParameterizedTest
    @BooleanSource
    void buildSumIndexFromIndexWithUpdatesAcrossBuiltBoundaryMatchesRecords(boolean reverse) {
        // A non-idempotent target built by a source index (with null source values) must stay exact when records
        // move between the built and unbuilt parts of the source index's key space while the build is paused
        populateWithSomeNullSourceValues();
        openSimpleMetaData(hook);
        buildIndexClean(srcIndex);

        openSimpleMetaData(hook);
        buildIndexAndCrashHalfway(tgtSumIndex, 2, byIndexPolicy(reverse));

        updateRecordsAtBothEnds();

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtSumIndex)
                .setIndexingPolicy(byIndexPolicy(!reverse))
                .setLimit(CHUNK_SIZE)
                .build()) {
            indexBuilder.buildIndex(true);
        }
        assertSumMatchesRecords();
    }

    @ParameterizedTest
    @ValueSource(strings = {"keyWithValue", "primaryKeyInKey", "keyWithValuePrimaryKeyInKey"})
    void buildSumIndexFromCompositeSourceWithUpdatesAcrossBuiltBoundaryMatchesRecords(String sourceShape) {
        // Same as above, for source indexes whose entry keys differ from the plain (value, primary key) shape: a covering
        // (key with value) source, and sources whose key already contains the primary key
        final KeyExpression root;
        switch (sourceShape) {
            case "keyWithValue":
                root = keyWithValue(concat(field("num_value_2"), field("num_value_3_indexed")), 1);
                break;
            case "primaryKeyInKey":
                root = concat(field("num_value_2"), field("rec_no"));
                break;
            default:
                root = keyWithValue(concat(field("num_value_2"), field("rec_no"), field("num_value_3_indexed")), 2);
                break;
        }
        srcIndex = new Index("src_index", root, IndexTypes.VALUE);
        hook = allIndexesHook(List.of(srcIndex, tgtSumIndex));

        populateWithSomeNullSourceValues();
        openSimpleMetaData(hook);
        buildIndexClean(srcIndex);

        for (boolean reverse : List.of(false, true)) {
            openSimpleMetaData(hook);
            try (FDBRecordContext context = openContext()) {
                recordStore.clearAndMarkIndexWriteOnly(tgtSumIndex).join();
                context.commit();
            }
            openSimpleMetaData(hook);
            buildIndexAndCrashHalfway(tgtSumIndex, 2, byIndexPolicy(reverse));
            updateRecordsAtBothEnds();
            openSimpleMetaData(hook);
            try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtSumIndex)
                    .setIndexingPolicy(byIndexPolicy(reverse))
                    .setLimit(CHUNK_SIZE)
                    .build()) {
                indexBuilder.buildIndex(true);
            }
            assertSumMatchesRecords();
            // restore the original records before the next round
            openSimpleMetaData(hook);
            saveRecordsWithSomeNullSourceValues();
            try (FDBRecordContext context = openContext()) {
                recordStore.deleteRecord(Tuple.from(1000L));
                recordStore.deleteRecord(Tuple.from(1001L));
                recordStore.deleteRecord(Tuple.from(1002L));
                context.commit();
            }
        }
    }

    @Test
    void buildIndexFromIndexSourceDisabledMidBuildFailsThenContinuesAfterSourceRebuilt() {
        // With forbidRecordScan, a source index that becomes disabled mid-build must fail the build (and leave the target
        // write-only). Once the source is rebuilt, the build continues by the same source and the result is exact.
        populateWithSomeNullSourceValues();
        openSimpleMetaData(hook);
        buildIndexClean(srcIndex);

        openSimpleMetaData(hook);
        buildIndexAndCrashHalfway(tgtSumIndex, 2, byIndexPolicy(false));

        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            recordStore.markIndexDisabled(srcIndex).join();
            context.commit();
        }
        updateRecordsAtBothEnds();

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtSumIndex)
                .setIndexingPolicy(byIndexPolicy(false))
                .setLimit(CHUNK_SIZE)
                .build()) {
            final Throwable thrown = catchThrowable(indexBuilder::buildIndex);
            assertThat(IndexingBase.isUnusableSourceIndexException(thrown)).as("thrown: %s", thrown).isTrue();
        }
        openSimpleMetaData(hook);
        try (FDBRecordContext context = openContext()) {
            assertThat(recordStore.getIndexState(tgtSumIndex)).isEqualTo(IndexState.WRITE_ONLY);
            context.commit();
        }

        openSimpleMetaData(hook);
        buildIndexClean(srcIndex);

        openSimpleMetaData(hook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtSumIndex)
                .setIndexingPolicy(byIndexPolicy(false))
                .setLimit(CHUNK_SIZE)
                .build()) {
            indexBuilder.buildIndex(true);
        }
        assertSumMatchesRecords();
    }

    @ParameterizedTest
    @BooleanSource
    void buildSumIndexFromIndexWithTinyWriteLimitDoesNotDoubleCount(boolean inlineRebuild) {
        // A write limit that is reached after every record forces a continuation after each record. Every record must
        // be added to the non-idempotent target exactly once, both by the online build and by the inline rebuild.
        populateWithSomeNullSourceValues();
        openSimpleMetaData(hook);
        buildIndexClean(srcIndex);

        openSimpleMetaData(hook);
        if (inlineRebuild) {
            try (FDBRecordContext context = openContext()) {
                try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtSumIndex)
                        .setIndexingPolicy(byIndexPolicy(false))
                        .setMaxWriteLimitBytes(1)
                        .build()) {
                    indexBuilder.rebuildIndex(recordStore);
                }
                recordStore.markIndexReadable(tgtSumIndex).join();
                context.commit();
            }
        } else {
            try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtSumIndex)
                    .setIndexingPolicy(byIndexPolicy(false))
                    .setMaxWriteLimitBytes(1)
                    .build()) {
                indexBuilder.buildIndex(true);
            }
        }
        assertSumMatchesRecords();
    }

    @Test
    void buildIndexFromReadableUniquePendingSourceIndexesAllRecords() {
        // A READABLE_UNIQUE_PENDING source is scannable, so it may serve as a source: all records (including the
        // duplicates in the source) must be indexed
        final Index uniqueSrc = new Index("unique_src", field("num_value_2"), EmptyKeyExpression.EMPTY,
                IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        final Index tgtIndex = new Index("tgt_value", field("num_value_3_indexed"), IndexTypes.VALUE);
        final FDBRecordStoreTestBase.RecordMetaDataHook uniqueHook = allIndexesHook(List.of(uniqueSrc, tgtIndex));
        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            IntStream.range(0, 30).forEach(i -> recordStore.saveRecord(simpleRecord(i, i % 10, i)));
            context.commit();
        }

        openSimpleMetaData(uniqueHook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(uniqueSrc)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder().allowUniquePendingState().build())
                .build()) {
            indexBuilder.buildIndex(true);
        }
        try (FDBRecordContext context = openContext()) {
            assertThat(recordStore.getIndexState(uniqueSrc)).isEqualTo(IndexState.READABLE_UNIQUE_PENDING);
            context.commit();
        }

        final FDBStoreTimer timer = new FDBStoreTimer();
        openSimpleMetaData(uniqueHook);
        try (OnlineIndexer indexBuilder = newIndexerBuilder(tgtIndex, timer)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder()
                        .setSourceIndex(uniqueSrc.getName())
                        .forbidRecordScan()
                        .build())
                .setLimit(7)
                .build()) {
            indexBuilder.buildIndex(true);
        }
        assertThat(timer.getCount(FDBStoreTimer.Counts.ONLINE_INDEX_BUILDER_RECORDS_INDEXED)).isEqualTo(30);
        try (FDBRecordContext context = openContext()) {
            assertThat(recordStore.getIndexState(tgtIndex)).isEqualTo(IndexState.READABLE);
            final List<Long> indexed = recordStore.scanIndex(tgtIndex, IndexScanType.BY_VALUE,
                            TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                    .map(entry -> entry.getKey().getLong(0))
                    .asList().join();
            assertThat(indexed).isEqualTo(IntStream.range(0, 30).mapToObj(i -> (long)i).collect(Collectors.toList()));
            context.commit();
        }
    }
}
