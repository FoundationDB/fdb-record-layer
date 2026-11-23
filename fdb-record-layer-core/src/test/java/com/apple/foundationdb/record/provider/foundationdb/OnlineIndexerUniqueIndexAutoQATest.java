/*
 * OnlineIndexerUniqueIndexAutoQATest.java
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

import com.apple.foundationdb.record.IndexState;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordIndexUniquenessViolation;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.tuple.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongFunction;
import java.util.stream.LongStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Additional tests for unique indexes built with {@link OnlineIndexer} into
 * {@link IndexState#READABLE_UNIQUE_PENDING}, and then resolved.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class OnlineIndexerUniqueIndexAutoQATest extends OnlineIndexerTest {

    private static final String INDEX_NAME = "simple$value_2_unique";

    @Nonnull
    private static Index uniqueIndex() {
        return new Index(INDEX_NAME, field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
    }

    // Save records, then build the unique index into READABLE_UNIQUE_PENDING
    private Index buildUniquePending(long numRecords, LongFunction<Integer> valueOf) {
        final Index index = uniqueIndex();
        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            LongStream.range(0, numRecords).forEach(recNo -> recordStore.saveRecord(
                    TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(recNo).setNumValue2(valueOf.apply(recNo)).build()));
            context.commit();
        }
        openSimpleMetaData(allIndexesHook(List.of(index)));
        disableAll(List.of(index));
        try (OnlineIndexer indexer = newIndexerBuilder(index)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder().allowUniquePendingState())
                .build()) {
            indexer.buildIndex();
        }
        try (FDBRecordContext context = openContext()) {
            assertThat(recordStore.getIndexState(index).isReadableUniquePending()).isTrue();
            context.commit();
        }
        return index;
    }

    private Set<Tuple> violatingValueKeys(Index index) {
        return new HashSet<>(recordStore.scanUniquenessViolations(index)
                .map(v -> v.getIndexEntry().getKey())
                .asList().join());
    }

    private void assertReadableWithNoViolations(Index index) {
        try (FDBRecordContext context = openContext()) {
            assertThat(recordStore.scanUniquenessViolations(index).getCount().join()).isZero();
            assertThat(recordStore.getIndexState(index)).isEqualTo(IndexState.READABLE);
            context.commit();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3, 5})
    void resolveUniquenessViolationWithNullPrimaryKeyMarksReadable(int duplicates) {
        // resolveUniquenessViolation with a null primary key deletes every duplicate. Once all violations are gone,
        // the READABLE_UNIQUE_PENDING index must be marked READABLE at commit (IndexStateCommitCheck).
        final Index index = buildUniquePending(duplicates, recNo -> 7);
        try (FDBRecordContext context = openContext()) {
            for (Tuple valueKey : violatingValueKeys(index)) {
                recordStore.resolveUniquenessViolation(index, valueKey, null).join();
            }
            assertThat(recordStore.scanUniquenessViolations(index).getCount().join()).isZero();
            context.commit();
        }
        assertReadableWithNoViolations(index);
    }

    @ParameterizedTest
    @ValueSource(ints = {3, 5})
    void resolveUniquenessViolationOfManyDuplicatesMarksReadable(int duplicates) {
        // Several records share one value. Keeping one of them through resolveUniquenessViolation (which deletes the
        // others in a pipeline) must resolve the last violation and mark the index READABLE at commit.
        final Index index = buildUniquePending(duplicates, recNo -> 7);
        try (FDBRecordContext context = openContext()) {
            for (Tuple valueKey : violatingValueKeys(index)) {
                final List<Tuple> primaryKeys = recordStore.scanUniquenessViolations(index, valueKey)
                        .map(RecordIndexUniquenessViolation::getPrimaryKey).asList().join();
                assertThat(primaryKeys).hasSize(duplicates);
                recordStore.resolveUniquenessViolation(index, valueKey, primaryKeys.get(0)).join();
            }
            context.commit();
        }
        assertReadableWithNoViolations(index);
        scrubAndValidate(List.of(index));
    }

    @Test
    void concurrentResolutionOfLastTwoViolationsDoesNotStayUniquePending() {
        // Two transactions resolve the last two (distinct) duplications concurrently. Each sees the other's violation
        // still present, so it must not be possible for both to commit and leave the index READABLE_UNIQUE_PENDING
        // with no violations: either one conflicts, or the index ends up READABLE.
        final Index index = buildUniquePending(4, recNo -> (int)(recNo % 2));
        final FDBRecordStore.Builder storeBuilder = createStoreBuilder();
        Exception secondFailure = null;
        try (FDBRecordContext context1 = fdb.openContext(); FDBRecordContext context2 = fdb.openContext()) {
            final FDBRecordStore store1 = storeBuilder.copyBuilder().setContext(context1).open();
            final FDBRecordStore store2 = storeBuilder.copyBuilder().setContext(context2).open();
            // value 0 is shared by records 0 and 2, value 1 by records 1 and 3
            store1.deleteRecord(Tuple.from(2L));
            store2.deleteRecord(Tuple.from(3L));
            context1.commit();
            try {
                context2.commit();
            } catch (Exception e) {
                secondFailure = e;
            }
        }
        try (FDBRecordContext context = openContext()) {
            final long remaining = recordStore.scanUniquenessViolations(index).getCount().join();
            if (secondFailure == null) {
                assertThat(remaining).isZero();
                assertThat(recordStore.getIndexState(index)).isEqualTo(IndexState.READABLE);
            } else {
                assertThat(FDBExceptions.wrapException(secondFailure)).isInstanceOf(FDBExceptions.FDBStoreTransactionConflictException.class);
                assertThat(remaining).isEqualTo(2);
                assertThat(recordStore.getIndexState(index).isReadableUniquePending()).isTrue();
            }
            context.commit();
        }
    }

    @Test
    void resolvingSomeValueKeysKeepsUniquePendingAndRebuildIsNoOp() {
        // Resolving only some of the duplications keeps the index READABLE_UNIQUE_PENDING; a later
        // allowUniquePendingState build must not mark it READABLE while violations remain.
        final Index index = buildUniquePending(10, recNo -> (int)(recNo % 5));
        try (FDBRecordContext context = openContext()) {
            recordStore.deleteRecord(Tuple.from(5L));
            recordStore.deleteRecord(Tuple.from(6L));
            context.commit();
        }
        try (OnlineIndexer indexer = newIndexerBuilder(index)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder().allowUniquePendingState())
                .build()) {
            indexer.buildIndex();
        }
        try (FDBRecordContext context = openContext()) {
            assertThat(recordStore.getIndexState(index).isReadableUniquePending()).isTrue();
            assertThat(recordStore.scanUniquenessViolations(index).getCount().join()).isEqualTo(6);
            final Throwable markReadable = catchThrowable(() -> recordStore.markIndexReadable(index).join());
            assertThat(FDBExceptions.wrapException(markReadable)).isInstanceOf(RecordIndexUniquenessViolation.class);
            assertThat(context.isClosed()).isFalse();
        }
        try (FDBRecordContext context = openContext()) {
            for (long recNo = 7; recNo < 10; recNo++) {
                recordStore.deleteRecord(Tuple.from(recNo));
            }
            context.commit();
        }
        assertReadableWithNoViolations(index);
        scrubAndValidate(List.of(index));
    }

    @Test
    void nonUniqueNullsAreNotUniquenessViolations() {
        // With the default NullStandin.NULL, a missing field disables uniqueness checks (Key.Expressions.field javadoc),
        // so a build over several records without num_value_2 must succeed and the index must be READABLE.
        final Index index = uniqueIndex();
        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            for (long recNo = 0; recNo < 4; recNo++) {
                recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(recNo).build());
            }
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(4L).setNumValue2(4).build());
            context.commit();
        }
        openSimpleMetaData(allIndexesHook(List.of(index)));
        disableAll(List.of(index));
        try (OnlineIndexer indexer = newIndexerBuilder(index).setLimit(2).build()) {
            indexer.buildIndex();
        }
        assertReadableWithNoViolations(index);
        scrubAndValidate(List.of(index));
    }

    @Test
    void nullUniqueNullsAreUniquenessViolations() {
        // With NullStandin.NULL_UNIQUE, null is treated like any other value for uniqueness. Records missing the field
        // are duplicates: the build ends READABLE_UNIQUE_PENDING, and resolving the null key marks it READABLE.
        final Index index = new Index(INDEX_NAME, field("num_value_2", KeyExpression.FanType.None, Key.Evaluated.NullStandin.NULL_UNIQUE),
                EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            for (long recNo = 0; recNo < 3; recNo++) {
                recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(recNo).build());
            }
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(3L).setNumValue2(3).build());
            context.commit();
        }
        openSimpleMetaData(allIndexesHook(List.of(index)));
        disableAll(List.of(index));
        try (OnlineIndexer indexer = newIndexerBuilder(index)
                .setLimit(1)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder().allowUniquePendingState())
                .build()) {
            indexer.buildIndex();
        }
        try (FDBRecordContext context = openContext()) {
            assertThat(recordStore.getIndexState(index).isReadableUniquePending()).isTrue();
            assertThat(violatingValueKeys(index)).containsExactly(Tuple.from((Object)null));
            assertThat(recordStore.scanUniquenessViolations(index).getCount().join()).isEqualTo(3);
            recordStore.resolveUniquenessViolation(index, Tuple.from((Object)null), Tuple.from(1L)).join();
            context.commit();
        }
        assertReadableWithNoViolations(index);
        scrubAndValidate(List.of(index));
    }

    @Test
    void violationsAreCompleteAfterCrashWritesAndContinuation() {
        // A unique index build crashes part way, then records are written while the index is partly built, and the
        // build continues. The violations after the continued build must match the final records, whatever the
        // crash point: value 0 is shared by 0, 5 and 20; value 1 is no longer shared; values 2, 3 and 4 are shared.
        final Index index = uniqueIndex();
        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            LongStream.range(0, 10).forEach(recNo -> recordStore.saveRecord(
                    TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(recNo).setNumValue2((int)(recNo % 5)).build()));
            context.commit();
        }
        openSimpleMetaData(allIndexesHook(List.of(index)));
        disableAll(List.of(index));
        final AtomicInteger configCalls = new AtomicInteger();
        try (OnlineIndexer indexer = newIndexerBuilder(index)
                .setLimit(1)
                .setConfigLoader(old -> {
                    if (configCalls.incrementAndGet() > 4) {
                        throw new RecordCoreException("Intentionally crash during test");
                    }
                    return old;
                })
                .build()) {
            final Throwable crash = catchThrowable(indexer::buildIndex);
            assertThat(crash).isInstanceOf(RecordCoreException.class);
        }
        try (FDBRecordContext context = openContext()) {
            assertThat(recordStore.getIndexState(index)).isEqualTo(IndexState.WRITE_ONLY);
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(20L).setNumValue2(0).build());
            recordStore.saveRecord(TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(1L).setNumValue2(77).build());
            context.commit();
        }
        try (OnlineIndexer indexer = newIndexerBuilder(index)
                .setLimit(1)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder().allowUniquePendingState())
                .build()) {
            indexer.buildIndex();
        }
        try (FDBRecordContext context = openContext()) {
            assertThat(recordStore.getIndexState(index).isReadableUniquePending()).isTrue();
            assertThat(violatingValueKeys(index)).containsExactlyInAnyOrder(Tuple.from(0L), Tuple.from(2L), Tuple.from(3L), Tuple.from(4L));
            assertThat(recordStore.scanUniquenessViolations(index, Tuple.from(0L))
                    .map(RecordIndexUniquenessViolation::getPrimaryKey).asList().join())
                    .containsExactlyInAnyOrder(Tuple.from(0L), Tuple.from(5L), Tuple.from(20L));
            assertThat(recordStore.scanUniquenessViolations(index).getCount().join()).isEqualTo(9);
            context.commit();
        }
        scrubAndValidate(List.of(index));
    }

    @Test
    void multiTargetContinuationMarksOtherTargetsReadable() {
        // A multi-target build (one unique index with duplicates, one non-unique, one unique without duplicates)
        // crashes part way and is continued with allowUniquePendingState. The duplicated index must end
        // READABLE_UNIQUE_PENDING with all its violations; the other targets must be READABLE.
        final Index indexA = new Index("indexA", field("num_value_2"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        final Index indexB = new Index("indexB", field("num_value_3_indexed"), IndexTypes.VALUE);
        final Index indexC = new Index("indexC", field("num_value_unique"), EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        final List<Index> indexes = List.of(indexA, indexB, indexC);
        openSimpleMetaData();
        try (FDBRecordContext context = openContext()) {
            LongStream.range(0, 10).forEach(recNo -> recordStore.saveRecord(
                    TestRecords1Proto.MySimpleRecord.newBuilder().setRecNo(recNo).setNumValue2((int)(recNo % 5))
                            .setNumValue3Indexed((int)(recNo % 3)).setNumValueUnique((int)recNo + 100).build()));
            context.commit();
        }
        openSimpleMetaData(allIndexesHook(indexes));
        disableAll(indexes);
        final AtomicInteger configCalls = new AtomicInteger();
        try (OnlineIndexer indexer = newIndexerBuilder(indexes)
                .setLimit(1)
                .setConfigLoader(old -> {
                    if (configCalls.incrementAndGet() > 4) {
                        throw new RecordCoreException("Intentionally crash during test");
                    }
                    return old;
                })
                .build()) {
            assertThat(catchThrowable(indexer::buildIndex)).isInstanceOf(RecordCoreException.class);
        }
        try (OnlineIndexer indexer = newIndexerBuilder(indexes)
                .setLimit(1)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder().allowUniquePendingState())
                .build()) {
            indexer.buildIndex();
        }
        try (FDBRecordContext context = openContext()) {
            assertThat(recordStore.getIndexState(indexA).isReadableUniquePending()).isTrue();
            assertThat(recordStore.scanUniquenessViolations(indexA).getCount().join()).isEqualTo(10);
            assertThat(recordStore.getIndexState(indexB)).isEqualTo(IndexState.READABLE);
            assertThat(recordStore.getIndexState(indexC)).isEqualTo(IndexState.READABLE);
            assertThat(recordStore.scanUniquenessViolations(indexC).getCount().join()).isZero();
            context.commit();
        }
        scrubAndValidate(indexes);
    }
}
