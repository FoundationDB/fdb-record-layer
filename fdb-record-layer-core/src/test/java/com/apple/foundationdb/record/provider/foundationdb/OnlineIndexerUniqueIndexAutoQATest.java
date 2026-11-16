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
import com.apple.foundationdb.record.RecordIndexUniquenessViolation;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TestRecordsWithHeaderProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase.RecordMetaDataHook;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.tuple.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.LongFunction;
import java.util.stream.LongStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
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
    void deleteRecordsWhereRemovesViolationsOfDeletedRecords() {
        // BUG AQA-3: StandardIndexMaintainer.deleteWhere clears only the index range, not the uniqueness violations of the
        // deleted records, so a READABLE_UNIQUE_PENDING index can never become READABLE. Spec: FDBRecordStoreBase
        // deleteRecordsWhere javadoc, "Delete records and associated index entries"; IndexStateCommitCheck javadoc.
        // deleteRecordsWhere deletes "records and associated index entries". The uniqueness violations of the deleted
        // records must go too, otherwise the READABLE_UNIQUE_PENDING index keeps violations no record write can resolve.
        final Index index = new Index("path_str_unique", concat(field("header").nest("path"), field("str_value")),
                EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.UNIQUE_OPTIONS);
        final RecordMetaDataHook hook = metaDataBuilder -> metaDataBuilder.getRecordType("MyRecord")
                .setPrimaryKey(concat(field("header").nest("path"), field("header").nest("rec_no")));
        openMetaData(TestRecordsWithHeaderProto.getDescriptor(), hook);
        try (FDBRecordContext context = openContext()) {
            for (long recNo = 0; recNo < 2; recNo++) {
                for (String path : List.of("a", "b")) {
                    recordStore.saveRecord(TestRecordsWithHeaderProto.MyRecord.newBuilder()
                            .setHeader(TestRecordsWithHeaderProto.HeaderRecord.newBuilder().setPath(path).setRecNo(recNo))
                            .setStrValue(path.equals("a") ? "dup" : "x" + recNo)
                            .build());
                }
            }
            context.commit();
        }
        openMetaData(TestRecordsWithHeaderProto.getDescriptor(), hook.andThen(metaDataBuilder ->
                metaDataBuilder.addIndex("MyRecord", index)));
        disableAll(List.of(index));
        try (OnlineIndexer indexer = newIndexerBuilder(index)
                .setIndexingPolicy(OnlineIndexer.IndexingPolicy.newBuilder().allowUniquePendingState())
                .build()) {
            indexer.buildIndex();
        }
        try (FDBRecordContext context = openContext()) {
            assertThat(recordStore.getIndexState(index).isReadableUniquePending()).isTrue();
            assertThat(recordStore.scanUniquenessViolations(index).getCount().join()).isEqualTo(2);
            recordStore.deleteRecordsWhere(Query.field("header").matches(Query.field("path").equalsValue("a")));
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            assertThat(recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN).getCount().join()).isEqualTo(2);
            assertThat(recordStore.scanUniquenessViolations(index).map(RecordIndexUniquenessViolation::getPrimaryKey)
                    .asList().join()).isEmpty();
            context.commit();
        }
    }
}
