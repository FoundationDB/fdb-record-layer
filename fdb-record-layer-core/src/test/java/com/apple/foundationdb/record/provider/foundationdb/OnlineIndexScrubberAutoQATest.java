/*
 * OnlineIndexScrubberAutoQATest.java
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

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.indexing.IndexingRangeSet;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Descriptors;
import com.google.protobuf.MessageOrBuilder;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Additional tests for {@link OnlineIndexScrubber}.
 */
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
class OnlineIndexScrubberAutoQATest extends OnlineIndexerTest {

    // A filter that maintains everything, and runs a one-shot hook for a chosen record while it is being indexed.
    private static final class HookFilter implements IndexMaintenanceFilter {
        private final AtomicBoolean armed = new AtomicBoolean(false);
        private final long triggerRecNo;
        private final Runnable hook;

        HookFilter(long triggerRecNo, Runnable hook) {
            this.triggerRecNo = triggerRecNo;
            this.hook = hook;
        }

        @Override
        public IndexValues maintainIndex(@Nonnull Index index, @Nonnull MessageOrBuilder rec) {
            final Descriptors.FieldDescriptor recNoField = rec.getDescriptorForType().findFieldByName("rec_no");
            if (recNoField != null && Long.valueOf(triggerRecNo).equals(rec.getField(recNoField)) &&
                    armed.compareAndSet(true, false)) {
                hook.run();
            }
            return IndexValues.ALL;
        }
    }

    private Index setUpIndex(KeyExpression rootExpression, List<TestRecords1Proto.MySimpleRecord> records) {
        Index index = new Index("tgt_index", rootExpression, EmptyKeyExpression.EMPTY, IndexTypes.VALUE, IndexOptions.EMPTY_OPTIONS);
        openSimpleMetaData(allIndexesHook(List.of(index)));
        try (FDBRecordContext context = openContext()) {
            records.forEach(recordStore::saveRecord);
            context.commit();
        }
        buildIndexClean(index);
        return index;
    }

    private void clearIndexKeys(Index index, List<Tuple> keys) {
        try (FDBRecordContext context = openContext()) {
            for (Tuple key : keys) {
                context.ensureActive().clear(recordStore.indexSubspace(index).pack(key));
            }
            context.commit();
        }
    }

    private void clearRecordsOnly(List<Long> recNos) {
        try (FDBRecordContext context = openContext()) {
            for (Long recNo : recNos) {
                context.ensureActive().clear(recordStore.recordsSubspace().subspace(Tuple.from(recNo)).range());
            }
            context.commit();
        }
    }

    private long scrub(Index index, FDBStoreTimer timer, boolean missing, int limit,
                       Consumer<OnlineIndexScrubber.ScrubbingPolicy.Builder> policyCustomizer) {
        final OnlineIndexScrubber.ScrubbingPolicy.Builder policy = OnlineIndexScrubber.ScrubbingPolicy.newBuilder()
                .setLogWarningsLimit(Integer.MAX_VALUE);
        policyCustomizer.accept(policy);
        try (OnlineIndexScrubber scrubber = newScrubberBuilder(index, timer)
                .setScrubbingPolicy(policy)
                .setLimit(limit)
                .build()) {
            return missing ? scrubber.scrubMissingIndexEntries() : scrubber.scrubDanglingIndexEntries();
        }
    }

    private List<IndexEntry> scanIndex(Index index) {
        try (FDBRecordContext context = openContext()) {
            return context.asyncToSync(FDBStoreTimer.Waits.WAIT_SCAN_INDEX_RECORDS,
                    recordStore.scanIndex(index, IndexScanType.BY_VALUE, TupleRange.ALL, null, ScanProperties.FORWARD_SCAN).asList());
        }
    }

    private boolean isScrubRangeSetEmpty(Index index, boolean missing) {
        try (FDBRecordContext context = openContext()) {
            final IndexingRangeSet rangeSet = missing ?
                                              IndexingRangeSet.forScrubbingRecords(recordStore, index, 0) :
                                              IndexingRangeSet.forScrubbingIndex(recordStore, index, 0);
            return context.asyncToSync(FDBStoreTimer.Waits.WAIT_ONLINE_BUILD_INDEX, rangeSet.isEmptyAsync());
        }
    }

    private static List<TestRecords1Proto.MySimpleRecord> simpleRecords(int count) {
        return IntStream.range(0, count).mapToObj(i -> TestRecords1Proto.MySimpleRecord.newBuilder()
                .setRecNo(i)
                .setNumValue2(i * 19)
                .build()).toList();
    }

    @Test
    void scrubMissingIndexEntriesCommitRetryCountsEachIssueOnce() {
        // BUG AQA-29: IndexScrubbing reports issues when the transaction body completes, before the commit, so a retried
        // transaction counts its issues twice. Spec: IndexScrubbing comment "report these issues only after their
        // transaction was completed successfully"; scrubMissingIndexEntries javadoc.
        // A scrubbing transaction that fails at commit and is retried must not report its issues twice.
        final int numRecords = 20;
        final byte[][] conflictKey = new byte[1][];
        final HookFilter filter = new HookFilter(15, () ->
                // Rewrite (with the same value) an index key the scrubber already read in this transaction,
                // so that its commit fails with a conflict and the runner retries it.
                fdb.database().run(tr -> {
                    tr.set(conflictKey[0], Tuple.from().pack());
                    return null;
                }));
        setIndexMaintenanceFilter(filter);
        final Index index = setUpIndex(field("num_value_2"), simpleRecords(numRecords));
        try (FDBRecordContext context = openContext()) {
            conflictKey[0] = recordStore.indexSubspace(index).pack(Tuple.from(0, 0L));
            context.commit();
        }
        clearIndexKeys(index, List.of(Tuple.from(10 * 19, 10L)));

        final FDBStoreTimer timer = new FDBStoreTimer();
        filter.armed.set(true);
        long res = scrub(index, timer, true, 1000, policy -> policy.setAllowRepair(false));
        assertThat(filter.armed.get()).as("hook should have fired").isFalse();
        assertThat(res).isEqualTo(1);
        assertThat(timer.getCount(FDBStoreTimer.Counts.INDEX_SCRUBBER_MISSING_ENTRIES)).isEqualTo(1);
    }
}
