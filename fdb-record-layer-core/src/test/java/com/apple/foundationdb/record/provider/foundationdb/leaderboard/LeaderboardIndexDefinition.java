/*
 * LeaderboardIndexDefinition.java
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

package com.apple.foundationdb.record.provider.foundationdb.leaderboard;

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsIndexScenariosProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexDefinition;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexTarget;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.ScenarioRecords;
import com.apple.foundationdb.tuple.Tuple;

import java.util.Collections;
import java.util.List;
import java.util.stream.LongStream;

class LeaderboardIndexDefinition implements IndexDefinition {
    @Override
    public TestRecordsIndexScenariosProto.IndexedMessage generateIndexedMessage(final int ordinal) {
        return TestRecordsIndexScenariosProto.IndexedMessage.newBuilder()
                .addEntries(TestRecordsIndexScenariosProto.ScoreEntry.newBuilder()
                        .setScore(100L + ordinal)
                        .setTimestamp(1000L + ordinal))
                .build();
    }

    @Override
    public Index buildIndex(final IndexTarget target) {
        // Within the indexed message: fan out over the repeated entries and take (score, timestamp).
        final KeyExpression withinIndexed = Key.Expressions.field(ScenarioRecords.ENTRIES, KeyExpression.FanType.FanOut)
                .nest(Key.Expressions.concat(Key.Expressions.field(ScenarioRecords.SCORE),
                        Key.Expressions.field(ScenarioRecords.TIMESTAMP)));
        final NestingKeyExpression scores = target.indexed(withinIndexed);
        final KeyExpression root = target.groupingPrefix().getColumnSize() == 0
                ? scores.ungrouped()
                : scores.groupBy(target.groupingPrefix());
        return new Index(ScenarioRecords.INDEX_NAME, root, IndexTypes.TIME_WINDOW_LEADERBOARD);
    }

    @Override
    public void setupIndex(final FDBRecordStore store, final Index index) {
        // A leaderboard index needs at least one leaderboard (window) to exist before records
        // can be indexed. Create an all-time leaderboard (no explicit time-window specs).
        store.performIndexOperation(index.getName(), new TimeWindowLeaderboardWindowUpdate(
                System.currentTimeMillis(), true, 0L, true, Collections.emptyList(),
                TimeWindowLeaderboardWindowUpdate.Rebuild.NEVER));
    }

    @Override
    public RecordCursor<IndexEntry> scanIndex(final FDBRecordStore store, final Index index,
                                              final ScanProperties scanProperties) {
        if (((GroupingKeyExpression)index.getRootExpression()).getGroupingCount() == 0) {
            return scanRange(store, index, TupleRange.ALL, null, scanProperties);
        }
        // A grouped leaderboard can only be scanned within a group ("Ranked scan range does not include
        // group"), so walk the groups the framework generates and chain the per-group scans together.
        final List<Long> groups = LongStream.range(0, ScenarioRecords.NUM_GROUPS).boxed().toList();
        return RecordCursor.flatMapPipelined(
                outerContinuation -> RecordCursor.fromList(store.getExecutor(), groups, outerContinuation),
                (group, innerContinuation) -> scanRange(store, index, TupleRange.allOf(Tuple.from(group)),
                        innerContinuation, scanProperties),
                null,
                1);
    }

    private static RecordCursor<IndexEntry> scanRange(final FDBRecordStore store, final Index index,
                                                      final TupleRange range, final byte[] continuation,
                                                      final ScanProperties scanProperties) {
        return store.scanIndex(index,
                new TimeWindowScanRange(TimeWindowLeaderboard.ALL_TIME_LEADERBOARD_TYPE, 0L, range),
                continuation, scanProperties);
    }
}
