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
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexDefinition;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexTarget;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.ScenarioRecords;

import java.util.Collections;

class LeaderboardIndexDefinition implements IndexDefinition {
    private final String indexName = "LeaderboardIndex";

    @Override
    public String getIndexName() {
        return indexName;
    }

    @Override
    public String getIndexedTypeName() {
        return ScenarioRecords.SCENARIO_RECORD;
    }

    @Override
    public boolean supportsGrouping() {
        // A grouped time-window leaderboard cannot be scanned with an all-groups range: the maintainer
        // requires the scan range to include the group ("Ranked scan range does not include group"). The
        // generic DeleteWhereGroup scenario scans the whole index (TupleRange.ALL), so it cannot apply here
        // without leaderboard-specific per-group scanning (or a maintainer change). Skip grouping.
        return false;
    }

    @Override
    public boolean supportsSynthetic() {
        // A time-window leaderboard needs windows created before indexing and a time-window scan; combining
        // that with synthetic record types (where the leaderboard entries are themselves a repeated field
        // within an unnested/joined constituent) is not supported without maintainer-level work. Skip.
        return false;
    }

    @Override
    public TestRecordsIndexScenariosProto.IndexedMessage generateIndexedMessage(final int index) {
        return TestRecordsIndexScenariosProto.IndexedMessage.newBuilder()
                .addEntries(TestRecordsIndexScenariosProto.ScoreEntry.newBuilder()
                        .setScore(100L + index)
                        .setTimestamp(1000L + index)
                        .setContext(index))
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
        return new Index(indexName, root, IndexTypes.TIME_WINDOW_LEADERBOARD);
    }

    @Override
    public void setupIndex(final FDBRecordStore store) {
        // A leaderboard index needs at least one leaderboard (window) to exist before records
        // can be indexed. Create an all-time leaderboard (no explicit time-window specs).
        store.performIndexOperation(indexName, new TimeWindowLeaderboardWindowUpdate(
                System.currentTimeMillis(), true, 0L, true, Collections.emptyList(),
                TimeWindowLeaderboardWindowUpdate.Rebuild.NEVER));
    }

    @Override
    public RecordCursor<IndexEntry> scanIndex(final FDBRecordStore store, final ScanProperties scanProperties) {
        return store.scanIndex(store.getRecordMetaData().getIndex(indexName),
                new TimeWindowScanRange(TimeWindowLeaderboard.ALL_TIME_LEADERBOARD_TYPE, 0L, TupleRange.ALL),
                null, scanProperties);
    }
}
