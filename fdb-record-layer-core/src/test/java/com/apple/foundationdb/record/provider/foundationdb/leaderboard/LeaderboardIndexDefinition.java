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
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsLeaderboardProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexDefinition;
import com.google.protobuf.Message;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class LeaderboardIndexDefinition implements IndexDefinition {
    private final String indexName = "LeaderboardIndex";

    @Override
    public RecordMetaData getMetaData() {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsLeaderboardProto.getDescriptor());
        metaDataBuilder.addIndex("NestedLeaderboardRecord",
                new Index(indexName,
                        Key.Expressions.field("scores", KeyExpression.FanType.FanOut)
                                .nest(Key.Expressions.concat(Key.Expressions.field("score"),
                                        Key.Expressions.field("timestamp")))
                                .ungrouped(),
                        IndexTypes.TIME_WINDOW_LEADERBOARD));
        return metaDataBuilder.build();
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
    public List<Message> generateRecords(final int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> (Message)TestRecordsLeaderboardProto.NestedLeaderboardRecord.newBuilder()
                        .setName("player-" + i)
                        .setGameId("game-1")
                        .addScores(TestRecordsLeaderboardProto.NestedLeaderboardEntry.newBuilder()
                                .setScore(100L + i)
                                .setTimestamp(1000L + i)
                                .setContext(i))
                        .build())
                .collect(Collectors.toList());
    }

    @Override
    public RecordCursor<IndexEntry> scanIndex(final FDBRecordStore store, final ScanProperties scanProperties) {
        return store.scanIndex(store.getRecordMetaData().getIndex(indexName),
                new TimeWindowScanRange(TimeWindowLeaderboard.ALL_TIME_LEADERBOARD_TYPE, 0L, TupleRange.ALL),
                null, scanProperties);
    }

    @Override
    public List<Message> generateOtherRecords(final int count) {
        // FlatLeaderboardRecord is not covered by the leaderboard index on NestedLeaderboardRecord.
        return IntStream.range(0, count)
                .mapToObj(i -> (Message)TestRecordsLeaderboardProto.FlatLeaderboardRecord.newBuilder()
                        .setName("other-" + i)
                        .setGameId("game-1")
                        .build())
                .collect(Collectors.toList());
    }

    @Override
    public String getIndexName() {
        return indexName;
    }
}
