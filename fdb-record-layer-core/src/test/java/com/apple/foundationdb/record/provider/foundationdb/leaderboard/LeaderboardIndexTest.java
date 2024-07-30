/*
 * LeaderboardIndexTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsLeaderboardProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.IndexOrphanBehavior;
import com.apple.foundationdb.record.provider.foundationdb.indexes.RankedSetHashFunctions;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryRecordFunction;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.cascades.typing.TypeRepository;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.apple.foundationdb.record.test.TestKeySpace;
import com.apple.foundationdb.record.test.TestKeySpacePathManagerExtension;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.primitives.Longs;
import com.google.protobuf.Message;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@code TIME_WINDOW_LEADERBOARD} indexes.
 */
@Tag(Tags.RequiresFDB)
public class LeaderboardIndexTest {
    @RegisterExtension
    final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();
    @RegisterExtension
    final TestKeySpacePathManagerExtension pathManager = new TestKeySpacePathManagerExtension(dbExtension);

    private FDBDatabase fdb;
    KeySpacePath path;
    FDBStoreTimer metrics;
    private static final int TEST_MAX_ATTEMPTS = 100;
    private static final long TEST_MAX_DELAY = 10L;

    @BeforeEach
    void setUp() {
        FDBDatabaseFactory factory = dbExtension.getDatabaseFactory();
        factory.setMaxAttempts(TEST_MAX_ATTEMPTS);
        factory.setMaxDelayMillis(TEST_MAX_DELAY);

        fdb = dbExtension.getDatabase();
        path = pathManager.createPath(TestKeySpace.RECORD_STORE);
        metrics = new FDBStoreTimer();
    }

    protected FDBRecordContext openContext() {
        return fdb.openContext(null, metrics);
    }

    public static final int TEN_UNITS = 2;
    public static final int FIVE_UNITS = 3;

    abstract class Leaderboards {

        RecordMetaData metaData;
        RecordQueryPlanner planner;
        FDBRecordStore recordStore;

        public void buildMetaData() {
            buildMetaData(this::addIndex);
        }

        public void buildMetaData(Consumer<RecordMetaDataBuilder> metaDataHook) {
            RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsLeaderboardProto.getDescriptor());
            metaDataHook.accept(metaDataBuilder);
            metaData = metaDataBuilder.getRecordMetaData();
        }

        public abstract void addIndex(RecordMetaDataBuilder metaDataBuilder);

        public void openRecordStore(FDBRecordContext context, boolean clearFirst) {
            if (clearFirst) {
                path.deleteAllData(context);
            }

            recordStore = FDBRecordStore.newBuilder().setMetaDataProvider(metaData).setContext(context).setKeySpacePath(path).build();
            planner = new RecordQueryPlanner(metaData, recordStore.getRecordStoreState());
        }

        public void addScores(String name, String gameId, long... scores) {
            FDBStoredRecord<Message> existingRecord = findByName(name);
            Message record;
            if (existingRecord == null) {
                record = buildRecord(name, gameId);
            } else {
                record = existingRecord.getRecord();
            }
            record = addScores(record, scores);
            recordStore.saveRecord(record);
        }

        protected abstract Message addScores(Message record, long... scores);

        @Nullable
        public FDBStoredRecord<Message> findByName(String name) {
            return recordStore.loadRecord(Tuple.from(name));
        }

        public abstract String getName(Message record);

        protected abstract Message buildRecord(String name, String gameId);

        public TimeWindowLeaderboardWindowUpdateResult updateWindows(boolean highScoreFirst, long baseTimestamp) {
            return updateWindows(highScoreFirst, baseTimestamp, TimeWindowLeaderboardWindowUpdate.Rebuild.IF_OVERLAPPING_CHANGED);
        }

        public TimeWindowLeaderboardWindowUpdateResult updateWindows(boolean highScoreFirst, long baseTimestamp,
                                                                     TimeWindowLeaderboardWindowUpdate.Rebuild rebuild) {
            return (TimeWindowLeaderboardWindowUpdateResult)
                    recordStore.performIndexOperation("LeaderboardIndex",
                            new TimeWindowLeaderboardWindowUpdate(System.currentTimeMillis(), highScoreFirst,
                                    baseTimestamp,
                                    true,
                                    Arrays.asList(
                                            new TimeWindowLeaderboardWindowUpdate.TimeWindowSpec(TEN_UNITS, baseTimestamp, 5, 10, 20),
                                            new TimeWindowLeaderboardWindowUpdate.TimeWindowSpec(FIVE_UNITS, baseTimestamp, 1, 5, 10)
                                    ),
                                    rebuild));
        }

        public RecordCursor<Message> scanIndexByRank(TupleRange range) {
            return recordStore.scanIndexRecords("LeaderboardIndex", IndexScanType.BY_RANK, range, null, ScanProperties.FORWARD_SCAN).map(FDBIndexedRecord::getRecord);
        }

        public RecordCursor<Message> scanIndexByScore(TupleRange range, boolean reverse) {
            return recordStore.scanIndexRecords("LeaderboardIndex", IndexScanType.BY_VALUE, range, null,
                            new ScanProperties(ExecuteProperties.SERIAL_EXECUTE, reverse))
                    .map(FDBIndexedRecord::getRecord);
        }

        public RecordCursor<Message> scanIndexByTimeWindow(TimeWindowScanRange range) {
            return recordStore.fetchIndexRecords(recordStore.scanIndex(metaData.getIndex("LeaderboardIndex"), range, null, ScanProperties.FORWARD_SCAN),
                            IndexOrphanBehavior.ERROR)
                    .map(FDBIndexedRecord::getRecord);
        }

        public RecordCursor<Message> scanIndexByTimeWindowWithLimit(TimeWindowScanRange range, int limit) {
            return recordStore.fetchIndexRecords(recordStore.scanIndex(metaData.getIndex("LeaderboardIndex"), range, null,
                                    new ScanProperties(ExecuteProperties.newBuilder()
                                            .setReturnedRowLimit(limit)
                                            .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                                            .build())),
                            IndexOrphanBehavior.ERROR)
                    .map(FDBIndexedRecord::getRecord);
        }

        public abstract GroupingKeyExpression getKeyExpression();

        public String getRecordType() {
            return metaData.recordTypesForIndex(metaData.getIndex("LeaderboardIndex")).iterator().next().getName();
        }

        public RecordQueryPlan planQuery(RecordQuery query) {
            return planner.plan(query.toBuilder().setRemoveDuplicates(false).build());
        }

        public RecordCursor<Message> executeQuery(RecordQueryPlan plan) {
            return executeQuery(plan, EvaluationContext.EMPTY);
        }

        public RecordCursor<Message> executeQuery(RecordQueryPlan plan, EvaluationContext context) {
            return plan.execute(recordStore, context).map(FDBQueriedRecord::getRecord);
        }

        public QueryRecordFunction<Long> queryRank() {
            return Query.rank(getKeyExpression());
        }

        public QueryRecordFunction<Long> queryTimeWindowRank(int type, long timestamp) {
            return Query.timeWindowRank(type, timestamp, getKeyExpression());
        }

        public QueryRecordFunction<Long> queryTimeWindowRank(String typeParameter, String timestampParameter) {
            return Query.timeWindowRank(typeParameter, timestampParameter, getKeyExpression());
        }

        public QueryRecordFunction<Tuple> queryTimeWindowRankAndEntry(int type, long timestamp) {
            return Query.timeWindowRankAndEntry(type, timestamp, getKeyExpression());
        }

        public QueryRecordFunction<Tuple> queryTimeWindowRankAndEntry(String typeParameter, String timestampParameter) {
            return Query.timeWindowRankAndEntry(typeParameter, timestampParameter, getKeyExpression());
        }

        public <T> T evaluateQueryFunction(QueryRecordFunction<T> function, FDBStoredRecord<Message> record) {
            return recordStore.evaluateRecordFunction(function.getFunction(), record).join();
        }

        public IndexAggregateFunction timeWindowCount(int type, long timestamp) {
            return new TimeWindowAggregateFunction(FunctionNames.TIME_WINDOW_COUNT, getKeyExpression(), null,
                    new TimeWindowForFunction(type, timestamp, null, null));
        }

        public IndexAggregateFunction scoreForTimeWindowRank(int type, long timestamp) {
            return new TimeWindowAggregateFunction(FunctionNames.SCORE_FOR_TIME_WINDOW_RANK, getKeyExpression(), null,
                    new TimeWindowForFunction(type, timestamp, null, null));
        }

        public IndexAggregateFunction timeWindowRankForScore(int type, long timestamp) {
            return new TimeWindowAggregateFunction(FunctionNames.TIME_WINDOW_RANK_FOR_SCORE, getKeyExpression(), null,
                    new TimeWindowForFunction(type, timestamp, null, null));
        }

        public Tuple evaluateAggregateFunction(IndexAggregateFunction function, Tuple values) {
            return recordStore.evaluateAggregateFunction(EvaluationContext.EMPTY, Collections.singletonList(getRecordType()), function, TupleRange.allOf(values), IsolationLevel.SERIALIZABLE).join();
        }

        public List<Key.Evaluated> getScores(FDBRecord<Message> record) {
            return metaData.getIndex("LeaderboardIndex").getRootExpression().evaluate(record);
        }

        public Collection<Tuple> trim(Collection<Key.Evaluated> untrimmed) {
            List<Tuple> untrimmedKeys = untrimmed.stream().map(Key.Evaluated::toTuple).collect(Collectors.toList());
            return ((TimeWindowLeaderboardScoreTrimResult)recordStore.performIndexOperation("LeaderboardIndex",
                    new TimeWindowLeaderboardScoreTrim(untrimmedKeys, true))).getScores();
        }

        public TimeWindowLeaderboardSubDirectory setGroupHighScoreFirst(Tuple group, boolean highScoreFirst) {
            return ((TimeWindowLeaderboardSubDirectoryResult)
                            recordStore.performIndexOperation("LeaderboardIndex",
                                    new TimeWindowLeaderboardSaveSubDirectory(new TimeWindowLeaderboardSubDirectory(group, highScoreFirst)))).getSubDirectory();
        }
    }

    abstract class NestedLeaderboards extends Leaderboards {
        @Override
        public String getName(Message record) {
            TestRecordsLeaderboardProto.NestedLeaderboardRecord.Builder recordBuilder =
                    TestRecordsLeaderboardProto.NestedLeaderboardRecord.newBuilder();
            recordBuilder.mergeFrom(record);
            return recordBuilder.getName();
        }

        @Override
        protected Message buildRecord(String name, String gameId) {
            return TestRecordsLeaderboardProto.NestedLeaderboardRecord.newBuilder()
                    .setName(name)
                    .setGameId(gameId)
                    .build();
        }

        @Override
        protected Message addScores(Message record, long... scores) {
            TestRecordsLeaderboardProto.NestedLeaderboardRecord.Builder recordBuilder =
                    TestRecordsLeaderboardProto.NestedLeaderboardRecord.newBuilder();
            recordBuilder.mergeFrom(record);
            for (int i = 0; i < scores.length; i += 3) {
                recordBuilder.addScoresBuilder()
                        .setScore(scores[i])
                        .setTimestamp(scores[i + 1])
                        .setContext(scores[i + 2]);
            }
            return recordBuilder.build();
        }
    }

    class UngroupedNestedLeaderboards extends NestedLeaderboards {
        protected final GroupingKeyExpression keyExpression = Key.Expressions.field("scores", KeyExpression.FanType.FanOut)
                .nest(Key.Expressions.concat(
                        Key.Expressions.field("score"),
                        Key.Expressions.field("timestamp")))
                .ungrouped();

        @Override
        public GroupingKeyExpression getKeyExpression() {
            return keyExpression;
        }

        @Override
        public void addIndex(RecordMetaDataBuilder metaDataBuilder) {
            metaDataBuilder.addIndex("NestedLeaderboardRecord", new Index("LeaderboardIndex", keyExpression, IndexTypes.TIME_WINDOW_LEADERBOARD));
        }
    }

    class GroupedNestedLeaderboards extends NestedLeaderboards {
        protected final GroupingKeyExpression keyExpression = Key.Expressions.field("scores", KeyExpression.FanType.FanOut)
                .nest(Key.Expressions.concat(
                        Key.Expressions.field("score"),
                        Key.Expressions.field("timestamp"),
                        Key.Expressions.field("context")))
                .groupBy(Key.Expressions.field("game_id"));

        @Override
        public GroupingKeyExpression getKeyExpression() {
            return keyExpression;
        }

        @Override
        public void addIndex(RecordMetaDataBuilder metaDataBuilder) {
            metaDataBuilder.addIndex("NestedLeaderboardRecord", new Index("LeaderboardIndex", keyExpression, IndexTypes.TIME_WINDOW_LEADERBOARD,
                    Collections.singletonMap(IndexOptions.RANK_HASH_FUNCTION, RankedSetHashFunctions.MURMUR3)));
        }
    }

    class FlatLeaderboards extends Leaderboards {
        protected final GroupingKeyExpression keyExpression = Key.Expressions.field("scores", KeyExpression.FanType.FanOut)
                .split(3)
                .groupBy(Key.Expressions.field("game_id"));

        @Override
        public GroupingKeyExpression getKeyExpression() {
            return keyExpression;
        }

        @Override
        public void addIndex(RecordMetaDataBuilder metaDataBuilder) {
            metaDataBuilder.addIndex("FlatLeaderboardRecord", new Index("LeaderboardIndex", keyExpression, IndexTypes.TIME_WINDOW_LEADERBOARD));
        }

        @Override
        public String getName(Message record) {
            TestRecordsLeaderboardProto.FlatLeaderboardRecord.Builder recordBuilder =
                    TestRecordsLeaderboardProto.FlatLeaderboardRecord.newBuilder();
            recordBuilder.mergeFrom(record);
            return recordBuilder.getName();
        }

        @Override
        protected Message buildRecord(String name, String gameId) {
            return TestRecordsLeaderboardProto.FlatLeaderboardRecord.newBuilder()
                    .setName(name)
                    .setGameId(gameId)
                    .build();
        }

        @Override
        protected Message addScores(Message record, long... scores) {
            TestRecordsLeaderboardProto.FlatLeaderboardRecord.Builder recordBuilder =
                    TestRecordsLeaderboardProto.FlatLeaderboardRecord.newBuilder();
            recordBuilder.mergeFrom(record);
            recordBuilder.addAllScores(Longs.asList(scores));
            return recordBuilder.build();
        }
    }

    protected void basicSetup(Leaderboards leaderboards, boolean highScoreFirst) {
        leaderboards.buildMetaData();
        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, true);
            leaderboards.updateWindows(highScoreFirst, 10100);
            addInitialScores(leaderboards);
            context.commit();
        }
    }

    protected void addInitialScores(Leaderboards leaderboards) {
        // Before the default range.
        leaderboards.addScores("patroclus", "game-1",
                1000, 11001, 111);
        // Both higher and lower scores than hector, so wins in both directions.
        leaderboards.addScores("achilles", "game-1",
                100, 10101, 666,
                99, 10102, 665, // Does not help any
                200, 10105, 667,
                300, 10201, 668);
        // Another game.
        leaderboards.addScores("helen", "game-2",
                750, 11001, 555);
        leaderboards.addScores("hector", "game-1",
                150, 10101, 777,
                160, 10105, 778,
                170, 10201, 779);
        // After the default range.
        leaderboards.addScores("hecuba", "game-1",
                750, 11201, 888);
    }

    @Test
    public void basicUngroupedNested() {
        Leaderboards leaderboards = new UngroupedNestedLeaderboards();
        basicSetup(leaderboards, false);
        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, false);

            assertEquals(Arrays.asList("achilles", "hector", "helen", "hecuba", "patroclus"),
                    leaderboards.scanIndexByRank(TupleRange.ALL)
                            .map(leaderboards::getName).asList().join());

            TupleRange top_2 = new TupleRange(Tuple.from(0), Tuple.from(1), EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE);
            assertEquals(Arrays.asList("achilles", "hector"),
                    leaderboards.scanIndexByRank(top_2)
                            .map(leaderboards::getName).asList().join());

            TupleRange no_2 = TupleRange.allOf(Tuple.from(1));
            assertEquals(Arrays.asList("hector"),
                    leaderboards.scanIndexByRank(no_2)
                            .map(leaderboards::getName).asList().join());

            TimeWindowScanRange ten_units = new TimeWindowScanRange(TEN_UNITS, 10101, TupleRange.ALL);
            assertEquals(Arrays.asList("achilles", "hector"),
                    leaderboards.scanIndexByTimeWindow(ten_units)
                            .map(leaderboards::getName).asList().join());

            TimeWindowScanRange five_units = new TimeWindowScanRange(FIVE_UNITS, 10102, TupleRange.ALL);
            assertEquals(Arrays.asList("achilles", "hector"),
                    leaderboards.scanIndexByTimeWindow(five_units)
                            .map(leaderboards::getName).asList().join());

            TimeWindowScanRange five_units_2 = new TimeWindowScanRange(FIVE_UNITS, 10107, TupleRange.ALL);
            assertEquals(Arrays.asList("hector", "achilles"),
                    leaderboards.scanIndexByTimeWindow(five_units_2)
                            .map(leaderboards::getName).asList().join());

            final FDBStoredRecord<Message> rec = leaderboards.findByName("helen");
            assertEquals((Long)2L, leaderboards.evaluateQueryFunction(leaderboards.queryRank(), rec));
            assertEquals(null, leaderboards.evaluateQueryFunction(leaderboards.queryTimeWindowRank(TEN_UNITS, 10102), rec));
        }
    }

    @Test
    public void basicUngroupedNestedReverse() {
        Leaderboards leaderboards = new UngroupedNestedLeaderboards();
        basicSetup(leaderboards, true);
        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, false);

            TupleRange score_300_1000 = new TupleRange(Tuple.from(300), Tuple.from(1000), EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_EXCLUSIVE);
            assertEquals(Arrays.asList("helen", "hecuba", "achilles"),
                    leaderboards.scanIndexByScore(score_300_1000, true)
                            .map(leaderboards::getName).asList().join());

            assertEquals(Arrays.asList("patroclus", "helen", "hecuba", "achilles", "hector"),
                    leaderboards.scanIndexByRank(TupleRange.ALL)
                            .map(leaderboards::getName).asList().join());

            TupleRange top_2 = new TupleRange(Tuple.from(0), Tuple.from(1), EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE);
            assertEquals(Arrays.asList("patroclus", "helen"),
                    leaderboards.scanIndexByRank(top_2)
                            .map(leaderboards::getName).asList().join());

            TupleRange no_2 = TupleRange.allOf(Tuple.from(1));
            assertEquals(Arrays.asList("helen"),
                    leaderboards.scanIndexByRank(no_2)
                            .map(leaderboards::getName).asList().join());

            TimeWindowScanRange ten_units = new TimeWindowScanRange(TEN_UNITS, 10101, TupleRange.ALL);
            assertEquals(Arrays.asList("achilles", "hector"),
                    leaderboards.scanIndexByTimeWindow(ten_units)
                            .map(leaderboards::getName).asList().join());

            TimeWindowScanRange five_units = new TimeWindowScanRange(FIVE_UNITS, 10102, TupleRange.ALL);
            assertEquals(Arrays.asList("hector", "achilles"),
                    leaderboards.scanIndexByTimeWindow(five_units)
                            .map(leaderboards::getName).asList().join());

            TimeWindowScanRange five_units_2 = new TimeWindowScanRange(FIVE_UNITS, 10106, TupleRange.ALL);
            assertEquals(Arrays.asList("achilles", "hector"),
                    leaderboards.scanIndexByTimeWindow(five_units_2)
                            .map(leaderboards::getName).asList().join());

            final FDBStoredRecord<Message> rec = leaderboards.findByName("helen");
            assertEquals((Long)1L, leaderboards.evaluateQueryFunction(leaderboards.queryRank(), rec));
            assertEquals(null, leaderboards.evaluateQueryFunction(leaderboards.queryTimeWindowRank(TEN_UNITS, 10102), rec));
        }
    }

    @Test
    public void basicGroupedNested() {
        basicGrouped(new GroupedNestedLeaderboards());
    }

    @Test
    public void flat() {
        basicGrouped(new FlatLeaderboards());
    }

    protected void basicGrouped(Leaderboards leaderboards) {
        basicSetup(leaderboards, true);
        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, false);

            TupleRange game_1 = TupleRange.allOf(Tuple.from("game-1"));
            assertEquals(Arrays.asList("patroclus", "hecuba", "achilles", "hector"),
                    leaderboards.scanIndexByRank(game_1)
                            .map(leaderboards::getName).asList().join());

            TupleRange top_2 = new TupleRange(Tuple.from("game-1", 0), Tuple.from("game-1", 1), EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE);
            assertEquals(Arrays.asList("patroclus", "hecuba"),
                    leaderboards.scanIndexByRank(top_2)
                            .map(leaderboards::getName).asList().join());

            TupleRange no_2 = TupleRange.allOf(Tuple.from("game-1", 1));
            assertEquals(Arrays.asList("hecuba"),
                    leaderboards.scanIndexByRank(no_2)
                            .map(leaderboards::getName).asList().join());

            TimeWindowScanRange ten_units = new TimeWindowScanRange(TEN_UNITS, 10100, TupleRange.allOf(Tuple.from("game-1")));
            assertEquals(Arrays.asList("achilles", "hector"),
                    leaderboards.scanIndexByTimeWindow(ten_units)
                            .map(leaderboards::getName).asList().join());
            assertEquals(Arrays.asList("achilles"),
                    leaderboards.scanIndexByTimeWindowWithLimit(ten_units, 1)
                            .map(leaderboards::getName).asList().join());

            TimeWindowScanRange top_2_ten_units = new TimeWindowScanRange(TEN_UNITS, 10102, new TupleRange(Tuple.from("game-1", 0), Tuple.from("game-1", 1), EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE));
            assertEquals(Arrays.asList("achilles", "hector"),
                    leaderboards.scanIndexByTimeWindow(top_2_ten_units)
                            .map(leaderboards::getName).asList().join());

            TimeWindowScanRange top_2_five_units = new TimeWindowScanRange(FIVE_UNITS, 10100, new TupleRange(Tuple.from("game-1", 0), Tuple.from("game-1", 1), EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE));
            assertEquals(Arrays.asList("hector", "achilles"),
                    leaderboards.scanIndexByTimeWindow(top_2_five_units)
                            .map(leaderboards::getName).asList().join());

            final FDBStoredRecord<Message> rec1 = leaderboards.findByName("patroclus");
            final FDBStoredRecord<Message> rec2 = leaderboards.findByName("achilles");
            final FDBStoredRecord<Message> rec3 = leaderboards.findByName("hecuba");

            final QueryRecordFunction<Long> rank1 = leaderboards.queryRank();
            assertEquals((Long)0L, leaderboards.evaluateQueryFunction(rank1, rec1));
            assertEquals((Long)2L, leaderboards.evaluateQueryFunction(rank1, rec2));
            assertEquals((Long)1L, leaderboards.evaluateQueryFunction(rank1, rec3));

            final QueryRecordFunction<Long> rank2 = leaderboards.queryTimeWindowRank(TEN_UNITS, 10100);
            assertEquals(null, leaderboards.evaluateQueryFunction(rank2, rec1));
            assertEquals((Long)0L, leaderboards.evaluateQueryFunction(rank2, rec2));
            assertEquals(null, leaderboards.evaluateQueryFunction(rank2, rec3));

            final QueryRecordFunction<Tuple> entry1 = leaderboards.queryTimeWindowRankAndEntry(TimeWindowLeaderboard.ALL_TIME_LEADERBOARD_TYPE, -1);
            assertEquals(Tuple.from(0, 1000, 11001, 111), leaderboards.evaluateQueryFunction(entry1, rec1));
            assertEquals(Tuple.from(2, 300, 10201, 668), leaderboards.evaluateQueryFunction(entry1, rec2));
            assertEquals(Tuple.from(1, 750, 11201, 888), leaderboards.evaluateQueryFunction(entry1, rec3));

            final QueryRecordFunction<Tuple> entry2 = leaderboards.queryTimeWindowRankAndEntry(TEN_UNITS, 10100);
            assertEquals(null, leaderboards.evaluateQueryFunction(entry2, rec1));
            assertEquals(Tuple.from(0, 200, 10105, 667), leaderboards.evaluateQueryFunction(entry2, rec2));
            assertEquals(null, leaderboards.evaluateQueryFunction(entry2, rec3));

            final IndexAggregateFunction count1 = leaderboards.timeWindowCount(TimeWindowLeaderboard.ALL_TIME_LEADERBOARD_TYPE, -1);
            assertEquals((Long)4L, leaderboards.evaluateAggregateFunction(count1, Tuple.from("game-1")).get(0));
            assertEquals((Long)1L, leaderboards.evaluateAggregateFunction(count1, Tuple.from("game-2")).get(0));

            final IndexAggregateFunction count2 = leaderboards.timeWindowCount(TEN_UNITS, 10100);
            assertEquals((Long)2L, leaderboards.evaluateAggregateFunction(count2, Tuple.from("game-1")).get(0));
        }
    }

    @Test
    public void queryGroupedNested() {
        basicQuery(new GroupedNestedLeaderboards());
    }

    @Test
    public void queryFlat() {
        basicQuery(new FlatLeaderboards());
    }

    protected void basicQuery(Leaderboards leaderboards) {
        basicSetup(leaderboards, true);
        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, false);

            final String recordType = leaderboards.getRecordType();

            final RecordQuery query1 = RecordQuery.newBuilder()
                    .setRecordType(recordType)
                    .setFilter(Query.and(
                            Query.field("game_id").equalsValue("game-1"),
                            leaderboards.queryRank().greaterThanOrEquals(1L),
                            leaderboards.queryRank().lessThanOrEquals(2L)))
                    .build();
            final RecordQueryPlan plan1 = leaderboards.planQuery(query1);
            assertEquals("Index(LeaderboardIndex [[game-1, 1],[game-1, 2]] BY_RANK)", plan1.toString());
            assertEquals(Arrays.asList("hecuba", "achilles"),
                    leaderboards.executeQuery(plan1).map(leaderboards::getName).asList().join());

            final RecordQuery query2 = RecordQuery.newBuilder()
                    .setRecordType(recordType)
                    .setFilter(Query.and(
                            Query.field("game_id").equalsValue("game-1"),
                            leaderboards.queryTimeWindowRank("l1", "l2").lessThanOrEquals(2L)))
                    .build();
            RecordQueryPlan plan2 = leaderboards.planQuery(query2);
            assertEquals("Index(LeaderboardIndex ([game-1, null],[game-1, 2]]@$l1,$l2 BY_TIME_WINDOW)", plan2.toString());

            final EvaluationContext evaluationContext1 = EvaluationContext.newBuilder()
                    .setBinding("l1", FIVE_UNITS)
                    .setBinding("l2", 10103)
                    .build(TypeRepository.empty());
            assertEquals(Arrays.asList("hector", "achilles"),
                    leaderboards.executeQuery(plan2, evaluationContext1).map(leaderboards::getName).asList().join());

            final EvaluationContext evaluationContext2 = EvaluationContext.newBuilder()
                    .setBinding("l1", FIVE_UNITS)
                    .setBinding("l2", 10105)
                    .build(TypeRepository.empty());
            assertEquals(Arrays.asList("achilles", "hector"),
                    leaderboards.executeQuery(plan2, evaluationContext2).map(leaderboards::getName).asList().join());
        }
    }

    @Test
    public void queryRanges() {
        Leaderboards leaderboards = new GroupedNestedLeaderboards();
        basicSetup(leaderboards, true);
        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, false);

            final String recordType = leaderboards.getRecordType();

            if (false) { // TODO: Planner does not handle this case yet.
                final RecordQuery query = RecordQuery.newBuilder()
                        .setRecordType(recordType)
                        .setFilter(Query.field("game_id").equalsValue("game-1"))
                        .setSort(leaderboards.getKeyExpression())
                        .build();
                final RecordQueryPlan plan = leaderboards.planQuery(query);
                assertEquals("Index(LeaderboardIndex [[game-1],[game-1]] BY_RANK)", plan.toString());
                assertEquals(Arrays.asList("patroclus", "hecuba", "achilles", "hector"),
                        leaderboards.executeQuery(plan).map(leaderboards::getName).asList().join());
            }

            final RecordQuery queryGreaterEqual = RecordQuery.newBuilder()
                    .setRecordType(recordType)
                    .setFilter(Query.and(
                            Query.field("game_id").equalsValue("game-1"),
                            leaderboards.queryRank().greaterThanOrEquals(2L)))
                    .build();
            final RecordQueryPlan planGreaterEqual = leaderboards.planQuery(queryGreaterEqual);
            assertEquals("Index(LeaderboardIndex [[game-1, 2],[game-1]] BY_RANK)", planGreaterEqual.toString());
            assertEquals(Arrays.asList("achilles", "hector"),
                    leaderboards.executeQuery(planGreaterEqual).map(leaderboards::getName).asList().join());

            final RecordQuery queryGreaterThan = RecordQuery.newBuilder()
                    .setRecordType(recordType)
                    .setFilter(Query.and(
                            Query.field("game_id").equalsValue("game-1"),
                            leaderboards.queryRank().greaterThan(1L)))
                    .build();
            final RecordQueryPlan planGreaterThan = leaderboards.planQuery(queryGreaterThan);
            assertEquals("Index(LeaderboardIndex ([game-1, 1],[game-1]] BY_RANK)", planGreaterThan.toString());
            assertEquals(Arrays.asList("achilles", "hector"),
                    leaderboards.executeQuery(planGreaterThan).map(leaderboards::getName).asList().join());

            final RecordQuery queryLessEqual = RecordQuery.newBuilder()
                    .setRecordType(recordType)
                    .setFilter(Query.and(
                            Query.field("game_id").equalsValue("game-1"),
                            leaderboards.queryRank().lessThanOrEquals(2L)))
                    .build();
            final RecordQueryPlan planLessEqual = leaderboards.planQuery(queryLessEqual);
            assertEquals("Index(LeaderboardIndex ([game-1, null],[game-1, 2]] BY_RANK)", planLessEqual.toString());
            assertEquals(Arrays.asList("patroclus", "hecuba", "achilles"),
                    leaderboards.executeQuery(planLessEqual).map(leaderboards::getName).asList().join());

            final RecordQuery queryLessThan = RecordQuery.newBuilder()
                    .setRecordType(recordType)
                    .setFilter(Query.and(
                            Query.field("game_id").equalsValue("game-1"),
                            leaderboards.queryRank().lessThan(2L)))
                    .build();
            final RecordQueryPlan planLessThan = leaderboards.planQuery(queryLessThan);
            assertEquals("Index(LeaderboardIndex ([game-1, null],[game-1, 2]) BY_RANK)", planLessThan.toString());
            assertEquals(Arrays.asList("patroclus", "hecuba"),
                    leaderboards.executeQuery(planLessThan).map(leaderboards::getName).asList().join());

            final RecordQuery queryGreaterEqualAndLessEqual = RecordQuery.newBuilder()
                    .setRecordType(recordType)
                    .setFilter(Query.and(
                            Query.field("game_id").equalsValue("game-1"),
                            leaderboards.queryRank().greaterThanOrEquals(1L),
                            leaderboards.queryRank().lessThanOrEquals(2L)))
                    .build();
            final RecordQueryPlan planGreaterEqualAndLessEqual = leaderboards.planQuery(queryGreaterEqualAndLessEqual);
            assertEquals("Index(LeaderboardIndex [[game-1, 1],[game-1, 2]] BY_RANK)", planGreaterEqualAndLessEqual.toString());
            assertEquals(Arrays.asList("hecuba", "achilles"),
                    leaderboards.executeQuery(planGreaterEqualAndLessEqual).map(leaderboards::getName).asList().join());

            final RecordQuery queryGreaterThanAndLessThan = RecordQuery.newBuilder()
                    .setRecordType(recordType)
                    .setFilter(Query.and(
                            Query.field("game_id").equalsValue("game-1"),
                            leaderboards.queryRank().greaterThan(1L),
                            leaderboards.queryRank().lessThan(4L)))
                    .build();
            final RecordQueryPlan planGreaterThanAndLessThan = leaderboards.planQuery(queryGreaterThanAndLessThan);
            assertEquals("Index(LeaderboardIndex ([game-1, 1],[game-1, 4]) BY_RANK)", planGreaterThanAndLessThan.toString());
            assertEquals(Arrays.asList("achilles", "hector"),
                    leaderboards.executeQuery(planGreaterThanAndLessThan).map(leaderboards::getName).asList().join());
        }
    }

    @Test
    public void scoreForRank() {
        Leaderboards leaderboards = new GroupedNestedLeaderboards();
        basicSetup(leaderboards, true);
        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, false);

            assertEquals(750L, leaderboards.evaluateAggregateFunction(leaderboards.scoreForTimeWindowRank(TimeWindowLeaderboard.ALL_TIME_LEADERBOARD_TYPE, -1), Tuple.from("game-1", 1L)).get(0));
        }
    }

    @Test
    public void rankForScore() {
        Leaderboards leaderboards = new GroupedNestedLeaderboards();
        basicSetup(leaderboards, true);
        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, false);

            assertEquals(1L, leaderboards.evaluateAggregateFunction(leaderboards.timeWindowRankForScore(TimeWindowLeaderboard.ALL_TIME_LEADERBOARD_TYPE, -1), Tuple.from("game-1", 750)).get(0));
            // This score is not present; it would take over 2nd place.
            assertEquals(1L, leaderboards.evaluateAggregateFunction(leaderboards.timeWindowRankForScore(TimeWindowLeaderboard.ALL_TIME_LEADERBOARD_TYPE, -1), Tuple.from("game-1", 751)).get(0));
        }
    }

    @Test
    public void changeUngrouped() {
        Leaderboards leaderboards = new UngroupedNestedLeaderboards();
        basicSetup(leaderboards, true);
        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, false);

            assertEquals(Arrays.asList("patroclus", "helen", "hecuba", "achilles", "hector"),
                    leaderboards.scanIndexByRank(TupleRange.ALL)
                            .map(leaderboards::getName).asList().join());

            final FDBStoredRecord<Message> rec1 = leaderboards.findByName("helen");
            final FDBStoredRecord<Message> rec2 = leaderboards.findByName("achilles");

            assertEquals((Long)1L, leaderboards.evaluateQueryFunction(leaderboards.queryRank(), rec1));
            assertEquals(null, leaderboards.evaluateQueryFunction(leaderboards.queryTimeWindowRank(TEN_UNITS, 10102), rec1));
            assertEquals((Long)3L, leaderboards.evaluateQueryFunction(leaderboards.queryRank(), rec2));
            assertEquals((Long)0L, leaderboards.evaluateQueryFunction(leaderboards.queryTimeWindowRank(TEN_UNITS, 10102), rec2));
        }
        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, false);

            TestRecordsLeaderboardProto.NestedLeaderboardRecord.Builder recordBuilder =
                    TestRecordsLeaderboardProto.NestedLeaderboardRecord.newBuilder();
            recordBuilder.mergeFrom(leaderboards.findByName("achilles").getRecord());
            recordBuilder.removeScores(3);
            recordBuilder.removeScores(2);
            leaderboards.recordStore.saveRecord(recordBuilder.build());

            assertEquals(Arrays.asList("patroclus", "helen", "hecuba", "hector", "achilles"),
                    leaderboards.scanIndexByRank(TupleRange.ALL)
                            .map(leaderboards::getName).asList().join());

            final FDBStoredRecord<Message> rec1 = leaderboards.findByName("helen");
            final FDBStoredRecord<Message> rec2 = leaderboards.findByName("achilles");

            assertEquals((Long)1L, leaderboards.evaluateQueryFunction(leaderboards.queryRank(), rec1));
            assertEquals(null, leaderboards.evaluateQueryFunction(leaderboards.queryTimeWindowRank(TEN_UNITS, 10102), rec1));
            assertEquals((Long)4L, leaderboards.evaluateQueryFunction(leaderboards.queryRank(), rec2));
            assertEquals((Long)1L, leaderboards.evaluateQueryFunction(leaderboards.queryTimeWindowRank(TEN_UNITS, 10102), rec2));
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, false);

            leaderboards.addScores("achilles", "game-1", 350, 10108, 669);
                
            assertEquals(Arrays.asList("patroclus", "helen", "hecuba", "achilles", "hector"),
                    leaderboards.scanIndexByRank(TupleRange.ALL)
                            .map(leaderboards::getName).asList().join());

            final FDBStoredRecord<Message> rec1 = leaderboards.findByName("helen");
            final FDBStoredRecord<Message> rec2 = leaderboards.findByName("achilles");

            assertEquals((Long)1L, leaderboards.evaluateQueryFunction(leaderboards.queryRank(), rec1));
            assertEquals(null, leaderboards.evaluateQueryFunction(leaderboards.queryTimeWindowRank(TEN_UNITS, 10102), rec1));
            assertEquals((Long)3L, leaderboards.evaluateQueryFunction(leaderboards.queryRank(), rec2));
            assertEquals((Long)0L, leaderboards.evaluateQueryFunction(leaderboards.queryTimeWindowRank(TEN_UNITS, 10102), rec2));
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, false);

            leaderboards.recordStore.deleteRecord(Tuple.from("patroclus"));
                
            assertEquals(Arrays.asList("helen", "hecuba", "achilles", "hector"),
                    leaderboards.scanIndexByRank(TupleRange.ALL)
                            .map(leaderboards::getName).asList().join());

            final FDBStoredRecord<Message> rec1 = leaderboards.findByName("helen");
            final FDBStoredRecord<Message> rec2 = leaderboards.findByName("achilles");

            assertEquals((Long)0L, leaderboards.evaluateQueryFunction(leaderboards.queryRank(), rec1));
            assertEquals(null, leaderboards.evaluateQueryFunction(leaderboards.queryTimeWindowRank(TEN_UNITS, 10102), rec1));
            assertEquals((Long)2L, leaderboards.evaluateQueryFunction(leaderboards.queryRank(), rec2));
            assertEquals((Long)0L, leaderboards.evaluateQueryFunction(leaderboards.queryTimeWindowRank(TEN_UNITS, 10102), rec2));
            context.commit();
        }
    }

    @Test
    public void changeGrouped() {
        Leaderboards leaderboards = new GroupedNestedLeaderboards();
        basicSetup(leaderboards, true);
        TupleRange game_1 = TupleRange.allOf(Tuple.from("game-1"));
        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, false);

            assertEquals(Arrays.asList("patroclus", "hecuba", "achilles", "hector"),
                    leaderboards.scanIndexByRank(game_1)
                            .map(leaderboards::getName).asList().join());

            final FDBStoredRecord<Message> rec1 = leaderboards.findByName("hecuba");
            final FDBStoredRecord<Message> rec2 = leaderboards.findByName("achilles");

            assertEquals((Long)1L, leaderboards.evaluateQueryFunction(leaderboards.queryRank(), rec1));
            assertEquals(null, leaderboards.evaluateQueryFunction(leaderboards.queryTimeWindowRank(TEN_UNITS, 10102), rec1));
            assertEquals((Long)2L, leaderboards.evaluateQueryFunction(leaderboards.queryRank(), rec2));
            assertEquals((Long)0L, leaderboards.evaluateQueryFunction(leaderboards.queryTimeWindowRank(TEN_UNITS, 10102), rec2));
        }
        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, false);

            TestRecordsLeaderboardProto.NestedLeaderboardRecord.Builder recordBuilder =
                    TestRecordsLeaderboardProto.NestedLeaderboardRecord.newBuilder();
            recordBuilder.mergeFrom(leaderboards.findByName("achilles").getRecord());
            recordBuilder.removeScores(3);
            recordBuilder.removeScores(2);
            leaderboards.recordStore.saveRecord(recordBuilder.build());

            assertEquals(Arrays.asList("patroclus", "hecuba", "hector", "achilles"),
                    leaderboards.scanIndexByRank(game_1)
                            .map(leaderboards::getName).asList().join());

            final FDBStoredRecord<Message> rec1 = leaderboards.findByName("hecuba");
            final FDBStoredRecord<Message> rec2 = leaderboards.findByName("achilles");

            assertEquals((Long)1L, leaderboards.evaluateQueryFunction(leaderboards.queryRank(), rec1));
            assertEquals(null, leaderboards.evaluateQueryFunction(leaderboards.queryTimeWindowRank(TEN_UNITS, 10102), rec1));
            assertEquals((Long)3L, leaderboards.evaluateQueryFunction(leaderboards.queryRank(), rec2));
            assertEquals((Long)1L, leaderboards.evaluateQueryFunction(leaderboards.queryTimeWindowRank(TEN_UNITS, 10102), rec2));
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, false);

            leaderboards.addScores("achilles", "game-1", 350, 10108, 669);
                
            assertEquals(Arrays.asList("patroclus", "hecuba", "achilles", "hector"),
                    leaderboards.scanIndexByRank(game_1)
                            .map(leaderboards::getName).asList().join());

            final FDBStoredRecord<Message> rec1 = leaderboards.findByName("hecuba");
            final FDBStoredRecord<Message> rec2 = leaderboards.findByName("achilles");

            assertEquals((Long)1L, leaderboards.evaluateQueryFunction(leaderboards.queryRank(), rec1));
            assertEquals(null, leaderboards.evaluateQueryFunction(leaderboards.queryTimeWindowRank(TEN_UNITS, 10102), rec1));
            assertEquals((Long)2L, leaderboards.evaluateQueryFunction(leaderboards.queryRank(), rec2));
            assertEquals((Long)0L, leaderboards.evaluateQueryFunction(leaderboards.queryTimeWindowRank(TEN_UNITS, 10102), rec2));
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, false);

            leaderboards.recordStore.deleteRecord(Tuple.from("patroclus"));
                
            assertEquals(Arrays.asList("hecuba", "achilles", "hector"),
                    leaderboards.scanIndexByRank(game_1)
                            .map(leaderboards::getName).asList().join());

            final FDBStoredRecord<Message> rec1 = leaderboards.findByName("hecuba");
            final FDBStoredRecord<Message> rec2 = leaderboards.findByName("achilles");

            assertEquals((Long)0L, leaderboards.evaluateQueryFunction(leaderboards.queryRank(), rec1));
            assertEquals(null, leaderboards.evaluateQueryFunction(leaderboards.queryTimeWindowRank(TEN_UNITS, 10102), rec1));
            assertEquals((Long)1L, leaderboards.evaluateQueryFunction(leaderboards.queryRank(), rec2));
            assertEquals((Long)0L, leaderboards.evaluateQueryFunction(leaderboards.queryTimeWindowRank(TEN_UNITS, 10102), rec2));
            context.commit();
        }
    }

    @Test
    public void omitAllTime() {
        Leaderboards leaderboards = new GroupedNestedLeaderboards();
        leaderboards.buildMetaData();
        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, true);
            leaderboards.recordStore.performIndexOperation("LeaderboardIndex",
                    new TimeWindowLeaderboardWindowUpdate(System.currentTimeMillis(), true,
                            10100,
                            false, // No allTime.
                            Arrays.asList(
                                    new TimeWindowLeaderboardWindowUpdate.TimeWindowSpec(TEN_UNITS, 10100, 5, 10, 20)
                            ), TimeWindowLeaderboardWindowUpdate.Rebuild.IF_OVERLAPPING_CHANGED));
            addInitialScores(leaderboards);
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, false);
            TupleRange game_1 = TupleRange.allOf(Tuple.from("game-1"));
            assertEquals(Collections.emptyList(),
                    leaderboards.scanIndexByRank(game_1)
                            .map(leaderboards::getName).asList().join());

            TimeWindowScanRange ten_units = new TimeWindowScanRange(TEN_UNITS, 10100, TupleRange.allOf(Tuple.from("game-1")));
            assertEquals(Arrays.asList("achilles", "hector"),
                    leaderboards.scanIndexByTimeWindow(ten_units)
                            .map(leaderboards::getName).asList().join());

            final FDBStoredRecord<Message> rec = leaderboards.findByName("achilles");

            final QueryRecordFunction<Long> rank1 = leaderboards.queryRank();
            assertEquals(null, leaderboards.evaluateQueryFunction(rank1, rec));

            final QueryRecordFunction<Long> rank2 = leaderboards.queryTimeWindowRank(TEN_UNITS, 10100);
            assertEquals((Long)0L, leaderboards.evaluateQueryFunction(rank2, rec));
        }
    }

    @Test
    public void rebuildOverlapping() {
        updateOverlapping(true);
    }        

    @Test
    public void noRebuildOverlapping() {
        updateOverlapping(false);
    }        

    private void updateOverlapping(boolean rebuild) {
        Leaderboards leaderboards = new GroupedNestedLeaderboards();
        basicSetup(leaderboards, true);
        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, false);

            metrics.reset();
            TimeWindowLeaderboardWindowUpdateResult result = leaderboards.updateWindows(true, 10100);
            assertFalse(result.isChanged());
            assertFalse(result.isRebuilt());
            assertEquals(0, metrics.getCount(FDBStoreTimer.Events.REBUILD_INDEX));

            // NOTE: no commit.
        }
        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, false);

            metrics.reset();
            TimeWindowLeaderboardWindowUpdateResult result = leaderboards.updateWindows(true, 11500);
            assertTrue(result.isChanged());
            assertFalse(result.isRebuilt());
            assertEquals(0, metrics.getCount(FDBStoreTimer.Events.REBUILD_INDEX));

            // NOTE: no commit.
        }
        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, false);

            metrics.reset();
            TimeWindowLeaderboardWindowUpdateResult result = leaderboards.updateWindows(true, 10500,
                    rebuild ? TimeWindowLeaderboardWindowUpdate.Rebuild.IF_OVERLAPPING_CHANGED : TimeWindowLeaderboardWindowUpdate.Rebuild.NEVER);
            assertTrue(result.isChanged());
            if (rebuild) {
                assertTrue(result.isRebuilt());
            } else {
                assertFalse(result.isRebuilt());
            }
            assertEquals(1, metrics.getCount(FDBStoreTimer.Counts.TIME_WINDOW_LEADERBOARD_OVERLAPPING_CHANGED));
            assertEquals(rebuild ? 1 : 0, metrics.getCount(FDBStoreTimer.Events.REBUILD_INDEX_EXPLICIT));
            assertEquals(rebuild ? 1 : 0, metrics.getCount(FDBStoreTimer.Events.REBUILD_INDEX));

            // NOTE: no commit.
        }
    }

    @Test
    public void rebuildChangeDirection() {
        Leaderboards leaderboards = new GroupedNestedLeaderboards();
        basicSetup(leaderboards, false);

        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, false);

            TupleRange game_1 = TupleRange.allOf(Tuple.from("game-1"));
            assertEquals(Arrays.asList("achilles", "hector", "hecuba", "patroclus"),
                    leaderboards.scanIndexByRank(game_1)
                            .map(leaderboards::getName).asList().join());

            TimeWindowScanRange top_2_ten_units = new TimeWindowScanRange(TEN_UNITS, 10100, new TupleRange(Tuple.from("game-1", 0), Tuple.from("game-1", 1), EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE));
            assertEquals(Arrays.asList("achilles", "hector"),
                    leaderboards.scanIndexByTimeWindow(top_2_ten_units)
                            .map(leaderboards::getName).asList().join());
        }

        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, false);

            metrics.reset();
            TimeWindowLeaderboardWindowUpdateResult result = leaderboards.updateWindows(true, 10100);
            assertTrue(result.isChanged());
            assertTrue(result.isRebuilt());
            assertEquals(1, metrics.getCount(FDBStoreTimer.Events.REBUILD_INDEX_EXPLICIT));
            assertEquals(1, metrics.getCount(FDBStoreTimer.Events.REBUILD_INDEX));

            context.commit();
        }

        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, false);

            TupleRange game_1 = TupleRange.allOf(Tuple.from("game-1"));
            assertEquals(Arrays.asList("patroclus", "hecuba", "achilles", "hector"),
                    leaderboards.scanIndexByRank(game_1)
                            .map(leaderboards::getName).asList().join());

            TimeWindowScanRange top_2_ten_units = new TimeWindowScanRange(TEN_UNITS, 10100, new TupleRange(Tuple.from("game-1", 0), Tuple.from("game-1", 1), EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_INCLUSIVE));
            assertEquals(Arrays.asList("achilles", "hector"),
                    leaderboards.scanIndexByTimeWindow(top_2_ten_units)
                            .map(leaderboards::getName).asList().join());
        }
    }

    @Test
    public void trimScores() {
        Leaderboards leaderboards = new FlatLeaderboards();
        basicSetup(leaderboards, true);
        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, false);

            List<Key.Evaluated> untrimmed = leaderboards.getScores(leaderboards.findByName("achilles"));
            assertEquals(Arrays.asList(
                    Key.Evaluated.concatenate("game-1", 100L, 10101L, 666L),
                    Key.Evaluated.concatenate("game-1", 99L, 10102L, 665L),
                    Key.Evaluated.concatenate("game-1", 200L, 10105L, 667L),
                    Key.Evaluated.concatenate("game-1", 300L, 10201L, 668L)),
                    untrimmed);
            Collection<Tuple> trimmed = leaderboards.trim(untrimmed);
            assertEquals(Arrays.asList(
                    Tuple.from("game-1", 300L, 10201L, 668L),
                    Tuple.from("game-1", 200L, 10105L, 667L),
                    Tuple.from("game-1", 100L, 10101L, 666L)),
                    trimmed);
        }
    }

    @Test
    public void mostNegativeHighScoreFirst() {
        Leaderboards leaderboards = new FlatLeaderboards();
        basicSetup(leaderboards, true);
        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, false);

            assertEquals(4L, leaderboards.evaluateAggregateFunction(leaderboards.timeWindowRankForScore(TimeWindowLeaderboard.ALL_TIME_LEADERBOARD_TYPE, -1), Tuple.from("game-1", Long.MIN_VALUE + 1)).get(0));
            assertEquals(4L, leaderboards.evaluateAggregateFunction(leaderboards.timeWindowRankForScore(TimeWindowLeaderboard.ALL_TIME_LEADERBOARD_TYPE, -1), Tuple.from("game-1", Long.MIN_VALUE)).get(0));
            assertEquals(4L, leaderboards.evaluateAggregateFunction(leaderboards.timeWindowRankForScore(TimeWindowLeaderboard.ALL_TIME_LEADERBOARD_TYPE, -1), Tuple.from("game-1", BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE))).get(0));
        }
        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, false);
            leaderboards.recordStore.deleteRecord(leaderboards.findByName("helen").getPrimaryKey());
            leaderboards.addScores("helen", "game-1", Long.MIN_VALUE, 10101, 888);

            TupleRange game_1 = TupleRange.allOf(Tuple.from("game-1"));
            assertEquals(Arrays.asList("patroclus", "hecuba", "achilles", "hector", "helen"),
                    leaderboards.scanIndexByRank(game_1)
                            .map(leaderboards::getName).asList().join());
        }
    }

    @Test
    public void subDirectoryLow() {
        subDirectory(false);
    }

    @Test
    public void subDirectoryHigh() {
        subDirectory(true);
    }

    private void subDirectory(boolean highScoreFirst) {
        Leaderboards leaderboards = new FlatLeaderboards();
        leaderboards.buildMetaData();
        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, true);
            leaderboards.updateWindows(false, 10100);
            leaderboards.setGroupHighScoreFirst(Tuple.from("game-1"), highScoreFirst);
            addInitialScores(leaderboards);
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, false);

            TupleRange game_1 = TupleRange.allOf(Tuple.from("game-1"));
            assertEquals(highScoreFirst ?
                         Arrays.asList("patroclus", "hecuba", "achilles", "hector") :
                         Arrays.asList("achilles", "hector", "hecuba", "patroclus"),
                    leaderboards.scanIndexByRank(game_1)
                            .map(leaderboards::getName).asList().join());
        }
    }

    @Test
    public void deleteWhere() {
        Leaderboards leaderboards = new GroupedNestedLeaderboards();
        leaderboards.buildMetaData(metaDataBuilder -> {
            // Make the primary keys include the game so we can range delete game's records.
            metaDataBuilder.getRecordType("NestedLeaderboardRecord").setPrimaryKey(Key.Expressions.concat(
                    Key.Expressions.field("game_id"),
                    Key.Expressions.field("name")));
            metaDataBuilder.getRecordType("FlatLeaderboardRecord").setPrimaryKey(Key.Expressions.concat(
                    Key.Expressions.field("game_id"),
                    Key.Expressions.field("name")));
            leaderboards.addIndex(metaDataBuilder);
        });
        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, true);
            leaderboards.updateWindows(true, 10100);
            addInitialScores(leaderboards);
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, false);

            TupleRange game_1 = TupleRange.allOf(Tuple.from("game-1"));
            TupleRange game_2 = TupleRange.allOf(Tuple.from("game-2"));

            assertEquals(Arrays.asList("patroclus", "hecuba", "achilles", "hector"),
                    leaderboards.scanIndexByRank(game_1)
                            .map(leaderboards::getName).asList().join());
            assertEquals(Arrays.asList("helen"),
                    leaderboards.scanIndexByRank(game_2)
                            .map(leaderboards::getName).asList().join());

            leaderboards.recordStore.deleteRecordsWhere(Query.field("game_id").equalsValue("game-1"));

            assertEquals(Collections.emptyList(),
                    leaderboards.scanIndexByRank(game_1)
                            .map(leaderboards::getName).asList().join());
            assertEquals(Arrays.asList("helen"),
                    leaderboards.scanIndexByRank(game_2)
                            .map(leaderboards::getName).asList().join());
        }
    }

    @Test
    public void deleteWhereWithoutGamePrimaryKey() {
        Leaderboards leaderboards = new GroupedNestedLeaderboards();
        basicSetup(leaderboards, true);
        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, false);
            assertThrows(Query.InvalidExpressionException.class, () ->
                    leaderboards.recordStore.deleteRecordsWhere(Query.field("game_id").equalsValue(1)));
        }
    }

    @Test
    public void notSuitableAsPrefix() {
        Leaderboards leaderboards = new FlatLeaderboards();
        basicSetup(leaderboards, false);

        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, false);
            leaderboards.addScores("diomedes", "game-1");

            final String recordType = leaderboards.getRecordType();

            final RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(recordType)
                    .setFilter(Query.field("game_id").equalsValue("game-1"))
                    .build();
            final RecordQueryPlan plan = leaderboards.planQuery(query);
            assertTrue(leaderboards.executeQuery(plan).map(leaderboards::getName).asList().join().contains("diomedes"), "should have player without scores");
            assertFalse(plan.hasIndexScan("LeaderboardIndex"), "should not use leaderboard");
        }
    }

    @Test
    public void concurrentAdd() throws Exception {
        Leaderboards leaderboards = new FlatLeaderboards();
        leaderboards.buildMetaData();
        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, true);
            leaderboards.updateWindows(true, 10100);
            leaderboards.addScores("player-0", "game-1", 100, 10100, 0);
            context.commit();
        }

        FDBRecordContext context1 = openContext();
        if (FDBDatabaseExtension.TRACE) {
            context1.ensureActive().options().setDebugTransactionIdentifier("tr1");
            context1.ensureActive().options().setLogTransaction();
        }
        Leaderboards leaderboards1 = new FlatLeaderboards();
        leaderboards1.recordStore = leaderboards.recordStore.asBuilder().setContext(context1).build();
        FDBRecordContext context2 = openContext();
        if (FDBDatabaseExtension.TRACE) {
            context2.ensureActive().options().setDebugTransactionIdentifier("tr2");
            context2.ensureActive().options().setLogTransaction();
        }
        Leaderboards leaderboards2 = new FlatLeaderboards();
        leaderboards2.recordStore = leaderboards.recordStore.asBuilder().setContext(context2).build();

        leaderboards1.addScores("player-1", "game-1", 101, 10100, 0);
        leaderboards2.addScores("player-2", "game-1", 102, 10100, 0);

        context1.commit();
        context2.commit();
    }

    @Test
    @Tag(Tags.Slow)
    public void parallel() throws Exception {
        Leaderboards leaderboards = new FlatLeaderboards();
        leaderboards.buildMetaData();
        try (FDBRecordContext context = openContext()) {
            leaderboards.openRecordStore(context, true);
            leaderboards.updateWindows(true, 10100);
            context.commit();
        }
        FDBRecordStore.Builder builder = leaderboards.recordStore.asBuilder();
        parallelThread(builder, 0, 500).run();
        Thread[] threads = new Thread[2];
        Map<String, String> uncaught = new HashMap<>();
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(parallelThread(builder.copyBuilder(), i + 1, 10));
            threads[i].setUncaughtExceptionHandler((t, e) -> uncaught.put("uncaught " + t.getName(), e.toString()));
        }
        for (int i = 0; i < threads.length; i++) {
            threads[i].start();
        }
        for (int i = 0; i < threads.length; i++) {
            threads[i].join();
        }
        final KeyValueLogMessage msg = KeyValueLogMessage.build("Results");
        msg.addKeysAndValues(metrics.getKeysAndValues());
        msg.addKeysAndValues(uncaught);
        System.out.println(msg.toString());
        assertThat(uncaught.values(), Matchers.emptyCollectionOf(String.class));
    }

    protected Runnable parallelThread(FDBRecordStore.Builder builder, int n, int count) {
        final String player = "player-" + n;
        final Random r = new Random();
        final Leaderboards privateLeaderboards = new FlatLeaderboards();
        return () -> {
            for (int i = 0; i < count; i++) {
                int score = r.nextInt(1000000);
                int pass = i;
                fdb.run(metrics, null, context -> {
                    context.ensureActive().options().setDebugTransactionIdentifier("t-" + n + "-" + pass);
                    context.ensureActive().options().setLogTransaction();
                    privateLeaderboards.recordStore = builder.setContext(context).build();
                    privateLeaderboards.addScores(player, "game-1", score, 10100, 0);
                    return null;
                });
            }
        };
    }

}
