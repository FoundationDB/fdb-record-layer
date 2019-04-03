/*
 * RankIndexTest.java
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.EndpointType;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.FunctionNames;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.PipelineOperation;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordFunction;
import com.apple.foundationdb.record.RecordIndexUniquenessViolation;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsRankProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexRecordFunction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryRecordFunction;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.Tags;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.coveringIndexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScanType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@code RANK} type indexes.
 */
@Tag(Tags.RequiresFDB)
public class RankIndexTest extends FDBRecordStoreTestBase {

    protected void openRecordStore(FDBRecordContext context) throws Exception {
        openRecordStore(context, NO_HOOK);
    }

    protected void openRecordStore(FDBRecordContext context, FDBRecordStoreTest.RecordMetaDataHook hook) throws Exception {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsRankProto.getDescriptor());
        metaDataBuilder.addIndex("BasicRankedRecord",
                new Index("rank_by_gender", Key.Expressions.field("score").groupBy(Key.Expressions.field("gender")),
                        IndexTypes.RANK));
        metaDataBuilder.addIndex("NestedRankedRecord",
                new Index("score_by_country",
                        Key.Expressions.concat(Key.Expressions.field("country"),
                                Key.Expressions.field("scores", KeyExpression.FanType.FanOut)
                                        .nest(Key.Expressions.concatenateFields("game", "tier", "score")))
                                .group(2),
                        IndexTypes.RANK));
        metaDataBuilder.getRecordType("HeaderRankedRecord").setPrimaryKey(Key.Expressions.field("header").nest(Key.Expressions.concatenateFields("group", "id")));
        metaDataBuilder.addIndex("HeaderRankedRecord",
                new Index("score_by_nested_id",
                        Key.Expressions.field("score").groupBy(Key.Expressions.field("header").nest(Key.Expressions.field("group"))),
                        IndexTypes.RANK));
        metaDataBuilder.addIndex("RepeatedRankedRecord",
                new Index("score_by_repeated_field",
                        Key.Expressions.field("score", KeyExpression.FanType.FanOut).ungrouped(),
                        IndexTypes.RANK));
        hook.apply(metaDataBuilder);
        createOrOpenRecordStore(context, metaDataBuilder.getRecordMetaData());
    }

    static final Object[][] RECORDS = new Object[][] {
        { "achilles", 100, "M" },
        { "helen", 200, "F" },
        { "hector", 75, "M" },
        { "penelope", 200, "F" },
        { "laodice", 300, "F" }
    };

    @BeforeEach
    public void loadRecords() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            for (Object[] rec : RECORDS) {
                recordStore.saveRecord(TestRecordsRankProto.BasicRankedRecord.newBuilder()
                                       .setName((String) rec[0])
                                       .setScore((Integer) rec[1])
                                       .setGender((String) rec[2])
                                       .build());
            }
            commit(context);
        }
    }

    @Test
    public void checkScores() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            FDBStoredRecord<Message> rec = recordStore.loadRecord(Tuple.from("achilles"));
            assertNotNull(rec);
            TestRecordsRankProto.BasicRankedRecord.Builder myrec = TestRecordsRankProto.BasicRankedRecord.newBuilder();
            myrec.mergeFrom(rec.getRecord());
            assertEquals(100, myrec.getScore());
            commit(context);
        }
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("BasicRankedRecord")
                .setFilter(Query.field("score").equalsValue(200))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertEquals("Index(BasicRankedRecord$score [[200],[200]])", plan.toString());

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecordsRankProto.BasicRankedRecord.Builder myrec = TestRecordsRankProto.BasicRankedRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertEquals(200, myrec.getScore());
                    i++;
                }
            }
            assertEquals(2, i);
        }
    }

    @Test
    public void checkRankScan() throws Exception {
        TupleRange range = new TupleRange(Tuple.from(0L), Tuple.from(2L), EndpointType.RANGE_INCLUSIVE, EndpointType.RANGE_EXCLUSIVE);
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            int i = 0;
            try (RecordCursorIterator<FDBIndexedRecord<Message>> cursor = recordStore.scanIndexRecords("BasicRankedRecord$score", IndexScanType.BY_RANK,
                                                                             range, null, ScanProperties.FORWARD_SCAN).asIterator()) {
                while (cursor.hasNext()) {
                    FDBIndexedRecord<Message> rec = cursor.next();
                    TestRecordsRankProto.BasicRankedRecord.Builder myrec = TestRecordsRankProto.BasicRankedRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertTrue(myrec.getScore() < 200);
                    i++;
                }
            }
            assertEquals(2, i);
        }
        range = TupleRange.allOf(Tuple.from("M", 0L));
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            try (RecordCursor<FDBIndexedRecord<Message>> cursor = recordStore.scanIndexRecords("rank_by_gender", IndexScanType.BY_RANK,
                    range, null, new ScanProperties(ExecuteProperties.newBuilder().setReturnedRowLimit(1).build()))) {
                FDBIndexedRecord<Message> rec = cursor.getNext().get();
                TestRecordsRankProto.BasicRankedRecord.Builder myrec = TestRecordsRankProto.BasicRankedRecord.newBuilder();
                myrec.mergeFrom(rec.getRecord());
                assertEquals("hector", myrec.getName());
                assertEquals(75, myrec.getScore());
            }
        }
    }

    @Test
    public void checkRanks() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            FDBStoredRecord<Message> rec = recordStore.loadRecord(Tuple.from("achilles"));
            assertNotNull(rec);
            RecordFunction<Long> rank = Query.rank("score").getFunction();
            assertEquals((Long)1L, recordStore.evaluateRecordFunction(rank, rec).get());
            commit(context);
        }
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("BasicRankedRecord")
                .setFilter(Query.rank("score").equalsValue(2L))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertEquals("Index(BasicRankedRecord$score [[2],[2]] BY_RANK)", plan.toString());

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            int i = 0;
            try (RecordCursorIterator<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan).asIterator()) {
                while (cursor.hasNext()) {
                    FDBQueriedRecord<Message> rec = cursor.next();
                    TestRecordsRankProto.BasicRankedRecord.Builder myrec = TestRecordsRankProto.BasicRankedRecord.newBuilder();
                    myrec.mergeFrom(rec.getRecord());
                    assertEquals(200, myrec.getScore());
                    i++;
                }
            }
            assertEquals(2, i); // 2 records tied for this rank.
        }
    }

    @Test
    public void checkUpdateWithTies() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            FDBStoredRecord<Message> rec = recordStore.loadRecord(Tuple.from("laodice"));
            RecordFunction<Long> rank = Query.rank("score").getFunction();

            assertEquals((Long)3L, recordStore.evaluateRecordFunction(rank, rec).get());

            assertTrue(recordStore.deleteRecord(Tuple.from("helen")));
            assertEquals((Long)3L, recordStore.evaluateRecordFunction(rank, rec).get());

            assertTrue(recordStore.deleteRecord(Tuple.from("penelope")));
            assertEquals((Long)2L, recordStore.evaluateRecordFunction(rank, rec).get());
        }
    }


    @Test
    public void complexRankQuery() throws Exception {
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("BasicRankedRecord")
                .setFilter(Query.and(
                        Query.field("gender").equalsValue("M"),
                        Query.rank(Key.Expressions.field("score").groupBy(Key.Expressions.field("gender"))).greaterThan(0L),
                        Query.rank(Key.Expressions.field("score").groupBy(Key.Expressions.field("gender"))).lessThan(2L)))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertEquals("Index(rank_by_gender ([M, 0],[M, 2]) BY_RANK)", plan.toString());

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan)) {
                FDBQueriedRecord<Message> rec = cursor.getNext().get();
                TestRecordsRankProto.BasicRankedRecord.Builder myrec = TestRecordsRankProto.BasicRankedRecord.newBuilder();
                myrec.mergeFrom(rec.getRecord());
                assertEquals("achilles", myrec.getName());
                assertEquals(100, myrec.getScore());
                assertFalse(cursor.getNext().hasNext());
            }
        }
    }

    @Test
    public void leftHalfIntervalRankQuery() throws Exception {
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("BasicRankedRecord")
                .setFilter(Query.rank("score").greaterThan(2L))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertEquals("Index(BasicRankedRecord$score ([2],> BY_RANK)", plan.toString());

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan)) {
                FDBQueriedRecord<Message> rec = cursor.getNext().get();
                TestRecordsRankProto.BasicRankedRecord.Builder myrec = TestRecordsRankProto.BasicRankedRecord.newBuilder();
                myrec.mergeFrom(rec.getRecord());
                assertEquals("laodice", myrec.getName());
                assertEquals(300, myrec.getScore());
                assertFalse(cursor.getNext().hasNext());
            }
        }
    }

    @Test
    public void rightHalfIntervalRankQuery() throws Exception {
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("BasicRankedRecord")
                .setFilter(Query.rank("score").lessThan(2L))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertEquals("Index(BasicRankedRecord$score ([null],[2]) BY_RANK)", plan.toString());

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan)) {
                RecordCursorResult<FDBQueriedRecord<Message>> result = cursor.getNext();
                FDBQueriedRecord<Message> rec = result.get();
                TestRecordsRankProto.BasicRankedRecord.Builder myrec = TestRecordsRankProto.BasicRankedRecord.newBuilder();
                myrec.mergeFrom(rec.getRecord());
                assertEquals("hector", myrec.getName());
                assertEquals(75, myrec.getScore());
                result = cursor.getNext();
                assertTrue(result.hasNext());
                rec = result.get();
                myrec = TestRecordsRankProto.BasicRankedRecord.newBuilder();
                myrec.mergeFrom(rec.getRecord());
                assertEquals("achilles", myrec.getName());
                assertEquals(100, myrec.getScore());
                result = cursor.getNext();
                assertFalse(result.hasNext());
            }
        }
    }

    @Test
    public void halfIntervalGroupedRankQuery() throws Exception {
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("BasicRankedRecord")
                .setFilter(Query.and(
                        Query.field("gender").equalsValue("M"),
                        Query.rank(Key.Expressions.field("score").groupBy(Key.Expressions.field("gender"))).lessThan(1L)))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertEquals("Index(rank_by_gender ([M, null],[M, 1]) BY_RANK)", plan.toString());

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan)) {
                FDBQueriedRecord<Message> rec = cursor.getNext().get();
                TestRecordsRankProto.BasicRankedRecord.Builder myrec = TestRecordsRankProto.BasicRankedRecord.newBuilder();
                myrec.mergeFrom(rec.getRecord());
                assertEquals("hector", myrec.getName());
                assertEquals(75, myrec.getScore());
                assertFalse(cursor.getNext().hasNext());
            }
        }
    }

    @Test
    public void outOfBoundsRankQueries() throws Exception {
        List<RecordQuery> queries = Arrays.asList(
                RecordQuery.newBuilder().setRecordType("BasicRankedRecord").setFilter(Query.rank("score").lessThan(-1L)).build(),
                RecordQuery.newBuilder().setRecordType("BasicRankedRecord").setFilter(Query.rank("score").lessThanOrEquals(-1L)).build(),
                RecordQuery.newBuilder().setRecordType("BasicRankedRecord").setFilter(Query.rank("score").greaterThan(-1L)).build(),
                RecordQuery.newBuilder().setRecordType("BasicRankedRecord").setFilter(Query.rank("score").greaterThanOrEquals(-1L)).build(),
                RecordQuery.newBuilder().setRecordType("BasicRankedRecord").setFilter(Query.rank("score").equalsValue(-1L)).build(),
                RecordQuery.newBuilder().setRecordType("BasicRankedRecord").setFilter(Query.rank("score").lessThan(0L)).build(),
                RecordQuery.newBuilder().setRecordType("BasicRankedRecord").setFilter(Query.rank("score").lessThanOrEquals(0L)).build(),
                RecordQuery.newBuilder().setRecordType("BasicRankedRecord").setFilter(Query.rank("score").greaterThan(0L)).build(),
                RecordQuery.newBuilder().setRecordType("BasicRankedRecord").setFilter(Query.rank("score").greaterThanOrEquals(0L)).build(),
                RecordQuery.newBuilder().setRecordType("BasicRankedRecord").setFilter(Query.rank("score").equalsValue(0L)).build(),
                RecordQuery.newBuilder().setRecordType("BasicRankedRecord").setFilter(Query.rank("score").lessThan(3L)).build(),
                RecordQuery.newBuilder().setRecordType("BasicRankedRecord").setFilter(Query.rank("score").lessThanOrEquals(3L)).build(),
                RecordQuery.newBuilder().setRecordType("BasicRankedRecord").setFilter(Query.rank("score").greaterThan(3L)).build(),
                RecordQuery.newBuilder().setRecordType("BasicRankedRecord").setFilter(Query.rank("score").greaterThanOrEquals(3L)).build(),
                RecordQuery.newBuilder().setRecordType("BasicRankedRecord").setFilter(Query.rank("score").equalsValue(3L)).build(),
                RecordQuery.newBuilder().setRecordType("BasicRankedRecord").setFilter(Query.rank("score").lessThan(4L)).build(),
                RecordQuery.newBuilder().setRecordType("BasicRankedRecord").setFilter(Query.rank("score").lessThanOrEquals(4L)).build(),
                RecordQuery.newBuilder().setRecordType("BasicRankedRecord").setFilter(Query.rank("score").greaterThan(4L)).build(),
                RecordQuery.newBuilder().setRecordType("BasicRankedRecord").setFilter(Query.rank("score").greaterThanOrEquals(4L)).build(),
                RecordQuery.newBuilder().setRecordType("BasicRankedRecord").setFilter(Query.rank("score").equalsValue(4L)).build(),
                RecordQuery.newBuilder().setRecordType("BasicRankedRecord").setFilter(Query.and(Query.rank("score").greaterThan(-3L), Query.rank("score").lessThan(-1L))).build(),
                RecordQuery.newBuilder().setRecordType("BasicRankedRecord").setFilter(Query.and(Query.rank("score").greaterThan(-3L), Query.rank("score").lessThan(2L))).build(),
                RecordQuery.newBuilder().setRecordType("BasicRankedRecord").setFilter(Query.and(Query.rank("score").greaterThan(-3L), Query.rank("score").lessThan(6L))).build(),
                RecordQuery.newBuilder().setRecordType("BasicRankedRecord").setFilter(Query.and(Query.rank("score").greaterThan(1L), Query.rank("score").lessThan(6L))).build(),
                RecordQuery.newBuilder().setRecordType("BasicRankedRecord").setFilter(Query.and(Query.rank("score").greaterThan(4L), Query.rank("score").lessThan(6L))).build()
        );
        List<String> planStrings = Arrays.asList(
                "Index(BasicRankedRecord$score ([null],[-1]) BY_RANK)",
                "Index(BasicRankedRecord$score ([null],[-1]] BY_RANK)",
                "Index(BasicRankedRecord$score ([-1],> BY_RANK)",
                "Index(BasicRankedRecord$score [[-1],> BY_RANK)",
                "Index(BasicRankedRecord$score [[-1],[-1]] BY_RANK)",
                "Index(BasicRankedRecord$score ([null],[0]) BY_RANK)",
                "Index(BasicRankedRecord$score ([null],[0]] BY_RANK)",
                "Index(BasicRankedRecord$score ([0],> BY_RANK)",
                "Index(BasicRankedRecord$score [[0],> BY_RANK)",
                "Index(BasicRankedRecord$score [[0],[0]] BY_RANK)",
                "Index(BasicRankedRecord$score ([null],[3]) BY_RANK)",
                "Index(BasicRankedRecord$score ([null],[3]] BY_RANK)",
                "Index(BasicRankedRecord$score ([3],> BY_RANK)",
                "Index(BasicRankedRecord$score [[3],> BY_RANK)",
                "Index(BasicRankedRecord$score [[3],[3]] BY_RANK)",
                "Index(BasicRankedRecord$score ([null],[4]) BY_RANK)",
                "Index(BasicRankedRecord$score ([null],[4]] BY_RANK)",
                "Index(BasicRankedRecord$score ([4],> BY_RANK)",
                "Index(BasicRankedRecord$score [[4],> BY_RANK)",
                "Index(BasicRankedRecord$score [[4],[4]] BY_RANK)",
                "Index(BasicRankedRecord$score ([-3],[-1]) BY_RANK)",
                "Index(BasicRankedRecord$score ([-3],[2]) BY_RANK)",
                "Index(BasicRankedRecord$score ([-3],[6]) BY_RANK)",
                "Index(BasicRankedRecord$score ([1],[6]) BY_RANK)",
                "Index(BasicRankedRecord$score ([4],[6]) BY_RANK)"
        );
        List<List<String>> resultLists = Arrays.asList(
                Collections.emptyList(),
                Collections.emptyList(),
                Arrays.asList("hector", "achilles", "helen", "penelope", "laodice"),
                Arrays.asList("hector", "achilles", "helen", "penelope", "laodice"),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.singletonList("hector"),
                Arrays.asList("achilles", "helen", "penelope", "laodice"),
                Arrays.asList("hector", "achilles", "helen", "penelope", "laodice"),
                Collections.singletonList("hector"),
                Arrays.asList("hector", "achilles", "helen", "penelope"),
                Arrays.asList("hector", "achilles", "helen", "penelope", "laodice"),
                Collections.emptyList(),
                Collections.singletonList("laodice"),
                Collections.singletonList("laodice"),
                Arrays.asList("hector", "achilles", "helen", "penelope", "laodice"),
                Arrays.asList("hector", "achilles", "helen", "penelope", "laodice"),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                Arrays.asList("hector", "achilles"),
                Arrays.asList("hector", "achilles", "helen", "penelope", "laodice"),
                Arrays.asList("helen", "penelope", "laodice"),
                Collections.emptyList()
        );

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);

            for (int i = 0; i < queries.size(); i++) {
                RecordQueryPlan plan = planner.plan(queries.get(i));
                assertEquals(planStrings.get(i), plan.toString(), "Iteration " + i);
                List<String> names = recordStore.executeQuery(plan)
                        .map(record -> TestRecordsRankProto.BasicRankedRecord.newBuilder().mergeFrom(record.getRecord()).getName())
                        .asList().join();
                assertEquals(names,  resultLists.get(i), "Iteration " + i);
            }
        }
    }

    @Test
    public void queryWithRanks() throws Exception {
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("BasicRankedRecord")
                .setFilter(Query.field("gender").equalsValue("M"))
                .setSort(Key.Expressions.field("score"))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        QueryRecordFunction<Long> ranker = Query.rank(Key.Expressions.field("score").groupBy(Key.Expressions.field("gender")));

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            try (RecordCursorIterator<? extends Pair<Message,Long>> cursor =
                         recordStore.executeQuery(plan)
                                 .mapPipelined(record -> ranker.eval(recordStore, EvaluationContext.EMPTY, record.getStoredRecord())
                                         .thenApply(rank -> new ImmutablePair<>(record.getRecord(), rank)), recordStore.getPipelineSize(PipelineOperation.RECORD_FUNCTION))
                                 .asIterator()) {
                long rank = 0;
                while (cursor.hasNext()) {
                    Pair<Message,Long> recWithRank = cursor.next();
                    TestRecordsRankProto.BasicRankedRecord.Builder myrec = TestRecordsRankProto.BasicRankedRecord.newBuilder();
                    myrec.mergeFrom(recWithRank.getLeft());
                    assertEquals((Long)rank++, recWithRank.getRight());
                }
            }
        }
    }

    @Test
    public void containsNullScore() throws Exception {
        fdb = FDBDatabaseFactory.instance().getDatabase();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            recordStore.deleteAllRecords(); // Undo loadRecords().

            TestRecordsRankProto.BasicRankedRecord.Builder rec = TestRecordsRankProto.BasicRankedRecord.newBuilder();
            rec.setName("achilles");
            rec.setGender("m");
            recordStore.saveRecord(rec.build());

            rec.clear();
            rec.setName("helen");
            rec.setScore(1);
            rec.setGender("f");
            recordStore.saveRecord(rec.build());

            commit(context);
        }

        List<RecordQuery> queries = Arrays.asList(
                RecordQuery.newBuilder().setRecordType("BasicRankedRecord").setFilter(Query.rank("score").equalsValue(0L)).build(),
                RecordQuery.newBuilder().setRecordType("BasicRankedRecord").setFilter(Query.rank("score").equalsValue(1L)).build(),
                RecordQuery.newBuilder().setRecordType("BasicRankedRecord").setFilter(Query.rank("score").lessThanOrEquals(1L)).build(),
                RecordQuery.newBuilder().setRecordType("BasicRankedRecord").setFilter(Query.rank("score").lessThan(1L)).build(),
                RecordQuery.newBuilder().setRecordType("BasicRankedRecord").setFilter(Query.rank("score").greaterThanOrEquals(0L)).build(),
                RecordQuery.newBuilder().setRecordType("BasicRankedRecord").setFilter(Query.rank("score").greaterThan(0L)).build()
        );
        List<String> planStrings = Arrays.asList(
                "Index(BasicRankedRecord$score [[0],[0]] BY_RANK)",
                "Index(BasicRankedRecord$score [[1],[1]] BY_RANK)",
                "Index(BasicRankedRecord$score ([null],[1]] BY_RANK)",
                "Index(BasicRankedRecord$score ([null],[1]) BY_RANK)",
                "Index(BasicRankedRecord$score [[0],> BY_RANK)",
                "Index(BasicRankedRecord$score ([0],> BY_RANK)"
        );
        List<RecordQueryPlan> plans = queries.stream().map(planner::plan).collect(Collectors.toList());
        for (int i = 0; i < plans.size(); i++) {
            assertEquals(planStrings.get(i), plans.get(i).toString());
        }
        List<List<String>> resultLists = Arrays.asList(
                Collections.singletonList("achilles"),
                Collections.singletonList("helen"),
                Arrays.asList("achilles", "helen"),
                Collections.singletonList("achilles"),
                Arrays.asList("achilles", "helen"),
                Collections.singletonList("helen")
        );
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);

            for (int i = 0; i < plans.size(); i++) {
                List<String> names = recordStore.executeQuery(plans.get(i))
                        .map(record -> TestRecordsRankProto.BasicRankedRecord.newBuilder().mergeFrom(record.getRecord()).getName())
                        .asList().join();
                assertEquals(resultLists.get(i), names);
            }
        }
    }

    @Test
    public void writeOnlyRankQuery() throws Exception {
        assertThrows(RecordCoreException.class, () -> {
            fdb = FDBDatabaseFactory.instance().getDatabase();
            try (FDBRecordContext context = openContext()) {
                openRecordStore(context);
                recordStore.markIndexWriteOnly("rank_by_gender").join();

                // Re-open to reload state.
                openRecordStore(context);
                RecordQuery query = RecordQuery.newBuilder()
                        .setRecordType("BasicRankedRecord")
                        .setFilter(Query.rank(Key.Expressions.field("score").groupBy(Key.Expressions.field("gender"))).equalsValue(1L))
                        .build();
                RecordQueryPlan plan = recordStore.planQuery(query);
                assertEquals("Scan(<,>) | [BasicRankedRecord] | rank([Field { 'gender' None}, Field { 'score' None}] group 1) EQUALS 1", plan.toString());

                List<TestRecordsRankProto.BasicRankedRecord> records = recordStore.executeQuery(plan)
                        .map(rec -> TestRecordsRankProto.BasicRankedRecord.newBuilder().mergeFrom(rec.getRecord()).build()).asList().get();
            } catch (ExecutionException e) {
                throw (Exception) e.getCause();
            }
        });
    }

    @Test
    public void writeOnlyLookup() throws Exception {
        assertThrows(RecordCoreException.class, () -> {
            fdb = FDBDatabaseFactory.instance().getDatabase();
            try (FDBRecordContext context = openContext()) {
                openRecordStore(context);
                recordStore.markIndexWriteOnly("rank_by_gender").join();

                // Re-open to reload state.
                openRecordStore(context);
                TestRecordsRankProto.BasicRankedRecord record = TestRecordsRankProto.BasicRankedRecord.newBuilder()
                        .setName("achilles")
                        .setGender("M")
                        .setScore(10)
                        .build();
                FDBStoredRecord<Message> storedRecord = recordStore.saveRecord(record);
                IndexRecordFunction<Long> function = (IndexRecordFunction<Long>) Query.rank(Key.Expressions.field("score").groupBy(Key.Expressions.field("gender"))).getFunction();
                recordStore.evaluateRecordFunction(function, storedRecord);
            }
        });
    }

    @Test
    public void nestedRankQuery() throws Exception {
        fdb = FDBDatabaseFactory.instance().getDatabase();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            recordStore.deleteAllRecords(); // Undo loadRecords().
            
            TestRecordsRankProto.NestedRankedRecord.Builder rec;
            TestRecordsRankProto.NestedRankedRecord.GameScore.Builder score;

            rec = TestRecordsRankProto.NestedRankedRecord.newBuilder();
            rec.setName("p1")
                    .setCountry("US");

            score = rec.addScoresBuilder();
            score.setGame("tennis")
                    .setTier("pro")
                    .setScore(10);

            score = rec.addScoresBuilder();
            score.setGame("pingpong")
                    .setTier("pro")
                    .setScore(100);

            recordStore.saveRecord(rec.build());

            rec = TestRecordsRankProto.NestedRankedRecord.newBuilder();
            rec.setName("p2")
                    .setCountry("US");

            score = rec.addScoresBuilder();
            score.setGame("tennis")
                    .setTier("pro")
                    .setScore(15);

            score = rec.addScoresBuilder();
            score.setGame("pingpong")
                    .setTier("pro")
                    .setScore(50);

            recordStore.saveRecord(rec.build());

            rec = TestRecordsRankProto.NestedRankedRecord.newBuilder();
            rec.setName("p3")
                    .setCountry("US");

            score = rec.addScoresBuilder();
            score.setGame("tennis")
                    .setTier("amateur")
                    .setScore(30);

            score = rec.addScoresBuilder();
            score.setGame("pingpong")
                    .setTier("amateur")
                    .setScore(200);

            recordStore.saveRecord(rec.build());

            rec = TestRecordsRankProto.NestedRankedRecord.newBuilder();
            rec.setName("p4")
                    .setCountry("UK");

            score = rec.addScoresBuilder();
            score.setGame("tennis")
                    .setTier("pro")
                    .setScore(50);

            score = rec.addScoresBuilder();
            score.setGame("pingpong")
                    .setTier("pro")
                    .setScore(90);

            recordStore.saveRecord(rec.build());

            commit(context);
        }

        // TODO: The nested query and ranking stuff isn't yet powerful enough to write the necessary query.

    }

    @Test
    public void repeatedRankQuery() throws Exception {
        fdb = FDBDatabaseFactory.instance().getDatabase();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            recordStore.deleteAllRecords(); // Undo loadRecords().

            TestRecordsRankProto.RepeatedRankedRecord.Builder rec = TestRecordsRankProto.RepeatedRankedRecord.newBuilder();

            rec.setName("patroclus")
                    .addScore(-5)
                    .addScore(5)
                    .addScore(-10)
                    .addScore(-11)
                    .addScore(-8);
            recordStore.saveRecord(rec.build());

            rec.clear();
            rec.setName("achilles")
                    .addScore(-14)
                    .addScore(5)
                    .addScore(9)
                    .addScore(-8)
                    .addScore(-1)
                    .addScore(-16);
            recordStore.saveRecord(rec.build());

            rec.clear();
            rec.setName("hector")
                    .addScore(-5)
                    .addScore(5)
                    .addScore(-3)
                    .addScore(-2)
                    .addScore(0)
                    .addScore(10);
            recordStore.saveRecord(rec.build());

            commit(context);
        }

        // Reordered by score:
        // [ -16 (Achilles), -14 (Achilles), -11 (Patroclus), -10 (Patroclus), -8 (Achilles, Patroclus), -5 (Hector, Patroclus),
        // -3 (Hector), -2 (Hector), -1 (Achilles), 0 (Hector), 5 (Achilles, Hector, Patroclus), 9 (Achilles), 10 (Hector)]

        GroupingKeyExpression expr = Key.Expressions.field("score", KeyExpression.FanType.FanOut).ungrouped();
        RecordQuery.Builder builder = RecordQuery.newBuilder()
                .setRecordType("RepeatedRankedRecord")
                .setFilter(Query.and(
                        Query.rank(expr).greaterThanOrEquals(4L),
                        Query.rank(expr).lessThanOrEquals(10L)));

        RecordQuery query = builder.setRemoveDuplicates(false).build();
        RecordQueryPlan plan = planner.plan(query);
        assertEquals("Index(score_by_repeated_field [[4],[10]] BY_RANK)", plan.toString());
        List<String> res;

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            res = recordStore.executeQuery(plan)
                    .map(rec -> TestRecordsRankProto.RepeatedRankedRecord.newBuilder().mergeFrom(rec.getRecord()).getName())
                    .asList().join();
        }
        assertEquals(Arrays.asList("achilles", "patroclus", "hector", "patroclus", "hector", "hector", "achilles", "hector", "achilles", "hector", "patroclus"), res);

        query = builder.setRemoveDuplicates(true).build();
        plan = planner.plan(query);
        assertEquals("Index(score_by_repeated_field [[4],[10]] BY_RANK) | UnorderedPrimaryKeyDistinct()", plan.toString());

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            res = recordStore.executeQuery(plan)
                    .map(rec -> TestRecordsRankProto.RepeatedRankedRecord.newBuilder().mergeFrom(rec.getRecord()).getName())
                    .asList().join();
        }
        assertEquals(Arrays.asList("achilles", "patroclus", "hector"), res);
    }

    @Test
    public void headerRankQuery() throws Exception {
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("HeaderRankedRecord")
                .setFilter(Query.and(
                        Query.field("header").matches(Query.field("group").equalsValue("buffaloes")),
                        Query.rank(Key.Expressions.field("score").groupBy(Key.Expressions.field("header").nest(Key.Expressions.field("group")))).greaterThan(1L)))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertEquals("Index(score_by_nested_id ([buffaloes, 1],[buffaloes]] BY_RANK)", plan.toString());
    }

    @Test
    public void rankWithoutGroupRestriction() throws Exception {
        // Grouped rank in filter but query results include all groups.
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("BasicRankedRecord")
                .setFilter(Query.rank(Key.Expressions.field("score").groupBy(Key.Expressions.field("gender"))).equalsValue(1L))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertFalse(plan.hasIndexScan("rank_by_gender"));
        assertTrue(plan.hasRecordScan());

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            List<String> names = recordStore.executeQuery(plan)
                    .map(rec -> TestRecordsRankProto.BasicRankedRecord.newBuilder().mergeFrom(rec.getRecord()).getName())
                    .asList().join();
            assertEquals(Arrays.asList("achilles", "laodice"), names);
            commit(context);
        }
    }

    @Test
    public void rankPlusOtherQuery() throws Exception {
        // Filter by the overall rank, not the rank by gender.
        // Further filter by the gender, for which by gender rank index in BY_VALUE mode is suitable.
        // Should then use one rank index for score_for_rank and the other for the index scan.
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("BasicRankedRecord")
                .setFilter(Query.and(
                        Query.field("gender").equalsValue("M"),
                        Query.rank(Key.Expressions.field("score").ungrouped()).greaterThanOrEquals(1L),
                        Query.rank(Key.Expressions.field("score").ungrouped()).lessThan(3L)))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertEquals("Index(rank_by_gender [EQUALS M, [GREATER_THAN_OR_EQUALS $__rank_0 && LESS_THAN $__rank_1]])" +
                " WHERE __rank_0 = BasicRankedRecord$score.score_for_rank(1)," +
                " __rank_1 = BasicRankedRecord$score.score_for_rank_else_skip(3)",
                plan.toString());

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            List<String> names = recordStore.executeQuery(plan)
                    .map(rec -> TestRecordsRankProto.BasicRankedRecord.newBuilder().mergeFrom(rec.getRecord()).getName())
                    .asList().join();
            assertEquals(Arrays.asList("achilles"), names);
            commit(context);
        }
    }

    @Test
    public void twoRankPredicates() throws Exception {
        // Different rank predicates: at most one can be used in the scan.
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("BasicRankedRecord")
                .setFilter(Query.and(
                        Query.field("gender").equalsValue("M"),
                        Query.rank(Key.Expressions.field("score").ungrouped()).lessThan(3L),
                        Query.rank(Key.Expressions.field("score").groupBy(Key.Expressions.field("gender"))).equalsValue(1L)))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertEquals("Index(rank_by_gender [[M, 1],[M, 1]] BY_RANK)" +
                        " | score LESS_THAN $__rank_0" +
                        " WHERE __rank_0 = BasicRankedRecord$score.score_for_rank_else_skip(3)",
                plan.toString());

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            List<String> names = recordStore.executeQuery(plan)
                    .map(rec -> TestRecordsRankProto.BasicRankedRecord.newBuilder().mergeFrom(rec.getRecord()).getName())
                    .asList().join();
            assertEquals(Arrays.asList("achilles"), names);
            commit(context);
        }
    }

    @Test
    public void rankPlusRankIn1() throws Exception {
        // Different rank predicates: at most one can be used in the scan.
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("BasicRankedRecord")
                .setFilter(Query.and(
                        Query.field("gender").equalsValue("M"),
                        Query.rank(Key.Expressions.field("score").ungrouped()).in(Arrays.asList(0L, 2L)),
                        Query.rank(Key.Expressions.field("score").groupBy(Key.Expressions.field("gender"))).lessThanOrEquals(1L)))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertEquals("Index(rank_by_gender ([M, null],[M, 1]] BY_RANK)" +
                        " | score EQUALS $__rank_0" +
                        " WHERE __rank_0 = BasicRankedRecord$score.score_for_rank($__in_rank(Field { 'score' None} group 1)__0)" +
                        " WHERE __in_rank(Field { 'score' None} group 1)__0 IN [0, 2]",
                plan.toString());

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            List<String> names = recordStore.executeQuery(plan)
                    .map(rec -> TestRecordsRankProto.BasicRankedRecord.newBuilder().mergeFrom(rec.getRecord()).getName())
                    .asList().join();
            assertEquals(Arrays.asList("hector"), names);
            commit(context);
        }
    }

    @Test
    public void rankPlusRankIn2() throws Exception {
        // Different rank predicates: at most one can be used in the scan.
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("BasicRankedRecord")
                .setFilter(Query.and(
                        Query.field("gender").equalsValue("M"),
                        Query.rank(Key.Expressions.field("score").ungrouped()).lessThan(3L),
                        Query.rank(Key.Expressions.field("score").groupBy(Key.Expressions.field("gender"))).in("mranks")))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertEquals("Index(rank_by_gender [EQUALS M, EQUALS $__in_rank([Field { 'gender' None}, Field { 'score' None}] group 1)__0] BY_RANK)" +
                        " | score LESS_THAN $__rank_0" +
                        " WHERE __rank_0 = BasicRankedRecord$score.score_for_rank_else_skip(3)" +
                        " WHERE __in_rank([Field { 'gender' None}, Field { 'score' None}] group 1)__0 IN $mranks",
                plan.toString());

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            EvaluationContext bound = EvaluationContext.forBinding("mranks", Arrays.asList(0L, 1L, 3L));
            List<String> names = plan.execute(recordStore, bound)
                    .map(rec -> TestRecordsRankProto.BasicRankedRecord.newBuilder().mergeFrom(rec.getRecord()).getName())
                    .asList().join();
            assertEquals(Arrays.asList("hector", "achilles"), names);
            commit(context);
        }
    }

    @Test
    public void headerRankAndIdQuery() throws Exception {
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("HeaderRankedRecord")
                .setFilter(Query.and(
                        Query.field("header").matches(Query.field("group").equalsValue("buffaloes")),
                        Query.field("header").matches(Query.field("id").greaterThan(100)),
                        Query.rank(Key.Expressions.field("score").groupBy(Key.Expressions.field("header").nest(Key.Expressions.field("group"))))
                                .lessThan(2L)))
                .build();
        planner.setIndexScanPreference(QueryPlanner.IndexScanPreference.PREFER_SCAN);
        RecordQueryPlan plan = planner.plan(query);
        assertEquals("Scan(([buffaloes, 100],[buffaloes]]) | [HeaderRankedRecord]" +
                        " | score LESS_THAN $__rank_0 WHERE __rank_0 = score_by_nested_id.score_for_rank_else_skip(buffaloes, 2)",
                plan.toString());

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            TestRecordsRankProto.HeaderRankedRecord.Builder recordBuilder = TestRecordsRankProto.HeaderRankedRecord.newBuilder();

            // id too low
            recordBuilder.getHeaderBuilder().setGroup("buffaloes").setId(99);
            recordBuilder.setScore(1);
            recordStore.saveRecord(recordBuilder.build());

            // rank too high
            recordBuilder.getHeaderBuilder().setGroup("buffaloes").setId(101);
            recordBuilder.setScore(100);
            recordStore.saveRecord(recordBuilder.build());

            // match
            recordBuilder.getHeaderBuilder().setGroup("buffaloes").setId(102);
            recordBuilder.setScore(10);
            recordStore.saveRecord(recordBuilder.build());

            // wrong group
            recordBuilder.getHeaderBuilder().setGroup("bison").setId(110);
            recordBuilder.setScore(2);
            recordStore.saveRecord(recordBuilder.build());

            List<Integer> ids = recordStore.executeQuery(plan)
                    .map(rec -> TestRecordsRankProto.HeaderRankedRecord.newBuilder().mergeFrom(rec.getRecord()).getHeader().getId())
                    .asList().join();
            assertEquals(Arrays.asList(102), ids);
            commit(context);
        }
    }

    @Test
    public void rankPlusRankHighOutOfBounds() throws Exception {
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("BasicRankedRecord")
                .setFilter(Query.and(
                        Query.field("gender").equalsValue("F"),
                        Query.rank(Key.Expressions.field("score").ungrouped()).lessThan(100L),
                        Query.rank(Key.Expressions.field("score").groupBy(Key.Expressions.field("gender"))).greaterThan(0L)))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertEquals("Index(rank_by_gender ([F, 0],[F]] BY_RANK)" +
                        " | score LESS_THAN $__rank_0" +
                        " WHERE __rank_0 = BasicRankedRecord$score.score_for_rank_else_skip(100)",
                plan.toString());

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            List<String> names = recordStore.executeQuery(plan)
                    .map(rec -> TestRecordsRankProto.BasicRankedRecord.newBuilder().mergeFrom(rec.getRecord()).getName())
                    .asList().join();
            assertEquals(Arrays.asList("laodice"), names);
            commit(context);
        }
    }

    @Test
    public void rankPlusRankLowOutOfBounds() throws Exception {
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("BasicRankedRecord")
                .setFilter(Query.and(
                        Query.field("gender").equalsValue("F"),
                        Query.rank(Key.Expressions.field("score").ungrouped()).greaterThan(100L),
                        Query.rank(Key.Expressions.field("score").groupBy(Key.Expressions.field("gender"))).greaterThan(0L)))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertEquals("Index(rank_by_gender ([F, 0],[F]] BY_RANK)" +
                        " | score GREATER_THAN $__rank_0" +
                        " WHERE __rank_0 = BasicRankedRecord$score.score_for_rank(100)",
                plan.toString());

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            List<String> names = recordStore.executeQuery(plan)
                    .map(rec -> TestRecordsRankProto.BasicRankedRecord.newBuilder().mergeFrom(rec.getRecord()).getName())
                    .asList().join();
            assertEquals(Collections.emptyList(), names);
            commit(context);
        }
    }

    @Test
    public void rankPlusMatchingNonRankIndex() throws Exception {
        RecordMetaDataHook hook = md -> md.addIndex("BasicRankedRecord", new Index("AaaSumIndex", Key.Expressions.field("score").ungrouped(), IndexTypes.SUM));
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            // Ordinarily the stable order is the persistent form; force new index to the front.
            recordStore.getRecordMetaData().getRecordType("BasicRankedRecord").getIndexes().sort(Comparator.comparing(Index::getName));
            recordStore.rebuildIndex(recordStore.getRecordMetaData().getIndex("AaaSumIndex")).join();
            commit(context);
        }
        // New index should not interfere with score_for_rank choice.
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("BasicRankedRecord")
                .setFilter(Query.and(
                        Query.field("gender").equalsValue("F"),
                        Query.rank(Key.Expressions.field("score").ungrouped()).equalsValue(2L)))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertEquals("Index(rank_by_gender [EQUALS F, EQUALS $__rank_0])" +
                        " WHERE __rank_0 = BasicRankedRecord$score.score_for_rank(2)",
                plan.toString());

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            List<String> names = recordStore.executeQuery(plan)
                    .map(rec -> TestRecordsRankProto.BasicRankedRecord.newBuilder().mergeFrom(rec.getRecord()).getName())
                    .asList().join();
            assertEquals(Arrays.asList("helen", "penelope"), names);
            assertEquals(875L, recordStore.evaluateAggregateFunction(Collections.singletonList("BasicRankedRecord"),
                    new IndexAggregateFunction(FunctionNames.SUM, Key.Expressions.field("score"), null), Key.Evaluated.EMPTY, IsolationLevel.SNAPSHOT)
                    .join().getLong(0));
            commit(context);
        }
    }

    @Test
    public void compoundWithNullOther() throws Exception {
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("BasicRankedRecord")
                .setFilter(Query.or(
                        Query.field("gender").notEquals("M"),
                        Query.rank("score").lessThanOrEquals(0L)))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertEquals("Scan(<,>) | [BasicRankedRecord] | Or([gender NOT_EQUALS M, rank(Field { 'score' None} group 1) LESS_THAN_OR_EQUALS 0])", plan.toString());

        TestRecordsRankProto.BasicRankedRecord recordWithNoGender = TestRecordsRankProto.BasicRankedRecord.newBuilder()
                .setName("no_assumptions").setScore(500).build();

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            recordStore.saveRecord(recordWithNoGender);
            List<String> names = recordStore.executeQuery(plan)
                    .map(rec -> TestRecordsRankProto.BasicRankedRecord.newBuilder().mergeFrom(rec.getRecord()).getName())
                    .asList().join();
            assertEquals(Arrays.asList("hector", "helen", "laodice", "penelope"), names);
            commit(context);
        }
    }

    @Test
    public void countNotPossibleWithoutUniqueIndex() throws Exception {
        assertThrows(RecordCoreException.class, () -> {
            try (FDBRecordContext context = openContext()) {
                openRecordStore(context);
                recordStore.evaluateAggregateFunction(Collections.singletonList("BasicRankedRecord"),
                        IndexFunctionHelper.count(Key.Expressions.field("gender")),
                        Key.Evaluated.scalar("M"),
                        IsolationLevel.SERIALIZABLE);
            }
        });
    }

    @Test
    public void uniquenessViolationWithTies() throws Exception {
        clearAndInitialize();   // Undo loadRecords.
        assertThrows(RecordIndexUniquenessViolation.class, () -> {
            try (FDBRecordContext context = openContext()) {
                openRecordStore(context, md -> {
                    md.addUniversalIndex(FDBRecordStoreTestBase.COUNT_INDEX);
                    md.removeIndex("rank_by_gender");
                    md.addIndex("BasicRankedRecord",
                            new Index("unique_rank_by_gender", Key.Expressions.field("score").groupBy(Key.Expressions.field("gender")), EmptyKeyExpression.EMPTY,
                                    IndexTypes.RANK, IndexOptions.UNIQUE_OPTIONS));
                });
                for (Object[] rec : RECORDS) {
                    recordStore.saveRecord(TestRecordsRankProto.BasicRankedRecord.newBuilder()
                            .setName((String) rec[0])
                            .setScore((Integer) rec[1])
                            .setGender((String) rec[2])
                            .build());
                }
                commit(context);
            }
        });
    }

    @Test
    public void countIfUnique() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            recordStore.deleteAllRecords(); // Undo loadRecords().
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, md -> {
                md.addUniversalIndex(FDBRecordStoreTestBase.COUNT_INDEX);
                md.removeIndex("rank_by_gender");
                md.addIndex("BasicRankedRecord",
                        new Index("unique_rank_by_gender", Key.Expressions.field("score").groupBy(Key.Expressions.field("gender")), EmptyKeyExpression.EMPTY,
                                IndexTypes.RANK, IndexOptions.UNIQUE_OPTIONS));
            });
            for (Object[] rec : RECORDS) {
                if ("F".equals(rec[2])) {
                    continue;
                }
                recordStore.saveRecord(TestRecordsRankProto.BasicRankedRecord.newBuilder()
                        .setName((String) rec[0])
                        .setScore((Integer) rec[1])
                        .setGender((String) rec[2])
                        .build());
            }
            assertEquals(2L,
                    recordStore.evaluateAggregateFunction(Collections.singletonList("BasicRankedRecord"),
                            IndexFunctionHelper.count(Key.Expressions.field("gender")),
                            Key.Evaluated.scalar("M"),
                            IsolationLevel.SERIALIZABLE)
                            .join().getLong(0));
            commit(context);
        }
    }

    @Test
    public void countDistinct() throws Exception {
        final IndexAggregateFunction countByGender = new IndexAggregateFunction(FunctionNames.COUNT_DISTINCT,
                Key.Expressions.field("score").groupBy(Key.Expressions.field("gender")),
                null);
        final List<String> types = Collections.singletonList("BasicRankedRecord");
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            assertEquals(2, recordStore.evaluateAggregateFunction(types, countByGender, Key.Evaluated.scalar("M"), IsolationLevel.SERIALIZABLE).join().getLong(0));
            assertEquals(2, recordStore.evaluateAggregateFunction(types, countByGender, Key.Evaluated.scalar("F"), IsolationLevel.SERIALIZABLE).join().getLong(0));
            commit(context);
        }
    }

    @Test
    public void covering() throws Exception {
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("BasicRankedRecord")
                .setFilter(Query.rank("score").greaterThanOrEquals(2L))
                .setRequiredResults(Arrays.asList(Key.Expressions.field("name"), Key.Expressions.field("score")))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, coveringIndexScan(
                indexScan(allOf(indexName("BasicRankedRecord$score"), indexScanType(IndexScanType.BY_RANK), bounds(hasTupleString("[[2],>"))))));

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            List<String> names = recordStore.executeQuery(plan)
                    .map(rec -> TestRecordsRankProto.BasicRankedRecord.newBuilder().mergeFrom(rec.getRecord()).getName())
                    .asList().join();
            assertEquals(Arrays.asList("helen", "penelope", "laodice"), names);
            commit(context);
        }
    }

    @Test
    public void coveringScoreValues() throws Exception {
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("BasicRankedRecord")
                .setFilter(Query.field("gender").equalsValue("M"))
                .setRequiredResults(Arrays.asList(Key.Expressions.field("gender"), Key.Expressions.field("score")))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertThat(plan, coveringIndexScan(
                indexScan(allOf(indexName("rank_by_gender"), indexScanType(IndexScanType.BY_VALUE), bounds(hasTupleString("[[M],[M]]"))))));

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            List<Integer> scores = recordStore.executeQuery(plan)
                    .map(rec -> TestRecordsRankProto.BasicRankedRecord.newBuilder().mergeFrom(rec.getRecord()).getScore())
                    .asList().join();
            assertEquals(Arrays.asList(75, 100), scores);
            commit(context);
        }
    }

    @Test
    public void rankScanContinuation() throws Exception {
        RecordQuery query = RecordQuery.newBuilder()
                .setRecordType("BasicRankedRecord")
                .setFilter(Query.rank("score").lessThan(100))
                .build();
        RecordQueryPlan plan = planner.plan(query);
        assertTrue(plan.hasIndexScan("BasicRankedRecord$score"));

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            Multiset<String> names = HashMultiset.create();
            byte[] continuation = null;
            do {
                RecordCursor<FDBQueriedRecord<Message>> recs = recordStore.executeQuery(plan,
                        continuation, ExecuteProperties.newBuilder().setReturnedRowLimit(2).build());
                recs.forEach(rec -> names.add(TestRecordsRankProto.BasicRankedRecord.newBuilder().mergeFrom(rec.getRecord()).getName())).join();
                continuation = recs.getNext().getContinuation().toBytes();
            } while (continuation != null);
            assertEquals(ImmutableMultiset.of("achilles", "hector", "helen", "penelope", "laodice"), names);
            commit(context);
        }
    }

    @Test
    public void rankScanIntersection() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, md -> {
                md.removeIndex("rank_by_gender");
                md.addIndex("BasicRankedRecord", "gender");
            });
            recordStore.rebuildIndex(recordStore.getRecordMetaData().getIndex("BasicRankedRecord$gender")).join();
            // Laodice fails the rank test; need something that fails the gender test.
            recordStore.saveRecord(TestRecordsRankProto.BasicRankedRecord.newBuilder()
                    .setName("patroclus")
                    .setScore(200)
                    .setGender("M")
                    .build());
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("BasicRankedRecord")
                    .setFilter(Query.and(
                            Query.rank("score").equalsValue(2),
                            Query.field("gender").equalsValue("F")))
                    .build();
            RecordQueryPlan plan = planner.plan(query);
            assertEquals("Index(BasicRankedRecord$score [[2],[2]] BY_RANK) ∩ Index(BasicRankedRecord$gender [[F],[F]])", plan.toString());
            Set<String> names = new HashSet<>();
            Function<FDBQueriedRecord<Message>, String> name = rec -> TestRecordsRankProto.BasicRankedRecord.newBuilder().mergeFrom(rec.getRecord()).getName();
            RecordCursor<String> cursor = recordStore.executeQuery(plan, null, ExecuteProperties.newBuilder().setReturnedRowLimit(1).build()).map(name);
            RecordCursorResult<String> result = cursor.getNext();
            assertTrue(result.hasNext());
            names.add(result.get());
            cursor = recordStore.executeQuery(plan, result.getContinuation().toBytes(), ExecuteProperties.newBuilder().setReturnedRowLimit(1).build()).map(name);
            result = cursor.getNext();
            assertTrue(result.hasNext());
            names.add(result.get());
            cursor = recordStore.executeQuery(plan, result.getContinuation().toBytes(), ExecuteProperties.newBuilder().setReturnedRowLimit(1).build()).map(name);
            result = cursor.getNext();
            assertFalse(result.hasNext());
            assertEquals(Sets.newHashSet("penelope", "helen"), names);
            commit(context);
        }
    }

    @Test
    public void checkScoreForRank() throws Exception {
        IndexAggregateFunction function = new IndexAggregateFunction(FunctionNames.SCORE_FOR_RANK, Key.Expressions.field("score").groupBy(Key.Expressions.field("gender")), null);
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            assertEquals(100, recordStore.evaluateAggregateFunction(Collections.singletonList("BasicRankedRecord"),
                    function, Key.Evaluated.concatenate("M", 1L), IsolationLevel.SERIALIZABLE)
                    .join().getLong(0));
            commit(context);
        }
    }

    @Test
    public void checkRankForScore() throws Exception {
        IndexAggregateFunction function = new IndexAggregateFunction(FunctionNames.RANK_FOR_SCORE, Key.Expressions.field("score").groupBy(Key.Expressions.field("gender")), null);
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            assertEquals(1L, recordStore.evaluateAggregateFunction(Collections.singletonList("BasicRankedRecord"),
                    function, Key.Evaluated.concatenate("M", 100), IsolationLevel.SERIALIZABLE)
                    .join().getLong(0));
            // Nobody has this score: this is the rank they would have with it.
            assertEquals(2L, recordStore.evaluateAggregateFunction(Collections.singletonList("BasicRankedRecord"),
                    function, Key.Evaluated.concatenate("M", 101), IsolationLevel.SERIALIZABLE)
                    .join().getLong(0));
            commit(context);
        }
    }

    @Test
    @Tag(Tags.Slow)
    public void testForRankUpdateTimingError() throws Exception {
        fdb = FDBDatabaseFactory.instance().getDatabase();

        // The NPE happens every so often, so I'm doing it 20 times as that seems to be "enough".
        for (int i = 0; i < 20; i++) {
            int score = 100 * (i + 1) + 1000;
            TestRecordsRankProto.BasicRankedRecord record1 = TestRecordsRankProto.BasicRankedRecord.newBuilder()
                    .setName("patroclus").setScore(score).setGender("M").build();
            score += 50;
            TestRecordsRankProto.BasicRankedRecord record2 = TestRecordsRankProto.BasicRankedRecord.newBuilder()
                    .setName("patroclus").setScore(score).setGender("M").build();

            try (FDBRecordContext context = openContext()) {
                openRecordStore(context);
                recordStore.saveRecord(record1);
                FDBStoredRecord<Message> storedRecord2 = recordStore.saveRecord(record2);

                RecordFunction<Long> rankFunction = Query.rank("score").getFunction();
                Long rank = recordStore.evaluateRecordFunction(rankFunction, storedRecord2).get();
                assertNotNull(rank);

                RecordQuery query = RecordQuery.newBuilder()
                        .setRecordType("BasicRankedRecord")
                        .setFilter(Query.rank("score").equalsValue(rank))
                        .build();
                RecordQueryPlan plan = planner.plan(query);
                assertEquals("Index(BasicRankedRecord$score [[" + rank + "],[" + rank + "]] BY_RANK)", plan.toString());

                recordStore.executeQuery(plan)
                        .map(rec -> TestRecordsRankProto.BasicRankedRecord.newBuilder().mergeFrom(rec.getRecord()).getName())
                        .asList()
                        .thenAccept(list -> assertTrue(list.contains(record2.getName())))
                        .get();

                recordStore.deleteRecord(Tuple.from("patroclus"));
                commit(context);
            } catch (NullPointerException e) {
                e.printStackTrace();
                fail("Null pointer exception: " + e.getMessage());
            }
        }
    }

}
