/*
 * ScenarioRecords.java
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

package com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios;

import com.apple.foundationdb.half.Half;
import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.record.TestRecordsIndexScenariosProto;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Shared field/type name constants and record generation over the standard
 * {@code TestRecordsIndexScenariosProto} schema. A single generator populates <em>every</em> field
 * so that any index type finds usable, deterministic data, letting the per-index-type definitions
 * stop generating records themselves.
 */
public final class ScenarioRecords {
    // Record type names.
    public static final String SCENARIO_RECORD = "ScenarioRecord";
    public static final String OTHER_RECORD = "OtherScenarioRecord";

    // Field names on ScenarioRecord.
    public static final String REC_NO = "rec_no";
    public static final String GROUP = "group";
    public static final String NUM_VALUE = "num_value";
    public static final String BITMAP_POSITION = "bitmap_position";
    public static final String STR_VALUE = "str_value";
    public static final String TEXT_VALUE = "text_value";
    public static final String DIM_X = "dim_x";
    public static final String DIM_Y = "dim_y";
    public static final String PERMUTED_VALUE = "permuted_value";
    public static final String VECTOR_DATA = "vector_data";
    public static final String SCORES = "scores";
    public static final String OTHER_REC_NO = "other_rec_no";

    // Field names on ScoreEntry.
    public static final String SCORE = "score";
    public static final String TIMESTAMP = "timestamp";

    // Synthetic type + constituent names.
    public static final String JOINED_TYPE = "JoinedScenario";
    public static final String UNNESTED_TYPE = "UnnestedScenario";
    public static final String SIMPLE_CONSTITUENT = "simple";
    public static final String OTHER_CONSTITUENT = "other";
    public static final String PARENT_CONSTITUENT = "parent";
    public static final String ENTRY_CONSTITUENT = "entry";

    /** The number of distinct groups records are spread across in grouped scenarios. */
    public static final int NUM_GROUPS = 3;
    /** The vector dimensionality used for the VECTOR index. */
    public static final int VECTOR_DIMENSIONS = 128;

    private ScenarioRecords() {
    }

    /** An empty grouping prefix, for ungrouped scenarios. */
    public static KeyExpression noPrefix() {
        return EmptyKeyExpression.EMPTY;
    }

    /**
     * Generate {@code count} {@code ScenarioRecord}s with every field populated deterministically so
     * that any index type finds usable data. {@code num_value} is distinct and monotonically
     * increasing (so RANK / MIN / MAX orderings are well-defined); {@code group} cycles over
     * {@link #NUM_GROUPS} so grouped scenarios see more than one group.
     *
     * @param count the number of records
     * @return the generated records
     */
    public static List<Message> scenarioRecords(final int count) {
        return IntStream.range(0, count)
                .mapToObj(ScenarioRecords::scenarioRecord)
                .toList();
    }

    private static Message scenarioRecord(final int i) {
        final Half[] components = new Half[VECTOR_DIMENSIONS];
        for (int d = 0; d < VECTOR_DIMENSIONS; d++) {
            components[d] = Half.valueOf(0.0f);
        }
        // Distinct distance-to-origin per record, so distance-sorted vector scans are deterministic.
        components[0] = Half.valueOf((float)(i + 1));
        final HalfRealVector vector = new HalfRealVector(components);
        return TestRecordsIndexScenariosProto.ScenarioRecord.newBuilder()
                .setRecNo(i)
                .setGroup(i % NUM_GROUPS)
                .setNumValue(3 * i + 1)
                .setBitmapPosition(i)
                .setStrValue("group")
                .setTextValue("term" + i)
                .setDimX(100L * i)
                .setDimY(100L * i + 50L)
                .setPermutedValue(3 * i + 1)
                .setVectorData(ByteString.copyFrom(vector.getRawData()))
                .setOtherRecNo(1000L + i)
                .addScores(TestRecordsIndexScenariosProto.ScoreEntry.newBuilder()
                        .setScore(100L + i)
                        .setTimestamp(1000L + i)
                        .setContext(i))
                .build();
    }

    /**
     * Generate records that produce no entry in any {@code ScenarioRecord} index: they are of a
     * different record type ({@code OtherScenarioRecord}), so writing them touches no index under
     * test. Used to give a transaction writes without touching the index (see {@code SnapshotScan}).
     *
     * @param count the number of records
     * @return the generated records
     */
    public static List<Message> otherRecords(final int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> (Message)TestRecordsIndexScenariosProto.OtherScenarioRecord.newBuilder()
                        .setRecNo(1000L + i)
                        .setGroup(i % NUM_GROUPS)
                        .setNumValue(3 * i + 1)
                        .build())
                .toList();
    }

    /**
     * Generate the constituent records for a joined synthetic type: for each {@code i}, a
     * {@code ScenarioRecord} whose {@code other_rec_no} matches a distinct {@code OtherScenarioRecord}
     * so exactly one joined synthetic record results per {@code i}.
     *
     * @param count the number of joined pairs
     * @return the constituent records to save
     */
    public static List<Message> joinedConstituents(final int count) {
        final List<Message> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            final long otherRecNo = 1000L + i;
            records.add(TestRecordsIndexScenariosProto.ScenarioRecord.newBuilder()
                    .setRecNo(i)
                    .setGroup(i % NUM_GROUPS)
                    .setNumValue(3 * i + 1)
                    .setOtherRecNo(otherRecNo)
                    .build());
            records.add(TestRecordsIndexScenariosProto.OtherScenarioRecord.newBuilder()
                    .setRecNo(otherRecNo)
                    .setGroup(i % NUM_GROUPS)
                    .setNumValue(7 * i + 2)
                    .build());
        }
        return records;
    }

    /**
     * Generate parent records for an unnested synthetic type: each {@code ScenarioRecord} carries two
     * {@code ScoreEntry}s (with distinct scores), so unnesting yields two synthetic records per parent.
     *
     * @param count the number of parent records
     * @return the parent records to save
     */
    public static List<Message> unnestedParents(final int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> (Message)TestRecordsIndexScenariosProto.ScenarioRecord.newBuilder()
                        .setRecNo(i)
                        .setGroup(i % NUM_GROUPS)
                        .addScores(TestRecordsIndexScenariosProto.ScoreEntry.newBuilder()
                                .setScore(2L * i)
                                .setTimestamp(1000L + i))
                        .addScores(TestRecordsIndexScenariosProto.ScoreEntry.newBuilder()
                                .setScore(2L * i + 1)
                                .setTimestamp(2000L + i))
                        .build())
                .toList();
    }
}
