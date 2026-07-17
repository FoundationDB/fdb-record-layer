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

import com.apple.foundationdb.record.TestRecordsIndexScenariosProto;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Shared field/type name constants and record generation over the standard
 * {@code TestRecordsIndexScenariosProto} schema. The framework wraps each definition's
 * {@link IndexDefinition#generateIndexedMessage} content into {@code ScenarioRecord}s, owning the
 * primary key ({@code rec_no}) and grouping field ({@code group}).
 */
public final class ScenarioRecords {
    // Record type names.
    public static final String SCENARIO_RECORD = "ScenarioRecord";
    public static final String OTHER_RECORD = "OtherScenarioRecord";

    // Field names on ScenarioRecord.
    public static final String REC_NO = "rec_no";
    public static final String GROUP = "group";
    public static final String INDEXED = "indexed";
    public static final String OTHER_REC_NO = "other_rec_no";

    // Field names on IndexedMessage.
    public static final String INT_VALUE = "int_value";
    public static final String LONG_VALUE = "long_value";
    public static final String LONG_VALUE_2 = "long_value_2";
    public static final String STRING_VALUE = "string_value";
    public static final String BYTES_VALUE = "bytes_value";
    public static final String ENTRIES = "entries";

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
     * Generate {@code count} {@code ScenarioRecord}s, wrapping the definition's indexed content and
     * setting the framework-owned primary key and grouping field ({@code group} cycles over
     * {@link #NUM_GROUPS} so grouped scenarios see more than one group).
     *
     * @param count the number of records
     * @param definition the index definition providing the indexed content
     * @return the generated records
     */
    public static List<Message> scenarioRecords(final int count, final IndexDefinition definition) {
        return IntStream.range(0, count)
                .mapToObj(i -> (Message)TestRecordsIndexScenariosProto.ScenarioRecord.newBuilder()
                        .setRecNo(i)
                        .setGroup(i % NUM_GROUPS)
                        .setIndexed(definition.generateIndexedMessage(i))
                        .build())
                .toList();
    }

    /**
     * Generate records that produce no entry in any index under test: they are of a different record
     * type ({@code OtherScenarioRecord}), so writing them touches no index under test. Used to give a
     * transaction writes without touching the index (see {@code SnapshotScan}).
     *
     * @param count the number of records
     * @return the generated records
     */
    public static List<Message> otherRecords(final int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> (Message)TestRecordsIndexScenariosProto.OtherScenarioRecord.newBuilder()
                        .setRecNo(1000L + i)
                        .setGroup(i % NUM_GROUPS)
                        .build())
                .toList();
    }

    /**
     * Generate the constituent records for a joined synthetic type: for each {@code i}, a
     * {@code ScenarioRecord} (carrying the definition's indexed content) whose {@code other_rec_no}
     * matches a distinct {@code OtherScenarioRecord}, so exactly one joined synthetic record results.
     *
     * @param count the number of joined pairs
     * @param definition the index definition providing the indexed content
     * @return the constituent records to save
     */
    public static List<Message> joinedConstituents(final int count, final IndexDefinition definition) {
        final List<Message> records = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            final long otherRecNo = 1000L + i;
            records.add(TestRecordsIndexScenariosProto.ScenarioRecord.newBuilder()
                    .setRecNo(i)
                    .setGroup(i % NUM_GROUPS)
                    .setIndexed(definition.generateIndexedMessage(i))
                    .setOtherRecNo(otherRecNo)
                    .build());
            records.add(TestRecordsIndexScenariosProto.OtherScenarioRecord.newBuilder()
                    .setRecNo(otherRecNo)
                    .setGroup(i % NUM_GROUPS)
                    .build());
        }
        return records;
    }

    /**
     * Generate parent records for an unnested synthetic type: each {@code ScenarioRecord} carries two
     * {@code ScoreEntry}s (with distinct scores), so unnesting yields two synthetic records per parent.
     * The unnested index is over {@code ScoreEntry.score}, so this content is fixed by the framework.
     *
     * @param count the number of parent records
     * @return the parent records to save
     */
    public static List<Message> unnestedParents(final int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> (Message)TestRecordsIndexScenariosProto.ScenarioRecord.newBuilder()
                        .setRecNo(i)
                        .setGroup(i % NUM_GROUPS)
                        .setIndexed(TestRecordsIndexScenariosProto.IndexedMessage.newBuilder()
                                .addEntries(TestRecordsIndexScenariosProto.ScoreEntry.newBuilder()
                                        .setScore(2L * i)
                                        .setTimestamp(1000L + i))
                                .addEntries(TestRecordsIndexScenariosProto.ScoreEntry.newBuilder()
                                        .setScore(2L * i + 1)
                                        .setTimestamp(2000L + i)))
                        .build())
                .toList();
    }
}
