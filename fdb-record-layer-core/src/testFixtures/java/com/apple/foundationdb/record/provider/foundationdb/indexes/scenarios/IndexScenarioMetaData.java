/*
 * IndexScenarioMetaData.java
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

import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsIndexScenariosProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.JoinedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.UnnestedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;

/**
 * Builds {@link RecordMetaData} for the scenario framework over the shared
 * {@code TestRecordsIndexScenariosProto} schema. The framework — not the individual definitions —
 * owns the metadata so it can align primary keys with grouping (for {@code deleteRecordsWhere}) and
 * wire up synthetic record types.
 */
public final class IndexScenarioMetaData {

    /** Whether the index (and primary keys) should be grouped by the {@code group} field. */
    public enum GroupingMode {
        UNGROUPED, GROUPED
    }

    /** The kind of synthetic record type to build the index over. */
    public enum SyntheticKind {
        JOINED, UNNESTED
    }

    private IndexScenarioMetaData() {
    }

    /**
     * Compose a value expression with a grouping prefix: returns {@code expr} when the prefix is
     * empty, else {@code concat(prefix, expr)}.
     *
     * @param prefix the grouping prefix (possibly empty)
     * @param expr the value expression
     * @return the composed expression
     */
    public static KeyExpression prefixed(final KeyExpression prefix, final KeyExpression expr) {
        return prefix.getColumnSize() == 0 ? expr : concat(prefix, expr);
    }

    /**
     * Build metadata for a normal (non-synthetic) scenario. In {@link GroupingMode#GROUPED} mode the
     * index is grouped by {@code group} and every record type's primary key is prefixed with
     * {@code group}, so {@code deleteRecordsWhere(field("group").equalsValue(...))} can clear whole
     * groups.
     *
     * @param definition the index definition under test
     * @param mode grouped or ungrouped
     * @return the built metadata
     */
    public static RecordMetaData forScenario(final IndexDefinition definition, final GroupingMode mode) {
        final RecordMetaDataBuilder builder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsIndexScenariosProto.getDescriptor());
        final KeyExpression prefix = mode == GroupingMode.GROUPED
                ? field(ScenarioRecords.GROUP) : ScenarioRecords.noPrefix();
        final Index index = definition.buildIndex(prefix);
        if (IndexTypes.VERSION.equals(index.getType())) {
            builder.setStoreRecordVersions(true);
        }
        final KeyExpression primaryKey = mode == GroupingMode.GROUPED
                ? concat(field(ScenarioRecords.GROUP), field(ScenarioRecords.REC_NO))
                : field(ScenarioRecords.REC_NO);
        builder.getRecordType(ScenarioRecords.SCENARIO_RECORD).setPrimaryKey(primaryKey);
        builder.getRecordType(ScenarioRecords.OTHER_RECORD).setPrimaryKey(primaryKey);
        builder.addIndex(definition.getIndexedTypeName(), index);
        return builder.build();
    }

    /**
     * Build metadata for a synthetic-type scenario, wiring up either a joined or an unnested record
     * type and adding the definition's index over it.
     *
     * @param definition the index definition under test
     * @param kind joined or unnested
     * @return the built metadata
     */
    public static RecordMetaData forSynthetic(final IndexDefinition definition, final SyntheticKind kind) {
        final RecordMetaDataBuilder builder = RecordMetaData.newBuilder()
                .setRecords(TestRecordsIndexScenariosProto.getDescriptor());
        final Index index;
        if (kind == SyntheticKind.JOINED) {
            final JoinedRecordTypeBuilder joined = builder.addJoinedRecordType(ScenarioRecords.JOINED_TYPE);
            joined.addConstituent(ScenarioRecords.SIMPLE_CONSTITUENT,
                    builder.getRecordType(ScenarioRecords.SCENARIO_RECORD), false);
            joined.addConstituent(ScenarioRecords.OTHER_CONSTITUENT,
                    builder.getRecordType(ScenarioRecords.OTHER_RECORD), false);
            joined.addJoin(ScenarioRecords.SIMPLE_CONSTITUENT, ScenarioRecords.OTHER_REC_NO,
                    ScenarioRecords.OTHER_CONSTITUENT, ScenarioRecords.REC_NO);
            // Index the simple constituent's indexed.int_value.
            index = definition.buildSyntheticIndex(field(ScenarioRecords.SIMPLE_CONSTITUENT)
                    .nest(field(ScenarioRecords.INDEXED).nest(ScenarioRecords.INT_VALUE)));
            maybeStoreVersions(builder, index);
            builder.addIndex(ScenarioRecords.JOINED_TYPE, index);
        } else {
            final UnnestedRecordTypeBuilder unnested = builder.addUnnestedRecordType(ScenarioRecords.UNNESTED_TYPE);
            unnested.addParentConstituent(ScenarioRecords.PARENT_CONSTITUENT,
                    builder.getRecordType(ScenarioRecords.SCENARIO_RECORD));
            // The repeated entries live inside the (singular) indexed message: indexed.entries.
            unnested.addNestedConstituent(ScenarioRecords.ENTRY_CONSTITUENT,
                    TestRecordsIndexScenariosProto.ScoreEntry.getDescriptor(),
                    ScenarioRecords.PARENT_CONSTITUENT,
                    field(ScenarioRecords.INDEXED).nest(field(ScenarioRecords.ENTRIES, KeyExpression.FanType.FanOut)));
            // Index the unnested entry constituent's score.
            index = definition.buildSyntheticIndex(field(ScenarioRecords.ENTRY_CONSTITUENT).nest(ScenarioRecords.SCORE));
            maybeStoreVersions(builder, index);
            builder.addIndex(ScenarioRecords.UNNESTED_TYPE, index);
        }
        return builder.build();
    }

    private static void maybeStoreVersions(final RecordMetaDataBuilder builder, final Index index) {
        if (IndexTypes.VERSION.equals(index.getType())) {
            builder.setStoreRecordVersions(true);
        }
    }
}
