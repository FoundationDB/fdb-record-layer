/*
 * PermutedMinMaxIndexDefinition.java
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

package com.apple.foundationdb.record.provider.foundationdb.indexes;

import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanRange;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexDefinition;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexScenarioMetaData;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.ScenarioRecords;

import java.util.Collections;

class PermutedMinMaxIndexDefinition implements IndexDefinition {
    private final String indexName = "permutedIndex";
    private final String indexType;

    public PermutedMinMaxIndexDefinition(final String indexType) {
        this.indexType = indexType;
    }

    @Override
    public String getIndexName() {
        return indexName;
    }

    @Override
    public String getIndexedTypeName() {
        return ScenarioRecords.SCENARIO_RECORD;
    }

    @Override
    public Index buildIndex(final KeyExpression groupingPrefix) {
        // Grouping columns: [group?, str_value, num_value]; grouped value: permuted_value; permuted size 1
        // permutes num_value.
        final KeyExpression grouping = new GroupingKeyExpression(IndexScenarioMetaData.prefixed(groupingPrefix,
                Key.Expressions.concatenateFields(ScenarioRecords.STR_VALUE, ScenarioRecords.NUM_VALUE,
                        ScenarioRecords.PERMUTED_VALUE)), 1);
        return new Index(indexName, grouping, indexType,
                Collections.singletonMap(IndexOptions.PERMUTED_SIZE_OPTION, "1"));
    }

    @Override
    public RecordCursor<IndexEntry> scanIndex(final FDBRecordStore store, final ScanProperties scanProperties) {
        return store.scanIndex(store.getRecordMetaData().getIndex(indexName),
                new IndexScanRange(IndexScanType.BY_GROUP, TupleRange.ALL), null, scanProperties);
    }
}
