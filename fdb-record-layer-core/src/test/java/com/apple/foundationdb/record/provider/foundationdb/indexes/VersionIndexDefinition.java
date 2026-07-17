/*
 * VersionIndexDefinition.java
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
import com.apple.foundationdb.record.TestRecordsIndexScenariosProto;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.IndexScanRange;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexDefinition;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexScenarioMetaData;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.ScenarioRecords;

class VersionIndexDefinition implements IndexDefinition {
    private final String indexName = "versionIndex";

    @Override
    public String getIndexName() {
        return indexName;
    }

    @Override
    public String getIndexedTypeName() {
        return ScenarioRecords.SCENARIO_RECORD;
    }

    @Override
    public TestRecordsIndexScenariosProto.IndexedMessage generateIndexedMessage(final int index) {
        // The version index is over the record version, not any indexed field.
        return TestRecordsIndexScenariosProto.IndexedMessage.getDefaultInstance();
    }

    @Override
    public Index buildIndex(final KeyExpression groupingPrefix) {
        return new Index(indexName, IndexScenarioMetaData.prefixed(groupingPrefix, VersionKeyExpression.VERSION),
                IndexTypes.VERSION);
    }

    @Override
    public RecordCursor<IndexEntry> scanIndex(final FDBRecordStore store, final ScanProperties scanProperties) {
        return store.scanIndex(store.getRecordMetaData().getIndex(indexName),
                new IndexScanRange(IndexScanType.BY_VALUE, TupleRange.ALL), null, scanProperties);
    }

    @Override
    public boolean supportsSynthetic() {
        return true;
    }

    @Override
    public Index buildSyntheticIndex(final KeyExpression valueExpression) {
        // The version is a property of the synthetic record as a whole.
        return new Index(indexName, VersionKeyExpression.VERSION, IndexTypes.VERSION);
    }
}
