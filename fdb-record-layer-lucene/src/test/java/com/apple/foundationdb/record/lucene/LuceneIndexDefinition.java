/*
 * LuceneIndexDefinition.java
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

package com.apple.foundationdb.record.lucene;

import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsIndexScenariosProto;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.NestingKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexDefinition;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.IndexTarget;
import com.apple.foundationdb.record.provider.foundationdb.indexes.scenarios.ScenarioRecords;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A Lucene index over the scenario schema's text field.
 * <p>
 * Lucene has no bespoke key-expression classes: it annotates ordinary key expressions with function
 * key expressions (here {@link LuceneFunctionNames#LUCENE_TEXT}), so the generic
 * {@link IndexTarget} rooting works unchanged.
 */
class LuceneIndexDefinition implements IndexDefinition {
    private final String indexName = "luceneIndex";

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
        return TestRecordsIndexScenariosProto.IndexedMessage.newBuilder()
                // The tokenized text that the index is built over.
                .setStringValue("term" + index)
                // Not indexed here, but a partitioned variant would partition on this.
                .setLongValue(index)
                .build();
    }

    @Override
    public Index buildIndex(final IndexTarget target) {
        final NestingKeyExpression text = target.indexed(
                Key.Expressions.function(LuceneFunctionNames.LUCENE_TEXT,
                        Key.Expressions.field(ScenarioRecords.STRING_VALUE)));
        final KeyExpression groupingPrefix = target.groupingPrefix();
        final KeyExpression root = groupingPrefix.getColumnSize() == 0
                ? text
                : text.groupBy(groupingPrefix);
        return new Index(indexName, root, LuceneIndexTypes.LUCENE,
                ImmutableMap.of(LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED, "true"));
    }

    @Override
    public RecordCursor<IndexEntry> scanIndex(final FDBRecordStore store, final ScanProperties scanProperties) {
        final Index index = store.getRecordMetaData().getIndex(indexName);
        if (index.getRootExpression() instanceof GroupingKeyExpression) {
            // A grouped Lucene index has no cross-group scan: every search is scoped to one grouping key.
            // Walk the groups the framework generates and chain the per-group searches together.
            final List<Long> groups = IntStream.range(0, ScenarioRecords.NUM_GROUPS)
                    .mapToObj(Long::valueOf)
                    .toList();
            return RecordCursor.flatMapPipelined(
                    outerContinuation -> RecordCursor.fromList(store.getExecutor(), groups, outerContinuation),
                    (group, innerContinuation) -> scanGroup(store, index, group, innerContinuation, scanProperties),
                    null,
                    1);
        }
        return scanGroup(store, index, null, null, scanProperties);
    }

    @Override
    public boolean supportsSynthetic() {
        // Joined synthetic types are well supported by the Lucene maintainer, but unnested types are not
        // exercised anywhere in the Lucene module. Enabled so both synthetic scenarios run; see the
        // scenario results if the unnested case needs to be revisited.
        return true;
    }

    @Override
    public boolean supportsSnapshotIsolation() {
        // A Lucene search has to read the index's directory metadata, and FDBDirectory always reads
        // through the plain transaction (`agilityContext.apply(aContext -> aContext.ensureActive()...)`)
        // rather than a snapshot read, so the scan takes read-conflict ranges no matter what isolation
        // level is requested. A concurrent writer committing segment changes therefore conflicts with the
        // scanning transaction. Honouring snapshot isolation here would be a maintainer change, so skip.
        return false;
    }

    @Override
    public void configureStore(final FDBRecordStore store) {
        // Lucene merges are deferred maintenance; running them inline at commit makes the commit write
        // segment/directory data, which then conflicts with any concurrent writer. Every Lucene test
        // disables this, so do the same here.
        store.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(false);
    }

    @Override
    public boolean scanResultsEqual(final List<IndexEntry> expected, final List<IndexEntry> actual) {
        // Lucene index entries cannot be compared with Tuple equality: their key tuple holds the index's
        // key expression, and Tuple comparison rejects it ("Unsupported data type: NestingKeyExpression").
        // Compare the matched records instead, which is what the Lucene tests themselves assert on. The
        // set (rather than list) comparison also absorbs Lucene's score/doc ordering, which is not
        // guaranteed to be stable between an incrementally maintained index and a bulk rebuild.
        return primaryKeys(expected).equals(primaryKeys(actual));
    }

    @Nonnull
    private static Set<Tuple> primaryKeys(@Nonnull final List<IndexEntry> entries) {
        return entries.stream().map(IndexEntry::getPrimaryKey).collect(Collectors.toSet());
    }

    private RecordCursor<IndexEntry> scanGroup(final FDBRecordStore store, final Index index,
                                               final Long group, final byte[] continuation,
                                               final ScanProperties scanProperties) {
        final ScanComparisons groupComparisons = group == null
                ? ScanComparisons.EMPTY
                : ScanComparisons.from(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, group));
        final LuceneScanParameters parameters = new LuceneScanQueryParameters(groupComparisons,
                LuceneQuerySearchClause.MATCH_ALL_DOCS_QUERY);
        return store.scanIndex(index, parameters.bind(store, index, EvaluationContext.EMPTY),
                continuation, scanProperties);
    }
}
