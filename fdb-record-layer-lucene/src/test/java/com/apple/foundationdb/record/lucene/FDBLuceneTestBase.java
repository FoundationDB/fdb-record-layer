/*
 * FDBLuceneTestBase.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.lucene.directory.FDBDirectoryManager;
import com.apple.foundationdb.record.lucene.ngram.NgramAnalyzer;
import com.apple.foundationdb.record.lucene.synonym.SynonymAnalyzer;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.common.text.AllSuffixesTextTokenizer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableMap;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Sort;
import org.junit.jupiter.api.Assertions;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.lucene.LuceneIndexOptions.INDEX_PARTITION_BY_FIELD_NAME;
import static com.apple.foundationdb.record.lucene.LuceneIndexOptions.INDEX_PARTITION_HIGH_WATERMARK;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;

/**
 * Base class for Lucene tests.
 */
public abstract class FDBLuceneTestBase extends FDBRecordStoreTestBase {
    protected static final String ENGINEER_JOKE = "A software engineer, a hardware engineer, and a departmental manager were driving down a steep mountain road when suddenly the brakes on their car failed. The car careened out of control down the road, bouncing off the crash barriers, ground to a halt scraping along the mountainside. The occupants were stuck halfway down a mountain in a car with no brakes. What were they to do?" +
            "'I know,' said the departmental manager. 'Let's have a meeting, propose a Vision, formulate a Mission Statement, define some Goals, and by a process of Continuous Improvement find a solution to the Critical Problems, and we can be on our way.'" +
            "'No, no,' said the hardware engineer. 'That will take far too long, and that method has never worked before. In no time at all, I can strip down the car's braking system, isolate the fault, fix it, and we can be on our way.'" +
            "'Wait, said the software engineer. 'Before we do anything, I think we should push the car back up the road and see if it happens again.'";
    protected static final String WAYLON = "There's always one more way to do things and that's your way, and you have a right to try it at least once.";

    protected static final Index COMPLEX_PARTITIONED = complexPartitionedIndex(Map.of(
            IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME,
            INDEX_PARTITION_BY_FIELD_NAME, "timestamp",
            INDEX_PARTITION_HIGH_WATERMARK, "10"));

    protected static final Index COMPLEX_PARTITIONED_NOGROUP = complexPartitionedIndexNoGroup(Map.of(
            IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME,
            INDEX_PARTITION_BY_FIELD_NAME, "timestamp",
            INDEX_PARTITION_HIGH_WATERMARK, "10"));

    protected static final Index MULTIPLE_ANALYZER_LUCENE_INDEX = new Index("Complex$multiple_analyzer_autocomplete",
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of(LuceneIndexOptions.LUCENE_ANALYZER_NAME_OPTION, SynonymAnalyzer.QueryOnlySynonymAnalyzerFactory.ANALYZER_FACTORY_NAME,
                    LuceneIndexOptions.LUCENE_ANALYZER_NAME_PER_FIELD_OPTION, "text2:" + NgramAnalyzer.NgramAnalyzerFactory.ANALYZER_FACTORY_NAME));

    protected static final Index JOINED_INDEX = getJoinedIndex(Map.of(
            INDEX_PARTITION_BY_FIELD_NAME, "complex.timestamp",
            INDEX_PARTITION_HIGH_WATERMARK, "10"));


    protected StoreTimer.Counter getCounter(@Nonnull final FDBRecordContext recordContext, @Nonnull final StoreTimer.Event event) {
        return Verify.verifyNotNull(recordContext.getTimer()).getCounter(event);
    }

    protected List<LucenePartitionInfoProto.LucenePartitionInfo> getPartitionMeta(Index index,
                                                                                  Tuple groupingKey,
                                                                                  RecordLayerPropertyStorage contextProps,
                                                                                  Consumer<FDBRecordContext> schemaSetup) {
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            LuceneIndexMaintainer indexMaintainer = (LuceneIndexMaintainer)recordStore.getIndexMaintainer(index);
            return indexMaintainer.getPartitioner().getAllPartitionMetaInfo(groupingKey).join();
        }
    }

    protected FDBDirectoryManager getDirectoryManager(final Index index) {
        return getIndexMaintainer(index).getDirectoryManager();
    }

    protected IndexReader getIndexReader(final Index index,
                                         final Tuple groupingKey,
                                         final int partitionId) throws IOException {
        final FDBDirectoryManager manager = getDirectoryManager(index);
        return manager.getIndexReader(groupingKey, partitionId);
    }

    @Nonnull
    protected LuceneIndexMaintainer getIndexMaintainer(final Index index) {
        return (LuceneIndexMaintainer)recordStore.getIndexMaintainer(index);
    }

    protected FDBDirectory getDirectory(final Index index, final Tuple groupingKey) {
        return getIndexMaintainer(index).getDirectory(groupingKey, null);
    }

    protected long getSegmentCount(final Index index, final Tuple groupingKey) throws IOException {
        final String[] files = getDirectory(index, groupingKey).listAll();
        return Arrays.stream(files).filter(FDBDirectory::isCompoundFile).count();
    }

    protected Map<Integer, Integer> getSegmentCounts(Index index,
                                                     Tuple groupingKey,
                                                     RecordLayerPropertyStorage contextProps,
                                                     Consumer<FDBRecordContext> schemaSetup) {
        final List<LucenePartitionInfoProto.LucenePartitionInfo> partitionMeta = getPartitionMeta(index, groupingKey, contextProps, schemaSetup);
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            return partitionMeta.stream()
                    .collect(Collectors.toMap(
                            LucenePartitionInfoProto.LucenePartitionInfo::getId,
                            partitionInfo -> Assertions.assertDoesNotThrow(() ->
                                    getIndexReader(index, groupingKey, partitionInfo.getId()).getContext().leaves().size())
                    ));
        }
    }

    @Nonnull
    protected static Index complexPartitionedIndex(final Map<String, String> options) {
        return new Index("Complex$partitioned",
                concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
                        function(LuceneFunctionNames.LUCENE_SORTED, field("timestamp"))).groupBy(field("group")),
                LuceneIndexTypes.LUCENE,
                options);
    }

    @Nonnull
    protected static Index getJoinedIndex(final Map<String, String> options) {
        return new Index("joinNestedConcat",
                concat(
                        field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("is_seen"))),
                        field("simple").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text"))),
                        field("complex").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("timestamp")))
                ).groupBy(field("complex").nest("group")), LuceneIndexTypes.LUCENE,
                options);
    }

    @Nonnull
    protected static Index complexPartitionedIndexNoGroup(final Map<String, String> options) {
        return new Index("Complex$partitioned_noGroup",
                concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
                        function(LuceneFunctionNames.LUCENE_SORTED, field("timestamp"))),
                LuceneIndexTypes.LUCENE,
                options);
    }

    protected LuceneScanBounds groupedTextSearch(Index index, String search, Object group) {
        return groupedSortedTextSearch(index, search, null, group);
    }

    protected LuceneScanBounds groupedSortedTextSearch(Index index, String search, Sort sort, Object group) {
        return LuceneIndexTestValidator.groupedSortedTextSearch(recordStore, index, search, sort, group);
    }

    protected LuceneScanBounds fullTextSearch(Index index, String search) {
        return LuceneIndexTestUtils.fullTextSearch(recordStore, index, search, false);
    }

    protected void validateDocsInPartition(Index index, int partitionId, Tuple groupingKey,
                                           Set<Tuple> expectedPrimaryKeys, final String universalSearch) throws IOException {
        LuceneIndexTestValidator.validateDocsInPartition(recordStore, index, partitionId, groupingKey, expectedPrimaryKeys, universalSearch);
    }

    protected Set<Tuple> makeKeyTuples(long group, int... ranges) {
        int[] rangeList = Arrays.stream(ranges).toArray();
        if (rangeList.length == 0 || rangeList.length % 2 == 1) {
            throw new IllegalArgumentException("specify ranges as pairs of (from, to)");
        }
        Set<Tuple> tuples = new HashSet<>();
        for (int i = 0; i < rangeList.length - 1; i += 2) {
            for (int j = rangeList[i]; j <= rangeList[i + 1]; j++) {
                tuples.add(Tuple.from(group, j));
            }
        }
        return tuples;
    }
}
