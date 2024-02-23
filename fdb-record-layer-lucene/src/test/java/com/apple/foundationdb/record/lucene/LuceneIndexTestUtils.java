/*
 * LuceneIndexTestUtils.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.lucene.ngram.NgramAnalyzer;
import com.apple.foundationdb.record.lucene.synonym.EnglishSynonymMapConfig;
import com.apple.foundationdb.record.lucene.synonym.SynonymAnalyzer;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.provider.common.text.AllSuffixesTextTokenizer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.OnlineIndexer;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.cascades.CascadesPlanner;
import com.apple.foundationdb.record.query.plan.cascades.debug.Debugger;
import com.apple.foundationdb.record.query.plan.debug.DebuggerWithSymbolTables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.search.Sort;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Random;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.COMPLEX_DOC;

/**
 * A Grab-bag of different utility objects and methods designed to allow the same constructs to be reused in
 * different test classes(and therefore, to make testing lucene components slightly easier).
 */
public class LuceneIndexTestUtils {
    public static final Index SIMPLE_TEXT_SUFFIXES = new Index("Simple$text_suffixes",
            function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME,
                    // Common index in Lucene tests, set the option to TRUE to be used in tests
                    LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED, "true"));

    public static final Index TEXT_AND_STORED = new Index(
            "Simple$test_stored",
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_STORED, field("group"))),
            LuceneIndexTypes.LUCENE,
            Collections.emptyMap());

    public static final Index TEXT_AND_STORED_COMPLEX = new Index(
            "Simple$test_stored_complex",
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
                    function(LuceneFunctionNames.LUCENE_STORED, field("text2")),
                    function(LuceneFunctionNames.LUCENE_STORED, field("group")),
                    function(LuceneFunctionNames.LUCENE_STORED, field("score")),
                    function(LuceneFunctionNames.LUCENE_STORED, field("time")),
                    function(LuceneFunctionNames.LUCENE_STORED, field("is_seen"))),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of(
                    // Common index in Lucene tests, set the option to TRUE to be used in tests
                    LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED, "true"));

    public static final Index QUERY_ONLY_SYNONYM_LUCENE_INDEX = new Index("synonym_index", function(LuceneFunctionNames.LUCENE_TEXT, field("text")), LuceneIndexTypes.LUCENE,
            ImmutableMap.of(
                    LuceneIndexOptions.LUCENE_ANALYZER_NAME_OPTION, SynonymAnalyzer.QueryOnlySynonymAnalyzerFactory.ANALYZER_FACTORY_NAME,
                    LuceneIndexOptions.TEXT_SYNONYM_SET_NAME_OPTION, EnglishSynonymMapConfig.ExpandedEnglishSynonymMapConfig.CONFIG_NAME));

    public static final Index NGRAM_LUCENE_INDEX = new Index("ngram_index", function(LuceneFunctionNames.LUCENE_TEXT, field("text")), LuceneIndexTypes.LUCENE,
            ImmutableMap.of(LuceneIndexOptions.LUCENE_ANALYZER_NAME_OPTION, NgramAnalyzer.NgramAnalyzerFactory.ANALYZER_FACTORY_NAME,
                    IndexOptions.TEXT_TOKEN_MIN_SIZE, "3",
                    IndexOptions.TEXT_TOKEN_MAX_SIZE, "5"));



    public static TestRecordsTextProto.SimpleDocument createSimpleDocument(long docId, String text, Integer group) {
        var doc = TestRecordsTextProto.SimpleDocument.newBuilder()
                .setDocId(docId)
                .setText(text);
        if (group != null) {
            doc.setGroup(group);
        }
        return doc.build();
    }

    static TestRecordsTextProto.SimpleDocument createSimpleDocument(long docId, int group) {
        return TestRecordsTextProto.SimpleDocument.newBuilder()
                .setDocId(docId)
                .setGroup(group)
                .build();
    }

    public static Pair<FDBRecordStore, QueryPlanner> rebuildIndexMetaData(final FDBRecordContext context,
                                                                          final KeySpacePath path,
                                                                          final String document,
                                                                          final Index index,
                                                                          boolean useCascadesPlanner) {
        FDBRecordStore store = openRecordStore(context, path, metaDataBuilder -> {
            metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
            metaDataBuilder.addIndex(document, index);
        });

        QueryPlanner planner = setupPlanner(store, null, useCascadesPlanner);
        return Pair.of(store, planner);
    }


    static FDBRecordStore openRecordStore(FDBRecordContext context,
                                          @Nonnull KeySpacePath path,
                                          FDBRecordStoreTestBase.RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsTextProto.getDescriptor());
        metaDataBuilder.getRecordType(COMPLEX_DOC).setPrimaryKey(concatenateFields("group", "doc_id"));
        hook.apply(metaDataBuilder);
        return getStoreBuilder(context, path, metaDataBuilder.getRecordMetaData())
                .setSerializer(TextIndexTestUtils.COMPRESSING_SERIALIZER)
                .createOrOpen();
    }

    @Nonnull
    private static FDBRecordStore.Builder getStoreBuilder(@Nonnull FDBRecordContext context,
                                                          @Nonnull KeySpacePath path,
                                                          @Nonnull RecordMetaData metaData) {
        return FDBRecordStore.newBuilder()
                .setFormatVersion(FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION) // set to max to test newest features (unsafe for real deployments)
                .setKeySpacePath(path)
                .setContext(context)
                .setMetaDataProvider(metaData);
    }

    static QueryPlanner setupPlanner(@Nonnull FDBRecordStore recordStore,
                                     @Nullable PlannableIndexTypes indexTypes, boolean useRewritePlanner) {
        QueryPlanner planner;
        if (useRewritePlanner) {
            planner = new CascadesPlanner(recordStore.getRecordMetaData(), recordStore.getRecordStoreState());
            if (Debugger.getDebugger() == null) {
                Debugger.setDebugger(new DebuggerWithSymbolTables());
            }
            Debugger.setup();
        } else {
            if (indexTypes == null) {
                indexTypes = new PlannableIndexTypes(
                        Sets.newHashSet(IndexTypes.VALUE, IndexTypes.VERSION),
                        Sets.newHashSet(IndexTypes.RANK, IndexTypes.TIME_WINDOW_LEADERBOARD),
                        Sets.newHashSet(IndexTypes.TEXT),
                        Sets.newHashSet(LuceneIndexTypes.LUCENE)
                );
            }
            planner = new LucenePlanner(recordStore.getRecordMetaData(), recordStore.getRecordStoreState(), indexTypes, recordStore.getTimer());
        }
        return planner;
    }

    public static LuceneScanBounds fullSortTextSearch(FDBRecordStore recordStore, Index index, String search, Sort sort) {
        LuceneScanParameters scan = new LuceneScanQueryParameters(
                ScanComparisons.EMPTY,
                new LuceneQueryMultiFieldSearchClause(LuceneQueryType.QUERY, search, false),
                sort, null, null,
                null);
        return scan.bind(recordStore, index, EvaluationContext.EMPTY);
    }

    public static LuceneScanBounds fullTextSearch(FDBRecordStore recordStore, Index index, String search, boolean highlight) {
        LuceneScanParameters scan = new LuceneScanQueryParameters(
                ScanComparisons.EMPTY,
                new LuceneQueryMultiFieldSearchClause(highlight
                                                      ? LuceneQueryType.QUERY_HIGHLIGHT
                                                      : LuceneQueryType.QUERY, search, false),
                null, null, null,
                highlight ? new LuceneScanQueryParameters.LuceneQueryHighlightParameters(-1, 10) : null);
        return scan.bind(recordStore, index, EvaluationContext.EMPTY);
    }

    public static LuceneScanBounds fullTextSearch(FDBRecordStore recordStore, Index index, String search, boolean highlight, int snippetSize) {
        LuceneScanParameters scan = new LuceneScanQueryParameters(
                ScanComparisons.EMPTY,
                new LuceneQueryMultiFieldSearchClause(highlight
                                                      ? LuceneQueryType.QUERY_HIGHLIGHT
                                                      : LuceneQueryType.QUERY, search, false),
                null, null, null,
                highlight ? new LuceneScanQueryParameters.LuceneQueryHighlightParameters(snippetSize, 10) : null);
        return scan.bind(recordStore, index, EvaluationContext.EMPTY);
    }

    public static String[] generateRandomWords(int numberOfWords) {
        assert numberOfWords > 0 : "Number of words have to be greater than 0";
        StringBuilder builder = new StringBuilder();
        Random random = new Random();
        char[] word = null;
        for (int i = 0; i < numberOfWords; i++) {
            word = new char[random.nextInt(8) + 3]; // words of length 3 through 10. (1 and 2 letter words are boring.)
            for (int j = 0; j < word.length; j++) {
                word[j] = (char)('a' + random.nextInt(26));
            }
            if (i != numberOfWords - 1) {
                builder.append(word).append(" ");
            }
        }
        String[] returnValue = new String[2];
        returnValue[0] = new String(word);
        returnValue[1] = builder.toString();
        return returnValue;
    }

    public static TestRecordsTextProto.ComplexDocument createComplexDocument(long docId, String text, String text2, int group) {
        return createComplexDocument(docId, text, text2, group, true);
    }

    public static TestRecordsTextProto.ComplexDocument createComplexDocument(long docId, String text, String text2, int group, boolean isSeen) {
        return TestRecordsTextProto.ComplexDocument.newBuilder()
                .setDocId(docId)
                .setText(text)
                .setText2(text2)
                .setGroup(group)
                .setIsSeen(isSeen)
                .build();
    }

    public static TestRecordsTextProto.ComplexDocument createComplexDocument(long docId, String text, long group, long timestamp) {
        return TestRecordsTextProto.ComplexDocument.newBuilder()
                .setDocId(docId)
                .setText(text)
                .setGroup(group)
                .setTimestamp(timestamp)
                .build();
    }

    public static TestRecordsTextProto.ComplexDocument createComplexDocument(long docId, String text, String text2, long group, int score, boolean isSeen, double time) {
        return TestRecordsTextProto.ComplexDocument.newBuilder()
                .setDocId(docId)
                .setText(text)
                .setText2(text2)
                .setGroup(group)
                .setScore(score)
                .setIsSeen(isSeen)
                .setTime(time)
                .build();
    }

    public static TestRecordsTextProto.MapDocument createComplexMapDocument(long docId, String text, String text2, int group) {
        return TestRecordsTextProto.MapDocument.newBuilder()
                .setDocId(docId)
                .setGroup(group)
                .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder()
                        .setKey(text2)
                        .setValue(text)
                        .setSecondValue("secondValue" + docId)
                        .setThirdValue("thirdValue" + docId)
                        .build())
                .build();
    }

    /**
     * Try to force a segment merge on the provided store using the given index. The merge will be triggered if there are
     * sufficient conditions for one.
     * @param recordStore the store where the index is stored
     * @param index the Lucene indes to merge
     */
    public static void mergeSegments(final FDBRecordStore recordStore, final Index index) {
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setRecordStore(recordStore)
                .setIndex(index)
                .build()) {
            indexBuilder.mergeIndex();
        }
    }
}
