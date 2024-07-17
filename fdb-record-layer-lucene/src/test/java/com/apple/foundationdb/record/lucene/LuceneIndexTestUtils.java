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
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
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
import com.apple.foundationdb.record.util.pair.Pair;
import com.google.auto.service.AutoService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.search.Sort;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.metadata.Key.Expressions.value;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.COMPLEX_DOC;

/**
 * A Grab-bag of different utility objects and methods designed to allow the same constructs to be reused in
 * different test classes(and therefore, to make testing lucene components slightly easier).
 */
public class LuceneIndexTestUtils {

    protected static final List<KeyExpression> lucene_keys = List.of(
            function(LuceneFunctionNames.LUCENE_TEXT, field("value")),
            function(LuceneFunctionNames.LUCENE_TEXT, field("second_value")),
            function(LuceneFunctionNames.LUCENE_TEXT, field("third_value")));

    protected static final List<KeyExpression> keys = ImmutableList.copyOf(Iterables.concat(List.of(field("key")), lucene_keys));
    protected static final String COMBINED_SYNONYM_SETS = "COMBINED_SYNONYM_SETS";

    protected static final String COMPLEX_MULTIPLE_TEXT_INDEXES_KEY = "complex_multiple_text_indexes_key";
    protected static final String SIMPLE_TEXT_SUFFIXES_KEY = "simple_text_suffixes_key";
    protected static final String MANY_FIELDS_INDEX_KEY = "many_fields_index_key";
    protected static final String TEXT_AND_BOOLEAN_INDEX_KEY = "text_and_boolean_index_key";
    protected static final String TEXT_AND_NUMBER_INDEX_KEY = "text_and_number_index_key";
    protected static final String SIMPLE_TEXT_WITH_AUTO_COMPLETE_KEY = "simple_text_with_auto_complete_key";
    protected static final String EMAIL_CJK_SYM_TEXT_WITH_AUTO_COMPLETE_KEY = "email_cjk_sym_text_with_auto_complete_key";
    protected static final String MAP_ON_VALUE_INDEX_KEY = "map_on_value_index_key";
    protected static final String SIMPLE_TEXT_SUFFIXES_WITH_PRIMARY_KEY_SEGMENT_INDEX_KEY = "simple_text_suffixes_with_primary_key_segment_index_key";
    protected static final String COMPLEX_GROUPED_WITH_PRIMARY_KEY_SEGMENT_INDEX_KEY = "complex_grouped_with_primary_key_segment_index_key";
    protected static final String QUERY_ONLY_SYNONYM_LUCENE_INDEX_KEY = "query_only_synonym_lucene_index_key";
    protected static final String AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX_KEY = "authoritative_synonym_only_lucene_index_key";
    protected static final String QUERY_ONLY_SYNONYM_LUCENE_COMBINED_SETS_INDEX_KEY = "query_only_synonym_lucene_combined_sets_index_key";
    protected static final String COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE_KEY = "complex_multiple_text_indexes_with_auto_complete_key";
    protected static final String MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE_KEY = "map_on_value_index_with_auto_complete_key";
    protected static final String MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE_EXCLUDED_FIELDS_KEY = "map_on_value_index_with_auto_complete_excluded_fields_key";
    protected static final String SIMPLE_TEXT_WITH_AUTO_COMPLETE_NO_FREQS_POSITIONS_KEY = "simple_text_with_auto_complete_no_freqs_positions";
    protected static final String SPELLCHECK_INDEX_KEY = "spellcheck_index_key";
    protected static final String SPELLCHECK_INDEX_COMPLEX_KEY = "spellcheck_index_complex_key";
    protected static final String COMPLEX_MULTIPLE_GROUPED_KEY = "complex_multiple_grouped_key";
    protected static final String COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE_KEY = "complex_multi_grouped_with_auto_complete_key";
    protected static final String ANALYZER_CHOOSER_TEST_LUCENE_INDEX_KEY = "analyzer_chooser_test_lucene_index_key";
    protected static final String AUTO_COMPLETE_SIMPLE_LUCENE_INDEX_KEY = "auto_complete_simple_lucene_index_key";

    public static final KeyExpression SIMPLE_TEXT_WITH_AUTO_COMPLETE_STORED_FIELD = function(LuceneFunctionNames.LUCENE_TEXT, field("text"));
    protected static final List<KeyExpression> COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE_STORED_FIELDS = ImmutableList.of(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_TEXT, field("text2")));
    public static final KeyExpression JOINED_SIMPLE_TEXT_WITH_AUTO_COMPLETE_STORED_FIELD = field("simple").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text")));
    protected static final List<KeyExpression> JOINED_COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE_STORED_FIELDS =
            ImmutableList.of(field("simple").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))));

    protected static final List<KeyExpression> MAP_ON_VALUE_INDEX_STORED_FIELDS =
            lucene_keys.stream()
                    .map(key -> field("entry", KeyExpression.FanType.FanOut).nest(key))
                    .collect(ImmutableList.toImmutableList());

    protected static final List<KeyExpression> JOINED_MAP_ON_VALUE_INDEX_STORED_FIELDS =
            ImmutableList.of(field("map").nest(concat(lucene_keys.stream()
                    .map(key -> field("entry", KeyExpression.FanType.FanOut).nest(key))
                    .collect(ImmutableList.toImmutableList()))));

    protected static final Index MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE_EXCLUDED_FIELDS =
            getMapOnValueIndexWithOption("Map_with_auto_complete_excluded_fields$entry-value", ImmutableMap.of());

    public static final Index SIMPLE_TEXT_SUFFIXES = simpleTextSuffixesIndex(options -> { });

    @Nonnull
    public static Index simpleTextSuffixesIndex(Consumer<Map<String, String>> optionsBuilder) {
        final Map<String, String> options = new HashMap<>();
        options.put(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME);
        options.put(LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED, "true");
        optionsBuilder.accept(options);
        return new Index("Simple$text_suffixes",
                function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
                LuceneIndexTypes.LUCENE,
                options);
    }

    protected static final Index MANY_FIELDS_INDEX = new Index(
            "many_fields_idx",
            concat( function(LuceneFunctionNames.LUCENE_TEXT, field("text0")),
                    function(LuceneFunctionNames.LUCENE_TEXT, field("text1")),
                    function(LuceneFunctionNames.LUCENE_TEXT, field("text3")),
                    function(LuceneFunctionNames.LUCENE_TEXT, field("text4")),
                    function(LuceneFunctionNames.LUCENE_TEXT, field("text5")),
                    function(LuceneFunctionNames.LUCENE_TEXT, field("text6")),
                    function(LuceneFunctionNames.LUCENE_TEXT, field("text7")),
                    function(LuceneFunctionNames.LUCENE_TEXT, field("text8")),
                    function(LuceneFunctionNames.LUCENE_TEXT, field("text9")),
                    function(LuceneFunctionNames.LUCENE_STORED, field("long0")),
                    function(LuceneFunctionNames.LUCENE_SORTED, field("long0")),
                    function(LuceneFunctionNames.LUCENE_STORED, field("long1")),
                    function(LuceneFunctionNames.LUCENE_SORTED, field("long1")),
                    function(LuceneFunctionNames.LUCENE_STORED, field("long2")),
                    function(LuceneFunctionNames.LUCENE_SORTED, field("long2")),
                    function(LuceneFunctionNames.LUCENE_STORED, field("long3")),
                    function(LuceneFunctionNames.LUCENE_SORTED, field("long3")),
                    function(LuceneFunctionNames.LUCENE_STORED, field("long4")),
                    function(LuceneFunctionNames.LUCENE_SORTED, field("long4")),
                    function(LuceneFunctionNames.LUCENE_STORED, field("long5")),
                    function(LuceneFunctionNames.LUCENE_SORTED, field("long5")),
                    function(LuceneFunctionNames.LUCENE_STORED, field("long6")),
                    function(LuceneFunctionNames.LUCENE_SORTED, field("long6")),
                    function(LuceneFunctionNames.LUCENE_STORED, field("long7")),
                    function(LuceneFunctionNames.LUCENE_SORTED, field("long7")),
                    function(LuceneFunctionNames.LUCENE_STORED, field("long8")),
                    function(LuceneFunctionNames.LUCENE_SORTED, field("long8")),
                    function(LuceneFunctionNames.LUCENE_STORED, field("long9")),
                    function(LuceneFunctionNames.LUCENE_SORTED, field("long9")),
                    function(LuceneFunctionNames.LUCENE_STORED, field("bool0")),
                    function(LuceneFunctionNames.LUCENE_SORTED, field("bool0")),
                    function(LuceneFunctionNames.LUCENE_STORED, field("bool1")),
                    function(LuceneFunctionNames.LUCENE_SORTED, field("bool1")),
                    function(LuceneFunctionNames.LUCENE_STORED, field("bool2")),
                    function(LuceneFunctionNames.LUCENE_SORTED, field("bool2")),
                    function(LuceneFunctionNames.LUCENE_STORED, field("bool3")),
                    function(LuceneFunctionNames.LUCENE_SORTED, field("bool3")),
                    function(LuceneFunctionNames.LUCENE_STORED, field("bool4")),
                    function(LuceneFunctionNames.LUCENE_SORTED, field("bool4")),
                    function(LuceneFunctionNames.LUCENE_STORED, field("bool5")),
                    function(LuceneFunctionNames.LUCENE_SORTED, field("bool5")),
                    function(LuceneFunctionNames.LUCENE_STORED, field("bool6")),
                    function(LuceneFunctionNames.LUCENE_SORTED, field("bool6")),
                    function(LuceneFunctionNames.LUCENE_STORED, field("bool7")),
                    function(LuceneFunctionNames.LUCENE_SORTED, field("bool7")),
                    function(LuceneFunctionNames.LUCENE_STORED, field("bool8")),
                    function(LuceneFunctionNames.LUCENE_SORTED, field("bool8")),
                    function(LuceneFunctionNames.LUCENE_STORED, field("bool9")),
                    function(LuceneFunctionNames.LUCENE_SORTED, field("bool9"))),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of(
                    LuceneIndexOptions.LUCENE_ANALYZER_NAME_OPTION, SynonymAnalyzer.QueryOnlySynonymAnalyzerFactory.ANALYZER_FACTORY_NAME,
                    LuceneIndexOptions.TEXT_SYNONYM_SET_NAME_OPTION, COMBINED_SYNONYM_SETS));

    protected static final Index TEXT_AND_BOOLEAN_INDEX = new Index(
            "text_and_number_idx",
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_STORED, field("is_seen"))),
            LuceneIndexTypes.LUCENE,
            Collections.emptyMap());

    protected static final Index TEXT_AND_NUMBER_INDEX = new Index(
            "text_and_number_idx",
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_STORED, field("group"))),
            LuceneIndexTypes.LUCENE,
            Collections.emptyMap());

    protected static final Index SIMPLE_TEXT_WITH_AUTO_COMPLETE = new Index("Simple_with_auto_complete",
            SIMPLE_TEXT_WITH_AUTO_COMPLETE_STORED_FIELD,
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of());

    protected static final Index EMAIL_CJK_SYM_TEXT_WITH_AUTO_COMPLETE = new Index("Email_cjk_sym_with_auto_complete",
            SIMPLE_TEXT_WITH_AUTO_COMPLETE_STORED_FIELD,
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of(LuceneIndexOptions.LUCENE_ANALYZER_NAME_OPTION, EmailCjkSynonymAnalyzer.UNIQUE_IDENTIFIER));

    protected static final Index MAP_ON_VALUE_INDEX = getMapOnValueIndexWithOption("Map$entry-value", ImmutableMap.of());

    protected static final Index MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE =
            getMapOnValueIndexWithOption("Map_with_auto_complete$entry-value", ImmutableMap.of());

    protected static final Index SIMPLE_TEXT_SUFFIXES_WITH_PRIMARY_KEY_SEGMENT_INDEX = new Index("Simple$text_suffixes_pky",
            function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME,
                    LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED, "true"));

    protected static final Index COMPLEX_GROUPED_WITH_PRIMARY_KEY_SEGMENT_INDEX = new Index("Complex$text_pky",
            function(LuceneFunctionNames.LUCENE_TEXT, field("text")).groupBy(field("group")),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME,
                    LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED, "true"));

    public static final Index COMPLEX_MULTIPLE_TEXT_INDEXES = new Index("Complex$text_multipleIndexes",
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME));

    public static final Index TEXT_AND_STORED = new Index(
            "Simple$test_stored",
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_STORED, field("group"))),
            LuceneIndexTypes.LUCENE,
            Collections.emptyMap());

    public static final Index TEXT_AND_STORED_COMPLEX = textAndStoredComplexIndex(options -> { });

    @Nonnull
    public static Index textAndStoredComplexIndex(final Consumer<Map<String, String>> optionsBuilder) {
        Map<String, String> options = new HashMap<>();
        options.put(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME);
        options.put(LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED, "true");
        optionsBuilder.accept(options);
        return new Index(
                "Simple$test_stored_complex",
                concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
                        function(LuceneFunctionNames.LUCENE_STORED, field("text2")),
                        function(LuceneFunctionNames.LUCENE_STORED, field("group")),
                        function(LuceneFunctionNames.LUCENE_STORED, field("score")),
                        function(LuceneFunctionNames.LUCENE_STORED, field("time")),
                        function(LuceneFunctionNames.LUCENE_STORED, field("is_seen"))),
                LuceneIndexTypes.LUCENE,
                options);
    }

    public static final Index QUERY_ONLY_SYNONYM_LUCENE_INDEX = new Index("synonym_index", function(LuceneFunctionNames.LUCENE_TEXT, field("text")), LuceneIndexTypes.LUCENE,
            ImmutableMap.of(
                    LuceneIndexOptions.LUCENE_ANALYZER_NAME_OPTION, SynonymAnalyzer.QueryOnlySynonymAnalyzerFactory.ANALYZER_FACTORY_NAME,
                    LuceneIndexOptions.TEXT_SYNONYM_SET_NAME_OPTION, EnglishSynonymMapConfig.ExpandedEnglishSynonymMapConfig.CONFIG_NAME));

    protected static final Index AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX = new Index("synonym_index", function(LuceneFunctionNames.LUCENE_TEXT, field("text")), LuceneIndexTypes.LUCENE,
            ImmutableMap.of(
                    LuceneIndexOptions.LUCENE_ANALYZER_NAME_OPTION, SynonymAnalyzer.AuthoritativeSynonymOnlyAnalyzerFactory.ANALYZER_FACTORY_NAME,
                    LuceneIndexOptions.TEXT_SYNONYM_SET_NAME_OPTION, EnglishSynonymMapConfig.AuthoritativeOnlyEnglishSynonymMapConfig.CONFIG_NAME));

    private static final Index QUERY_ONLY_SYNONYM_LUCENE_COMBINED_SETS_INDEX = new Index("synonym_combined_sets_index", function(LuceneFunctionNames.LUCENE_TEXT, field("text")), LuceneIndexTypes.LUCENE,
            ImmutableMap.of(
                    LuceneIndexOptions.LUCENE_ANALYZER_NAME_OPTION, SynonymAnalyzer.QueryOnlySynonymAnalyzerFactory.ANALYZER_FACTORY_NAME,
                    LuceneIndexOptions.TEXT_SYNONYM_SET_NAME_OPTION, COMBINED_SYNONYM_SETS));


    public static final Index NGRAM_LUCENE_INDEX = new Index("ngram_index", function(LuceneFunctionNames.LUCENE_TEXT, field("text")), LuceneIndexTypes.LUCENE,
            ImmutableMap.of(LuceneIndexOptions.LUCENE_ANALYZER_NAME_OPTION, NgramAnalyzer.NgramAnalyzerFactory.ANALYZER_FACTORY_NAME,
                    IndexOptions.TEXT_TOKEN_MIN_SIZE, "3",
                    IndexOptions.TEXT_TOKEN_MAX_SIZE, "5"));

    protected static final List<KeyExpression> COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE_STORED_FIELDS =
            List.of(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_TEXT, field("text2")));

    protected static final Index COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE = new Index("Complex$text_multipleIndexes",
            concat(COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE_STORED_FIELDS),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of());

    private static final Index SIMPLE_TEXT_WITH_AUTO_COMPLETE_NO_FREQS_POSITIONS = new Index("Simple_with_auto_complete",
            function(LuceneFunctionNames.LUCENE_TEXT, concat(field("text"),
                    function(LuceneFunctionNames.LUCENE_AUTO_COMPLETE_FIELD_INDEX_OPTIONS, value(LuceneFunctionNames.LuceneFieldIndexOptions.DOCS.name())),
                    function(LuceneFunctionNames.LUCENE_FULL_TEXT_FIELD_INDEX_OPTIONS, value(LuceneFunctionNames.LuceneFieldIndexOptions.DOCS.name())))),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of());

    protected static final Index SPELLCHECK_INDEX = new Index(
            "spellcheck_index",
            function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
            LuceneIndexTypes.LUCENE,
            Collections.emptyMap());

    protected static final Index SPELLCHECK_INDEX_COMPLEX = new Index(
            "spellcheck_index_complex",
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))),
            LuceneIndexTypes.LUCENE,
            Collections.emptyMap());

    protected static final Index COMPLEX_MULTIPLE_GROUPED = new Index("Complex$text_multiple_grouped",
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))).groupBy(field("group")),
            LuceneIndexTypes.LUCENE);

    protected static final Index COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE = new Index("Complex$text_multiple_grouped_autocomplete",
            concat(COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE_STORED_FIELDS).groupBy(field("group")),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of());

    private static final Index ANALYZER_CHOOSER_TEST_LUCENE_INDEX = new Index("analyzer_chooser_test_index", function(LuceneFunctionNames.LUCENE_TEXT, field("text")), LuceneIndexTypes.LUCENE,
            ImmutableMap.of(
                    LuceneIndexOptions.LUCENE_ANALYZER_NAME_OPTION, TestAnalyzerFactory.ANALYZER_FACTORY_NAME));

    private static final Index AUTO_COMPLETE_SIMPLE_LUCENE_INDEX = new Index("Complex$multiple_analyzer_autocomplete",
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of());


    // =============== JOINED INDEXES =================

    protected static final Index JOINED_COMPLEX_GROUPED_WITH_PRIMARY_KEY_SEGMENT_INDEX = new Index("JoinedComplex$text_pky",
            concat(
                    field("simple").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text"))),
                    field("complex").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))),
                    field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("score"))),
                    field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("group"))),
                    field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("is_seen"))),
                    field("complex").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("timestamp")))
            ).groupBy(field("complex").nest("score")), LuceneIndexTypes.LUCENE,
            ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME,
                    LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED, "true"));

    protected static final Index JOINED_COMPLEX_MULTIPLE_TEXT_INDEXES =
            new Index("JoinedComplex$text_multipleIndexes",
                    concat(
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("is_seen"))),
                            field("simple").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("score"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("group"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("timestamp")))
                    ), LuceneIndexTypes.LUCENE,
                    ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME,
                            LuceneIndexOptions.TEXT_SYNONYM_SET_NAME_OPTION, EnglishSynonymMapConfig.ExpandedEnglishSynonymMapConfig.CONFIG_NAME,
                            LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED, "true"));

    protected static final Index JOINED_COMPLEX_MULTIPLE_EMAIL_CJK_SYM_INDEXES =
            new Index("JoinedComplex$text_multipleEmailCJKSymIndexes",
                    concat(
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("is_seen"))),
                            field("simple").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("score"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("group"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("timestamp")))
                    ), LuceneIndexTypes.LUCENE,
                    ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME,
                            LuceneIndexOptions.LUCENE_ANALYZER_NAME_OPTION, EmailCjkSynonymAnalyzer.UNIQUE_IDENTIFIER,
                            LuceneIndexOptions.TEXT_SYNONYM_SET_NAME_OPTION, EnglishSynonymMapConfig.ExpandedEnglishSynonymMapConfig.CONFIG_NAME,
                            LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED, "true"));

    protected static final Index JOINED_SYNONYM_ONLY_COMPLEX_MULTIPLE_TEXT_INDEXES =
            new Index("JoinedComplex$synonym_only_text_multipleIndexes",
                    concat(
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("is_seen"))),
                            field("simple").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("score"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("group"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("timestamp")))
                    ), LuceneIndexTypes.LUCENE,
                    ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME,
                            LuceneIndexOptions.LUCENE_ANALYZER_NAME_OPTION, SynonymAnalyzer.QueryOnlySynonymAnalyzerFactory.ANALYZER_FACTORY_NAME,
                            LuceneIndexOptions.TEXT_SYNONYM_SET_NAME_OPTION, EnglishSynonymMapConfig.ExpandedEnglishSynonymMapConfig.CONFIG_NAME,
                            LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED, "true"));

    protected static final Index JOINED_COMPLEX_MULTIPLE_TEXT_ANALYZER_CHOOSER_INDEXES =
            new Index("JoinedComplex$text_analyzerChooser_multipleIndexes",
                    concat(
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("is_seen"))),
                            field("simple").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("score"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("group"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("timestamp")))
                    ), LuceneIndexTypes.LUCENE,
                    ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME,
                            LuceneIndexOptions.TEXT_SYNONYM_SET_NAME_OPTION, EnglishSynonymMapConfig.ExpandedEnglishSynonymMapConfig.CONFIG_NAME,
                            LuceneIndexOptions.LUCENE_ANALYZER_NAME_OPTION, TestAnalyzerFactory.ANALYZER_FACTORY_NAME,
                            LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED, "true"));

    protected static final Index JOINED_AUTHORITATIVE_SYNONYM_COMPLEX_MULTIPLE_TEXT_INDEXES =
            new Index("JoinedSynonymAuthoritativeComplex$text_multipleIndexes",
                    concat(
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("is_seen"))),
                            field("simple").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("score"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("group"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("timestamp")))
                    ), LuceneIndexTypes.LUCENE,
                    ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME,
                            LuceneIndexOptions.LUCENE_ANALYZER_NAME_OPTION, SynonymAnalyzer.AuthoritativeSynonymOnlyAnalyzerFactory.ANALYZER_FACTORY_NAME,
                            LuceneIndexOptions.TEXT_SYNONYM_SET_NAME_OPTION, EnglishSynonymMapConfig.AuthoritativeOnlyEnglishSynonymMapConfig.CONFIG_NAME,
                            LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED, "true"));

    protected static final Index JOINED_QUERY_ONLY_SYNONYM_LUCENE_COMBINED_SETS_INDEX =
            new Index("JoinedSynonymCombinedComplex$text_multipleIndexes",
                    concat(
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("is_seen"))),
                            field("simple").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("score"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("group"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("timestamp")))
                    ), LuceneIndexTypes.LUCENE,
                    ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME,
                            LuceneIndexOptions.LUCENE_ANALYZER_NAME_OPTION, SynonymAnalyzer.AuthoritativeSynonymOnlyAnalyzerFactory.ANALYZER_FACTORY_NAME,
                            LuceneIndexOptions.TEXT_SYNONYM_SET_NAME_OPTION, COMBINED_SYNONYM_SETS,
                            LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED, "true"));

    protected static final Index JOINED_COMPLEX_MULTIPLE_TEXT_PRIMARY_KEY_SEGMENT_INDEXES =
            new Index("JoinedComplex$text_multipleIndexes",
                    concat(
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("is_seen"))),
                            field("simple").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("score"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("group"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("timestamp")))
                    ), LuceneIndexTypes.LUCENE,
                    ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME,
                            LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED, "true"));

    protected static final Index JOINED_MANY_FIELDS_INDEX = new Index(
            "Joined$many_fields_idx",
            concat(
                    field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("is_seen"))),
                    field("complex").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))),
                    field("complex").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("timestamp"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text0"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text1"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text3"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text4"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text5"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text6"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text7"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text8"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text9"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_STORED, field("long0"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("long0"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_STORED, field("long1"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("long1"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_STORED, field("long2"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("long2"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_STORED, field("long3"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("long3"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_STORED, field("long4"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("long4"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_STORED, field("long5"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("long5"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_STORED, field("long6"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("long6"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_STORED, field("long7"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("long7"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_STORED, field("long8"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("long8"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_STORED, field("long9"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("long9"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_STORED, field("bool0"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("bool0"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_STORED, field("bool1"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("bool1"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_STORED, field("bool2"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("bool2"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_STORED, field("bool3"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("bool3"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_STORED, field("bool4"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("bool4"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_STORED, field("bool5"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("bool5"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_STORED, field("bool6"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("bool6"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_STORED, field("bool7"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("bool7"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_STORED, field("bool8"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("bool8"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_STORED, field("bool9"))),
                    field("many").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("bool9")))),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of(
                    LuceneIndexOptions.LUCENE_ANALYZER_NAME_OPTION, SynonymAnalyzer.QueryOnlySynonymAnalyzerFactory.ANALYZER_FACTORY_NAME,
                    LuceneIndexOptions.TEXT_SYNONYM_SET_NAME_OPTION, COMBINED_SYNONYM_SETS));

    protected static final Index JOINED_COMPLEX_MAP_TEXT_INDEXES =
            new Index("JoinedComplex$map_textIndexes", new GroupingKeyExpression(
                    concat(
                            field("map").nest(field("entry", KeyExpression.FanType.FanOut).nest(concat(keys))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("is_seen"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("score"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("group"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("timestamp")))
                    ), 8), LuceneIndexTypes.LUCENE,
                    ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME,
                            LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_V2_ENABLED, "true"));

    protected static final Index JOINED_COMPLEX_MULTIPLE_TEXT_INDEXES_NO_FREQS_POSITIONS =
            new Index("JoinedComplex$text_multipleIndexes",
                    concat(
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("is_seen"))),
                            field("simple").nest(function(LuceneFunctionNames.LUCENE_TEXT, concat(field("text"),
                                    function(LuceneFunctionNames.LUCENE_AUTO_COMPLETE_FIELD_INDEX_OPTIONS, value(LuceneFunctionNames.LuceneFieldIndexOptions.DOCS.name())),
                                    function(LuceneFunctionNames.LUCENE_FULL_TEXT_FIELD_INDEX_OPTIONS, value(LuceneFunctionNames.LuceneFieldIndexOptions.DOCS.name()))))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("score"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("group"))),
                            field("complex").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("timestamp")))
                    ), LuceneIndexTypes.LUCENE,
                    ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME,
                            LuceneIndexOptions.LUCENE_ANALYZER_NAME_OPTION, SynonymAnalyzer.QueryOnlySynonymAnalyzerFactory.ANALYZER_FACTORY_NAME,
                            LuceneIndexOptions.TEXT_SYNONYM_SET_NAME_OPTION, EnglishSynonymMapConfig.ExpandedEnglishSynonymMapConfig.CONFIG_NAME,
                            LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED, "true"));

    public static Stream<IndexedType> luceneIndexMapParams() {
        return Stream.of(IndexedType.values());
    }

    protected static Index getMapOnValueIndexWithOption(@Nonnull String name, @Nonnull ImmutableMap<String, String> options) {
        return new Index(
                name,
                new GroupingKeyExpression(field("entry", KeyExpression.FanType.FanOut).nest(concat(keys)), 3),
                LuceneIndexTypes.LUCENE,
                options);
    }

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
                Debugger.setDebugger(DebuggerWithSymbolTables.withSanityChecks());
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

    public static TestRecordsTextProto.ComplexDocument createComplexDocument(long docId, String text, String text2, long group, Integer score, boolean isSeen, double time) {
        TestRecordsTextProto.ComplexDocument.Builder builder = TestRecordsTextProto.ComplexDocument.newBuilder()
                .setDocId(docId)
                .setText(text)
                .setText2(text2)
                .setGroup(group)
                .setIsSeen(isSeen)
                .setTime(time);
        if (score != null) {
            builder.setScore(score);
        }
        return builder.build();
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

    static TestRecordsTextProto.ManyFieldsDocument createManyFieldsDocument(long docId, String text, long number, boolean bool) {
        return TestRecordsTextProto.ManyFieldsDocument.newBuilder()
                .setDocId(docId)
                .setText0(text)
                .setText1(text)
                .setText2(text)
                .setText3(text)
                .setText4(text)
                .setText5(text)
                .setText6(text)
                .setText7(text)
                .setText8(text)
                .setText9(text)
                .setLong0(number)
                .setLong1(number)
                .setLong2(number)
                .setLong3(number)
                .setLong4(number)
                .setLong5(number)
                .setLong6(number)
                .setLong7(number)
                .setLong8(number)
                .setLong9(number)
                .setBool0(bool)
                .setBool1(bool)
                .setBool2(bool)
                .setBool3(bool)
                .setBool4(bool)
                .setBool5(bool)
                .setBool6(bool)
                .setBool7(bool)
                .setBool8(bool)
                .setBool9(bool)
                .build();
    }

    protected static TestRecordsTextProto.MapDocument createMultiEntryMapDoc(long docId, String text, String text2, String text3,
                                                                             String text4, int group) {
        return TestRecordsTextProto.MapDocument.newBuilder()
                .setDocId(docId)
                .setGroup(group)
                .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder()
                        .setKey(text2)
                        .setValue(text)
                        .setSecondValue("firstEntrySecondValue")
                        .setThirdValue("firstEntryThirdValue"))
                .addEntry(TestRecordsTextProto.MapDocument.Entry.newBuilder()
                        .setKey(text4)
                        .setValue(text3)
                        .setSecondValue("secondEntrySecondValue")
                        .setThirdValue("secondEntryThirdValue"))
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

    public static void rebalancePartitions(final FDBRecordStore recordStore, final Index index) throws Exception {
        LuceneIndexMaintainer indexMaintainer = (LuceneIndexMaintainer)recordStore.getIndexMaintainer(index);
        try {
            indexMaintainer.rebalancePartitions().get();
        } catch (ExecutionException e) {
            if (e.getCause() != null) {
                // strip the wrapper to get to the actual cause of the exception
                throw (Exception)e.getCause();
            } else {
                throw e;
            }
        }
    }

    /**
     * A testing analyzer factory to verify the logic for {@link AnalyzerChooser}.
     */
    @AutoService(LuceneAnalyzerFactory.class)
    public static class TestAnalyzerFactory implements LuceneAnalyzerFactory {
        protected static final String ANALYZER_FACTORY_NAME = "TEST_ANALYZER";

        @Override
        @Nonnull
        public String getName() {
            return ANALYZER_FACTORY_NAME;
        }

        @Override
        @Nonnull
        public LuceneAnalyzerType getType() {
            return LuceneAnalyzerType.FULL_TEXT;
        }

        @Override
        @Nonnull
        public AnalyzerChooser getIndexAnalyzerChooser(@Nonnull Index index) {
            return new TestAnalyzerChooser();
        }
    }

    private static class TestAnalyzerChooser implements AnalyzerChooser {
        @Override
        @Nonnull
        public LuceneAnalyzerWrapper chooseAnalyzer(@Nonnull List<String> texts) {
            if (texts.stream().anyMatch(t -> t.contains("synonym"))) {
                return new LuceneAnalyzerWrapper("TEST_SYNONYM",
                        new SynonymAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET, EnglishSynonymMapConfig.ExpandedEnglishSynonymMapConfig.CONFIG_NAME));
            } else {
                return new LuceneAnalyzerWrapper("TEST_NGRAM",
                        new NgramAnalyzer(EnglishAnalyzer.ENGLISH_STOP_WORDS_SET, 3, 30, false));
            }
        }
    }

    /**
     * Whether to test with synthetic record types or standard record types.
     */
    public enum IndexedType {
        Synthetic(Map.ofEntries(
                Map.entry(SIMPLE_TEXT_SUFFIXES_KEY, JOINED_COMPLEX_MULTIPLE_TEXT_INDEXES),
                Map.entry(MANY_FIELDS_INDEX_KEY, JOINED_MANY_FIELDS_INDEX),
                Map.entry(TEXT_AND_BOOLEAN_INDEX_KEY, JOINED_COMPLEX_MULTIPLE_TEXT_INDEXES),
                Map.entry(TEXT_AND_NUMBER_INDEX_KEY, JOINED_COMPLEX_MULTIPLE_TEXT_INDEXES),
                Map.entry(SIMPLE_TEXT_WITH_AUTO_COMPLETE_KEY, JOINED_COMPLEX_MULTIPLE_TEXT_INDEXES),
                Map.entry(EMAIL_CJK_SYM_TEXT_WITH_AUTO_COMPLETE_KEY, JOINED_COMPLEX_MULTIPLE_EMAIL_CJK_SYM_INDEXES),
                Map.entry(MAP_ON_VALUE_INDEX_KEY, JOINED_COMPLEX_MAP_TEXT_INDEXES),
                Map.entry(SIMPLE_TEXT_SUFFIXES_WITH_PRIMARY_KEY_SEGMENT_INDEX_KEY, JOINED_COMPLEX_MULTIPLE_TEXT_PRIMARY_KEY_SEGMENT_INDEXES),
                Map.entry(COMPLEX_MULTIPLE_TEXT_INDEXES_KEY, JOINED_COMPLEX_MULTIPLE_TEXT_INDEXES),
                Map.entry(COMPLEX_GROUPED_WITH_PRIMARY_KEY_SEGMENT_INDEX_KEY, JOINED_COMPLEX_GROUPED_WITH_PRIMARY_KEY_SEGMENT_INDEX),
                Map.entry(QUERY_ONLY_SYNONYM_LUCENE_INDEX_KEY, JOINED_SYNONYM_ONLY_COMPLEX_MULTIPLE_TEXT_INDEXES),
                Map.entry(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX_KEY, JOINED_AUTHORITATIVE_SYNONYM_COMPLEX_MULTIPLE_TEXT_INDEXES),
                Map.entry(QUERY_ONLY_SYNONYM_LUCENE_COMBINED_SETS_INDEX_KEY, JOINED_QUERY_ONLY_SYNONYM_LUCENE_COMBINED_SETS_INDEX),
                Map.entry(COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE_KEY, JOINED_COMPLEX_MULTIPLE_TEXT_INDEXES),
                Map.entry(MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE_KEY, JOINED_COMPLEX_MAP_TEXT_INDEXES),
                Map.entry(MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE_EXCLUDED_FIELDS_KEY, JOINED_COMPLEX_MAP_TEXT_INDEXES),
                Map.entry(SIMPLE_TEXT_WITH_AUTO_COMPLETE_NO_FREQS_POSITIONS_KEY, JOINED_COMPLEX_MULTIPLE_TEXT_INDEXES_NO_FREQS_POSITIONS),
                Map.entry(SPELLCHECK_INDEX_KEY, JOINED_COMPLEX_MULTIPLE_TEXT_INDEXES),
                Map.entry(SPELLCHECK_INDEX_COMPLEX_KEY, JOINED_COMPLEX_MULTIPLE_TEXT_INDEXES),
                Map.entry(COMPLEX_MULTIPLE_GROUPED_KEY, JOINED_COMPLEX_GROUPED_WITH_PRIMARY_KEY_SEGMENT_INDEX),
                Map.entry(COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE_KEY, JOINED_COMPLEX_GROUPED_WITH_PRIMARY_KEY_SEGMENT_INDEX),
                Map.entry(ANALYZER_CHOOSER_TEST_LUCENE_INDEX_KEY, JOINED_COMPLEX_MULTIPLE_TEXT_ANALYZER_CHOOSER_INDEXES),
                Map.entry(AUTO_COMPLETE_SIMPLE_LUCENE_INDEX_KEY, JOINED_COMPLEX_MULTIPLE_TEXT_INDEXES)),
                true,
                List.of(1373414429, -542327065, 1373414429, -1751615347, -1644529491, -1644529491, 2019229269)),
        NonSynthetic(Map.ofEntries(
                Map.entry(SIMPLE_TEXT_SUFFIXES_KEY, SIMPLE_TEXT_SUFFIXES),
                Map.entry(MANY_FIELDS_INDEX_KEY, MANY_FIELDS_INDEX),
                Map.entry(TEXT_AND_BOOLEAN_INDEX_KEY, TEXT_AND_BOOLEAN_INDEX),
                Map.entry(TEXT_AND_NUMBER_INDEX_KEY, TEXT_AND_NUMBER_INDEX),
                Map.entry(SIMPLE_TEXT_WITH_AUTO_COMPLETE_KEY, SIMPLE_TEXT_WITH_AUTO_COMPLETE),
                Map.entry(EMAIL_CJK_SYM_TEXT_WITH_AUTO_COMPLETE_KEY, EMAIL_CJK_SYM_TEXT_WITH_AUTO_COMPLETE),
                Map.entry(MAP_ON_VALUE_INDEX_KEY, MAP_ON_VALUE_INDEX),
                Map.entry(SIMPLE_TEXT_SUFFIXES_WITH_PRIMARY_KEY_SEGMENT_INDEX_KEY, SIMPLE_TEXT_SUFFIXES_WITH_PRIMARY_KEY_SEGMENT_INDEX),
                Map.entry(COMPLEX_MULTIPLE_TEXT_INDEXES_KEY, COMPLEX_MULTIPLE_TEXT_INDEXES),
                Map.entry(COMPLEX_GROUPED_WITH_PRIMARY_KEY_SEGMENT_INDEX_KEY, COMPLEX_GROUPED_WITH_PRIMARY_KEY_SEGMENT_INDEX),
                Map.entry(QUERY_ONLY_SYNONYM_LUCENE_INDEX_KEY, QUERY_ONLY_SYNONYM_LUCENE_INDEX),
                Map.entry(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX_KEY, AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX),
                Map.entry(QUERY_ONLY_SYNONYM_LUCENE_COMBINED_SETS_INDEX_KEY, QUERY_ONLY_SYNONYM_LUCENE_COMBINED_SETS_INDEX),
                Map.entry(COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE_KEY, COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE),
                Map.entry(MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE_KEY, MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE),
                Map.entry(MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE_EXCLUDED_FIELDS_KEY, MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE_EXCLUDED_FIELDS),
                Map.entry(SIMPLE_TEXT_WITH_AUTO_COMPLETE_NO_FREQS_POSITIONS_KEY, SIMPLE_TEXT_WITH_AUTO_COMPLETE_NO_FREQS_POSITIONS),
                Map.entry(SPELLCHECK_INDEX_KEY, SPELLCHECK_INDEX),
                Map.entry(SPELLCHECK_INDEX_COMPLEX_KEY, SPELLCHECK_INDEX_COMPLEX),
                Map.entry(COMPLEX_MULTIPLE_GROUPED_KEY, COMPLEX_MULTIPLE_GROUPED),
                Map.entry(COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE_KEY, COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE),
                Map.entry(ANALYZER_CHOOSER_TEST_LUCENE_INDEX_KEY, ANALYZER_CHOOSER_TEST_LUCENE_INDEX),
                Map.entry(AUTO_COMPLETE_SIMPLE_LUCENE_INDEX_KEY, AUTO_COMPLETE_SIMPLE_LUCENE_INDEX)),
                false,
                List.of(1498044543, -417696951, -687982540, -1626985233, -1008465729, 1532371150, -42167700));

        private final Map<String, Index> indexes;
        private final boolean isSynthetic;
        private final List<Integer> planHashes;

        IndexedType(final Map<String, Index> indexes, final boolean isSynthetic, final List<Integer> planHashes) {
            this.indexes = indexes;
            this.isSynthetic = isSynthetic;
            this.planHashes = planHashes;
        }

        public Index getIndex(final String key) {
            return indexes.get(key);
        }

        public boolean isSynthetic() {
            return isSynthetic;
        }

        public List<Integer> getPlanHashes() {
            return planHashes;
        }
    }
}
