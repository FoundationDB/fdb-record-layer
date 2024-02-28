/*
 * LuceneIndexTest.java
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
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.IsolationLevel;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestHelpers;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.TestRecordsTextProto.ComplexDocument;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.lucene.codec.LuceneOptimizedPostingsFormat;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.lucene.directory.FDBLuceneFileReference;
import com.apple.foundationdb.record.lucene.ngram.NgramAnalyzer;
import com.apple.foundationdb.record.lucene.synonym.EnglishSynonymMapConfig;
import com.apple.foundationdb.record.lucene.synonym.SynonymAnalyzer;
import com.apple.foundationdb.record.lucene.synonym.SynonymMapConfig;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.JoinedRecordTypeBuilder;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.RecordType;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.common.text.AllSuffixesTextTokenizer;
import com.apple.foundationdb.record.provider.common.text.TextSamples;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.provider.foundationdb.OnlineIndexer;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyStorage;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.IndexKeyValueToPartialRecord;
import com.apple.foundationdb.record.query.plan.PlannableIndexTypes;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.ScanComparisons;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryFetchFromPartialRecordPlan;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.BooleanSource;
import com.apple.test.RandomizedTestUtils;
import com.apple.test.Tags;
import com.google.auto.service.AutoService;
import com.google.common.base.Verify;
import com.google.common.collect.Comparators;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.lucene.LuceneIndexOptions.INDEX_PARTITION_BY_FIELD_NAME;
import static com.apple.foundationdb.record.lucene.LuceneIndexOptions.INDEX_PARTITION_HIGH_WATERMARK;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.NGRAM_LUCENE_INDEX;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.QUERY_ONLY_SYNONYM_LUCENE_INDEX;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.SIMPLE_TEXT_SUFFIXES;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.createComplexDocument;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.createSimpleDocument;
import static com.apple.foundationdb.record.lucene.LucenePartitioner.PARTITION_META_SUBSPACE;
import static com.apple.foundationdb.record.lucene.LucenePlanMatchers.group;
import static com.apple.foundationdb.record.lucene.LucenePlanMatchers.query;
import static com.apple.foundationdb.record.lucene.LucenePlanMatchers.scanParams;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.metadata.Key.Expressions.function;
import static com.apple.foundationdb.record.metadata.Key.Expressions.recordType;
import static com.apple.foundationdb.record.metadata.Key.Expressions.value;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.COMPLEX_DOC;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.MANY_FIELDS_DOC;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.MAP_DOC;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.SIMPLE_DOC;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexScan;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests for {@code LUCENE} type indexes.
 */
@SuppressWarnings({"resource", "SameParameterValue"})
@Tag(Tags.RequiresFDB)
public class LuceneIndexTest extends FDBRecordStoreTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(LuceneIndexTest.class);
    private static final KeyExpression SIMPLE_TEXT_WITH_AUTO_COMPLETE_STORED_FIELD = function(LuceneFunctionNames.LUCENE_TEXT, field("text"));
    private static final Index SIMPLE_TEXT_WITH_AUTO_COMPLETE = new Index("Simple_with_auto_complete",
            SIMPLE_TEXT_WITH_AUTO_COMPLETE_STORED_FIELD,
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of());

    private static final Index SIMPLE_TEXT_WITH_AUTO_COMPLETE_NO_FREQS_POSITIONS = new Index("Simple_with_auto_complete",
            function(LuceneFunctionNames.LUCENE_TEXT, concat(field("text"),
                    function(LuceneFunctionNames.LUCENE_AUTO_COMPLETE_FIELD_INDEX_OPTIONS, value(LuceneFunctionNames.LuceneFieldIndexOptions.DOCS.name())),
                    function(LuceneFunctionNames.LUCENE_FULL_TEXT_FIELD_INDEX_OPTIONS, value(LuceneFunctionNames.LuceneFieldIndexOptions.DOCS.name())))),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of());

    private static final Index COMPLEX_MULTIPLE_TEXT_INDEXES = new Index("Complex$text_multipleIndexes",
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME));

    private static final List<KeyExpression> COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE_STORED_FIELDS =
            List.of(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_TEXT, field("text2")));

    private static final Index COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE = new Index("Complex$text_multipleIndexes",
            concat(COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE_STORED_FIELDS),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of());

    protected static final Index COMPLEX_MULTIPLE_GROUPED = new Index("Complex$text_multiple_grouped",
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))).groupBy(field("group")),
            LuceneIndexTypes.LUCENE);

    protected static final Index COMPLEX_PARTITIONED = complexPartitionedIndex(Map.of(
            IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME,
            INDEX_PARTITION_BY_FIELD_NAME, "timestamp",
            INDEX_PARTITION_HIGH_WATERMARK, "10"));

    @Nonnull
    private static Index complexPartitionedIndex(final Map<String, String> options) {
        return new Index("Complex$partitioned",
                concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
                        function(LuceneFunctionNames.LUCENE_SORTED, field("timestamp"))).groupBy(field("group")),
                LuceneIndexTypes.LUCENE,
                options);
    }

    protected static final Index COMPLEX_PARTITIONED_NOGROUP = complexPartitionedIndexNoGroup(Map.of(
            IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME,
            INDEX_PARTITION_BY_FIELD_NAME, "timestamp",
            INDEX_PARTITION_HIGH_WATERMARK, "10"));

    @Nonnull
    private static Index complexPartitionedIndexNoGroup(final Map<String, String> options) {
        return new Index("Complex$partitioned_noGroup",
                concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
                       function(LuceneFunctionNames.LUCENE_SORTED, field("timestamp"))),
                LuceneIndexTypes.LUCENE,
                options);
    }

    private static final List<KeyExpression> COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE_STORED_FIELDS = ImmutableList.of(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_TEXT, field("text2")));
    private static final Index COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE = new Index("Complex$text_multiple_grouped_autocomplete",
            concat(COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE_STORED_FIELDS).groupBy(field("group")),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of());


    private static final Index AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX = new Index("synonym_index", function(LuceneFunctionNames.LUCENE_TEXT, field("text")), LuceneIndexTypes.LUCENE,
            ImmutableMap.of(
                    LuceneIndexOptions.LUCENE_ANALYZER_NAME_OPTION, SynonymAnalyzer.AuthoritativeSynonymOnlyAnalyzerFactory.ANALYZER_FACTORY_NAME,
                    LuceneIndexOptions.TEXT_SYNONYM_SET_NAME_OPTION, EnglishSynonymMapConfig.AuthoritativeOnlyEnglishSynonymMapConfig.CONFIG_NAME));

    private static final String COMBINED_SYNONYM_SETS = "COMBINED_SYNONYM_SETS";

    private static final Index QUERY_ONLY_SYNONYM_LUCENE_COMBINED_SETS_INDEX = new Index("synonym_combined_sets_index", function(LuceneFunctionNames.LUCENE_TEXT, field("text")), LuceneIndexTypes.LUCENE,
            ImmutableMap.of(
                    LuceneIndexOptions.LUCENE_ANALYZER_NAME_OPTION, SynonymAnalyzer.QueryOnlySynonymAnalyzerFactory.ANALYZER_FACTORY_NAME,
                    LuceneIndexOptions.TEXT_SYNONYM_SET_NAME_OPTION, COMBINED_SYNONYM_SETS));

    private static final Index SPELLCHECK_INDEX = new Index(
            "spellcheck_index",
            function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
            LuceneIndexTypes.LUCENE,
            Collections.emptyMap());

    private static final Index SPELLCHECK_INDEX_COMPLEX = new Index(
            "spellcheck_index_complex",
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))),
            LuceneIndexTypes.LUCENE,
            Collections.emptyMap());

    private static final List<KeyExpression> lucene_keys = List.of(
            function(LuceneFunctionNames.LUCENE_TEXT, field("value")),
            function(LuceneFunctionNames.LUCENE_TEXT, field("second_value")),
            function(LuceneFunctionNames.LUCENE_TEXT, field("third_value")));

    protected static final List<KeyExpression> MAP_ON_VALUE_INDEX_STORED_FIELDS =
            lucene_keys.stream()
                    .map(key -> field("entry", KeyExpression.FanType.FanOut).nest(key))
                    .collect(ImmutableList.toImmutableList());

    protected static final List<KeyExpression> keys = ImmutableList.copyOf(Iterables.concat(List.of(field("key")), lucene_keys));

    private static final Index TEXT_AND_NUMBER_INDEX = new Index(
            "text_and_number_idx",
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_STORED, field("group"))),
            LuceneIndexTypes.LUCENE,
            Collections.emptyMap());

    private static final Index TEXT_AND_BOOLEAN_INDEX = new Index(
            "text_and_number_idx",
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_STORED, field("is_seen"))),
            LuceneIndexTypes.LUCENE,
            Collections.emptyMap());

    private static final Index MANY_FIELDS_INDEX = new Index(
            "many_fields_idx",
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text0")),
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

    private static final Index ANALYZER_CHOOSER_TEST_LUCENE_INDEX = new Index("analyzer_chooser_test_index", function(LuceneFunctionNames.LUCENE_TEXT, field("text")), LuceneIndexTypes.LUCENE,
            ImmutableMap.of(
                    LuceneIndexOptions.LUCENE_ANALYZER_NAME_OPTION, TestAnalyzerFactory.ANALYZER_FACTORY_NAME));

    private static final Index MULTIPLE_ANALYZER_LUCENE_INDEX = new Index("Complex$multiple_analyzer_autocomplete",
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of(LuceneIndexOptions.LUCENE_ANALYZER_NAME_OPTION, SynonymAnalyzer.QueryOnlySynonymAnalyzerFactory.ANALYZER_FACTORY_NAME,
                    LuceneIndexOptions.LUCENE_ANALYZER_NAME_PER_FIELD_OPTION, "text2:" + NgramAnalyzer.NgramAnalyzerFactory.ANALYZER_FACTORY_NAME));

    private static final Index AUTO_COMPLETE_SIMPLE_LUCENE_INDEX = new Index("Complex$multiple_analyzer_autocomplete",
            concat(function(LuceneFunctionNames.LUCENE_TEXT, field("text")), function(LuceneFunctionNames.LUCENE_TEXT, field("text2"))),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of());

    private static final Index JOINED_INDEX = getJoinedIndex(Map.of(
            INDEX_PARTITION_BY_FIELD_NAME, "complex.timestamp",
            INDEX_PARTITION_HIGH_WATERMARK, "10"));

    @Nonnull
    private static Index getJoinedIndex(final Map<String, String> options) {
        return new Index("joinNestedConcat",
                concat(
                        field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("is_seen"))),
                        field("simple").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text"))),
                        field("complex").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("timestamp")))
                ).groupBy(field("complex").nest("group")), LuceneIndexTypes.LUCENE,
                options);
    }

    private static final Index JOINED_INDEX_NOGROUP = getJoinedIndexNoGroup(Map.of(
            INDEX_PARTITION_BY_FIELD_NAME, "complex.timestamp",
            INDEX_PARTITION_HIGH_WATERMARK, "10"));

    @Nonnull
    private static Index getJoinedIndexNoGroup(final Map<String, String> options) {
        return new Index("joinNestedConcat",
                concat(
                        field("complex").nest(function(LuceneFunctionNames.LUCENE_STORED, field("is_seen"))),
                        field("simple").nest(function(LuceneFunctionNames.LUCENE_TEXT, field("text"))),
                        field("complex").nest(function(LuceneFunctionNames.LUCENE_SORTED, field("timestamp")))
                ), LuceneIndexTypes.LUCENE, options);
    }

    protected static final String ENGINEER_JOKE = "A software engineer, a hardware engineer, and a departmental manager were driving down a steep mountain road when suddenly the brakes on their car failed. The car careened out of control down the road, bouncing off the crash barriers, ground to a halt scraping along the mountainside. The occupants were stuck halfway down a mountain in a car with no brakes. What were they to do?" +
            "'I know,' said the departmental manager. 'Let's have a meeting, propose a Vision, formulate a Mission Statement, define some Goals, and by a process of Continuous Improvement find a solution to the Critical Problems, and we can be on our way.'" +
            "'No, no,' said the hardware engineer. 'That will take far too long, and that method has never worked before. In no time at all, I can strip down the car's braking system, isolate the fault, fix it, and we can be on our way.'" +
            "'Wait, said the software engineer. 'Before we do anything, I think we should push the car back up the road and see if it happens again.'";

    protected static final String WAYLON = "There's always one more way to do things and that's your way, and you have a right to try it at least once.";
    private long timestamp60DaysAgo;
    private long timestamp30DaysAgo;
    private long timestamp29DaysAgo;
    private long yesterday;

    private static Index getMapOnValueIndexWithOption(@Nonnull String name, @Nonnull ImmutableMap<String, String> options) {
        return new Index(
                name,
                new GroupingKeyExpression(field("entry", KeyExpression.FanType.FanOut).nest(concat(keys)), 3),
                LuceneIndexTypes.LUCENE,
                options);
    }

    private static final Index MAP_ON_VALUE_INDEX = getMapOnValueIndexWithOption("Map$entry-value", ImmutableMap.of());

    protected static final Index MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE =
            getMapOnValueIndexWithOption("Map_with_auto_complete$entry-value", ImmutableMap.of());

    protected static final Index MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE_2 =
            new Index(
                    "Map_with_auto_complete$entry-value",
                    new GroupingKeyExpression(field("entry", KeyExpression.FanType.FanOut).nest(concat(keys)), 1),
                    LuceneIndexTypes.LUCENE,
                    ImmutableMap.of());

    private static final Index MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE_EXCLUDED_FIELDS =
            getMapOnValueIndexWithOption("Map_with_auto_complete_excluded_fields$entry-value", ImmutableMap.of());

    private static final Index SIMPLE_TEXT_SUFFIXES_WITH_PRIMARY_KEY_SEGMENT_INDEX = new Index("Simple$text_suffixes_pky",
            function(LuceneFunctionNames.LUCENE_TEXT, field("text")),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME,
                    LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_ENABLED, "true"));

    private static final Index COMPLEX_GROUPED_WITH_PRIMARY_KEY_SEGMENT_INDEX = new Index("Complex$text_pky",
            function(LuceneFunctionNames.LUCENE_TEXT, field("text")).groupBy(field("group")),
            LuceneIndexTypes.LUCENE,
            ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME,
                    LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_ENABLED, "true"));

    protected void openRecordStore(FDBRecordContext context, FDBRecordStoreTestBase.RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsTextProto.getDescriptor());
        metaDataBuilder.getRecordType(COMPLEX_DOC).setPrimaryKey(concatenateFields("group", "doc_id"));
        hook.apply(metaDataBuilder);
        recordStore = getStoreBuilder(context, metaDataBuilder.getRecordMetaData()).createOrOpen();

        PlannableIndexTypes indexTypes = new PlannableIndexTypes(
                Sets.newHashSet(IndexTypes.VALUE, IndexTypes.VERSION),
                Sets.newHashSet(IndexTypes.RANK, IndexTypes.TIME_WINDOW_LEADERBOARD),
                Sets.newHashSet(IndexTypes.TEXT),
                Sets.newHashSet(LuceneIndexTypes.LUCENE)
        );
        planner = new LucenePlanner(recordStore.getRecordMetaData(), recordStore.getRecordStoreState(), indexTypes, recordStore.getTimer());
        planner.setConfiguration(planner.getConfiguration()
                .asBuilder()
                .setPlanOtherAttemptWholeFilter(false)
                .build());
    }

    @Nonnull
    protected FDBRecordStore.Builder getStoreBuilder(@Nonnull FDBRecordContext context, @Nonnull RecordMetaData metaData) {
        return FDBRecordStore.newBuilder()
                .setFormatVersion(FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION) // set to max to test newest features (unsafe for real deployments)
                .setKeySpacePath(path)
                .setContext(context)
                .setMetaDataProvider(metaData);
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

    private TestRecordsTextProto.ManyFieldsDocument createManyFieldsDocument(long docId, String text, long number, boolean bool) {
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

    @Override
    protected RecordLayerPropertyStorage.Builder addDefaultProps(final RecordLayerPropertyStorage.Builder props) {
        return super.addDefaultProps(props)
                .addProp(LuceneRecordContextProperties.LUCENE_INDEX_COMPRESSION_ENABLED, true);
    }

    private LuceneScanBounds fullTextSearch(Index index, String search) {
        return LuceneIndexTestUtils.fullTextSearch(recordStore, index, search, false);
    }

    private LuceneScanBounds specificFieldSearch(Index index, String search, String field) {
        LuceneScanParameters scan = new LuceneScanQueryParameters(
                ScanComparisons.EMPTY,
                new LuceneQuerySearchClause(LuceneQueryType.QUERY, field, search, false));
        return scan.bind(recordStore, index, EvaluationContext.EMPTY);
    }

    private LuceneScanBounds groupedTextSearch(Index index, String search, Object group) {
        return groupedSortedTextSearch(index, search, null, group);
    }

    private LuceneScanBounds groupedSortedTextSearch(Index index, String search, Sort sort, Object group) {
        return LuceneIndexTestValidator.groupedSortedTextSearch(recordStore, index, search, sort, group);
    }

    @Nonnull
    @SuppressWarnings("unused")
    private LuceneScanBounds groupedAutoCompleteBounds(@Nonnull final Index index, @Nonnull final String search,
                                                       @Nonnull final Object group, @Nonnull final Iterable<String> fields) {
        LuceneScanParameters scan = groupedAutoCompleteScanParams(search, group, fields);
        return scan.bind(recordStore, index, EvaluationContext.EMPTY);
    }

    @Nonnull
    protected static LuceneScanParameters groupedAutoCompleteScanParams(@Nonnull final String search,
                                                                        @Nonnull final Object group,
                                                                        @Nonnull final Iterable<String> fields) {
        return new LuceneScanQueryParameters(
                Verify.verifyNotNull(ScanComparisons.from(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, group))),
                new LuceneAutoCompleteQueryClause(search, false, fields));
    }

    @Nonnull
    private LuceneScanBounds autoCompleteBounds(@Nonnull final Index index, @Nonnull final String search,
                                                @Nonnull final Iterable<String> fields) {
        LuceneScanParameters scan = autoCompleteScanParams(search, fields);
        return scan.bind(recordStore, index, EvaluationContext.EMPTY);
    }

    @Nonnull
    private LuceneScanParameters autoCompleteScanParams(@Nonnull final String search,
                                                        @Nonnull final Iterable<String> fields) {
        return new LuceneScanQueryParameters(
                Verify.verifyNotNull(ScanComparisons.EMPTY),
                new LuceneAutoCompleteQueryClause(search, false, fields));
    }

    private LuceneScanBounds spellCheck(Index index, String search) {
        LuceneScanParameters scan = new LuceneScanSpellCheckParameters(
                ScanComparisons.EMPTY,
                search, false);
        return scan.bind(recordStore, index, EvaluationContext.EMPTY);
    }

    private LuceneScanBounds groupedSpellCheck(Index index, String search, Object group) {
        LuceneScanParameters scan = new LuceneScanSpellCheckParameters(
                Verify.verifyNotNull(ScanComparisons.from(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, group))),
                search, false);
        return scan.bind(recordStore, index, EvaluationContext.EMPTY);
    }

    @Nonnull
    private StoreTimer.Counter getCounter(@Nonnull final FDBRecordContext recordContext, @Nonnull final StoreTimer.Event event) {
        return Verify.verifyNotNull(Verify.verifyNotNull(recordContext.getTimer()).getCounter(event));
    }

    @Test
    void basicGroupedPartitionedTest() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, COMPLEX_PARTITIONED);
            recordStore.saveRecord(createComplexDocument(6666L, ENGINEER_JOKE, 1, Instant.now().toEpochMilli()));
            recordStore.saveRecord(createComplexDocument(7777L, ENGINEER_JOKE, 2, Instant.now().toEpochMilli()));
            recordStore.saveRecord(createComplexDocument(8888L, WAYLON, 2, Instant.now().plus(1, ChronoUnit.DAYS).toEpochMilli()));
            recordStore.saveRecord(createComplexDocument(9999L, "hello world!", 1, Instant.now().plus(2, ChronoUnit.DAYS).toEpochMilli()));

            // should find only one match for ENGINEER_JOKE (the other match is in the other group)
            assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(2, 7777L)),
                    recordStore.scanIndex(COMPLEX_PARTITIONED, groupedTextSearch(COMPLEX_PARTITIONED, "text:propose", 2), null, ScanProperties.FORWARD_SCAN));
            assertEquals(1, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());

            // should not find any match for WAYLON in this group (it's in the other group)
            assertIndexEntryPrimaryKeyTuples(Set.of(),
                    recordStore.scanIndex(COMPLEX_PARTITIONED, groupedTextSearch(COMPLEX_PARTITIONED, "text:things", 1), null, ScanProperties.FORWARD_SCAN));
            assertEquals(1, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());

            // now delete ENGINEER_JOKE from group 1, and verify
            recordStore.deleteRecord(Tuple.from(1, 6666L));
            assertIndexEntryPrimaryKeyTuples(Set.of(),
                    recordStore.scanIndex(COMPLEX_PARTITIONED, groupedTextSearch(COMPLEX_PARTITIONED, "text:propose", 1), null, ScanProperties.FORWARD_SCAN));

            final Subspace partition1Subspace = recordStore.indexSubspace(COMPLEX_PARTITIONED).subspace(Tuple.from(1, LucenePartitioner.PARTITION_DATA_SUBSPACE).add(0));
            final Subspace partition2Subspace = recordStore.indexSubspace(COMPLEX_PARTITIONED).subspace(Tuple.from(2, LucenePartitioner.PARTITION_DATA_SUBSPACE).add(0));

            validateSegmentAndIndexIntegrity(COMPLEX_PARTITIONED, partition1Subspace, context, "_0.cfs");
            validateSegmentAndIndexIntegrity(COMPLEX_PARTITIONED, partition2Subspace, context, "_0.cfs");
        }
    }

    @Test
    void basicNonGroupedPartitionedTest() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, COMPLEX_PARTITIONED_NOGROUP);
            recordStore.saveRecord(createComplexDocument(6666L, ENGINEER_JOKE, 1, Instant.now().toEpochMilli()));
            recordStore.saveRecord(createComplexDocument(7777L, ENGINEER_JOKE, 2, Instant.now().toEpochMilli()));
            recordStore.saveRecord(createComplexDocument(8888L, WAYLON, 2, Instant.now().plus(1, ChronoUnit.DAYS).toEpochMilli()));
            recordStore.saveRecord(createComplexDocument(9999L, "hello world!", 1, Instant.now().plus(2, ChronoUnit.DAYS).toEpochMilli()));

            // should find matches for ENGINEER_JOKE
            assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(2, 7777L), Tuple.from(1, 6666L)),
                    recordStore.scanIndex(COMPLEX_PARTITIONED_NOGROUP, fullTextSearch(COMPLEX_PARTITIONED_NOGROUP, "text:propose"), null, ScanProperties.FORWARD_SCAN));
            assertEquals(2, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());

            // now delete ENGINEER_JOKE and verify
            recordStore.deleteRecord(Tuple.from(1, 6666L));
            assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(2, 7777L)),
                    recordStore.scanIndex(COMPLEX_PARTITIONED_NOGROUP, fullTextSearch(COMPLEX_PARTITIONED_NOGROUP, "text:propose"), null, ScanProperties.FORWARD_SCAN));

            final Subspace partition1Subspace = recordStore.indexSubspace(COMPLEX_PARTITIONED_NOGROUP).subspace(Tuple.from(LucenePartitioner.PARTITION_DATA_SUBSPACE).add(0));

            validateSegmentAndIndexIntegrity(COMPLEX_PARTITIONED_NOGROUP, partition1Subspace, context, "_0.cfs");
        }
    }

    static Stream<Pair<Index, Tuple>> dualGroupModeIndexProvider() {
        return Stream.of(Pair.of(COMPLEX_PARTITIONED, Tuple.from(1L)), Pair.of(COMPLEX_PARTITIONED_NOGROUP, Tuple.from()));
    }

    @ParameterizedTest
    @MethodSource(value = {"dualGroupModeIndexProvider"})
    void repartitionGroupedTest(Pair<Index, Tuple> indexAndGroupingKey) throws IOException {
        Index index = indexAndGroupingKey.getLeft();
        Tuple groupingKey = indexAndGroupingKey.getRight();
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 6)
                .build();

        final int totalDocCount = 20;
        Consumer<FDBRecordContext> schemaSetup = context -> rebuildIndexMetaData(context, COMPLEX_DOC, index);
        long docGroupFieldValue = groupingKey.isEmpty() ? 0L : groupingKey.getLong(0);

        // create/save documents
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            long start = Instant.now().toEpochMilli();
            for (int i = 0; i < totalDocCount; i++) {
                recordStore.saveRecord(createComplexDocument(1000L + i, ENGINEER_JOKE, docGroupFieldValue, start + i * 100));
            }
            commit(context);
        }

        // initially, all documents are saved into one partition
        List<LucenePartitionInfoProto.LucenePartitionInfo> partitionInfos = getPartitionMeta(index,
                groupingKey, contextProps, schemaSetup);
        assertEquals(1, partitionInfos.size());
        assertEquals(totalDocCount, partitionInfos.get(0).getCount());

        // run re-partitioning
        explicitMergeIndex(index, contextProps, schemaSetup);
        partitionInfos = getPartitionMeta(index,
                groupingKey, contextProps, schemaSetup);
        // It should first move 6 from the most-recent to a new, older partition, then move 6 again into a partition
        // in between the two
        assertEquals(List.of(6, 6, 8),
                partitionInfos.stream()
                        .sorted(Comparator.comparing(partitionInfo -> Tuple.fromBytes(partitionInfo.getFrom().toByteArray())))
                        .map(LucenePartitionInfoProto.LucenePartitionInfo::getCount)
                        .collect(Collectors.toList()));
        assertEquals(List.of(1, 2, 0),
                partitionInfos.stream()
                        .sorted(Comparator.comparing(partitionInfo -> Tuple.fromBytes(partitionInfo.getFrom().toByteArray())))
                        .map(LucenePartitionInfoProto.LucenePartitionInfo::getId)
                        .collect(Collectors.toList()));

        // run re-partitioning again, nothing should happend, as the first rebalance should have done everything
        explicitMergeIndex(index, contextProps, schemaSetup);
        assertEquals(partitionInfos, getPartitionMeta(index, groupingKey, contextProps, schemaSetup));

        // partition metadata validated above. now validate that the documents have indeed been
        // moved.
        // expected: docs 1000-1005 -> partition 1 (oldest)
        //                1006-1011 -> partition 2 (middle)
        //                1012-1019 -> partition 0 (newest)
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);

            validateDocsInPartition(index, 0, groupingKey, makeKeyTuples(docGroupFieldValue, 1012, 1019), "text:propose");
            validateDocsInPartition(index, 2, groupingKey, makeKeyTuples(docGroupFieldValue, 1006, 1011), "text:propose");
            validateDocsInPartition(index, 1, groupingKey, makeKeyTuples(docGroupFieldValue, 1000, 1005), "text:propose");
        }
    }

    private void validateDocsInPartition(Index index, int partitionId, Tuple groupingKey,
                                         Set<Tuple> expectedPrimaryKeys, final String universalSearch) throws IOException {
        LuceneIndexTestValidator.validateDocsInPartition(recordStore, index, partitionId, groupingKey, expectedPrimaryKeys, universalSearch);
    }

    private Map<Integer, Integer> getSegmentCounts(Index index,
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
                                    getIndexReader(index, partitionInfo.getId(), groupingKey).getContext().leaves().size())
                    ));
        }
    }

    private IndexReader getIndexReader(final Index index, final int partitionId, final Tuple groupingKey) throws IOException {
        return LuceneIndexTestValidator.getIndexReader(recordStore, index, groupingKey, partitionId);
    }

    public static Stream<Arguments> repartitionAndMerge() {
        return Stream.of(2, 3).flatMap(repartitionCount ->
                Stream.of(2).flatMap(mergeSegmentsPerTier ->
                        Stream.of(
                                Arguments.of(COMPLEX_PARTITIONED, Tuple.from(1), repartitionCount, mergeSegmentsPerTier),
                                Arguments.of(COMPLEX_PARTITIONED_NOGROUP, Tuple.from(), repartitionCount, mergeSegmentsPerTier)
                        )));
    }

    @Tag(Tags.Slow)
    @ParameterizedTest
    @MethodSource()
    void repartitionAndMerge(Index index, Tuple groupingKey, int repartitionCount, int mergeSegmentsPerTier) throws IOException {
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, repartitionCount)
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, (double)mergeSegmentsPerTier)
                .build();

        Consumer<FDBRecordContext> schemaSetup = context -> rebuildIndexMetaData(context, COMPLEX_DOC, index);
        long docGroupFieldValue = groupingKey.isEmpty() ? 0L : groupingKey.getLong(0);

        int transactionCount = 100;
        int docsPerTransaction = 2;
        // create/save documents
        long id = 0;
        List<Long> allIds = new ArrayList<>();
        for (int i = 0; i < transactionCount; i++) {
            try (FDBRecordContext context = openContext(contextProps)) {
                schemaSetup.accept(context);
                recordStore.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(false);
                long start = Instant.now().toEpochMilli();
                for (int j = 0; j < docsPerTransaction; j++) {
                    id++;
                    recordStore.saveRecord(createComplexDocument(id, ENGINEER_JOKE, docGroupFieldValue, start + id));
                    allIds.add(id);
                }
                commit(context);
            }
        }

        // we haven't done any merges yet, or repartitioning, so each transaction should be one new segment
        assertEquals(Map.of(0, transactionCount),
                getSegmentCounts(index, groupingKey, contextProps, schemaSetup));

        timer.reset();
        explicitMergeIndex(index, contextProps, schemaSetup);
        final Map<Integer, Integer> segmentCounts = getSegmentCounts(index, groupingKey, contextProps, schemaSetup);
        final int partitionSize = repartitionCount == 3 ? 9 : 10;
        final int partitionCount;
        if (repartitionCount == 3) {
            partitionCount = allIds.size() / partitionSize + 1;
            assertThat(segmentCounts, Matchers.aMapWithSize(partitionCount));
            assertEquals(IntStream.range(0, partitionCount).boxed()
                            .collect(Collectors.toMap(Function.identity(), partitionId -> partitionId == partitionCount - 1 ? 1 : 2)),
                    segmentCounts);
        } else {
            partitionCount = allIds.size() / partitionSize;
            assertThat(segmentCounts, Matchers.aMapWithSize(partitionCount));
            assertEquals(IntStream.range(0, partitionCount).boxed()
                            .collect(Collectors.toMap(Function.identity(), partitionId -> 2)),
                    segmentCounts);
        }

        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            validateDocsInPartition(index, 0, groupingKey,
                    allIds.stream()
                            .skip(repartitionCount == 3 ? 192 : 190)
                            .map(idLong -> Tuple.from(docGroupFieldValue, idLong))
                            .collect(Collectors.toSet()),
                    "text:propose");
            for (int i = 1; i < 20; i++) {
                // 0 should have the newest
                // everyone else should increase
                validateDocsInPartition(index, i, groupingKey,
                        allIds.stream().skip((i - 1) * partitionSize)
                                .limit(partitionSize)
                                .map(idLong -> Tuple.from(docGroupFieldValue, idLong))
                                .collect(Collectors.toSet()),
                        "text:propose");
            }
            List<LucenePartitionInfoProto.LucenePartitionInfo> partitionInfos = getPartitionMeta(index,
                    groupingKey, contextProps, schemaSetup);
            assertEquals(partitionCount, partitionInfos.size());
        }
    }

    static Stream<Arguments> randomizedRepartitionTest() {
        // This has found situations that should have explicit tests:
        //      1. Multiple groups
        //      2. When the size of first partition is exactly highWatermark+repartitionCount
        return Stream.concat(
                Stream.of(
                        // there's not much special about which flags are enabled and the numbers are used, it's just
                        // to make sure we have some variety, and make sure we have a test with each boolean true, and
                        // false.
                        // For partitionHighWatermark vs repartitionCount it is important to have both an even factor,
                        // and not.
                        Arguments.of(true, false, false, 13, 3, 20, 9237590782644L),
                        Arguments.of(true, true, true, 10, 2, 23, -644766138635622644L),
                        Arguments.of(false, true, true, 11, 4, 20, -1089113174774589435L),
                        Arguments.of(false, false, false, 5, 1, 18, 6223372946177329440L)),
                RandomizedTestUtils.randomArguments(random ->
                        Arguments.of(random.nextBoolean(),
                                random.nextBoolean(),
                                random.nextBoolean(),
                                random.nextInt(20) + 2,
                                random.nextInt(10) + 1,
                                0,
                                random.nextLong())));
    }

    @ParameterizedTest
    @MethodSource
    void randomizedRepartitionTest(boolean isGrouped,
                                   boolean isSynthetic,
                                   boolean primaryKeySegmentIndexEnabled,
                                   int partitionHighWatermark,
                                   int repartitionCount,
                                   int minDocumentCount,
                                   long seed) throws IOException {
        Random random = new Random(seed);
        final boolean optimizedStoredFields = random.nextBoolean();
        final Map<String, String> options = Map.of(
                INDEX_PARTITION_BY_FIELD_NAME, isSynthetic ? "complex.timestamp" : "timestamp",
                INDEX_PARTITION_HIGH_WATERMARK, String.valueOf(partitionHighWatermark),
                LuceneIndexOptions.OPTIMIZED_STORED_FIELDS_FORMAT_ENABLED, String.valueOf(optimizedStoredFields),
                LuceneIndexOptions.PRIMARY_KEY_SEGMENT_INDEX_ENABLED, String.valueOf(primaryKeySegmentIndexEnabled));
        LOGGER.info(KeyValueLogMessage.of("Running randomizedRepartitionTest",
                "isGrouped", isGrouped,
                "isSynthetic", isSynthetic,
                "repartitionCount", repartitionCount,
                "options", options,
                "seed", seed));
        Pair<Index, Consumer<FDBRecordContext>> indexConsumerPair = setupIndex(options, isGrouped, isSynthetic);
        final Index index = indexConsumerPair.getLeft();
        Consumer<FDBRecordContext> schemaSetup = indexConsumerPair.getRight();

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, repartitionCount)
                .build();

        // Generate random documents
        Map<Tuple, Map<Tuple, Long>> ids = new HashMap<>();
        final int transactionCount = random.nextInt(15) + 1;
        final long start = Instant.now().toEpochMilli();
        Map<Integer, Set<Long>> allExistingTimestamps = new HashMap<>();
        int i = 0;
        while (i < transactionCount ||
                // keep inserting data until at least two groups have at least minDocumentCount
                ids.entrySet().stream()
                        .map(entry -> entry.getValue().size())
                        .sorted(Comparator.reverseOrder())
                        .limit(2).skip(isGrouped ? 1 : 0).findFirst()
                        .orElse(0) < minDocumentCount) {
            final int docCount = random.nextInt(10) + 1;
            try (FDBRecordContext context = openContext(contextProps)) {
                schemaSetup.accept(context);
                for (int j = 0; j < docCount; j++) {
                    final int group = isGrouped ? random.nextInt(random.nextInt(10) + 1) : 0; // irrelevant if !isGrouped
                    final Tuple groupTuple = isGrouped ? Tuple.from(group) : Tuple.from();
                    final int countInGroup = ids.computeIfAbsent(groupTuple, key -> new HashMap<>()).size();
                    // we currently don't support multiple records with the same timestamp, specifically at the boundaries
                    long timestamp = start + countInGroup + random.nextInt(20) - 5;
                    final Set<Long> existingTimestamps = allExistingTimestamps.computeIfAbsent(group, key -> new HashSet<>());
                    while (!existingTimestamps.add(timestamp)) {
                        timestamp++;
                    }
                    ComplexDocument cd = ComplexDocument.newBuilder()
                            .setGroup(group)
                            .setDocId(1000L + countInGroup)
                            .setIsSeen(true)
                            .setTimestamp(timestamp)
                            .setHeader(ComplexDocument.Header.newBuilder().setHeaderId(1000L - countInGroup))
                            .setText("A word about what I want to say")
                            .build();
                    Tuple primaryKey;
                    if (isSynthetic) {
                        TestRecordsTextProto.SimpleDocument sd = TestRecordsTextProto.SimpleDocument.newBuilder()
                                .setGroup(group)
                                .setDocId(1000L - countInGroup)
                                .setText("Four score and seven years ago our fathers brought forth")
                                .build();
                        final Tuple syntheticRecordTypeKey = recordStore.getRecordMetaData()
                                .getSyntheticRecordType("luceneJoinedPartitionedIdx")
                                .getRecordTypeKeyTuple();
                        primaryKey = Tuple.from(syntheticRecordTypeKey.getItems().get(0),
                                recordStore.saveRecord(cd).getPrimaryKey().getItems(),
                                recordStore.saveRecord(sd).getPrimaryKey().getItems());
                    } else {
                        primaryKey = recordStore.saveRecord(cd).getPrimaryKey();
                    }
                    ids.computeIfAbsent(groupTuple, key -> new HashMap<>()).put(primaryKey, timestamp);
                }
                commit(context);
            }
            i++;
        }

        explicitMergeIndex(index, contextProps, schemaSetup);

        new LuceneIndexTestValidator(() -> openContext(contextProps), context -> {
            schemaSetup.accept(context);
            return recordStore;
        }).validate(index, ids, repartitionCount, isSynthetic ? "simple_text:forth" : "text:about");
    }

    @Test
    void repartitionSyntheticGroupedTest() throws IOException {
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 6)
                .build();

        final int totalDocCount = 20;
        Consumer<FDBRecordContext> schemaSetup = context -> openRecordStore(context, LuceneIndexTest::joinedPartitionedLuceneIndexMetadataHook);

        List<Tuple> ids = new ArrayList<>();
        // create/save documents
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            long start = Instant.now().toEpochMilli();
            for (int i = 0; i < totalDocCount; i++) {
                TestRecordsTextProto.ComplexDocument cd = TestRecordsTextProto.ComplexDocument.newBuilder()
                        .setGroup(42)
                        .setDocId(1000L + i)
                        .setIsSeen(true)
                        .setHeader(ComplexDocument.Header.newBuilder().setHeaderId(1000L - i))
                        .setTimestamp(start + i * 100)
                        .build();
                TestRecordsTextProto.SimpleDocument sd = TestRecordsTextProto.SimpleDocument.newBuilder()
                        .setGroup(42)
                        .setDocId(1000L - i)
                        .setText("Four score and seven years ago our fathers brought forth propose")
                        .build();
                final Tuple syntheticRecordTypeKey = recordStore.getRecordMetaData()
                        .getSyntheticRecordType("luceneJoinedPartitionedIdx")
                        .getRecordTypeKeyTuple();
                ids.add(Tuple.from(syntheticRecordTypeKey.getItems().get(0),
                        recordStore.saveRecord(cd).getPrimaryKey().getItems(),
                        recordStore.saveRecord(sd).getPrimaryKey().getItems()));
            }
            commit(context);
        }

        // initially, all documents are saved into one partition
        final Tuple groupingKey = Tuple.from(42);
        List<LucenePartitionInfoProto.LucenePartitionInfo> partitionInfos = getPartitionMeta(JOINED_INDEX,
                groupingKey, contextProps, schemaSetup);
        assertEquals(1, partitionInfos.size());
        assertEquals(totalDocCount, partitionInfos.get(0).getCount());

        // run re-partitioning
        explicitMergeIndex(JOINED_INDEX, contextProps, schemaSetup);
        partitionInfos = getPartitionMeta(JOINED_INDEX, groupingKey, contextProps, schemaSetup);
        // now there should be 2 partitions: partition 0 with (totalDocCount - 6) docs, and partition 1 with 6 docs

        assertEquals(List.of(6, 6, 8),
                partitionInfos.stream()
                        .sorted(Comparator.comparing(partitionInfo -> Tuple.fromBytes(partitionInfo.getFrom().toByteArray())))
                        .map(LucenePartitionInfoProto.LucenePartitionInfo::getCount)
                        .collect(Collectors.toList()));
        assertEquals(List.of(1, 2, 0),
                partitionInfos.stream()
                        .sorted(Comparator.comparing(partitionInfo -> Tuple.fromBytes(partitionInfo.getFrom().toByteArray())))
                        .map(LucenePartitionInfoProto.LucenePartitionInfo::getId)
                        .collect(Collectors.toList()));

        // re-running shouldn't do anything
        explicitMergeIndex(JOINED_INDEX, contextProps, schemaSetup);
        assertEquals(partitionInfos, getPartitionMeta(JOINED_INDEX, groupingKey, contextProps, schemaSetup));

        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);

            validateDocsInPartition(JOINED_INDEX, 0, groupingKey,
                    Set.copyOf(ids.subList(12, totalDocCount)), "simple_text:propose");
            validateDocsInPartition(JOINED_INDEX, 2, groupingKey,
                    Set.copyOf(ids.subList(6, 12)), "simple_text:propose");
            validateDocsInPartition(JOINED_INDEX, 1, groupingKey,
                    Set.copyOf(ids.subList(0, 6)), "simple_text:propose");
        }
    }

    @ParameterizedTest
    @MethodSource(value = {"dualGroupModeIndexProvider"})
    void optimizedPartitionInsertionTest(Pair<Index, Tuple> indexAndGroupingKey) throws IOException {
        Index index = indexAndGroupingKey.getLeft();
        Tuple groupingKey = indexAndGroupingKey.getRight();
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 6)
                .build();

        final int totalDocCount = 10; // configured index's highwater mark
        Consumer<FDBRecordContext> schemaSetup = context -> rebuildIndexMetaData(context, COMPLEX_DOC, index);
        long docGroupFieldValue = groupingKey.isEmpty() ? 0L : groupingKey.getLong(0);

        // create/save documents
        long start = Instant.now().toEpochMilli();
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            for (int i = 0; i < totalDocCount; i++) {
                recordStore.saveRecord(createComplexDocument(1000L + i, ENGINEER_JOKE, docGroupFieldValue, start + i * 100));
            }
            commit(context);
        }

        // partition 0 should be at capacity now
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            validateDocsInPartition(index, 0, groupingKey, makeKeyTuples(docGroupFieldValue, 1000, 1009), "text:propose");
        }

        // now add 20 documents older than the oldest document in partition 0
        // they should go into partitions 1 and 2
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            for (int i = 0; i < 20; i++) {
                recordStore.saveRecord(createComplexDocument(1000L + totalDocCount + i, ENGINEER_JOKE, docGroupFieldValue, start - i - 1));
            }
            validateDocsInPartition(index, 1, groupingKey, makeKeyTuples(docGroupFieldValue, 1010, 1019), "text:propose");
            validateDocsInPartition(index, 2, groupingKey, makeKeyTuples(docGroupFieldValue, 1020, 1029), "text:propose");
        }
    }

    private void explicitMergeIndex(Index index, RecordLayerPropertyStorage contextProps, Consumer<FDBRecordContext> schemaSetup) {
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                    .setRecordStore(recordStore)
                    .setIndex(index)
                    .setTimer(timer)
                    .build()) {
                indexBuilder.mergeIndex();
            }
        }
    }

    private List<LucenePartitionInfoProto.LucenePartitionInfo> getPartitionMeta(Index index,
                                                                                Tuple groupingKey,
                                                                                RecordLayerPropertyStorage contextProps,
                                                                                Consumer<FDBRecordContext> schemaSetup) {
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            LuceneIndexMaintainer indexMaintainer = (LuceneIndexMaintainer)recordStore.getIndexMaintainer(index);
            return indexMaintainer.getPartitioner().getAllPartitionMetaInfo(groupingKey).join();
        }
    }

    private static void joinedPartitionedLuceneIndexMetadataHook(@Nonnull final RecordMetaDataBuilder metaDataBuilder) {
        metaDataBuilder.addIndex(joinedMetadataHook(metaDataBuilder), JOINED_INDEX);
    }

    private static void joinedPartitionedUngroupedLuceneIndexMetadataHook(@Nonnull final RecordMetaDataBuilder metaDataBuilder) {
        metaDataBuilder.addIndex(joinedMetadataHook(metaDataBuilder), JOINED_INDEX_NOGROUP);
    }

    private static JoinedRecordTypeBuilder joinedMetadataHook(@Nonnull final RecordMetaDataBuilder metaDataBuilder) {
        //set up the joined index
        final JoinedRecordTypeBuilder joined = metaDataBuilder.addJoinedRecordType("luceneJoinedPartitionedIdx");
        joined.addConstituent("complex", "ComplexDocument");
        joined.addConstituent("simple", "SimpleDocument");
        joined.addJoin("simple", field("doc_id"), "complex", field("header").nest("header_id"));
        return joined;

    }

    @Test
    void partitionedJoinedIndexTest() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, LuceneIndexTest::joinedPartitionedUngroupedLuceneIndexMetadataHook);

            TestRecordsTextProto.ComplexDocument cd = TestRecordsTextProto.ComplexDocument.newBuilder()
                    .setGroup(42)
                    .setDocId(5)
                    .setIsSeen(true)
                    .setHeader(ComplexDocument.Header.newBuilder().setHeaderId(143))
                    .setTimestamp(System.currentTimeMillis())
                    .build();
            TestRecordsTextProto.SimpleDocument sd = TestRecordsTextProto.SimpleDocument.newBuilder()
                    .setDocId(143)
                    .setGroup(42)
                    .setText("Four score and seven years ago our fathers brought forth")
                    .build();
            recordStore.saveRecord(cd);
            recordStore.saveRecord(sd);

            String luceneSearch = "simple_text: \"fathers\"";

            QueryComponent filter = new LuceneQueryComponent(luceneSearch, List.of("simple", "complex"));
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("luceneJoinedPartitionedIdx")
                    .setFilter(filter)
                    .setRequiredResults(List.of(Key.Expressions.field("simple").nest("text")))
                    .build();
            final RecordQueryPlan plan = planner.plan(query);
            final List<?> results = plan.execute(recordStore).asList().join();
            assertNotNull(results);
            assertEquals(1, results.size());
        }
    }

    private Pair<Index, Consumer<FDBRecordContext>> setupIndex(Map<String, String> options, boolean isGrouped, boolean isSynthetic) {
        Index index;
        Consumer<FDBRecordContext> schemaSetup;
        if (isGrouped) {
            if (isSynthetic) {
                index = getJoinedIndex(options);
                schemaSetup = context -> openRecordStore(context, metaDataBuilder -> metaDataBuilder.addIndex(joinedMetadataHook(metaDataBuilder), index));
            } else {
                index = complexPartitionedIndex(options);
                schemaSetup = context -> rebuildIndexMetaData(context, COMPLEX_DOC, index);
            }
        } else {
            if (isSynthetic) {
                index = getJoinedIndexNoGroup(options);
                schemaSetup = context -> openRecordStore(context, metaDataBuilder -> metaDataBuilder.addIndex(joinedMetadataHook(metaDataBuilder), index));
            } else {
                index = complexPartitionedIndexNoGroup(options);
                schemaSetup = context -> rebuildIndexMetaData(context, COMPLEX_DOC, index);
            }
        }
        return Pair.of(index, schemaSetup);
    }

    private enum SortType {
        ASCENDING,
        DESCENDING,
        UNSORTED,
    }

    static Stream<Arguments> continuationDuringRepartitioningTest() {
        return Stream.of(true, false)
                .flatMap(grouped -> Stream.of(true, false)
                        .flatMap(uniqueTimestamps -> Stream.of(true, false)
                                .flatMap(synthetic -> Arrays.stream(SortType.values())
                                        .map(sortType -> Arguments.of(grouped, synthetic, uniqueTimestamps, sortType)))));
    }

    @ParameterizedTest(name = "isGrouped: {0}, isSynthetic: {1}, with unique timestamps: {2}, sort type: {3}")
    @MethodSource
    void continuationDuringRepartitioningTest(boolean isGrouped,
                                              boolean isSynthetic,
                                              boolean uniqueTimestamps,
                                              SortType sortType) throws IOException, ExecutionException, InterruptedException {

        final Map<String, String> options = Map.of(
                INDEX_PARTITION_BY_FIELD_NAME, isSynthetic ? "complex.timestamp" : "timestamp",
                INDEX_PARTITION_HIGH_WATERMARK, String.valueOf(10));
        Pair<Index, Consumer<FDBRecordContext>> indexConsumerPair = setupIndex(options, isGrouped, isSynthetic);
        final Index index = indexConsumerPair.getLeft();
        Consumer<FDBRecordContext> schemaSetup = indexConsumerPair.getRight();

        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_REPARTITION_DOCUMENT_COUNT, 6)
                .build();

        final int group = isGrouped ? 1 : 0;
        final Tuple groupTuple = isGrouped ? Tuple.from(group) : Tuple.from();
        final long start = Instant.now().toEpochMilli();
        final String luceneSearch = isSynthetic ? "simple_text:forth" : "text:about";

        final int docCount = 25;
        List<Tuple> primaryKeys = new ArrayList<>();
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            for (int i = 0; i < docCount; i++) {
                ComplexDocument cd = ComplexDocument.newBuilder()
                        .setGroup(group)
                        .setDocId(1000L + i)
                        .setIsSeen(true)
                        .setText("A word about what I want to say")
                        .setTimestamp(uniqueTimestamps ? start + i * 100 : start)
                        .setHeader(ComplexDocument.Header.newBuilder().setHeaderId(1000L - i))
                        .build();
                final Tuple primaryKey;
                if (isSynthetic) {
                    TestRecordsTextProto.SimpleDocument sd = TestRecordsTextProto.SimpleDocument.newBuilder()
                            .setGroup(group)
                            .setDocId(1000L - i)
                            .setText("Four score and seven years ago our fathers brought forth")
                            .build();
                    final Tuple syntheticRecordTypeKey = recordStore.getRecordMetaData()
                            .getSyntheticRecordType("luceneJoinedPartitionedIdx")
                            .getRecordTypeKeyTuple();
                    primaryKey = Tuple.from(syntheticRecordTypeKey.getItems().get(0),
                            recordStore.saveRecord(cd).getPrimaryKey().getItems(),
                            recordStore.saveRecord(sd).getPrimaryKey().getItems());
                } else {
                    primaryKey = recordStore.saveRecord(cd).getPrimaryKey();
                }
                primaryKeys.add(primaryKey);
            }
            commit(context);
        }

        // initially, all documents are saved into one partition
        List<LucenePartitionInfoProto.LucenePartitionInfo> partitionInfos = getPartitionMeta(index,
                groupTuple, contextProps, schemaSetup);
        assertEquals(1, partitionInfos.size());
        assertEquals(docCount, partitionInfos.get(0).getCount());

        byte[] continuation;
        final Sort sort;
        if (sortType == SortType.UNSORTED) {
            sort = null;
        } else if (uniqueTimestamps) {
            sort = new Sort(new SortField(isSynthetic ? "complex_timestamp" : "timestamp", SortField.Type.LONG, sortType == SortType.DESCENDING));
        } else {
            sort = new Sort(new SortField(isSynthetic ? "complex_timestamp" : "timestamp", SortField.Type.LONG, sortType == SortType.DESCENDING),
                    new SortField(LuceneIndexMaintainer.PRIMARY_KEY_SEARCH_NAME, SortField.Type.STRING, sortType == SortType.DESCENDING));
        }

        LuceneScanQueryParameters scan = new LuceneScanQueryParameters(
                isGrouped ? Verify.verifyNotNull(ScanComparisons.from(new Comparisons.SimpleComparison(Comparisons.Type.EQUALS, group))) : ScanComparisons.EMPTY,
                new LuceneQueryMultiFieldSearchClause(LuceneQueryType.QUERY, luceneSearch, false),
                sort,
                null,
                null,
                null);
        LuceneScanQuery scanQuery = scan.bind(recordStore, index, EvaluationContext.EMPTY);

        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);

            RecordCursor<IndexEntry> indexEntryCursor = recordStore.scanIndex(index, scanQuery, null, ExecuteProperties.newBuilder().setReturnedRowLimit(15).build().asScanProperties(false));

            // Get 15 results and continuation
            List<IndexEntry> entries = indexEntryCursor.asList().join();
            assertEquals(15, entries.size());
            RecordCursorResult<IndexEntry> lastResult = indexEntryCursor.onNext().get();
            assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, lastResult.getNoNextReason());
            LuceneContinuationProto.LuceneIndexContinuation parsed = LuceneContinuationProto.LuceneIndexContinuation.parseFrom(lastResult.getContinuation().toBytes());
            // we stopped in partition 0
            assertEquals(0, parsed.getPartitionId());
            final Set<Tuple> expectedKeys;
            if (sortType == SortType.ASCENDING || sortType == SortType.UNSORTED) {
                expectedKeys = Set.copyOf(primaryKeys.subList(0, 15));
            } else {
                expectedKeys = Set.copyOf(primaryKeys.subList(10, 25));
            }

            assertEquals(expectedKeys, entries.stream().map(IndexEntry::getPrimaryKey).collect(Collectors.toSet()));
            continuation = lastResult.getContinuation().toBytes();
        }

        // run re-partitioning
        explicitMergeIndex(index, contextProps, schemaSetup);
        // now there should be 4 partitions:
        //  partition 0: with docs 18 - 24
        //  partition 3: with docs 12 - 17
        //  partition 2: with docs  6 - 11
        //  partition 1: with docs  0 - 5
        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);
            validateDocsInPartition(index, 0, groupTuple, Set.copyOf(primaryKeys.subList(18, 25)), luceneSearch);
            validateDocsInPartition(index, 3, groupTuple, Set.copyOf(primaryKeys.subList(12, 18)), luceneSearch);
            validateDocsInPartition(index, 2, groupTuple, Set.copyOf(primaryKeys.subList(6, 12)), luceneSearch);
            validateDocsInPartition(index, 1, groupTuple, Set.copyOf(primaryKeys.subList(0, 6)), luceneSearch);
        }

        try (FDBRecordContext context = openContext(contextProps)) {
            schemaSetup.accept(context);

            RecordCursor<IndexEntry> indexEntryCursor = recordStore.scanIndex(index, scanQuery, continuation, ExecuteProperties.newBuilder().build().asScanProperties(false));

            // so now we should get the remaining docs
            List<IndexEntry> entries = indexEntryCursor.asList().join();
            final Set<Tuple> expectedKeys;
            final RecordCursorResult<IndexEntry> lastResult = indexEntryCursor.onNext().get();
            final int expectedCount;
            if (sortType == SortType.ASCENDING) {
                expectedKeys = Set.copyOf(primaryKeys.subList(15, 25));
                expectedCount = 10;
            } else if (sortType == SortType.DESCENDING) {
                expectedKeys = Set.copyOf(primaryKeys.subList(0, 10));
                expectedCount = 10;
            } else {
                expectedKeys = Set.copyOf(primaryKeys.subList(0, 18));
                expectedCount = 18;
            }
            assertEquals(expectedCount, entries.size());
            assertEquals(expectedKeys, entries.stream().map(IndexEntry::getPrimaryKey).collect(Collectors.toSet()));
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, lastResult.getNoNextReason());
        }
    }

    @Test
    void simpleCrossPartitionQuery() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, COMPLEX_PARTITIONED);
            setTimestamps();
            createDualPartitionsWithComplexDocs(1);

            // query will return results from both partitions
            assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(1, 0L), Tuple.from(1, 1000L)),
                    recordStore.scanIndex(COMPLEX_PARTITIONED, groupedTextSearch(COMPLEX_PARTITIONED, "text:propose", 1), null, ScanProperties.FORWARD_SCAN));
            assertEquals(2, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());

            commit(context);
        }
    }

    /**
     * test LIMIT spanning partitions.
     */
    @Test
    void testPartitionedLimit() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, COMPLEX_PARTITIONED);
            setTimestamps();
            createDualPartitionsWithComplexDocs(15);

            // get 20 records (15 from partition 1, and 5 from partition 0)
            assertIndexEntryPrimaryKeyTuples(makeKeyTuples(1L, 1000, 1014, 0, 4), recordStore.scanIndex(COMPLEX_PARTITIONED, groupedTextSearch(COMPLEX_PARTITIONED, "text:propose", 1), null, ExecuteProperties.newBuilder().setReturnedRowLimit(20).build().asScanProperties(false)));

            validatePartitionSegmentIntegrity(COMPLEX_PARTITIONED, context, 1, 0, 1);
        }
    }

    /**
     * test SKIP spanning partitions.
     */
    @Test
    void testPartitionedSkip() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, COMPLEX_PARTITIONED);
            setTimestamps();
            createDualPartitionsWithComplexDocs(30);

            // 25 messages from partition 0 (all partition 1 skipped)
            assertIndexEntryPrimaryKeyTuples(makeKeyTuples(1L, 5, 29), recordStore.scanIndex(COMPLEX_PARTITIONED, groupedTextSearch(COMPLEX_PARTITIONED, "text:propose", 1), null, ExecuteProperties.newBuilder().setSkip(35).build().asScanProperties(false)));

            validatePartitionSegmentIntegrity(COMPLEX_PARTITIONED, context, 1, 0, 1);
        }
    }

    /**
     * test skip with limit spanning partitions.
     */
    @Test
    void testPartitionedSkipWithLimit() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, COMPLEX_PARTITIONED);
            setTimestamps();
            createDualPartitionsWithComplexDocs(30);

            // 20 messages from partition 0 (all partition 1 skipped)
            assertIndexEntryPrimaryKeyTuples(makeKeyTuples(1L, 5, 24), recordStore.scanIndex(COMPLEX_PARTITIONED, groupedTextSearch(COMPLEX_PARTITIONED, "text:propose", 1), null, ExecuteProperties.newBuilder().setReturnedRowLimit(20).setSkip(35).build().asScanProperties(false)));

            validatePartitionSegmentIntegrity(COMPLEX_PARTITIONED, context, 1, 0, 1);
        }
    }

    /**
     * test limit with continuation spanning partitions.
     */
    @Test
    void testPartitionedLimitWithContinuation() throws ExecutionException, InterruptedException, InvalidProtocolBufferException {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, COMPLEX_PARTITIONED);
            setTimestamps();
            createDualPartitionsWithComplexDocs(10);

            RecordCursor<IndexEntry> indexEntryCursor = recordStore.scanIndex(COMPLEX_PARTITIONED, groupedTextSearch(COMPLEX_PARTITIONED, "text:propose", 1), null, ExecuteProperties.newBuilder().setReturnedRowLimit(5).build().asScanProperties(false));

            // Get 5 results and continuation
            List<IndexEntry> entries = indexEntryCursor.asList().join();
            assertEquals(5, entries.size());
            assertEquals(5, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            RecordCursorResult<IndexEntry> lastResult = indexEntryCursor.onNext().get();
            assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, lastResult.getNoNextReason());
            LuceneContinuationProto.LuceneIndexContinuation parsed = LuceneContinuationProto.LuceneIndexContinuation.parseFrom(lastResult.getContinuation().toBytes());
            // we stopped in partition 1
            assertEquals(1, parsed.getPartitionId());

            RecordCursor<IndexEntry> indexEntryCursor2 = recordStore.scanIndex(COMPLEX_PARTITIONED, groupedTextSearch(COMPLEX_PARTITIONED, "text:propose", 1), lastResult.getContinuation().toBytes(), ExecuteProperties.newBuilder().setReturnedRowLimit(10).build().asScanProperties(false));
            // Get 10 results and continuation
            List<IndexEntry> entries2 = indexEntryCursor2.asList().join();
            assertEquals(10, entries2.size());
            RecordCursorResult<IndexEntry> lastResult2 = indexEntryCursor2.onNext().get();
            assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, lastResult.getNoNextReason());
            LuceneContinuationProto.LuceneIndexContinuation parsed2 = LuceneContinuationProto.LuceneIndexContinuation.parseFrom(lastResult2.getContinuation().toBytes());
            // we stopped in partition 0
            assertEquals(0, parsed2.getPartitionId());

            // assert we got the right docs
            assertEquals(makeKeyTuples(1L, 1005, 1009, 0, 4), entries2.stream().map(IndexEntry::getPrimaryKey).collect(Collectors.toSet()));

            validatePartitionSegmentIntegrity(COMPLEX_PARTITIONED, context, 1, 0, 1);
        }
    }

    /**
     * test cross partition limit query with multiple scans.
     */
    @Test
    void testPartitionedLimitNeedsMultipleScans() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, COMPLEX_PARTITIONED);
            setTimestamps();
            createDualPartitionsWithComplexDocs(300);

            assertEquals(451, recordStore.scanIndex(COMPLEX_PARTITIONED, groupedTextSearch(COMPLEX_PARTITIONED, "text:propose", 1), null, ExecuteProperties.newBuilder().setReturnedRowLimit(451).build().asScanProperties(false))
                    .getCount().join());
            assertEquals(3, getCounter(context, LuceneEvents.Events.LUCENE_INDEX_SCAN).getCount());
            assertEquals(451, getCounter(context, LuceneEvents.Counts.LUCENE_SCAN_MATCHED_DOCUMENTS).getCount());

            validatePartitionSegmentIntegrity(COMPLEX_PARTITIONED, context, 1, 0, 1);
        }
    }

    /**
     * test cross partition skip over max page size.
     */
    @Test
    void testPartitionedSkipOverMaxPageSize() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, COMPLEX_PARTITIONED);
            setTimestamps();
            createDualPartitionsWithComplexDocs(150);
            assertEquals(99, recordStore.scanIndex(COMPLEX_PARTITIONED, groupedTextSearch(COMPLEX_PARTITIONED, "text:propose", 1), null, ExecuteProperties.newBuilder().setReturnedRowLimit(351).setSkip(201).build().asScanProperties(false))
                    .getCount().join());
            assertEquals(3, getCounter(context, LuceneEvents.Events.LUCENE_INDEX_SCAN).getCount());
            assertEquals(300, getCounter(context, LuceneEvents.Counts.LUCENE_SCAN_MATCHED_DOCUMENTS).getCount());

            validatePartitionSegmentIntegrity(COMPLEX_PARTITIONED, context, 1, 0, 1);
        }
    }

    @Test
    void testPartitionedSorted() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, COMPLEX_PARTITIONED);
            setTimestamps();
            createDualPartitionsWithComplexDocs(10);

            LuceneScanQuery scanQuery = (LuceneScanQuery)groupedSortedTextSearch(COMPLEX_PARTITIONED, "text:propose", new Sort(new SortField("timestamp", SortField.Type.LONG, true)), 1);
            RecordCursor<IndexEntry> cursor = recordStore.scanIndex(COMPLEX_PARTITIONED, scanQuery, null, ExecuteProperties.newBuilder().setReturnedRowLimit(15).build().asScanProperties(false));
            List<IndexEntry> entries = cursor.asList().join();
            assertEquals(15, entries.size());
            assertEquals(15, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());

            List<Long> timestamps = entries.stream().map(a -> ((FieldDoc)((LuceneRecordCursor.ScoreDocIndexEntry)a).getScoreDoc()).fields[0]).map(Long.class::cast).collect(Collectors.toList());
            Comparator<Long> comparator = Long::compareTo;
            assertTrue(Comparators.isInOrder(timestamps, comparator.reversed()));

            validatePartitionSegmentIntegrity(COMPLEX_PARTITIONED, context, 1, 0, 1);
        }
    }

    private Set<Tuple> makeKeyTuples(long group, int... ranges) {
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

    private void setTimestamps() {
        timestamp60DaysAgo = Instant.now().minus(60, ChronoUnit.DAYS).toEpochMilli();
        timestamp30DaysAgo = Instant.now().minus(30, ChronoUnit.DAYS).toEpochMilli();
        timestamp29DaysAgo = Instant.now().minus(29, ChronoUnit.DAYS).toEpochMilli();
        yesterday = Instant.now().minus(1, ChronoUnit.DAYS).toEpochMilli();
    }

    void createDualPartitionsWithComplexDocs(int docCount) {
        // two partitions: 0 and 1. 0 has older messages, 1 has newer.
        // doc keys in 0 start at (1, 0), doc keys in 1 start at (1, 1000)
        createPartitionMetadata(COMPLEX_PARTITIONED, Tuple.from(1L), 0, timestamp60DaysAgo, timestamp30DaysAgo);
        createPartitionMetadata(COMPLEX_PARTITIONED, Tuple.from(1L), 1, timestamp29DaysAgo, yesterday);
        for (int i = 0; i < docCount; i++) {
            recordStore.saveRecord(createComplexDocument(i, ENGINEER_JOKE, 1, ThreadLocalRandom.current().nextLong(timestamp60DaysAgo, timestamp30DaysAgo + 1)));
        }
        for (int i = 0; i < docCount; i++) {
            recordStore.saveRecord(createComplexDocument(1000L + i, ENGINEER_JOKE, 1, ThreadLocalRandom.current().nextLong(timestamp29DaysAgo, yesterday + 1)));
        }
    }

    void createPartitionMetadata(Index index, Tuple groupKey, int partitionId, long fromTimestamp, long toTimestamp) {
        Tuple from = Tuple.from(fromTimestamp).add(Tuple.from(1, 0));
        Tuple to = Tuple.from(toTimestamp).add(Tuple.from(1, Long.MAX_VALUE));
        LucenePartitionInfoProto.LucenePartitionInfo partitionInfo = LucenePartitionInfoProto.LucenePartitionInfo.newBuilder()
                .setCount(0)
                .setFrom(ByteString.copyFrom(from.pack()))
                .setTo(ByteString.copyFrom(to.pack()))
                .setId(partitionId)
                .build();

        byte[] primaryKey = recordStore.indexSubspace(index).pack(groupKey.add(PARTITION_META_SUBSPACE).addAll(from));
        recordStore.getContext().ensureActive().set(primaryKey, partitionInfo.toByteArray());
    }

    void validatePartitionSegmentIntegrity(Index index, FDBRecordContext context, long group, int... partitionIds) {
        for (int partitionId : partitionIds) {
            final Subspace subspace = recordStore.indexSubspace(COMPLEX_PARTITIONED).subspace(Tuple.from(group, LucenePartitioner.PARTITION_DATA_SUBSPACE).add(partitionId));
            validateSegmentAndIndexIntegrity(index, subspace, context, "_0.cfs");
        }
    }

    @Test
    void simpleInsertAndSearch() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "\"propose a Vision\""), null, ScanProperties.FORWARD_SCAN));
            assertEquals(1, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());

            validateSegmentAndIndexIntegrity(SIMPLE_TEXT_SUFFIXES, recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs");
        }
    }

    @Test
    void largeMetadataTest() {
        // Test a document with many fields, where the field metadata is larger than a data block
        try (FDBRecordContext context = openContext()) {

            rebuildIndexMetaData(context, MANY_FIELDS_DOC, MANY_FIELDS_INDEX);
            recordStore.saveRecord(createManyFieldsDocument(1623L, "propose a Vision", 1L, true));
            recordStore.saveRecord(createManyFieldsDocument(1547L, "different smoochies", 2L, false));

            assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(1623L)),
                    recordStore.scanIndex(MANY_FIELDS_INDEX, fullTextSearch(MANY_FIELDS_INDEX, "text0:Vision AND bool0: true"), null, ScanProperties.FORWARD_SCAN));
            assertEquals(1, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());

            validateSegmentAndIndexIntegrity(MANY_FIELDS_INDEX, recordStore.indexSubspace(MANY_FIELDS_INDEX), context, "_0.cfs");
        }
    }

    /**
     * Make sure the text search for individual fields is not confused when there are multiple fields in the
     * fieldsFormat schema.
     * Fields are overlapping (0 and 1).
     */
    @Test
    void differentFieldSearch() {
        try (FDBRecordContext context = openContext()) {

            rebuildIndexMetaData(context, MANY_FIELDS_DOC, MANY_FIELDS_INDEX);
            TestRecordsTextProto.ManyFieldsDocument doc1 = TestRecordsTextProto.ManyFieldsDocument.newBuilder()
                    .setDocId(11L)
                    .setText0("matching text for field 0 pineapple")
                    .setText1("non matching text for field 1 orange")
                    .build();
            TestRecordsTextProto.ManyFieldsDocument doc2 = TestRecordsTextProto.ManyFieldsDocument.newBuilder()
                    .setDocId(387L)
                    .setText0("non matching text for field 0 orange")
                    .setText1("matching text for field 1 pineapple")
                    .build();
            recordStore.saveRecord(doc1);
            recordStore.saveRecord(doc2);

            // Make sure the text search for individual fields
            assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(11)),
                    recordStore.scanIndex(MANY_FIELDS_INDEX, fullTextSearch(MANY_FIELDS_INDEX, "text0:pineapple"), null, ScanProperties.FORWARD_SCAN));
            assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(387)),
                    recordStore.scanIndex(MANY_FIELDS_INDEX, fullTextSearch(MANY_FIELDS_INDEX, "text1:pineapple"), null, ScanProperties.FORWARD_SCAN));
        }
    }

    /**
     * Make sure the text search for individual fields is not confused when there are multiple fields in the
     * fieldsFormat schema.
     * This test has no overlap in the fields (0/1 vs 3/4).
     */
    @Test
    void differentFieldSearchNoOverlap() {
        try (FDBRecordContext context = openContext()) {

            rebuildIndexMetaData(context, MANY_FIELDS_DOC, MANY_FIELDS_INDEX);
            TestRecordsTextProto.ManyFieldsDocument doc1 = TestRecordsTextProto.ManyFieldsDocument.newBuilder()
                    .setDocId(11L)
                    .setText0("matching text for field 0 pineapple")
                    .setText1("non matching text for field 1 orange")
                    .build();
            TestRecordsTextProto.ManyFieldsDocument doc2 = TestRecordsTextProto.ManyFieldsDocument.newBuilder()
                    .setDocId(387L)
                    .setText3("non matching text for field 3 orange")
                    .setText4("matching text for field 4 pineapple")
                    .build();
            recordStore.saveRecord(doc1);
            recordStore.saveRecord(doc2);

            // Make sure the text search for individual fields
            assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(11)),
                    recordStore.scanIndex(MANY_FIELDS_INDEX, fullTextSearch(MANY_FIELDS_INDEX, "text0:pineapple"), null, ScanProperties.FORWARD_SCAN));
            assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(387)),
                    recordStore.scanIndex(MANY_FIELDS_INDEX, fullTextSearch(MANY_FIELDS_INDEX, "text4:pineapple"), null, ScanProperties.FORWARD_SCAN));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"Ω", "ç"})
    void insertAndSearchWithSpecialCharacters(String specialCharacter) {
        String baseText = "Do we match special characters like %s, even when its mashed together like %snoSpaces?";
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            //one document when the chars, one without
            recordStore.saveRecord(createSimpleDocument(1623L, String.format(baseText, specialCharacter, specialCharacter), 2));
            recordStore.saveRecord(createSimpleDocument(1547L, String.format(baseText, " ", " "), 1));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, specialCharacter), null, ScanProperties.FORWARD_SCAN));
            assertEquals(1, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());

            validateSegmentAndIndexIntegrity(SIMPLE_TEXT_SUFFIXES, recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs");
        }
    }

    @Test
    void searchTextQueryWithBooleanEquals() {
        /*
         * Check that a point query on a number type and a text match together return the correct result
         */
        try (FDBRecordContext context = openContext()) {

            rebuildIndexMetaData(context, COMPLEX_DOC, TEXT_AND_BOOLEAN_INDEX);
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1623L, ENGINEER_JOKE, "propose a Vision", 2, true));
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1547L, ENGINEER_JOKE, "different smoochies", 2, false));

            assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(2, 1623L)),
                    recordStore.scanIndex(TEXT_AND_BOOLEAN_INDEX, fullTextSearch(TEXT_AND_BOOLEAN_INDEX, "\"propose a Vision\" AND is_seen: true"), null, ScanProperties.FORWARD_SCAN));
            assertEquals(1, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());

            validateSegmentAndIndexIntegrity(TEXT_AND_BOOLEAN_INDEX, recordStore.indexSubspace(TEXT_AND_BOOLEAN_INDEX), context, "_0.cfs");
        }
    }

    @Test
    void searchTextQueryWithBooleanNotEquals() {
        /*
         * Check that a point query on a number type and a text match together return the correct result
         */
        try (FDBRecordContext context = openContext()) {

            rebuildIndexMetaData(context, COMPLEX_DOC, TEXT_AND_BOOLEAN_INDEX);
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1623L, ENGINEER_JOKE, "propose a Vision", 2, true));
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1547L, ENGINEER_JOKE, "different smoochies", 2, false));

            assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(2, 1547L)),
                    recordStore.scanIndex(TEXT_AND_BOOLEAN_INDEX, fullTextSearch(TEXT_AND_BOOLEAN_INDEX, "\"propose a Vision\" AND NOT is_seen: true"), null, ScanProperties.FORWARD_SCAN));
            assertEquals(1, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());

            validateSegmentAndIndexIntegrity(TEXT_AND_BOOLEAN_INDEX, recordStore.indexSubspace(TEXT_AND_BOOLEAN_INDEX), context, "_0.cfs");
        }
    }

    @Test
    void searchTextQueryWithBooleanRange() {
        /*
         * Check that a point query on a number type and a text match together return the correct result
         */
        try (FDBRecordContext context = openContext()) {

            rebuildIndexMetaData(context, COMPLEX_DOC, TEXT_AND_BOOLEAN_INDEX);
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1623L, ENGINEER_JOKE, "propose a Vision", 2, true));
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1547L, ENGINEER_JOKE, "different smoochies", 2, false));

            assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(2, 1623L), Tuple.from(2, 1547L)),
                    recordStore.scanIndex(TEXT_AND_BOOLEAN_INDEX, fullTextSearch(TEXT_AND_BOOLEAN_INDEX, "\"propose a Vision\" AND is_seen: [false TO true]"), null, ScanProperties.FORWARD_SCAN));
            assertEquals(2, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());

            validateSegmentAndIndexIntegrity(TEXT_AND_BOOLEAN_INDEX, recordStore.indexSubspace(TEXT_AND_BOOLEAN_INDEX), context, "_0.cfs");
        }
    }

    @Test
    void searchTextQueryWithBooleanBoth() {
        /*
         * Check that a point query on a number type and a text match together return the correct result
         */
        try (FDBRecordContext context = openContext()) {

            rebuildIndexMetaData(context, COMPLEX_DOC, TEXT_AND_BOOLEAN_INDEX);
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1623L, ENGINEER_JOKE, "propose a Vision", 2, true));
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1547L, ENGINEER_JOKE, "different smoochies", 2, false));

            assertIndexEntryPrimaryKeyTuples(Set.of(),
                    recordStore.scanIndex(TEXT_AND_BOOLEAN_INDEX, fullTextSearch(TEXT_AND_BOOLEAN_INDEX, "\"propose a Vision\" AND is_seen: true AND is_seen: false"), null, ScanProperties.FORWARD_SCAN));

            validateSegmentAndIndexIntegrity(TEXT_AND_BOOLEAN_INDEX, recordStore.indexSubspace(TEXT_AND_BOOLEAN_INDEX), context, "_0.cfs");
        }
    }

    @Test
    void searchTextQueryWithBooleanEither() {
        /*
         * Check that a point query on a number type and a text match together return the correct result
         */
        try (FDBRecordContext context = openContext()) {

            rebuildIndexMetaData(context, COMPLEX_DOC, TEXT_AND_BOOLEAN_INDEX);
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1623L, ENGINEER_JOKE, "propose a Vision", 2, true));
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1547L, ENGINEER_JOKE, "different smoochies", 2, false));

            assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(2, 1623L), Tuple.from(2, 1547L)),
                    recordStore.scanIndex(TEXT_AND_BOOLEAN_INDEX, fullTextSearch(TEXT_AND_BOOLEAN_INDEX, "\"propose a Vision\" AND (is_seen: true OR is_seen: false)"), null, ScanProperties.FORWARD_SCAN));
            assertEquals(2, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());

            validateSegmentAndIndexIntegrity(TEXT_AND_BOOLEAN_INDEX, recordStore.indexSubspace(TEXT_AND_BOOLEAN_INDEX), context, "_0.cfs");
        }
    }

    @Test
    void searchTextQueryWithNumberEquals() {
        /*
         * Check that a point query on a number type and a text match together return the correct result
         */
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, TEXT_AND_NUMBER_INDEX);
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1547L, ENGINEER_JOKE, 1));
            recordStore.saveRecord(createSimpleDocument(1548L, ENGINEER_JOKE, null));

            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(TEXT_AND_NUMBER_INDEX, fullTextSearch(TEXT_AND_NUMBER_INDEX, "\"propose a Vision\" AND group:2"), null, ScanProperties.FORWARD_SCAN));
            assertEquals(1, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());

            validateSegmentAndIndexIntegrity(TEXT_AND_NUMBER_INDEX, recordStore.indexSubspace(TEXT_AND_NUMBER_INDEX), context, "_0.cfs");
        }
    }

    @Test
    void searchTextWithEmailPrefix() {
        /*
         * Check that a prefix query with an email in it will return the email
         */
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, TEXT_AND_NUMBER_INDEX);
            recordStore.saveRecord(createSimpleDocument(1241L, "{to: aburritoofjoy@tacos.com, from: tacosareevil@badfoodtakes.net}", 1));
            recordStore.saveRecord(createSimpleDocument(1342L, "{to: aburritoofjoy@tacos.com, from: tacosareevil@badfoodtakes.net}", 2));

            assertIndexEntryPrimaryKeys(Set.of(1241L),
                    recordStore.scanIndex(TEXT_AND_NUMBER_INDEX, fullTextSearch(TEXT_AND_NUMBER_INDEX, "aburritoofjoy@tacos.com* AND group: 1"), null, ScanProperties.FORWARD_SCAN));
        }
    }

    @Test
    void searchTextQueryWithNumberRange() {
        /*
         * Check that a range query on a number type and a text match together return the correct result
         */
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, TEXT_AND_NUMBER_INDEX);
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1547L, ENGINEER_JOKE, 1));

            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(TEXT_AND_NUMBER_INDEX, fullTextSearch(TEXT_AND_NUMBER_INDEX, "\"propose a Vision\" AND group:[2 TO 4]"), null, ScanProperties.FORWARD_SCAN));
            assertEquals(1, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());

            validateSegmentAndIndexIntegrity(TEXT_AND_NUMBER_INDEX, recordStore.indexSubspace(TEXT_AND_NUMBER_INDEX), context, "_0.cfs");
        }
    }

    @Test
    void searchTextWithNumberRangeInfinite() {
        /*
         * Check that a range query returns empty if you feed it a range that is logically empty (i.e. (Long.MAX_VALUE,...)
         */
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, TEXT_AND_NUMBER_INDEX);
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1547L, ENGINEER_JOKE, 1));

            //positive infinity
            assertIndexEntryPrimaryKeys(Set.of(),
                    recordStore.scanIndex(TEXT_AND_NUMBER_INDEX, fullTextSearch(TEXT_AND_NUMBER_INDEX, "\"propose a Vision\" AND group:{" + Long.MAX_VALUE + " TO " + Long.MAX_VALUE + "]"), null, ScanProperties.FORWARD_SCAN));


            //negative infinite
            assertIndexEntryPrimaryKeys(Set.of(),
                    recordStore.scanIndex(TEXT_AND_NUMBER_INDEX, fullTextSearch(TEXT_AND_NUMBER_INDEX, "\"propose a Vision\" AND group:[" + Long.MIN_VALUE + " TO " + Long.MIN_VALUE + "}"), null, ScanProperties.FORWARD_SCAN));

            validateSegmentAndIndexIntegrity(TEXT_AND_NUMBER_INDEX, recordStore.indexSubspace(TEXT_AND_NUMBER_INDEX), context, "_0.cfs");
        }
    }

    private static Stream<Arguments> bitsetParams() {
        return Stream.of(
                Arguments.of(0b0, List.of(1623L, 1547L)),
                Arguments.of(0b100, List.of(1623L)),
                Arguments.of(0b10, List.of(1547L)),
                Arguments.of(0b1000, List.of(1623L, 1547L)),
                Arguments.of(0b110, List.of()),
                Arguments.of(0b11000, List.of(1623L, 1547L)),
                Arguments.of(0b1100, List.of(1623L))
        );
    }

    @ParameterizedTest
    @MethodSource("bitsetParams")
    void bitset(int mask, List<Long> expectedResult) {
        /*
         * Check that a bitset_contains query returns the right result given a certain mask
         */
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, TEXT_AND_NUMBER_INDEX);
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 0b11100));
            recordStore.saveRecord(createSimpleDocument(1547L, ENGINEER_JOKE, 0b11010));

            assertIndexEntryPrimaryKeys(
                    expectedResult,
                    recordStore.scanIndex(
                            TEXT_AND_NUMBER_INDEX,
                            fullTextSearch(TEXT_AND_NUMBER_INDEX, "\"propose a Vision\" AND group:BITSET_CONTAINS(" + mask + ")"),
                            null,
                            ScanProperties.FORWARD_SCAN));

            assertIndexEntryPrimaryKeys(
                    expectedResult,
                    recordStore.scanIndex(
                            TEXT_AND_NUMBER_INDEX,
                            fullTextSearch(TEXT_AND_NUMBER_INDEX, "group:BITSET_CONTAINS(" + mask + ")"),
                            null,
                            ScanProperties.FORWARD_SCAN));
        }
    }

    private static Stream<Arguments> bitsetOrParams() {
        return Stream.of(
                Arguments.of(0b0, 0b0, List.of(1623L, 1547L)),
                Arguments.of(0b100, 0b1, List.of(1623L)),
                Arguments.of(0b10, 0b1, List.of(1547L)),
                Arguments.of(0b1000, 0b1000, List.of(1623L, 1547L)),
                Arguments.of(0b100, 0b10, List.of(1623L, 1547L)),
                Arguments.of(0b10000, 0b1000, List.of(1623L, 1547L)),
                Arguments.of(0b1000, 0b100, List.of(1623L, 1547L))
        );
    }

    @ParameterizedTest
    @MethodSource("bitsetOrParams")
    void bitsetOr(int mask1, int mask2, List<Long> expectedResult) {
        /*
         * Check that a bitset_contains query returns the right result given a certain mask
         */
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, TEXT_AND_NUMBER_INDEX);
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 0b11100));
            recordStore.saveRecord(createSimpleDocument(1547L, ENGINEER_JOKE, 0b11010));

            assertIndexEntryPrimaryKeys(
                    expectedResult,
                    recordStore.scanIndex(
                            TEXT_AND_NUMBER_INDEX,
                            fullTextSearch(TEXT_AND_NUMBER_INDEX, "group:BITSET_CONTAINS(" + mask1 + ") OR group:BITSET_CONTAINS(" + mask2 + ")"),
                            null,
                            ScanProperties.FORWARD_SCAN));
        }
    }

    @Test
    void simpleEmptyIndex() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "something"), null, ScanProperties.FORWARD_SCAN)) {
                assertEquals(RecordCursorResult.exhausted(), cursor.getNext());
            }
        }
    }

    @Test
    void simpleEmptyAutoComplete() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_WITH_AUTO_COMPLETE);
            try (RecordCursor<IndexEntry> cursor = recordStore.scanIndex(SIMPLE_TEXT_WITH_AUTO_COMPLETE,
                    autoCompleteBounds(SIMPLE_TEXT_WITH_AUTO_COMPLETE, "something", ImmutableSet.of("text")),
                    null, ScanProperties.FORWARD_SCAN)) {
                assertEquals(RecordCursorResult.exhausted(), cursor.getNext());
            }
        }
    }

    @Test
    void simpleInsertAndSearchNumFDBFetches() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "Vision"), null, ScanProperties.FORWARD_SCAN));
            assertEquals(1, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());

            validateSegmentAndIndexIntegrity(SIMPLE_TEXT_SUFFIXES, recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs");
        }
    }

    @Test
    void testContinuation() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1624L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1625L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1626L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
            LuceneContinuationProto.LuceneIndexContinuation continuation = LuceneContinuationProto.LuceneIndexContinuation.newBuilder()
                    .setDoc(1)
                    .setScore(0.21973526F)
                    .setShard(0)
                    .build();
            assertIndexEntryPrimaryKeys(Set.of(1625L, 1626L),
                    recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "Vision"), continuation.toByteArray(), ScanProperties.FORWARD_SCAN));

            validateSegmentAndIndexIntegrity(SIMPLE_TEXT_SUFFIXES, recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs");
        }
    }

    @Test
    void testNullValue() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            recordStore.saveRecord(createSimpleDocument(1623L, 2));
            recordStore.saveRecord(createSimpleDocument(1632L, ENGINEER_JOKE, 2));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "*:* AND NOT text:[* TO *]"), null, ScanProperties.FORWARD_SCAN));

            validateSegmentAndIndexIntegrity(SIMPLE_TEXT_SUFFIXES, recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs");
        }
    }

    @Test
    void testLimit() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            for (int i = 0; i < 200; i++) {
                recordStore.saveRecord(createSimpleDocument(1623L + i, ENGINEER_JOKE, 2));
            }
            assertEquals(50, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "Vision"), null, ExecuteProperties.newBuilder().setReturnedRowLimit(50).build().asScanProperties(false))
                    .getCount().join());

            validateSegmentAndIndexIntegrity(SIMPLE_TEXT_SUFFIXES, recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs");
        }
    }

    @Test
    void testSkip() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            for (int i = 0; i < 50; i++) {
                recordStore.saveRecord(createSimpleDocument(1623L + i, ENGINEER_JOKE, 2));
            }
            assertEquals(40, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "Vision"), null, ExecuteProperties.newBuilder().setSkip(10).build().asScanProperties(false))
                    .getCount().join());

            validateSegmentAndIndexIntegrity(SIMPLE_TEXT_SUFFIXES, recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs");
        }
    }

    @Test
    void testSkipWithLimit() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            for (int i = 0; i < 50; i++) {
                recordStore.saveRecord(createSimpleDocument(1623L + i, ENGINEER_JOKE, 2));
            }
            assertEquals(40, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "Vision"), null, ExecuteProperties.newBuilder().setReturnedRowLimit(50).setSkip(10).build().asScanProperties(false))
                    .getCount().join());

            validateSegmentAndIndexIntegrity(SIMPLE_TEXT_SUFFIXES, recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs");
        }
    }

    @Test
    void testLimitWithContinuation() throws ExecutionException, InterruptedException {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            for (int i = 0; i < 200; i++) {
                recordStore.saveRecord(createSimpleDocument(1623L + i, ENGINEER_JOKE, 2));
            }
            LuceneContinuationProto.LuceneIndexContinuation continuation = LuceneContinuationProto.LuceneIndexContinuation.newBuilder()
                    .setDoc(151)
                    .setScore(0.0019047183F)
                    .setShard(0)
                    .build();
            assertEquals(48, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "Vision"), continuation.toByteArray(), ExecuteProperties.newBuilder().setReturnedRowLimit(50).build().asScanProperties(false))
                    .getCount().join());

            validateSegmentAndIndexIntegrity(SIMPLE_TEXT_SUFFIXES, recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs");
        }
    }

    @Test
    void testLimitNeedsMultipleScans() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            for (int i = 0; i < 800; i++) {
                recordStore.saveRecord(createSimpleDocument(1623L + i, ENGINEER_JOKE, 2));
            }
            assertEquals(251, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "Vision"), null, ExecuteProperties.newBuilder().setReturnedRowLimit(251).build().asScanProperties(false))
                    .getCount().join());
            assertEquals(2, getCounter(context, LuceneEvents.Events.LUCENE_INDEX_SCAN).getCount());
            assertEquals(251, getCounter(context, LuceneEvents.Counts.LUCENE_SCAN_MATCHED_DOCUMENTS).getCount());

            validateSegmentAndIndexIntegrity(SIMPLE_TEXT_SUFFIXES, recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs");
        }
    }

    @Test
    void testSkipOverMaxPageSize() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            for (int i = 0; i < 251; i++) {
                recordStore.saveRecord(createSimpleDocument(1623L + i, ENGINEER_JOKE, 2));
            }
            assertEquals(50, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "Vision"), null, ExecuteProperties.newBuilder().setReturnedRowLimit(251).setSkip(201).build().asScanProperties(false))
                    .getCount().join());
            assertEquals(2, getCounter(context, LuceneEvents.Events.LUCENE_INDEX_SCAN).getCount());
            assertEquals(251, getCounter(context, LuceneEvents.Counts.LUCENE_SCAN_MATCHED_DOCUMENTS).getCount());

            validateSegmentAndIndexIntegrity(SIMPLE_TEXT_SUFFIXES, recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs");
        }
    }

    @Test
    void testNestedFieldSearch() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, MAP_DOC, MAP_ON_VALUE_INDEX);
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexMapDocument(1623L, ENGINEER_JOKE, "sampleTextSong", 2));
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexMapDocument(1547L, WAYLON, "sampleTextPhrase", 1));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(MAP_ON_VALUE_INDEX, groupedTextSearch(MAP_ON_VALUE_INDEX, "entry_value:Vision", "sampleTextSong"), null, ScanProperties.FORWARD_SCAN));
            assertEquals(1, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());

            validateSegmentAndIndexIntegrity(MAP_ON_VALUE_INDEX, recordStore.indexSubspace(MAP_ON_VALUE_INDEX).subspace(Tuple.from("sampleTextSong")), context, "_0.cfs");
        }
    }

    @Test
    void testGroupedRecordSearch() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, MAP_DOC, MAP_ON_VALUE_INDEX);
            recordStore.saveRecord(createMultiEntryMapDoc(1623L, ENGINEER_JOKE, "sampleTextPhrase", WAYLON, "sampleTextSong", 2));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(MAP_ON_VALUE_INDEX, groupedTextSearch(MAP_ON_VALUE_INDEX, "entry_value:Vision", "sampleTextPhrase"), null, ScanProperties.FORWARD_SCAN));
            assertEquals(1, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());

            validateSegmentAndIndexIntegrity(MAP_ON_VALUE_INDEX, recordStore.indexSubspace(MAP_ON_VALUE_INDEX).subspace(Tuple.from("sampleTextPhrase")), context, "_0.cfs");
        }
    }

    @Test
    void testMultipleFieldSearch() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, COMPLEX_MULTIPLE_TEXT_INDEXES);
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1623L, ENGINEER_JOKE, "john_leach@apple.com", 2));
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1547L, WAYLON, "hering@gmail.com", 2));
            assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(2L, 1623L)),
                    recordStore.scanIndex(COMPLEX_MULTIPLE_TEXT_INDEXES, fullTextSearch(COMPLEX_MULTIPLE_TEXT_INDEXES, "text:\"Vision\" AND text2:\"john_leach@apple.com\""), null, ScanProperties.FORWARD_SCAN));

            validateSegmentAndIndexIntegrity(COMPLEX_MULTIPLE_TEXT_INDEXES, recordStore.indexSubspace(COMPLEX_MULTIPLE_TEXT_INDEXES), context, "_0.cfs");
        }
    }

    @Test
    void testFuzzySearchWithDefaultEdit2() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, COMPLEX_MULTIPLE_TEXT_INDEXES);
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1623L, ENGINEER_JOKE, "john_leach@apple.com", 2));
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1547L, WAYLON, "hering@gmail.com", 2));
            assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(2L, 1623L)),
                    recordStore.scanIndex(COMPLEX_MULTIPLE_TEXT_INDEXES, fullTextSearch(COMPLEX_MULTIPLE_TEXT_INDEXES, "text:\"Vision\" AND text2:jonleach@apple.com~"), null, ScanProperties.FORWARD_SCAN));

            validateSegmentAndIndexIntegrity(COMPLEX_MULTIPLE_TEXT_INDEXES, recordStore.indexSubspace(COMPLEX_MULTIPLE_TEXT_INDEXES), context, "_0.cfs");
        }
    }

    @Test
    void simpleInsertDeleteAndSearch() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1624L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 2));
            assertIndexEntryPrimaryKeys(Set.of(1623L, 1624L),
                    recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "Vision"), null, ScanProperties.FORWARD_SCAN));
            assertTrue(recordStore.deleteRecord(Tuple.from(1624L)));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "Vision"), null, ScanProperties.FORWARD_SCAN));

            validateSegmentAndIndexIntegrity(SIMPLE_TEXT_SUFFIXES, recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs");
        }
    }

    @Test
    void simpleInsertAndSearchSingleTransaction() {
        LuceneOptimizedPostingsFormat.setAllowCheckDataIntegrity(false);
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            // Save one record and try and search for it
            for (int docId = 0; docId < 100; docId++) {
                recordStore.saveRecord(createSimpleDocument(docId, ENGINEER_JOKE, 2));
                assertEquals(docId + 1, recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "formulate"), null, ScanProperties.FORWARD_SCAN)
                        .getCount().join());
            }

            commit(context);
        }
    }

    @Test
    void testCommit() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "Vision"), null, ScanProperties.FORWARD_SCAN));

            validateSegmentAndIndexIntegrity(SIMPLE_TEXT_SUFFIXES, recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs");

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "Vision"), null, ScanProperties.FORWARD_SCAN));

            validateSegmentAndIndexIntegrity(SIMPLE_TEXT_SUFFIXES, recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs");
        }
    }

    @Test
    void testRollback() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "Vision"), null, ScanProperties.FORWARD_SCAN));

            validateSegmentAndIndexIntegrity(SIMPLE_TEXT_SUFFIXES, recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs");

            context.ensureActive().cancel();
        }
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 1));
            assertIndexEntryPrimaryKeys(Collections.emptySet(),
                    recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "Vision"), null, ScanProperties.FORWARD_SCAN));
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "Vision"), null, ScanProperties.FORWARD_SCAN));

            validateSegmentAndIndexIntegrity(SIMPLE_TEXT_SUFFIXES, recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, "_0.cfs");
        }
    }

    @Test
    void testDataLoad() {
        FDBRecordContext context = openContext();
        for (int i = 0; i < 2000; i++) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            String[] randomWords = LuceneIndexTestUtils.generateRandomWords(500);
            final TestRecordsTextProto.SimpleDocument dylan = TestRecordsTextProto.SimpleDocument.newBuilder()
                    .setDocId(i)
                    .setText(randomWords[1])
                    .setGroup(2)
                    .build();
            recordStore.saveRecord(dylan);
            if ((i + 1) % 50 == 0) {
                commit(context);
                context = openContext();
                validateIndexIntegrity(SIMPLE_TEXT_SUFFIXES, recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, null, null);
            }
        }
        context.close();
    }

    @ParameterizedTest
    @BooleanSource
    void testSimpleUpdate(boolean primaryKeySegmentIndexEnabled) throws IOException {
        final Index index = primaryKeySegmentIndexEnabled ? SIMPLE_TEXT_SUFFIXES_WITH_PRIMARY_KEY_SEGMENT_INDEX : SIMPLE_TEXT_SUFFIXES;
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, 3.0)
                .build();
        Set<Tuple> primaryKeys = new HashSet<>();
        for (int i = 0; i < 20; i++) {
            try (FDBRecordContext context = openContext(contextProps)) {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                var existenceCheck = i < 5
                                     ? FDBRecordStoreBase.RecordExistenceCheck.ERROR_IF_EXISTS
                                     : FDBRecordStoreBase.RecordExistenceCheck.ERROR_IF_NOT_EXISTS;
                final TestRecordsTextProto.SimpleDocument record = createSimpleDocument(1000L + i % 5, numbersText(i + 1), null);
                primaryKeys.add(recordStore.saveRecord(record, existenceCheck).getPrimaryKey());
                context.commit();
            }
        }

        if (primaryKeySegmentIndexEnabled) {
            assertThat(timer.getCount(LuceneEvents.Counts.LUCENE_MERGE_SEGMENTS), greaterThan(10));
            assertThat(timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY), equalTo(0));
            assertThat(timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_PRIMARY_KEY), greaterThan(10));
        } else {
            assertThat(timer.getCount(LuceneEvents.Counts.LUCENE_MERGE_SEGMENTS), equalTo(0));
            assertThat(timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY), greaterThan(10));
            assertThat(timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_PRIMARY_KEY), equalTo(0));
        }

        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            assertIndexEntryPrimaryKeys(Set.of(1002L), // 18
                    recordStore.scanIndex(index, fullTextSearch(index, "three"), null, ScanProperties.FORWARD_SCAN));
            assertIndexEntryPrimaryKeys(Set.of(1000L, 1004L),  // 16,20
                    recordStore.scanIndex(index, fullTextSearch(index, "four"), null, ScanProperties.FORWARD_SCAN));
            assertIndexEntryPrimaryKeys(Set.of(),
                    recordStore.scanIndex(index, fullTextSearch(index, "seven"), null, ScanProperties.FORWARD_SCAN));

            if (primaryKeySegmentIndexEnabled) {
                // TODO: Is there a more stable way to check this?
                final LucenePrimaryKeySegmentIndex primaryKeySegmentIndex = getDirectory(index, Tuple.from())
                        .getPrimaryKeySegmentIndex();
                assertEquals(List.of(
                                List.of(1000L, "_q", 2),
                                List.of(1001L, "_q", 0),
                                List.of(1002L, "_o", 0),
                                List.of(1003L, "_q", 1),
                                List.of(1004L, "_r", 0)
                        ),
                        primaryKeySegmentIndex.readAllEntries());
            }
            LuceneIndexTestValidator.validatePrimaryKeySegmentIndex(recordStore, index,
                    Tuple.from(), null, primaryKeys);
        }
    }

    @Test
    void simpleDeleteSegmentIndex() {
        final Index index = SIMPLE_TEXT_SUFFIXES_WITH_PRIMARY_KEY_SEGMENT_INDEX;
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, 3.0)
                .build();
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            recordStore.saveRecord(createSimpleDocument(1623L, ENGINEER_JOKE, 2));
            recordStore.saveRecord(createSimpleDocument(1624L, ENGINEER_JOKE, 2));
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            recordStore.saveRecord(createSimpleDocument(1547L, WAYLON, 2));
            context.commit();
        }
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            assertIndexEntryPrimaryKeys(Set.of(1623L, 1624L),
                    recordStore.scanIndex(index, fullTextSearch(index, "Vision"), null, ScanProperties.FORWARD_SCAN));
            assertTrue(recordStore.deleteRecord(Tuple.from(1624L)));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(index, fullTextSearch(index, "Vision"), null, ScanProperties.FORWARD_SCAN));
            assertThat(timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY), equalTo(0));
            assertThat(timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_PRIMARY_KEY), equalTo(1));
        }
        timer.reset();
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            assertIndexEntryPrimaryKeys(Set.of(1623L, 1624L, 1547L),
                    recordStore.scanIndex(index, fullTextSearch(index, "way"), null, ScanProperties.FORWARD_SCAN));
            assertTrue(recordStore.deleteRecord(Tuple.from(1547L)));
            assertIndexEntryPrimaryKeys(Set.of(1623L, 1624L),
                    recordStore.scanIndex(index, fullTextSearch(index, "way"), null, ScanProperties.FORWARD_SCAN));
            assertThat(timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY), equalTo(0));
            assertThat(timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_PRIMARY_KEY), equalTo(1));
            recordStore.saveRecord(createSimpleDocument(1547L, ENGINEER_JOKE, 2));
            assertIndexEntryPrimaryKeys(Set.of(1623L, 1624L, 1547L),
                    recordStore.scanIndex(index, fullTextSearch(index, "Vision"), null, ScanProperties.FORWARD_SCAN));
        }
    }

    @Test
    void fullDeleteSegmentIndex() throws Exception {
        fullDeleteHelper(indexMaintainer -> {
            final LucenePrimaryKeySegmentIndex primaryKeySegmentIndex1 = indexMaintainer
                    .getDirectory(Tuple.from(), null)
                    .getPrimaryKeySegmentIndex();
            assertEquals(List.of(), primaryKeySegmentIndex1.readAllEntries());
        }, indexMaintainer -> {
            final LucenePrimaryKeySegmentIndex primaryKeySegmentIndex = indexMaintainer
                    .getDirectory(Tuple.from(), null)
                    .getPrimaryKeySegmentIndex();
            assertNotEquals(List.of(), primaryKeySegmentIndex.readAllEntries());
        });
    }

    @Nonnull
    private Index fullDeleteHelper(final TestHelpers.DangerousConsumer<LuceneIndexMaintainer> assertEmpty,
                                   final TestHelpers.DangerousConsumer<LuceneIndexMaintainer> assertNotEmpty) throws Exception {
        final Index index = SIMPLE_TEXT_SUFFIXES_WITH_PRIMARY_KEY_SEGMENT_INDEX;
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, 3.0)
                .build();
        try (FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            assertTrue(recordStore.getIndexDeferredMaintenanceControl().shouldAutoMergeDuringCommit());
            assertEmpty.accept(getIndexMaintainer(index));
        }
        Set<Tuple> primaryKeys = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            try (FDBRecordContext context = openContext(contextProps)) {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                primaryKeys.add(recordStore.saveRecord(createSimpleDocument(1000 + i, ENGINEER_JOKE, 2)).getPrimaryKey());
                primaryKeys.add(recordStore.saveRecord(createSimpleDocument(1010 + i, WAYLON, 2)).getPrimaryKey());
                context.commit();
            }
        }
        try (FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            assertTrue(recordStore.getIndexDeferredMaintenanceControl().shouldAutoMergeDuringCommit());
            assertNotEmpty.accept(getIndexMaintainer(index));
            LuceneIndexTestValidator.validatePrimaryKeySegmentIndex(recordStore, index,
                    Tuple.from(), null, primaryKeys);
        }
        for (int i = 0; i < 4; i++) {
            try (FDBRecordContext context = openContext(contextProps)) {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                assertTrue(recordStore.getIndexDeferredMaintenanceControl().shouldAutoMergeDuringCommit());
                for (int j = 0; j < 5; j++) {
                    final int docId = 1000 + i * 5 + j;
                    recordStore.deleteRecord(Tuple.from(docId));
                    primaryKeys.remove(Tuple.from(docId));
                }
                context.commit();
            }

            try (FDBRecordContext context = openContext(contextProps)) {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                LuceneIndexTestValidator.validatePrimaryKeySegmentIndex(recordStore, index,
                        Tuple.from(), null, primaryKeys);
            }
        }
        // without this Lucene might not cleanup the files for the segments that have no live documents in them
        try (OnlineIndexer indexBuilder = OnlineIndexer.newBuilder()
                .setRecordStore(recordStore)
                .setIndex(index)
                .build()) {
            indexBuilder.mergeIndex();
        }

        try (FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            assertEmpty.accept(getIndexMaintainer(index));
            LuceneIndexTestValidator.validatePrimaryKeySegmentIndex(recordStore, index,
                    Tuple.from(), null, Set.of());
        }
        return index;
    }

    @Test
    void fullDeleteFieldInfos() throws Exception {
        // if we delete all the documents, the FieldInfos should be cleared out
        fullDeleteHelper(indexMaintainer -> {
            FDBDirectory fdbDirectory = indexMaintainer.getDirectory(Tuple.from(), null);
            final var allFieldInfos = fdbDirectory.getFieldInfosStorage().getAllFieldInfos();
            assertEquals(Map.of(), allFieldInfos,
                    () -> String.join(", ", indexMaintainer.getDirectory(Tuple.from(), null).listAll()));
        }, indexMaintainer -> {
            FDBDirectory fdbDirectory = indexMaintainer.getDirectory(Tuple.from(), null);
            final var allFieldInfos = fdbDirectory.getFieldInfosStorage().getAllFieldInfos();
            assertNotEquals(Map.of(), allFieldInfos,
                    () -> String.join(", ", indexMaintainer.getDirectory(Tuple.from(), null).listAll()));
        });
    }

    @Test
    void checkFieldInfoCountingAfterMerge() {
        final Index index = SIMPLE_TEXT_SUFFIXES_WITH_PRIMARY_KEY_SEGMENT_INDEX;
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, 3.0)
                .build();
        int lastFileCount = -1;
        boolean mergeHappened = false;
        for (int i = 0; i < 10; i++) {
            try (FDBRecordContext context = openContext(contextProps)) {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                recordStore.saveRecord(createSimpleDocument(1000 + i, ENGINEER_JOKE, 2));
                recordStore.saveRecord(createSimpleDocument(1010 + i, WAYLON, 2));
                validateIndexIntegrity(SIMPLE_TEXT_SUFFIXES, recordStore.indexSubspace(SIMPLE_TEXT_SUFFIXES), context, null, null);
                final int fileCount = getDirectory(index, Tuple.from()).listAll().length;
                if (fileCount < lastFileCount) {
                    mergeHappened = true;
                }
                lastFileCount = fileCount;
                context.commit();
            }
        }
        assertTrue(mergeHappened);
    }

    @ParameterizedTest
    @BooleanSource
    void testMultipleUpdateSegments(boolean autoMerge) {
        final Index index = COMPLEX_GROUPED_WITH_PRIMARY_KEY_SEGMENT_INDEX;
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, 3.0)
                .build();
        try (FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, COMPLEX_DOC, index);
            for (int i = 0; i < 20; i++) {
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(i, "", "", 0));
            }
            context.commit();
        }
        final long segmentCountBefore;
        try (FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, COMPLEX_DOC, index);
            segmentCountBefore = getSegmentCount(index, Tuple.from(0));
        }
        try (FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, COMPLEX_DOC, index);
            recordStore.getIndexDeferredMaintenanceControl().setAutoMergeDuringCommit(autoMerge);
            for (int i = 0; i < 10; i++) {
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(i, numbersText(i), "", 0));
            }
            context.commit();
        }
        assertThat(timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY), equalTo(0));
        assertThat(timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_PRIMARY_KEY), equalTo(10));
        assertThat(timer.getCount(LuceneEvents.Events.LUCENE_FIND_MERGES), lessThan(5));
        try (FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, COMPLEX_DOC, index);
            final long segmentCountAfter = getSegmentCount(index, Tuple.from(0));
            assertThat(segmentCountAfter - segmentCountBefore, lessThan(5L));
        }
    }

    @Test
    void testGroupedMultipleUpdate() {
        final Index index = COMPLEX_GROUPED_WITH_PRIMARY_KEY_SEGMENT_INDEX;
        final RecordLayerPropertyStorage contextProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_MERGE_SEGMENTS_PER_TIER, 3.0)
                .build();
        for (int g = 0; g < 3; g++) {
            for (int i = 0; i < 5; i++) {
                try (FDBRecordContext context = openContext(contextProps)) {
                    rebuildIndexMetaData(context, COMPLEX_DOC, index);
                    for (int j = 0; j < 10; j++) {
                        int n = i * 10 + j + 1;
                        recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(n, numbersText(n), "", g));
                    }
                    context.commit();
                }
            }
        }
        try (FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, COMPLEX_DOC, index);
            assertEquals(IntStream.rangeClosed(1, 7).mapToObj(i -> Tuple.from(0, i * 7)).collect(Collectors.toSet()),
                    new HashSet<>(recordStore.scanIndex(index, groupedTextSearch(index, "text:seven", 0L), null, ScanProperties.FORWARD_SCAN)
                            .map(IndexEntry::getPrimaryKey).asList().join()));
        }
        try (FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, COMPLEX_DOC, index);
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(49, "not here", "", 0));
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(35, "nor here either", "", 0));
            context.commit();
        }
        try (FDBRecordContext context = openContext(contextProps)) {
            rebuildIndexMetaData(context, COMPLEX_DOC, index);
            assertEquals(Stream.of(1, 2, 3, 4, 6).map(i -> Tuple.from(0, i * 7)).collect(Collectors.toSet()),
                    new HashSet<>(recordStore.scanIndex(index, groupedTextSearch(index, "text:seven", 0L), null, ScanProperties.FORWARD_SCAN)
                            .map(IndexEntry::getPrimaryKey).asList().join()));
            assertEquals(Stream.of(5, 7).map(i -> Tuple.from(0, i * 7)).collect(Collectors.toSet()),
                    new HashSet<>(recordStore.scanIndex(index, groupedTextSearch(index, "text:here", 0L), null, ScanProperties.FORWARD_SCAN)
                            .map(IndexEntry::getPrimaryKey).asList().join()));
        }
        assertThat(timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_QUERY), equalTo(0));
        assertThat(timer.getCount(LuceneEvents.Events.LUCENE_DELETE_DOCUMENT_BY_PRIMARY_KEY), equalTo(2));
    }

    private String numbersText(int i) {
        final String[] nums = {"zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine"};
        return IntStream.range(1, nums.length)
                .filter(n -> i % n == 0)
                .mapToObj(n -> nums[n])
                .collect(Collectors.joining(" "));
    }

    private String matchAll(String... words) {
        return "text:(" +
                Arrays.stream(words).map(word -> "+\"" + word + "\"").collect(Collectors.joining(" AND ")) +
                ")";
    }

    @Test
    void scanWithQueryOnlySynonymIndex() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, QUERY_ONLY_SYNONYM_LUCENE_INDEX);
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "hullo record layer"), null, ScanProperties.FORWARD_SCAN));
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello recor layer", 1));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "hullo record layer"), null, ScanProperties.FORWARD_SCAN));
            // "hullo" is synonym of "hello"
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, matchAll("hullo", "record", "layer")), null, ScanProperties.FORWARD_SCAN));
            // it doesn't match due to the "recor"
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
            assertIndexEntryPrimaryKeys(Collections.emptySet(),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, matchAll("hullo", "recor", "layer")), null, ScanProperties.FORWARD_SCAN));
            // "hullo" is synonym of "hello", and "show" is synonym of "record"
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, matchAll("hullo", "show", "layer")), null, ScanProperties.FORWARD_SCAN));
        }
    }

    @Test
    void scanWithAuthoritativeSynonymOnlyIndex() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX);
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
            assertEquals(1, recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, "hullo record layer"), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello recor layer", 1));
            assertEquals(1, recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, "hullo record layer"), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            // "hullo" is synonym of "hello"
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
            assertEquals(1, recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX,
                            fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, matchAll("hullo", "record", "layer")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            // it doesn't match due to the "recor"
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
            assertEquals(0, recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX,
                            fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, matchAll("hullo", "recor", "layer")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            // "hullo" is synonym of "hello", and "show" is synonym of "record"
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
            assertEquals(1, recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX,
                            fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, matchAll("hullo", "show", "layer")), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
        }
    }

    @Test
    void phraseSearchBasedOnQueryOnlySynonymIndex() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, QUERY_ONLY_SYNONYM_LUCENE_INDEX);
            // Save a document to verify synonym search based on the group {'motivation','motive','need'}
            recordStore.saveRecord(createSimpleDocument(1623L, "I think you need to search with Lucene index", 1));
            // Search for original phrase
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "\"you need to\""), null, ScanProperties.FORWARD_SCAN));
            // Search for phrase with changed order of tokens, no match is expected
            assertIndexEntryPrimaryKeys(Collections.emptySet(),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "\"need you to\""), null, ScanProperties.FORWARD_SCAN));
            // Search for phrase with "motivation" as synonym of "need"
            // "Motivation" is the authoritative term of the group {'motivation','motive','need'}
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "\"you motivation to\""), null, ScanProperties.FORWARD_SCAN));
            // Search for phrase with "motivation" with changed order of tokens, no match is expected
            assertIndexEntryPrimaryKeys(Collections.emptySet(),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "\"motivation you to\""), null, ScanProperties.FORWARD_SCAN));
            // Search for phrase with "motive" as synonym of "need"
            // "Motive" is not the authoritative term of the group {'motivation','motive','need'}
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "\"you motive to\""), null, ScanProperties.FORWARD_SCAN));
            // Search for phrase with "motive" with changed order of tokens, no match is expected
            assertIndexEntryPrimaryKeys(Collections.emptySet(),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "\"motive you to\""), null, ScanProperties.FORWARD_SCAN));
            // Term query rather than phrase query, so match is expected although the order is changed
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "motivation you to"), null, ScanProperties.FORWARD_SCAN));

            // Save a document to verify synonym search based on the group {'departure','going','going away','leaving'}
            // This group contains multi-word term "going away", and also the single-word term "going"
            recordStore.saveRecord(createSimpleDocument(1624L, "He is leaving for New York next week", 1));
            assertIndexEntryPrimaryKeys(Set.of(1624L),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "\"is departure for\""), null, ScanProperties.FORWARD_SCAN));

            // Search for phrase with "going away"
            assertIndexEntryPrimaryKeys(Set.of(1624L),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "\"is going away for\""), null, ScanProperties.FORWARD_SCAN));

            //Search for phrase with "going"
            assertIndexEntryPrimaryKeys(Set.of(1624L),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "\"is going for\""), null, ScanProperties.FORWARD_SCAN));

            // Search for phrase with only "away", no match is expected
            assertIndexEntryPrimaryKeys(Collections.emptySet(),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "\"is away for\""), null, ScanProperties.FORWARD_SCAN));
        }
    }

    @Test
    void phraseSearchBasedOnAuthoritativeSynonymOnlyIndex() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX);
            // Save a document to verify synonym search based on the group {'motivation','motive','need'}
            recordStore.saveRecord(createSimpleDocument(1623L, "I think you need to search with Lucene index", 1));
            // Search for original phrase
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, "\"you need to\""), null, ScanProperties.FORWARD_SCAN));
            // Search for phrase with changed order of tokens, no match is expected
            assertIndexEntryPrimaryKeys(Collections.emptySet(),
                    recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, "\"need you to\""), null, ScanProperties.FORWARD_SCAN));
            // Search for phrase with "motivation" as synonym of "need"
            // "Motivation" is the authoritative term of the group {'motivation','motive','need'}
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, "\"you motivation to\""), null, ScanProperties.FORWARD_SCAN));
            // Search for phrase with "motivation" with changed order of tokens, no match is expected
            assertIndexEntryPrimaryKeys(Collections.emptySet(),
                    recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, "\"motivation you to\""), null, ScanProperties.FORWARD_SCAN));
            // Search for phrase with "motive" as synonym of "need"
            // "Motive" is not the authoritative term of the group {'motivation','motive','need'}
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, "\"you motive to\""), null, ScanProperties.FORWARD_SCAN));
            // Search for phrase with "motive" with changed order of tokens, no match is expected
            assertIndexEntryPrimaryKeys(Collections.emptySet(),
                    recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, "\"motive you to\""), null, ScanProperties.FORWARD_SCAN));
            // Term query rather than phrase query, so match is expected although the order is changed
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, "motivation you to"), null, ScanProperties.FORWARD_SCAN));

            // Save a document to verify synonym search based on the group {'departure','going','going away','leaving'}
            // This group contains multi-word term "going away", and also the single-word term "going"
            recordStore.saveRecord(createSimpleDocument(1624L, "He is leaving for New York next week", 1));
            assertIndexEntryPrimaryKeys(Set.of(1624L),
                    recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, "\"is departure for\""), null, ScanProperties.FORWARD_SCAN));

            // Search for phrase with "going away"
            assertIndexEntryPrimaryKeys(Set.of(1624L),
                    recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, "\"is going away for\""), null, ScanProperties.FORWARD_SCAN));

            //Search for phrase with "going"
            assertIndexEntryPrimaryKeys(Set.of(1624L),
                    recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, "\"is going for\""), null, ScanProperties.FORWARD_SCAN));

            // Search for phrase with only "away", the correct behavior is to return no match. But match is still hit due to the poor handling of positional data for multi-word synonym by this analyzer
            assertIndexEntryPrimaryKeys(Set.of(1624L),
                    recordStore.scanIndex(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, fullTextSearch(AUTHORITATIVE_SYNONYM_ONLY_LUCENE_INDEX, "\"is away for\""), null, ScanProperties.FORWARD_SCAN));
        }
    }


    /**
     * Test config with a combined set of synonyms.
     */
    @AutoService(SynonymMapConfig.class)
    public static class CombinedSynonymSetsConfig implements SynonymMapConfig {
        @Override
        public String getName() {
            return COMBINED_SYNONYM_SETS;
        }

        @Override
        public boolean expand() {
            return true;
        }

        @Override
        public InputStream getSynonymInputStream() {
            InputStream is1 = null;
            InputStream is2 = null;
            try {
                is1 = new EnglishSynonymMapConfig.ExpandedEnglishSynonymMapConfig().getSynonymInputStream();
                is2 = SynonymMapConfig.openFile("test.txt");
                return new SequenceInputStream(is1, is2);
            } catch (RecordCoreException e) {
                try {
                    if (is1 != null) {
                        is1.close();
                    }
                    if (is2 != null) {
                        is2.close();
                    }
                    throw e;
                } catch (IOException ignored) {
                    throw e;
                }
            }
        }
    }

    @Test
    void scanWithCombinedSetsSynonymIndex() {
        // The COMBINED_SYNONYM_SETS adds this extra line to our synonym set:
        // 'synonym', 'nonsynonym'
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, QUERY_ONLY_SYNONYM_LUCENE_INDEX);
                metaDataBuilder.addIndex(SIMPLE_DOC, QUERY_ONLY_SYNONYM_LUCENE_COMBINED_SETS_INDEX);
            });
            recordStore.saveRecord(createSimpleDocument(1623L, "synonym is fun", 1));
            assertIndexEntryPrimaryKeys(Collections.emptySet(),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, matchAll("nonsynonym", "is", "fun")), null, ScanProperties.FORWARD_SCAN));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_COMBINED_SETS_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_COMBINED_SETS_INDEX, matchAll("nonsynonym", "is", "fun")), null, ScanProperties.FORWARD_SCAN));
        }
    }

    @Test
    void proximitySearchOnMultiFieldWithMultiWordSynonym() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, QUERY_ONLY_SYNONYM_LUCENE_INDEX);

            // Both "hello" and "record" have multi-word synonyms
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello FoundationDB record layer", 1));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "\"hello record\"~10"), null, ScanProperties.FORWARD_SCAN));
        }
    }

    @Test
    void proximitySearchOnSpecificFieldWithMultiWordSynonym() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, QUERY_ONLY_SYNONYM_LUCENE_INDEX);

            // Both "hello" and "record" have multi-word synonyms
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello FoundationDB record layer", 1));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, specificFieldSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "\"hello record\"~10", "text"), null, ScanProperties.FORWARD_SCAN));
        }
    }

    @Test
    void scanWithNgramIndex() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, NGRAM_LUCENE_INDEX);
            recordStore.saveRecord(createSimpleDocument(1623L, "Hello record layer", 1));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(NGRAM_LUCENE_INDEX, fullTextSearch(NGRAM_LUCENE_INDEX, "hello record layer"), null, ScanProperties.FORWARD_SCAN));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(NGRAM_LUCENE_INDEX, fullTextSearch(NGRAM_LUCENE_INDEX, "hello"), null, ScanProperties.FORWARD_SCAN));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(NGRAM_LUCENE_INDEX, fullTextSearch(NGRAM_LUCENE_INDEX, "hel"), null, ScanProperties.FORWARD_SCAN));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(NGRAM_LUCENE_INDEX, fullTextSearch(NGRAM_LUCENE_INDEX, "ell"), null, ScanProperties.FORWARD_SCAN));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(NGRAM_LUCENE_INDEX, fullTextSearch(NGRAM_LUCENE_INDEX, "ecord"), null, ScanProperties.FORWARD_SCAN));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(NGRAM_LUCENE_INDEX, fullTextSearch(NGRAM_LUCENE_INDEX, "hel ord aye"), null, ScanProperties.FORWARD_SCAN));
            assertIndexEntryPrimaryKeys(Set.of(1623L),
                    recordStore.scanIndex(NGRAM_LUCENE_INDEX, fullTextSearch(NGRAM_LUCENE_INDEX, "ello record"), null, ScanProperties.FORWARD_SCAN));
            // The term "ella" is not expected
            assertIndexEntryPrimaryKeys(Collections.emptySet(),
                    recordStore.scanIndex(NGRAM_LUCENE_INDEX, fullTextSearch(NGRAM_LUCENE_INDEX, "ella"), null, ScanProperties.FORWARD_SCAN));
        }
    }

    @Test
    void searchForAutoComplete() throws Exception {
        searchForAutoCompleteAndAssert("good", true, false, 1498044543);
    }

    @Test
    void searchForAutoCompleteWithPrefix() throws Exception {
        searchForAutoCompleteAndAssert("goo", true, false, -417696951);
    }

    @Test
    void searchForAutoCompleteWithHighlight() throws Exception {
        searchForAutoCompleteAndAssert("good", true, true, 1498044543);
    }

    /**
     * To verify the suggestion lookup can work correctly if the suggester is never built and no entries exist in the
     * directory.
     */
    @Test
    void searchForAutoCompleteWithLoadingNoRecords() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_WITH_AUTO_COMPLETE);
            });

            assertIndexEntryPrimaryKeys(Collections.emptySet(),
                    recordStore.scanIndex(SIMPLE_TEXT_WITH_AUTO_COMPLETE, autoCompleteBounds(SIMPLE_TEXT_WITH_AUTO_COMPLETE, "hello", ImmutableSet.of("text")), null, ScanProperties.FORWARD_SCAN));
            assertEquals(0, Verify.verifyNotNull(context.getTimer()).getCount(LuceneEvents.Counts.LUCENE_SCAN_MATCHED_AUTO_COMPLETE_SUGGESTIONS));
        }
    }

    @SuppressWarnings("UnstableApiUsage")
    @Test
    void searchForAutoCompleteAcrossMultipleFields() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(COMPLEX_DOC, COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE);
            });

            // Write 8 texts and 6 of them contain the key "good"
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1623L, "Good morning", "", 1));
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1624L, "Good afternoon", "", 1));
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1625L, "good evening", "", 1));
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1626L, "Good night", "", 1));
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1627L, "", "That's really good!", 1));
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1628L, "Good day", "I'm good", 1));
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1629L, "", "Hello Record Layer", 1));
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1630L, "", "Hello FoundationDB!", 1));

            final RecordQueryPlan luceneIndexPlan =
                    LuceneIndexQueryPlan.of(COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE.getName(),
                            autoCompleteScanParams("good", ImmutableSet.of("text", "text2")),
                            RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                            false,
                            null,
                            COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE_STORED_FIELDS);
            assertEquals(-687982540, luceneIndexPlan.planHash(PlanHashable.CURRENT_LEGACY));

            final List<FDBQueriedRecord<Message>> results =
                    recordStore.executeQuery(luceneIndexPlan, null, ExecuteProperties.SERIAL_EXECUTE)
                            .asList().get();

            final ImmutableList<Pair<String, String>> expectedResults = ImmutableList.of(
                    Pair.of("Good day", "I'm good"),
                    Pair.of("Good morning", ""),
                    Pair.of("Good afternoon", ""),
                    Pair.of("good evening", ""),
                    Pair.of("Good night", ""),
                    Pair.of("", "That's really good!")
            );
            Assertions.assertEquals(expectedResults.size(), results.size());

            Assertions.assertTrue(Streams.zip(expectedResults.stream(), results.stream(),
                    (expectedResult, result) -> {
                        final Message record = Verify.verifyNotNull(result.getRecord());
                        final Descriptors.Descriptor descriptor = record.getDescriptorForType();
                        final Descriptors.FieldDescriptor textDescriptor = descriptor.findFieldByName("text");
                        assertEquals(expectedResult.getLeft(), record.getField(textDescriptor));
                        final Descriptors.FieldDescriptor text2Descriptor = descriptor.findFieldByName("text2");
                        assertEquals(expectedResult.getRight(), record.getField(text2Descriptor));
                        return true;
                    }).allMatch(r -> r));

            Subspace subspace = recordStore.indexSubspace(COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE);
            validateSegmentAndIndexIntegrity(COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE, subspace, context, "_0.cfs");

            List<Tuple> primaryKeys = results.stream()
                    .map(FDBQueriedRecord::getIndexEntry)
                    .map(Verify::verifyNotNull)
                    .map(IndexEntry::getPrimaryKey).collect(Collectors.toList());
            assertEquals(ImmutableList.of(Tuple.from(1L, 1628L), Tuple.from(1L, 1623L), Tuple.from(1L, 1624L), Tuple.from(1L, 1625L), Tuple.from(1L, 1626L), Tuple.from(1L, 1627L)), primaryKeys);

            commit(context);
        }
    }

    @Test
    void searchForAutoCompleteWithContinueTyping() throws Exception {
        try (FDBRecordContext context = openContext()) {
            addIndexAndSaveRecordForAutoComplete(context);

            final RecordQueryPlan luceneIndexPlan =
                    LuceneIndexQueryPlan.of(SIMPLE_TEXT_WITH_AUTO_COMPLETE.getName(),
                            autoCompleteScanParams("good mor", ImmutableSet.of("text")),
                            RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                            false,
                            null,
                            ImmutableList.of(SIMPLE_TEXT_WITH_AUTO_COMPLETE_STORED_FIELD));
            assertEquals(-1626985233, luceneIndexPlan.planHash(PlanHashable.CURRENT_LEGACY));
            final List<FDBQueriedRecord<Message>> results =
                    recordStore.executeQuery(luceneIndexPlan, null, ExecuteProperties.SERIAL_EXECUTE)
                            .asList().get();

            // Assert only 1 suggestion returned
            assertEquals(1, results.size());

            // Assert the suggestion's key and field
            final FDBQueriedRecord<Message> result = Iterables.getOnlyElement(results);
            final Message record = result.getRecord();
            final Descriptors.Descriptor descriptor = record.getDescriptorForType();
            final Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("text");
            final String field = Verify.verifyNotNull((String)record.getField(fieldDescriptor));
            assertEquals("Good morning", field);

            IndexEntry entry = result.getIndexEntry();
            Assertions.assertNotNull(entry);
            assertEquals(1623L, entry.getPrimaryKey().get(0));

            assertAutoCompleteEntriesAndSegmentInfoStoredInCompoundFile(SIMPLE_TEXT_WITH_AUTO_COMPLETE, recordStore.indexSubspace(SIMPLE_TEXT_WITH_AUTO_COMPLETE),
                    context, "_0.cfs");

            commit(context);
        }
    }

    @Test
    void searchForAutoCompleteForGroupedRecord() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                TextIndexTestUtils.addRecordTypePrefix(metaDataBuilder);
                metaDataBuilder.addIndex(MAP_DOC, MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE);
            });
            recordStore.saveRecord(createMultiEntryMapDoc(1623L, ENGINEER_JOKE, "sampleTextPhrase", WAYLON, "sampleTextSong", 2));

            final RecordQueryPlan luceneIndexPlan =
                    LuceneIndexQueryPlan.of(MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE.getName(),
                            groupedAutoCompleteScanParams("Vision", "sampleTextPhrase", ImmutableSet.of("entry_value")),
                            RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                            false,
                            null,
                            MAP_ON_VALUE_INDEX_STORED_FIELDS);
            assertEquals(-1008465729, luceneIndexPlan.planHash(PlanHashable.CURRENT_LEGACY));
            final List<FDBQueriedRecord<Message>> results =
                    recordStore.executeQuery(luceneIndexPlan, null, ExecuteProperties.SERIAL_EXECUTE)
                            .asList().get();

            assertEquals(1, results.size());
            final var resultRecord = results.get(0);
            final IndexEntry indexEntry = resultRecord.getIndexEntry();
            assertNotNull(indexEntry);
            final Message message = resultRecord.getRecord();

            Descriptors.Descriptor recordDescriptor = TestRecordsTextProto.MapDocument.getDescriptor();

            Descriptors.FieldDescriptor docIdDescriptor = recordDescriptor.findFieldByName("doc_id");
            assertEquals(1623L, message.getField(docIdDescriptor));

            Descriptors.FieldDescriptor entryDescriptor = recordDescriptor.findFieldByName("entry");
            Message entry = (Message)message.getRepeatedField(entryDescriptor, 0);

            Descriptors.FieldDescriptor keyDescriptor = entryDescriptor.getMessageType().findFieldByName("key");
            Descriptors.FieldDescriptor valueDescriptor = entryDescriptor.getMessageType().findFieldByName("value");

            assertEquals("sampleTextPhrase", entry.getField(keyDescriptor));
            assertEquals(ENGINEER_JOKE, entry.getField(valueDescriptor));

            assertAutoCompleteEntriesAndSegmentInfoStoredInCompoundFile(MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE, recordStore.indexSubspace(MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE).subspace(Tuple.from("sampleTextPhrase")),
                    context, "_0.cfs");

            commit(context);
        }
    }

    @Test
    void searchForAutoCompleteExcludedFieldsForGroupedRecord() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                TextIndexTestUtils.addRecordTypePrefix(metaDataBuilder);
                metaDataBuilder.addIndex(MAP_DOC, MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE_EXCLUDED_FIELDS);
            });
            recordStore.saveRecord(createMultiEntryMapDoc(1623L, ENGINEER_JOKE, "sampleTextPhrase", WAYLON, "sampleTextSong", 2));

            final RecordQueryPlan luceneIndexPlan =
                    LuceneIndexQueryPlan.of(MAP_ON_VALUE_INDEX_WITH_AUTO_COMPLETE_EXCLUDED_FIELDS.getName(),
                            groupedAutoCompleteScanParams("Vision", "sampleTextPhrase", ImmutableSet.of("entry_second_value")),
                            RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                            false,
                            null,
                            MAP_ON_VALUE_INDEX_STORED_FIELDS);
            assertEquals(1532371150, luceneIndexPlan.planHash(PlanHashable.CURRENT_LEGACY));
            final List<FDBQueriedRecord<Message>> results =
                    recordStore.executeQuery(luceneIndexPlan, null, ExecuteProperties.SERIAL_EXECUTE)
                            .asList().get();

            assertEquals(0, results.size());
            commit(context);
        }
    }

    @Test
    void testAutoCompleteSearchForPhrase() throws Exception {
        try (FDBRecordContext context = openContext()) {
            final Index index = SIMPLE_TEXT_WITH_AUTO_COMPLETE;
            final List<KeyExpression> storedFields = ImmutableList.of(SIMPLE_TEXT_WITH_AUTO_COMPLETE_STORED_FIELD);

            addIndexAndSaveRecordsForAutoCompleteOfPhrase(context, index);

            // All records are matches because they all contain both "united" and "states"
            queryAndAssertAutoCompleteSuggestionsReturned(index,
                    storedFields,
                    "text",
                    "united states",
                    ImmutableList.of("united states of america",
                            "united states is a country in the continent of america",
                            "united kingdom, france, the states",
                            "states united as a country",
                            "states have been united as a country",
                            "all the states united as a country",
                            "all the states have been united as a country",
                            "welcome to the united states of america",
                            "The countries are united kingdom, france, the states"));

            // Only the texts containing "united states" are returned, the last token "states" is queried with term query,
            // same as the other tokens due to the white space following it
            queryAndAssertAutoCompleteSuggestionsReturned(index,
                    storedFields,
                    "text",
                    "\"united states \"",
                    ImmutableList.of("united states of america",
                            "united states is a country in the continent of america",
                            "welcome to the united states of america"));

            // Only the texts containing "united states" are returned, the last token "states" is queried with prefix query
            queryAndAssertAutoCompleteSuggestionsReturned(index,
                    storedFields,
                    "text",
                    "\"united states\"",
                    ImmutableList.of("united states of america",
                            "united states is a country in the continent of america",
                            "welcome to the united states of america"));

            // Only the texts containing "united state" are returned, the last token "state" is queried with prefix query
            queryAndAssertAutoCompleteSuggestionsReturned(index,
                    storedFields,
                    "text",
                    "\"united state\"",
                    ImmutableList.of("united states of america",
                            "united states is a country in the continent of america",
                            "welcome to the united states of america"));
        }
    }

    @Test
    void autoCompletePhraseSearchIncludingStopWords() throws Exception {
        try (FDBRecordContext context = openContext()) {
            final Index index = SIMPLE_TEXT_WITH_AUTO_COMPLETE;
            final List<KeyExpression> storedFields = ImmutableList.of(SIMPLE_TEXT_WITH_AUTO_COMPLETE_STORED_FIELD);

            addIndexAndSaveRecordsForAutoCompleteOfPhrase(context, index);

            queryAndAssertAutoCompleteSuggestionsReturned(index,
                    storedFields,
                    "text",
                    "\"united states of ameri\"",
                    ImmutableList.of("united states of america",
                            "welcome to the united states of america"));

            queryAndAssertAutoCompleteSuggestionsReturned(index,
                    storedFields,
                    "text",
                    "\"united states of \"",
                    ImmutableList.of("united states of america",
                            "welcome to the united states of america",
                            "united states is a country in the continent of america"));

            // Stop-words are interchangeable, so client would have to enforce "exact" stop-word suggestion if required
            queryAndAssertAutoCompleteSuggestionsReturned(index,
                    storedFields,
                    "text",
                    "\"united states of\"",
                    ImmutableList.of("united states of america",
                            "welcome to the united states of america",
                            "united states is a country in the continent of america"));

            // Prefix match on stop-words is not supported and is hard to do. Should be a rare corner case.
            // The user would have to type the entire stop-word
            queryAndAssertAutoCompleteSuggestionsReturned(index,
                    storedFields,
                    "text",
                    "\"united states o\"",
                    ImmutableList.of());

            commit(context);
        }
    }

    @Test
    void autoCompletePhraseSearchWithLeadingStopWords() throws Exception {
        try (FDBRecordContext context = openContext()) {
            final Index index = SIMPLE_TEXT_WITH_AUTO_COMPLETE;
            final List<KeyExpression> storedFields = ImmutableList.of(SIMPLE_TEXT_WITH_AUTO_COMPLETE_STORED_FIELD);

            addIndexAndSaveRecordsForAutoCompleteOfPhrase(context, index);

            queryAndAssertAutoCompleteSuggestionsReturned(index,
                    storedFields,
                    "text",
                    "\"of ameri\"",
                    ImmutableList.of("united states of america",
                            "welcome to the united states of america",
                            "united states is a country in the continent of america"));

            queryAndAssertAutoCompleteSuggestionsReturned(index,
                    storedFields,
                    "text",
                    "\"and of ameri\"",
                    ImmutableList.of("united states of america",
                            "welcome to the united states of america",
                            "united states is a country in the continent of america"));

            commit(context);
        }
    }

    @Test
    void testAutoCompleteSearchMultipleResultsSingleDocument() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(COMPLEX_DOC, COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE);
            });

            ComplexDocument doc = ComplexDocument.newBuilder()
                    .setDocId(1597L)
                    // Romeo and Juliet, Act II, Scene II.
                    .setText("Good night! Good night! Parting is such sweet sorrow")
                    .setText2("That I shall say good night till it be morrow")
                    .build();
            recordStore.saveRecord(doc);

            final RecordQueryPlan luceneIndexPlan =
                    LuceneIndexQueryPlan.of(COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE.getName(),
                            autoCompleteScanParams("good night", ImmutableSet.of("text")),
                            RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                            false,
                            null,
                            COMPLEX_MULTIPLE_TEXT_INDEXES_WITH_AUTO_COMPLETE_STORED_FIELDS);
            assertEquals(-42167700, luceneIndexPlan.planHash(PlanHashable.CURRENT_LEGACY));
            final List<FDBQueriedRecord<Message>> results =
                    recordStore.executeQuery(luceneIndexPlan, null, ExecuteProperties.SERIAL_EXECUTE)
                            .asList().get();

            assertThat(results, hasSize(1));
            final FDBQueriedRecord<Message> result = Iterables.getOnlyElement(results);
            assertEquals(Tuple.from(null, 1597L), Verify.verifyNotNull(result.getIndexEntry()).getPrimaryKey());
            commit(context);
        }
    }

    @Test
    void testAutoCompleteSearchForPhraseWithoutFreqsAndPositions() {
        try (FDBRecordContext context = openContext()) {
            final Index index = SIMPLE_TEXT_WITH_AUTO_COMPLETE_NO_FREQS_POSITIONS;
            final List<KeyExpression> storedFields = ImmutableList.of(index.getRootExpression());

            addIndexAndSaveRecordsForAutoCompleteOfPhrase(context, index);

            // Phrase search is not supported if positions are not indexed
            assertThrows(ExecutionException.class,
                    () -> queryAndAssertAutoCompleteSuggestionsReturned(index,
                            storedFields,
                            "text",
                            "\"united states \"",
                            ImmutableList.of()));

            commit(context);
        }
    }

    @Test
    void searchForSpellCheck() throws Exception {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SPELLCHECK_INDEX);
            long docId = 1623L;
            for (String word : List.of("hello", "monitor", "keyboard", "mouse", "trackpad", "cable", "help", "elmo", "elbow", "helps", "helm", "helms", "gulps")) {
                recordStore.saveRecord(createSimpleDocument(docId++, word, 1));
            }

            CompletableFuture<List<IndexEntry>> resultsI = recordStore.scanIndex(SPELLCHECK_INDEX,
                    spellCheck(SPELLCHECK_INDEX, "keyboad"),
                    null,
                    ScanProperties.FORWARD_SCAN).asList();
            List<IndexEntry> results = resultsI.get();

            assertEquals(1, results.size());
            IndexEntry result = results.get(0);
            assertEquals("keyboard", result.getKey().getString(1));
            assertEquals("text", result.getKey().getString(0));
            assertEquals(0.85714287F, result.getValue().get(0));

            List<IndexEntry> results2 = recordStore.scanIndex(SPELLCHECK_INDEX,
                    spellCheck(SPELLCHECK_INDEX, "text:keyboad"),
                    null,
                    ScanProperties.FORWARD_SCAN).asList().get();
            assertEquals(1, results2.size());
            IndexEntry result2 = results2.get(0);
            assertEquals("keyboard", result2.getKey().get(1));
            assertEquals("text", result2.getKey().get(0));
            assertEquals(0.85714287F, result2.getValue().get(0));

            commit(context);
        }
    }

    @Test
    void searchForSpellcheckForGroupedRecord() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(MAP_DOC, MAP_ON_VALUE_INDEX);
            });
            FDBStoredRecord<Message> fdbRecord = recordStore.saveRecord(createMultiEntryMapDoc(1623L, ENGINEER_JOKE, "sampleTextPhrase", WAYLON, "sampleTextSong", 2));
            List<IndexEntry> indexEntries = recordStore.scanIndex(MAP_ON_VALUE_INDEX,
                    groupedSpellCheck(MAP_ON_VALUE_INDEX, "Visin", "sampleTextPhrase"),
                    null,
                    ScanProperties.FORWARD_SCAN).asList().get();

            assertEquals(1, indexEntries.size());
            IndexEntry indexEntry = indexEntries.get(0);
            assertEquals(0.8F, indexEntry.getValue().get(0));

            Descriptors.Descriptor recordDescriptor = TestRecordsTextProto.MapDocument.getDescriptor();
            IndexKeyValueToPartialRecord toPartialRecord = LuceneIndexKeyValueToPartialRecordUtils.getToPartialRecord(
                    MAP_ON_VALUE_INDEX, fdbRecord.getRecordType(), LuceneScanTypes.BY_LUCENE_SPELL_CHECK);
            Message message = toPartialRecord.toRecord(recordDescriptor, indexEntry);

            Descriptors.FieldDescriptor entryDescriptor = recordDescriptor.findFieldByName("entry");
            Message entry = (Message)message.getRepeatedField(entryDescriptor, 0);

            Descriptors.FieldDescriptor keyDescriptor = entryDescriptor.getMessageType().findFieldByName("key");
            Descriptors.FieldDescriptor valueDescriptor = entryDescriptor.getMessageType().findFieldByName("value");

            //TODO: This seems like the wrong field string to return. I'm not sure what to do here
            assertEquals("sampleTextPhrase", entry.getField(keyDescriptor));
            assertEquals("vision", entry.getField(valueDescriptor));

            commit(context);
        }
    }

    private void spellCheckHelper(final Index index, @Nonnull String query, List<Pair<String, String>> expectedSuggestions) throws ExecutionException, InterruptedException {
        List<IndexEntry> suggestions = recordStore.scanIndex(index,
                spellCheck(index, query),
                null,
                ScanProperties.FORWARD_SCAN).asList().get();

        assertEquals(expectedSuggestions.size(), suggestions.size());
        for (int i = 0; i < expectedSuggestions.size(); ++i) {
            assertThat(suggestions.get(i).getKey().get(1), equalTo(expectedSuggestions.get(i).getKey()));
            assertThat(suggestions.get(i).getKey().get(0), equalTo(expectedSuggestions.get(i).getValue()));
        }
    }

    @Test
    void spellCheckMultipleMatches() throws Exception {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SPELLCHECK_INDEX);
            long docId = 1623L;
            for (String word : List.of("hello", "monitor", "keyboard", "mouse", "trackpad", "cable", "help", "elmo", "elbow", "helps", "helm", "helms", "gulps")) {
                recordStore.saveRecord(createSimpleDocument(docId++, word, 1));
            }
            spellCheckHelper(SPELLCHECK_INDEX, "keyboad", List.of(Pair.of("keyboard", "text")));
            spellCheckHelper(SPELLCHECK_INDEX, "text:keyboad", List.of(Pair.of("keyboard", "text")));
            spellCheckHelper(SPELLCHECK_INDEX, "helo", List.of(
                    Pair.of("hello", "text"),
                    Pair.of("helm", "text"),
                    Pair.of("help", "text"),
                    Pair.of("helms", "text"),
                    Pair.of("helps", "text")
            ));
            spellCheckHelper(SPELLCHECK_INDEX, "hello", List.of());
            spellCheckHelper(SPELLCHECK_INDEX, "mous", List.of(Pair.of("mouse", "text")));

            final List<Pair<String, String>> emptyList = List.of();
            assertThrows(RecordCoreException.class,
                    () -> spellCheckHelper(SPELLCHECK_INDEX, "wrongField:helo", emptyList),
                    "Invalid field name in Lucene index query");
        }
    }

    @Test
    void spellCheckComplexDocument() throws Exception {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, SPELLCHECK_INDEX_COMPLEX);
            long docId = 1623L;
            List<String> text = List.of("beaver", "leopard", "hello", "help", "helm", "boat", "road", "fowl", "foot", "tare", "tire");
            List<String> text2 = List.of("beavers", "lizards", "hell", "helps", "helms", "boot", "read", "fowl", "fool", "tire", "tire");
            assertThat(text2, hasSize(text.size()));
            for (int i = 0; i < text.size(); ++i) {
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(docId++, text.get(i), text2.get(i), 1));
            }
            spellCheckHelper(SPELLCHECK_INDEX_COMPLEX, "baver", List.of(Pair.of("beaver", "text"), Pair.of("beavers", "text2")));
            spellCheckHelper(SPELLCHECK_INDEX_COMPLEX, "text:baver", List.of(Pair.of("beaver", "text")));
            spellCheckHelper(SPELLCHECK_INDEX_COMPLEX, "text2:baver", List.of(Pair.of("beavers", "text2")));

            spellCheckHelper(SPELLCHECK_INDEX_COMPLEX, "lepard", List.of(Pair.of("leopard", "text")));
            spellCheckHelper(SPELLCHECK_INDEX_COMPLEX, "text:lepard", List.of(Pair.of("leopard", "text")));
            spellCheckHelper(SPELLCHECK_INDEX_COMPLEX, "text2:lepard", List.of());

            spellCheckHelper(SPELLCHECK_INDEX_COMPLEX, "lizerds", List.of(Pair.of("lizards", "text2")));
            spellCheckHelper(SPELLCHECK_INDEX_COMPLEX, "text:lizerds", List.of());
            spellCheckHelper(SPELLCHECK_INDEX_COMPLEX, "text2:lizerds", List.of(Pair.of("lizards", "text2")));

            // Apply the limit of 5 fields so do not return "helms" which has a lower score than the rest
            spellCheckHelper(SPELLCHECK_INDEX_COMPLEX, "hela", List.of(
                    Pair.of("hell", "text2"),
                    Pair.of("helm", "text"),
                    Pair.of("help", "text"),
                    Pair.of("hello", "text"),
                    Pair.of("helms", "text2")));

            // Same score and same frequency, this is sorted by field name
            spellCheckHelper(SPELLCHECK_INDEX_COMPLEX, "bost", List.of(
                    Pair.of("boat", "text"),
                    Pair.of("boot", "text2")));
            spellCheckHelper(SPELLCHECK_INDEX_COMPLEX, "rlad", List.of(
                    Pair.of("read", "text2"),
                    Pair.of("road", "text")));

            // Same score but different frequency, priority to the more frequent item
            spellCheckHelper(SPELLCHECK_INDEX_COMPLEX, "foml", List.of(
                    Pair.of("fowl", "text"),
                    Pair.of("fool", "text2"),
                    Pair.of("foot", "text")));
            // Same, but this time, getRight() should be text2 because tire was more frequent in text2 than text
            spellCheckHelper(SPELLCHECK_INDEX_COMPLEX, "tbre", List.of(
                    Pair.of("tire", "text2"),
                    Pair.of("tare", "text")));
        }
    }

    @Test
    void testDeleteWhereSimple() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                TextIndexTestUtils.addRecordTypePrefix(metaDataBuilder);
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
                metaDataBuilder.getRecordType(SIMPLE_DOC)
                        .setPrimaryKey(concat(recordType(), field("text")));
            });

            TestRecordsTextProto.SimpleDocument simple = TestRecordsTextProto.SimpleDocument.newBuilder()
                    .setDocId(1066L)
                    .setText("foo bar")
                    .build();
            recordStore.saveRecord(simple);
            Query.InvalidExpressionException err = assertThrows(Query.InvalidExpressionException.class,
                    () -> recordStore.deleteRecordsWhere(SIMPLE_DOC, Query.field("text").equalsValue("foo bar")));
            assertThat(err.getMessage(), containsString(String.format("deleteRecordsWhere not supported by index %s", SIMPLE_TEXT_SUFFIXES.getName())));

            FDBStoredRecord<Message> storedRecord = recordStore.loadRecord(Tuple.from(recordStore.getRecordMetaData().getRecordType(SIMPLE_DOC).getRecordTypeKey(), "foo bar"));
            assertNotNull(storedRecord);
            assertEquals(simple, TestRecordsTextProto.SimpleDocument.newBuilder().mergeFrom(storedRecord.getRecord()).build());

            commit(context);
        }
    }

    @Test
    void testDeleteWhereComplexGrouped() {
        final RecordMetaDataHook hook = metaDataBuilder -> {
            TextIndexTestUtils.addRecordTypePrefix(metaDataBuilder);
            metaDataBuilder.addIndex(COMPLEX_DOC, COMPLEX_MULTIPLE_GROUPED);
        };

        final ComplexDocument zeroGroupDoc = ComplexDocument.newBuilder()
                .setGroup(0)
                .setDocId(1623L)
                .setText(TextSamples.ROMEO_AND_JULIET_PROLOGUE)
                .setText2(TextSamples.ANGSTROM)
                .build();
        final ComplexDocument oneGroupDoc = ComplexDocument.newBuilder()
                .setGroup(1)
                .setDocId(1623L)
                .setText(TextSamples.ROMEO_AND_JULIET_PROLOGUE)
                .setText2(TextSamples.ANGSTROM)
                .build();

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            recordStore.saveRecord(zeroGroupDoc);
            recordStore.saveRecord(oneGroupDoc);
            commit(context);
        }

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);

            RecordQuery recordQuery = RecordQuery.newBuilder()
                    .setRecordType(COMPLEX_DOC)
                    .setFilter(Query.and(
                            Query.field("group").equalsParameter("group_value"),
                            new LuceneQueryComponent("text:\"continuance\" AND text2:\"named\"", List.of("text", "text2"))
                    ))
                    .build();
            LucenePlanner planner = new LucenePlanner(recordStore.getRecordMetaData(), recordStore.getRecordStoreState(), PlannableIndexTypes.DEFAULT, recordStore.getTimer());
            RecordQueryPlan plan = planner.plan(recordQuery);
            assertThat(plan, indexScan(allOf(
                    indexName(COMPLEX_MULTIPLE_GROUPED.getName()),
                    scanParams(allOf(
                            group(hasTupleString("[EQUALS $group_value]")),
                            query(hasToString("text:\"continuance\" AND text2:\"named\"")))))));

            assertEquals(Collections.singletonList(zeroGroupDoc),
                    plan.execute(recordStore, EvaluationContext.forBinding("group_value", zeroGroupDoc.getGroup()))
                            .map(FDBQueriedRecord::getRecord)
                            .map(rec -> ComplexDocument.newBuilder().mergeFrom(rec).build())
                            .asList()
                            .join());
            assertEquals(Collections.singletonList(oneGroupDoc),
                    plan.execute(recordStore, EvaluationContext.forBinding("group_value", oneGroupDoc.getGroup()))
                            .map(FDBQueriedRecord::getRecord)
                            .map(rec -> ComplexDocument.newBuilder().mergeFrom(rec).build())
                            .asList()
                            .join());

            // Issue a delete where to delete the zero group
            recordStore.deleteRecordsWhere(COMPLEX_DOC, Query.field("group").equalsValue(zeroGroupDoc.getGroup()));

            assertEquals(Collections.emptyList(),
                    plan.execute(recordStore, EvaluationContext.forBinding("group_value", zeroGroupDoc.getGroup()))
                            .map(FDBQueriedRecord::getRecord)
                            .map(rec -> ComplexDocument.newBuilder().mergeFrom(rec).build())
                            .asList()
                            .join());
            assertEquals(Collections.singletonList(oneGroupDoc),
                    plan.execute(recordStore, EvaluationContext.forBinding("group_value", oneGroupDoc.getGroup()))
                            .map(FDBQueriedRecord::getRecord)
                            .map(rec -> ComplexDocument.newBuilder().mergeFrom(rec).build())
                            .asList()
                            .join());
        }
    }

    @Test
    void testDeleteWhereAutoComplete() throws Exception {
        final RecordMetaDataHook hook = metaDataBuilder -> {
            TextIndexTestUtils.addRecordTypePrefix(metaDataBuilder);
            metaDataBuilder.addIndex(COMPLEX_DOC, COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE);
        };
        final int maxGroup = 10;
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            for (int group = 0; group < maxGroup; group++) {
                for (long docId = 0L; docId < 10L; docId++) {
                    ComplexDocument doc = ComplexDocument.newBuilder()
                            .setGroup(group)
                            .setDocId(docId)
                            .setText(String.format("hello there %d", group))
                            .setText2(TextSamples.TELUGU)
                            .build();
                    recordStore.saveRecord(doc);
                }
            }
            commit(context);
        }
        // Re-initialize the builder so the LUCENE_INDEX_COMPRESSION_ENABLED prop is not added twice
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            for (long group = 0; group < maxGroup; group++) {
                final RecordQueryPlan luceneIndexPlan =
                        LuceneIndexQueryPlan.of(COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE.getName(),
                                groupedAutoCompleteScanParams("hello", group, ImmutableList.of("text", "text2")),
                                RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                                false,
                                null,
                                COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE_STORED_FIELDS);
                final List<FDBQueriedRecord<Message>> results =
                        recordStore.executeQuery(luceneIndexPlan, null, ExecuteProperties.SERIAL_EXECUTE)
                                .asList().get();
                assertThat(results, hasSize(10));
                int docId = 0;
                for (FDBQueriedRecord<?> result : results) {
                    final Message record = result.getRecord();
                    final Descriptors.Descriptor descriptor = record.getDescriptorForType();
                    final Descriptors.FieldDescriptor textFieldDescriptor = descriptor.findFieldByName("text");
                    assertTrue(record.hasField(textFieldDescriptor));
                    final String textField = (String)record.getField(textFieldDescriptor);
                    assertEquals(String.format("hello there %d", group), textField);

                    final IndexEntry entry = result.getIndexEntry();
                    Assertions.assertNotNull(entry);
                    Tuple primaryKey = entry.getPrimaryKey();
                    // The 1st element is the key for the record type
                    assertEquals(group, primaryKey.get(1));
                    assertEquals((long)docId, primaryKey.get(2));
                    docId++;
                }
            }

            final int groupToDelete = maxGroup / 2;
            recordStore.deleteRecordsWhere(COMPLEX_DOC, Query.field("group").equalsValue(groupToDelete));

            for (long group = 0; group < maxGroup; group++) {
                final RecordQueryPlan luceneIndexPlan =
                        LuceneIndexQueryPlan.of(COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE.getName(),
                                groupedAutoCompleteScanParams("hello", group, ImmutableList.of("text", "text2")),
                                RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                                false,
                                null,
                                COMPLEX_MULTI_GROUPED_WITH_AUTO_COMPLETE_STORED_FIELDS);
                final List<FDBQueriedRecord<Message>> results =
                        recordStore.executeQuery(luceneIndexPlan, null, ExecuteProperties.SERIAL_EXECUTE)
                                .asList().get();

                if (group == groupToDelete) {
                    assertThat(results, empty());
                } else {
                    assertThat(results, hasSize(10));
                    int docId = 0;
                    for (FDBQueriedRecord<?> result : results) {
                        final Message record = result.getRecord();
                        final Descriptors.Descriptor descriptor = record.getDescriptorForType();
                        final Descriptors.FieldDescriptor textFieldDescriptor = descriptor.findFieldByName("text");
                        assertTrue(record.hasField(textFieldDescriptor));
                        final String textField = (String)record.getField(textFieldDescriptor);
                        assertEquals(String.format("hello there %d", group), textField);
                        final IndexEntry entry = result.getIndexEntry();
                        Assertions.assertNotNull(entry);
                        Tuple primaryKey = entry.getPrimaryKey();
                        // The 1st element is the key for the record type
                        assertEquals(group, primaryKey.get(1));
                        assertEquals((long)docId, primaryKey.get(2));
                        docId++;
                    }
                }
            }
        }
    }

    @Test
    void analyzerPerField() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, MULTIPLE_ANALYZER_LUCENE_INDEX);
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1623L, "Hello, I am working on record layer", "Hello, I am working on FoundationDB", 1));
            // text field uses the default SYNONYM analyzer, so "hullo" should have match
            assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(1L, 1623L)),
                    recordStore.scanIndex(MULTIPLE_ANALYZER_LUCENE_INDEX, fullTextSearch(MULTIPLE_ANALYZER_LUCENE_INDEX, "text:hullo"), null, ScanProperties.FORWARD_SCAN));
            // text2 field has NGRAM analyzer override, so "hullo" should not have match
            assertIndexEntryPrimaryKeyTuples(Collections.emptySet(),
                    recordStore.scanIndex(MULTIPLE_ANALYZER_LUCENE_INDEX, fullTextSearch(MULTIPLE_ANALYZER_LUCENE_INDEX, "text2:hullo"), null, ScanProperties.FORWARD_SCAN));
            // text field uses the default SYNONYM analyzer, so "orkin" should not have match
            assertIndexEntryPrimaryKeyTuples(Collections.emptySet(),
                    recordStore.scanIndex(MULTIPLE_ANALYZER_LUCENE_INDEX, fullTextSearch(MULTIPLE_ANALYZER_LUCENE_INDEX, "text:orkin"), null, ScanProperties.FORWARD_SCAN));
            // text2 field has NGRAM analyzer override, so "orkin" should have match
            assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(1L, 1623L)),
                    recordStore.scanIndex(MULTIPLE_ANALYZER_LUCENE_INDEX, fullTextSearch(MULTIPLE_ANALYZER_LUCENE_INDEX, "text2:orkin"), null, ScanProperties.FORWARD_SCAN));
        }
    }

    @Test
    void testSimpleAutoComplete() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, AUTO_COMPLETE_SIMPLE_LUCENE_INDEX);
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1623L, "Hello, I am working on record layer", "Hello, I am working on FoundationDB", 1));
            // text field has auto-complete enabled, so the auto-complete query for "record layer" should have match
            assertIndexEntryPrimaryKeyTuples(Set.of(Tuple.from(1L, 1623L)),
                    recordStore.scanIndex(AUTO_COMPLETE_SIMPLE_LUCENE_INDEX, autoCompleteBounds(AUTO_COMPLETE_SIMPLE_LUCENE_INDEX, "record layer", ImmutableSet.of("text")), null, ScanProperties.FORWARD_SCAN));
        }
    }

    @Test
    void analyzerChooserTest() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, ANALYZER_CHOOSER_TEST_LUCENE_INDEX);

            // Synonym analyzer is chosen due to the keyword "synonym" from the text
            recordStore.saveRecord(createSimpleDocument(1623L, "synonym food", 1));
            assertEquals(1, recordStore.scanIndex(ANALYZER_CHOOSER_TEST_LUCENE_INDEX, fullTextSearch(ANALYZER_CHOOSER_TEST_LUCENE_INDEX, "nutrient"), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            assertEquals(0, recordStore.scanIndex(ANALYZER_CHOOSER_TEST_LUCENE_INDEX, fullTextSearch(ANALYZER_CHOOSER_TEST_LUCENE_INDEX, "foo"), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());

            // Ngram analyzer is chosen due to no keyword "synonym" from the text
            recordStore.saveRecord(createSimpleDocument(1624L, "ngram motivation", 1));
            assertEquals(0, recordStore.scanIndex(ANALYZER_CHOOSER_TEST_LUCENE_INDEX, fullTextSearch(ANALYZER_CHOOSER_TEST_LUCENE_INDEX, "need"), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
            assertEquals(1, recordStore.scanIndex(ANALYZER_CHOOSER_TEST_LUCENE_INDEX, fullTextSearch(ANALYZER_CHOOSER_TEST_LUCENE_INDEX, "motivatio"), null, ScanProperties.FORWARD_SCAN)
                    .getCount().join());
        }
    }

    @Test
    void basicLuceneCursorTest() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            // Save 20 records
            for (int i = 0; i < 20; i++) {
                recordStore.saveRecord(createSimpleDocument(1600L + i, "testing text" + i, 1));
            }
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "\"testing\""), null, ScanProperties.FORWARD_SCAN);

            List<IndexEntry> entries = indexEntries.asList().join();
            assertEquals(20, entries.size());
            assertEquals(20, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
        }
    }

    @Test
    void luceneCursorTestWithMultiplePages() throws Exception {
        // Configure page size as 10
        final RecordLayerPropertyStorage.Builder storageBuilder = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_INDEX_CURSOR_PAGE_SIZE, 10);
        try (FDBRecordContext context = openContext(storageBuilder)) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            // Save 20 records
            for (int i = 0; i < 20; i++) {
                recordStore.saveRecord(createSimpleDocument(1600L + i, "testing text" + i, 1));
            }

            ScanProperties scanProperties = new ScanProperties(ExecuteProperties.newBuilder()
                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                    .build());
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "\"testing\""), null, scanProperties);

            List<IndexEntry> entries = indexEntries.asList().join();
            assertEquals(20, entries.size());
            assertEquals(20, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, indexEntries.onNext().get().getNoNextReason());
        }
    }

    @Test
    void luceneCursorTestWith3rdPage() throws Exception {
        // Configure page size as 10
        final RecordLayerPropertyStorage.Builder storageBuilder = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_INDEX_CURSOR_PAGE_SIZE, 10);
        try (FDBRecordContext context = openContext(storageBuilder)) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            // Save 21 records
            for (int i = 0; i < 21; i++) {
                recordStore.saveRecord(createSimpleDocument(1600L + i, "testing text" + i, 1));
            }

            ScanProperties scanProperties = new ScanProperties(ExecuteProperties.newBuilder()
                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                    .build());
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "\"testing\""), null, scanProperties);

            List<IndexEntry> entries = indexEntries.asList().join();
            assertEquals(21, entries.size());
            assertEquals(21, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, indexEntries.onNext().get().getNoNextReason());
        }
    }

    @Test
    void luceneCursorTestWithMultiplePagesWithSkip() throws Exception {
        // Configure page size as 10
        final RecordLayerPropertyStorage.Builder storageBuilder = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_INDEX_CURSOR_PAGE_SIZE, 10);
        try (FDBRecordContext context = openContext(storageBuilder)) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            // Save 31 records
            for (int i = 0; i < 31; i++) {
                recordStore.saveRecord(createSimpleDocument(1600L + i, "testing text" + i, 1));
            }

            ScanProperties scanProperties = new ScanProperties(ExecuteProperties.newBuilder()
                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                    .setSkip(12)
                    .build());
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "\"testing\""), null, scanProperties);

            List<IndexEntry> entries = indexEntries.asList().join();
            assertEquals(19, entries.size());
            assertEquals(19, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, indexEntries.onNext().get().getNoNextReason());
        }
    }

    @Test
    void luceneCursorTestWithLimit() throws Exception {
        // Configure page size as 10
        final RecordLayerPropertyStorage.Builder storageBuilder = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_INDEX_CURSOR_PAGE_SIZE, 10);
        try (FDBRecordContext context = openContext(storageBuilder)) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            // Save 21 records
            for (int i = 0; i < 21; i++) {
                recordStore.saveRecord(createSimpleDocument(1600L + i, "testing text" + i, 1));
            }

            // Scan with limit = 10
            ScanProperties scanProperties = new ScanProperties(ExecuteProperties.newBuilder()
                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                    .setReturnedRowLimit(8)
                    .build());
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "\"testing\""), null, scanProperties);

            // Get 8 results and continuation
            List<IndexEntry> entries = indexEntries.asList().join();
            assertEquals(8, entries.size());
            assertEquals(8, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            RecordCursorResult<IndexEntry> lastResult = indexEntries.onNext().get();
            assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, lastResult.getNoNextReason());

            indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "\"testing\""), lastResult.getContinuation().toBytes(), scanProperties);

            // Get 8 results and continuation
            entries = indexEntries.asList().join();
            assertEquals(8, entries.size());
            assertEquals(16, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            lastResult = indexEntries.onNext().get();
            assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, lastResult.getNoNextReason());

            indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "\"testing\""), lastResult.getContinuation().toBytes(), scanProperties);

            // Get 3 results
            entries = indexEntries.asList().join();
            assertEquals(5, entries.size());
            assertEquals(21, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, indexEntries.onNext().get().getNoNextReason());
        }
    }

    @Test
    void luceneCursorTestWithLimitAndSkip() throws Exception {
        // Configure page size as 10
        final RecordLayerPropertyStorage.Builder storageBuilder = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_INDEX_CURSOR_PAGE_SIZE, 10);
        try (FDBRecordContext context = openContext(storageBuilder)) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            // Save 21 records
            for (int i = 0; i < 21; i++) {
                recordStore.saveRecord(createSimpleDocument(1600L + i, "testing text" + i, 1));
            }

            // Scan with limit = 8 and skip = 2
            ScanProperties scanProperties = new ScanProperties(ExecuteProperties.newBuilder()
                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                    .setReturnedRowLimit(8)
                    .setSkip(2)
                    .build());
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "\"testing\""), null, scanProperties);

            // Get 8 results and continuation
            List<IndexEntry> entries = indexEntries.asList().join();
            assertEquals(8, entries.size());
            assertEquals(8, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            RecordCursorResult<IndexEntry> lastResult = indexEntries.onNext().get();
            assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, lastResult.getNoNextReason());

            // Scan with limit = 8, no skip
            scanProperties = new ScanProperties(ExecuteProperties.newBuilder()
                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                    .setReturnedRowLimit(8)
                    .build());
            indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "\"testing\""), lastResult.getContinuation().toBytes(), scanProperties);

            // Get 8 results and continuation
            entries = indexEntries.asList().join();
            assertEquals(8, entries.size());
            assertEquals(16, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            lastResult = indexEntries.onNext().get();
            assertEquals(RecordCursor.NoNextReason.RETURN_LIMIT_REACHED, lastResult.getNoNextReason());

            indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "\"testing\""), lastResult.getContinuation().toBytes(), scanProperties);

            // Get 3 results
            entries = indexEntries.asList().join();
            assertEquals(3, entries.size());
            assertEquals(19, getCounter(context, FDBStoreTimer.Counts.LOAD_SCAN_ENTRY).getCount());
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, indexEntries.onNext().get().getNoNextReason());
        }
    }

    @Test
    void luceneCursorTestAllMatchesSkipped() throws Exception {
        // Configure page size as 10
        final RecordLayerPropertyStorage.Builder storageBuilder = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_INDEX_CURSOR_PAGE_SIZE, 10);
        try (FDBRecordContext context = openContext(storageBuilder)) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            // Save 6 records
            for (int i = 0; i < 6; i++) {
                recordStore.saveRecord(createSimpleDocument(1600L + i, "testing text" + i, 1));
            }
            // Scan with skip = 15
            ScanProperties scanProperties = new ScanProperties(ExecuteProperties.newBuilder()
                    .setIsolationLevel(IsolationLevel.SERIALIZABLE)
                    .setSkip(15)
                    .build());
            RecordCursor<IndexEntry> indexEntries = recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "\"testing\""), null, scanProperties);

            // No matches are found and source is exhausted
            RecordCursorResult<IndexEntry> next = indexEntries.onNext().get();
            assertFalse(next.hasNext());
            assertEquals(RecordCursor.NoNextReason.SOURCE_EXHAUSTED, next.getNoNextReason());
            assertNull(Verify.verifyNotNull(context.getTimer()).getCounter(FDBStoreTimer.Counts.LOAD_SCAN_ENTRY));
        }
    }

    @ParameterizedTest
    @BooleanSource
    void manySegmentsParallelOpen(boolean primaryKeySegmentIndexEnabled) {
        final Index index = primaryKeySegmentIndexEnabled ? SIMPLE_TEXT_SUFFIXES_WITH_PRIMARY_KEY_SEGMENT_INDEX : SIMPLE_TEXT_SUFFIXES;
        for (int i = 0; i < 20; i++) {
            final RecordLayerPropertyStorage.Builder insertProps = RecordLayerPropertyStorage.newBuilder()
                    .addProp(LuceneRecordContextProperties.LUCENE_MERGE_MAX_SIZE, 0.001); // Don't merge
            try (FDBRecordContext context = openContext(insertProps)) {
                rebuildIndexMetaData(context, SIMPLE_DOC, index);
                recordStore.saveRecord(createSimpleDocument(1000 + i, ENGINEER_JOKE, 2));
                context.commit();
            }
        }
        final RecordLayerPropertyStorage.Builder scanProps = RecordLayerPropertyStorage.newBuilder()
                .addProp(LuceneRecordContextProperties.LUCENE_OPEN_PARALLELISM, 2); // Decrease parallelism when opening segments
        try (FDBRecordContext context = openContext(scanProps)) {
            rebuildIndexMetaData(context, SIMPLE_DOC, index);
            assertEquals(20,
                    recordStore.scanIndex(index, fullTextSearch(index, "Vision"), null, ScanProperties.FORWARD_SCAN).getCount().join());
            try (FDBDirectory directory = new FDBDirectory(recordStore.indexSubspace(index), context, index.getOptions())) {
                assertEquals(61, directory.listAll().length);
            }
        }
    }

    private static void assertAutoCompleteEntriesAndSegmentInfoStoredInCompoundFile(Index index, @Nonnull Subspace subspace, @Nonnull FDBRecordContext context, @Nonnull String segment) {
        validateSegmentAndIndexIntegrity(index, subspace, context, segment);
    }

    private static void validateSegmentAndIndexIntegrity(Index index, @Nonnull Subspace subspace, @Nonnull FDBRecordContext context, @Nonnull String segmentFile) {
        try (final FDBDirectory directory = new FDBDirectory(subspace, context, index.getOptions())) {
            final FDBLuceneFileReference reference = directory.getFDBLuceneFileReference(segmentFile);
            assertNotNull(reference);
            // Extract the segment name from the file name
            final String segmentName = segmentFile.substring(0, segmentFile.indexOf("."));
            validateIndexIntegrity(index, subspace, context, directory, segmentName);
        }
    }

    @Nonnull
    private LuceneIndexMaintainer getIndexMaintainer(final Index index) {
        return (LuceneIndexMaintainer)recordStore.getIndexMaintainer(index);
    }

    private FDBDirectory getDirectory(final Index index, final Tuple groupingKey) {
        return getIndexMaintainer(index).getDirectory(groupingKey, null);
    }

    private long getSegmentCount(final Index index, final Tuple groupingKey) {
        final String[] files = getDirectory(index, groupingKey).listAll();
        return Arrays.stream(files).filter(FDBDirectory::isCompoundFile).count();
    }

    private static void validateIndexIntegrity(Index index, @Nonnull Subspace subspace, @Nonnull FDBRecordContext context, @Nullable FDBDirectory fdbDirectory, @Nullable String segmentName) {
        final FDBDirectory directory = fdbDirectory == null ? new FDBDirectory(subspace, context, index.getOptions()) : fdbDirectory;
        String[] allFiles = directory.listAll();
        Set<Long> usedFieldInfos = new HashSet<>();
        final Set<Long> allFieldInfos = assertDoesNotThrow(() -> directory.getFieldInfosStorage().getAllFieldInfos().keySet());
        int segmentCount = 0;
        for (String file : allFiles) {
            final FDBLuceneFileReference fileReference = directory.getFDBLuceneFileReference(file);
            if (FDBDirectory.isEntriesFile(file) || FDBDirectory.isSegmentInfo(file) || FDBDirectory.isFieldInfoFile(file)
                    || file.endsWith(".pky")) {
                assertFalse(fileReference.getContent().isEmpty(), "fileName=" + file);
            } else if (FDBDirectory.isStoredFieldsFile(file) && (segmentName != null)) {
                // It is OK for a stored fields info for the next segment to be created at this point.
                // Make sure it is not around for the verified segment only
                if (file.startsWith(segmentName)) {
                    fail("Found stored fields file that should have been removed");
                }
            } else {
                assertTrue(FDBDirectory.isCompoundFile(file) || file.startsWith(IndexFileNames.SEGMENTS),
                        "fileName=" + file);
                assertTrue(fileReference.getContent().isEmpty(), "fileName=" + file);
            }
            if (FDBDirectory.isSegmentInfo(file)) {
                segmentCount++;
            }
            usedFieldInfos.add(validateFieldInfos(file, fileReference, directory));
        }
        usedFieldInfos.remove(0L);
        assertEquals(allFieldInfos, usedFieldInfos);
        assertThat(allFieldInfos.size(), Matchers.lessThanOrEqualTo(segmentCount));
    }

    private static long validateFieldInfos(final String file, final FDBLuceneFileReference fileReference, final FDBDirectory directory) {
        if (FDBDirectory.isFieldInfoFile(file) || FDBDirectory.isEntriesFile(file)) {
            assertNotEquals(0, fileReference.getFieldInfosId());
            assertNotEquals(ByteString.EMPTY, fileReference.getFieldInfosBitSet());
            final LuceneFieldInfosProto.FieldInfos fieldInfos = assertDoesNotThrow(() -> directory.getFieldInfosStorage().readFieldInfos(fileReference.getFieldInfosId()));
            final BitSet bitSet = BitSet.valueOf(fileReference.getFieldInfosBitSet().toByteArray());
            final Set<Integer> fieldNumbers = fieldInfos.getFieldInfoList().stream()
                    .map(LuceneFieldInfosProto.FieldInfo::getNumber)
                    .collect(Collectors.toSet());
            for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
                // operate on index i here
                if (i == Integer.MAX_VALUE) {
                    break; // or (i+1) would overflow
                }
                assertTrue(fieldNumbers.contains(i));
            }
        } else {
            assertEquals(0, fileReference.getFieldInfosId());
            assertEquals(ByteString.EMPTY, fileReference.getFieldInfosBitSet());
        }
        return fileReference.getFieldInfosId();
    }

    private void searchForAutoCompleteAndAssert(String query, boolean matches, boolean highlight, int planHash) throws Exception {
        try (FDBRecordContext context = openContext()) {
            addIndexAndSaveRecordForAutoComplete(context);

            final RecordQueryPlan luceneIndexPlan =
                    LuceneIndexQueryPlan.of(SIMPLE_TEXT_WITH_AUTO_COMPLETE.getName(),
                            autoCompleteScanParams(query, ImmutableSet.of("text")),
                            RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                            false,
                            null,
                            ImmutableList.of(SIMPLE_TEXT_WITH_AUTO_COMPLETE_STORED_FIELD));
            assertEquals(planHash, luceneIndexPlan.planHash(PlanHashable.CURRENT_LEGACY));
            final List<FDBQueriedRecord<Message>> results =
                    recordStore.executeQuery(luceneIndexPlan, null, ExecuteProperties.SERIAL_EXECUTE)
                            .asList().get();

            final var timer = Verify.verifyNotNull(context.getTimer());
            if (!matches) {
                // Assert no suggestions
                assertTrue(results.isEmpty());
                assertEquals(0, timer.getCount(LuceneEvents.Counts.LUCENE_SCAN_MATCHED_AUTO_COMPLETE_SUGGESTIONS));
                return;
            }

            // Assert the count of suggestions
            assertEquals(6, results.size());

            // Assert the suggestions' keys
            List<String> suggestions = results.stream()
                    .map(FDBQueriedRecord::getRecord)
                    .map(record -> {
                        final Descriptors.Descriptor descriptor = record.getDescriptorForType();
                        final Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName("text");
                        Assertions.assertTrue(record.hasField(fieldDescriptor));
                        return Verify.verifyNotNull((String)record.getField(fieldDescriptor));
                    }).collect(Collectors.toList());
            if (highlight) {
                assertEquals(ImmutableList.of("Good morning", "Good afternoon", "good evening", "Good night", "That's really good!", "I'm good"), suggestions);
            } else {
                assertEquals(ImmutableList.of("Good morning", "Good afternoon", "good evening", "Good night", "That's really good!", "I'm good"), suggestions);
            }

            assertAutoCompleteEntriesAndSegmentInfoStoredInCompoundFile(SIMPLE_TEXT_WITH_AUTO_COMPLETE, recordStore.indexSubspace(SIMPLE_TEXT_WITH_AUTO_COMPLETE),
                    context, "_0.cfs");

            commit(context);
        }
    }

    @SuppressWarnings("UnusedReturnValue")
    private RecordType addIndexAndSaveRecordForAutoComplete(@Nonnull FDBRecordContext context) {
        openRecordStore(context, metaDataBuilder -> {
            metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
            metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_WITH_AUTO_COMPLETE);
        });

        // Write 8 texts and 6 of them contain the key "good"
        recordStore.saveRecord(createSimpleDocument(1623L, "Good morning", 1));
        recordStore.saveRecord(createSimpleDocument(1624L, "Good afternoon", 1));
        recordStore.saveRecord(createSimpleDocument(1625L, "good evening", 1));
        recordStore.saveRecord(createSimpleDocument(1626L, "Good night", 1));
        recordStore.saveRecord(createSimpleDocument(1627L, "That's really good!", 1));
        recordStore.saveRecord(createSimpleDocument(1628L, "I'm good", 1));
        recordStore.saveRecord(createSimpleDocument(1629L, "Hello Record Layer", 1));
        recordStore.saveRecord(createSimpleDocument(1630L, "Hello FoundationDB!", 1));
        return recordStore.saveRecord(createSimpleDocument(1631L, ENGINEER_JOKE, 1)).getRecordType();
    }

    private void addIndexAndSaveRecordsForAutoCompleteOfPhrase(@Nonnull FDBRecordContext context, @Nonnull Index index) {
        openRecordStore(context, metaDataBuilder -> {
            metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
            metaDataBuilder.addIndex(SIMPLE_DOC, index);
        });

        recordStore.saveRecord(createSimpleDocument(1623L, "united states of america", 1));
        recordStore.saveRecord(createSimpleDocument(1624L, "welcome to the united states of america", 1));
        recordStore.saveRecord(createSimpleDocument(1625L, "united kingdom, france, the states", 1));
        recordStore.saveRecord(createSimpleDocument(1626L, "The countries are united kingdom, france, the states", 1));
        recordStore.saveRecord(createSimpleDocument(1627L, "states united as a country", 1));
        recordStore.saveRecord(createSimpleDocument(1628L, "all the states united as a country", 1));
        recordStore.saveRecord(createSimpleDocument(1629L, "states have been united as a country", 1));
        recordStore.saveRecord(createSimpleDocument(1630L, "all the states have been united as a country", 1));
        recordStore.saveRecord(createSimpleDocument(1631L, "united states is a country in the continent of america", 1));
    }

    private void queryAndAssertAutoCompleteSuggestionsReturned(@Nonnull Index index, @Nonnull List<KeyExpression> storedFields,
                                                               @Nonnull String queriedField,
                                                               @Nonnull String searchKey, @Nonnull List<String> expectedSuggestions) throws Exception {
        final RecordQueryPlan luceneIndexPlan =
                LuceneIndexQueryPlan.of(index.getName(),
                        autoCompleteScanParams(searchKey, ImmutableSet.of(queriedField)),
                        RecordQueryFetchFromPartialRecordPlan.FetchIndexRecords.PRIMARY_KEY,
                        false,
                        null,
                        storedFields);
        final List<FDBQueriedRecord<Message>> results =
                recordStore.executeQuery(luceneIndexPlan, null, ExecuteProperties.SERIAL_EXECUTE)
                        .asList().get();

        assertEquals(expectedSuggestions.size(), results.size());
        List<String> suggestions = results.stream()
                .map(FDBQueriedRecord::getRecord)
                .map(record -> {
                    final Descriptors.Descriptor descriptor = record.getDescriptorForType();
                    final Descriptors.FieldDescriptor fieldDescriptor = descriptor.findFieldByName(queriedField);
                    Assertions.assertTrue(record.hasField(fieldDescriptor));
                    return Verify.verifyNotNull((String)record.getField(fieldDescriptor));
                })
                .collect(Collectors.toList());
        assertThat(suggestions, containsInAnyOrder(expectedSuggestions.stream().map(Matchers::equalTo).collect(Collectors.toList())));
    }

    private void rebuildIndexMetaData(final FDBRecordContext context, final String document, final Index index) {
        Pair<FDBRecordStore, QueryPlanner> pair = LuceneIndexTestUtils.rebuildIndexMetaData(context, path, document, index, useCascadesPlanner);
        this.recordStore = pair.getLeft();
        this.planner = pair.getRight();
    }

    private void assertIndexEntryPrimaryKeys(Collection<Long> primaryKeys, RecordCursor<IndexEntry> cursor) {
        List<IndexEntry> indexEntries = cursor.asList().join();
        assertEquals(primaryKeys.stream().map(Tuple::from).collect(Collectors.toSet()),
                indexEntries.stream().map(IndexEntry::getPrimaryKey).collect(Collectors.toSet()));
    }

    private void assertIndexEntryPrimaryKeyTuples(Set<Tuple> primaryKeys, RecordCursor<IndexEntry> cursor) {
        List<IndexEntry> indexEntries = cursor.asList().join();
        assertEquals(primaryKeys,
                indexEntries.stream().map(IndexEntry::getPrimaryKey).collect(Collectors.toSet()));
    }


    /**
     * A testing analyzer factory to verify the logic for {@link AnalyzerChooser}.
     */
    @AutoService(LuceneAnalyzerFactory.class)
    public static class TestAnalyzerFactory implements LuceneAnalyzerFactory {
        private static final String ANALYZER_FACTORY_NAME = "TEST_ANALYZER";

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
}
