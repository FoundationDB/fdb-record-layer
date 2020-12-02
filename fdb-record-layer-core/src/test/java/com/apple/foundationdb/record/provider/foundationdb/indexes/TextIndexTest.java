/*
 * TextIndexTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.map.BunchedMap;
import com.apple.foundationdb.map.BunchedMapMultiIterator;
import com.apple.foundationdb.map.BunchedMapScanEntry;
import com.apple.foundationdb.map.SubspaceSplitter;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.ExecuteProperties;
import com.apple.foundationdb.record.IndexEntry;
import com.apple.foundationdb.record.PlanHashable;
import com.apple.foundationdb.record.RecordCoreArgumentException;
import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorIterator;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.TestRecordsTextProto.ComplexDocument;
import com.apple.foundationdb.record.TestRecordsTextProto.MapDocument;
import com.apple.foundationdb.record.TestRecordsTextProto.SimpleDocument;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.logging.KeyValueLogMessage;
import com.apple.foundationdb.record.logging.LogMessageKeys;
import com.apple.foundationdb.record.logging.TestLogMessageKeys;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexOptions;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.MetaDataException;
import com.apple.foundationdb.record.metadata.RecordTypeBuilder;
import com.apple.foundationdb.record.metadata.expressions.EmptyKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.GroupingKeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression;
import com.apple.foundationdb.record.metadata.expressions.KeyExpression.FanType;
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.common.text.AllSuffixesTextTokenizer;
import com.apple.foundationdb.record.provider.common.text.DefaultTextTokenizer;
import com.apple.foundationdb.record.provider.common.text.DefaultTextTokenizerFactory;
import com.apple.foundationdb.record.provider.common.text.FilteringTextTokenizer;
import com.apple.foundationdb.record.provider.common.text.PrefixTextTokenizer;
import com.apple.foundationdb.record.provider.common.text.TextSamples;
import com.apple.foundationdb.record.provider.common.text.TextTokenizer;
import com.apple.foundationdb.record.provider.common.text.TextTokenizerFactory;
import com.apple.foundationdb.record.provider.common.text.TextTokenizerRegistryImpl;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBExceptions;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoredRecord;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.AndOrComponent;
import com.apple.foundationdb.record.query.expressions.Comparisons;
import com.apple.foundationdb.record.query.expressions.ComponentWithComparison;
import com.apple.foundationdb.record.query.expressions.FieldWithComparison;
import com.apple.foundationdb.record.query.expressions.OrComponent;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.RecordQueryPlanner;
import com.apple.foundationdb.record.query.plan.match.PlanMatchers;
import com.apple.foundationdb.record.query.plan.planning.BooleanNormalizer;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.TupleHelpers;
import com.apple.foundationdb.util.LoggableException;
import com.apple.test.Tags;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import org.apache.commons.lang3.tuple.Pair;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.IndexScanType.BY_GROUP;
import static com.apple.foundationdb.record.IndexScanType.BY_RANK;
import static com.apple.foundationdb.record.IndexScanType.BY_TEXT_TOKEN;
import static com.apple.foundationdb.record.IndexScanType.BY_TIME_WINDOW;
import static com.apple.foundationdb.record.IndexScanType.BY_VALUE;
import static com.apple.foundationdb.record.RecordCursor.NoNextReason.RETURN_LIMIT_REACHED;
import static com.apple.foundationdb.record.RecordCursor.NoNextReason.SCAN_LIMIT_REACHED;
import static com.apple.foundationdb.record.RecordCursor.NoNextReason.SOURCE_EXHAUSTED;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.concatenateFields;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexBunchedSerializerTest.entryOf;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.COMPLEX_DOC;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.SIMPLE_DOC;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.bounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.coveringIndexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.descendant;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.fetch;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.filter;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.groupingBounds;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.hasTupleString;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.indexName;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.primaryKeyDistinct;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.textComparison;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.textIndexScan;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.typeFilter;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.unbounded;
import static com.apple.foundationdb.record.query.plan.match.PlanMatchers.unorderedUnion;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@code TEXT} type indexes.
 */
@Tag(Tags.RequiresFDB)
@SuppressWarnings({"squid:S1192", "squid:S00112"}) // constant definition, throwing "Exception"
public class TextIndexTest extends FDBRecordStoreTestBase {
    private static final TextTokenizerFactory FILTERING_TOKENIZER = FilteringTextTokenizer.create(
            "filter_by_length$" + TextIndexTest.class.getCanonicalName(),
            new DefaultTextTokenizerFactory(),
            (token, version) -> token.length() < 10
    );
    private static final Logger LOGGER = LoggerFactory.getLogger(TextIndexTest.class);
    private static final BunchedMap<Tuple, List<Integer>> BUNCHED_MAP = new BunchedMap<>(TextIndexBunchedSerializer.instance(), Comparator.naturalOrder(), 20);
    private static final Index COMPLEX_TEXT_BY_GROUP = new Index("Complex$text_by_group", field("text").groupBy(field("group")), IndexTypes.TEXT);
    private static final Index SIMPLE_TEXT_PREFIX = new Index("Simple$text_prefix", field("text"), IndexTypes.TEXT,
            ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, PrefixTextTokenizer.NAME, IndexOptions.TEXT_TOKENIZER_VERSION_OPTION, "1"));
    private static final Index SIMPLE_TEXT_FILTERING = new Index("Simple$text_filter", field("text"), IndexTypes.TEXT,
            ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, FILTERING_TOKENIZER.getName()));
    private static final Index SIMPLE_TEXT_PREFIX_LEGACY = new Index("Simple$text_prefix", field("text"), IndexTypes.TEXT,
            ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, PrefixTextTokenizer.NAME)); // no version -- default to zero
    private static final Index SIMPLE_TEXT_SUFFIXES = new Index("Simple$text_suffixes", field("text"), IndexTypes.TEXT,
            ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, AllSuffixesTextTokenizer.NAME));
    private static final Index SIMPLE_TEXT_NO_POSITIONS = new Index("Simple$text_no_positions", field("text"), IndexTypes.TEXT,
            ImmutableMap.of(IndexOptions.TEXT_OMIT_POSITIONS_OPTION, "true"));
    private static final Index COMBINED_TEXT_BY_GROUP = new Index("Combined$text_by_group", field("text").groupBy(field("group")), IndexTypes.TEXT);
    private static final Index COMPLEX_MULTI_TAG_INDEX = new Index("Complex$multi_tag", field("text").groupBy(field("tag", FanType.FanOut)), IndexTypes.TEXT);
    private static final Index COMPLEX_THEN_TAG_INDEX = new Index("Complex$text_tag", concat(field("text"), field("tag", FanType.FanOut)), IndexTypes.TEXT);
    private static final Index MULTI_TYPE_INDEX = new Index("Simple&Complex$text", field("text"), IndexTypes.TEXT);
    private static final Index MAP_ON_VALUE_INDEX = new Index("Map$entry-value", new GroupingKeyExpression(field("entry", FanType.FanOut).nest(concatenateFields("key", "value")), 1), IndexTypes.TEXT);
    private static final Index MAP_ON_VALUE_PREFIX_LEGACY = new Index("Map$entry-value_prefix", new GroupingKeyExpression(field("entry", FanType.FanOut).nest(concatenateFields("key", "value")), 1), IndexTypes.TEXT,
            ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, PrefixTextTokenizer.NAME, IndexOptions.TEXT_TOKENIZER_VERSION_OPTION, "0"));
    private static final Index MAP_ON_VALUE_PREFIX = new Index("Map$entry-value_prefix", new GroupingKeyExpression(field("entry", FanType.FanOut).nest(concatenateFields("key", "value")), 1), IndexTypes.TEXT,
            ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, PrefixTextTokenizer.NAME, IndexOptions.TEXT_TOKENIZER_VERSION_OPTION, "1"));
    private static final Index MAP_ON_VALUE_GROUPED_INDEX = new Index("Map$entry-value_by_group", new GroupingKeyExpression(concat(field("group"), field("entry", FanType.FanOut).nest(concatenateFields("key", "value"))), 1), IndexTypes.TEXT);
    private static final String MAP_DOC = "MapDocument";

    @BeforeEach
    public void resetRegistry() {
        TextTokenizerRegistryImpl.instance().reset();
    }

    protected void openRecordStore(FDBRecordContext context) throws Exception {
        openRecordStore(context, store -> { });
    }

    protected void openRecordStore(FDBRecordContext context, RecordMetaDataHook hook) {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsTextProto.getDescriptor());
        metaDataBuilder.getRecordType(COMPLEX_DOC).setPrimaryKey(concatenateFields("group", "doc_id"));
        hook.apply(metaDataBuilder);
        recordStore = getStoreBuilder(context, metaDataBuilder.getRecordMetaData())
                .setSerializer(TextIndexTestUtils.COMPRESSING_SERIALIZER)
                .uncheckedOpen();
        setupPlanner(null);
    }

    @Nonnull
    private static FDBStoreTimer getTimer(@Nonnull FDBRecordStore recordStore) {
        final FDBStoreTimer timer = recordStore.getTimer();
        assertNotNull(timer, "store has not been initialized with a timer");
        return timer;
    }

    private static void resetTimer(@Nonnull FDBRecordStore recordStore) {
        getTimer(recordStore).reset();
    }

    private static int getCount(@Nonnull FDBRecordStore recordStore, @Nonnull StoreTimer.Event event) {
        return getTimer(recordStore).getCount(event);
    }

    private static int getLoadIndexKeyCount(@Nonnull FDBRecordStore recordStore) {
        return getCount(recordStore, FDBStoreTimer.Counts.LOAD_INDEX_KEY);
    }

    private static int getSaveIndexKeyCount(@Nonnull FDBRecordStore recordStore) {
        return getCount(recordStore, FDBStoreTimer.Counts.SAVE_INDEX_KEY);
    }

    private static int getSaveIndexKeyBytes(@Nonnull FDBRecordStore recordStore) {
        return getCount(recordStore, FDBStoreTimer.Counts.SAVE_INDEX_KEY_BYTES);
    }

    private static int getSaveIndexValueBytes(@Nonnull FDBRecordStore recordStore) {
        return getCount(recordStore, FDBStoreTimer.Counts.SAVE_INDEX_VALUE_BYTES);
    }

    private static int getDeleteIndexKeyCount(@Nonnull FDBRecordStore recordStore) {
        return getCount(recordStore, FDBStoreTimer.Counts.DELETE_INDEX_KEY);
    }

    private static int getDeleteIndexKeyBytes(@Nonnull FDBRecordStore recordStore) {
        return getCount(recordStore, FDBStoreTimer.Counts.DELETE_INDEX_KEY_BYTES);
    }

    private static int getDeleteIndexValueBytes(@Nonnull FDBRecordStore recordStore) {
        return getCount(recordStore, FDBStoreTimer.Counts.DELETE_INDEX_VALUE_BYTES);
    }

    private static int getLoadTextEntryCount(@Nonnull FDBRecordStore recordStore) {
        return getCount(recordStore, FDBStoreTimer.Counts.LOAD_TEXT_ENTRY);
    }

    private static void validateSorted(@Nonnull List<IndexEntry> entryList) {
        if (entryList.isEmpty()) {
            return;
        }
        IndexEntry last = null;
        for (IndexEntry entry : entryList) {
            if (last != null) {
                assertThat(entry.getKey(), greaterThan(last.getKey()));
            }
            last = entry;
        }
    }

    @Nonnull
    private static List<IndexEntry> scanIndex(@Nonnull FDBRecordStore store, @Nonnull Index index, @Nonnull TupleRange range, @Nonnull ScanProperties scanProperties) throws ExecutionException, InterruptedException {
        return store.scanIndex(index, BY_TEXT_TOKEN, range, null, scanProperties).asList().get();
    }

    @Nonnull
    private static List<IndexEntry> scanIndex(@Nonnull FDBRecordStore store, @Nonnull Index index, @Nonnull TupleRange range) throws ExecutionException, InterruptedException {
        List<IndexEntry> results = scanIndex(store, index, range, ScanProperties.FORWARD_SCAN);
        validateSorted(results);
        List<IndexEntry> backwardResults = new ArrayList<>(scanIndex(store, index, range, ScanProperties.REVERSE_SCAN));
        Collections.reverse(backwardResults);
        assertEquals(results, backwardResults);

        // Validate that
        final int limit = 3;
        for (int i = 0; i < 8; i++) {
            ExecuteProperties.Builder propertiesBuilder = ExecuteProperties.newBuilder();
            if (i < 4) {
                propertiesBuilder.setReturnedRowLimit(limit);
            } else {
                propertiesBuilder.setScannedRecordsLimit(limit);
            }
            List<IndexEntry> paginatedResults = new ArrayList<>(results.size());
            boolean done = false;
            byte[] continuation = null;
            do {
                if (i >= 2 && i < 4) {
                    // Use skip instead of continuation to achieve the same results.
                    continuation = null;
                    propertiesBuilder.setSkip(paginatedResults.size());
                }
                ScanProperties scanProperties = propertiesBuilder.build().asScanProperties(i % 2 == 0);
                int retrieved = 0;
                RecordCursorIterator<IndexEntry> cursor = store.scanIndex(index, BY_TEXT_TOKEN, range, continuation, scanProperties).asIterator();
                while (cursor.hasNext()) {
                    paginatedResults.add(cursor.next());
                    retrieved++;
                }
                if (done) {
                    assertEquals(0, retrieved);
                    assertNull(cursor.getContinuation());
                }
                if (retrieved < limit) {
                    assertEquals(SOURCE_EXHAUSTED, cursor.getNoNextReason());
                } else {
                    assertEquals(limit, retrieved);
                    assertEquals(i < 4 ? RETURN_LIMIT_REACHED : SCAN_LIMIT_REACHED, cursor.getNoNextReason());
                }
                done = cursor.getNoNextReason().isSourceExhausted();
                continuation = cursor.getContinuation();
            } while (continuation != null);

            if (i % 2 == 0) {
                Collections.reverse(paginatedResults);
            }
            assertEquals(results, paginatedResults);
        }

        return results;
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    private List<Map.Entry<Tuple, List<Integer>>> toMapEntries(@Nonnull List<IndexEntry> indexEntries, @Nullable Tuple prefix) {
        List<Map.Entry<Tuple, List<Integer>>> mapEntries = new ArrayList<>(indexEntries.size());
        for (IndexEntry entry : indexEntries) {
            List<Integer> positionList = (List<Integer>)entry.getValue().get(0);
            if (prefix == null) {
                mapEntries.add(entryOf(entry.getKey(), positionList));
            } else {
                assertEquals(TupleHelpers.subTuple(entry.getKey(), 0, prefix.size()), prefix);
                mapEntries.add(entryOf(TupleHelpers.subTuple(entry.getKey(), prefix.size(), entry.getKey().size()), positionList));
            }
        }
        return mapEntries;
    }

    @Nonnull
    private List<Map.Entry<Tuple, List<Integer>>> scanMapEntries(@Nonnull FDBRecordStore store, @Nonnull Index index, @Nonnull Tuple prefix) throws ExecutionException, InterruptedException {
        return toMapEntries(scanIndex(store, index, TupleRange.allOf(prefix)), prefix);
    }

    @Nonnull
    private List<BunchedMapScanEntry<Tuple, List<Integer>, String>> scanMulti(@Nonnull FDBRecordStore store, @Nonnull Subspace mapSubspace) throws ExecutionException, InterruptedException {
        SubspaceSplitter<String> splitter = new SubspaceSplitter<String>() {
            @Nonnull
            @Override
            public Subspace subspaceOf(@Nonnull byte[] keyBytes) {
                Tuple t = mapSubspace.unpack(keyBytes);
                return mapSubspace.subspace(TupleHelpers.subTuple(t, 0, 1));
            }

            @Nonnull
            @Override
            public String subspaceTag(@Nonnull Subspace subspace) {
                return mapSubspace.unpack(subspace.getKey()).getString(0);
            }
        };
        BunchedMapMultiIterator<Tuple, List<Integer>, String> iterator = BUNCHED_MAP.scanMulti(store.ensureContextActive(), mapSubspace, splitter);
        return AsyncUtil.collectRemaining(iterator).get();
    }

    @Nonnull
    private List<Pair<Tuple, Integer>> scanTokenizerVersions(@Nonnull FDBRecordStore store, @Nonnull Index index) throws ExecutionException, InterruptedException {
        final Subspace tokenizerVersionSubspace = store.indexSecondarySubspace(index).subspace(TextIndexMaintainer.TOKENIZER_VERSION_SUBSPACE_TUPLE);
        return recordStore.ensureContextActive().getRange(tokenizerVersionSubspace.range()).asList().get().stream()
                .map(kv -> Pair.of(tokenizerVersionSubspace.unpack(kv.getKey()), (int)Tuple.fromBytes(kv.getValue()).getLong(0)))
                .collect(Collectors.toList());
    }

    @Test
    public void saveSimpleDocuments() throws Exception {
        final SimpleDocument simpleDocument = SimpleDocument.newBuilder()
                .setDocId(1066L)
                .setText("This is a simple document. There isn't much going on here, if I'm honest.")
                .setGroup(0)
                .build();
        final SimpleDocument buffaloDocument = SimpleDocument.newBuilder()
                .setDocId(1415L)
                .setText("Buffalo buffalo Buffalo buffalo buffalo buffalo Buffalo buffalo Buffalo buffalo buffalo.")
                .setGroup(1)
                .build();
        final SimpleDocument shakespeareDocument = SimpleDocument.newBuilder()
                .setDocId(1623L)
                .setText(TextSamples.ROMEO_AND_JULIET_PROLOGUE)
                .setGroup(2)
                .build();
        final SimpleDocument noTextDocument = SimpleDocument.newBuilder()
                .setDocId(0L)
                .setGroup(0)
                .build();
        final SimpleDocument emptyDocument = SimpleDocument.newBuilder()
                .setDocId(1L)
                .setGroup(1)
                .setText("")
                .build();

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);

            Index index = recordStore.getRecordMetaData().getIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);

            recordStore.saveRecord(simpleDocument);
            final int firstKeys = getSaveIndexKeyCount(recordStore);
            assertEquals(simpleDocument.getText().split(" ").length, firstKeys);
            final int firstKeyBytesWritten = getSaveIndexKeyBytes(recordStore);
            final int firstValueBytesWritten = getSaveIndexValueBytes(recordStore);
            List<Map.Entry<Tuple, List<Integer>>> entryList = scanMapEntries(recordStore, index, Tuple.from("document"));
            assertEquals(Collections.singletonList(entryOf(Tuple.from(1066L), Collections.singletonList(4))), entryList);

            resetTimer(recordStore);
            recordStore.saveRecord(buffaloDocument);
            final int secondKeys = getSaveIndexKeyCount(recordStore);
            assertEquals(1, secondKeys);
            entryList = scanMapEntries(recordStore, index, Tuple.from("buffalo"));
            assertEquals(Collections.singletonList(entryOf(Tuple.from(1415L), IntStream.range(0, 11).boxed().collect(Collectors.toList()))), entryList);

            resetTimer(recordStore);
            recordStore.saveRecord(shakespeareDocument);
            final int thirdKeys = getSaveIndexKeyCount(recordStore);
            assertEquals(82, thirdKeys);
            final int thirdBytesWritten = getSaveIndexKeyBytes(recordStore) + getSaveIndexValueBytes(recordStore);
            entryList = scanMapEntries(recordStore, index, Tuple.from("parents"));
            assertEquals(Collections.singletonList(entryOf(Tuple.from(1623L), Arrays.asList(57, 72))), entryList);

            entryList = toMapEntries(scanIndex(recordStore, index, TupleRange.prefixedBy("h")), null);
            assertEquals(Arrays.asList(
                    entryOf(Tuple.from("hands", 1623), Collections.singletonList(26)),
                    entryOf(Tuple.from("here", 1066), Collections.singletonList(10)),
                    entryOf(Tuple.from("here", 1623), Collections.singletonList(101)),
                    entryOf(Tuple.from("honest", 1066), Collections.singletonList(13)),
                    entryOf(Tuple.from("hours", 1623), Collections.singletonList(87)),
                    entryOf(Tuple.from("households", 1623), Collections.singletonList(1))
            ), entryList);
            List<Message> recordList = recordStore.scanIndexRecords(index.getName(), BY_TEXT_TOKEN, TupleRange.prefixedBy("h"), null, ScanProperties.FORWARD_SCAN)
                    .map(FDBIndexedRecord::getRecord)
                    .asList()
                    .get();
            assertEquals(Arrays.asList(shakespeareDocument, simpleDocument, shakespeareDocument, simpleDocument, shakespeareDocument, shakespeareDocument), recordList);

            resetTimer(recordStore);
            recordStore.saveRecord(noTextDocument);
            assertEquals(0, getSaveIndexKeyCount(recordStore));
            assertEquals(0, getLoadIndexKeyCount(recordStore));

            resetTimer(recordStore);
            recordStore.saveRecord(emptyDocument);
            assertEquals(0, getSaveIndexKeyCount(recordStore));
            assertEquals(0, getLoadIndexKeyCount(recordStore));

            resetTimer(recordStore);
            recordStore.deleteRecord(Tuple.from(1623L));
            assertEquals(thirdKeys - 4, getDeleteIndexKeyCount(recordStore)); // all deleted but four overlaps with first record
            assertEquals(4, getSaveIndexKeyCount(recordStore)); // four keys of overlap overwritten
            assertThat(getDeleteIndexKeyBytes(recordStore) + getDeleteIndexValueBytes(recordStore), allOf(greaterThan(thirdKeys - 1), lessThan(thirdBytesWritten)));
            entryList = scanMapEntries(recordStore, index, Tuple.from("parents"));
            assertEquals(Collections.emptyList(), entryList);

            resetTimer(recordStore);
            recordStore.saveRecord(simpleDocument.toBuilder().setDocId(1707L).build());
            assertEquals(firstKeys * 2, getLoadIndexKeyCount(recordStore));
            assertEquals(firstKeys, getSaveIndexKeyCount(recordStore));
            assertEquals(firstKeyBytesWritten, getSaveIndexKeyBytes(recordStore)); // should overwrite all the same keys
            final int seventhValueBytesWritten = getSaveIndexValueBytes(recordStore);
            assertThat(seventhValueBytesWritten, allOf(greaterThan(firstValueBytesWritten), lessThan(firstKeyBytesWritten + firstValueBytesWritten))); // contains same info as first value bytes + extra keys, but not key prefixes
            entryList = scanMapEntries(recordStore, index, Tuple.from("document"));
            assertEquals(Arrays.asList(entryOf(Tuple.from(1066L), Collections.singletonList(4)), entryOf(Tuple.from(1707L), Collections.singletonList(4))), entryList);

            resetTimer(recordStore);
            recordStore.deleteRecord(Tuple.from(1066L));
            assertEquals(firstKeys, getLoadIndexKeyCount(recordStore));
            assertEquals(firstKeys, getDeleteIndexKeyCount(recordStore)); // each of the original keys are deleted
            assertEquals(firstKeyBytesWritten, getDeleteIndexKeyBytes(recordStore));
            assertEquals(firstValueBytesWritten + seventhValueBytesWritten, getDeleteIndexValueBytes(recordStore));
            assertEquals(firstKeys, getSaveIndexKeyCount(recordStore)); // a new set of keys are all written
            assertEquals(firstKeyBytesWritten, getSaveIndexKeyBytes(recordStore)); // they should have the same size (though their contents are different)
            assertEquals(firstValueBytesWritten, getSaveIndexValueBytes(recordStore));
            entryList = scanMapEntries(recordStore, index, Tuple.from("document"));
            assertEquals(Collections.singletonList(entryOf(Tuple.from(1707L), Collections.singletonList(4))), entryList);

            commit(context);
        }
    }

    // An older implementation did reverse range scan to find the keys before and after in order
    // to find where insertions should go. This was able to reproduce an error where two keys could
    // be returned after the scan that were both greater than the map key due to a race condition.
    // This was able to reproduce the error when run alone.
    @Test
    public void backwardsRangeScanRaceCondition() throws Exception {
        final Random r = new Random(0x5ca1ab1e);
        final List<String> lexicon = Arrays.asList(TextSamples.ROMEO_AND_JULIET_PROLOGUE.split(" "));
        final SimpleDocument bigDocument = getRandomRecords(r, 1, lexicon, 100, 0).get(0);
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> metaDataBuilder.setSplitLongRecords(true));
            LOGGER.info(KeyValueLogMessage.of("saving document", LogMessageKeys.DOCUMENT, bigDocument));
            recordStore.saveRecord(bigDocument);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> metaDataBuilder.setSplitLongRecords(true));
            recordStore.deleteRecord(Tuple.from(bigDocument.getDocId()));
            recordStore.saveRecord(bigDocument);
            // do not commit
        } catch (RuntimeException e) {
            Throwable err = e;
            while (!(err instanceof LoggableException) && err != null) {
                err = err.getCause();
            }
            if (err != null) {
                LoggableException logE = (LoggableException) err;
                LOGGER.error(KeyValueLogMessage.build("unable to save record")
                        .addKeysAndValues(logE.getLogInfo())
                        .toString(), err);
                throw logE;
            } else {
                throw e;
            }
        }
    }

    @Test
    public void saveSimpleDocumentsWithPrefix() throws Exception {
        final SimpleDocument shakespeareDocument = SimpleDocument.newBuilder()
                .setDocId(1623L)
                .setText(TextSamples.ROMEO_AND_JULIET_PROLOGUE)
                .setGroup(2)
                .build();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_PREFIX_LEGACY);
            });
            recordStore.saveRecord(shakespeareDocument);
            assertEquals(74, getSaveIndexKeyCount(recordStore));

            List<Map.Entry<Tuple, List<Integer>>> entryList = scanMapEntries(recordStore, SIMPLE_TEXT_PREFIX_LEGACY, Tuple.from("par"));
            assertEquals(Collections.singletonList(entryOf(Tuple.from(1623L), Arrays.asList(57, 72))), entryList);

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore.deleteStore(context, recordStore.getSubspace());
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_PREFIX);
            });
            recordStore.saveRecord(shakespeareDocument);
            assertEquals(79, getSaveIndexKeyCount(recordStore));
            List<Map.Entry<Tuple, List<Integer>>> entryList = scanMapEntries(recordStore, SIMPLE_TEXT_PREFIX, Tuple.from("par"));
            assertEquals(Collections.emptyList(), entryList);
            entryList = scanMapEntries(recordStore, SIMPLE_TEXT_PREFIX, Tuple.from("pare"));
            assertEquals(Collections.singletonList(entryOf(Tuple.from(1623L), Arrays.asList(57, 72))), entryList);
            commit(context);
        }
    }

    @Test
    public void saveSimpleDocumentsWithFilter() throws Exception {
        final SimpleDocument russianDocument = SimpleDocument.newBuilder()
                .setDocId(1547L)
                .setText(TextSamples.RUSSIAN)
                .build();

        try (FDBRecordContext context = openContext()) {
            // Because missing tokenizer
            assertThrows(MetaDataException.class, () -> openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_FILTERING);
            }));
        }
        TextTokenizerRegistryImpl.instance().register(FILTERING_TOKENIZER);
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_FILTERING);
            });
            recordStore.saveRecord(russianDocument);
            // Note that достопримечательности has been filtered out, so it's probably a
            // lot less interesting to visit.
            assertEquals(4, getSaveIndexKeyCount(recordStore));
            List<Map.Entry<Tuple, List<Integer>>> entryList = scanMapEntries(recordStore, SIMPLE_TEXT_FILTERING, Tuple.from("достопримечательности"));
            assertEquals(Collections.emptyList(), entryList);
            entryList = scanMapEntries(recordStore, SIMPLE_TEXT_FILTERING, Tuple.from("москвы"));
            assertEquals(Collections.singletonList(entryOf(Tuple.from(1547L), Collections.singletonList(4))), entryList);
            commit(context);
        }
    }

    @Test
    public void saveSimpleDocumentsWithSuffixes() throws Exception {
        final SimpleDocument germanDocument = SimpleDocument.newBuilder()
                .setDocId(1623L)
                .setText(TextSamples.GERMAN)
                .setGroup(2)
                .build();
        final SimpleDocument russianDocument = SimpleDocument.newBuilder()
                .setDocId(1547L)
                .setText(TextSamples.RUSSIAN)
                .build();
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            });
            recordStore.saveRecord(germanDocument);
            assertEquals(82, getSaveIndexKeyCount(recordStore));
            recordStore.saveRecord(russianDocument);
            assertEquals(82 + 45, getSaveIndexKeyCount(recordStore));
            List<Map.Entry<Tuple, List<Integer>>> entryList = scanMapEntries(recordStore, SIMPLE_TEXT_SUFFIXES, Tuple.from("mannschaft"));
            assertEquals(Collections.singletonList(entryOf(Tuple.from(1623L), Collections.singletonList(11))), entryList);
            entryList = scanMapEntries(recordStore, SIMPLE_TEXT_SUFFIXES, Tuple.from("schaft"));
            assertEquals(Collections.singletonList(entryOf(Tuple.from(1623L), Arrays.asList(15, 38))), entryList);
            entryList = scanMapEntries(recordStore, SIMPLE_TEXT_SUFFIXES, Tuple.from("ности"));
            assertEquals(Collections.singletonList(entryOf(Tuple.from(1547L), Collections.singletonList(34))), entryList);
            commit(context);
        }
    }

    @Test
    public void saveSimpleDocumentsWithNoPositions() throws Exception {
        final SimpleDocument shakespeareDocument = SimpleDocument.newBuilder()
                .setDocId(1623L)
                .setText(TextSamples.ROMEO_AND_JULIET_PROLOGUE)
                .build();
        final SimpleDocument germanDocument = SimpleDocument.newBuilder()
                .setDocId(1066L)
                .setText(TextSamples.GERMAN)
                .build();

        // Save with positions
        final int shakespeareValueBytes;
        final int germanValueBytes;
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            recordStore.saveRecord(shakespeareDocument);
            shakespeareValueBytes = getSaveIndexValueBytes(recordStore);
            recordStore.saveRecord(germanDocument);
            germanValueBytes = getSaveIndexValueBytes(recordStore) - shakespeareValueBytes;
            commit(context);
        }

        // Save without positions
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_NO_POSITIONS);
            });
            recordStore.deleteAllRecords();
            recordStore.saveRecord(shakespeareDocument);
            assertThat(getSaveIndexValueBytes(recordStore), lessThan(shakespeareValueBytes));
            final int newShakespeareBytes = getSaveIndexValueBytes(recordStore);
            recordStore.saveRecord(germanDocument);
            assertThat(getSaveIndexValueBytes(recordStore) - newShakespeareBytes, lessThan(germanValueBytes));

            List<Map.Entry<Tuple, List<Integer>>> entryList = scanMapEntries(recordStore, SIMPLE_TEXT_NO_POSITIONS, Tuple.from("gewonnen"));
            assertEquals(Collections.singletonList(entryOf(Tuple.from(1066L), Collections.emptyList())), entryList);
            entryList = scanMapEntries(recordStore, SIMPLE_TEXT_NO_POSITIONS, Tuple.from("dignity"));
            assertEquals(Collections.singletonList(entryOf(Tuple.from(1623L), Collections.emptyList())), entryList);

            commit(context);
        }
    }

    @Test
    public void saveSimpleDocumentsWithPositionsOptionChange() throws Exception {
        final SimpleDocument shakespeareDocument = SimpleDocument.newBuilder()
                .setDocId(1623L)
                .setText(TextSamples.ROMEO_AND_JULIET_PROLOGUE)
                .build();
        final SimpleDocument yiddishDocument = SimpleDocument.newBuilder()
                .setDocId(1945L)
                .setText(TextSamples.YIDDISH)
                .build();
        final SimpleDocument frenchDocument = SimpleDocument.newBuilder()
                .setDocId(1871L)
                .setText(TextSamples.FRENCH)
                .build();

        // Save one document *with* positions
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.addIndex(SIMPLE_DOC, new Index(SIMPLE_TEXT_NO_POSITIONS.getName(), SIMPLE_TEXT_NO_POSITIONS.getRootExpression(), IndexTypes.TEXT));
            });
            recordStore.saveRecord(shakespeareDocument);
            commit(context);
        }
        // Save one document *without* positions
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_NO_POSITIONS);
            });
            recordStore.saveRecord(yiddishDocument);
            commit(context);
        }
        // Save one more document *with* positions
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.addIndex(SIMPLE_DOC, new Index(SIMPLE_TEXT_NO_POSITIONS.getName(), SIMPLE_TEXT_NO_POSITIONS.getRootExpression(), IndexTypes.TEXT));
            });
            recordStore.saveRecord(frenchDocument);

            List<Map.Entry<Tuple, List<Integer>>> entryList = scanMapEntries(recordStore, SIMPLE_TEXT_NO_POSITIONS, Tuple.from("civil"));
            assertEquals(Collections.singletonList(entryOf(Tuple.from(1623L), Arrays.asList(22, 25))), entryList);
            entryList = scanMapEntries(recordStore, SIMPLE_TEXT_NO_POSITIONS, Tuple.from("דיאלעקט"));
            assertEquals(Collections.singletonList(entryOf(Tuple.from(1945L), Collections.emptyList())), entryList);
            entryList = scanMapEntries(recordStore, SIMPLE_TEXT_NO_POSITIONS, Tuple.from("recu"));
            assertEquals(Collections.singletonList(entryOf(Tuple.from(1871L), Collections.singletonList(5))), entryList);

            commit(context);
        }
    }

    @Test
    public void saveComplexDocuments() throws Exception {
        ComplexDocument complexDocument = ComplexDocument.newBuilder()
                .setGroup(0)
                .setDocId(1066L)
                .setText("Very complex. Not to be trifled with.")
                .build();
        ComplexDocument shakespeareDocument = ComplexDocument.newBuilder()
                .setGroup(0)
                .setDocId(1623L)
                .setText(TextSamples.ROMEO_AND_JULIET_PROLOGUE)
                .addTag("a")
                .addTag("b")
                .build();
        ComplexDocument yiddishDocument = ComplexDocument.newBuilder()
                .setGroup(1)
                .setDocId(1944L)
                .setText(TextSamples.YIDDISH)
                .addTag("c")
                .build();

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> metaDataBuilder.addIndex(COMPLEX_DOC, COMPLEX_TEXT_BY_GROUP));
            recordStore.saveRecord(complexDocument);
            int firstKeys = getSaveIndexKeyCount(recordStore);
            assertEquals(complexDocument.getText().split(" ").length, firstKeys);

            recordStore.saveRecord(shakespeareDocument);
            int secondKeys = getSaveIndexKeyCount(recordStore) - firstKeys;
            assertEquals(82, secondKeys);

            recordStore.saveRecord(yiddishDocument);
            int thirdKeys = getSaveIndexKeyCount(recordStore) - secondKeys - firstKeys;
            assertEquals(9, thirdKeys);

            List<Map.Entry<Tuple, List<Integer>>> entryList = scanMapEntries(recordStore, COMPLEX_TEXT_BY_GROUP, Tuple.from(0L, "to"));
            assertEquals(Arrays.asList(
                    entryOf(Tuple.from(1066L), Collections.singletonList(3)),
                    entryOf(Tuple.from(1623L), Arrays.asList(18, 108))
            ), entryList);
            List<Message> recordList = recordStore.scanIndexRecords(COMPLEX_TEXT_BY_GROUP.getName(), BY_TEXT_TOKEN, TupleRange.allOf(Tuple.from(0L, "to")), null, ScanProperties.FORWARD_SCAN)
                    .map(FDBIndexedRecord::getRecord)
                    .asList()
                    .get();
            assertEquals(Arrays.asList(complexDocument, shakespeareDocument), recordList);

            entryList = toMapEntries(scanIndex(recordStore, COMPLEX_TEXT_BY_GROUP, TupleRange.prefixedBy("א").prepend(Tuple.from(1L))), null);
            assertEquals(Arrays.asList(
                    entryOf(Tuple.from(1L, "א", 1944L), Arrays.asList(0, 3)),
                    entryOf(Tuple.from(1L, "און", 1944L), Collections.singletonList(8)),
                    entryOf(Tuple.from(1L, "איז", 1944L), Collections.singletonList(2)),
                    entryOf(Tuple.from(1L, "אן", 1944L), Collections.singletonList(6)),
                    entryOf(Tuple.from(1L, "ארמיי", 1944L), Collections.singletonList(7))
            ), entryList);

            // Read the whole store and make sure the values come back in a somewhat sensible way
            entryList = toMapEntries(scanIndex(recordStore, COMPLEX_TEXT_BY_GROUP, TupleRange.ALL), null);
            assertEquals(firstKeys + secondKeys + thirdKeys, entryList.size());
            int i = 0;
            String last = null;
            for (Map.Entry<Tuple, List<Integer>> entry : entryList) {
                assertEquals(3, entry.getKey().size());
                if (i < firstKeys + secondKeys) {
                    assertEquals(0L, entry.getKey().getLong(0));
                    assertThat(entry.getKey().getLong(2), anyOf(is(1066L), is(1623L)));
                } else {
                    assertEquals(1L, entry.getKey().getLong(0));
                    assertEquals(1944L, entry.getKey().getLong(2));
                    if (i == firstKeys + secondKeys) {
                        last = null;
                    }
                }
                if (last != null) {
                    assertThat(entry.getKey().getString(1), greaterThanOrEqualTo(last));
                }
                last = entry.getKey().getString(1);
                i++;
            }

            commit(context);
        }
    }

    @Test
    public void saveComplexMultiDocuments() throws Exception {
        final ComplexDocument shakespeareDocument = ComplexDocument.newBuilder()
                .setGroup(0)
                .setDocId(1623L)
                .setText(TextSamples.ROMEO_AND_JULIET_PROLOGUE)
                .addTag("a")
                .addTag("b")
                .build();

        // First, repeated key comes *before* text.
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> metaDataBuilder.addIndex(COMPLEX_DOC, COMPLEX_MULTI_TAG_INDEX));
            recordStore.saveRecord(shakespeareDocument);
            int indexKeys = getSaveIndexKeyCount(recordStore);
            assertEquals(164, indexKeys);
            List<Map.Entry<Tuple, List<Integer>>> entryList = scanMapEntries(recordStore, COMPLEX_MULTI_TAG_INDEX, Tuple.from("a", "civil"));
            assertEquals(Collections.singletonList(entryOf(Tuple.from(0L, 1623L), Arrays.asList(22, 25))), entryList);
            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            FDBRecordStore.deleteStore(context, recordStore.getSubspace());
            commit(context);
        }
        // Then, repeated key comes *after* text.
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> metaDataBuilder.addIndex(COMPLEX_DOC, COMPLEX_THEN_TAG_INDEX));
            recordStore.saveRecord(shakespeareDocument);
            int indexKeys = getSaveIndexKeyCount(recordStore);
            assertEquals(164, indexKeys);
            List<Map.Entry<Tuple, List<Integer>>> entryList = scanMapEntries(recordStore, COMPLEX_THEN_TAG_INDEX, Tuple.from("civil"));
            assertEquals(Arrays.asList(
                    entryOf(Tuple.from("a", 0L, 1623L), Arrays.asList(22, 25)),
                    entryOf(Tuple.from("b", 0L, 1623L), Arrays.asList(22, 25))
            ), entryList);
            commit(context);
        }
    }

    @Test
    public void saveMapDocuments() throws Exception {
        final MapDocument firstDocument = MapDocument.newBuilder()
                .setDocId(1066L)
                .addEntry(MapDocument.Entry.newBuilder()
                        .setKey("a")
                        .setValue(TextSamples.ANGSTROM)
                )
                .addEntry(MapDocument.Entry.newBuilder()
                        .setKey("b")
                        .setValue(TextSamples.ROMEO_AND_JULIET_PROLOGUE)
                )
                .build();
        final MapDocument secondDocument = MapDocument.newBuilder()
                .setDocId(1415L)
                .addEntry(MapDocument.Entry.newBuilder()
                        .setKey("a")
                        .setValue(TextSamples.OLD_S)
                )
                .addEntry(MapDocument.Entry.newBuilder()
                        .setKey("c")
                        .setValue(TextSamples.FRENCH)
                )
                .build();

        final MapDocument documentWithEmpties = MapDocument.newBuilder()
                .setDocId(1815L)
                .addEntry(MapDocument.Entry.newBuilder()
                        .setKey("a") // value intentionally left unset
                )
                .addEntry(MapDocument.Entry.newBuilder()
                        .setKey("b")
                        .setValue("")
                )
                .build();

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(MAP_DOC, MAP_ON_VALUE_INDEX);
            });

            recordStore.saveRecord(firstDocument);
            recordStore.saveRecord(secondDocument);
            recordStore.saveRecord(documentWithEmpties);

            assertEquals(Arrays.asList(entryOf(Tuple.from(1066L), Collections.singletonList(0)), entryOf(Tuple.from(1415L), Collections.singletonList(7))),
                    scanMapEntries(recordStore, MAP_ON_VALUE_INDEX, Tuple.from("a", "the")));
            assertEquals(Collections.singletonList(entryOf(Tuple.from(1066L), Arrays.asList(22, 25))),
                    scanMapEntries(recordStore, MAP_ON_VALUE_INDEX, Tuple.from("b", "civil")));
            assertEquals(Collections.singletonList(entryOf(Tuple.from(1415L), Collections.singletonList(4))),
                    scanMapEntries(recordStore, MAP_ON_VALUE_INDEX, Tuple.from("c", "a")));

            commit(context);
        }
    }

    @Test
    public void saveCombinedByGroup() throws Exception {
        final SimpleDocument simpleDocument = SimpleDocument.newBuilder()
                .setGroup(0)
                .setDocId(1907L)
                .setText(TextSamples.ANGSTROM)
                .build();
        final ComplexDocument complexDocument = ComplexDocument.newBuilder()
                .setGroup(0)
                .setDocId(966L)
                .setText(TextSamples.AETHELRED)
                .build();

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addMultiTypeIndex(
                        Arrays.asList(metaDataBuilder.getRecordType(COMPLEX_DOC), metaDataBuilder.getRecordType(SIMPLE_DOC)),
                        COMBINED_TEXT_BY_GROUP
                );
            });
            recordStore.saveRecord(simpleDocument);
            int firstKeys = getSaveIndexKeyCount(recordStore);
            assertEquals(8, firstKeys);
            recordStore.saveRecord(complexDocument);
            int secondKeys = getSaveIndexKeyCount(recordStore) - firstKeys;
            assertEquals(11, secondKeys);
            List<Map.Entry<Tuple, List<Integer>>> entryList = scanMapEntries(recordStore, COMBINED_TEXT_BY_GROUP, Tuple.from(0, "was"));
            assertEquals(Arrays.asList(
                    entryOf(Tuple.from(0L, 966L), Collections.singletonList(7)),
                    entryOf(Tuple.from(1907L), Collections.singletonList(4))
            ), entryList);
            commit(context);
        }
    }

    @Test
    private void saveTwoRecordsConcurrently(@Nonnull RecordMetaDataHook hook, @Nonnull Message record1, @Nonnull Message record2, boolean shouldSucceed) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            recordStore.saveRecord(record1);
            try (FDBRecordContext context2 = openContext()) {
                openRecordStore(context2, hook);
                recordStore.saveRecord(record2);
                commit(context2);
            }
            if (shouldSucceed) {
                commit(context);
            } else {
                assertThrows(FDBExceptions.FDBStoreTransactionConflictException.class, () -> commit(context));
            }
        }
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            Set<Message> records = new HashSet<>(recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN)
                    .map(FDBStoredRecord::getRecord)
                    .asList()
                    .get());
            Set<Message> expectedRecords = shouldSucceed ? ImmutableSet.of(record1, record2) : Collections.singleton(record2);
            assertEquals(expectedRecords, records);
            commit(context);
        }
    }

    @Test
    public void saveSimpleWithAggressiveConflictRanges() throws Exception {
        // These two documents are from different languages and thus have no conflicts, so
        // without the aggressive conflict ranges, they wouldn't conflict if not
        // for the aggressive conflict ranges
        final SimpleDocument shakespeareDocument = SimpleDocument.newBuilder()
                .setDocId(1623L)
                .setGroup(0)
                .setText(TextSamples.ROMEO_AND_JULIET_PROLOGUE)
                .build();
        final SimpleDocument yiddishDocument = SimpleDocument.newBuilder()
                .setDocId(1945L)
                .setGroup(0)
                .setText(TextSamples.YIDDISH)
                .build();
        final RecordMetaDataHook hook = metaDataBuilder -> {
            final Index oldIndex = metaDataBuilder.getIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
            metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
            final Index newIndex = new Index(TextIndexTestUtils.SIMPLE_DEFAULT_NAME + "-new", oldIndex.getRootExpression(), IndexTypes.TEXT,
                    ImmutableMap.of(IndexOptions.TEXT_ADD_AGGRESSIVE_CONFLICT_RANGES_OPTION, "true"));
            metaDataBuilder.addIndex(SIMPLE_DOC, newIndex);
        };
        saveTwoRecordsConcurrently(hook, shakespeareDocument, yiddishDocument, false);
    }

    @Test
    public void saveComplexWithAggressiveConflictRanges() throws Exception {
        // These two documents are in different groups, so even with aggressive conflict
        // ranges, they should be able to be committed concurrently.
        final ComplexDocument zeroGroupDocument = ComplexDocument.newBuilder()
                .setGroup(0)
                .setDocId(1623L)
                .setText(TextSamples.ROMEO_AND_JULIET_PROLOGUE)
                .build();
        final ComplexDocument oneGroupDocument = ComplexDocument.newBuilder()
                .setGroup(1)
                .setDocId(1623L)
                .setText(TextSamples.ROMEO_AND_JULIET_PROLOGUE)
                .build();
        final RecordMetaDataHook hook = metaDataBuilder -> {
            final Index newIndex = new Index(COMPLEX_TEXT_BY_GROUP.getName(), COMBINED_TEXT_BY_GROUP.getRootExpression(), IndexTypes.TEXT,
                    ImmutableMap.of(IndexOptions.TEXT_ADD_AGGRESSIVE_CONFLICT_RANGES_OPTION, "true"));
            metaDataBuilder.addIndex(COMPLEX_DOC, newIndex);
        };
        saveTwoRecordsConcurrently(hook, zeroGroupDocument, oneGroupDocument, true);
    }

    @Test
    public void tokenizerVersionChange() throws Exception {
        final SimpleDocument shakespeareDocument = SimpleDocument.newBuilder()
                .setDocId(1623L)
                .setText(TextSamples.ROMEO_AND_JULIET_PROLOGUE)
                .build();
        final SimpleDocument aethelredDocument1 = SimpleDocument.newBuilder()
                .setDocId(966L)
                .setText(TextSamples.AETHELRED)
                .build();
        final SimpleDocument aethelredDocument2 = SimpleDocument.newBuilder()
                .setDocId(1016L)
                .setText(TextSamples.AETHELRED)
                .build();

        try (FDBRecordContext context = openContext()) {
            // Use a version of the prefix filter that only keeps first 3 letters
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_PREFIX_LEGACY);
            });
            recordStore.saveRecord(shakespeareDocument);
            recordStore.saveRecord(aethelredDocument1);
            List<Map.Entry<Tuple, List<Integer>>> scannedEntries = scanMapEntries(recordStore, SIMPLE_TEXT_PREFIX_LEGACY, Tuple.from("the"));
            assertEquals(Arrays.asList(entryOf(Tuple.from(966L), Arrays.asList(2, 5)), entryOf(Tuple.from(1623L), Arrays.asList(30, 34, 44, 53, 56, 59, 63, 68, 71, 76, 85, 92))), scannedEntries);
            assertEquals(Arrays.asList(Pair.of(Tuple.from(966L), 0), Pair.of(Tuple.from(1623L), 0)),
                    scanTokenizerVersions(recordStore, SIMPLE_TEXT_PREFIX_LEGACY));

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            // Use a version of the prefix filter that keeps the first 4 letters instead
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_PREFIX);
            });
            // check saving new document
            recordStore.saveRecord(aethelredDocument2);
            List<Map.Entry<Tuple, List<Integer>>> scannedEntries = toMapEntries(scanIndex(recordStore, SIMPLE_TEXT_PREFIX, TupleRange.prefixedBy("enc")), TupleHelpers.EMPTY);
            assertEquals(Arrays.asList(entryOf(Tuple.from("enc", 966L), Collections.singletonList(3)), entryOf(Tuple.from("ency", 1016L), Collections.singletonList(3))), scannedEntries);
            assertEquals(Arrays.asList(Pair.of(Tuple.from(966L), 0), Pair.of(Tuple.from(1016L), 1), Pair.of(Tuple.from(1623L), 0)),
                    scanTokenizerVersions(recordStore, SIMPLE_TEXT_PREFIX));

            // check document is re-indexed
            recordStore.saveRecord(aethelredDocument1);
            scannedEntries = scanMapEntries(recordStore, SIMPLE_TEXT_PREFIX, Tuple.from("ency"));
            assertEquals(Arrays.asList(entryOf(Tuple.from(966L), Collections.singletonList(3)), entryOf(Tuple.from(1016L), Collections.singletonList(3))), scannedEntries);
            scannedEntries = scanMapEntries(recordStore, SIMPLE_TEXT_PREFIX, Tuple.from("enc"));
            assertEquals(Collections.emptyList(), scannedEntries);
            assertEquals(Arrays.asList(Pair.of(Tuple.from(966L), 1), Pair.of(Tuple.from(1016L), 1), Pair.of(Tuple.from(1623L), 0)),
                    scanTokenizerVersions(recordStore, SIMPLE_TEXT_PREFIX));

            // check document that matches tokenizer version is *not* re-indexed
            int beforeSaveKeys = getSaveIndexKeyCount(recordStore);
            recordStore.saveRecord(aethelredDocument1);
            int afterSaveKeys = getSaveIndexKeyCount(recordStore);
            assertEquals(afterSaveKeys, beforeSaveKeys);
            assertEquals(Arrays.asList(Pair.of(Tuple.from(966L), 1), Pair.of(Tuple.from(1016L), 1), Pair.of(Tuple.from(1623L), 0)),
                    scanTokenizerVersions(recordStore, SIMPLE_TEXT_PREFIX));

            // check old index entries are the ones deleted
            scannedEntries = scanMapEntries(recordStore, SIMPLE_TEXT_PREFIX, Tuple.from("civ"));
            assertEquals(Collections.singletonList(entryOf(Tuple.from(1623L), Arrays.asList(22, 25))), scannedEntries);
            recordStore.deleteRecord(Tuple.from(1623L));
            scannedEntries = scanMapEntries(recordStore, SIMPLE_TEXT_PREFIX, Tuple.from("civ"));
            assertEquals(Collections.emptyList(), scannedEntries);
            assertEquals(Arrays.asList(Pair.of(Tuple.from(966L), 1), Pair.of(Tuple.from(1016L), 1)),
                    scanTokenizerVersions(recordStore, SIMPLE_TEXT_PREFIX));

            commit(context);
        }
    }

    @Test
    public void tokenizerVersionChangeWithMultipleEntries() throws Exception {
        final MapDocument map1 = MapDocument.newBuilder()
                .setDocId(1066L)
                .addEntry(MapDocument.Entry.newBuilder().setKey("fr").setValue(TextSamples.FRENCH))
                .addEntry(MapDocument.Entry.newBuilder().setKey("de").setValue(TextSamples.GERMAN))
                .build();

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(MAP_DOC, MAP_ON_VALUE_PREFIX_LEGACY);
            });
            recordStore.saveRecord(map1);
            List<Map.Entry<Tuple, List<Integer>>> scannedEntries = scanMapEntries(recordStore, MAP_ON_VALUE_PREFIX_LEGACY, Tuple.from("fr", "rec"));
            assertEquals(Collections.singletonList(entryOf(Tuple.from(1066L), Collections.singletonList(5))), scannedEntries);
            scannedEntries = scanMapEntries(recordStore, MAP_ON_VALUE_PREFIX_LEGACY, Tuple.from("de", "wah"));
            assertEquals(Collections.singletonList(entryOf(Tuple.from(1066L), Collections.singletonList(8))), scannedEntries);
            assertEquals(Collections.singletonList(Pair.of(Tuple.from(1066L), 0)),
                    scanTokenizerVersions(recordStore, MAP_ON_VALUE_PREFIX_LEGACY));

            commit(context);
        }
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(MAP_DOC, MAP_ON_VALUE_PREFIX);
            });

            // save an document where most of the source entries remain the same but are re-indexed because of the tokenizer version change
            final MapDocument map1prime = map1.toBuilder()
                    .addEntry(MapDocument.Entry.newBuilder().setKey("yi").setValue(TextSamples.YIDDISH))
                    .build();
            recordStore.saveRecord(map1prime);
            List<Map.Entry<Tuple, List<Integer>>> scannedEntries = scanMapEntries(recordStore, MAP_ON_VALUE_PREFIX, Tuple.from("yi", "שפרא"));
            assertEquals(Collections.singletonList(entryOf(Tuple.from(1066L), Collections.singletonList(1))), scannedEntries);
            scannedEntries = toMapEntries(scanIndex(recordStore, MAP_ON_VALUE_PREFIX, TupleRange.prefixedBy("rec").prepend(Tuple.from("fr"))), Tuple.from("fr"));
            assertEquals(Collections.singletonList(entryOf(Tuple.from("recu", 1066L), Collections.singletonList(5))), scannedEntries);
            scannedEntries = toMapEntries(scanIndex(recordStore, MAP_ON_VALUE_PREFIX, TupleRange.prefixedBy("wah").prepend(Tuple.from("de"))), Tuple.from("de"));
            assertEquals(Collections.singletonList(entryOf(Tuple.from("wahr", 1066L), Collections.singletonList(8))), scannedEntries);
            assertEquals(Collections.singletonList(Pair.of(Tuple.from(1066L), 1)),
                    scanTokenizerVersions(recordStore, MAP_ON_VALUE_PREFIX_LEGACY));

            commit(context);
        }
    }

    private void scanWithZeroScanRecordLimit(@Nonnull Index index, @Nonnull String token, boolean reverse) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            ScanProperties scanProperties = ExecuteProperties.newBuilder().setScannedRecordsLimit(0).build().asScanProperties(reverse);
            RecordCursor<IndexEntry> cursor = recordStore.scanIndex(index, BY_TEXT_TOKEN, TupleRange.allOf(Tuple.from(token)), null, scanProperties);
            RecordCursorResult<IndexEntry> result = cursor.getNext();
            if (!result.hasNext()) {
                assertEquals(SOURCE_EXHAUSTED, result.getNoNextReason());
                return;
            }
            result = cursor.getNext();
            assertThat(result.hasNext(), is(false));
            assertEquals(SCAN_LIMIT_REACHED, result.getNoNextReason());
        }
    }

    private void scanMultipleWithScanRecordLimits(@Nonnull Index index, @Nonnull List<String> tokens, int scanRecordLimit, boolean reverse) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            ScanProperties scanProperties = ExecuteProperties.newBuilder().setScannedRecordsLimit(scanRecordLimit).build().asScanProperties(reverse);

            List<RecordCursor<IndexEntry>> cursors = tokens.stream()
                    .map(token -> recordStore.scanIndex(index, BY_TEXT_TOKEN, TupleRange.allOf(Tuple.from(token)), null, scanProperties))
                    .collect(Collectors.toList());

            int cursorIndex = 0;
            int retrieved = 0;
            while (!cursors.isEmpty()) {
                RecordCursor<IndexEntry> cursor = cursors.get(cursorIndex);
                RecordCursorResult<IndexEntry> result = cursor.getNext();
                if (result.hasNext()) {
                    retrieved++;
                    cursorIndex = (cursorIndex + 1) % cursors.size();
                } else {
                    if (!result.getNoNextReason().isSourceExhausted()) {
                        assertEquals(SCAN_LIMIT_REACHED, result.getNoNextReason());
                    }
                    cursors.remove(cursorIndex);
                    if (cursorIndex == cursors.size()) {
                        cursorIndex = 0;
                    }
                }
            }
            // With the order that they are retrieved, the maximum value is the scanRecordLimit
            // or the number of tokens.
            assertThat(retrieved, lessThanOrEqualTo(Math.max(scanRecordLimit, tokens.size())));
        }
    }

    private void scanWithContinuations(@Nonnull Index index, @Nonnull String token, int limit, boolean reverse) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final List<IndexEntry> firstResults = scanIndex(recordStore, index, TupleRange.allOf(Tuple.from(token)));
            validateSorted(firstResults);
            List<IndexEntry> scanResults = new ArrayList<>(firstResults.size());
            byte[] continuation = null;

            do {
                RecordCursorIterator<IndexEntry> cursor = recordStore.scanIndex(index, BY_TEXT_TOKEN, TupleRange.allOf(Tuple.from(token)), continuation, reverse ? ScanProperties.REVERSE_SCAN : ScanProperties.FORWARD_SCAN).asIterator();
                for (int i = 0; i < limit || limit == 0; i++) {
                    if (cursor.hasNext()) {
                        scanResults.add(cursor.next());
                    } else {
                        break;
                    }
                }
                continuation = cursor.getContinuation();
                CompletableFuture<Boolean> hasNextFuture = cursor.onHasNext(); // fire off but don't wait for an on-has-next
                assertEquals(hasNextFuture.get(), cursor.hasNext()); // wait for the same on-has-next
                if (!cursor.hasNext()) {
                    assertThat(cursor.getNoNextReason().isSourceExhausted(), is(true));
                }
            } while (continuation != null);

            if (reverse) {
                Collections.reverse(scanResults);
            }
            assertEquals(firstResults, scanResults);
        }
    }

    public void scanWithSkip(@Nonnull Index index, @Nonnull String token, int skip, int limit, boolean reverse) throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final List<IndexEntry> fullResults = scanIndex(recordStore, index, TupleRange.allOf(Tuple.from(token)));
            validateSorted(fullResults);
            final ScanProperties scanProperties = ExecuteProperties.newBuilder().setReturnedRowLimit(limit).setSkip(skip).build().asScanProperties(reverse);
            final RecordCursor<IndexEntry> cursor = recordStore.scanIndex(index, BY_TEXT_TOKEN, TupleRange.allOf(Tuple.from(token)), null, scanProperties);
            List<IndexEntry> scanResults = cursor.asList().get();
            RecordCursorResult<IndexEntry> noNextResult = cursor.getNext();
            assertThat(noNextResult.hasNext(), is(false));
            assertEquals((limit != ReadTransaction.ROW_LIMIT_UNLIMITED && scanResults.size() == limit) ? RETURN_LIMIT_REACHED : SOURCE_EXHAUSTED, noNextResult.getNoNextReason());
            List<IndexEntry> expectedResults;
            if (reverse) {
                scanResults = new ArrayList<>(scanResults);
                Collections.reverse(scanResults);
                expectedResults = fullResults.subList(
                        (limit == ReadTransaction.ROW_LIMIT_UNLIMITED || limit == Integer.MAX_VALUE) ? 0 : Math.max(0, fullResults.size() - skip - limit),
                        Math.max(0, fullResults.size() - skip));
            } else {
                expectedResults = fullResults.subList(Math.min(fullResults.size(), skip),
                        (limit == ReadTransaction.ROW_LIMIT_UNLIMITED || limit == Integer.MAX_VALUE) ? fullResults.size() : Math.min(fullResults.size(), skip + limit));
            }
            assertEquals(expectedResults, scanResults);
        }
    }

    @Test
    public void scan() throws Exception {
        final Random r = new Random(0x5ca1ab1e);
        List<SimpleDocument> records = getRandomRecords(r, 50);
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            records.forEach(recordStore::saveRecord);
            commit(context);
        }
        final Index index = recordStore.getRecordMetaData().getIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);

        scanWithZeroScanRecordLimit(index, "angstrom", false); // existing token
        scanWithZeroScanRecordLimit(index, "angstrom", true); // existing token
        scanWithZeroScanRecordLimit(index, "asdfasdf", false); // non-existent token
        scanWithZeroScanRecordLimit(index, "asdfasdf", true); // non-existent token

        final List<Integer> limits = Arrays.asList(0, 1, 2, Integer.MAX_VALUE);
        final List<Integer> skips = Arrays.asList(0, 1, 10, 1000);
        final List<String> tokens = Arrays.asList("angstrom", "the", "not_a_token_in_the_lexicon", "שפראך");
        for (int limit : limits) {
            scanMultipleWithScanRecordLimits(index, tokens, limit, false);
            scanMultipleWithScanRecordLimits(index, tokens, limit, true);
            scanWithContinuations(index, "достопримечательности", limit, false);
            scanWithContinuations(index, "достопримечательности", limit, false);
            for (int skip : skips) {
                scanWithSkip(index, "toil", skip, limit, false);
                scanWithSkip(index, "toil", skip, limit, true);
            }
        }
    }

    @Test
    public void invalidScans() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            final Index index = recordStore.getRecordMetaData().getIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
            assertThrows(RecordCoreException.class, () -> recordStore.scanIndex(index, BY_VALUE, TupleRange.ALL, null, ScanProperties.REVERSE_SCAN));
            assertThrows(RecordCoreException.class, () -> recordStore.scanIndex(index, BY_GROUP, TupleRange.ALL, null, ScanProperties.REVERSE_SCAN));
            assertThrows(RecordCoreException.class, () -> recordStore.scanIndex(index, BY_RANK, TupleRange.ALL, null, ScanProperties.REVERSE_SCAN));
            assertThrows(RecordCoreException.class, () -> recordStore.scanIndex(index, BY_TIME_WINDOW, TupleRange.ALL, null, ScanProperties.REVERSE_SCAN));
        }
    }

    @Nonnull
    private RecordCursor<Tuple> queryDocuments(@Nullable List<String> recordTypes, @Nullable List<KeyExpression> requiredResults, @Nonnull QueryComponent filter, int planHash,
                                               @Nonnull Matcher<RecordQueryPlan> planMatcher) {
        RecordQuery.Builder queryBuilder = RecordQuery.newBuilder();
        if (recordTypes != null) {
            if (recordTypes.size() == 1) {
                queryBuilder.setRecordType(recordTypes.get(0));
            } else {
                queryBuilder.setRecordTypes(recordTypes);
            }
        }
        queryBuilder.setFilter(filter);
        queryBuilder.setRemoveDuplicates(true);
        if (requiredResults != null) {
            queryBuilder.setRequiredResults(requiredResults);
        }
        final RecordQuery query = queryBuilder.build();
        final RecordQueryPlan plan = planner.plan(query);
        LOGGER.info(KeyValueLogMessage.of("planned query",
                        TestLogMessageKeys.QUERY, query,
                        LogMessageKeys.PLAN, plan,
                        TestLogMessageKeys.PLAN_HASH, plan.planHash()));
        assertThat(plan, planMatcher);
        if (planHash == 0) {
            LOGGER.warn(KeyValueLogMessage.of("unset plan hash",
                            TestLogMessageKeys.PLAN_HASH, plan.planHash(),
                            LogMessageKeys.FILTER, filter));
        } else {
            assertEquals(planHash, plan.planHash(), "Mismatched hash for: " + filter);
        }
        return recordStore.executeQuery(plan).map(FDBQueriedRecord::getPrimaryKey);
    }

    @Nonnull
    private List<Long> querySimpleDocumentsWithScan(@Nonnull QueryComponent filter, int planHash) throws InterruptedException, ExecutionException {
        return queryDocuments(Collections.singletonList(SIMPLE_DOC), Collections.singletonList(field("doc_id")), filter, planHash,
                    filter(BooleanNormalizer.getDefaultInstance().normalize(filter), typeFilter(contains(SIMPLE_DOC), PlanMatchers.scan(unbounded()))))
                .map(t -> t.getLong(0))
                .asList()
                .get();
    }

    @Nonnull
    private List<Long> querySimpleDocumentsWithIndex(@Nonnull QueryComponent filter, @Nonnull String indexName, int planHash, boolean isCoveringIndexExpected,
                                                     @Nonnull Matcher<? super Comparisons.TextComparison> comparisonMatcher) throws InterruptedException, ExecutionException {
        Matcher<RecordQueryPlan> textMatcher = textIndexScan(allOf(indexName(indexName), textComparison(comparisonMatcher)));
        Matcher<RecordQueryPlan> indexMatcher = anyOf(textMatcher, coveringIndexScan(textMatcher));
        if (isCoveringIndexExpected) {
            indexMatcher = allOf(indexMatcher, not(descendant(fetch(any(RecordQueryPlan.class)))));
        }
        return queryDocuments(Collections.singletonList(SIMPLE_DOC), Collections.singletonList(field("doc_id")), filter, planHash,
                    descendant(indexMatcher))
                .map(t -> t.getLong(0))
                .asList()
                .get();
    }

    @Nonnull
    private List<Long> querySimpleDocumentsWithIndex(@Nonnull QueryComponent filter, int planHash, boolean isCoveringIndexExpected,
                                                     @Nonnull Matcher<? super Comparisons.TextComparison> comparisonMatcher) throws InterruptedException, ExecutionException {
        return querySimpleDocumentsWithIndex(filter, TextIndexTestUtils.SIMPLE_DEFAULT_NAME, planHash, isCoveringIndexExpected, comparisonMatcher);
    }

    @Nullable
    private List<Long> querySimpleDocumentsWithIndex(@Nonnull QueryComponent filter, @Nonnull String indexName, @Nonnull QueryComponent textFilter, int planHash, boolean isCoveringIndexExpected) throws InterruptedException, ExecutionException {
        if (textFilter instanceof ComponentWithComparison && ((ComponentWithComparison)textFilter).getComparison() instanceof Comparisons.TextComparison) {
            return querySimpleDocumentsWithIndex(filter, indexName, planHash, isCoveringIndexExpected, equalTo(((ComponentWithComparison)textFilter).getComparison()));
        } else if (textFilter instanceof AndOrComponent) {
            for (QueryComponent childFilter : ((AndOrComponent)textFilter).getChildren()) {
                List<Long> childResults = querySimpleDocumentsWithIndex(filter, indexName, childFilter, planHash, isCoveringIndexExpected);
                if (childResults != null) {
                    return childResults;
                }
            }
        }
        return null;
    }

    @Nonnull
    private List<Long> querySimpleDocumentsWithIndex(@Nonnull QueryComponent filter, @Nonnull String indexName, int planHash, boolean isCoveringIndexExpected) throws InterruptedException, ExecutionException {
        List<Long> queryResults = querySimpleDocumentsWithIndex(filter, indexName, filter, planHash, isCoveringIndexExpected);
        if (queryResults != null) {
            return queryResults;
        } else {
            throw new RecordCoreArgumentException("no text filter found");
        }
    }

    @Nonnull
    private List<Long> querySimpleDocumentsWithIndex(@Nonnull QueryComponent filter, int planHash, boolean isCoveringIndexExpected) throws InterruptedException, ExecutionException {
        return querySimpleDocumentsWithIndex(filter, TextIndexTestUtils.SIMPLE_DEFAULT_NAME, planHash, isCoveringIndexExpected);
    }

    @Test
    public void querySimpleDocuments() throws Exception {
        final List<SimpleDocument> documents = TextIndexTestUtils.toSimpleDocuments(Arrays.asList(
                TextSamples.ANGSTROM,
                TextSamples.AETHELRED,
                TextSamples.ROMEO_AND_JULIET_PROLOGUE,
                TextSamples.YIDDISH,
                TextSamples.CHINESE_SIMPLIFIED,
                TextSamples.KOREAN,
                "a b a b a b c"
        ));

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            documents.forEach(recordStore::saveRecord);

            // Contains
            assertEquals(Arrays.asList(0L, 1L, 2L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().contains("the"), 329921958, true));
            assertEquals(Collections.singletonList(0L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().contains("angstrom"), -1859676822, true));
            assertEquals(Collections.emptyList(),
                    querySimpleDocumentsWithIndex(Query.field("text").text().contains("Ångström"), 2028628575, true));
            assertEquals(Collections.singletonList(3L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().contains("שפראך"), 1151275308, true));

            // Contains all
            assertEquals(Collections.singletonList(0L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsAll("Ångström"), 1999999424, true));
            assertEquals(Collections.emptyList(),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsAll(Collections.singletonList("Ångström")), 2028628575, true));
            assertEquals(Collections.singletonList(0L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsAll("the angstrom"), 865061914, true));
            assertEquals(Collections.singletonList(0L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsAll(Arrays.asList("the", "angstrom")), 4380219, true));
            assertEquals(Collections.singletonList(0L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsAll(Arrays.asList("", "angstrom")), -1000802292, true));
            assertEquals(Collections.singletonList(5L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsAll("한국어를"), -1046915537, true));

            // Contains all within a distance
            assertEquals(Collections.singletonList(0L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsAll("Ångström named", 4), -1408252035, true));
            assertEquals(Collections.singletonList(0L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsAll("Ångström named", 3), -1408252996, true));
            assertEquals(Collections.emptyList(),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsAll("Ångström named", 2), -1408253957, true));
            assertEquals(Collections.emptyList(),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsAll(Arrays.asList("Ångström", "named"), 4), -2041874864, true));
            assertEquals(Collections.singletonList(6L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsAll("a c", 2), 2135218554, true));
            assertEquals(Collections.emptyList(),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsAll("a c", 1), 2135217593, true));
            assertEquals(Collections.singletonList(6L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsAll("b c", 2), -416938407, true));
            assertEquals(Collections.singletonList(6L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsAll("b c", 1), -416939368, true));

            // Contains any
            assertEquals(Collections.singletonList(0L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsAny("Ångström"), -147781547, true));
            assertEquals(Collections.emptyList(),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsAny(Collections.singletonList("Ångström")), -119152396, true));
            assertEquals(Arrays.asList(0L, 1L, 2L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsAny("the angstrom"), -1282719057, true));
            assertEquals(Arrays.asList(0L, 1L, 2L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsAny(Arrays.asList("the", "angstrom")), -2143400752, true));

            // Contains phrase
            assertEquals(Collections.singletonList(2L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsPhrase("Civil blood makes. Civil hands unclean"), -993768059, true));
            assertEquals(Collections.singletonList(2L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsPhrase(Arrays.asList("civil", "blood", "makes", "civil", "", "unclean")), 1855137352, true));
            assertEquals(Collections.emptyList(),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsPhrase(Arrays.asList("Civil", "blood", "makes", "civil", "", "unclean")), 853144168, true));
            assertEquals(Collections.singletonList(2L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsPhrase(Arrays.asList("", "civil", "blood", "makes", "civil", "", "unclean", "")), 930039198, true));
            assertEquals(Collections.singletonList(6L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsPhrase("a b a b c"), -623744405, true));

            // Contains prefix
            assertEquals(Arrays.asList(2L, 0L, 1L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsPrefix("un"), 1067159426, true));
            assertEquals(Collections.singletonList(3L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsPrefix("א"), -1009839303, true));
            assertEquals(Collections.singletonList(4L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsPrefix("苹果"), -1529274452, true));
            assertEquals(Collections.singletonList(5L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsPrefix(Normalizer.normalize("한국", Normalizer.Form.NFKD)), -1860545817, true));
            assertEquals(Collections.singletonList(5L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsPrefix("한구"), 1377518291, true)); // note that the second character is only 2 of the 3 Jamo components

            // Contains any prefix
            assertEquals(ImmutableSet.of(0L, 1L, 2L, 3L),
                    new HashSet<>(querySimpleDocumentsWithIndex(Query.field("text").text().containsAnyPrefix("civ א un"), 1227233680, true)));
            assertEquals(ImmutableSet.of(0L, 1L, 2L, 3L),
                    new HashSet<>(querySimpleDocumentsWithIndex(Query.field("text").text().containsAnyPrefix("cIv ַא Un"), -794472473, true)));
            assertEquals(ImmutableSet.of(0L, 1L, 2L, 3L),
                    new HashSet<>(querySimpleDocumentsWithIndex(Query.field("text").text().containsAnyPrefix(Arrays.asList("civ", "א", "un")), 1486849487, true)));
            assertEquals(ImmutableSet.of(2L),
                    new HashSet<>(querySimpleDocumentsWithIndex(Query.field("text").text().containsAnyPrefix(Arrays.asList("civ", "אַ", "Un")), 1905505336, true)));

            // Contains all prefixes
            assertEquals(Collections.singletonList(2L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsAllPrefixes("civ un"), 1757831895, false));
            assertEquals(Collections.singletonList(2L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsAllPrefixes("civ un", false), -900079353, true));
            assertEquals(ImmutableSet.of(0L, 1L),
                    new HashSet<>(querySimpleDocumentsWithIndex(Query.field("text").text().containsAllPrefixes("wa th"), -1203466155, false)));
            assertEquals(ImmutableSet.of(0L, 1L),
                    new HashSet<>(querySimpleDocumentsWithIndex(Query.field("text").text().containsAllPrefixes("wa th", false), -433119192, true)));

            commit(context);
        }
    }

    @Test
    public void queryDocumentsWithScanLimit() throws Exception {
        // Load a big (ish) data set
        final int recordCount = 100;
        final int batchSize = 10;

        for (int i = 0; i < recordCount; i += batchSize) {
            try (FDBRecordContext context = openContext()) {
                openRecordStore(context);
                for (int j = 0; j < batchSize; j++) {
                    SimpleDocument document = SimpleDocument.newBuilder()
                            .setDocId(i + j)
                            .setText((i + j) % 2 == 0 ? "some" : "text")
                            .build();
                    recordStore.saveRecord(document);
                }
                commit(context);
            }
        }

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);

            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(SIMPLE_DOC)
                    .setFilter(Query.field("text").text().containsAll("some text"))
                    .build();
            RecordQueryPlan plan = planner.plan(query);

            boolean done = false;
            int totalKeysLoaded = 0;
            byte[] continuation = null;
            while (!done) {
                final int priorKeysLoaded = getLoadTextEntryCount(recordStore);
                ExecuteProperties executeProperties = ExecuteProperties.newBuilder().setScannedRecordsLimit(50).build();
                RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan, continuation, executeProperties);
                assertEquals(Collections.emptyList(), cursor.asList().get());
                RecordCursorResult<FDBQueriedRecord<Message>> noNextResult = cursor.getNext();
                assertThat(noNextResult.hasNext(), is(false));
                final int newKeysLoaded = getLoadTextEntryCount(recordStore);
                totalKeysLoaded += newKeysLoaded - priorKeysLoaded;
                if (!noNextResult.getNoNextReason().isSourceExhausted()) {
                    assertEquals(50, newKeysLoaded - priorKeysLoaded);
                    assertEquals(RecordCursor.NoNextReason.SCAN_LIMIT_REACHED, noNextResult.getNoNextReason());
                    assertNotNull(noNextResult.getContinuation().toBytes());
                } else {
                    assertNull(noNextResult.getContinuation().toBytes());
                    done = true;
                }
                continuation = noNextResult.getContinuation().toBytes();
            }
            assertEquals(recordCount + 2, totalKeysLoaded);

            commit(context);
        }
    }

    @Test
    public void querySimpleDocumentsWithAdditionalFilters() throws Exception {
        final List<SimpleDocument> documents = TextIndexTestUtils.toSimpleDocuments(Arrays.asList(
                TextSamples.ROMEO_AND_JULIET_PROLOGUE,
                TextSamples.ROMEO_AND_JULIET_PROLOGUE,
                TextSamples.AETHELRED,
                TextSamples.ANGSTROM
        ));

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            documents.forEach(recordStore::saveRecord);

            // Equality text predicates
            assertEquals(Collections.singletonList(3L),
                    querySimpleDocumentsWithIndex(Query.and(
                            Query.field("group").equalsValue(1L),
                            Query.field("text").text().contains("was")
                    ), 661433949, false));
            assertEquals(Collections.singletonList(0L),
                    querySimpleDocumentsWithIndex(Query.and(
                            Query.field("group").equalsValue(0L),
                            Query.field("text").text().containsPhrase("bury their parents' strife")
                    ), -1454788243, false));
            assertEquals(Collections.singletonList(1L),
                    querySimpleDocumentsWithIndex(Query.and(
                            Query.field("group").equalsValue(1L),
                            Query.field("text").text().containsPhrase("bury their parents' strife")
                    ), -1454788242, false));
            assertEquals(Arrays.asList(0L, 1L),
                    querySimpleDocumentsWithIndex(Query.and(
                            Query.field("group").lessThanOrEquals(2L),
                            Query.field("text").text().containsAny("bury their parents' strife")
                    ), -1259238340, false));
            // In theory, this could be an index intersection, but it is not.
            assertEquals(Collections.singletonList(2L),
                    querySimpleDocumentsWithIndex(Query.and(
                            Query.field("text").text().contains("the"),
                            Query.field("text").text().contains("king")
                    ), 742257848, false));

            // Prefix text predicates

            assertEquals(Arrays.asList(0L, 1L),
                    querySimpleDocumentsWithIndex(Query.and(
                            Query.field("group").lessThanOrEquals(2L),
                            Query.field("text").text().containsPrefix("par"),
                            Query.field("text").text().containsPrefix("blo")
                    ), -416906621, false));
            assertEquals(Arrays.asList(1L, 3L),
                    querySimpleDocumentsWithIndex(Query.and(
                            Query.field("group").equalsValue(1L),
                            Query.field("text").text().containsPrefix("an")
                    ), 1318510566, false));
            assertEquals(Arrays.asList(0L, 1L),
                    querySimpleDocumentsWithIndex(Query.and(
                            Query.field("text").text().containsAll("civil unclean blood"),
                            Query.field("text").text().containsPrefix("blo")
                    ), 912028198, false));

            // Performs a union of the two text queries.

            assertEquals(ImmutableSet.of(0L, 1L, 2L),
                    ImmutableSet.copyOf(querySimpleDocumentsWithIndex(
                            Query.or(
                                    Query.field("text").text().containsPrefix("ency"),
                                    Query.field("text").text().containsPrefix("civ")
                    ), -1250585991, false)));

            assertEquals(Arrays.asList(0L, 2L),
                    querySimpleDocumentsWithIndex(Query.and(
                            Query.field("group").equalsValue(0L),
                            Query.or(
                                    Query.field("text").text().containsAll("civil unclean blood", 4),
                                    Query.field("text").text().containsAll("king was 1016")
                            )
                    ), 1313228370, false));

            assertEquals(ImmutableSet.of(0L, 2L),
                    ImmutableSet.copyOf(querySimpleDocumentsWithIndex(Query.and(
                            Query.field("group").equalsValue(0L),
                            Query.or(
                                    Query.field("text").text().containsAll("civil unclean blood", 4),
                                    Query.field("text").text().containsPrefix("ency")
                            )
                    ), 873750052, false)));

            // Just a not. There's not a lot this could query could do to be performed because it can return
            // a lot of results by its very nature.
            assertEquals(Collections.singletonList(3L),
                    querySimpleDocumentsWithScan(Query.not(Query.field("text").text().containsAny("king unclean")), 784296935));

            // Scans the index for the first predicate and then applies the second as a not.
            // In theory, it could scan the index twice and filter out the "not".
            assertEquals(Arrays.asList(0L, 1L, 3L),
                    querySimpleDocumentsWithIndex(Query.and(
                            Query.field("text").text().contains("the"),
                            Query.not(Query.field("text").text().contains("king"))
                    ), 742257849, false));

            commit(context);
        }
    }

    @Test
    public void querySimpleDocumentsWithDifferentTokenizers() throws Exception {
        final List<SimpleDocument> documents = TextIndexTestUtils.toSimpleDocuments(Arrays.asList(
                TextSamples.ROMEO_AND_JULIET_PROLOGUE,
                TextSamples.RUSSIAN,
                TextSamples.GERMAN,
                TextSamples.KOREAN
        ));
        TextTokenizerRegistryImpl.instance().register(FILTERING_TOKENIZER);

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                final RecordTypeBuilder simpleDocRecordType = metaDataBuilder.getRecordType(SIMPLE_DOC);
                metaDataBuilder.addIndex(simpleDocRecordType, SIMPLE_TEXT_PREFIX);
                metaDataBuilder.addIndex(simpleDocRecordType, SIMPLE_TEXT_FILTERING);
                metaDataBuilder.addIndex(simpleDocRecordType, SIMPLE_TEXT_SUFFIXES);
            });
            documents.forEach(recordStore::saveRecord);

            // Filtering tokenizer
            final String filteringTokenizerName = FILTERING_TOKENIZER.getName();
            assertEquals(Collections.singletonList(2L),
                    querySimpleDocumentsWithIndex(Query.field("text").text(DefaultTextTokenizer.NAME).contains("weltmeisterschaft"), TextIndexTestUtils.SIMPLE_DEFAULT_NAME, -1172646540, true));
            assertEquals(Collections.emptyList(),
                    querySimpleDocumentsWithIndex(Query.field("text").text(filteringTokenizerName).contains("weltmeisterschaft"), SIMPLE_TEXT_FILTERING.getName(), 835135314, true));
            assertEquals(Collections.singletonList(1L),
                    querySimpleDocumentsWithIndex(Query.field("text").text(DefaultTextTokenizer.NAME).contains("достопримечательности"), TextIndexTestUtils.SIMPLE_DEFAULT_NAME , -1291535616, true));
            assertEquals(Collections.emptyList(),
                    querySimpleDocumentsWithIndex(Query.field("text").text(filteringTokenizerName).contains("достопримечательности"), SIMPLE_TEXT_FILTERING.getName(), 716246238, true));
            assertEquals(Collections.singletonList(2L),
                    querySimpleDocumentsWithIndex(Query.field("text").text(filteringTokenizerName).containsAll("Weltmeisterschaft gewonnen"), SIMPLE_TEXT_FILTERING.getName(), 696188882, true));
            assertEquals(Collections.emptyList(),
                    querySimpleDocumentsWithIndex(Query.field("text").text(filteringTokenizerName).containsAll(Arrays.asList("weltmeisterschaft", "gewonnen")), SIMPLE_TEXT_FILTERING.getName(), 1945779923, true));
            assertEquals(Collections.singletonList(2L),
                    querySimpleDocumentsWithIndex(Query.field("text").text(DefaultTextTokenizer.NAME).containsAll("Weltmeisterschaft Nationalmannschaft Friedrichstraße"), TextIndexTestUtils.SIMPLE_DEFAULT_NAME, 625333664, true));
            assertEquals(Collections.emptyList(),
                    querySimpleDocumentsWithIndex(Query.field("text").text(filteringTokenizerName).containsAll("Weltmeisterschaft Nationalmannschaft Friedrichstraße"), SIMPLE_TEXT_FILTERING.getName(), -1661851778, true));

            // Prefix tokenizer
            final String prefixTokenizerName = PrefixTextTokenizer.NAME;
            assertEquals(Collections.emptyList(),
                    querySimpleDocumentsWithIndex(Query.field("text").text(DefaultTextTokenizer.NAME).containsAny("civic lover"), TextIndexTestUtils.SIMPLE_DEFAULT_NAME, 1358697044, true));
            assertEquals(Collections.singletonList(0L),
                    querySimpleDocumentsWithIndex(Query.field("text").text(prefixTokenizerName).containsAll("civic lover"), SIMPLE_TEXT_PREFIX.getName(), 2070491434, true));
            assertEquals(Collections.emptyList(),
                    querySimpleDocumentsWithIndex(Query.field("text").text(DefaultTextTokenizer.NAME).containsAll("못핵"), TextIndexTestUtils.SIMPLE_DEFAULT_NAME, -1414597326, true));
            assertEquals(Collections.singletonList(3L),
                    querySimpleDocumentsWithIndex(Query.field("text").text(prefixTokenizerName).containsAll("못핵"), SIMPLE_TEXT_PREFIX.getName(), 1444383389, true));

            // Suffixes tokenizer
            // Note that prefix scans using the suffixes tokenizer are equivalent to infix searches on the original tokens
            assertEquals(Collections.emptyList(),
                    querySimpleDocumentsWithIndex(Query.field("text").text(DefaultTextTokenizer.NAME).containsPrefix("meister"), TextIndexTestUtils.SIMPLE_DEFAULT_NAME, -2049073113, true));
            assertEquals(Collections.singletonList(2L),
                    querySimpleDocumentsWithIndex(Query.field("text").text(AllSuffixesTextTokenizer.NAME).containsPrefix("meister"), SIMPLE_TEXT_SUFFIXES.getName(), -628393471, true));
            assertEquals(Collections.emptyList(),
                    querySimpleDocumentsWithIndex(Query.field("text").text(DefaultTextTokenizer.NAME).containsAnyPrefix("meister ivi"), TextIndexTestUtils.SIMPLE_DEFAULT_NAME, 279029713, true));
            assertEquals(ImmutableSet.of(0L, 2L),
                    new HashSet<>(querySimpleDocumentsWithIndex(Query.field("text").text(AllSuffixesTextTokenizer.NAME).containsAnyPrefix("meister ivi"), SIMPLE_TEXT_SUFFIXES.getName(), 1699709355, true)));
            assertEquals(Collections.emptyList(),
                    querySimpleDocumentsWithIndex(Query.field("text").text(DefaultTextTokenizer.NAME).containsAllPrefixes("meister won", false), TextIndexTestUtils.SIMPLE_DEFAULT_NAME, 993745490, true));
            assertEquals(Collections.singletonList(2L),
                    querySimpleDocumentsWithIndex(Query.field("text").text(AllSuffixesTextTokenizer.NAME).containsAllPrefixes("meister won", false), SIMPLE_TEXT_SUFFIXES.getName(), -1880542164, true));
            assertEquals(Arrays.asList(0L, 2L),
                    querySimpleDocumentsWithIndex(Query.field("text").text(AllSuffixesTextTokenizer.NAME).containsAny("y e"), SIMPLE_TEXT_SUFFIXES.getName(), -1665999070, true));
            assertEquals(Collections.emptyList(),
                    querySimpleDocumentsWithIndex(Query.field("text").text(AllSuffixesTextTokenizer.NAME).containsAny("bloody civilize"), SIMPLE_TEXT_SUFFIXES.getName(), 1290016358, true));
            assertEquals(Collections.singletonList(0L),
                    querySimpleDocumentsWithIndex(Query.field("text").text(AllSuffixesTextTokenizer.NAME).containsAll("ood ivil nds"), SIMPLE_TEXT_SUFFIXES.getName(), -1619880168, true));

            commit(context);
        }
    }

    @Test
    public void querySimpleDocumentsMaybeCovering() throws Exception {
        final List<SimpleDocument> documents = TextIndexTestUtils.toSimpleDocuments(Arrays.asList(
                TextSamples.ANGSTROM,
                TextSamples.AETHELRED,
                TextSamples.ROMEO_AND_JULIET_PROLOGUE,
                TextSamples.FRENCH
        ));

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            documents.forEach(recordStore::saveRecord);

            final QueryComponent filter1 = Query.field("text").text().containsPhrase("civil blood makes civil hands unclean");
            final Comparisons.Comparison comparison1 = new Comparisons.TextComparison(Comparisons.Type.TEXT_CONTAINS_PHRASE, "civil blood makes civil hands unclean", null, DefaultTextTokenizer.NAME);
            final QueryComponent filter2 = Query.field("text").text().containsPrefix("th");
            final Comparisons.Comparison comparison2 = new Comparisons.TextComparison(Comparisons.Type.TEXT_CONTAINS_PREFIX,  Collections.singletonList("th"), null, DefaultTextTokenizer.NAME);

            // Query for full records
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(SIMPLE_DOC)
                    .setFilter(filter1)
                    .build();
            RecordQueryPlan plan = planner.plan(query);
            assertThat(plan, textIndexScan(allOf(indexName(TextIndexTestUtils.SIMPLE_DEFAULT_NAME), textComparison(equalTo(comparison1)))));
            assertEquals(814602491, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1101247748, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1215587201, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            List<Long> primaryKeys = recordStore.executeQuery(plan).map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
            assertEquals(Collections.singletonList(2L), primaryKeys);

            query = RecordQuery.newBuilder()
                    .setRecordType(SIMPLE_DOC)
                    .setFilter(filter2)
                    .build();
            plan = planner.plan(query);
            assertThat(plan, primaryKeyDistinct(textIndexScan(allOf(indexName(TextIndexTestUtils.SIMPLE_DEFAULT_NAME), textComparison(equalTo(comparison2))))));
            assertEquals(1032989149, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1513880131, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1570861632, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            primaryKeys = recordStore.executeQuery(plan).map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
            assertEquals(Arrays.asList(0L, 1L, 2L, 3L), primaryKeys);

            // Query for just primary key
            query = RecordQuery.newBuilder()
                    .setRecordType(SIMPLE_DOC)
                    .setRequiredResults(Collections.singletonList(field("doc_id")))
                    .setFilter(filter1)
                    .build();
            plan = planner.plan(query);
            assertThat(plan, coveringIndexScan(textIndexScan(allOf(indexName(TextIndexTestUtils.SIMPLE_DEFAULT_NAME), textComparison(equalTo(comparison1))))));
            assertEquals(814602491, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-786467136, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(1191665211, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            primaryKeys = recordStore.executeQuery(plan).map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
            assertEquals(Collections.singletonList(2L), primaryKeys);

            query = RecordQuery.newBuilder()
                    .setRecordType(SIMPLE_DOC)
                    .setRequiredResults(Collections.singletonList(field("doc_id")))
                    .setFilter(filter2)
                    .build();
            plan = planner.plan(query);
            assertThat(plan, primaryKeyDistinct(coveringIndexScan(textIndexScan(allOf(indexName(TextIndexTestUtils.SIMPLE_DEFAULT_NAME), textComparison(equalTo(comparison2)))))));
            assertEquals(1032989149, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(893372281, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(836390780, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            primaryKeys = recordStore.executeQuery(plan).map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
            assertEquals(Arrays.asList(0L, 1L, 2L, 3L), primaryKeys);

            // Query for primary key but also have a filter on something outside the index
            query = RecordQuery.newBuilder()
                    .setRecordType(SIMPLE_DOC)
                    .setRequiredResults(Collections.singletonList(field("doc_id")))
                    .setFilter(Query.and(filter1, Query.field("group").equalsValue(0L)))
                    .build();
            plan = planner.plan(query);
            assertThat(plan, filter(Query.field("group").equalsValue(0L),
                    textIndexScan(allOf(indexName(TextIndexTestUtils.SIMPLE_DEFAULT_NAME), textComparison(equalTo(comparison1))))));
            assertEquals(-1328921799, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(390154904, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-611539723, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            primaryKeys = recordStore.executeQuery(plan).map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
            assertEquals(Collections.singletonList(2L), primaryKeys);

            query = RecordQuery.newBuilder()
                    .setRecordType(SIMPLE_DOC)
                    .setRequiredResults(Collections.singletonList(field("doc_id")))
                    .setFilter(Query.and(filter2, Query.field("group").equalsValue(0L)))
                    .build();
            plan = planner.plan(query);
            System.out.println(plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            System.out.println(plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            System.out.println(plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            assertThat(plan, filter(Query.field("group").equalsValue(0L),
                    fetch(primaryKeyDistinct(coveringIndexScan(textIndexScan(allOf(indexName(TextIndexTestUtils.SIMPLE_DEFAULT_NAME), textComparison(equalTo(comparison2)))))))));
            assertEquals(792432470, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-879354804, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-545069279, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            primaryKeys = recordStore.executeQuery(plan).map(FDBQueriedRecord::getPrimaryKey).map(t -> t.getLong(0)).asList().get();
            assertEquals(Arrays.asList(0L, 2L), primaryKeys);

            // Query for the text field, which produces the first token that matches
            // Arguably, this should produce an error, but that requires a more sophisticated
            // check when trying to determine if the index covers the query
            final Descriptors.FieldDescriptor docIdDescriptor = SimpleDocument.getDescriptor().findFieldByNumber(SimpleDocument.DOC_ID_FIELD_NUMBER);
            final Descriptors.FieldDescriptor textDescriptor = SimpleDocument.getDescriptor().findFieldByNumber(SimpleDocument.TEXT_FIELD_NUMBER);

            query = RecordQuery.newBuilder()
                    .setRecordType(SIMPLE_DOC)
                    .setRequiredResults(Collections.singletonList(field("text")))
                    .setFilter(filter1)
                    .build();
            plan = planner.plan(query);
            assertThat(plan, textIndexScan(allOf(indexName(TextIndexTestUtils.SIMPLE_DEFAULT_NAME), textComparison(equalTo(comparison1)))));
            assertEquals(814602491, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(1101247748, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1215587201, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            List<Tuple> idTextTuples = recordStore.executeQuery(plan)
                    .map(record -> {
                        final Object docId = record.getRecord().getField(docIdDescriptor);
                        final Object text = record.getRecord().getField(textDescriptor);
                        return Tuple.from(docId, text);
                    })
                    .asList()
                    .get();
            assertEquals(Collections.singletonList(Tuple.from(2L, TextSamples.ROMEO_AND_JULIET_PROLOGUE)), idTextTuples);

            query = RecordQuery.newBuilder()
                    .setRecordType(SIMPLE_DOC)
                    .setRequiredResults(Collections.singletonList(field("text")))
                    .setFilter(filter2)
                    .build();
            plan = planner.plan(query);
            assertThat(plan, fetch(primaryKeyDistinct(coveringIndexScan(textIndexScan(allOf(indexName(TextIndexTestUtils.SIMPLE_DEFAULT_NAME), textComparison(equalTo(comparison2))))))));
            assertEquals(-1359010536, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1017914160, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(-1074895661, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            idTextTuples = recordStore.executeQuery(plan)
                    .map(record -> {
                        final Object docId = record.getRecord().getField(docIdDescriptor);
                        final Object text = record.getRecord().getField(textDescriptor);
                        return Tuple.from(docId, text);
                    })
                    .asList()
                    .get();
            assertEquals(Arrays.asList(
                    Tuple.from(0L, TextSamples.ANGSTROM),
                    Tuple.from(1L, TextSamples.AETHELRED),
                    Tuple.from(2L, TextSamples.ROMEO_AND_JULIET_PROLOGUE),
                    Tuple.from(3L, TextSamples.FRENCH)), idTextTuples);


            commit(context);
        }
    }

    @Test
    public void querySimpleDocumentsWithoutPositions() throws Exception {
        final List<SimpleDocument> documents = TextIndexTestUtils.toSimpleDocuments(Arrays.asList(
                TextSamples.ANGSTROM,
                TextSamples.AETHELRED,
                TextSamples.ROMEO_AND_JULIET_PROLOGUE,
                TextSamples.FRENCH
        ));

        // Query but make sure
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, SIMPLE_TEXT_NO_POSITIONS);
            });
            documents.forEach(recordStore::saveRecord);

            // Queries that *don't* require position information should be planned to use the index
            assertEquals(Arrays.asList(1L, 2L, 3L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsAny("king civil récu"), SIMPLE_TEXT_NO_POSITIONS.getName(), 0, true));
            assertEquals(Collections.singletonList(2L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsAll("unclean verona"), SIMPLE_TEXT_NO_POSITIONS.getName(), 0, true));
            assertEquals(Arrays.asList(0L, 1L, 2L, 3L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsPrefix("th"), SIMPLE_TEXT_NO_POSITIONS.getName(), 0, true));

            // Queries that *do* require position information must be planned as scans
            assertEquals(Collections.singletonList(2L),
                    querySimpleDocumentsWithScan(Query.field("text").text().containsPhrase("civil blood makes civil hands unclean"), 0));
            assertEquals(Collections.singletonList(3L),
                    querySimpleDocumentsWithScan(Query.field("text").text().containsAll("France Napoleons", 3), 0));

            commit(context);
        }

        final List<SimpleDocument> newDocuments = documents.stream()
                .map(doc -> doc.toBuilder().setDocId(doc.getDocId() + documents.size()).build())
                .collect(Collectors.toList());

        // Upgrade to writing position information
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(SIMPLE_DOC, new Index(SIMPLE_TEXT_NO_POSITIONS.getName(), SIMPLE_TEXT_NO_POSITIONS.getRootExpression(), IndexTypes.TEXT));
            });
            newDocuments.forEach(recordStore::saveRecord);

            // Queries that *don't* require position information produce the same plan
            assertEquals(Arrays.asList(1L, 2L, 3L, 5L, 6L, 7L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsAny("king civil récu"), SIMPLE_TEXT_NO_POSITIONS.getName(), 0, true));
            assertEquals(Arrays.asList(2L, 6L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsAll("unclean verona"), SIMPLE_TEXT_NO_POSITIONS.getName(), 0, true));
            assertEquals(Arrays.asList(0L, 1L, 2L, 4L, 5L, 6L, 3L, 7L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsPrefix("th"), SIMPLE_TEXT_NO_POSITIONS.getName(), 0, true));

            // Queries that *do* require position information now use the index, but previously written documents show up in the
            // query spuriously
            assertEquals(Arrays.asList(2L, 6L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsPhrase("civil blood makes civil hands unclean"), SIMPLE_TEXT_NO_POSITIONS.getName(), 0, true));
            assertEquals(Collections.singletonList(2L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsPhrase("unclean verona"), SIMPLE_TEXT_NO_POSITIONS.getName(), 0, true));
            assertEquals(Arrays.asList(3L, 7L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsAll("France Napoleons", 3), SIMPLE_TEXT_NO_POSITIONS.getName(), 0, true));
            assertEquals(Collections.singletonList(3L),
                    querySimpleDocumentsWithIndex(Query.field("text").text().containsAll("Thiers Napoleons", 3), SIMPLE_TEXT_NO_POSITIONS.getName(), 0, true));

            commit(context);
        }
    }

    @Nonnull
    private List<Tuple> queryComplexDocumentsWithPlan(@Nonnull QueryComponent filter, int planHash, Matcher<RecordQueryPlan> planMatcher) throws InterruptedException, ExecutionException {
        return queryDocuments(Collections.singletonList(COMPLEX_DOC), Arrays.asList(field("group"), field("doc_id")), filter, planHash, planMatcher)
                .asList()
                .get();
    }

    @Nonnull
    private List<Tuple> queryComplexDocumentsWithScan(@Nonnull QueryComponent textFilter, long group, int planHash) throws InterruptedException, ExecutionException {
        final Matcher<RecordQueryPlan> planMatcher = filter(textFilter,
                typeFilter(equalTo(Collections.singleton(COMPLEX_DOC)), PlanMatchers.scan(bounds(hasTupleString("[[" + group + "],[" + group + "]]")))));
        return queryComplexDocumentsWithPlan(Query.and(Query.field("group").equalsValue(group), textFilter), planHash, planMatcher);
    }

    @Nonnull
    private List<Tuple> queryComplexDocumentsWithIndex(@Nonnull QueryComponent textFilter, @Nullable QueryComponent additionalFilter, boolean skipFilterCheck, long group, int planHash) throws InterruptedException, ExecutionException {
        if (!(textFilter instanceof ComponentWithComparison)) {
            throw new RecordCoreArgumentException("filter without comparison provided as text filter");
        }
        final Matcher<RecordQueryPlan> textScanMatcher = textIndexScan(allOf(
                indexName(COMPLEX_TEXT_BY_GROUP.getName()),
                groupingBounds(allOf(notNullValue(), hasTupleString("[[" + group + "],[" + group + "]]"))),
                textComparison(equalTo(((ComponentWithComparison)textFilter).getComparison()))
        ));
        // Don't care whether it's covering or not
        final Matcher<RecordQueryPlan> textPlanMatcher = anyOf(textScanMatcher, coveringIndexScan(textScanMatcher));
        final Matcher<RecordQueryPlan> planMatcher;
        final QueryComponent filter;
        if (additionalFilter != null) {
            if (skipFilterCheck) {
                planMatcher = descendant(textPlanMatcher);
            } else {
                planMatcher = descendant(filter(additionalFilter, descendant(textPlanMatcher)));
            }
            filter = Query.and(textFilter, additionalFilter, Query.field("group").equalsValue(group));
        } else {
            planMatcher = descendant(textPlanMatcher);
            filter = Query.and(textFilter, Query.field("group").equalsValue(group));
        }
        return queryComplexDocumentsWithPlan(filter, planHash, planMatcher);
    }

    @Nonnull
    private List<Tuple> queryComplexDocumentsWithIndex(@Nonnull QueryComponent textFilter, @Nullable QueryComponent additionalFilter, long group, int planHash) throws InterruptedException, ExecutionException {
        return queryComplexDocumentsWithIndex(textFilter, additionalFilter, false, group, planHash);
    }

    @Nonnull
    private List<Tuple> queryComplexDocumentsWithIndex(@Nonnull QueryComponent textFilter, long group, int planHash) throws InterruptedException, ExecutionException {
        return queryComplexDocumentsWithIndex(textFilter, null, group, planHash);
    }

    @Nonnull
    private List<Tuple> queryComplexDocumentsWithOr(@Nonnull OrComponent orFilter, long group, int planHash) throws InterruptedException, ExecutionException {
        final Matcher<RecordQueryPlan> textPlanMatcher = textIndexScan(allOf(
                indexName(COMPLEX_TEXT_BY_GROUP.getName()),
                groupingBounds(allOf(notNullValue(), hasTupleString("[[" + group + "],[" + group + "]]"))),
                textComparison(any(Comparisons.TextComparison.class))
        ));
        final QueryComponent filter = Query.and(orFilter, Query.field("group").equalsValue(group));
        final Matcher<RecordQueryPlan> planMatcher = descendant(textPlanMatcher);
        return queryComplexDocumentsWithPlan(filter, planHash, planMatcher);
    }

    @Test
    public void queryComplexDocuments() throws Exception {
        final List<String> textSamples = Arrays.asList(
                TextSamples.ANGSTROM,
                TextSamples.ROMEO_AND_JULIET_PROLOGUE,
                TextSamples.AETHELRED,
                TextSamples.FRENCH
        );
        final List<ComplexDocument> documents = IntStream.range(0, textSamples.size())
                .mapToObj(i -> ComplexDocument.newBuilder().setDocId(i).setGroup(i % 2).setText(textSamples.get(i)).build())
                .collect(Collectors.toList());

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                final RecordTypeBuilder complexDocRecordType = metaDataBuilder.getRecordType(COMPLEX_DOC);
                metaDataBuilder.addIndex(complexDocRecordType, COMPLEX_TEXT_BY_GROUP);
            });
            documents.forEach(recordStore::saveRecord);

            assertEquals(Collections.singletonList(Tuple.from(0L, 0L)),
                    queryComplexDocumentsWithIndex(Query.field("text").text().contains("angstrom"), 0, 372972877));
            assertEquals(Collections.emptyList(),
                    queryComplexDocumentsWithIndex(Query.field("text").text().containsAll("civil blood parents' strife"), 0L, -1615886689));
            assertEquals(Collections.singletonList(Tuple.from(1L, 1L)),
                    queryComplexDocumentsWithIndex(Query.field("text").text().containsAll("civil blood parents' strife"), 1L, -1615886658));
            assertEquals(Collections.emptyList(),
                    queryComplexDocumentsWithIndex(Query.field("text").text().containsAll("civil blood parents' strife", 4), 1L, -1436111364));
            assertEquals(Collections.singletonList(Tuple.from(1L, 1L)),
                    queryComplexDocumentsWithIndex(Query.field("text").text().containsAll("civil blood parents' strife", 35), 1L, -1436081573));
            assertEquals(Arrays.asList(Tuple.from(0L, 0L), Tuple.from(0L, 2L)),
                    queryComplexDocumentsWithIndex(Query.field("text").text().containsAny("angstrom parents king napoleons"), 0L, -1092421072));
            assertEquals(Collections.singletonList(Tuple.from(1L, 3L)),
                    queryComplexDocumentsWithIndex(Query.field("text").text().containsPhrase("recu un Thiers"), 1L, 1395848801));
            assertEquals(Collections.singletonList(Tuple.from(0L, 0L)),
                    queryComplexDocumentsWithIndex(Query.field("text").text().containsPrefix("ang"), 0L, -1013515738));
            assertEquals(Arrays.asList(Tuple.from(1L, 3L), Tuple.from(1L, 1L)),
                    queryComplexDocumentsWithIndex(Query.field("text").text().containsPrefix("un"), 1L, -995158140));
            assertEquals(ImmutableSet.of(Tuple.from(0L, 0L), Tuple.from(0L, 2L)),
                    ImmutableSet.copyOf(queryComplexDocumentsWithIndex(Query.field("text").text().containsAnyPrefix("ang par nap kin"), 0L, -1089713854)));
            assertEquals(Collections.singletonList(Tuple.from(0L, 0L)),
                    queryComplexDocumentsWithIndex(Query.field("text").text().containsAllPrefixes("ang uni name", false), 0L, 646414402));

            commit(context);
        }
    }

    @Test
    public void queryComplexDocumentsWithAdditionalFilters() throws Exception {
        final List<String> textSamples = Arrays.asList(
                TextSamples.ANGSTROM,
                TextSamples.ROMEO_AND_JULIET_PROLOGUE,
                TextSamples.AETHELRED,
                TextSamples.FRENCH,
                TextSamples.GERMAN,
                TextSamples.ROMEO_AND_JULIET_PROLOGUE,
                TextSamples.YIDDISH,
                "Napoleon and the Duke of Wellington met in Waterloo in 1815."
        );
        final List<ComplexDocument> documents = IntStream.range(0, textSamples.size())
                .mapToObj(i -> ComplexDocument.newBuilder()
                        .setDocId(i)
                        .setGroup(i % 2)
                        .setText(textSamples.get(i))
                        .addTag("3:" + (i % 3))
                        .setScore(i)
                        .build())
                .collect(Collectors.toList());

        final Index rankIndex = new Index("Complex$rank(score)", field("score").groupBy(field("group")), IndexTypes.RANK);
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                final RecordTypeBuilder complexDocRecordType = metaDataBuilder.getRecordType(COMPLEX_DOC);
                metaDataBuilder.addIndex(complexDocRecordType, COMPLEX_TEXT_BY_GROUP);
                metaDataBuilder.addIndex(complexDocRecordType, rankIndex);
            });
            documents.forEach(recordStore::saveRecord);

            assertEquals(Collections.singletonList(Tuple.from(1L, 5L)),
                    queryComplexDocumentsWithIndex(
                            Query.field("text").text().containsAll("fearful passage love", 7),
                            Query.field("tag").oneOfThem().equalsValue("3:2"),
                            1, 758136568));

            assertEquals(Arrays.asList(Tuple.from(1L, 1L), Tuple.from(1L, 5L)),
                    queryComplexDocumentsWithIndex(
                            Query.field("text").text().containsAll("fearful passage love", 7),
                            Query.field("text").text().containsPrefix("continu"),
                            1, -1043653062));

            assertEquals(Collections.singletonList(Tuple.from(1L, 7L)),
                    queryComplexDocumentsWithIndex(
                            Query.field("text").text().contains("napoleon"),
                            Query.and(Query.field("text").text().containsPrefix("th"), Query.field("text").text().contains("waterloo")),
                            1, -754900112));

            assertEquals(Collections.singletonList(Tuple.from(1L, 1L)),
                    queryComplexDocumentsWithIndex(
                            Query.field("text").text().containsAll("fearful passage love", 7),
                            Query.not(Query.field("tag").oneOfThem().equalsValue("3:2")),
                            1, 758136569));

            assertEquals(Collections.singletonList(Tuple.from(1L, 1L)),
                    queryComplexDocumentsWithIndex(
                            Query.field("text").text().containsAll("fearful passage love", 7),
                            Query.not(Query.field("tag").oneOfThem().equalsValue("3:2")),
                            1, 758136569));

            assertEquals(Arrays.asList(Tuple.from(0L, 0L), Tuple.from(0L, 6L)),
                    queryComplexDocumentsWithOr((OrComponent) Query.or(
                            Query.field("text").text().containsAll("unit named after"),
                            Query.field("text").text().containsPhrase("אן ארמיי און פלאט")
                    ), 0, -1558384887));

            assertEquals(Collections.singletonList(Tuple.from(1L, 5L)),
                    queryComplexDocumentsWithIndex(
                            Query.field("text").text().containsAll("fearful passage love", 7),
                            Query.or(
                                    Query.field("tag").oneOfThem().equalsValue("3:2"),
                                    Query.field("tag").oneOfThem().equalsValue("3:0")),
                            true, 1, -27568755));

            assertEquals(Collections.singletonList(Tuple.from(1L, 1L)),
                    queryComplexDocumentsWithIndex(
                            Query.field("text").text().containsAll("fearful passage love", 7),
                            Query.rank(field("score").groupBy(field("group"))).lessThan(2L),
                            true, 1, -2132208833));

            assertEquals(Collections.singletonList(Tuple.from(1L, 5L)),
                    queryComplexDocumentsWithIndex(
                            Query.field("text").text().containsAllPrefixes("fear pass love", true),
                            Query.field("tag").oneOfThem().equalsValue("3:2"),
                            true, 1, -419325379));

            assertEquals(Collections.singletonList(Tuple.from(1L, 5L)),
                    queryComplexDocumentsWithIndex(
                            Query.field("text").text().containsAllPrefixes("fear pass love", false),
                            Query.field("tag").oneOfThem().equalsValue("3:2"),
                            false, 1, -1902024530));

            assertEquals(Collections.singletonList(Tuple.from(1L, 1L)),
                    queryComplexDocumentsWithIndex(
                            Query.field("text").text().containsAllPrefixes("fear pass love"),
                            Query.rank(field("score").groupBy(field("group"))).lessThan(2L),
                            true, 1, 669157421));

            commit(context);
        }
    }

    @Test
    public void queryComplexDocumentsCovering() throws Exception {
        final List<String> textSamples = Arrays.asList(
                TextSamples.FRENCH,
                TextSamples.GERMAN,
                TextSamples.ROMEO_AND_JULIET_PROLOGUE,
                TextSamples.YIDDISH
        );
        final List<ComplexDocument> documents = IntStream.range(0, textSamples.size())
                .mapToObj(i -> ComplexDocument.newBuilder()
                        .setDocId(i)
                        .setGroup(i % 2)
                        .setText(textSamples.get(i))
                        .setScore(i)
                        .build())
                .collect(Collectors.toList());

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addIndex(COMPLEX_DOC, COMPLEX_TEXT_BY_GROUP);
            });
            documents.forEach(recordStore::saveRecord);

            // Try to plan a covered query with separate group and doc_id fields
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(COMPLEX_DOC)
                    .setRequiredResults(Arrays.asList(field("group"), field("doc_id")))
                    .setFilter(Query.and(
                            Query.field("group").equalsValue(0L),
                            Query.field("text").text().containsPhrase("continuance of their parents' rage")
                    ))
                    .build();
            RecordQueryPlan plan = planner.plan(query);
            assertThat(plan, coveringIndexScan(textIndexScan(allOf(
                    indexName(COMPLEX_TEXT_BY_GROUP.getName()),
                    groupingBounds(hasTupleString("[[0],[0]]")),
                    textComparison(equalTo(new Comparisons.TextComparison(Comparisons.Type.TEXT_CONTAINS_PHRASE, "continuance of their parents' rage", null, DefaultTextTokenizer.NAME)))))));
            assertEquals(822541560, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1798902497, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(770172924, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            List<ComplexDocument> results = recordStore.executeQuery(plan)
                    .map(rec -> ComplexDocument.newBuilder().mergeFrom(rec.getRecord()).build())
                    .asList()
                    .get();
            assertEquals(results.size(), 1);
            ComplexDocument result = results.get(0);
            assertEquals(result.getGroup(), 0L);
            assertEquals(result.getDocId(), 2L);
            assertThat(result.hasScore(), is(false));

            // Try to plan a covered query with one concatenated field
            query = RecordQuery.newBuilder()
                    .setRecordType(COMPLEX_DOC)
                    .setRequiredResults(Collections.singletonList(concatenateFields("group", "doc_id")))
                    .setFilter(Query.and(
                            Query.field("group").equalsValue(0L),
                            Query.field("text").text().containsPhrase("continuance of their parents' rage")
                    ))
                    .build();
            plan = planner.plan(query);
            assertThat(plan, coveringIndexScan(textIndexScan(allOf(
                    indexName(COMPLEX_TEXT_BY_GROUP.getName()),
                    groupingBounds(hasTupleString("[[0],[0]]")),
                    textComparison(equalTo(new Comparisons.TextComparison(Comparisons.Type.TEXT_CONTAINS_PHRASE, "continuance of their parents' rage", null, DefaultTextTokenizer.NAME)))))));
            assertEquals(822541560, plan.planHash(PlanHashable.PlanHashKind.LEGACY));
            assertEquals(-1798902497, plan.planHash(PlanHashable.PlanHashKind.FOR_CONTINUATION));
            assertEquals(770172924, plan.planHash(PlanHashable.PlanHashKind.STRUCTURAL_WITHOUT_LITERALS));
            results = recordStore.executeQuery(plan)
                    .map(rec -> ComplexDocument.newBuilder().mergeFrom(rec.getRecord()).build())
                    .asList()
                    .get();
            assertEquals(results.size(), 1);
            result = results.get(0);
            assertEquals(result.getGroup(), 0L);
            assertEquals(result.getDocId(), 2L);
            assertThat(result.hasScore(), is(false));

            commit(context);
        }
    }

    @Nonnull
    private List<Long> queryMultiTypeDocuments(@Nonnull QueryComponent textFilter, @Nonnull List<String> recordTypes, int planHash) throws InterruptedException, ExecutionException {
        if (!(textFilter instanceof ComponentWithComparison)) {
            throw new RecordCoreArgumentException("filter without comparison provided as text filter");
        }
        Matcher<RecordQueryPlan> planMatcher = descendant(textIndexScan(allOf(
                indexName(MULTI_TYPE_INDEX.getName()),
                textComparison(equalTo(((ComponentWithComparison)textFilter).getComparison())
        ))));
        if (recordTypes.size() != 2) {
            planMatcher = descendant(typeFilter(contains(recordTypes.get(0)), planMatcher));
        }
        return queryDocuments(recordTypes, Collections.singletonList(field("doc_id")), textFilter, planHash, planMatcher)
                .map(t -> t.getLong(0))
                .asList()
                .join();
    }

    @Test
    public void queryMultiTypeDocuments() throws Exception {
        final List<String> bothTypes = Arrays.asList(SIMPLE_DOC, COMPLEX_DOC);
        final List<String> simpleTypes = Collections.singletonList(SIMPLE_DOC);
        final List<String> complexTypes = Collections.singletonList(COMPLEX_DOC);

        final List<String> textSamples = Arrays.asList(
                TextSamples.ROMEO_AND_JULIET_PROLOGUE,
                TextSamples.ROMEO_AND_JULIET_PROLOGUE,
                TextSamples.ANGSTROM,
                TextSamples.AETHELRED,
                TextSamples.FRENCH,
                TextSamples.GERMAN
        );
        final List<Message> documents = IntStream.range(0, textSamples.size())
                .mapToObj(i -> {
                    final String text = textSamples.get(i);
                    if (i % 2 == 0) {
                        return SimpleDocument.newBuilder()
                                .setDocId(i)
                                .setText(text)
                                .setGroup(i % 4)
                                .build();
                    } else {
                        return ComplexDocument.newBuilder()
                                .setDocId(i)
                                .setText(text)
                                .setGroup(i % 4)
                                .build();
                    }
                })
                .collect(Collectors.toList());

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> {
                metaDataBuilder.getRecordType(COMPLEX_DOC).setPrimaryKey(field("doc_id"));
                metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
                metaDataBuilder.addMultiTypeIndex(Arrays.asList(
                        metaDataBuilder.getRecordType(SIMPLE_DOC),
                        metaDataBuilder.getRecordType(COMPLEX_DOC)
                ), MULTI_TYPE_INDEX);
            });
            documents.forEach(recordStore::saveRecord);

            assertEquals(Arrays.asList(0L, 1L),
                    queryMultiTypeDocuments(Query.field("text").text().containsPhrase("where we lay our scene"), bothTypes, 1755757799));
            assertEquals(Collections.singletonList(0L),
                    queryMultiTypeDocuments(Query.field("text").text().containsPhrase("where we lay our scene"), simpleTypes, -1489953261));
            assertEquals(Collections.singletonList(1L),
                    queryMultiTypeDocuments(Query.field("text").text().containsPhrase("where we lay our scene"), complexTypes, -1333764399));

            assertEquals(Arrays.asList(2L, 4L, 5L),
                    queryMultiTypeDocuments(Query.field("text").text().containsPrefix("na"), bothTypes, -714642562));
            assertEquals(Arrays.asList(2L, 4L),
                    queryMultiTypeDocuments(Query.field("text").text().containsPrefix("na"), simpleTypes, 334613674));
            assertEquals(Collections.singletonList(5L),
                    queryMultiTypeDocuments(Query.field("text").text().containsPrefix("na"), complexTypes, 490802536));

            commit(context);
        }
    }

    @Nonnull
    private List<Long> queryMapDocumentsWithScan(@Nonnull QueryComponent filter, int planHash) throws InterruptedException, ExecutionException {
        return queryDocuments(Collections.singletonList(MAP_DOC), Collections.singletonList(field("doc_id")), filter, planHash,
                filter(filter, typeFilter(equalTo(Collections.singleton(MAP_DOC)), PlanMatchers.scan(unbounded()))))
                .map(t -> t.getLong(0))
                .asList()
                .get();
    }

    @Nonnull
    private List<Long> queryMapDocumentsWithIndex(@Nonnull String key, @Nonnull QueryComponent textFilter, int planHash, boolean isCoveringIndexExpected) throws InterruptedException, ExecutionException {
        if (!(textFilter instanceof ComponentWithComparison)) {
            throw new RecordCoreArgumentException("filter without comparison provided as text filter");
        }
        final QueryComponent filter = Query.field("entry").oneOfThem().matches(Query.and(Query.field("key").equalsValue(key), textFilter));
        Matcher<RecordQueryPlan> indexMatcher = textIndexScan(allOf(
                indexName(MAP_ON_VALUE_INDEX.getName()),
                groupingBounds(allOf(notNullValue(), hasTupleString("[[" + key + "],[" + key + "]]"))),
                textComparison(equalTo(((ComponentWithComparison)textFilter).getComparison())))
        );
        if (isCoveringIndexExpected) {
            indexMatcher = coveringIndexScan(indexMatcher);
        }
        final Matcher<RecordQueryPlan> planMatcher = descendant(indexMatcher);
        return queryDocuments(Collections.singletonList(MAP_DOC), Collections.singletonList(field("doc_id")), filter, planHash, planMatcher)
                .map(t -> t.getLong(0))
                .asList()
                .get();
    }

    @Nonnull
    private List<Long> queryMapDocumentsWithGroupedIndex(@Nonnull String key, @Nonnull QueryComponent textFilter, long group, int planHash) throws InterruptedException, ExecutionException {
        if (!(textFilter instanceof ComponentWithComparison)) {
            throw new RecordCoreArgumentException("filter without comparison provided as text filter");
        }
        final QueryComponent filter = Query.and(
                Query.field("group").equalsValue(group),
                Query.field("entry").oneOfThem().matches(Query.and(Query.field("key").equalsValue(key), textFilter))
        );
        final Matcher<RecordQueryPlan> planMatcher = descendant(coveringIndexScan(textIndexScan(allOf(
                indexName(MAP_ON_VALUE_GROUPED_INDEX.getName()),
                groupingBounds(allOf(notNullValue(), hasTupleString("[[" + group + ", " + key + "],[" + group + ", " + key + "]]"))),
                textComparison(equalTo(((ComponentWithComparison)textFilter).getComparison())))
        )));
        return queryDocuments(Collections.singletonList(MAP_DOC), Collections.singletonList(field("doc_id")), filter, planHash, planMatcher)
                .map(t -> t.getLong(0))
                .asList()
                .get();
    }

    @Test
    public void queryMapDocuments() throws Exception {
        final List<String> textSamples = Arrays.asList(
                TextSamples.ROMEO_AND_JULIET_PROLOGUE,
                TextSamples.AETHELRED,
                TextSamples.ROMEO_AND_JULIET_PROLOGUE,
                TextSamples.ANGSTROM,
                TextSamples.AETHELRED,
                TextSamples.FRENCH
        );
        final List<MapDocument> documents = IntStream.range(0, textSamples.size() / 2)
                .mapToObj(i -> MapDocument.newBuilder()
                        .setDocId(i)
                        .addEntry(MapDocument.Entry.newBuilder().setKey("a").setValue(textSamples.get(i * 2)).build())
                        .addEntry(MapDocument.Entry.newBuilder().setKey("b").setValue(textSamples.get(i * 2 + 1)).build())
                        .setGroup(i % 2)
                        .build()
                )
                .collect(Collectors.toList());

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> metaDataBuilder.addIndex(MAP_DOC, MAP_ON_VALUE_INDEX));
            documents.forEach(recordStore::saveRecord);

            assertEquals(Collections.singletonList(2L),
                    queryMapDocumentsWithIndex("a", Query.field("value").text().containsAny("king unknown_token"), 1059912699, true));
            assertEquals(Arrays.asList(0L, 1L),
                    queryMapDocumentsWithIndex("a", Query.field("value").text().containsPhrase("civil blood makes civil hands unclean"), 1085034960, true));
            assertEquals(Collections.emptyList(),
                    queryMapDocumentsWithIndex("b", Query.field("value").text().containsPhrase("civil blood makes civil hands unclean"), 1085034991, true));
            assertEquals(Arrays.asList(1L, 2L),
                    queryMapDocumentsWithIndex("b", Query.field("value").text().containsPrefix("na"), 1125182095, true));
            assertEquals(Arrays.asList(0L, 1L),
                    queryMapDocumentsWithIndex("a", Query.field("value").text().containsAllPrefixes("civ mut ha"), 0, false));
            assertEquals(Arrays.asList(1L, 2L),
                    queryMapDocumentsWithIndex("b", Query.field("value").text().containsAnyPrefix("civ mut na"), 0, true));

            RecordQuery queryWithAdditionalFilter = RecordQuery.newBuilder()
                    .setRecordType(MAP_DOC)
                    .setFilter(Query.and(
                            Query.field("group").equalsValue(0L),
                            Query.field("entry").oneOfThem().matches(Query.and(
                                    Query.field("key").equalsValue("b"),
                                    Query.field("value").text().containsAny("anders king")
                            ))
                    ))
                    .build();
            RecordQueryPlan planWithAdditionalFilter = recordStore.planQuery(queryWithAdditionalFilter);
            assertThat(planWithAdditionalFilter, filter(Query.field("group").equalsValue(0L), descendant(textIndexScan(anything()))));
            List<Long> queryResults = recordStore.executeQuery(planWithAdditionalFilter).map(FDBQueriedRecord::getPrimaryKey).map(tuple -> tuple.getLong(0)).asList().join();
            assertEquals(Collections.singletonList(0L), queryResults);

            queryWithAdditionalFilter = RecordQuery.newBuilder()
                    .setRecordType(MAP_DOC)
                    .setFilter(Query.or(
                            Query.field("entry").oneOfThem().matches(Query.and(
                                    Query.field("key").equalsValue("a"),
                                    Query.field("value").text().containsPhrase("bury their parents strife")
                            )),
                            Query.field("entry").oneOfThem().matches(Query.and(
                                    Query.field("key").equalsValue("b"),
                                    Query.field("value").text().containsPrefix("th")
                            ))
                    ))
                    .build();
            planWithAdditionalFilter = recordStore.planQuery(queryWithAdditionalFilter);
            assertThat(planWithAdditionalFilter, primaryKeyDistinct(unorderedUnion(
                    descendant(textIndexScan(indexName(equalTo(MAP_ON_VALUE_INDEX.getName())))),
                    descendant(textIndexScan(indexName(equalTo(MAP_ON_VALUE_INDEX.getName()))))
            )));
            queryResults = recordStore.executeQuery(planWithAdditionalFilter).map(FDBQueriedRecord::getPrimaryKey).map(tuple -> tuple.getLong(0)).asList().join();
            assertEquals(3, queryResults.size());
            assertEquals(ImmutableSet.of(0L, 1L, 2L), ImmutableSet.copyOf(queryResults));

            // Planner bug that can happen with certain malformed queries. This plan actually
            // returns records where the key and the value match in the same entry, but it is
            // asking for all records where *any* entry has a key matching "a" and *any* entry
            // has a value matching the text predicate. In reality, this is probably a sign
            // the user didn't input their query correctly, but it requires more work from the
            // planner not to plan this kind of query.
            // FIXME: Full Text: The Planner doesn't always correctly handle ands with nesteds (https://github.com/FoundationDB/fdb-record-layer/issues/53)
            final QueryComponent malformedMapFilter = Query.and(
                    Query.field("entry").oneOfThem().matches(Query.field("key").equalsValue("a")),
                    Query.field("entry").oneOfThem().matches(Query.field("value").text().containsAll("civil hands unclean")));
            RecordQueryPlan malformedMapPlan = planner.plan(RecordQuery.newBuilder().setRecordType(MAP_DOC).setFilter(malformedMapFilter).build());
            assertThat(malformedMapPlan, descendant(textIndexScan(allOf(
                    indexName(MAP_ON_VALUE_INDEX.getName()),
                    groupingBounds(allOf(notNullValue(), hasTupleString("[[a],[a]]"))),
                    textComparison(equalTo(new Comparisons.TextComparison(Comparisons.Type.TEXT_CONTAINS_ALL, "civil hands unclean", null, DefaultTextTokenizer.NAME)))
            ))));

            commit(context);
        }
    }

    @Test
    public void queryMapsWithGroups() throws Exception {
        final List<String> textSamples = Arrays.asList(
                TextSamples.ROMEO_AND_JULIET_PROLOGUE,
                TextSamples.AETHELRED,
                TextSamples.ROMEO_AND_JULIET_PROLOGUE,
                TextSamples.ANGSTROM
        );
        final List<MapDocument> documents = IntStream.range(0, textSamples.size() / 2)
                .mapToObj(i -> MapDocument.newBuilder()
                        .setDocId(i)
                        .addEntry(MapDocument.Entry.newBuilder().setKey("a").setValue(textSamples.get(i * 2)).build())
                        .addEntry(MapDocument.Entry.newBuilder().setKey("b").setValue(textSamples.get(i * 2 + 1)).build())
                        .setGroup(i % 2)
                        .build()
                )
                .collect(Collectors.toList());

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> metaDataBuilder.addIndex(MAP_DOC, MAP_ON_VALUE_GROUPED_INDEX));
            documents.forEach(recordStore::saveRecord);

            assertEquals(Collections.singletonList(1L),
                    queryMapDocumentsWithGroupedIndex("a", Query.field("value").text().containsPhrase("both alike in dignity"), 1L, 1376087127));
            assertEquals(Collections.singletonList(0L),
                    queryMapDocumentsWithGroupedIndex("b", Query.field("value").text().containsAny("king anders"), 0L, -1204479544));

            commit(context);
        }
    }

    @Nonnull
    private Set<Long> performQueryWithRecordStoreScan(@Nonnull RecordMetaDataHook hook, @Nonnull QueryComponent filter) throws Exception {
        final ScanProperties scanProperties = new ScanProperties(ExecuteProperties.newBuilder().setTimeLimit(3000).build());
        Set<Long> results = new HashSet<>();
        byte[] continuation = null;
        do {
            try (FDBRecordContext context = openContext()) {
                openRecordStore(context);
                try (RecordCursor<Long> cursor = recordStore.scanRecords(continuation, scanProperties)
                        .filter(record -> record.getRecordType().getName().equals(SIMPLE_DOC))
                        .filter(record -> filter.eval(recordStore, EvaluationContext.EMPTY, record) == Boolean.TRUE)
                        .map(record -> record.getPrimaryKey().getLong(0))) {
                    cursor.forEach(results::add).get();

                    RecordCursorResult<Long> noNextResult = cursor.getNext();
                    continuation = noNextResult.getContinuation().toBytes();
                }
            }
        } while (continuation != null);
        return results;
    }

    @Nonnull
    private Set<Long> performQueryWithIndexScan(@Nonnull RecordMetaDataHook hook, @Nonnull Index index, @Nonnull QueryComponent filter) throws Exception {
        final ExecuteProperties executeProperties = ExecuteProperties.newBuilder().setTimeLimit(3000).build();
        final RecordQuery query = RecordQuery.newBuilder()
                .setRecordType(SIMPLE_DOC)
                .setRequiredResults(Collections.singletonList(field("doc_id")))
                .setFilter(filter)
                .build();
        Set<Long> results = new HashSet<>();
        RecordQueryPlan plan;
        byte[] continuation;

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, hook);
            RecordQueryPlanner planner = new RecordQueryPlanner(recordStore.getRecordMetaData(), recordStore.getRecordStoreState());
            plan = planner.plan(query);
            assertThat(filter, instanceOf(FieldWithComparison.class));
            FieldWithComparison fieldFilter = (FieldWithComparison)filter;
            if (fieldFilter.getComparison() instanceof Comparisons.TextContainsAllPrefixesComparison
                    && ((Comparisons.TextContainsAllPrefixesComparison)fieldFilter.getComparison()).isStrict()) {
                // Strict field field comparisons cannot be covering
                assertThat(plan, descendant(textIndexScan(indexName(index.getName()))));
            } else {
                assertThat(plan, descendant(coveringIndexScan(textIndexScan(indexName(index.getName())))));
            }

            try (RecordCursor<Long> cursor = recordStore.executeQuery(plan, null, executeProperties)
                    .map(record -> record.getPrimaryKey().getLong(0))) {
                cursor.forEach(results::add).get();
                continuation = cursor.getNext().getContinuation().toBytes();
            }
        }

        while (continuation != null) {
            try (FDBRecordContext context = openContext()) {
                openRecordStore(context, hook);
                try (RecordCursor<Long> cursor = recordStore.executeQuery(plan, continuation, executeProperties)
                        .map(record -> record.getPrimaryKey().getLong(0))) {
                    cursor.forEach(results::add).get();
                    continuation = cursor.getNext().getContinuation().toBytes();
                }
            }
        }

        return results;
    }

    @Nonnull
    public static Stream<Arguments> indexArguments() {
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsTextProto.getDescriptor());
        Index simpleIndex = metaDataBuilder.getIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
        simpleIndex.setSubspaceKey(TextIndexTestUtils.SIMPLE_DEFAULT_NAME + "-2");
        return Stream.of(simpleIndex, SIMPLE_TEXT_FILTERING, SIMPLE_TEXT_PREFIX, SIMPLE_TEXT_SUFFIXES)
                .map(Arguments::of);
    }

    /**
     * Generate random documents and then make sure that querying them using the index
     * produces the same result as performing a full scan of all records.
     */
    @MethodSource("indexArguments")
    @ParameterizedTest
    public void queryScanEquivalence(@Nonnull Index index) throws Exception {
        final Random r = new Random(0xba5eba1L + index.getName().hashCode());
        final int recordCount = 100;
        final int recordBatch = 25;
        final int queryCount = 25;
        final List<String> lexicon = getStandardLexicon();

        TextTokenizerRegistryImpl.instance().register(FILTERING_TOKENIZER);
        final TextTokenizer tokenizer = TextIndexMaintainer.getTokenizer(index);
        final RecordMetaDataHook hook = metaDataBuilder -> {
            metaDataBuilder.removeIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME);
            metaDataBuilder.addIndex(SIMPLE_DOC, index);
        };

        long seed = r.nextLong();
        LOGGER.info(KeyValueLogMessage.of("initializing random number generator", TestLogMessageKeys.SEED, seed));
        r.setSeed(seed);

        for (int i = 0; i < recordCount; i += recordBatch) {
            List<SimpleDocument> records = getRandomRecords(r, recordBatch, lexicon);
            LOGGER.info(KeyValueLogMessage.of("creating and saving random records", TestLogMessageKeys.BATCH_SIZE, recordBatch));
            try (FDBRecordContext context = openContext()) {
                openRecordStore(context, hook);
                records.forEach(recordStore::saveRecord);
                commit(context);
            }
        }

        double[] proportions = getZipfProportions(lexicon);
        long totalScanningTime = 0;
        long totalQueryingTime = 0;
        long totalResults = 0;

        for (int i = 0; i < queryCount; i++) {
            // Generate a random text query
            List<String> tokens = getRandomWords(r, lexicon, proportions, 6, 3);
            String tokenString = String.join(" ", tokens);
            double filterChoice = r.nextDouble();
            final QueryComponent filter;
            if (filterChoice < 0.2) {
                filter = Query.field("text").text(tokenizer.getName()).containsAll(tokenString);
            } else if (filterChoice < 0.4) {
                filter = Query.field("text").text(tokenizer.getName()).containsAny(tokenString);
            } else if (filterChoice < 0.6) {
                filter = Query.field("text").text(tokenizer.getName()).containsPhrase(tokenString);
            } else if (filterChoice < 0.8) {
                int maxDistance = r.nextInt(10) + tokens.size();
                filter = Query.field("text").text(tokenizer.getName()).containsAll(tokenString, maxDistance);
            } else if (filterChoice < 0.9) {
                filter = Query.field("text").text(tokenizer.getName()).containsAnyPrefix(tokenString);
            } else if (filterChoice < 0.95) {
                filter = Query.field("text").text(tokenizer.getName()).containsAllPrefixes(tokenString);
            } else {
                if (tokens.isEmpty()) {
                    continue;
                }
                // Choose the first non-empty token from the iterator
                Iterator<? extends CharSequence> tokenIterator = tokenizer.tokenize(tokenString, tokenizer.getMaxVersion(), TextTokenizer.TokenizerMode.QUERY);
                String firstToken = null;
                while (tokenIterator.hasNext()) {
                    String nextToken = tokenIterator.next().toString();
                    if (!nextToken.isEmpty()) {
                        firstToken = nextToken;
                        break;
                    }
                }
                if (firstToken == null) {
                    continue;
                }
                int prefixEnd;
                if (firstToken.length() > 1) {
                    prefixEnd = r.nextInt(firstToken.length() - 1) + 1;
                } else {
                    prefixEnd = 1;
                }
                filter = Query.field("text").text(tokenizer.getName()).containsPrefix(firstToken.substring(0, prefixEnd));
            }
            LOGGER.info(KeyValueLogMessage.of("generated random filter",
                            TestLogMessageKeys.ITERATION, i,
                            LogMessageKeys.FILTER, filter));

            // Manual scan all of the records
            long startTime = System.nanoTime();
            final Set<Long> manualRecordIds = performQueryWithRecordStoreScan(hook, filter);
            long endTime = System.nanoTime();
            LOGGER.info(KeyValueLogMessage.of("manual scan completed", TestLogMessageKeys.SCAN_MILLIS, TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS)));
            totalScanningTime += endTime - startTime;

            // Generate a query and use the index
            startTime = System.nanoTime();
            final Set<Long> queryRecordIds = performQueryWithIndexScan(hook, index, filter);
            endTime = System.nanoTime();
            LOGGER.info(KeyValueLogMessage.of("query completed", TestLogMessageKeys.SCAN_MILLIS, TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS)));
            totalQueryingTime += endTime - startTime;

            if (!manualRecordIds.equals(queryRecordIds)) {
                Set<Long> onlyManual = new HashSet<>(manualRecordIds);
                onlyManual.removeAll(queryRecordIds);
                Set<Long> onlyQuery = new HashSet<>(queryRecordIds);
                onlyManual.removeAll(manualRecordIds);
                LOGGER.warn(KeyValueLogMessage.of("results did not match",
                        LogMessageKeys.FILTER, filter,
                        TestLogMessageKeys.MANUAL_RESULT_COUNT, manualRecordIds.size(),
                        TestLogMessageKeys.QUERY_RESULT_COUNT, queryRecordIds.size(),
                        TestLogMessageKeys.ONLY_MANUAL_COUNT, onlyManual.size(),
                        TestLogMessageKeys.ONLY_QUERY_COUNT, onlyQuery.size()));
            }
            assertEquals(manualRecordIds, queryRecordIds);
            LOGGER.info(KeyValueLogMessage.of("results matched", LogMessageKeys.FILTER, filter, TestLogMessageKeys.RESULT_COUNT, manualRecordIds.size()));
            totalResults += queryRecordIds.size();
        }

        LOGGER.info(KeyValueLogMessage.of("test completed",
                        TestLogMessageKeys.TOTAL_SCAN_MILLIS, TimeUnit.MILLISECONDS.convert(totalScanningTime, TimeUnit.NANOSECONDS),
                        TestLogMessageKeys.TOTAL_QUERY_MILLIS, TimeUnit.MILLISECONDS.convert(totalQueryingTime, TimeUnit.NANOSECONDS),
                        TestLogMessageKeys.TOTAL_RESULT_COUNT, totalResults));
    }

    @Test
    public void invalidQueries() {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context, metaDataBuilder -> metaDataBuilder.addIndex(SIMPLE_DOC, new Index("SimpleDocument$max_group", field("group").ungrouped(), IndexTypes.MAX_EVER_TUPLE)));
            List<RecordQuery> queries = Arrays.asList(
                    RecordQuery.newBuilder()
                            .setRecordType(SIMPLE_DOC)
                            .setFilter(Query.field("text").equalsValue("asdf"))
                            .build(),
                    RecordQuery.newBuilder()
                            .setRecordType(SIMPLE_DOC)
                            .setFilter(Query.field("text").startsWith("asdf"))
                            .build()
            // FIXME: This query requires improving the planner to not pick incorrect indexes for sorting
            /*
                    RecordQuery.newBuilder()
                            .setRecordType(SIMPLE_DOC)
                            .setSort(field("text"))
                            .build()
             */
            );
            for (RecordQuery query : queries) {
                try {
                    RecordQueryPlan plan = planner.plan(query);
                    assertThat(plan.getUsedIndexes(), not(contains(TextIndexTestUtils.SIMPLE_DEFAULT_NAME)));
                } catch (RecordCoreException e) {
                    assertThat(e.getMessage(), containsString("Cannot sort without appropriate index"));
                }
            }
        }
    }

    @Nonnull
    private <T extends Exception> T invalidateIndex(@Nonnull Class<T> clazz, @Nonnull String recordType, @Nonnull Index index) {
        return assertThrows(clazz, () -> {
            RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecordsTextProto.getDescriptor());
            metaDataBuilder.addIndex(recordType, index);
            metaDataBuilder.getRecordType(COMPLEX_DOC).setPrimaryKey(concatenateFields("group", "doc_id"));
            RecordMetaData metaData = metaDataBuilder.getRecordMetaData();
        });
    }

    @Nonnull
    private <T extends Exception> T invalidateIndex(@Nonnull Class<T> clazz, @Nonnull Index index) {
        return invalidateIndex(clazz, SIMPLE_DOC, index);
    }

    @Test
    public void invalidIndexes() {
        final String testIndex = "test_index";
        List<KeyExpression> expressions = Arrays.asList(
                // VersionKeyExpression.VERSION, // no version expression allowed
                concat(field("text"), VersionKeyExpression.VERSION), // Version but not where text is
                field("group"), // not on text field
                field("group").groupBy(field("text")),
                concatenateFields("group", "text"),
                field("text", FanType.FanOut)
        );
        for (KeyExpression expression : expressions) {
            LOGGER.info(KeyValueLogMessage.of("testing index expression", LogMessageKeys.KEY_EXPRESSION, expression));
            invalidateIndex(KeyExpression.InvalidExpressionException.class,
                    new Index(testIndex, expression, IndexTypes.TEXT));
        }

        // Verify doesn't work with repeated fields but in a case where a repeated field is possible.
        invalidateIndex(KeyExpression.InvalidExpressionException.class, "MultiDocument",
                new Index(testIndex, field("text", FanType.FanOut), IndexTypes.TEXT));

        // Verify that unique indexes are invalid
        invalidateIndex(MetaDataException.class,
                new Index(testIndex, field("text"), EmptyKeyExpression.EMPTY, IndexTypes.TEXT, IndexOptions.UNIQUE_OPTIONS));

        // Verify that it fails when there is a value expression
        invalidateIndex(KeyExpression.InvalidExpressionException.class,
                new Index(testIndex, field("text"), field("group"), IndexTypes.TEXT, IndexOptions.EMPTY_OPTIONS));
        invalidateIndex(KeyExpression.InvalidExpressionException.class,
                new Index(testIndex, field("text").groupBy(field("group")), field("group"), IndexTypes.TEXT, IndexOptions.EMPTY_OPTIONS));

        // Tokenizer does not exist
        invalidateIndex(MetaDataException.class,
                new Index(testIndex, field("text"), IndexTypes.TEXT, ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, "not_a_real_tokenizer")));

        // Invalid tokenizer versions
        invalidateIndex(MetaDataException.class,
                new Index(testIndex, field("text"), IndexTypes.TEXT, ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, PrefixTextTokenizer.NAME, IndexOptions.TEXT_TOKENIZER_VERSION_OPTION, "-1")));
        invalidateIndex(MetaDataException.class,
                new Index(testIndex, field("text"), IndexTypes.TEXT, ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, PrefixTextTokenizer.NAME, IndexOptions.TEXT_TOKENIZER_VERSION_OPTION, "1000")));
        invalidateIndex(MetaDataException.class,
                new Index(testIndex, field("text"), IndexTypes.TEXT, ImmutableMap.of(IndexOptions.TEXT_TOKENIZER_NAME_OPTION, PrefixTextTokenizer.NAME, IndexOptions.TEXT_TOKENIZER_VERSION_OPTION, "one")));
    }

    @Nonnull
    private List<String> getStandardLexicon() {
        List<String> words = new ArrayList<>();
        for (String sample : TextSamples.ALL) {
            words.addAll(Arrays.asList(sample.split(" ")));
        }
        return words;
    }

    @Nonnull
    private double[] getZipfProportions(@Nonnull List<String> lexicon) {
        // Use a zipf distribution for tokens. This is roughly the distribution of words in human
        // text, though in real text, the more common words are also shorter, so this is somewhat bogus.
        double hN = 0.0;
        for (int i = 0; i < lexicon.size(); i++) {
            hN += 1.0 / (i + 1);
        }
        if (hN == 0.0) {
            // An empty lexicon was provided.
            throw new IllegalArgumentException("cannot generate words with an empty lexicon");
        }
        double[] proportions = new double[lexicon.size()];
        double hI = 0.0;
        for (int i = 0; i < lexicon.size(); i++) {
            hI += 1.0 / (i + 1);
            proportions[i] = hI / hN;
        }
        return proportions;
    }

    @Nonnull
    private List<String> getRandomWords(@Nonnull Random r, @Nonnull List<String> lexicon, @Nonnull double[] proportions, int tokenAverage, int tokenSd) {
        int tokenCount = (int)Math.abs(r.nextGaussian() * tokenSd + tokenAverage);
        List<String> words = new ArrayList<>(tokenCount);
        for (int j = 0; j < tokenCount; j++) {
            double tokenChoice = r.nextDouble();
            int tokenIndex = Arrays.binarySearch(proportions, tokenChoice);
            if (tokenIndex < 0) {
                // If Arrays.binarySearch returns a negative number, it gives us
                // i = (-insertion_point) - 1. We want to find the insertion point,
                // i.e., the first value greater than our random choice, so
                // we find insertion_point = -i - 1;
                tokenIndex = -tokenIndex - 1;
            }
            words.add(lexicon.get(tokenIndex));
        }
        return words;
    }

    @Nonnull
    private List<SimpleDocument> getRandomRecords(@Nonnull Random r, int count, @Nonnull List<String> lexicon, int tokenAverage, int tokenSd) {
        List<SimpleDocument> list = new ArrayList<>(count);

        double[] proportions = getZipfProportions(lexicon);

        for (int i = 0; i < count; i++) {
            long id = r.nextLong();
            List<String> words = getRandomWords(r, lexicon, proportions, tokenAverage, tokenSd);
            SimpleDocument document = SimpleDocument.newBuilder()
                    .setDocId(id)
                    .setText(String.join(" ", words))
                    .build();
            list.add(document);
        }

        return list;
    }

    @Nonnull
    private List<SimpleDocument> getRandomRecords(@Nonnull Random r, int count, @Nonnull List<String> lexicon) {
        return getRandomRecords(r, count, lexicon, 1000, 100);
    }

    @Nonnull
    private List<SimpleDocument> getRandomRecords(@Nonnull Random r, int count) {
        return getRandomRecords(r, count, getStandardLexicon());
    }

    private void printUsage() throws Exception {
        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            Subspace indexSubspace = recordStore.getIndexMaintainer(recordStore.getRecordMetaData().getIndex(TextIndexTestUtils.SIMPLE_DEFAULT_NAME)).getIndexSubspace();
            final int indexSuspaceLength = indexSubspace.getKey().length;
            int subspaceOverhead = 0;
            int keySize = 0;
            int valueSize = 0;
            for (KeyValue kv : context.ensureActive().getRange(indexSubspace.range())) {
                subspaceOverhead += indexSuspaceLength;
                keySize += kv.getKey().length - indexSuspaceLength;
                valueSize += kv.getValue().length;
            }

            int textSize = 0;
            RecordCursorIterator<String> cursor = recordStore.scanRecords(null, ScanProperties.FORWARD_SCAN)
                    .map(record -> {
                        Message msg = record.getRecord();
                        Descriptors.FieldDescriptor fd = msg.getDescriptorForType().findFieldByName("text");
                        return msg.getField(fd).toString();
                    })
                    .asIterator();
            while (cursor.hasNext()) {
                textSize += cursor.next().length();
            }

            LOGGER.info("Usage:");
            LOGGER.info("  Subspace: {} kB", subspaceOverhead * 1e-3);
            LOGGER.info("  Keys:     {} kB", keySize * 1e-3);
            LOGGER.info("  Values:   {} kB", valueSize * 1e-3);
            LOGGER.info("  Text:     {} kB", textSize * 1e-3);
            LOGGER.info("  Overhead: {}", textSize == 0.0 ? Double.POSITIVE_INFINITY : ((subspaceOverhead + keySize + valueSize) * 1.0 / textSize));
        }
    }

    @Tag(Tags.Performance)
    @Test
    public void textIndexPerf1000SerialInsert() throws Exception {
        // Create 1000 records
        Random r = new Random();
        List<SimpleDocument> records = getRandomRecords(r, 1000);

        long startTime = System.nanoTime();

        for (int i = 0; i < records.size(); i += 10) {
            try (FDBRecordContext context = openContext()) {
                openRecordStore(context);
                for (SimpleDocument document : records.subList(i, i + 10)) {
                    recordStore.saveRecord(document);
                }
                commit(context);
            }
        }

        long endTime = System.nanoTime();
        LOGGER.info("performed 1000 serial insertions in {} seconds.", (endTime - startTime) * 1e-9);
        printUsage();
    }

    @Tag(Tags.Performance)
    @Test
    public void textIndexPerf100InsertOneBatch() throws Exception {
        // Create 1000 records
        Random r = new Random();
        List<SimpleDocument> records = getRandomRecords(r, 100);

        long startTime = System.nanoTime();

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            for (int i = 0; i < records.size(); i ++) {
                recordStore.saveRecord(records.get(i));
            }
            commit(context);
        }

        long endTime = System.nanoTime();
        LOGGER.info("performed 100 serial insertions in {} seconds.", (endTime - startTime) * 1e-9);
        printUsage();
    }

    @Tag(Tags.Performance)
    @Test
    public void textIndexPerf1000SerialInsertNoBatching() throws Exception {
        // Create 1000 records
        Random r = new Random();
        List<SimpleDocument> records = getRandomRecords(r, 1000);

        long startTime = System.nanoTime();

        for (int i = 0; i < records.size(); i ++) {
            try (FDBRecordContext context = openContext()) {
                openRecordStore(context);
                recordStore.saveRecord(records.get(i));
                commit(context);
            }
        }

        long endTime = System.nanoTime();
        LOGGER.info("performed 1000 serial insertions in {} seconds.", (endTime - startTime) * 1e-9);
        printUsage();
    }

    @Tag(Tags.Performance)
    @Test
    public void textIndexPerf1000ParallelInsert() throws Exception {
        // Create 1000 records
        Random r = new Random();
        List<SimpleDocument> records = getRandomRecords(r, 1000);

        try (FDBRecordContext context = openContext()) {
            openRecordStore(context);
            recordStore.asBuilder().create();
            commit(context);
        }
        final FDBRecordStore.Builder storeBuilder = recordStore.asBuilder();

        long startTime = System.nanoTime();

        int oldMaxAttempts = FDBDatabaseFactory.instance().getMaxAttempts();
        FDBDatabaseFactory.instance().setMaxAttempts(Integer.MAX_VALUE);

        try {
            CompletableFuture<?>[] workerFutures = new CompletableFuture<?>[10];
            int recordsPerWorker = records.size() / workerFutures.length;
            for (int i = 0; i < workerFutures.length; i++) {
                List<SimpleDocument> workerDocs = records.subList(i * recordsPerWorker, (i + 1) * recordsPerWorker);
                CompletableFuture<Void> workerFuture = new CompletableFuture<>();
                Thread workerThread = new Thread(() -> {
                    try {
                        for (int j = 0; j < workerDocs.size(); j += 10) {
                            // Use retry loop to catch not_committed errors
                            List<SimpleDocument> batchDocuments = workerDocs.subList(j, j + 10);
                            fdb.run(context -> {
                                try {
                                    FDBRecordStore store = storeBuilder.copyBuilder().setContext(context).open();
                                    for (SimpleDocument document : batchDocuments) {
                                        store.saveRecord(document);
                                    }
                                    return null;
                                } catch (RecordCoreException e) {
                                    throw e;
                                } catch (Exception e) {
                                    throw new RecordCoreException(e);
                                }
                            });
                        }
                        workerFuture.complete(null);
                    } catch (RuntimeException e) {
                        workerFuture.completeExceptionally(e);
                    }
                });
                workerThread.setName("insert-worker-" + i);
                workerThread.start();
                workerFutures[i] = workerFuture;
            }
            CompletableFuture.allOf(workerFutures).get();

            long endTime = System.nanoTime();
            LOGGER.info("performed 1000 parallel insertions in {} seconds.", (endTime - startTime) * 1e-9);
            printUsage();
        } finally {
            FDBDatabaseFactory.instance().setMaxAttempts(oldMaxAttempts);
        }
    }
}
