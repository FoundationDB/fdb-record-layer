/*
 * FDBLuceneHighlightingTest.java
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

package com.apple.foundationdb.record.lucene.highlight;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TestRecordsTextProto;
import com.apple.foundationdb.record.lucene.LuceneIndexTestUtils;
import com.apple.foundationdb.record.lucene.LuceneQueryComponent;
import com.apple.foundationdb.record.lucene.LuceneQueryType;
import com.apple.foundationdb.record.lucene.LuceneScanBounds;
import com.apple.foundationdb.record.lucene.LuceneScanQueryParameters;
import com.apple.foundationdb.record.lucene.synonym.EnglishSynonymMapConfig;
import com.apple.foundationdb.record.lucene.synonym.SynonymMapRegistryImpl;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.IndexOrphanBehavior;
import com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.QueryComponent;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.query.plan.plans.RecordQueryPlan;
import com.apple.foundationdb.record.util.pair.Pair;
import com.google.common.collect.Lists;
import com.google.protobuf.Message;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.NGRAM_LUCENE_INDEX;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.QUERY_ONLY_SYNONYM_LUCENE_INDEX;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.SIMPLE_TEXT_SUFFIXES;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.TEXT_AND_STORED;
import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.TEXT_AND_STORED_COMPLEX;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.COMPLEX_DOC;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.SIMPLE_DOC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests around the highlighting structure as used in Lucene.
 */
public class FDBLuceneHighlightingTest extends FDBRecordStoreTestBase {

    @BeforeAll
    public static void setup() {
        //set up the English Synonym Map so that we don't spend forever setting it up for every test, because this takes a long time
        SynonymMapRegistryImpl.instance().getSynonymMap(EnglishSynonymMapConfig.ExpandedEnglishSynonymMapConfig.CONFIG_NAME);
    }

    @Test
    void highlightedPrefix() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            recordStore.saveRecord(LuceneIndexTestUtils.createSimpleDocument(1645L, "Hello record layer", 1));
            /*
             * Note(scottfines): This test is changed at this commit to reflect the fact that Lucene's
             * highlighting structure highlights the full matched term, not just the prefix elements
             */
            assertRecordHighlights(List.of("Hello {record} layer"),
                    recordStore.fetchIndexRecords(
                            recordStore.scanIndex(SIMPLE_TEXT_SUFFIXES, fullTextSearch(SIMPLE_TEXT_SUFFIXES, "recor*"), null, ScanProperties.FORWARD_SCAN),
                            IndexOrphanBehavior.ERROR));
        }
    }

    @Test
    void highlightedBitsetQuery() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, LuceneIndexTestUtils.TEXT_AND_STORED);
            recordStore.saveRecord(LuceneIndexTestUtils.createSimpleDocument(1623L, "Hello record layer", 1));
            assertRecordHighlights(List.of("Hello {record} layer"),
                    recordStore.fetchIndexRecords(
                            recordStore.scanIndex(TEXT_AND_STORED, fullTextSearch(TEXT_AND_STORED, "text: record AND group: BITSET_CONTAINS(1)"), null, ScanProperties.FORWARD_SCAN),
                            IndexOrphanBehavior.ERROR));
            assertRecordHighlights(Collections.emptyList(),
                    recordStore.fetchIndexRecords(
                            recordStore.scanIndex(TEXT_AND_STORED, fullTextSearch(TEXT_AND_STORED, "group: BITSET_CONTAINS(1)"), null, ScanProperties.FORWARD_SCAN),
                            IndexOrphanBehavior.ERROR));
        }
    }

    @Test
    void highlightedNumberRangeQuery() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, TEXT_AND_STORED_COMPLEX);
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1623L, "Hello record layer", "Hello record layer 2", 5, 12, false, 8.123));
            assertRecordHighlights(List.of("Hello {record} layer"),
                    recordStore.fetchIndexRecords(
                            recordStore.scanIndex(TEXT_AND_STORED_COMPLEX, fullTextSearch(TEXT_AND_STORED_COMPLEX, "text: record AND group: 5"), null, ScanProperties.FORWARD_SCAN),
                            IndexOrphanBehavior.ERROR));
            assertRecordHighlights(List.of("Hello {record} layer"),
                    recordStore.fetchIndexRecords(
                            recordStore.scanIndex(TEXT_AND_STORED_COMPLEX, fullTextSearch(TEXT_AND_STORED_COMPLEX, "text: record AND group: [4 TO 6]"), null, ScanProperties.FORWARD_SCAN),
                            IndexOrphanBehavior.ERROR));
            assertRecordHighlights(List.of("Hello {record} layer"),
                    recordStore.fetchIndexRecords(
                            recordStore.scanIndex(TEXT_AND_STORED_COMPLEX, fullTextSearch(TEXT_AND_STORED_COMPLEX, "text: record AND score: [10 TO 15]"), null, ScanProperties.FORWARD_SCAN),
                            IndexOrphanBehavior.ERROR));
            assertRecordHighlights(List.of("Hello {record} layer"),
                    recordStore.fetchIndexRecords(
                            recordStore.scanIndex(TEXT_AND_STORED_COMPLEX, fullTextSearch(TEXT_AND_STORED_COMPLEX, "text: record AND time: [4.913442 TO 8.14234]"), null, ScanProperties.FORWARD_SCAN),
                            IndexOrphanBehavior.ERROR));
            assertRecordHighlights(Collections.emptyList(),
                    recordStore.fetchIndexRecords(
                            recordStore.scanIndex(TEXT_AND_STORED_COMPLEX, fullTextSearch(TEXT_AND_STORED_COMPLEX, "group: 5"), null, ScanProperties.FORWARD_SCAN),
                            IndexOrphanBehavior.ERROR));
            assertRecordHighlights(Collections.emptyList(),
                    recordStore.fetchIndexRecords(
                            recordStore.scanIndex(TEXT_AND_STORED_COMPLEX, fullTextSearch(TEXT_AND_STORED_COMPLEX, "score: [10 TO 15]"), null, ScanProperties.FORWARD_SCAN),
                            IndexOrphanBehavior.ERROR));
            assertRecordHighlights(Collections.emptyList(),
                    recordStore.fetchIndexRecords(
                            recordStore.scanIndex(TEXT_AND_STORED_COMPLEX, fullTextSearch(TEXT_AND_STORED_COMPLEX, "time: [4.913442 TO 8.14234]"), null, ScanProperties.FORWARD_SCAN),
                            IndexOrphanBehavior.ERROR));
        }
    }

    @Test
    void highlightedBooleanRangeQuery() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, LuceneIndexTestUtils.TEXT_AND_STORED_COMPLEX);
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1623L, "Hello record layer", "Hello record layer 2", 5, false));
            assertRecordHighlights(List.of("Hello {record} layer"),
                    recordStore.fetchIndexRecords(
                            recordStore.scanIndex(TEXT_AND_STORED_COMPLEX, fullTextSearch(TEXT_AND_STORED_COMPLEX, "text: record AND is_seen: [false TO true]"), null, ScanProperties.FORWARD_SCAN),
                            IndexOrphanBehavior.ERROR));
            assertRecordHighlights(Collections.emptyList(),
                    recordStore.fetchIndexRecords(
                            recordStore.scanIndex(TEXT_AND_STORED_COMPLEX, fullTextSearch(TEXT_AND_STORED_COMPLEX, "text: record AND is_seen: [true TO true]"), null, ScanProperties.FORWARD_SCAN),
                            IndexOrphanBehavior.ERROR));
            assertRecordHighlights(Collections.emptyList(),
                    recordStore.fetchIndexRecords(
                            recordStore.scanIndex(TEXT_AND_STORED_COMPLEX, fullTextSearch(TEXT_AND_STORED_COMPLEX, "is_seen: [false TO true]"), null, ScanProperties.FORWARD_SCAN),
                            IndexOrphanBehavior.ERROR));
        }
    }

    @Test
    void highlightedTermRangeQuery() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, LuceneIndexTestUtils.TEXT_AND_STORED_COMPLEX);
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1623L, "Hello record layer", "Hello record layer 2", 5, false));
            assertThrows(RecordCoreException.class, () -> recordStore.fetchIndexRecords(
                            recordStore.scanIndex(TEXT_AND_STORED_COMPLEX, fullTextSearch(TEXT_AND_STORED_COMPLEX, "text: record AND text2: [hello TO hello]"), null, ScanProperties.FORWARD_SCAN),
                            IndexOrphanBehavior.ERROR));
        }
    }

    @Test
    void highlightedTermQuery() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, COMPLEX_DOC, LuceneIndexTestUtils.TEXT_AND_STORED_COMPLEX);
            recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1623L, "Hello record layer", "inbox", 5, false));
            assertRecordHighlights(List.of("Hello {record} layer"),
                    recordStore.fetchIndexRecords(
                            recordStore.scanIndex(TEXT_AND_STORED_COMPLEX, fullTextSearch(TEXT_AND_STORED_COMPLEX, "text: record AND text2: \"inbox\""), null, ScanProperties.FORWARD_SCAN),
                            IndexOrphanBehavior.ERROR));
            assertRecordHighlights(Collections.emptyList(),
                    recordStore.fetchIndexRecords(
                            recordStore.scanIndex(TEXT_AND_STORED_COMPLEX, fullTextSearch(TEXT_AND_STORED_COMPLEX, "text2: \"inbox\""), null, ScanProperties.FORWARD_SCAN),
                            IndexOrphanBehavior.ERROR));
        }
    }

    @Test
    void highlightedSynonymIndex() {
        final String original = "peanut butter and jelly sandwich";
        final String highlighted = "{peanut} butter and jelly sandwich";
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, QUERY_ONLY_SYNONYM_LUCENE_INDEX);
            recordStore.saveRecord(LuceneIndexTestUtils.createSimpleDocument(1236L, original, 1));
            // Search for original token
            assertRecordHighlights(List.of(highlighted),
                    recordStore.fetchIndexRecords(
                            recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "peanut"), null, ScanProperties.FORWARD_SCAN),
                            IndexOrphanBehavior.ERROR));
            // Search for synonym word
            assertRecordHighlights(List.of(highlighted),
                    recordStore.fetchIndexRecords(
                            recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "groundnut"), null, ScanProperties.FORWARD_SCAN),
                            IndexOrphanBehavior.ERROR));
            // Search for synonym phrase
            assertRecordHighlights(List.of(highlighted),
                    recordStore.fetchIndexRecords(
                            recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "\"monkey nut\""), null, ScanProperties.FORWARD_SCAN),
                            IndexOrphanBehavior.ERROR));
        }
    }


    @Test
    void highlightedNgramIndex() {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, NGRAM_LUCENE_INDEX);
            recordStore.saveRecord(LuceneIndexTestUtils.createSimpleDocument(1623L, "Hello record layer", 1));
            assertRecordHighlights(List.of("{Hello} record layer"),
                    recordStore.fetchIndexRecords(
                            recordStore.scanIndex(NGRAM_LUCENE_INDEX, fullTextSearch(NGRAM_LUCENE_INDEX, "hello"), null, ScanProperties.FORWARD_SCAN),
                            IndexOrphanBehavior.ERROR));
            assertRecordHighlights(List.of("{Hello} record layer"),
                    recordStore.fetchIndexRecords(
                            recordStore.scanIndex(NGRAM_LUCENE_INDEX, fullTextSearch(NGRAM_LUCENE_INDEX, "hel"), null, ScanProperties.FORWARD_SCAN),
                            IndexOrphanBehavior.ERROR));
            assertRecordHighlights(List.of("Hello {record} layer"),
                    recordStore.fetchIndexRecords(
                            recordStore.scanIndex(NGRAM_LUCENE_INDEX, fullTextSearch(NGRAM_LUCENE_INDEX, "cord"), null, ScanProperties.FORWARD_SCAN),
                            IndexOrphanBehavior.ERROR));
        }
    }

    @Test
    void highlightingWithSnippets() {
        try (FDBRecordContext context = openContext()) {
            String text = "Good Morning From Apple News It?s Monday, July 11. Here?s what you need to know. " +
                          "Top Stories Former Trump adviser Steve Bannon agreed to testify to the January 6 committee " +
                          "after months of defying a congressional subpoena. The Washington Post https://apple.news/AgZ7a_IT4TpKFF3kAyZsvUg?";
            rebuildIndexMetaData(context, SIMPLE_DOC, QUERY_ONLY_SYNONYM_LUCENE_INDEX);
            recordStore.saveRecord(LuceneIndexTestUtils.createSimpleDocument(1241L, text, 1));

            final List<HighlightedTerm> highlightedTerms = recordStore.fetchIndexRecords(
                            recordStore.scanIndex(QUERY_ONLY_SYNONYM_LUCENE_INDEX, fullTextSearch(QUERY_ONLY_SYNONYM_LUCENE_INDEX, "appl*", 3), null, ScanProperties.FORWARD_SCAN),
                            IndexOrphanBehavior.ERROR
                    ).map(rec -> LuceneHighlighting.highlightedTermsForMessage(FDBQueriedRecord.indexed(rec), null))
                    .asStream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());

            List<HighlightedTerm> expectedResults = List.of(new HighlightedTerm("text", "Good Morning From Apple News It?s...", new int[] {18}, new int[] {23}));
            assertEquals(expectedResults.size(), highlightedTerms.size(), "Incorrect number of snippets returned!");
            Iterator<HighlightedTerm> expected = expectedResults.iterator();
            Iterator<HighlightedTerm> actual = highlightedTerms.iterator();
            while (expected.hasNext()) {
                assertTrue(actual.hasNext());
                HighlightedTerm expTerm = expected.next();
                HighlightedTerm actTerm = actual.next();
                assertEquals(expTerm.getFieldName(), actTerm.getFieldName(), "Incorrect field name!");
                assertEquals(expTerm.getSummarizedText(), actTerm.getSummarizedText(), "Incorrect snippet");

                assertEquals(expTerm.getNumHighlights(), actTerm.getNumHighlights(), "Incorrect positions for snippet " + expTerm.getSummarizedText());
                for (int i = 0; i < expTerm.getNumHighlights(); i++) {
                    assertEquals(expTerm.getHighlightStart(i), actTerm.getHighlightStart(i), "Incorrect position for snippet <" + expTerm.getSummarizedText() + ">");
                }
            }
        }
    }

    @Test
    void highlightPositionsCorrectWhenPlanned() throws Exception {
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, SIMPLE_TEXT_SUFFIXES);
            final String text = "record record record record record record " +
                                "layer " +
                                "record record record record record record record record record record " +
                                "layer " +
                                "record record " +
                                "layer " +
                                "record record record record record record record record";
            TestRecordsTextProto.SimpleDocument simpleDocument = TestRecordsTextProto.SimpleDocument.newBuilder().setDocId(0).setGroup(0).setText(text).build();
            recordStore.saveRecord(simpleDocument);

            final QueryComponent filter = new LuceneQueryComponent(LuceneQueryType.QUERY_HIGHLIGHT,
                    "layer", false, Lists.newArrayList(), true,
                    new LuceneScanQueryParameters.LuceneQueryHighlightParameters(4, 10), null);
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(SIMPLE_DOC)
                    .setFilter(filter)
                    .build();

            RecordQueryPlan plan = planner.plan(query);

            try (RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan)) {
                List<FDBQueriedRecord<Message>> queriedRecordList = cursor.asList().get();
                assertEquals(1, queriedRecordList.size());
                FDBQueriedRecord<Message> queriedRecord = queriedRecordList.get(0);

                List<HighlightedTerm> highlightedTerms = LuceneHighlighting.highlightedTermsForMessage(queriedRecord, null);
                assertEquals(1, highlightedTerms.size());
                HighlightedTerm highlightedTerm = highlightedTerms.get(0);
                assertEquals("text", highlightedTerm.getFieldName());
                assertEquals("...record record record record layer record record record record...record record record record layer record record layer record record record record...", highlightedTerm.getSummarizedText());
                assertEquals(3, highlightedTerm.getNumHighlights());
                for (int i = 0; i < highlightedTerm.getNumHighlights(); i++) {
                    assertEquals("layer", highlightedTerm.getSummarizedText().substring(highlightedTerm.getHighlightStart(i), highlightedTerm.getHighlightEnd(i)));
                }
            }
        }
    }

    @Test
    void synonymHighlightsWorksWhenPlanned() throws Exception {
        final String text = "record record record record record record " +
                            "layer " +
                            "record record record record record record record record record record " +
                            "stratum " +
                            "record record " +
                            "layer " +
                            "record record record record record record record record";
        try (FDBRecordContext context = openContext()) {
            rebuildIndexMetaData(context, SIMPLE_DOC, QUERY_ONLY_SYNONYM_LUCENE_INDEX);

            TestRecordsTextProto.SimpleDocument document = TestRecordsTextProto.SimpleDocument.newBuilder()
                    .setDocId(1L)
                    .setGroup(1)
                    .setText(text)
                    .build();
            recordStore.saveRecord(document);


            final QueryComponent filter = new LuceneQueryComponent(LuceneQueryType.QUERY_HIGHLIGHT,
                    "layer", false, Lists.newArrayList(), true,
                    new LuceneScanQueryParameters.LuceneQueryHighlightParameters(2, 10), null);
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType(TextIndexTestUtils.SIMPLE_DOC)
                    .setFilter(filter)
                    .build();

            RecordQueryPlan plan = planner.plan(query);

            try (final RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(plan)) {
                List<FDBQueriedRecord<Message>> queriedRecordList = cursor.asList().get();
                assertEquals(1, queriedRecordList.size());
                FDBQueriedRecord<Message> queriedRecord = queriedRecordList.get(0);

                List<HighlightedTerm> highlightedTerms = LuceneHighlighting.highlightedTermsForMessage(queriedRecord, null);
                assertEquals(1, highlightedTerms.size());
                HighlightedTerm highlightedTerm = highlightedTerms.get(0);
                assertEquals("text", highlightedTerm.getFieldName());
                assertEquals("...record record layer record record...record record stratum record record layer record record...", highlightedTerm.getSummarizedText());

                assertEquals(3, highlightedTerm.getNumHighlights());
                assertEquals("layer", highlightedTerm.getSummarizedText().substring(highlightedTerm.getHighlightStart(0), highlightedTerm.getHighlightEnd(0)));
                assertEquals("stratum", highlightedTerm.getSummarizedText().substring(highlightedTerm.getHighlightStart(1), highlightedTerm.getHighlightEnd(1)));
                assertEquals("layer", highlightedTerm.getSummarizedText().substring(highlightedTerm.getHighlightStart(2), highlightedTerm.getHighlightEnd(2)));
            }
        }
    }

    /* ***************************************************************************************************/
    /*private helper methods*/


    private LuceneScanBounds fullTextSearch(Index index, String search) {
        return LuceneIndexTestUtils.fullTextSearch(recordStore, index, search, true);
    }

    @SuppressWarnings("SameParameterValue") //deliberately placed here to make it easier to add new tests
    private LuceneScanBounds fullTextSearch(Index index, String search, int snippetSize) {
        return LuceneIndexTestUtils.fullTextSearch(recordStore, index, search, true, snippetSize);
    }

    @SuppressWarnings("SameParameterValue") //deliberately placed here to make it easier to add new tests
    private void rebuildIndexMetaData(final FDBRecordContext context, final String document, final Index index) {
        Pair<FDBRecordStore, QueryPlanner> pair = LuceneIndexTestUtils.rebuildIndexMetaData(context, path, document, index, isUseCascadesPlanner());
        this.recordStore = pair.getLeft();
        this.planner = pair.getRight();
    }

    private void assertRecordHighlights(List<String> texts, RecordCursor<FDBIndexedRecord<Message>> cursor) {
        List<String> highlighted = new ArrayList<>();
        cursor.forEach(rec -> {
            for (HighlightedTerm highlightedTerm : LuceneHighlighting.highlightedTermsForMessage(FDBQueriedRecord.indexed(rec), null)) {
                final StringBuilder str = new StringBuilder(highlightedTerm.getSummarizedText());
                int offset = 0;
                for (int p = 0; p < highlightedTerm.getNumHighlights(); p++) {
                    int start = highlightedTerm.getHighlightStart(p);
                    int end = highlightedTerm.getHighlightEnd(p);
                    str.insert(offset + start, "{");
                    offset++;
                    str.insert(offset + end, "}");
                    offset++;
                }
                highlighted.add(str.toString());
            }
        }).join();
        assertEquals(texts, highlighted);
    }
}
