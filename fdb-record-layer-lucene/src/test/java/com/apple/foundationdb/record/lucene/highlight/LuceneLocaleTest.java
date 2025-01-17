/*
 * LuceneLocaleTest.java
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

package com.apple.foundationdb.record.lucene.highlight;

import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.lucene.LuceneIndexTestUtils;
import com.apple.foundationdb.record.lucene.synonym.EnglishSynonymMapConfig;
import com.apple.foundationdb.record.lucene.synonym.SynonymMapRegistryImpl;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.provider.foundationdb.FDBIndexedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.IndexOrphanBehavior;
import com.apple.foundationdb.record.query.plan.QueryPlanner;
import com.apple.foundationdb.record.util.pair.Pair;
import com.google.protobuf.Message;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static com.apple.foundationdb.record.lucene.LuceneIndexTestUtils.TEXT_AND_STORED_COMPLEX;
import static com.apple.foundationdb.record.provider.foundationdb.indexes.TextIndexTestUtils.COMPLEX_DOC;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests to make sure Lucene isn't using the default system locale.
 * <p>
 *     At some point, we may want to make the locale used for query parsing into an index option, or
 *     {@link com.apple.foundationdb.record.lucene.LuceneRecordContextProperties}, but for now it is just set to
 *     {@link Locale#ROOT}.
 * </p>
 */
public class LuceneLocaleTest extends FDBRecordStoreTestBase {

    @BeforeAll
    public static void setup() {
        //set up the English Synonym Map so that we don't spend forever setting it up for every test, because this takes a long time
        SynonymMapRegistryImpl.instance().getSynonymMap(EnglishSynonymMapConfig.ExpandedEnglishSynonymMapConfig.CONFIG_NAME);
    }

    @ParameterizedTest
    @ValueSource(strings = {"default", "en_US", "de", "de_DE", "fr"})
    void highlightedNumberRangeQuery(Locale locale) {
        final Locale defaultLocale = Locale.getDefault();
        try {
            Locale.setDefault(locale);
            try (FDBRecordContext context = openContext()) {
                rebuildIndexMetaData(context, COMPLEX_DOC, TEXT_AND_STORED_COMPLEX);
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1623L, "Hello record layer", "Hello record layer 2", 5, 1_234_097, false, 8.123));
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1623L, "Hello record layer", "Hello record layer 2", -110_000_000, 999_999, false, 4.134059823));
                recordStore.saveRecord(LuceneIndexTestUtils.createComplexDocument(1623L, "Hello record layer", "Hello record layer 2", 51_000, 2_000_000, false, 9.2));

                Assertions.assertAll(
                        () -> assertHighlightMatches("text: record AND group: [-12,340,984 TO 42,938]"),
                        () -> assertHighlightMatches("text: record AND score: [1,000,000 TO 1,500,000]"),
                        () -> assertHighlightMatches("text: record AND time: [4.913442 TO 8.14234]"),
                        () -> assertSearchMatches("text: record AND group: [-12,340,984 TO 42,938]"),
                        () -> assertSearchMatches("text: record AND score: [1,000,000 TO 1,500,000]"),
                        () -> assertSearchMatches("text: record AND time: [4.913442 TO 8.14234]"));
            }
        } finally {
            Locale.setDefault(defaultLocale);
        }
    }

    private void assertSearchMatches(String search) {
        Assertions.assertEquals(List.of(1623L),
                recordStore.fetchIndexRecords(
                        recordStore.scanIndex(TEXT_AND_STORED_COMPLEX, LuceneIndexTestUtils.fullTextSearch(recordStore, TEXT_AND_STORED_COMPLEX, search, false), null, ScanProperties.FORWARD_SCAN),
                        IndexOrphanBehavior.ERROR)
                        .map(r -> r.getPrimaryKey().get(1))
                        .asList().join());
    }

    private void assertHighlightMatches(final String search) {
        assertRecordHighlights(List.of("Hello {record} layer"),
                recordStore.fetchIndexRecords(
                        recordStore.scanIndex(TEXT_AND_STORED_COMPLEX, LuceneIndexTestUtils.fullTextSearch(recordStore, TEXT_AND_STORED_COMPLEX, search, true), null, ScanProperties.FORWARD_SCAN),
                        IndexOrphanBehavior.ERROR));
    }
    /* ***************************************************************************************************/
    /*private helper methods*/


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
