/*
 * FDBLuceneFunctionalityTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene.directory;

import com.apple.foundationdb.record.lucene.codec.LuceneOptimizedPostingsFormat;
import com.apple.test.Tags;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test for FDBDirectory validating it can function as a backing store
 * for Lucene.
 */
@Tag(Tags.RequiresFDB)
public class FDBLuceneFunctionalityTest extends FDBDirectoryBaseTest {
    private FDBLuceneTestIndex luceneIndex;

    @BeforeEach
    public void setUp() {
        super.setUp();
        luceneIndex = new FDBLuceneTestIndex(directory, new StandardAnalyzer());
    }

    @Test
    public void givenSearchQueryWhenFetchedDocumentThenCorrect() throws Exception {
        luceneIndex.indexDocument("Hello world", "Some hello world ");
        List<Document> documents = luceneIndex.searchIndex("body", "world");
        assertEquals("Hello world", documents.get(0).get("title"));
    }


    @Test
    public void givenTermQueryWhenFetchedDocumentThenCorrect() throws Exception {
        LuceneOptimizedPostingsFormat.setAllowCheckDataIntegrity(false);
        luceneIndex.indexDocument("activity", "running in track");
        luceneIndex.indexDocument("activity", "Cars are running on road");
        Term term = new Term("body", "running");
        Query query = new TermQuery(term);
        List<Document> documents = luceneIndex.searchIndex(query);
        assertEquals(2, documents.size());
    }

    @Test
    public void givenPrefixQueryWhenFetchedDocumentThenCorrect() throws Exception {
        luceneIndex.indexDocument("article", "Lucene introduction");
        luceneIndex.indexDocument("article", "Introduction to Lucene");
        Term term = new Term("body", "intro");
        Query query = new PrefixQuery(term);
        List<Document> documents = luceneIndex.searchIndex(query);
        assertEquals(2, documents.size());
    }

    @Test
    public void givenBooleanQueryWhenFetchedDocumentThenCorrect() throws Exception {
        luceneIndex.indexDocument("Destination", "Las Vegas singapore car");
        LuceneOptimizedPostingsFormat.setAllowCheckDataIntegrity(false);
        luceneIndex.indexDocument("Commutes in singapore", "Bus Car Bikes");
        Term term1 = new Term("body", "singapore");
        Term term2 = new Term("body", "car");
        TermQuery query1 = new TermQuery(term1);
        TermQuery query2 = new TermQuery(term2);
        BooleanQuery booleanQuery = new BooleanQuery.Builder().add(query1, BooleanClause.Occur.MUST)
                .add(query2, BooleanClause.Occur.MUST).build();
        List<Document> documents = luceneIndex.searchIndex(booleanQuery);
        assertEquals(1, documents.size());
    }

    @Test
    public void givenPhraseQueryWhenFetchedDocumentThenCorrect() throws Exception {
        luceneIndex.indexDocument("quotes", "A rose by any other name would smell as sweet.");
        Query query = new PhraseQuery(1, "body", new BytesRef("smell"), new BytesRef("sweet"));
        List<Document> documents = luceneIndex.searchIndex(query);
        assertEquals(1, documents.size());
    }

    @Test
    public void givenFuzzyQueryWhenFetchedDocumentThenCorrect() throws Exception {
        LuceneOptimizedPostingsFormat.setAllowCheckDataIntegrity(false);
        luceneIndex.indexDocument("article", "Halloween Festival");
        luceneIndex.indexDocument("decoration", "Decorations for Halloween");
        Term term = new Term("body", "hallowen");
        Query query = new FuzzyQuery(term);
        List<Document> documents = luceneIndex.searchIndex(query);
        assertEquals(2, documents.size());
    }

    @Test
    public void givenWildCardQueryWhenFetchedDocumentThenCorrect() throws Exception {
        luceneIndex.indexDocument("article", "Lucene introduction");
        luceneIndex.indexDocument("article", "Introducing Lucene with Spring");
        Term term = new Term("body", "intro*");
        Query query = new WildcardQuery(term);
        List<Document> documents = luceneIndex.searchIndex(query);
        assertEquals(2, documents.size());
    }

    @Test
    public void givenSortFieldWhenSortedThenCorrect() throws Exception {
        luceneIndex.indexDocument("Ganges", "River in India");
        luceneIndex.indexDocument("Mekong", "This river flows in south Asia");
        LuceneOptimizedPostingsFormat.setAllowCheckDataIntegrity(false);
        luceneIndex.indexDocument("Amazon", "Rain forest river");
        luceneIndex.indexDocument("Rhine", "Belongs to Europe");
        luceneIndex.indexDocument("Nile", "Longest River");

        Term term = new Term("body", "river");
        Query query = new WildcardQuery(term);

        SortField sortField = new SortField("title", SortField.Type.STRING_VAL, false);
        Sort sortByTitle = new Sort(sortField);

        List<Document> documents = luceneIndex.searchIndex(query, sortByTitle);
        assertEquals(4, documents.size());
        assertEquals("Amazon", documents.get(0).getField("title").stringValue());
    }

    @Test
    public void whenDocumentDeletedThenCorrect() throws IOException {
        LuceneOptimizedPostingsFormat.setAllowCheckDataIntegrity(false);
        luceneIndex.indexDocument("Ganges", "River in India");
        luceneIndex.indexDocument("Mekong", "This river flows in south Asia");
        Term term = new Term("title", "ganges");
        luceneIndex.deleteDocument(term);
        Query query = new TermQuery(term);
        assertEquals(0, luceneIndex.searchIndex(query).size());
        term = new Term("title", "mekong");
        query = new TermQuery(term);
        assertEquals(1, luceneIndex.searchIndex(query).size());
    }

}
