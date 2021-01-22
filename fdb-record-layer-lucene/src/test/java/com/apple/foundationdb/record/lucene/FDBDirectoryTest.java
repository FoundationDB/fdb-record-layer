/*
 * FDBDirectoryTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2020 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.TestRecords1Proto;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreTestBase;
import com.apple.foundationdb.record.provider.foundationdb.TestKeySpace;
import com.apple.foundationdb.subspace.Subspace;
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
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test for FDBDirectory validating it can function as a backing store
 * for Lucene.
 */
@Tag(Tags.RequiresFDB)
public class FDBDirectoryTest extends FDBRecordStoreTestBase {
    private FDBRecordStore recordStore;
    private FDBDatabase fdb;
    private Subspace subspace;
    private FDBLuceneTestIndex luceneIndex;

    @BeforeEach
    public void setUp() {
        if (fdb == null) {
            fdb = FDBDatabaseFactory.instance().getDatabase();
        }
        if (subspace == null) {
            subspace = fdb.run(context -> TestKeySpace.getKeyspacePath("record-test", "unit", "indexTest", "version").toSubspace(context));
        }
        fdb.run(context -> {
            FDBRecordStore.deleteStore(context, subspace);
            return null;
        });
        RecordMetaDataBuilder metaDataBuilder = RecordMetaData.newBuilder().setRecords(TestRecords1Proto.getDescriptor());
        FDBRecordContext context = fdb.openContext();
        recordStore = FDBRecordStore.newBuilder()
                .setMetaDataProvider(metaDataBuilder)
                .setContext(context)
                .setSubspace(subspace)
                .setFormatVersion(FDBRecordStore.MAX_SUPPORTED_FORMAT_VERSION)
                .createOrOpen();
        luceneIndex = new FDBLuceneTestIndex(new FDBDirectory(subspace, recordStore.ensureContextActive()), new StandardAnalyzer());
    }

    @Test
    public void testDirectoryCreate() {
        FDBDirectory directory = new FDBDirectory(subspace, recordStore.ensureContextActive());
        assertNotNull(directory);
        assertEquals(subspace, directory.subspace);
        assertEquals(directory.txn, recordStore.ensureContextActive());
    }

    @Test
    public void testSetGetIncrement() {
        FDBDirectory directory = new FDBDirectory(subspace, recordStore.ensureContextActive());
        // If increment is null, get starts the increment: TODO: Why is it returning 2 the first time?
        assertEquals(1, directory.getIncrement());
        // subsequent gets return the next value
        assertEquals(2, directory.getIncrement());
    }

    @Test
    public void testWriteGetLuceneFileReference() {
        // TODO: Assert that at the start the reference cache and subspace is empty.

        // get unknown file reference.
        FDBDirectory directory = new FDBDirectory(subspace, recordStore.ensureContextActive());
        CompletableFuture<FDBLuceneFileReference> luceneFileReference = directory.getFDBLuceneFileReference("NonExist");
        try {
            assertNull(luceneFileReference.get(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            fail("Interrupted exception thrown when getting lucene reference: " + e.toString());
            e.printStackTrace();
        } catch (ExecutionException e) {
            fail("Exception thrown when getting lucene file reference: " + e.toString());
            e.printStackTrace();
        } catch (TimeoutException e) {
            fail("Timeout when getting lucene file reference: " + e.toString());
            e.printStackTrace();
        }

        // get known file reference
        String luceneReference1 = "luceneReference1";
        FDBLuceneFileReference fileReference = new FDBLuceneFileReference(1, 10, 10);
        directory.writeFDBLuceneFileReference(luceneReference1, fileReference);
        luceneFileReference = directory.getFDBLuceneFileReference(luceneReference1);
        try {
            FDBLuceneFileReference actual = luceneFileReference.get(5, TimeUnit.SECONDS);
            assertNotNull(actual);
            assertEquals(actual, fileReference);
        } catch (InterruptedException e) {
            fail("Interrupted exception thrown when getting lucene reference: " + e.toString());
            e.printStackTrace();
        } catch (ExecutionException e) {
            fail("Exception thrown when getting lucene file reference: " + e.toString());
            e.printStackTrace();
        } catch (TimeoutException e) {
            fail("Timeout when getting lucene file reference: " + e.toString());
            e.printStackTrace();
        }
    }

    @Test
    public void testWriteLuceneFileReference() {
        // write already created file reference
        FDBDirectory directory = new FDBDirectory(subspace, recordStore.ensureContextActive());
        FDBLuceneFileReference reference1 = new FDBLuceneFileReference(2, 1, 1);
        directory.writeFDBLuceneFileReference("test1", reference1);
        FDBLuceneFileReference reference2 = new FDBLuceneFileReference(3, 1, 1);
        directory.writeFDBLuceneFileReference("test1", reference2);

        CompletableFuture<FDBLuceneFileReference> luceneFileReference = directory.getFDBLuceneFileReference("test1");
        try {
            FDBLuceneFileReference actual = luceneFileReference.get(5, TimeUnit.SECONDS);
            assertNotNull(actual);
            // TODO: this overwrites the file reference under that name, if expected complete this test.
        } catch (InterruptedException e) {
            fail("Interrupted exception thrown when getting lucene reference: " + e.toString());
            e.printStackTrace();
        } catch (ExecutionException e) {
            fail("Exception thrown when getting lucene file reference: " + e.toString());
            e.printStackTrace();
        } catch (TimeoutException e) {
            fail("Timeout when getting lucene file reference: " + e.toString());
            e.printStackTrace();
        }
        // write invalid file reference
        // TODO: not sure how/if we can have invalid references.
    }


    @Test
    public void testWriteSeekData() {
        Transaction transaction = recordStore.ensureContextActive();

        FDBDirectory directory = new FDBDirectory(subspace, transaction);
        // seek data from non-existant lucene file.
        try {
            directory.seekData("testDescription", directory.getFDBLuceneFileReference("testReference"), 1);
            fail();
        } catch (IOException | NullPointerException e) {
            // TODO: should throw NPE?
            assertTrue(e instanceof NullPointerException, "This should throw NPE not IOException: " + e.toString());
        }

        // seek data from existent lucene file reference without data being written.
        FDBLuceneFileReference reference1 = new FDBLuceneFileReference(2, 1, 1);
        directory.writeFDBLuceneFileReference("testReference1", reference1);
        try {
            CompletableFuture<byte[]> seekData = directory.seekData("testReference1", directory.getFDBLuceneFileReference("testReference1"), 1);
            byte[] bytes = seekData.get(5, TimeUnit.SECONDS);
            assertNull(bytes);
        } catch (Exception e) {
            fail("Unexpected exception thrown: " + e.toString());
        }

        // data written before commit isn't written to record store yet.

        FDBLuceneFileReference reference2 = new FDBLuceneFileReference(2, 1, 200);
        directory.writeFDBLuceneFileReference("testReference2", reference2);
        byte[] bytes1 = "test string for write".getBytes(StandardCharsets.UTF_8);
        directory.writeData(2, 1, bytes1);

        try {
            CompletableFuture<byte[]> seekData = directory.seekData("testReference2", directory.getFDBLuceneFileReference("testReference1"), 1);
            byte[] bytes = seekData.get(5, TimeUnit.SECONDS);
            assertNull(bytes);
        } catch (Exception e) {
            fail("Unexpected exception thrown: " + e.toString());
        }

        // after commit data is seekable
        transaction.commit();
        FDBLuceneFileReference reference3 = new FDBLuceneFileReference(2, bytes1.length, bytes1.length);
        directory.writeFDBLuceneFileReference("testReference3", reference3);
        try {
            CompletableFuture<byte[]> seekData = directory.seekData("testReference3", directory.getFDBLuceneFileReference("testReference1"), 1);
            byte[] bytes = seekData.get(5, TimeUnit.SECONDS);
            //TODO figure out what this is missing to seek the data or commit it.
            assertNotNull(bytes);
        } catch (Exception e) {
            fail("Unexpected exception thrown: " + e.toString());
        }
    }

    @Test
    public void testListAll() {
        FDBDirectory directory = new FDBDirectory(subspace, recordStore.ensureContextActive());

        // test list all on emtpy directory returns empty list.
        assertEquals(directory.listAll().length, 0);

        // test list all adds elements to cache.
        // TODO: expose cache to do this

        // test list all returns all elements
        FDBLuceneFileReference reference1 = new FDBLuceneFileReference(1, 1, 1);
        directory.writeFDBLuceneFileReference("test1", reference1);
        FDBLuceneFileReference reference2 = new FDBLuceneFileReference(2, 1, 1);
        directory.writeFDBLuceneFileReference("test2", reference2);
        FDBLuceneFileReference reference3 = new FDBLuceneFileReference(3, 1, 1);
        directory.writeFDBLuceneFileReference("test3", reference3);
        String[] references = directory.listAll();
        String[] actual = {"test1", "test2", "test3"};
        assertTrue(references[0].equals(actual[0]) && references[1].equals(actual[1]) &&
                   references[2].equals(actual[2]));
    }

    @Test
    public void testDeleteData() {
        FDBDirectory directory = new FDBDirectory(subspace, recordStore.ensureContextActive());

        //test delete data on empty data
        try {
            directory.deleteFile("NonExist");
            wait(200);
            fail();
        } catch (Exception e) {
            assertTrue(e.getCause().getCause() instanceof NoSuchFileException);


        }

        // test delete data on existing data
        FDBLuceneFileReference reference1 = new FDBLuceneFileReference(1, 1, 1);
        directory.writeFDBLuceneFileReference("test1", reference1);
        directory.deleteFile("test1");
        assertTrue(directory.listAll().length == 0);
    }

    @Test
    public void testFileLength() {
        FDBDirectory directory = new FDBDirectory(subspace, recordStore.ensureContextActive());
        // get file length for non-existent file throws exception.
        try {
            directory.fileLength("nonExist");
            fail("NoSuchFileException should have been thrown for non-existent file.");
        } catch (NoSuchFileException e) { }

        // get file length for existing file.
        FDBLuceneFileReference reference1 = new FDBLuceneFileReference(1, 1, 1);
        directory.writeFDBLuceneFileReference("test1", reference1);
        try {
            long fileSize = directory.fileLength("test1");
            assertEquals(1, fileSize);
        } catch (NoSuchFileException e) {
            fail("Unexpected exception was thrown: " + e.toString());
        }
    }

    @Test
    public void testCreateOutput() {
        fail();
    }

    @Test
    public void testCreateTempOutput() {
        fail();
    }

    @Test
    public void testSync() {
        fail();
    }

    @Test
    public void testSyncMetadata() {

        fail();
    }

    @Test
    public void testRename() {
        FDBDirectory directory = new FDBDirectory(subspace, recordStore.ensureContextActive());
        try {
            directory.rename("NoExist", "newName");
        } catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
        }


        fail();
    }

    @Test
    public void testOpenInput() {
        fail();
    }

    @Test
    public void testObtainLock() {
        fail();
    }

    @Test
    public void testClose() {
        fail();
    }

    @Test
    public void testGetPendingDeletions() {
        fail();
        // Test getting when no pending deletions
        // test getting when deletions exist
    }

    @Test
    public void testGetBlockSize() {
        fail();
        // getting empty block.
        // getting existing block.
    }

    @Test
    public void givenSearchQueryWhenFetchedDocumentThenCorrect() throws Exception {
        luceneIndex.indexDocument("Hello world", "Some hello world ");
        List<Document> documents = luceneIndex.searchIndex("body", "world");
        assertEquals("Hello world", documents.get(0).get("title"));
    }


    @Test
    public void givenTermQueryWhenFetchedDocumentThenCorrect() throws Exception {
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
        luceneIndex.indexDocument("Ganges", "River in India");
        luceneIndex.indexDocument("Mekong", "This river flows in south Asia");
        Term term = new Term("title", "ganges");
        luceneIndex.deleteDocument(term);
        Query query = new TermQuery(term);
        List<Document> documents = luceneIndex.searchIndex(query);
        assertEquals(0, documents.size());
    }

}
