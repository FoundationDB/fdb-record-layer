/*
 * LuceneOptimizedDocValuesFormatTest.java
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

package com.apple.foundationdb.record.lucene.codec;


import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.BaseDocValuesFormatTestCase;
import org.apache.lucene.index.BaseIndexFileFormatTestCaseUtils;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TestRuleLimitSysouts;
import org.apache.lucene.util.TestUtil;
import org.junit.Ignore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;

import static org.apache.lucene.index.SortedSetDocValues.NO_MORE_ORDS;

/**
 * Test that gets the actual test cases from {@link BaseDocValuesFormatTestCase}.
 */
// Tip: if you see a failure that has something like:
//  at __randomizedtesting.SeedInfo.seed([C185081D42F0F43C]:0)
// or
//  at __randomizedtesting.SeedInfo.seed([C185081D42F0F43C:33261A5D888FEB6A]:0)
// You can add
// @Seed("C185081D42F0F43C")
// to rerun the test class with the same seed. That will work even if you then only run one of the tests
@ThreadLeakFilters(defaultFilters = true, filters = {
        FDBThreadFilter.class
})
@TestRuleLimitSysouts.Limit(bytes = 85_000L) // 85k assuming debug logging
// sonarcloud doesn't seem to be able to detect the junit4 style of just having the method start with "test"
@SuppressWarnings("java:S2187")
public class LuceneOptimizedDocValuesFormatTest extends BaseDocValuesFormatTestCase {

    @Override
    protected Codec getCodec() {
        return BaseIndexFileFormatTestCaseUtils.getCodec();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        BaseIndexFileFormatTestCaseUtils.resetStaticConfigs();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        BaseIndexFileFormatTestCaseUtils.resetStaticConfigs();
    }

    @Override
    public void testMultiClose() throws IOException {
        BaseIndexFileFormatTestCaseUtils.testMultiClose(this);
    }

    @Override
    public void testMissingSortedBytes() throws IOException {
        // The seed below causes LuceneTestCase.wrapDirectory to replace the desired directory with an
        // NRTCachingDirectory, which doesn't work because our codec only works with FDBDirectory (specifically,
        // LuceneOptimizedFieldInfosFormat expects directory.createOutput(filename).close() to create the file reference
        // so that it can add info to the file reference
        // It would be great if we could change LuceneTestCase to never do that, but wrapDirectory is a static/final
        // method, and it's called in a whole stack of static methods
        // by calling nextInt we change it so that check doesn't fire, and it continues with the desired directory
        if (RandomizedContext.current().getRunnerSeedAsString().equals("C185081D42F0F43C")) {
            random().nextInt();
        }
        super.testMissingSortedBytes();
    }

    /**
     * This is a direct copy of the
     * {@link BaseDocValuesFormatTestCase#doTestSortedSetVsStoredFields(int, int, int, int, int)}, except in that
     * version it always creates a FSDirectory, whereas here, if testing against FDB we use the directory under test.
     * <p>
     *     You can search for "--BEGIN CUSTOM--" and "--END CUSTOM--" in the code to see exactly which lines.
     *     The code was also reformatted, and two instances of {@code String stringValues[]} were changed to
     *     {@code String[] stringValues}.
     * </p>
     *
     * @throws IOException if there's issues
     */
    @Override
    protected void doTestSortedSetVsStoredFields(final int numDocs, final int minLength, final int maxLength,
                                                 final int maxValuesPerDoc, final int maxUniqueValues) throws Exception {
        // --BEGIN CUSTOM--
        Directory dir;
        if (BaseIndexFileFormatTestCaseUtils.isUsingFDBDirectory()) {
            dir = newDirectory();
        } else {
            dir = newFSDirectory(createTempDir("dvduel"));
        }
        // --END CUSTOM--
        IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, conf);

        Set<String> valueSet = new HashSet<String>();
        for (int i = 0; i < 10000 && valueSet.size() < maxUniqueValues; ++i) {
            final int length = TestUtil.nextInt(random(), minLength, maxLength);
            valueSet.add(TestUtil.randomSimpleString(random(), length));
        }
        String[] uniqueValues = valueSet.toArray(new String[0]);

        // index some docs
        if (VERBOSE) {
            System.out.println("\nTEST: now add numDocs=" + numDocs);
        }
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            Field idField = new StringField("id", Integer.toString(i), Field.Store.NO);
            doc.add(idField);
            int numValues = TestUtil.nextInt(random(), 0, maxValuesPerDoc);
            // create a random set of strings
            Set<String> values = new TreeSet<>();
            for (int v = 0; v < numValues; v++) {
                values.add(RandomPicks.randomFrom(random(), uniqueValues));
            }

            // add ordered to the stored field
            for (String v : values) {
                doc.add(new StoredField("stored", v));
            }

            // add in any order to the dv field
            ArrayList<String> unordered = new ArrayList<>(values);
            Collections.shuffle(unordered, random());
            for (String v : unordered) {
                doc.add(new SortedSetDocValuesField("dv", newBytesRef(v)));
            }

            writer.addDocument(doc);
            if (random().nextInt(31) == 0) {
                writer.commit();
            }
        }

        // delete some docs
        int numDeletions = random().nextInt(numDocs / 10);
        if (VERBOSE) {
            System.out.println("\nTEST: now delete " + numDeletions + " docs");
        }
        for (int i = 0; i < numDeletions; i++) {
            int id = random().nextInt(numDocs);
            writer.deleteDocuments(new Term("id", Integer.toString(id)));
        }

        // compare
        if (VERBOSE) {
            System.out.println("\nTEST: now get reader");
        }
        DirectoryReader ir = writer.getReader();
        TestUtil.checkReader(ir);
        for (LeafReaderContext context : ir.leaves()) {
            LeafReader r = context.reader();
            SortedSetDocValues docValues = r.getSortedSetDocValues("dv");
            for (int i = 0; i < r.maxDoc(); i++) {
                String[] stringValues = r.document(i).getValues("stored");
                if (docValues != null) {
                    if (docValues.docID() < i) {
                        docValues.nextDoc();
                    }
                }
                if (docValues != null && stringValues.length > 0) {
                    assertEquals(i, docValues.docID());
                    for (int j = 0; j < stringValues.length; j++) {
                        assert docValues != null;
                        long ord = docValues.nextOrd();
                        assert ord != NO_MORE_ORDS;
                        BytesRef scratch = docValues.lookupOrd(ord);
                        assertEquals(stringValues[j], scratch.utf8ToString());
                    }
                    assertEquals(NO_MORE_ORDS, docValues.nextOrd());
                }
            }
        }
        if (VERBOSE) {
            System.out.println("\nTEST: now close reader");
        }
        ir.close();
        if (VERBOSE) {
            System.out.println("TEST: force merge");
        }
        writer.forceMerge(1);

        // compare again
        ir = writer.getReader();
        TestUtil.checkReader(ir);
        for (LeafReaderContext context : ir.leaves()) {
            LeafReader r = context.reader();
            SortedSetDocValues docValues = r.getSortedSetDocValues("dv");
            for (int i = 0; i < r.maxDoc(); i++) {
                String[] stringValues = r.document(i).getValues("stored");
                if (docValues.docID() < i) {
                    docValues.nextDoc();
                }
                if (docValues != null && stringValues.length > 0) {
                    assertEquals(i, docValues.docID());
                    for (int j = 0; j < stringValues.length; j++) {
                        assert docValues != null;
                        long ord = docValues.nextOrd();
                        assert ord != NO_MORE_ORDS;
                        BytesRef scratch = docValues.lookupOrd(ord);
                        assertEquals(stringValues[j], scratch.utf8ToString());
                    }
                    assertEquals(NO_MORE_ORDS, docValues.nextOrd());
                }
            }
        }
        if (VERBOSE) {
            System.out.println("TEST: close reader");
        }
        ir.close();
        if (VERBOSE) {
            System.out.println("TEST: close writer");
        }
        writer.close();
        if (VERBOSE) {
            System.out.println("TEST: close dir");
        }
        dir.close();
    }

    /**
     * This is a direct copy of the
     * {@link BaseDocValuesFormatTestCase#doTestSortedVsStoredFields(int, double, Supplier)},
     * except in that version it always creates a FSDirectory, whereas here, if testing against FDB we use the
     * directory
     * under test.
     * <p>
     * You can search for "--BEGIN CUSTOM--" and "--END CUSTOM--" in the code to see exactly which lines.
     * </p>
     *
     * @throws IOException if there's issues
     */
    @Override
    protected void doTestSortedVsStoredFields(final int numDocs, final double density, final Supplier<byte[]> bytes) throws Exception {
        // --BEGIN CUSTOM--
        Directory dir;
        if (BaseIndexFileFormatTestCaseUtils.isUsingFDBDirectory()) {
            dir = newDirectory();
        } else {
            dir = newFSDirectory(createTempDir("dvduel"));
        }
        // --END CUSTOM--
        IndexWriterConfig conf = newIndexWriterConfig(new MockAnalyzer(random()));
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, conf);
        Document doc = new Document();
        Field idField = new StringField("id", "", Field.Store.NO);
        Field storedField = new StoredField("stored", new byte[0]);
        Field dvField = new SortedDocValuesField("dv", newBytesRef());
        doc.add(idField);
        doc.add(storedField);
        doc.add(dvField);

        // index some docs
        for (int i = 0; i < numDocs; i++) {
            if (random().nextDouble() > density) {
                writer.addDocument(new Document());
                continue;
            }
            idField.setStringValue(Integer.toString(i));
            byte[] buffer = bytes.get();
            storedField.setBytesValue(buffer);
            dvField.setBytesValue(buffer);
            writer.addDocument(doc);
            if (random().nextInt(31) == 0) {
                writer.commit();
            }
        }

        // delete some docs
        int numDeletions = random().nextInt(numDocs / 10);
        for (int i = 0; i < numDeletions; i++) {
            int id = random().nextInt(numDocs);
            writer.deleteDocuments(new Term("id", Integer.toString(id)));
        }

        // compare
        DirectoryReader ir = writer.getReader();
        TestUtil.checkReader(ir);
        for (LeafReaderContext context : ir.leaves()) {
            LeafReader r = context.reader();
            BinaryDocValues docValues = DocValues.getBinary(r, "dv");
            docValues.nextDoc();
            for (int i = 0; i < r.maxDoc(); i++) {
                BytesRef binaryValue = r.document(i).getBinaryValue("stored");
                if (binaryValue == null) {
                    assertTrue(docValues.docID() > i);
                } else {
                    assertEquals(i, docValues.docID());
                    assertEquals(binaryValue, docValues.binaryValue());
                    docValues.nextDoc();
                }
            }
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, docValues.docID());
        }
        ir.close();
        writer.forceMerge(1);

        // compare again
        ir = writer.getReader();
        TestUtil.checkReader(ir);
        for (LeafReaderContext context : ir.leaves()) {
            LeafReader r = context.reader();
            BinaryDocValues docValues = DocValues.getBinary(r, "dv");
            docValues.nextDoc();
            for (int i = 0; i < r.maxDoc(); i++) {
                BytesRef binaryValue = r.document(i).getBinaryValue("stored");
                if (binaryValue == null) {
                    assertTrue(docValues.docID() > i);
                } else {
                    assertEquals(i, docValues.docID());
                    assertEquals(binaryValue, docValues.binaryValue());
                    docValues.nextDoc();
                }
            }
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, docValues.docID());
        }
        ir.close();
        writer.close();
        dir.close();
    }

    @Override
    @Ignore // Issue #2598: Make Lucene @Nightly tests pass for fixed seed
    @Nightly
    public void testRamBytesUsed() throws IOException {
        super.testRamBytesUsed();
    }

    @Override
    @Ignore // Issue #2598: Make Lucene @Nightly tests pass for fixed seed
    @Nightly
    public void testThreads3() throws Exception {
        super.testThreads3();
    }
}
