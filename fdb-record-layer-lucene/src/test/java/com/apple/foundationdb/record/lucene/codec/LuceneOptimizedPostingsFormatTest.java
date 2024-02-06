/*
 * LuceneOptimizedPostingsFormatTest.java
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


import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.BaseIndexFileFormatTestCaseUtils;
import org.apache.lucene.index.BasePostingsFormatTestCase;
import org.apache.lucene.index.BaseStoredFieldsFormatTestCase;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.RandomPostingsTester;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestRuleLimitSysouts;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.EnumSet;

/**
 * Test for {@link LuceneOptimizedStoredFieldsFormat} that gets the actual test cases from {@link BaseStoredFieldsFormatTestCase}.
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
@TestRuleLimitSysouts.Limit(bytes = 50_000L) // 50k assuming debug logging
// sonarcloud doesn't seem to be able to detect the junit4 style of just having the method start with "test"
@SuppressWarnings("java:S2187")
public class LuceneOptimizedPostingsFormatTest extends BasePostingsFormatTestCase {

    private static LuceneOptimizedRandomPostingsTester fdbPostingsTester;

    @BeforeClass
    public static void beforeClass() {
        BaseIndexFileFormatTestCaseUtils.beforeClass();
    }

    @BeforeClass
    public static void createFdbPostingsTester() throws IOException {
        fdbPostingsTester = new LuceneOptimizedRandomPostingsTester(random());
    }

    @AfterClass
    public static void resetFdbPostingsTester() throws Exception {
        fdbPostingsTester = null;
    }

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
    public void testDocsOnly() throws Exception {
        // Use the fdb postings tester that creates an FDBDirectory when needed
        fdbPostingsTester.testFull(getCodec(), createTempDir("testPostingsFormat.testExact"), IndexOptions.DOCS, false);
    }

    @Override
    public void testDocsAndFreqs() throws Exception {
        // Use the fdb postings tester that creates an FDBDirectory when needed
        fdbPostingsTester.testFull(getCodec(), createTempDir("testPostingsFormat.testExact"), IndexOptions.DOCS_AND_FREQS, false);
    }

    @Override
    public void testDocsAndFreqsAndPositions() throws Exception {
        // Use the fdb postings tester that creates an FDBDirectory when needed
        fdbPostingsTester.testFull(getCodec(), createTempDir("testPostingsFormat.testExact"), IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, false);
    }

    @Override
    public void testDocsAndFreqsAndPositionsAndPayloads() throws Exception {
        // Use the fdb postings tester that creates an FDBDirectory when needed
        fdbPostingsTester.testFull(getCodec(), createTempDir("testPostingsFormat.testExact"), IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, true);
    }

    @Override
    public void testDocsAndFreqsAndPositionsAndOffsets() throws Exception {
        // Use the fdb postings tester that creates an FDBDirectory when needed
        fdbPostingsTester.testFull(getCodec(), createTempDir("testPostingsFormat.testExact"), IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, false);
    }

    @Override
    public void testDocsAndFreqsAndPositionsAndOffsetsAndPayloads() throws Exception {
        // Use the fdb postings tester that creates an FDBDirectory when needed
        fdbPostingsTester.testFull(getCodec(), createTempDir("testPostingsFormat.testExact"), IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, true);
    }

    /**
     * A copy of the superclass {@link BasePostingsFormatTestCase#testRandom()} that changes the directory used
     * See --BEGIN CUSTOM-- and --END CUSTOM-- comments below.
     * Also, since the PostingTester field is not available here, use the local one instead (fdbPostingsTester)
     * @throws Exception in case of error
     */
    @Override
    public void testRandom() throws Exception {
        int iters = 5;

        for (int iter = 0; iter < iters; ++iter) {
            // --BEGIN CUSTOM--
            MockDirectoryWrapper dir;
            if (BaseIndexFileFormatTestCaseUtils.isUsingFDBDirectory()) {
                dir = LuceneTestCase.newMockDirectory();
            } else {
                dir = LuceneTestCase.newMockFSDirectory(LuceneTestCase.createTempDir("CFSManySubFiles"));
            }
            // --END CUSTOM--
            boolean indexPayloads = random().nextBoolean();
            FieldsProducer fieldsProducer = fdbPostingsTester.buildIndex(this.getCodec(), dir, IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, indexPayloads, false);
            fdbPostingsTester.testFields(fieldsProducer);
            fdbPostingsTester.testTerms(fieldsProducer, EnumSet.allOf(RandomPostingsTester.Option.class), IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, false);
            fieldsProducer.close();
            fieldsProducer = null;
            dir.close();
        }
    }

    /**
     * This test cannot be reproduced as it accesses package-protected fields.
     * TODO: This can be changed with AccessManager
     */
    @Override
    public void testPostingsEnumReuse() throws Exception {
    }
}
