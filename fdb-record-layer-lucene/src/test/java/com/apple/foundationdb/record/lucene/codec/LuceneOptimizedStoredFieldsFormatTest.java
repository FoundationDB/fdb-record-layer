/*
 * LuceneOptimizedStoredFieldsFormatTest.java
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

import com.apple.foundationdb.record.test.FDBDatabaseExtension;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.compressing.CompressingCodec;
import org.apache.lucene.index.BaseIndexFileFormatTestCaseUtils;
import org.apache.lucene.index.BaseStoredFieldsFormatTestCase;
import org.apache.lucene.util.TestRuleLimitSysouts;
import org.junit.Before;
import org.junit.Ignore;

import java.io.IOException;
import java.util.Random;

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
public class LuceneOptimizedStoredFieldsFormatTest extends BaseStoredFieldsFormatTestCase {
    private static final FDBDatabaseExtension dbExtension = new FDBDatabaseExtension();

    public LuceneOptimizedStoredFieldsFormatTest() {
    }

    @Override
    protected Codec getCodec() {
        if (isUsingFDBDirectory()) {
            return new TestingCodec();
        } else {
            return CompressingCodec.randomInstance(new Random());
        }
    }

    private static boolean isUsingFDBDirectory() {
        return System.getProperty("tests.directory", "random").equals(TestFDBDirectory.class.getName());
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        TestingCodec.reset();
        // We have to manually call the extension call backs because this is a JUnit4 class
        dbExtension.beforeEach(null);
        TestFDBDirectory.reset();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        TestingCodec.reset();
        TestFDBDirectory.reset();
        dbExtension.afterEach(null);
    }

    @Override
    public void testNumericField() throws Exception {
        TestingCodec.disableLaziness();
        super.testNumericField();
    }

    @Override
    public void testRandomExceptions() throws Exception {
        // Failed due to UncheckedIOException with @Seed("6EA33D597F925691")
        TestingCodec.disableLazinessForLiveDocs();
        super.testRandomExceptions();
    }

    @Override
    @Ignore // Issue #2598: Make Lucene @Nightly tests pass for fixed seed
    @Nightly // copied from base implementation, it doesn't appear to be inherited
    public void testRamBytesUsed() throws IOException {
        TestingCodec.disableLaziness();
        TestFDBDirectory.useFullBufferToSurviveDeletes();
        super.testRamBytesUsed();
    }

    @Override
    @Ignore // Issue #2598: Make Lucene @Nightly tests pass for fixed seed
    @Nightly
    public void testBigDocuments() throws IOException {
        super.testBigDocuments();
    }

    @Override
    public void testMismatchedFields() throws Exception {
        TestFDBDirectory.allowAddIndexes();
        super.testMismatchedFields();
    }

    @Override
    public void testMultiClose() throws IOException {
        BaseIndexFileFormatTestCaseUtils.testMultiClose(this);
    }
}
