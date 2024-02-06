/*
 * LuceneOptimizedPointsFormatTest.java
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


import com.carrotsearch.randomizedtesting.annotations.Seed;
import com.carrotsearch.randomizedtesting.annotations.Seeds;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.BaseIndexFileFormatTestCaseUtils;
import org.apache.lucene.index.BasePointsFormatTestCase;
import org.apache.lucene.util.TestRuleLimitSysouts;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.io.IOException;

/**
 * Test that gets the actual test cases from {@link BasePointsFormatTestCase}.
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
public class LuceneOptimizedPointsFormatTest extends BasePointsFormatTestCase {

    @BeforeClass
    public static void beforeClass() {
        BaseIndexFileFormatTestCaseUtils.beforeClass();
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
    @Ignore("Always makes an FSDirectory, and copying the implementation would require copying a lot of private methods")
    public void testWithExceptions() throws Exception {
        super.testWithExceptions();
    }

    @Override
    public void testAddIndexes() throws IOException {
        TestFDBDirectory.allowAddIndexes();
        super.testAddIndexes();
    }

    @Override
    public void testAllEqual() throws Exception {
        TestFDBDirectory.allowAddIndexes();
        super.testAllEqual();
    }

    @Override
    public void testOneDimTwoValues() throws Exception {
        TestFDBDirectory.allowAddIndexes();
        super.testOneDimTwoValues();
    }

    @Override
    public void testOneDimEqual() throws Exception {
        TestFDBDirectory.allowAddIndexes();
        super.testOneDimEqual();
    }

    @Seeds({@Seed(), @Seed("FA63D2AE2DE7C6B0")})
    @Override
    public void testMultiValued() throws Exception {
        TestFDBDirectory.allowAddIndexes();
        super.testMultiValued();
    }

    @Override
    public void testRandomBinaryTiny() throws Exception {
        TestFDBDirectory.allowAddIndexes();
        super.testRandomBinaryTiny();
    }

    @Override
    public void testRandomBinaryMedium() throws Exception {
        TestFDBDirectory.allowAddIndexes();
        super.testRandomBinaryMedium();
    }

    @Override
    @Nightly
    public void testRandomBinaryBig() throws Exception {
        TestFDBDirectory.allowAddIndexes();
        super.testRandomBinaryBig();
    }
}
