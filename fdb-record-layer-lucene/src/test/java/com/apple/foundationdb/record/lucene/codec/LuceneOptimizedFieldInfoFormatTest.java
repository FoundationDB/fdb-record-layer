/*
 * LuceneOptimizedFieldInfoFormatTest.java
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
import org.apache.lucene.index.BaseFieldInfoFormatTestCase;
import org.apache.lucene.index.BaseIndexFileFormatTestCaseUtils;
import org.apache.lucene.util.TestRuleLimitSysouts;
import org.junit.Ignore;

import java.io.IOException;

/**
 * Test that gets the actual test cases from {@link BaseFieldInfoFormatTestCase}.
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
public class LuceneOptimizedFieldInfoFormatTest extends BaseFieldInfoFormatTestCase {

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
    @Ignore("LuceneOptimizedFieldInfosFormat doesn't call openInput")
    public void testExceptionOnCloseInput() throws Exception {
        super.testExceptionOnCloseInput();
    }

    @Override
    @Ignore("LuceneOptimizedFieldInfosFormat doesn't call openInput")
    public void testExceptionOnOpenInput() throws Exception {
        super.testExceptionOnOpenInput();
    }

    @Override
    public void testRandom() throws Exception {
        // https://github.com/FoundationDB/fdb-record-layer/issues/3005
        // It may write a value that is too large, and will fail when trying to read it back out.
        // Note that because we don't commit, it won't fail on the write, it will fail on the read.
        TestFDBDirectory.disableFieldInfosCountCheck();
        super.testRandom();
    }
}
