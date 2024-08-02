/*
 * LuceneOptimizedCompoundFormatTest.java
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
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.BaseCompoundFormatTestCase;
import org.apache.lucene.index.BaseFieldInfoFormatTestCase;
import org.apache.lucene.index.BaseIndexFileFormatTestCaseUtils;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.store.NRTCachingDirectory;
import org.apache.lucene.util.TestRuleLimitSysouts;
import org.junit.Ignore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
@Ignore
public class LuceneOptimizedCompoundFormatTest extends BaseCompoundFormatTestCase {

    @Override
    protected Codec getCodec() {
        return BaseIndexFileFormatTestCaseUtils.getCodec();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        BaseIndexFileFormatTestCaseUtils.resetStaticConfigs();
        TestingCodec.allowRandomCompoundFiles();
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

    /**
     * This is a direct copy of the {@link BaseCompoundFormatTestCase#testManySubFiles()}, except in that version it
     * always creates a FSDirectory, whereas here, if testing against FDB we use the directory under test.
     * <p>
     * You can search for "--BEGIN CUSTOM--" and "--END CUSTOM--" in the code to see exactly which lines.
     * </p>
     *
     * @throws IOException if there's issues
     */
    @Override
    public void testManySubFiles() throws IOException {
        // --BEGIN CUSTOM--
        MockDirectoryWrapper dir;
        if (BaseIndexFileFormatTestCaseUtils.isUsingFDBDirectory()) {
            dir = newMockDirectory();
        } else {
            dir = newMockFSDirectory(createTempDir("CFSManySubFiles"));
        }
        // fileCount was originally FILE_COUNT, but that goes against checkStyle
        final int fileCount = atLeast(500);
        // --END CUSTOM--

        List<String> files = new ArrayList<>();
        SegmentInfo si = newSegmentInfo(dir, "_123");
        for (int fileIdx = 0; fileIdx < fileCount; fileIdx++) {
            String file = "_123." + fileIdx;
            files.add(file);
            try (IndexOutput out = dir.createOutput(file, newIOContext(random()))) {
                CodecUtil.writeIndexHeader(out, "Foo", 0, si.getId(), "suffix");
                out.writeByte((byte)fileIdx);
                CodecUtil.writeFooter(out);
            }
        }

        assertEquals(0, dir.getFileHandleCount());

        si.setFiles(files);
        si.getCodec().compoundFormat().write(dir, si, IOContext.DEFAULT);
        Directory cfs = si.getCodec().compoundFormat().getCompoundReader(dir, si, IOContext.DEFAULT);

        final IndexInput[] ins = new IndexInput[fileCount];
        for (int fileIdx = 0; fileIdx < fileCount; fileIdx++) {
            ins[fileIdx] = cfs.openInput("_123." + fileIdx, newIOContext(random()));
            CodecUtil.checkIndexHeader(ins[fileIdx], "Foo", 0, 0, si.getId(), "suffix");
        }

        assertEquals(1, dir.getFileHandleCount());

        for (int fileIdx = 0; fileIdx < fileCount; fileIdx++) {
            assertEquals((byte)fileIdx, ins[fileIdx].readByte());
        }

        assertEquals(1, dir.getFileHandleCount());

        for (int fileIdx = 0; fileIdx < fileCount; fileIdx++) {
            ins[fileIdx].close();
        }
        cfs.close();

        dir.close();
    }

    /**
     * This is a direct copy of the {@link BaseCompoundFormatTestCase#testLargeCFS()}, except in that version it
     * always creates a FSDirectory, whereas here, if testing against FDB we use the directory under test.
     * <p>
     * You can search for "--BEGIN CUSTOM--" and "--END CUSTOM--" in the code to see exactly which lines.
     * </p>
     *
     * @throws IOException if there's issues
     */
    @Override
    @Ignore("Locally this test takes 2 minutes trying to write 500MB to a single file")
    public void testLargeCFS() throws IOException {
        final String testfile = "_123.test";
        IOContext context = new IOContext(new FlushInfo(0, 512 * 1024 * 1024));

        // --BEGIN CUSTOM--
        Directory dir;
        if (BaseIndexFileFormatTestCaseUtils.isUsingFDBDirectory()) {
            dir = newDirectory();
        } else {
            // this is what the base code does
            dir = new NRTCachingDirectory(newFSDirectory(createTempDir()), 2.0, 25.0);
        }
        // --END CUSTOM--

        SegmentInfo si = newSegmentInfo(dir, "_123");
        try (IndexOutput out = dir.createOutput(testfile, context)) {
            CodecUtil.writeIndexHeader(out, "Foo", 0, si.getId(), "suffix");
            byte[] bytes = new byte[512];
            for (int i = 0; i < 1024 * 1024; i++) {
                out.writeBytes(bytes, 0, bytes.length);
            }
            CodecUtil.writeFooter(out);
        }

        si.setFiles(Collections.singleton(testfile));
        si.getCodec().compoundFormat().write(dir, si, context);

        dir.close();
    }
}
