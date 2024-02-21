/*
 * LuceneOptimizedRandomPostingsTester.java
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

package com.apple.foundationdb.record.lucene.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.BaseIndexFileFormatTestCaseUtils;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.RandomPostingsTester;
import org.apache.lucene.store.MockDirectoryWrapper;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Random;

/**
 * A subclass of {@link RandomPostingsTester} that overrides and modifies properties that are required for FDB-style testing.
 */
public class LuceneOptimizedRandomPostingsTester extends RandomPostingsTester {

    public LuceneOptimizedRandomPostingsTester(final Random random) throws IOException {
        super(random);
    }

    /**
     * This is a direct copy of the {@link RandomPostingsTester#testFull(Codec, Path, IndexOptions, boolean)}, except in
     * that version it always creates a FSDirectory, whereas here, if testing against FDB we use the directory under test.
     * <p>
     * You can search for "--BEGIN CUSTOM--" and "--END CUSTOM--" in the code to see exactly which lines.
     * </p>
     *
     * @throws Exception if there's issues
     */
    @Override
    public void testFull(Codec codec, Path path, IndexOptions options, boolean withPayloads) throws Exception {
        // --BEGIN CUSTOM--
        MockDirectoryWrapper dir;
        if (BaseIndexFileFormatTestCaseUtils.isUsingFDBDirectory()) {
            dir = LuceneTestCase.newMockDirectory();
        } else {
            dir = LuceneTestCase.newMockFSDirectory(LuceneTestCase.createTempDir("CFSManySubFiles"));
        }
        // --END CUSTOM--

        // TODO test thread safety of buildIndex too
        FieldsProducer fieldsProducer = buildIndex(codec, dir, options, withPayloads, true);

        testFields(fieldsProducer);

        IndexOptions[] allOptions = IndexOptions.values();
        int maxIndexOption = Arrays.asList(allOptions).indexOf(options);

        for (int i = 0; i <= maxIndexOption; i++) {
            testTerms(fieldsProducer, EnumSet.allOf(Option.class), allOptions[i], options, true);
            if (withPayloads) {
                // If we indexed w/ payloads, also test enums w/o accessing payloads:
                testTerms(fieldsProducer, EnumSet.complementOf(EnumSet.of(Option.PAYLOADS)), allOptions[i], options, true);
            }
        }

        fieldsProducer.close();
        dir.close();
    }
}
