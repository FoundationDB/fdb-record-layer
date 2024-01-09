/*
 * LuceneOptimizedPostingsFormat.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.lucene.LuceneIndexOptions;
import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.lucene.directory.FDBDirectoryUtils;
import com.google.auto.service.AutoService;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.blocktree.BlockTreeTermsReader;
import org.apache.lucene.codecs.lucene84.Lucene84PostingsFormat;
import org.apache.lucene.codecs.lucene84.LuceneOptimizedPostingsReaderByBlocks;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;

import java.io.IOException;
import java.util.Iterator;

/**
 * {@code PostingsFormat} optimized for FDB storage.
 */
@AutoService(PostingsFormat.class)
public class LuceneOptimizedPostingsFormat extends PostingsFormat {
    public static final String POSTINGS_EXTENSION = "fpf";
    // This is the inner format that is used when LuceneIndexOptions.OPTIMIZED_POSTINGS_FORMAT_ENABLED is FALSE
    private final PostingsFormat postingsFormat;
    static boolean allowCheckDataIntegrity = true;

    public LuceneOptimizedPostingsFormat() {
        this(new Lucene84PostingsFormat());
    }

    public LuceneOptimizedPostingsFormat(PostingsFormat postingsFormat) {
        super("RL" + postingsFormat.getName());
        this.postingsFormat = postingsFormat;
    }

    public static void setAllowCheckDataIntegrity(boolean allow) {
        allowCheckDataIntegrity = allow;
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    public FieldsConsumer fieldsConsumer(final SegmentWriteState state) throws IOException {
        final FDBDirectory fdbDirectory = FDBDirectoryUtils.getFDBDirectory(state.directory);

        // Use FALSE as the default OPTIMIZED_POSTINGS_FORMAT_ENABLED option, for backwards compatibility
        if (fdbDirectory.getBooleanIndexOption(LuceneIndexOptions.OPTIMIZED_POSTINGS_FORMAT_ENABLED, false)) {
            // Create a "dummy" file to tap into the lifecycle management (e.g. be notified when to delete the data)
            state.directory.createOutput(IndexFileNames.segmentFileName(state.segmentInfo.name, "",
                            LuceneOptimizedPostingsFormat.POSTINGS_EXTENSION), state.context)
                    .close();
            LuceneOptimizedPostingsWriter postingsWriter = new LuceneOptimizedPostingsWriter(state);
            return new LuceneOptimizedPostingsFieldsConsumer(state, postingsWriter);
        } else {
            return postingsFormat.fieldsConsumer(state);
        }
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        final FDBDirectory fdbDirectory = FDBDirectoryUtils.getFDBDirectory(state.directory);

        if (fdbDirectory.getBooleanIndexOption(LuceneIndexOptions.OPTIMIZED_POSTINGS_FORMAT_ENABLED, false)) {
            LuceneOptimizedPostingsReader postingsReader = new LuceneOptimizedPostingsReader(state);
            return new LuceneOptimizedPostingsFieldsProducer(state, postingsReader);
        } else {
            return new LazyFieldsProducer(state);
        }
    }

    private static class LazyFieldsProducer extends FieldsProducer {

        private final LazyCloseable<FieldsProducer> fieldsProducer;

        private LazyFieldsProducer(final SegmentReadState state) {
            fieldsProducer = LazyCloseable.supply(() -> {
                PostingsReaderBase postingsReader = new LuceneOptimizedPostingsReaderByBlocks(state);
                return new BlockTreeTermsReader(postingsReader, state);
            });
        }

        @Override
        public void close() throws IOException {
            fieldsProducer.close();
        }

        @Override
        public void checkIntegrity() throws IOException {
            if (allowCheckDataIntegrity) {
                fieldsProducer.get().checkIntegrity();
            }
        }

        @Override
        public Iterator<String> iterator() {
            return fieldsProducer.getUnchecked().iterator();
        }

        @Override
        public Terms terms(final String field) throws IOException {
            return fieldsProducer.get().terms(field);
        }

        @Override
        public int size() {
            return fieldsProducer.getUnchecked().size();
        }

        @Override
        public long ramBytesUsed() {
            return fieldsProducer.getUnchecked().ramBytesUsed();
        }
    }

}
