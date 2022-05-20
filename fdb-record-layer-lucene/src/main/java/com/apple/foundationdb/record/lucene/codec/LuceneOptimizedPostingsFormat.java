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

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.lucene84.Lucene84PostingsFormat;
import org.apache.lucene.codecs.lucene84.LuceneOptimizedPostingsReader;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;

/**
 * {@code PostingsFormat} optimized for FDB storage.
 */
public class LuceneOptimizedPostingsFormat extends PostingsFormat {
    PostingsFormat postingsFormat;

    public LuceneOptimizedPostingsFormat() {
        super("Lucene84Optimized");
        postingsFormat = new Lucene84PostingsFormat();
    }

    @Override
    public FieldsConsumer fieldsConsumer(final SegmentWriteState state) throws IOException {
        return postingsFormat.fieldsConsumer(state);
    }

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        PostingsReaderBase postingsReader = new LuceneOptimizedPostingsReader(state);
        boolean success = false;
        try {
            FieldsProducer ret = new LuceneOptimizedBlockTreeTermsReader(postingsReader, state);
            success = true;
            return ret;
        } finally {
            if (!success) {
                IOUtils.closeWhileHandlingException(postingsReader);
            }
        }
    }

}
