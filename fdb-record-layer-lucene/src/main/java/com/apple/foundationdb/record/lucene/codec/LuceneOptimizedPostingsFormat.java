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

import com.google.auto.service.AutoService;
import com.google.common.base.Suppliers;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.blocktree.BlockTreeTermsReader;
import org.apache.lucene.codecs.lucene84.Lucene84PostingsFormat;
import org.apache.lucene.codecs.lucene84.Lucene84PostingsWriter;
import org.apache.lucene.codecs.lucene84.LuceneOptimizedPostingsReader;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.function.Supplier;

/**
 * {@code PostingsFormat} optimized for FDB storage.
 */
@AutoService(PostingsFormat.class)
public class LuceneOptimizedPostingsFormat extends PostingsFormat {
    PostingsFormat postingsFormat;
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
    public FieldsConsumer fieldsConsumer(final SegmentWriteState state) throws IOException {
        PostingsWriterBase postingsWriter = new Lucene84PostingsWriter(state);
        return new LuceneOptimizedPostingsFieldsConsumer(state, postingsWriter);
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new LuceneOptimizedPostingsFieldsProducer(state, new LuceneOptimizedPostingsReader(state));
    }

}
