/*
 * LuceneOptimizedPostingsFormat.java
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

package com.apple.foundationdb.record.lucene.codec.postings;

import com.google.auto.service.AutoService;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene84.Lucene84PostingsFormat;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

/**
 * {@code PostingsFormat} optimized for FDB storage.
 */
@AutoService(PostingsFormat.class)
public class LuceneOptimizedPostingsFormat extends PostingsFormat {
    PostingsFormat postingsFormat;
    public static boolean allowCheckDataIntegrity = true;

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
        return new LuceneOptimizedPostingsFieldsConsumer(state);
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new LuceneOptimizedPostingsFieldsProducer(state);
    }

}
