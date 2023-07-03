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
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.codecs.blocktree.BlockTreeTermsReader;
import org.apache.lucene.codecs.lucene84.Lucene84PostingsFormat;
import org.apache.lucene.codecs.lucene84.LuceneOptimizedPostingsReader;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;

/**
 * {@code PostingsFormat} optimized for FDB storage.
 */
@AutoService(PostingsFormat.class)
public class LuceneOptimizedPostingsFormat extends PostingsFormat {
    PostingsFormat postingsFormat;

    public LuceneOptimizedPostingsFormat() {
        this(new Lucene84PostingsFormat());
    }

    public LuceneOptimizedPostingsFormat(PostingsFormat postingsFormat) {
        super("RL" + postingsFormat.getName());
        this.postingsFormat = postingsFormat;
    }

    @Override
    public FieldsConsumer fieldsConsumer(final SegmentWriteState state) throws IOException {
        return postingsFormat.fieldsConsumer(state);
    }

    @Override
    @SuppressWarnings("PMD.CloseResource")
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new LazyFieldsProducer(state);
    }

    private static class LazyFieldsProducer extends FieldsProducer {

        private Supplier<FieldsProducer> fieldsProducer;

        private boolean initialized;

        private LazyFieldsProducer(final SegmentReadState state) {
            fieldsProducer = Suppliers.memoize(() -> {
                try {
                    PostingsReaderBase postingsReader = new LuceneOptimizedPostingsReader(state);
                    return new BlockTreeTermsReader(postingsReader, state);
                } catch (IOException ioe) {
                    throw new UncheckedIOException(ioe);
                } finally {
                    initialized = true;
                }
            });
        }

        @Override
        public void close() throws IOException {
            if (initialized) {
                fieldsProducer.get().close();
            }
        }

        @Override
        public void checkIntegrity() throws IOException {
            fieldsProducer.get().checkIntegrity();
        }

        @Override
        public Iterator<String> iterator() {
            return fieldsProducer.get().iterator();
        }

        @Override
        public Terms terms(final String field) throws IOException {
            return fieldsProducer.get().terms(field);
        }

        @Override
        public int size() {
            return fieldsProducer.get().size();
        }

        @Override
        public long ramBytesUsed() {
            return fieldsProducer.get().ramBytesUsed();
        }
    }

}
