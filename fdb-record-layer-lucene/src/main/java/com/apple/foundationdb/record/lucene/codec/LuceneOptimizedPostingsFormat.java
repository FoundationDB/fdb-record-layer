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

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
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
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

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
    @SuppressWarnings("PMD.CloseResource")
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new NonBlockingFieldsProducer(state);
    }

    private static class NonBlockingFieldsProducer extends FieldsProducer {
        private CompletableFuture<FieldsProducer> fieldsProducerFuture;
        private IOException exception;

        @SuppressWarnings("PMD.CloseResource")
        private NonBlockingFieldsProducer(SegmentReadState state) throws IOException {
            FDBRecordContext context = getFDBRecordContext(state);
            Supplier<FieldsProducer> fieldsProducerSupplier = () -> {
                PostingsReaderBase postingsReader = null;
                try {
                    postingsReader = new LuceneOptimizedPostingsReader(state);
                    return new BlockTreeTermsReader(postingsReader, state);
                } catch (Exception e) {
                    this.exception = new IOException(e);
                } finally {
                    if (exception != null) {
                        if (postingsReader != null) {
                            IOUtils.closeWhileHandlingException(postingsReader);
                        }
                    }
                }
                return null;
            };
            if (context != null) {
                fieldsProducerFuture = CompletableFuture.supplyAsync(fieldsProducerSupplier, context.getExecutor());
            } else {
                fieldsProducerFuture = CompletableFuture.supplyAsync(fieldsProducerSupplier);
            }
        }

        private FDBRecordContext getFDBRecordContext(SegmentReadState state) {
            if (state.directory instanceof LuceneOptimizedCompoundReader) {
                return ((LuceneOptimizedCompoundReader) state.directory).getFDBRecordContext();
            }
            return null;
        }

        private FieldsProducer getProducer() throws IOException {
            try {
                if (exception == null) {
                    return fieldsProducerFuture.get();
                }
            } catch (Exception e) {
                this.exception = new IOException(e);
            }
            throw exception;
        }

        @Override
        public void close() throws IOException {
            getProducer().close();
        }

        @Override
        public void checkIntegrity() throws IOException {
            getProducer().checkIntegrity();
        }

        @Override
        public Iterator<String> iterator() {
            try {
                return getProducer().iterator();
            } catch (Exception e) {
                return Collections.emptyIterator();
            }
        }

        @Override
        public Terms terms(final String field) throws IOException {
            return getProducer().terms(field);
        }

        @Override
        public int size() {
            try {
                return getProducer().size();
            } catch (Exception e) {
                return -1;
            }
        }

        @Override
        public long ramBytesUsed() {
            try {
                return getProducer().ramBytesUsed();
            } catch (Exception e) {
                return -1;
            }
        }
    }

}
