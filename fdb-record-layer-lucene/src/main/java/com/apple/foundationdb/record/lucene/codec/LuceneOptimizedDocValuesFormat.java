/*
 * LuceneOptimizedDocValuesFormat.java
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

import com.google.auto.service.AutoService;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene80.Lucene80DocValuesFormat;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;

import java.io.IOException;

/**
 * This class provides a Lazy reader implementation to limit the amount of
 * data needed to be read from FDB.
 *
 */
@AutoService(DocValuesFormat.class)
public class LuceneOptimizedDocValuesFormat extends DocValuesFormat {

    private DocValuesFormat docValuesFormat;

    public LuceneOptimizedDocValuesFormat() {
        this(new Lucene80DocValuesFormat());
    }

    public LuceneOptimizedDocValuesFormat(DocValuesFormat docValuesFormat) {
        super("RL80");
        this.docValuesFormat = docValuesFormat;
    }


    @Override
    public DocValuesConsumer fieldsConsumer(final SegmentWriteState state) throws IOException {
        return docValuesFormat.fieldsConsumer(state);
    }

    @Override
    public DocValuesProducer fieldsProducer(final SegmentReadState state) throws IOException {
        return new LazyDocValuesProducer(state);
    }

    private class LazyDocValuesProducer extends DocValuesProducer {
        private final LazyCloseable<DocValuesProducer> docValuesProducer;


        public LazyDocValuesProducer(SegmentReadState state) {
            docValuesProducer = LazyCloseable.supply(() -> docValuesFormat.fieldsProducer(state));
        }

        @Override
        public NumericDocValues getNumeric(final FieldInfo field) throws IOException {
            return docValuesProducer.get().getNumeric(field);
        }

        @Override
        public BinaryDocValues getBinary(final FieldInfo field) throws IOException {
            return docValuesProducer.get().getBinary(field);
        }

        @Override
        public SortedDocValues getSorted(final FieldInfo field) throws IOException {
            return docValuesProducer.get().getSorted(field);
        }

        @Override
        public SortedNumericDocValues getSortedNumeric(final FieldInfo field) throws IOException {
            return docValuesProducer.get().getSortedNumeric(field);
        }

        @Override
        public SortedSetDocValues getSortedSet(final FieldInfo field) throws IOException {
            return docValuesProducer.get().getSortedSet(field);
        }

        @Override
        public void checkIntegrity() throws IOException {
            if (LuceneOptimizedPostingsFormat.allowCheckDataIntegrity) {
                docValuesProducer.get().checkIntegrity();
            }
        }

        @Override
        public void close() throws IOException {
            docValuesProducer.close();
        }

        @Override
        public long ramBytesUsed() {
            return docValuesProducer.getUnchecked().ramBytesUsed();
        }
    }

}
