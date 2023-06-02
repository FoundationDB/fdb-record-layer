/*
 * LuceneOptimizedStoredFieldsFormat.java
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

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Accountable;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * {@code StoredFieldsFormat} optimized for FDB storage.
 */
public class LuceneOptimizedStoredFieldsFormat extends StoredFieldsFormat {
    StoredFieldsFormat underlying;

    public LuceneOptimizedStoredFieldsFormat(final StoredFieldsFormat underlying) {
        this.underlying = underlying;
    }

    @Override
    public StoredFieldsReader fieldsReader(final Directory directory, final SegmentInfo si, final FieldInfos fn, final IOContext context) throws IOException {
        return new NonBlockingFieldsReader(directory, si, fn, context, underlying);
    }

    @Override
    public StoredFieldsWriter fieldsWriter(final Directory directory, final SegmentInfo si, final IOContext context) throws IOException {
        return underlying.fieldsWriter(directory, si, context);
    }

    private static class NonBlockingFieldsReader extends StoredFieldsReader {
        private final Future<StoredFieldsReader> readerFuture;
        private final Directory directory;
        private final SegmentInfo si;
        private final FieldInfos fn;
        private final IOContext context;
        private final StoredFieldsFormat underlying;

        @SuppressWarnings("PMD.CloseResource")
        public NonBlockingFieldsReader(final Directory directory, final SegmentInfo si, final FieldInfos fn, final IOContext context, StoredFieldsFormat underlying) {
            this.directory = directory;
            this.si = si;
            this.fn = fn;
            this.context = context;
            this.underlying = underlying;
            Supplier<StoredFieldsReader> supplier = () -> {
                try {
                    return underlying.fieldsReader(directory, si, fn, context);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            };
            FDBRecordContext ctx = getFDBRecordContext(directory);
            if (ctx != null) {
                readerFuture = CompletableFuture.supplyAsync(supplier, ctx.getExecutor());
            } else {
                readerFuture = CompletableFuture.supplyAsync(supplier);
            }
        }

        @Override
        public void visitDocument(final int docID, final StoredFieldVisitor visitor) throws IOException {
            getReader().visitDocument(docID, visitor);
        }

        @Override
        @SuppressWarnings({"java:S1182", "java:S2975", "PMD.ProperCloneImplementation"})
        public NonBlockingFieldsReader clone() {
            return new NonBlockingFieldsReader(directory, si, fn, context, underlying);
        }

        @Override
        public void checkIntegrity() throws IOException {
            getReader().checkIntegrity();
        }

        @Override
        public void close() throws IOException {
            getReader().close();
        }

        @Override
        public long ramBytesUsed() {
            try {
                return getReader().ramBytesUsed();
            } catch (Exception e) {
                return -1;
            }
        }

        @Override
        public StoredFieldsReader getMergeInstance() {
            try {
                return getReader().getMergeInstance();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Collection<Accountable> getChildResources() {
            try {
                return getReader().getChildResources();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private StoredFieldsReader getReader() throws IOException {
            try {
                return readerFuture.get();
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        private FDBRecordContext getFDBRecordContext(Directory directory) {
            if (directory instanceof LuceneOptimizedCompoundReader) {
                return ((LuceneOptimizedCompoundReader)directory).getFDBRecordContext();
            }
            return null;
        }
    }
}
