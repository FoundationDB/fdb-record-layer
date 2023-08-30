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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class provides a Lazy reader implementation to limit the amount of
 * data needed to be read from FDB.
 *
 */
public class LuceneOptimizedStoredFieldsFormat extends StoredFieldsFormat {

    private StoredFieldsFormat storedFieldsFormat;

    LuceneOptimizedStoredFieldsFormat(StoredFieldsFormat storedFieldsFormat) {
        this.storedFieldsFormat = storedFieldsFormat;
    }

    @Override
    public StoredFieldsReader fieldsReader(final Directory directory, final SegmentInfo si, final FieldInfos fn, final IOContext context) throws IOException {
        return new LazyStoredFieldsReader(directory, si, fn, context);
    }

    @Override
    public StoredFieldsWriter fieldsWriter(final Directory directory, final SegmentInfo si, final IOContext context) throws IOException {
        return storedFieldsFormat.fieldsWriter(directory, si, context);
    }

    private class LazyStoredFieldsReader extends StoredFieldsReader {
        private Supplier<StoredFieldsReader> storedFieldsReader;
        private AtomicBoolean initialized = new AtomicBoolean(false);
        private Directory directory;
        private SegmentInfo si;
        private FieldInfos fn;
        private IOContext context;

        public LazyStoredFieldsReader(final Directory directory, final SegmentInfo si, final FieldInfos fn, final IOContext context) {
            this.directory = directory;
            this.si = si;
            this.fn = fn;
            this.context = context;
            storedFieldsReader = Suppliers.memoize(() -> {
                try {
                    return storedFieldsFormat.fieldsReader(directory, si, fn, context);
                } catch (IOException ioe) {
                    throw new UncheckedIOException(ioe);
                } finally {
                    initialized.set(true);
                }
            });
        }

        public LazyStoredFieldsReader(LazyStoredFieldsReader other) {
            this.directory = other.directory;
            this.si = other.si;
            this.fn = other.fn;
            this.context = other.context;
            // TODO this makes this less lazy, but if we don't do this the tests fail because we don't close
            //      the underlying handles. There is almost certainly a way to fix both
            this.storedFieldsReader = Suppliers.memoize(() -> other.storedFieldsReader.get().clone());
            initialized.set(true);
        }

        @Override
        public void visitDocument(final int docID, final StoredFieldVisitor visitor) throws IOException {
            getStoredFieldsReader().visitDocument(docID, visitor);
        }

        private StoredFieldsReader getStoredFieldsReader() throws IOException {
            try {
                return storedFieldsReader.get();
            } catch (UncheckedIOException e) {
                throw e.getCause();
            }
        }

        @Override
        @SuppressWarnings({"java:S1182", "java:S2975", "PMD.ProperCloneImplementation"})
        public LazyStoredFieldsReader clone() {
            return new LazyStoredFieldsReader(this);
        }

        @Override
        public void checkIntegrity() throws IOException {
            if (LuceneOptimizedPostingsFormat.allowCheckDataIntegrity) {
                getStoredFieldsReader().checkIntegrity();
            }
        }

        @Override
        public void close() throws IOException {
            if (initialized.get()) { // Needed to not fetch data...
                getStoredFieldsReader().close();
            }
        }

        @Override
        public long ramBytesUsed() {
            return storedFieldsReader.get().ramBytesUsed();
        }
    }
}
