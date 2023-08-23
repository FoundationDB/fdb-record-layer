/*
 * WrappedDirectory.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.lucene.directory.FDBDirectory;
import com.apple.foundationdb.record.lucene.directory.FDBLuceneFileReference;
import com.google.common.primitives.Longs;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.apple.foundationdb.record.lucene.codec.LuceneOptimizedCompoundFormat.DATA_EXTENSION;


/**
 * An Optimized Directory that understands that we store segments and names in the file reference instead of
 * stand-alone files.
 * TODO: I think this could be merged down into FDBDirectory, the only question is how to maintain FieldInfos in the schema
 */
class LuceneOptimizedWrappedDirectory extends Directory {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneOptimizedWrappedDirectory.class);
    private final FDBDirectory fdbDirectory;
    private final Directory wrappedDirectory;
    private FieldInfos fieldInfos;

    LuceneOptimizedWrappedDirectory(Directory directory) {
        this.wrappedDirectory = directory;
        this.fdbDirectory = (FDBDirectory)FilterDirectory.unwrap(directory);
    }

    LuceneOptimizedWrappedDirectory(Directory directory, FieldInfos fieldInfos) {
        this(directory);
        this.fieldInfos = fieldInfos;
    }

    @Override
    public String[] listAll() throws IOException {
        throw new UnsupportedOperationException("Not Supported");
    }

    @Override
    public void deleteFile(final String name) throws IOException {
        throw new UnsupportedOperationException("Not Supported");
    }

    @Override
    public long fileLength(final String name) throws IOException {
        throw new UnsupportedOperationException("Not Supported");
    }

    @Override
    public IndexOutput createOutput(String name, final IOContext context) throws IOException {
        if (FDBDirectory.isFieldInfoFile(name)) {
            return new LuceneOptimizedWrappedIndexOutput(name) {
                @Override
                public void close() throws IOException {
                    FDBLuceneFileReference reference = new FDBLuceneFileReference(-1, -1, -1, -1);
                    List<Long> words = getBitSetWords(fieldInfos);
                    reference.setBitSetWords(words);
                    byte[] schema = fdbDirectory.readSchema(words);
                    if (schema == null) {
                        fdbDirectory.writeSchema(words, outputStream.toByteArray());
                    }
                    ((FDBDirectory)FilterDirectory.unwrap(fdbDirectory)).writeFDBLuceneFileReference(name, reference);
                }
            };
        } else {
            return wrappedDirectory.createOutput(name, context);
        }
    }

    @Override
    public IndexOutput createTempOutput(final String prefix, final String suffix, final IOContext context) throws IOException {
        throw new UnsupportedOperationException("Not Supported");
    }

    @Override
    public void sync(final Collection<String> names) throws IOException {
        throw new UnsupportedOperationException("Not Supported");
    }

    @Override
    public void syncMetaData() throws IOException {
        throw new UnsupportedOperationException("Not Supported");
    }

    @Override
    public void rename(final String source, final String dest) throws IOException {
        throw new UnsupportedOperationException("Not Supported");
    }

    @Override
    public IndexInput openInput(String name, final IOContext context) throws IOException {
        return wrappedDirectory.openInput(name, context);
    }

    /**
     * Opens a lazy input with performing a seek.
     *
     * @param name name
     * @param initialOffset offset
     * @param position current position
     * @return IndexInput
     * @throws IOException exception
     */
    public IndexInput openLazyInput(@Nonnull final String name, long initialOffset, long position) throws IOException {
        return fdbDirectory.openLazyInput(name, initialOffset, position);
    }

    @Override
    public Lock obtainLock(final String name) throws IOException {
        throw new UnsupportedOperationException("Not Supported");
    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException("Not Supported");
    }

    /**
     * Places a prefetchable buffer over the checksum input in an attempt to pipeline reads when an
     * FDBIndexOutput performs a copyBytes operation.
     *
     * @param name file name
     * @param context io context
     * @return ChecksumIndexInput
     * @throws IOException ioexception
     */
    @Override
    public ChecksumIndexInput openChecksumInput(final String name, final IOContext context) throws IOException {
        return new PrefetchableBufferedChecksumIndexInput(openInput(name, context));
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return Collections.emptySet();
    }

    public FDBDirectory getFdbDirectory() {
        return fdbDirectory;
    }

    public static String convertToDataFile(String name) {
        if (FDBDirectory.isSegmentInfo(name)) {
            return name.substring(0, name.length() - 2) + DATA_EXTENSION;
        } else if (FDBDirectory.isEntriesFile(name) || FDBDirectory.isFieldInfoFile(name)) {
            return name.substring(0, name.length() - 3) + DATA_EXTENSION;
        } else {
            return name;
        }

    }

    public static List<Long> getBitSetWords(final FieldInfos infos) {
        BitSet bitSet = new BitSet(infos.size());
        for (FieldInfo fi : infos) {
            bitSet.set(fi.number);
        }
        return Longs.asList(bitSet.toLongArray());
    }
}
