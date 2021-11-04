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
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;

import java.io.IOException;
import java.util.Collection;

import static com.apple.foundationdb.record.lucene.directory.FDBDirectory.isEntriesFile;
import static com.apple.foundationdb.record.lucene.directory.FDBDirectory.isSegmentInfo;

public class LuceneOptimizedWrappedDirectory extends Directory {
    private final FDBDirectory fdbDirectory;
    private final Directory wrappedDirectory;

    public LuceneOptimizedWrappedDirectory(Directory directory) {
        this.wrappedDirectory = directory;
        this.fdbDirectory = (FDBDirectory)FilterDirectory.unwrap(directory);
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
        if (isSegmentInfo(name) || isEntriesFile(name)) {
            return new LuceneOptimizedWrappedIndexOutput(name, fdbDirectory, isSegmentInfo(name));
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
        if (isSegmentInfo(name) || isEntriesFile(name)) {
            return new LuceneOptimizedWrappedIndexInput(name, fdbDirectory, isSegmentInfo(name));
        } else {
            return wrappedDirectory.openInput(name, context);
        }
    }

    @Override
    public Lock obtainLock(final String name) throws IOException {
        throw new UnsupportedOperationException("Not Supported");
    }

    @Override
    public void close() throws IOException {
        throw new UnsupportedOperationException("Not Supported");
    }

    @Override
    public ChecksumIndexInput openChecksumInput(final String name, final IOContext context) throws IOException {
        return new BufferedChecksumIndexInput(openInput(name, context));
    }
}
