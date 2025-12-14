/*
 * FDBDirectoryNoLock.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.lucene.directory;

import com.apple.foundationdb.annotation.API;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

/**
 * A {@link Directory} wrapper that delegates all operations to an underlying directory
 * but uses {@link NoOpLockFactory} to avoid acquiring locks. This is used for read-only
 * {@link org.apache.lucene.index.IndexWriter} instances that need to write documents
 * without actually committing or flushing, such as when replaying queued operations
 * while a merge is in progress.
 *
 * <p>Since this directory does not acquire locks, it should only be used when:</p>
 * <ul>
 *     <li>The writer will not perform any flush or commit operations</li>
 *     <li>Another writer with proper locking is managing the actual index updates</li>
 *     <li>You need to write documents but avoid lock conflicts</li>
 * </ul>
 *
 * TODO: Make this read-only so that we can't commit or flush read-only writers
 */
@API(API.Status.INTERNAL)
public class ReadOnlyFDBDirectory extends Directory {

    private final Directory delegate;

    public ReadOnlyFDBDirectory(@Nonnull final Directory delegate) {
        this.delegate = delegate;
    }

    @Override
    @Nonnull
    public Lock obtainLock(@Nonnull final String lockName) throws IOException {
        // Return a no-op lock instead of actually acquiring the lock
        return NoOpLockFactory.INSTANCE.obtainLock(this, lockName);
    }

    @Override
    public String[] listAll() throws IOException {
        return delegate.listAll();
    }

    @Override
    public void deleteFile(String name) throws IOException {
        delegate.deleteFile(name);
    }

    @Override
    public long fileLength(String name) throws IOException {
        return delegate.fileLength(name);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        return delegate.createOutput(name, context);
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        return delegate.createTempOutput(prefix, suffix, context);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        delegate.sync(names);
    }

    @Override
    public void syncMetaData() throws IOException {
        delegate.syncMetaData();
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        delegate.rename(source, dest);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        return delegate.openInput(name, context);
    }

    @Override
    public void close() throws IOException {
        // Don't close the delegate - it's managed by the wrapper
    }

    @Override
    public Set<String> getPendingDeletions() throws IOException {
        return delegate.getPendingDeletions();
    }
}
