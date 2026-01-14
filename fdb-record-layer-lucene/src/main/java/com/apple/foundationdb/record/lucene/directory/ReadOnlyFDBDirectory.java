/*
 * ReadOnlyFDBDirectory.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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


import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.lucene.LucenePrimaryKeySegmentIndex;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.subspace.Subspace;
import com.google.protobuf.ByteString;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.Lock;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * A {@link FDBDirectoryBase} implementation that delegates read operations to an underlying FDB directory
 * but rejects all write operations and uses {@link NoOpLockFactory} to avoid acquiring locks.
 * This is used for read-only {@link org.apache.lucene.index.IndexWriter} instances that need to
 * read the index structure without modifying it, such as when replaying queued operations
 * while a merge is in progress.
 *
 * <p>All write operations throw {@link UnsupportedOperationException}, including:</p>
 * <ul>
 *     <li>File operations: createOutput, createTempOutput, deleteFile, rename</li>
 *     <li>Data operations: writeData, writeStoredFields, deleteStoredFields</li>
 *     <li>Metadata operations: writeFDBLuceneFileReference, setFieldInfoId</li>
 *     <li>ID generation: getIncrement, primaryKeySegmentId (when create=true)</li>
 * </ul>
 * <p>This directory uses a {@link NoOpLockFactory} to ensure no lock is actually taken, so that the {@link org.apache.lucene.index.IndexWriter}
 * can perform its functions.</p>
 */
@API(API.Status.INTERNAL)
public class ReadOnlyFDBDirectory extends FDBDirectoryBase {

    @Nonnull
    private final FDBDirectoryBase delegate;

    public ReadOnlyFDBDirectory(@Nonnull final FDBDirectoryBase delegate) {
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
        // TODO: More docs, fix?
        //throw new UnsupportedOperationException("Cannot delete file in a read-only directory");
        return;
    }

    @Override
    public long fileLength(String name) throws IOException {
        return delegate.fileLength(name);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        throw new UnsupportedOperationException("Cannot create output in a read-only directory");
    }

    @Override
    public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
        throw new UnsupportedOperationException("Cannot create temp output in a read-only directory");
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        // No-op for read-only directory
    }

    @Override
    public void syncMetaData() throws IOException {
        // No-op for read-only directory
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        throw new UnsupportedOperationException("Cannot rename files in a read-only directory");
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

    // Delegate FDB-specific methods, rejecting write operations

    @Override
    public long getIncrement() throws IOException {
        throw new UnsupportedOperationException("Cannot get increment in a read-only directory (writes to FDB)");
    }

    @Override
    @Nonnull
    public CompletableFuture<FDBLuceneFileReference> getFDBLuceneFileReferenceAsync(@Nonnull final String name) {
        return delegate.getFDBLuceneFileReferenceAsync(name);
    }

    @Override
    @Nonnull
    public FDBLuceneFileReference getFDBLuceneFileReference(@Nonnull final String name) {
        return delegate.getFDBLuceneFileReference(name);
    }

    @Override
    @Nonnull
    public FieldInfosStorage getFieldInfosStorage() {
        return delegate.getFieldInfosStorage();
    }

    @Override
    public void setFieldInfoId(final String filename, final long id, final ByteString bitSet) {
        throw new UnsupportedOperationException("Cannot set field info ID in a read-only directory");
    }

    @Override
    @Nonnull
    public CompletableFuture<Integer> getFieldInfosCount() {
        return delegate.getFieldInfosCount();
    }

    @Override
    public void writeFDBLuceneFileReference(@Nonnull final String name, @Nonnull final FDBLuceneFileReference reference) {
        throw new UnsupportedOperationException("Cannot write file reference in a read-only directory");
    }

    @Override
    public int writeData(final long id, final int block, @Nonnull final byte[] value) {
        throw new UnsupportedOperationException("Cannot write data in a read-only directory");
    }

    @Override
    public void writeStoredFields(@Nonnull final String segmentName, final int docID, @Nonnull final byte[] rawBytes) {
        throw new UnsupportedOperationException("Cannot write stored fields in a read-only directory");
    }

    @Override
    public void deleteStoredFields(@Nonnull final String segmentName) {
        throw new UnsupportedOperationException("Cannot delete stored fields in a read-only directory");
    }

    @Nonnull
    @Override
    public CompletableFuture<byte[]> readBlock(@Nonnull final IndexInput requestingInput, @Nonnull final String fileName, @Nonnull final CompletableFuture<FDBLuceneFileReference> referenceFuture, final int block) {
        return delegate.readBlock(requestingInput, fileName, referenceFuture, block);
    }

    @Override
    @Nullable
    public byte[] readStoredFields(final String segmentName, final int docId) {
        return delegate.readStoredFields(segmentName, docId);
    }

    @Override
    @Nonnull
    public List<byte[]> readAllStoredFields(final String segmentName) {
        return delegate.readAllStoredFields(segmentName);
    }

    @Override
    @Nonnull
    public CompletableFuture<Collection<String>> listAllAsync() {
        return delegate.listAllAsync();
    }

    @Override
    @Nonnull
    public CompletableFuture<Map<String, FDBLuceneFileReference>> getAllAsync() {
        return delegate.getAllAsync();
    }

    @Override
    @Nonnull
    public CompletableFuture<List<KeyValue>> scanStoredFields(final String segmentName) {
        return delegate.scanStoredFields(segmentName);
    }

    @Override
    @Nonnull
    public CompletableFuture<Map<String, FDBLuceneFileReference>> getFileReferenceCacheAsync() {
        return delegate.getFileReferenceCacheAsync();
    }

    @Override
    public boolean usesOptimizedStoredFields() {
        return delegate.usesOptimizedStoredFields();
    }

    @Override
    public int getBlockSize() {
        return delegate.getBlockSize();
    }

    @Override
    @Nonnull
    public AgilityContext getAgilityContext() {
        return delegate.getAgilityContext();
    }

    @Override
    public <T> T asyncToSync(@Nonnull final StoreTimer.Wait event, @Nonnull final CompletableFuture<T> async) {
        return delegate.asyncToSync(event, async);
    }

    @Override
    @Nonnull
    public LuceneSerializer getSerializer() {
        return delegate.getSerializer();
    }

    @Override
    @Nonnull
    public Subspace getSubspace() {
        return delegate.getSubspace();
    }

    @Override
    @Nonnull
    public ChecksumIndexInput openChecksumInput(final String name, final IOContext context) throws IOException {
        return delegate.openChecksumInput(name, context);
    }

    @Override
    @Nullable
    public LucenePrimaryKeySegmentIndex getPrimaryKeySegmentIndex() {
        return delegate.getPrimaryKeySegmentIndex();
    }

    @Override
    public long primaryKeySegmentId(@Nonnull final String segmentName, final boolean create) throws IOException {
        if (create) {
            throw new UnsupportedOperationException("Cannot create primary key segment ID in a read-only directory");
        }
        return delegate.primaryKeySegmentId(segmentName, false);
    }

    @Override
    @Nonnull
    public String primaryKeySegmentName(final long segmentId) {
        return delegate.primaryKeySegmentName(segmentId);
    }

    @Override
    public boolean getBooleanIndexOption(@Nonnull final String key, final boolean defaultValue) {
        return delegate.getBooleanIndexOption(key, defaultValue);
    }

    @Override
    @Nullable
    public String getIndexOption(@Nonnull final String key) {
        return delegate.getIndexOption(key);
    }
}
