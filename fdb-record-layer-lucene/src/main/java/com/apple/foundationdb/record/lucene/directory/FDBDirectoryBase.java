/*
 * FDBDirectoryBase.java
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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Abstract class for FDB-backed Lucene directories. Extends the standard Lucene Directory
 * class with FDB-specific methods.
 */
@API(API.Status.INTERNAL)
public abstract class FDBDirectoryBase extends Directory {

    public abstract long getIncrement() throws IOException;

    @Nonnull
    public abstract CompletableFuture<FDBLuceneFileReference> getFDBLuceneFileReferenceAsync(@Nonnull String name);

    @Nonnull
    public abstract FDBLuceneFileReference getFDBLuceneFileReference(@Nonnull String name);

    @Nonnull
    public abstract FieldInfosStorage getFieldInfosStorage();

    public abstract void setFieldInfoId(String filename, long id, ByteString bitSet);

    @Nonnull
    public abstract CompletableFuture<Integer> getFieldInfosCount();

    public abstract void writeFDBLuceneFileReference(@Nonnull String name, @Nonnull FDBLuceneFileReference reference);

    public abstract int writeData(long id, int block, @Nonnull byte[] value);

    public abstract void writeStoredFields(@Nonnull String segmentName, int docID, @Nonnull byte[] rawBytes);

    public abstract void deleteStoredFields(@Nonnull String segmentName) throws IOException;

    @Nonnull
    public abstract CompletableFuture<byte[]> readBlock(@Nonnull IndexInput requestingInput,
                                                        @Nonnull String fileName,
                                                        @Nonnull CompletableFuture<FDBLuceneFileReference> referenceFuture,
                                                        int block);

    @Nullable
    public abstract byte[] readStoredFields(String segmentName, int docId);

    @Nonnull
    public abstract List<byte[]> readAllStoredFields(String segmentName);

    @Nonnull
    public abstract CompletableFuture<Collection<String>> listAllAsync();

    @Nonnull
    public abstract CompletableFuture<Map<String, FDBLuceneFileReference>> getAllAsync();

    @Nonnull
    public abstract CompletableFuture<List<KeyValue>> scanStoredFields(String segmentName);

    @Nonnull
    public abstract CompletableFuture<Map<String, FDBLuceneFileReference>> getFileReferenceCacheAsync();

    public abstract boolean usesOptimizedStoredFields();

    public abstract int getBlockSize();

    @Nonnull
    public abstract AgilityContext getAgilityContext();

    public abstract <T> T asyncToSync(@Nonnull StoreTimer.Wait event, @Nonnull CompletableFuture<T> async);

    @Nonnull
    public abstract LuceneSerializer getSerializer();

    @Nonnull
    public abstract Subspace getSubspace();

    @Nonnull
    public abstract ChecksumIndexInput openChecksumInput(String name, IOContext context) throws IOException;

    @Nullable
    public abstract LucenePrimaryKeySegmentIndex getPrimaryKeySegmentIndex();

    public abstract long primaryKeySegmentId(@Nonnull String segmentName, boolean create) throws IOException;

    @Nonnull
    public abstract String primaryKeySegmentName(long segmentId);

    public abstract boolean getBooleanIndexOption(@Nonnull String key, boolean defaultValue);

    @Nullable
    public abstract String getIndexOption(@Nonnull String key);
}
