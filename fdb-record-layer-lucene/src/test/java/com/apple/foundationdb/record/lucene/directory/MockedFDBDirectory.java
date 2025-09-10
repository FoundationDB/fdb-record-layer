/*
 * MockedFDBDirectory.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.record.lucene.LucenePrimaryKeySegmentIndex;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.util.pair.NonnullPair;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.lucene.store.IndexInput;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository.Methods.LUCENE_DELETE_FILE_INTERNAL;
import static com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository.Methods.LUCENE_GET_ALL_FIELDS_INFO_STREAM;
import static com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository.Methods.LUCENE_GET_FDB_LUCENE_FILE_REFERENCE_ASYNC;
import static com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository.Methods.LUCENE_GET_FILE_REFERENCE_CACHE_ASYNC;
import static com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository.Methods.LUCENE_GET_INCREMENT;
import static com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository.Methods.LUCENE_GET_PRIMARY_KEY_SEGMENT_INDEX;
import static com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository.Methods.LUCENE_LIST_ALL;
import static com.apple.foundationdb.record.lucene.directory.InjectedFailureRepository.Methods.LUCENE_READ_BLOCK;

/**
 * An {@link FDBDirectory} used for testing. It has the ability to delegate calls to the production implementation as well as fail as needed.
 * Failures are described as a triplet (method, exception, count), where method is the method name that should fail,
 * exception is the exception that should be throws and count is the number of "clean" invocation to allow before starting to fail.
 */
public class MockedFDBDirectory extends FDBDirectory {
    private InjectedFailureRepository injectedFailures;

    public MockedFDBDirectory(final Subspace subspace, final Map<String, String> options, final FDBDirectorySharedCacheManager sharedCacheManager, final Tuple sharedCacheKey, final boolean useCompoundFile, final AgilityContext agilityContext, final int blockCacheMaximumSize) {
        super(subspace, options, sharedCacheManager, sharedCacheKey, useCompoundFile, agilityContext, blockCacheMaximumSize);
    }

    public MockedFDBDirectory(@Nonnull final Subspace subspace, @Nonnull final FDBRecordContext context, @Nullable final Map<String, String> indexOptions) {
        super(subspace, context, indexOptions);
    }

    @Override
    public long getIncrement() throws IOException {
        injectedFailures.checkFailureForIoException(LUCENE_GET_INCREMENT);
        return super.getIncrement();
    }

    @Nonnull
    @Override
    public CompletableFuture<FDBLuceneFileReference> getFDBLuceneFileReferenceAsync(@Nonnull final String name) {
        return super.getFDBLuceneFileReferenceAsync(name)
                .thenApply(ref -> {
                    injectedFailures.checkFailureForCoreException(LUCENE_GET_FDB_LUCENE_FILE_REFERENCE_ASYNC);
                    return ref;
                });
    }

    @Nonnull
    @Override
    public CompletableFuture<byte[]> readBlock(@Nonnull final IndexInput requestingInput, @Nonnull final String fileName, @Nonnull final CompletableFuture<FDBLuceneFileReference> referenceFuture, final int block) {
        return super.readBlock(requestingInput, fileName, referenceFuture, block)
                .thenApply(bytes -> {
                    injectedFailures.checkFailureForCoreException(LUCENE_READ_BLOCK);
                    return bytes;
                });
    }

    @Nonnull
    @Override
    public String[] listAll() throws IOException {
        injectedFailures.checkFailureForIoException(LUCENE_LIST_ALL);
        return super.listAll();
    }

    @Nonnull
    @Override
    public CompletableFuture<Map<String, FDBLuceneFileReference>> getFileReferenceCacheAsync() {
        return super.getFileReferenceCacheAsync()
                .thenApply(cache -> {
                    injectedFailures.checkFailureForCoreException(LUCENE_GET_FILE_REFERENCE_CACHE_ASYNC);
                    return cache;
                });
    }

    @Override
    protected boolean deleteFileInternal(@Nonnull final Map<String, FDBLuceneFileReference> cache, @Nonnull final String name) throws IOException {
        injectedFailures.checkFailureForCoreException(LUCENE_DELETE_FILE_INTERNAL);
        return super.deleteFileInternal(cache, name);
    }

    @Nullable
    @Override
    public LucenePrimaryKeySegmentIndex getPrimaryKeySegmentIndex() {
        injectedFailures.checkFailureForCoreException(LUCENE_GET_PRIMARY_KEY_SEGMENT_INDEX);
        return super.getPrimaryKeySegmentIndex();
    }

    @Override
    Stream<NonnullPair<Long, byte[]>> getAllFieldInfosStream() {
        injectedFailures.checkFailureForCoreException(LUCENE_GET_ALL_FIELDS_INFO_STREAM);
        return super.getAllFieldInfosStream();
    }

    public void setInjectedFailures(final InjectedFailureRepository injectedFailures) {
        this.injectedFailures = injectedFailures;
    }
}
