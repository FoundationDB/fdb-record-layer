/*
 * MockedFdbDirectory.java
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


import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import org.apache.lucene.store.IndexInput;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An {@link FDBDirectory} used for testing. It has the ability to delegate calls to the production implementation as well as fail as needed.
 * Failures are described as a triplet (method, exception, count), where method is the method name that should fail,
 * exception is the exception that should be throws and count is the number of "clean" invocation to allow before starting to fail.
 */
public class MockedFDBDirectory extends FDBDirectory {
    /**
     * The method descriptions that can fail execution.
     */
    public enum Methods { GET_INCREMENT,
        GET_FDB_LUCENE_FILE_REFERENCE_ASYNC,
        READ_BLOCK,
        LIST_ALL,
        GET_FILE_REFERENCE_CACHE_ASYNC,
        DELETE_FILE_INTERNAL
    }


    private EnumMap<Methods, FailureDescription> failureDescriptions = new EnumMap<>(Methods.class);
    private EnumMap<Methods, AtomicLong> invocationCounts = new EnumMap<>(Methods.class);

    public MockedFDBDirectory(@Nonnull final Subspace subspace, @Nonnull final FDBRecordContext context, @Nullable final Map<String, String> indexOptions) {
        super(subspace, context, indexOptions);
    }

    public void addFailure(@Nonnull Methods method, @Nonnull Exception exception, long count) {
        failureDescriptions.put(method, new FailureDescription(method, exception, count));
    }

    public void removeFailure(@Nonnull Methods method) {
        failureDescriptions.remove(method);
        invocationCounts.remove(method);
    }

    @Override
    public long getIncrement() throws IOException {
        checkFailureForIoException(Methods.GET_INCREMENT);
        return super.getIncrement();
    }

    @Nonnull
    @Override
    public CompletableFuture<FDBLuceneFileReference> getFDBLuceneFileReferenceAsync(@Nonnull final String name) {
        return super.getFDBLuceneFileReferenceAsync(name)
                .thenApply(ref -> {
                    checkFailureForCoreException(Methods.GET_FDB_LUCENE_FILE_REFERENCE_ASYNC);
                    return ref;
                });
    }

    @Nonnull
    @Override
    public CompletableFuture<byte[]> readBlock(@Nonnull final IndexInput requestingInput, @Nonnull final String fileName, @Nonnull final CompletableFuture<FDBLuceneFileReference> referenceFuture, final int block) {
        return super.readBlock(requestingInput, fileName, referenceFuture, block)
                .thenApply(bytes -> {
                    checkFailureForCoreException(Methods.READ_BLOCK);
                    return bytes;
                });
    }

    @Nonnull
    @Override
    public String[] listAll() throws IOException {
        checkFailureForIoException(Methods.LIST_ALL);
        return super.listAll();
    }

    @Nonnull
    @Override
    protected CompletableFuture<Map<String, FDBLuceneFileReference>> getFileReferenceCacheAsync() {
        return super.getFileReferenceCacheAsync()
                .thenApply(cache -> {
                    checkFailureForCoreException(Methods.GET_FILE_REFERENCE_CACHE_ASYNC);
                    return cache;
                });
    }

    @Override
    protected boolean deleteFileInternal(@Nonnull final Map<String, FDBLuceneFileReference> cache, @Nonnull final String name) throws IOException {
        checkFailureForCoreException(Methods.DELETE_FILE_INTERNAL);
        return super.deleteFileInternal(cache, name);
    }

    private void checkFailureForIoException(@Nonnull final Methods method) throws IOException {
        try {
            checkFailure(method);
        } catch (IOException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IllegalStateException("Expected IOException, got " + ex.getClass().getSimpleName(), ex);
        }
    }

    private void checkFailureForCoreException(@Nonnull final Methods method) throws RecordCoreException {
        try {
            checkFailure(method);
        } catch (RecordCoreException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new IllegalStateException("Expected RecordCoreException, got " + ex.getClass().getSimpleName(), ex);
        }
    }

    private void checkFailure(@Nonnull final Methods method) throws Exception {
        FailureDescription failureDescription = failureDescriptions.get(method);
        if (failureDescription != null) {
            AtomicLong count = invocationCounts.computeIfAbsent(method, m -> new AtomicLong(0));
            long invocations = count.incrementAndGet();
            if (invocations > failureDescription.getCount()) {
                throw failureDescription.getException();
            }
        }
    }


    private class FailureDescription {
        @Nonnull
        private Methods method;
        @Nonnull
        private Exception exception;
        private long count;

        public FailureDescription(@Nonnull final Methods method, @Nonnull final Exception exception, final long count) {
            this.method = method;
            this.exception = exception;
            this.count = count;
        }

        @Nonnull
        public Methods getMethod() {
            return method;
        }

        @Nonnull
        public Exception getException() {
            return exception;
        }

        public long getCount() {
            return count;
        }
    }
}
