/*
 * NonAgileContext.java
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

import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A non-agile context - plainly use caller's context as context and never commit.
 */
public class NonAgileContext implements AgilityContext {
    private final FDBRecordContext callerContext;
    private boolean closed = false;

    public NonAgileContext(final FDBRecordContext callerContext) {
        this.callerContext = callerContext;
    }

    @Override
    public <R> CompletableFuture<R> apply(Function<FDBRecordContext, CompletableFuture<R>> function) {
        ensureOpen();
        return function.apply(callerContext);
    }

    @Override
    public <R> CompletableFuture<R> applyInRecoveryPath(Function<FDBRecordContext, CompletableFuture<R>> function) {
        // Best effort - skip ensureOpen, ignore exceptions.
        return function.apply(callerContext).exceptionally(ex -> null);
    }

    @Override
    public void accept(final Consumer<FDBRecordContext> function) {
        ensureOpen();
        function.accept(callerContext);
    }

    @Override
    public void set(byte[] key, byte[] value) {
        accept(context -> context.ensureActive().set(key, value));
    }

    @Override
    @Nonnull
    public FDBRecordContext getCallerContext() {
        return callerContext;
    }

    private void ensureOpen() {
        if (closed) {
            throw new RecordCoreStorageException("NonAgile context is already closed");
        }
    }

    @Override
    public void flush() {
        // This is a no-op as the caller context should be committed by the caller.
    }

    @Override
    public void flushAndClose() {
        closed = true;
    }

    @Override
    public void abortAndClose() {
        // Nothing is aborted because the caller context should be handled by the caller.
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void setCommitCheck(final Function<FDBRecordContext, CompletableFuture<Void>> commitCheck) {
        callerContext.addCommitCheck(() -> commitCheck.apply(callerContext));
    }
}
