/*
 * ReadOnlyNonAgileContext.java
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.RecordCoreStorageException;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContextConfig;
import com.apple.foundationdb.record.provider.foundationdb.PreventCommitCheck;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A non-agile context that creates a read-only transaction.
 * This AgilityContext creates a read-only usable context using the given callerContext's GRV (so it ends up using
 * the same transaction start point), and makes the usable context read-only (commit will fail).
 * The created context is closed once this instance is closed, but the caller context is not.
 */
public class ReadOnlyNonAgileContext implements AgilityContext {
    private final FDBRecordContext callerContext;
    private final FDBRecordContext readOnlyContext;
    private boolean closed = false;

    public ReadOnlyNonAgileContext(final FDBRecordContext callerContext, @Nullable FDBRecordContextConfig.Builder contextBuilder) {
        this.callerContext = callerContext;

        FDBRecordContextConfig.Builder contextConfigBuilder = contextBuilder != null ? contextBuilder : callerContext.getConfig().toBuilder();
        final FDBRecordContextConfig contextConfig = contextConfigBuilder.build();
        readOnlyContext = callerContext.getDatabase().openContext(contextConfig);
        // Use the same read version for the new context
        readOnlyContext.setReadVersion(callerContext.getReadVersion());
        // reject all commit attempts for the read-only context
        readOnlyContext.addCommitCheck("ReadOnlyAgilityContextCommitCheck",
                new PreventCommitCheck(() -> new RecordCoreException("Commit failed since the agility context is a read-only one.")));
    }

    @Override
    public <R> CompletableFuture<R> apply(Function<FDBRecordContext, CompletableFuture<R>> function) {
        ensureOpen();
        return function.apply(readOnlyContext);
    }

    @Override
    public <R> CompletableFuture<R> applyInRecoveryPath(Function<FDBRecordContext, CompletableFuture<R>> function) {
        // Best effort - skip ensureOpen, ignore exceptions.
        return function.apply(readOnlyContext).exceptionally(ex -> null);
    }

    @Override
    public void accept(final Consumer<FDBRecordContext> function) {
        ensureOpen();
        function.accept(readOnlyContext);
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
            throw new RecordCoreStorageException("ReadOnlyNonAgile context is already closed");
        }
    }

    @Override
    public void flush() {
        throw new RecordCoreStorageException("ReadOnlyNonAgile context should not be committed");
    }

    @Override
    public void flushAndClose() {
        readOnlyContext.close();
        closed = true;
        throw new RecordCoreStorageException("ReadOnlyNonAgile context should not be committed");
    }

    @Override
    public void abortAndClose() {
        readOnlyContext.close();
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void setCommitCheck(final Function<FDBRecordContext, CompletableFuture<Void>> commitCheck) {
        readOnlyContext.addCommitCheck(() -> commitCheck.apply(readOnlyContext));
    }
}
