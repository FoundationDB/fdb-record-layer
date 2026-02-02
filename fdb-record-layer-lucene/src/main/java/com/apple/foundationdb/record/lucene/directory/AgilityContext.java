/*
 * AgilityContext.java
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

package com.apple.foundationdb.record.lucene.directory;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.record.lucene.LuceneConcurrency;
import com.apple.foundationdb.record.provider.common.StoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContextConfig;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.record.provider.foundationdb.properties.RecordLayerPropertyKey;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Create floating sub contexts from a caller context and commit when they reach time/write quota.
 */
public interface AgilityContext {

    static AgilityContext nonAgile(FDBRecordContext callerContext) {
        return new NonAgileContext(callerContext);
    }

    static AgilityContext agile(FDBRecordContext callerContext, @Nullable FDBRecordContextConfig.Builder contextBuilder, final long timeQuotaMillis, final long sizeQuotaBytes) {
        return new AgileContext(callerContext, contextBuilder, timeQuotaMillis, sizeQuotaBytes);
    }

    static AgilityContext agile(FDBRecordContext callerContext, final long timeQuotaMillis, final long sizeQuotaBytes) {
        return agile(callerContext, null, timeQuotaMillis, sizeQuotaBytes);
    }

    static AgilityContext readOnlyNonAgile(FDBRecordContext callerContext, @Nullable FDBRecordContextConfig.Builder contextBuilder) {
        return new ReadOnlyNonAgileContext(callerContext, contextBuilder);
    }

    /**
     * `apply` should be called when a returned value is expected. Performed under appropriate lock.
     * @param function a function accepting context, returning a future
     * @return the future of the function above
     * @param <R> future's type
     */
    <R> CompletableFuture<R> apply(Function<FDBRecordContext, CompletableFuture<R>> function) ;


    /**
     * This function is similar to {@link #apply(Function)}, but should only be called in the recovery path. In
     * Agile mode, it creates a new context for the function's apply to allow recovery, in non-Agile mode it's a
     * no-op. This function may be called after agility context
     * was closed.
     * @param function a function accepting context, returning a future
     * @return the future of the function above
     * @param <R> future's type
     */
    <R> CompletableFuture<R> applyInRecoveryPath(Function<FDBRecordContext, CompletableFuture<R>> function) ;

    /**
     * `accept` should be called when a returned value is not expected. Performed under appropriate lock.
     * @param function a function that accepts context
     */
    void accept(Consumer<FDBRecordContext> function);

    /**
     * `set` should be called for writes - keeping track of write size (if needed).
     * @param key key
     * @param value value
     */
    void set(byte[] key, byte[] value);

    void flush();

    /**
     * This can be called to declare the object as 'closed' after flush. Future "flush" will succeed, but any attempt
     * to perform a transaction read/write will cause an exception.
     */
    void flushAndClose();

    /**
     * This will abort the existing agile context (if agile) and prevent future operations. The only reason to
     * call this function is after an exception, by a wrapper function.
     * to clean potential locks after a failure and close, one should use {@link #applyInRecoveryPath(Function)}
     */
    void abortAndClose();

    boolean isClosed();

    default CompletableFuture<byte[]> get(byte[] key) {
        return apply(context -> context.ensureActive().get(key));
    }

    default void clear(byte[] key) {
        accept(context -> context.ensureActive().clear(key));
    }

    default void clear(Range range) {
        accept(context -> context.clear(range));
    }

    default CompletableFuture<List<KeyValue>> getRange(byte[] begin, byte[] end) {
        return apply(context -> context.ensureActive().getRange(begin, end).asList());
    }

    /**
     * This function returns the caller's context. If called by external entities, it should only be used for testing.
     * @return caller's context
     */
    @Nonnull
    FDBRecordContext getCallerContext();

    default <T> CompletableFuture<T> instrument(StoreTimer.Event event,
                                                CompletableFuture<T> future ) {
        return getCallerContext().instrument(event, future);
    }

    default <T> CompletableFuture<T> instrument(StoreTimer.Event event,
                                                CompletableFuture<T> future,
                                                long start) {
        return getCallerContext().instrument(event, future, start);
    }

    default void increment(@Nonnull StoreTimer.Count count) {
        getCallerContext().increment(count);
    }

    default void increment(@Nonnull StoreTimer.Count count, int size) {
        getCallerContext().increment(count, size);
    }

    default void recordEvent(@Nonnull StoreTimer.Event event, long timeDelta) {
        getCallerContext().record(event, timeDelta);
    }

    default void recordSize(@Nonnull StoreTimer.SizeEvent sizeEvent, long size) {
        getCallerContext().recordSize(sizeEvent, size);
    }

    @Nullable
    default <T> T asyncToSync(StoreTimer.Wait event, @Nonnull CompletableFuture<T> async) {
        return LuceneConcurrency.asyncToSync(event, async, getCallerContext());
    }

    @Nullable
    default <T> T getPropertyValue(@Nonnull RecordLayerPropertyKey<T> propertyKey) {
        return getCallerContext().getPropertyStorage().getPropertyValue(propertyKey);
    }

    /**
     * The {@code commitCheck} callback is expected to use the argument context but should never call other
     * agility context functions as it may cause a deadlock.
     *
     * @param commitCheck callback
     */
    void setCommitCheck(Function<FDBRecordContext, CompletableFuture<Void>> commitCheck);

    default void commit(@Nonnull FDBRecordContext context) {
        LuceneConcurrency.asyncToSync(FDBStoreTimer.Waits.WAIT_COMMIT, context.commitAsync(), context);
    }

}
