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

import com.apple.foundationdb.Range;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContextConfig;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Create floating sub contexts from a caller context and commit when they reach time/write quota.
 */
public interface AgilityContext {

    static AgilityContext factory(FDBRecordContext callerContext, boolean useAgileContext) {
        return useAgileContext ? new Agile(callerContext) : new NonAgile(callerContext);
    }

    // `apply` should be called when returned value is expected
    <R> R apply(Function<FDBRecordContext, R> function) ;

    // `accept` should be called when returned value is not expected
    void accept(Consumer<FDBRecordContext> function);

    // `set` should be called for writes - keeping track of write size
    void set(byte[] key, byte[] value);

    void flush();

    default CompletableFuture<byte[]> get(byte[] key) {
        return apply(context -> context.ensureActive().get(key));
    }

    default void clear(byte[] key) {
        accept(context -> context.ensureActive().clear(key));
    }

    default void clear(Range range) {
        accept(context -> context.ensureActive().clear(range));
    }


    /**
     * A floating window (agile) context - create sub contexts and commit them as they reach time/size quota.
     */
    class Agile implements AgilityContext {

        final FDBRecordContextConfig.Builder contextConfigBuilder;
        final FDBDatabase database;
        FDBRecordContext currentContext;
        long creationTime;
        int currentWriteSize;

        // `apply` should be called when returned value is expected
        Agile(FDBRecordContext callerContext) {
            contextConfigBuilder = callerContext.getConfig().toBuilder();
            contextConfigBuilder.setWeakReadSemantics(null); // Since this context may be used for retries, do not allow week read semantic
            database = callerContext.getDatabase();
            callerContext.getOrCreateCommitCheck("FDBDirectory", name -> () -> CompletableFuture.runAsync(this::commitNow));
        }

        private long now() {
            return System.currentTimeMillis();
        }

        private void createIfNeeded() {
            if (currentContext == null) {
                FDBRecordContextConfig contextConfig = contextConfigBuilder.build();
                currentContext = database.openContext(contextConfig);
                creationTime = now();
            }
        }

        private boolean reachedTimeQuota() {
            return now() > creationTime + 3500;
        }

        private boolean reachedSizeQuota() {
            return currentWriteSize > 850_000;
        }

        private boolean shouldCommit() {
            return currentContext != null && (reachedSizeQuota() || reachedTimeQuota());
        }

        private void commitIfNeeded() {
            if (shouldCommit()) {
                commitNow();
            }
        }

        public synchronized void commitNow() {
            // This function is called:
            // 1. when time/size quota is reached.
            // 2. when object close or callerContext commit are called - the earlier of the two is the effective one.
            if (currentContext != null) {
                currentContext.commit();
                currentContext.close();
                currentContext = null;
                currentWriteSize = 0;
            }
        }

        @Override
        public <R> R apply(Function<FDBRecordContext, R> function) {
            createIfNeeded();
            R ret = function.apply(currentContext);
            commitIfNeeded();
            return ret;
        }

        // `accept` should be called when returned value is not expected
        @Override
        public void accept(final Consumer<FDBRecordContext> function) {
            createIfNeeded();
            function.accept(currentContext);
            commitIfNeeded();
        }

        @Override
        public void set(byte[] key, byte[] value) {
            accept(context -> context.ensureActive().set(key, value));
            if (currentContext != null) {
                currentWriteSize += key.length + value.length;
            }
        }

        @Override
        public void flush() {
            commitNow();
        }
    }

    /**
     * A non-agile context - plainly use caller's context as context and never commit.
     */
    class NonAgile implements AgilityContext {
        final FDBRecordContext callerContext;

        public NonAgile(final FDBRecordContext callerContext) {
            this.callerContext = callerContext;
        }

        // `apply` should be called when returned value is expected
        @Override
        public <R> R apply(Function<FDBRecordContext, R> function) {
            return function.apply(callerContext);
        }

        // `accept` should be called when returned value is not expected
        @Override
        public void accept(final Consumer<FDBRecordContext> function) {
            function.accept(callerContext);
        }

        @Override
        public void set(byte[] key, byte[] value) {
            accept(context -> context.ensureActive().set(key, value));
        }

        @Override
        public void flush() {
            // This is a no-op as the caller context should be committed by the caller.
        }
    }

}
