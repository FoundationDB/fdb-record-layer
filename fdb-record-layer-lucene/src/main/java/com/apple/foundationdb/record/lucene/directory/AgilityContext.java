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

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContextConfig;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Create floating sub contexts from a caller context and commit when they reach time/write quota.
 */
public class AgilityContext {
    final FDBRecordContextConfig.Builder contextConfigBuilder;
    final FDBDatabase database;
    final FDBRecordContext callerContext;
    FDBRecordContext currentContext;
    long creationTime;
    int currentWriteSize;
    final boolean useAgilityContext;

    AgilityContext(FDBRecordContext callerContext, boolean useAgilityContext) {
        this.callerContext = callerContext;
        this.useAgilityContext = useAgilityContext;
        contextConfigBuilder = callerContext.getConfig().toBuilder();
        contextConfigBuilder.setWeakReadSemantics(null); // Since this context may be used for retries, do not allow week read semantic
        database = callerContext.getDatabase();
        if (useAgilityContext) {
            callerContext.getOrCreateCommitCheck("FDBDirectory", name -> () -> CompletableFuture.runAsync(this::commitNow));
        }
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

    private boolean reachcedTimeQuota() {
        return now() > creationTime + 3500;
    }

    private boolean reachedSizeQuota() {
        return currentWriteSize > 850000;
    }

    private boolean shouldCommit() {
        return currentContext != null && (reachedSizeQuota() || reachcedTimeQuota());
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

    // `apply` should be called when returned value is expected
    public <R> R apply(Function<FDBRecordContext, R> function) {
        if (useAgilityContext) {
            createIfNeeded();
            R ret = function.apply(currentContext);
            commitIfNeeded();
            return ret;
        } else {
            return function.apply(callerContext);
        }
    }

    // `accept` should be called when returned value is not expected
    public void accept(final Consumer<FDBRecordContext> function) {
        if (useAgilityContext) {
            createIfNeeded();
            function.accept(currentContext);
            commitIfNeeded();
        } else {
            function.accept(callerContext);
        }
    }

    // `set` should be called for writes - keeping track of write size
    public void set(byte[] key, byte[] value) {
        accept(context -> context.ensureActive().set(key, value));
        if (currentContext != null) {
            currentWriteSize += key.length + value.length;
        }
    }
}
