/*
 * TransactionalLimitedRunner.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2022 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

@API(API.Status.EXPERIMENTAL)
public class TransactionalLimitedRunner extends LimitedRunner {

    private final TransactionalRunner transactionalRunner;
    private boolean closed;

    public TransactionalLimitedRunner(@Nonnull FDBDatabase database,
                                      FDBRecordContextConfig.Builder contextConfigBuilder,
                                      int maxLimit) {
        super(database.newContextExecutor(contextConfigBuilder.getMdcContext()), maxLimit);
        this.transactionalRunner = new TransactionalRunner(database, contextConfigBuilder);
    }

    public CompletableFuture<Void> runAsync(Runner runnable) {
        AtomicBoolean isFirst = new AtomicBoolean(true);
        return runAsync(limit ->
                runnable.prep(limit).thenCompose(vignore ->
                        transactionalRunner.runAsync(isFirst.getAndSet(false),
                                context -> runnable.runAsync(context, limit))));
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        transactionalRunner.close();
        super.close();
        this.closed = true;
    }

    public interface Runner {

        default CompletableFuture<Void> prep(int limit) {
            return AsyncUtil.DONE;
        }

        CompletableFuture<Boolean> runAsync(FDBRecordContext context, int limit);
    }
}
