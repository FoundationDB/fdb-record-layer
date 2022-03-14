/*
 * ImportRunner.java
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

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCursor;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public abstract class ImportRunner<T> implements LimittedRunner.Runner {
    private RecordCursor<T> cursor;
    private final FDBDatabaseRunner runner;
    private List<T> buffer;
    int position = 0;

    public ImportRunner(final RecordCursor<T> cursor, FDBDatabaseRunner runner) {
        this.cursor = cursor;
        this.runner = runner;
    }

    protected abstract void save(T value, FDBRecordContext context);

    @Override
    public CompletableFuture<Boolean> runAsync(int limit) {
        final CompletableFuture<Void> fillBufferFuture;
        if (buffer.size() < limit) {
            fillBufferFuture = cursor.limitRowsTo(limit - buffer.size())
                    .forEach(val -> buffer.add(val));
        } else {
            fillBufferFuture = CompletableFuture.completedFuture(null);
        }
        return fillBufferFuture.thenCompose(ignored -> {
            if (buffer.size() == 0) {
                return AsyncUtil.READY_FALSE;
            }
            return runner.runAsync(context -> {
                while (position < limit && position < buffer.size()) {
                    // TODO allow save to return a future
                    save(buffer.get(position), context);
                }
                return AsyncUtil.READY_TRUE;
            }).thenApply(result -> {
                // these entries were successfully saved, so remove them from the buffer
                buffer.subList(0, position).clear();
                return result;
            });
        });
    }
}
