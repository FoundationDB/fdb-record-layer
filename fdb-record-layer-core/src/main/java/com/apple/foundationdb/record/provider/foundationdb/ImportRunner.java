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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public abstract class ImportRunner<T> implements LimitedRunner.Runner {
    private final Iterator<T> source;
    private final FDBDatabaseRunner runner;
    private List<T> buffer;
    int position = 0;

    public ImportRunner(final Iterator<T> source, FDBDatabaseRunner runner) {
        this.source = source;
        this.runner = runner;
        buffer = new ArrayList<>();
    }

    protected abstract CompletableFuture<Void> save(T value, FDBRecordContext context);

    @Override
    public CompletableFuture<Boolean> runAsync(int limit) {
        fillBuffer(limit);
        if (buffer.size() == 0) {
            return AsyncUtil.READY_FALSE;
        }
        return runner.runAsync(context -> {
            while (position < limit && position < buffer.size()) {
                save(buffer.get(position), context);
            }
            return AsyncUtil.READY_TRUE;
        }).thenApply(result -> {
            // these entries were successfully saved, so remove them from the buffer
            buffer.subList(0, position).clear();
            // fill the buffer back up, and check if there's anything left
            fillBuffer(limit);
            return buffer.size() > 0;
        });
    }

    private void fillBuffer(final int limit) {
        while (buffer.size() < limit && source.hasNext()) {
            buffer.add(source.next());
        }
    }
}
