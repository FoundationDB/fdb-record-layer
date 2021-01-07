/*
 * IteratorCursor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.cursors;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.record.RecordCursorResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.Nonnull;
import java.io.Closeable;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * A cursor that returns the elements of an ordinary synchronous iterator.
 * While it supports continuation, resuming an iterator cursor with a continuation is very inefficient since it needs to
 * advance the underlying iterator up to the point that stopped. For that reason, the {@link ListCursor} should be used
 * instead where possible.
 * @param <T> the type of elements of the cursor
 */
@API(API.Status.MAINTAINED)
public class IteratorCursor<T> extends IteratorCursorBase<T, Iterator<T>> {
    private static final Logger LOG = LoggerFactory.getLogger(IteratorCursor.class);
    private Closeable closeable;
    public IteratorCursor(@Nonnull Executor executor, @Nonnull Iterator<T> iterator) {
        super(executor, iterator);
    }

    public IteratorCursor(@Nonnull Executor executor, @Nonnull Iterator<T> iterator, Closeable closeable) {
        super(executor, iterator);
        this.closeable = closeable;
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<T>> onNext() {
        return CompletableFuture.completedFuture(computeNextResult(iterator.hasNext()));
    }

    @Override
    public void close() {
        try {
            if (closeable != null)
                closeable.close();
        } catch (Exception ioe) {
            LOG.warn("close failed",ioe);
        }
        super.close();
    }
}
