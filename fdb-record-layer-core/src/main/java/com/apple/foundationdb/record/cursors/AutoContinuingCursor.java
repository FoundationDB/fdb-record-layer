/*
 * AutoContinuingCursor.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordCursorResult;
import com.apple.foundationdb.record.RecordCursorVisitor;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseRunner;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;

/**
 * A cursor that can iterate over a cursor across transactions.
 *
 * It is provided a generator that produces a cursor (referred to as <i>underlying cursor</i>). The <i>underlying
 * cursor</i> is iterated over until it is either exhausted or until one of the scan limits is reached:
 * <ul>
 *     <li>
 *         If the <i>underlying cursor</i> is exhausted, the {@link AutoContinuingCursor} is also exhausted.
 *     </li>
 *     <li>
 *         If scan limit properties of the <i>underlying cursor</i> are reached, the generator is asked for a new
 *         <i>underlying cursor</i> which (1) is in the context of a new transaction and (2) takes the continuation from
 *         the previous <i>underlying cursor</i>. Then the process is repeated.
 *     </li>
 * </ul>
 *
 * <p>
 * {@link AutoContinuingCursor} is responsible for all {@link FDBRecordContext} management, so all reads it does are in
 * the scope of transactions that it controls.
 * </p>
 *
 * <p>
 * The {@link AutoContinuingCursor} has no visibility into the {@link com.apple.foundationdb.record.ScanProperties} of
 * the <i>underlying cursor</i> and, therefore, will not be involved in enforcing any limits that may be individually
 * applied to the <i>underlying cursor</i>. For example, if the generator returns an <i>underlying cursor</i> that
 * specified, say, a record scan limit of 10 records, the {@link AutoContinuingCursor} will scan all data until it is
 * exhausted, at most 10 records at a transaction.
 * </p>
 *
 * @param <T> the type of elements returned by this cursor
 */
@API(API.Status.EXPERIMENTAL)
public class AutoContinuingCursor<T> implements RecordCursor<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AutoContinuingCursor.class);

    @Nonnull
    private FDBDatabaseRunner runner;
    @Nonnull
    private BiFunction<FDBRecordContext, byte[], RecordCursor<T>> nextCursorGenerator;

    @Nullable
    private RecordCursor<T> currentCursor;
    @Nullable
    private FDBRecordContext currentContext;

    @Nullable
    private CompletableFuture<Boolean> nextFuture;
    @Nullable
    private RecordCursorResult<T> lastResult;

    // for detecting incorrect cursor usage
    private boolean mayGetContinuation = false;

    /**
     * Creates a new {@link AutoContinuingCursor}.
     * @param runner the runner from which it can open new contexts
     * @param nextCursorGenerator the method which can generate the underlying cursor given a record context and a
     * continuation
     */
    public AutoContinuingCursor(@Nonnull FDBDatabaseRunner runner,
                                @Nonnull BiFunction<FDBRecordContext, byte[], RecordCursor<T>> nextCursorGenerator) {
        this.runner = runner;
        this.nextCursorGenerator = nextCursorGenerator;
    }

    @Nonnull
    @Override
    public CompletableFuture<RecordCursorResult<T>> onNext() {
        if (currentCursor == null) {
            openContextAndGenerateCursor(null);
        }
        return AsyncUtil.whileTrue(() ->
                        currentCursor.onNext().thenApply(result -> {
                            if (result.hasStoppedBeforeEnd()) {
                                openContextAndGenerateCursor(result.getContinuation().toBytes());
                                return true;
                            } else {
                                lastResult = result;
                                return false;
                            }
                        }), getExecutor())
                .thenApply(ignore -> lastResult);
    }

    @Nonnull
    @Override
    public RecordCursorResult<T> getNext() {
        return runner.asyncToSync(FDBStoreTimer.Waits.WAIT_ADVANCE_CURSOR, onNext());
    }

    private void openContextAndGenerateCursor(@Nullable byte[] continuation) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Open context and generate a cursor");
        }
        if (currentContext != null) {
            currentContext.close();
        }
        currentContext = runner.openContext();
        currentCursor = nextCursorGenerator.apply(currentContext, continuation);
    }

    @Nonnull
    @Override
    @Deprecated
    public CompletableFuture<Boolean> onHasNext() {
        if (nextFuture == null) {
            mayGetContinuation = false;
            nextFuture = onNext().thenApply(RecordCursorResult::hasNext);
        }
        return nextFuture;
    }

    @Nullable
    @Override
    @Deprecated
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        nextFuture = null;
        mayGetContinuation = true;
        return lastResult.get();
    }

    @Nullable
    @Override
    @Deprecated
    public byte[] getContinuation() {
        IllegalContinuationAccessChecker.check(mayGetContinuation);
        return lastResult.getContinuation().toBytes();
    }

    @Nonnull
    @Override
    @Deprecated
    public NoNextReason getNoNextReason() {
        return lastResult.getNoNextReason();
    }

    @Override
    public void close() {
        if (nextFuture != null) {
            nextFuture.cancel(true);
        }
        if (currentContext != null) {
            currentContext.close();
        }
    }

    @Nonnull
    @Override
    public Executor getExecutor() {
        return runner.getExecutor();
    }

    @Override
    public boolean accept(@Nonnull RecordCursorVisitor visitor) {
        if (visitor.visitEnter(this) && currentCursor != null) {
            currentCursor.accept(visitor);
        }
        return visitor.visitLeave(this);
    }
}
