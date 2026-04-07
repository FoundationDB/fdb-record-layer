/*
 * CallbackUtils.java
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

package com.apple.foundationdb.util;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.async.AsyncUtil;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

/**
 * A utility to invoke a collection of callbacks with an effort to ensure all are called when facing errors.
 */
public class CallbackUtils {
    /**
     * A utility to invoke multiple callbacks and return their results.
     * This method guarantees that each of the callbacks is invoked (regardless of whether previous calls threw an
     * exception).
     * The returned result holds the result of every successful call and an exception that holds every thrown
     * exception (null if none).
     *
     * @param callbacks the given callbacks to invoke
     *
     * @return an {@link InvokeResults} with a list of results returned from successful invocations and a {@link CallbackException}
     * that holds the exceptions thrown during unsuccessful invocations
     */
    @API(API.Status.INTERNAL)
    @Nonnull
    public static <T> InvokeResults<T> invokeAll(@Nonnull List<Supplier<T>> callbacks) {
        List<T> results = new ArrayList<>(callbacks.size());
        CallbackException accumulatedException = null;
        for (Supplier<T> callback : callbacks) {
            try {
                results.add(callback.get());
            } catch (RuntimeException e) {
                if (accumulatedException == null) {
                    accumulatedException = new CallbackException(e);
                } else {
                    accumulatedException.addSuppressed(e);
                }
            }
        }
        return new InvokeResults<>(results, accumulatedException);
    }

    /**
     * A utility to invoke multiple suppliers of {@link CompletableFuture} and wait for the future's completion.
     * This method guarantees that each of the callbacks is invoked and that each of the futures is accounted for
     * regardless of whether they succeed or fail.
     *
     * @param callbacks the given sequence of callbacks
     *
     * @return a future that completes when all the produced futures complete. The returned future's result is
     * <code>CompletedSuccessfully</code> if all futures were created and completed successfully, or an exception that
     * holds all the exceptions that were thrown during the process
     */
    @API(API.Status.INTERNAL)
    @Nonnull
    public static <T> CompletableFuture<Void> invokeAllFutures(@Nonnull List<Supplier<CompletableFuture<T>>> callbacks) {
        if (callbacks.isEmpty()) {
            return AsyncUtil.DONE;
        }
        final InvokeResults<CompletableFuture<T>> results = invokeAll(callbacks);
        final CompletableFuture<Void> whenAll = AsyncUtil.whenAll(results.getResults());
        return whenAll.handle((ignore, whenAllEx) -> {
            if ((whenAllEx == null) && (results.accumulatedException == null)) {
                // no exception was thrown in creation or invocation of callbacks
                return null;
            }
            // combine creation exception with invocation exception
            CallbackException ex = results.getAccumulatedException();
            if (ex == null) {
                ex = new CallbackException(whenAllEx);
            } else if (whenAllEx != null) {
                ex.addSuppressed(whenAllEx);
            }
            throw new CompletionException(ex);
        });
    }

    public static class InvokeResults<T> {
        @Nonnull
        private final List<T> results;
        @Nullable
        private final CallbackException accumulatedException;

        public InvokeResults(@Nonnull List<T> results, @Nullable CallbackException accumulatedException) {
            this.results = results;
            this.accumulatedException = accumulatedException;
        }

        @Nonnull
        public List<T> getResults() {
            return results;
        }

        @Nullable
        public CallbackException getAccumulatedException() {
            return accumulatedException;
        }
    }

    private CallbackUtils() {
    }
}
