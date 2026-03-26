/*
 * CloseableUtils.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
 * Utility methods to help interact with {@link AutoCloseable} classes.
 **/
public class CloseableUtils {
    /**
     * A utility to close multiple {@link AutoCloseable} objects, preserving all the caught exceptions.
     * The method would attempt to close all closeables in order, even if some failed.
     * Note that {@link CloseException} is used to wrap any exception thrown during the closing process. The reason for
     * that is the compiler fails to compile a {@link AutoCloseable#close()} implementation that throws a generic
     * {@link Exception} (due to {@link InterruptedException} issue) - We therefore have to catch and wrap all exceptions.
     * @param closeables the given sequence of {@link AutoCloseable}
     * @throws CloseException in case any exception was caught during the process. The first exception will be added
     * as a {@code cause}. In case more than one exception was caught, it will be added as Suppressed.
     */
    @API(API.Status.INTERNAL)
    @SuppressWarnings("PMD.CloseResource")
    public static void closeAll(AutoCloseable... closeables) throws CloseException {
        CloseException accumulatedException = null;
        for (AutoCloseable closeable: closeables) {
            try {
                closeable.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (accumulatedException == null) {
                    accumulatedException = new CloseException(e);
                } else {
                    accumulatedException.addSuppressed(e);
                }
            } catch (Exception e) {
                if (accumulatedException == null) {
                    accumulatedException = new CloseException(e);
                } else {
                    accumulatedException.addSuppressed(e);
                }
            }
        }
        if (accumulatedException != null) {
            throw accumulatedException;
        }
    }

    /**
     * A utility to invoke multiple callbacks and return their results.
     * This method guarantees that each of the callbacks is invoked (regardless of whether previous calls threw an
     * exception).
     * The returned result holds the result of every successful call and an exception that holds every thrown
     * exception.
     *
     * @param callbacks the given callbacks to invoke
     *
     * @return a {@link InvokeResults} with a list of results returned from successful invocations and a {@link CloseException}
     * that holds the exceptions thrown during unsuccessful invocations
     */
    @API(API.Status.INTERNAL)
    @Nonnull
    public static <T> InvokeResults<T> invokeAll(List<Supplier<T>> callbacks) {
        List<T> results = new ArrayList<>(callbacks.size());
        CloseException accumulatedException = null;
        for (Supplier<T> callback : callbacks) {
            try {
                results.add(callback.get());
            } catch (Exception e) {
                if (accumulatedException == null) {
                    accumulatedException = new CloseException(e);
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
     *
     * @throws CompletionException with the accumulated exceptions if there are any
     */
    @API(API.Status.INTERNAL)
    @Nonnull
    public static <T> CompletableFuture<Void> invokeAllFutures(List<Supplier<CompletableFuture<T>>> callbacks) {
        final InvokeResults<CompletableFuture<T>> results = invokeAll(callbacks);
        final CompletableFuture<Void> whenAll = AsyncUtil.whenAll(results.getResults());
        return whenAll.handle((ignore, whenAllEx) -> {
            if ((whenAllEx == null) && (results.accumulatedException == null)) {
                // no exception was thrown in creation or invocation of callbacks
                return null;
            }
            // combine creation exception with invocation exception
            CloseException ex = results.getAccumulatedException();
            if (ex == null) {
                ex = new CloseException(whenAllEx);
            } else if (whenAllEx != null) {
                ex.addSuppressed(whenAllEx);
            }
            throw new CompletionException(ex);
        });
    }

    private CloseableUtils() {
        // prevent constructor from being called
    }

    public static class InvokeResults<T> {
        private final List<T> results;
        private final CloseException accumulatedException;

        public InvokeResults(@Nonnull List<T> results, @Nullable CloseException accumulatedException) {
            this.results = results;
            this.accumulatedException = accumulatedException;
        }

        public @Nonnull List<T> getResults() {
            return results;
        }

        public @Nullable CloseException getAccumulatedException() {
            return accumulatedException;
        }
    }
}
