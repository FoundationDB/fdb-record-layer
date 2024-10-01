/*
 * LazyOpener.java
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

package com.apple.foundationdb.record.lucene.codec;

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.lucene.LuceneExceptions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

/**
 * Class to lazily "open" something that may throw an IOException when opening.
 * @param <T> the type of object that the opener returns
 */
public class LazyOpener<T> {

    private final CompletableFuture<Void> starter;
    private final CompletableFuture<T> future;

    /**
     * Executor that runs whatever it is given in the current thread.
     */
    private static final Executor executor = Runnable::run;

    private LazyOpener(final Opener<T> opener) {
        // Empty future that will gate the completion of the opener task
        starter = new CompletableFuture<>();
        future = starter.thenApplyAsync(ignored -> {
            try {
                return opener.open();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }, executor);
    }

    /**
     * Create a new {@link LazyOpener} for the given {@link LazyOpener.Opener}.
     * <p>
     *     The {@code opener} will be called at most once; see {@link Suppliers#memoize(Supplier)}.
     * </p>
     * @param <U> the return type of the {@code opener}
     * @param opener a function to "open" a resource
     * @return a new lazily opened object
     */
    public static <U> LazyOpener<U> supply(LazyOpener.Opener<U> opener) {
        return new LazyOpener<U>(opener);
    }

    /**
     * Get the object returned by the {@code opener}.
     * @return the object returned by the {@code opener}
     * @throws IOException if calling {@link Opener#open()} threw an {@link IOException}
     */
    @SuppressWarnings("PMD.PreserveStackTrace")
    public T get() throws IOException {
        try {
            return getInternal();
        } catch (ExecutionException e) {
            final Throwable outerCause = e.getCause();
            if (outerCause instanceof UncheckedIOException) {
                final IOException innerCause = ((UncheckedIOException)outerCause).getCause();
                innerCause.addSuppressed(e);
                throw innerCause;
            } else {
                throw LuceneExceptions.toIoException(outerCause, e);
            }
        }
    }

    /**
     * Get the object returned by the {@code opener}, throwing {@link UncheckedIOException} if the {@link Opener#open()}
     * method threw {@link IOException}.
     * @return the object returned by the {@code opener}
     */
    @SuppressWarnings("PMD.PreserveStackTrace")
    public T getUnchecked() {
        try {
            return getInternal();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                // Runtime exception - just rethrow - for example, RecordCoreException, UncheckedIOException
                cause.addSuppressed(e);
                throw (RuntimeException)cause;
            } else if (cause instanceof IOException) {
                // Try to unwrap the cause for the IOException
                throw LuceneExceptions.toRecordCoreException(cause.getMessage(), (IOException)cause);
            } else {
                // Otherwise, wrap with generic RecordCoreException
                throw new RecordCoreException(cause);
            }
        }
    }

    private T getInternal() throws ExecutionException {
        // Once "opening" is required, complete the starter future to initiate the realization of the future
        starter.complete(null);
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RecordCoreException(e);
        }
    }

    /**
     * A function that returns an object, but may throw an {@link IOException}.
     * @param <T> the type returned
     */
    @FunctionalInterface
    public interface Opener<T> {
        @Nonnull
        T open() throws IOException;
    }
}
