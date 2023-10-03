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
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Class to lazily "open" something that may throw an IOException when opening.
 * @param <T> the type of object that the opener returns
 */
public class LazyOpener<T> {

    private final CompletableFuture<T> openerFuture;

    private LazyOpener(final CompletableFuture<T> openerFuture) {
        this.openerFuture = openerFuture;
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
        return new LazyOpener<>(CompletableFuture.supplyAsync(() -> {
            // This would run asynchronously in a different thread
            try {
                return opener.open();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }));
    }

    /**
     * Get the object returned by the {@code opener}.
     * @return the object returned by the {@code opener}
     * @throws IOException if calling {@link Opener#open()} threw an {@link IOException}
     */
    public T get() throws IOException {
        try {
            return openerFuture.get();
        } catch (UncheckedIOException e) {
            final IOException cause = e.getCause();
            cause.addSuppressed(e);
            throw cause;
        } catch (ExecutionException | InterruptedException e) {
            throw new RecordCoreException(e);
        }
    }

    /**
     * Get the object returned by the {@code opener}, throwing {@link UncheckedIOException} if the {@link Opener#open()}
     * method threw {@link IOException}.
     * @return the object returned by the {@code opener}
     */
    public T getUnchecked() {
        try {
            return openerFuture.get();
        } catch (ExecutionException | InterruptedException e) {
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
