/*
 * LazyCloseable.java
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

import com.google.common.base.Supplier;

import java.io.Closeable;
import java.io.IOException;

/**
 * Similar to {@link LazyOpener}, but that is also {@link Closeable}.
 * <p>
 *     Calling {@link #close()} on this object will not call the opener, and only close the stored value if it was
 *     already opened.
 * </p>
 * @param <T> the value opened
 */
public class LazyCloseable<T extends Closeable> implements Closeable {
    private final LazyOpener<T> lazyOpener;
    private boolean initialized;

    /**
     * Create a new {@link LazyCloseable} for the given {@link LazyOpener.Opener}.
     * <p>
     *     The {@code opener} will be called at most once; see {@link com.google.common.base.Suppliers#memoize(Supplier)}.
     * </p>
     * @param opener a function to "open" a resource
     * @param <U> the return type of the {@code opener}
     * @return a new lazily opened object
     */
    public static <U extends Closeable> LazyCloseable<U> supply(LazyOpener.Opener<U> opener) {
        return new LazyCloseable<>(opener);
    }

    private LazyCloseable(LazyOpener.Opener<T> opener) {
        this.initialized = false;
        this.lazyOpener = LazyOpener.supply(() -> {
            final T result = opener.open();
            initialized = true;
            return result;
        });
    }

    /**
     * Get the object returned by the {@code opener}.
     * @return the object returned by the {@code opener}
     * @throws IOException if calling {@link LazyOpener.Opener#open()} threw an {@link IOException}
     */
    public T get() throws IOException {
        return lazyOpener.get();
    }

    /**
     * Get the object returned by the {@code opener}, throwing {@link java.io.UncheckedIOException} if the
     * {@link LazyOpener.Opener#open()} method threw {@link IOException}.
     * @return the object returned by the {@code opener}
     */
    public T getUnchecked() {
        return lazyOpener.getUnchecked();
    }

    /**
     * Calls close on the result of the {@code opener} if it was already accessed through {@link #get} or
     * {@link #getUnchecked}.
     * @throws IOException if the underlying {@code close} throws {@link IOException}.
     */
    @Override
    public void close() throws IOException {
        if (initialized) {
            lazyOpener.get().close();
        }
    }

}
