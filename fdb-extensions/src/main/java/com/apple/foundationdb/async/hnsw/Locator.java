/*
 * Locator.java
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

package com.apple.foundationdb.async.hnsw;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.subspace.Subspace;
import com.google.common.base.Suppliers;

import javax.annotation.Nonnull;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * A basic wrapper around the fundamental access information we need to interact with an HNSW.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class Locator {
    @Nonnull
    private final Subspace subspace;
    @Nonnull
    private final Executor executor;
    @Nonnull
    private final Config config;
    @Nonnull
    private final OnWriteListener onWriteListener;
    @Nonnull
    private final OnReadListener onReadListener;

    @Nonnull
    private final Supplier<Primitives> primitivesSupplier;
    @Nonnull
    private final Supplier<Search> searchSupplier;
    @Nonnull
    private final Supplier<Insert> insertSupplier;
    @Nonnull
    private final Supplier<Delete> deleteSupplier;

    /**
     * Constructs a new HNSW graph instance.
     * <p>
     * This constructor initializes the HNSW graph with the necessary components for storage,
     * execution, configuration, and event handling. All parameters are mandatory and must not be null.
     *
     * @param subspace the {@link Subspace} where the graph data is stored.
     * @param executor the {@link Executor} service to use for concurrent operations.
     * @param config the {@link Config} object containing HNSW algorithm parameters.
     * @param onWriteListener a listener to be notified of write events on the graph.
     * @param onReadListener a listener to be notified of read events on the graph.
     *
     * @throws NullPointerException if any of the parameters are {@code null}.
     */
    @SuppressWarnings("this-escape")
    public Locator(@Nonnull final Subspace subspace,
                   @Nonnull final Executor executor,
                   @Nonnull final Config config,
                   @Nonnull final OnWriteListener onWriteListener,
                   @Nonnull final OnReadListener onReadListener) {
        this.subspace = subspace;
        this.executor = executor;
        this.config = config;
        this.onWriteListener = onWriteListener;
        this.onReadListener = onReadListener;
        this.primitivesSupplier = Suppliers.memoize(() -> new Primitives(this));
        this.searchSupplier = Suppliers.memoize(() -> new Search(this));
        this.insertSupplier = Suppliers.memoize(() -> new Insert(this));
        this.deleteSupplier = Suppliers.memoize(() -> new Delete(this));
    }

    /**
     * Gets the subspace associated with this object.
     *
     * @return the non-null subspace
     */
    @Nonnull
    public Subspace getSubspace() {
        return subspace;
    }

    /**
     * Get the executor used by this hnsw.
     * @return executor used when running asynchronous tasks
     */
    @Nonnull
    public Executor getExecutor() {
        return executor;
    }

    /**
     * Get this hnsw's configuration.
     * @return hnsw configuration
     */
    @Nonnull
    public Config getConfig() {
        return config;
    }

    /**
     * Get the on-write listener.
     * @return the on-write listener
     */
    @Nonnull
    public OnWriteListener getOnWriteListener() {
        return onWriteListener;
    }

    /**
     * Get the on-read listener.
     * @return the on-read listener
     */
    @Nonnull
    public OnReadListener getOnReadListener() {
        return onReadListener;
    }

    @Nonnull
    Primitives primitives() {
        return primitivesSupplier.get();
    }

    @Nonnull
    Search search() {
        return searchSupplier.get();
    }

    @Nonnull
    Insert insert() {
        return insertSupplier.get();
    }

    @Nonnull
    Delete delete() {
        return deleteSupplier.get();
    }
}
