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

import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * A basic wrapper around the fundamental access information we need to interact with an HNSW.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public class Locator {
    private final Subspace subspace;
    private final Executor executor;
    private final Config config;
    private final OnWriteListener onWriteListener;
    private final OnReadListener onReadListener;

    @SuppressWarnings("this-escape")
    private final Supplier<Primitives> primitivesSupplier = Suppliers.memoize(() -> new Primitives(this));
    @SuppressWarnings("this-escape")
    private final Supplier<Search> searchSupplier = Suppliers.memoize(() -> new Search(this));
    @SuppressWarnings("this-escape")
    private final Supplier<Insert> insertSupplier = Suppliers.memoize(() -> new Insert(this));
    @SuppressWarnings("this-escape")
    private final Supplier<Delete> deleteSupplier = Suppliers.memoize(() -> new Delete(this));

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
    public Locator(final Subspace subspace,
                   final Executor executor,
                   final Config config,
                   final OnWriteListener onWriteListener,
                   final OnReadListener onReadListener) {
        this.subspace = subspace;
        this.executor = executor;
        this.config = config;
        this.onWriteListener = onWriteListener;
        this.onReadListener = onReadListener;

    }

    /**
     * Gets the subspace associated with this object.
     *
     * @return the non-null subspace
     */
    public Subspace getSubspace() {
        return subspace;
    }

    /**
     * Get the executor used by this hnsw.
     * @return executor used when running asynchronous tasks
     */
    public Executor getExecutor() {
        return executor;
    }

    /**
     * Get this hnsw's configuration.
     * @return hnsw configuration
     */
    public Config getConfig() {
        return config;
    }

    /**
     * Get the on-write listener.
     * @return the on-write listener
     */
    public OnWriteListener getOnWriteListener() {
        return onWriteListener;
    }

    /**
     * Get the on-read listener.
     * @return the on-read listener
     */
    public OnReadListener getOnReadListener() {
        return onReadListener;
    }

    Primitives primitives() {
        return primitivesSupplier.get();
    }

    Search search() {
        return searchSupplier.get();
    }

    Insert insert() {
        return insertSupplier.get();
    }

    Delete delete() {
        return deleteSupplier.get();
    }
}
