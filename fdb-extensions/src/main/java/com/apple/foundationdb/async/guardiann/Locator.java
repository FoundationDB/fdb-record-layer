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

package com.apple.foundationdb.async.guardiann;

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.subspace.Subspace;
import com.google.common.base.Suppliers;

import java.util.concurrent.Executor;
import java.util.function.Supplier;

/**
 * Wires up and holds the shared collaborators of a Guardiann structure: its {@link StorageAdapter}, the
 * {@link Executor}, and the lazily-created {@link Primitives}, {@link Search}, {@link Insert} and
 * {@link Delete} operation objects.
 */
@API(API.Status.EXPERIMENTAL)
@SuppressWarnings("checkstyle:AbbreviationAsWordInName")
public final class Locator {
    private final Executor executor;
    private final StorageAdapter storageAdapter;

    private final Supplier<Primitives> primitivesSupplier = Suppliers.memoize(() -> new Primitives(this));
    private final Supplier<Search> searchSupplier = Suppliers.memoize(() -> new Search(this));
    private final Supplier<Insert> insertSupplier = Suppliers.memoize(() -> new Insert(this));
    private final Supplier<Delete> deleteSupplier = Suppliers.memoize(() -> new Delete(this));

    /**
     * Constructs a new {@code Locator}.
     * <p>
     * Initializes the Locator with the storage adapter (built from the given subspace, config and listeners) and
     * the executor used for the Guardiann structure's asynchronous operations. All parameters are mandatory and
     * must not be null.
     *
     * @param subspace the {@link Subspace} where the data is stored.
     * @param executor the {@link Executor} service to use for concurrent operations.
     * @param config the {@link Config} containing the Guardiann parameters.
     * @param onWriteListener a listener to be notified of write events.
     * @param onReadListener a listener to be notified of read events.
     *
     * @throws NullPointerException if any of the parameters are {@code null}.
     */
    public Locator(final Subspace subspace,
                   final Executor executor,
                   final Config config,
                   final OnWriteListener onWriteListener,
                   final OnReadListener onReadListener) {
        this.executor = executor;
        this.storageAdapter = new StorageAdapter(config, subspace, onWriteListener, onReadListener);
    }

    StorageAdapter getStorageAdapter() {
        return storageAdapter;
    }

    /**
     * Gets the subspace associated with this object.
     *
     * @return the non-null subspace
     */
    public Subspace getSubspace() {
        return getStorageAdapter().getSubspace();
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
        return getStorageAdapter().getConfig();
    }

    /**
     * Get the on-write listener.
     * @return the on-write listener
     */
    public OnWriteListener getOnWriteListener() {
        return getStorageAdapter().getOnWriteListener();
    }

    /**
     * Get the on-read listener.
     * @return the on-read listener
     */
    public OnReadListener getOnReadListener() {
        return getStorageAdapter().getOnReadListener();
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
