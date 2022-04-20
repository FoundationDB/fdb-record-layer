/*
 * StorageCluster.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.recordlayer;

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.TransactionManager;
import com.apple.foundationdb.relational.api.catalog.RelationalDatabase;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import java.net.URI;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A Representation of a cluster of storage servers (either FDB, or some theoretical alternative).
 *
 * A cluster of storage servers needs to provide two mechanisms:
 * 1. The ability to create many databases in an efficient multi-tenant way(e.g. {@link #loadDatabase(URI, Options)})
 * 2. Have a transaction system which can be used across all databases within the same cluster. (Note that it is not
 * necessary for transactions to span multiple StorageCluster systems; As of April 2022, this is not even possible).
 */
public interface StorageCluster {
    /**
     * Load the specified database, if it is located within this cluster.
     *
     * If the specified database does not exist in this cluster, {@code null} is returned.
     *
     * //TODO(bfines) ensure implementations actually hold to that spec.
     * @param url the path to the database of interest.
     * @param connOptions options to use when connecting
     * @return the Database at the specified location, or {@code null} if no such database exist within this cluster.
     * @throws RelationalException if something goes wrong.
     */
    @Nullable
    RelationalDatabase loadDatabase(@Nonnull URI url,
                                  @Nonnull Options connOptions) throws RelationalException;

    /**
     * Get the transaction manager for this cluster.
     *
     * @return the transaction manager for this cluster;
     */
    TransactionManager getTransactionManager();
}
