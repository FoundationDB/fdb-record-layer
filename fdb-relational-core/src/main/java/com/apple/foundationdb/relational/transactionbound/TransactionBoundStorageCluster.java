/*
 * TransactionBoundStorageCluster.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.transactionbound;

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.StorageCluster;
import com.apple.foundationdb.relational.api.TransactionManager;
import com.apple.foundationdb.relational.api.catalog.RelationalDatabase;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.HollowTransactionManager;
import com.apple.foundationdb.relational.recordlayer.catalog.TransactionBoundDatabase;
import com.apple.foundationdb.relational.recordlayer.query.cache.RelationalPlanCache;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;

/**
 * A thin implementation of {@link StorageCluster} creates a new {@link TransactionBoundDatabase}
 * when {@link #loadDatabase(URI, Options)} is called.
 */
@API(API.Status.EXPERIMENTAL)
public class TransactionBoundStorageCluster implements StorageCluster {
    @Nullable
    private final RelationalPlanCache planCache;

    public TransactionBoundStorageCluster(@Nullable RelationalPlanCache planCache) {
        this.planCache = planCache;
    }

    @Nullable
    @Override
    public RelationalDatabase loadDatabase(@Nonnull URI url, @Nonnull Options connOptions) throws RelationalException {
        return new TransactionBoundDatabase(url, connOptions, planCache);
    }

    @Override
    public TransactionManager getTransactionManager() {
        return HollowTransactionManager.INSTANCE;
    }
}
