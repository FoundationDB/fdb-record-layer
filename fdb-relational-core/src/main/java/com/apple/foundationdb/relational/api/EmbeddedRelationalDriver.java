/*
 * EmbeddedRelationalDriver.java
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

package com.apple.foundationdb.relational.api;

import com.apple.foundationdb.relational.api.catalog.RelationalDatabase;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import java.net.URI;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class EmbeddedRelationalDriver implements RelationalDriver {
    private final EmbeddedRelationalEngine engine;

    public EmbeddedRelationalDriver(EmbeddedRelationalEngine engine) {
        this.engine = engine;
    }

    @Override
    public RelationalConnection connect(@Nonnull URI url,
                                      @Nullable Transaction existingTransaction,
                                      @Nonnull TransactionConfig transactionConfig,
                                      @Nonnull Options connectionOptions) throws RelationalException {
        //first, we decide which cluster this database belongs to

        RelationalDatabase frl = null;
        for (StorageCluster cluster : engine.getStorageClusters()) {
            frl = cluster.loadDatabase(url, connectionOptions);
            if (frl != null) {
                //we found the cluster!
                break;
            }
        }
        if (frl == null) {
            throw new RelationalException("Database <" + url.getPath() + "> does not exist", ErrorCode.DATABASE_NOT_FOUND);
        }
        return frl.connect(existingTransaction, transactionConfig);
    }

    @Override
    public boolean acceptsURL(URI url) {
        return "embed".equalsIgnoreCase(url.getScheme());
    }
}
