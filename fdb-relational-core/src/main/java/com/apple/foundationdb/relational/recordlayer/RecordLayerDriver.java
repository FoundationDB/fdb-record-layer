/*
 * RecordLayerDriver.java
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

import com.apple.foundationdb.relational.api.DatabaseConnection;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.TransactionConfig;
import com.apple.foundationdb.relational.api.RelationalDriver;
import com.apple.foundationdb.relational.api.catalog.RelationalDatabase;
import com.apple.foundationdb.relational.api.exceptions.InvalidTypeException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import java.net.URI;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class RecordLayerDriver implements RelationalDriver {
    private final RecordLayerEngine engine;

    //internal constructor, use factory patterns instead
    RecordLayerDriver(RecordLayerEngine engine) {
        this.engine = engine;
    }

    @Override
    public DatabaseConnection connect(@Nonnull URI url, @Nullable Transaction existingTransaction, @Nonnull TransactionConfig transactionConfig, @Nonnull Options connectionOptions) throws RelationalException {
        /*
         * Basic Algorithm for Opening a connection:
         *
         * 1. Go to Catalog and verify that the given Database exists
         */
        RelationalDatabase frl = engine.getCatalog().getDatabase(url);
        assert frl instanceof RecordLayerDatabase : "Catalog does not produce RecordLayer Databases, use a different driver for type <" + frl.getClass() + ">";
        if (existingTransaction != null && !(existingTransaction instanceof RecordContextTransaction)) {
            throw new InvalidTypeException("Invalid Transaction type to use to connect to FDB");
        }
        RecordStoreConnection conn = new RecordStoreConnection((RecordLayerDatabase) frl, transactionConfig, (RecordContextTransaction) existingTransaction);
        ((RecordLayerDatabase) frl).setConnection(conn);
        return conn;
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean acceptsURL(URI url) {
        return engine.getScheme().equalsIgnoreCase(url.getScheme());
        // return url.startsWith(engine.getScheme());
    }

    /* private helper methods */
    private int parseFormatVersion(URI url, Options connectionOptions) {
        return 1;
    }
}
