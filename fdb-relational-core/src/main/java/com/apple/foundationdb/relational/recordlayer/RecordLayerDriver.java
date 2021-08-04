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
import com.apple.foundationdb.relational.api.InvalidTypeException;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalDriver;
import com.apple.foundationdb.relational.api.RelationalException;
import com.apple.foundationdb.relational.api.catalog.Catalog;
import com.apple.foundationdb.relational.api.catalog.RelationalDatabase;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;

public class RecordLayerDriver implements RelationalDriver {
    private final Catalog dataCatalog;

    //internal constructor, use factory patterns instead
    RecordLayerDriver(Catalog dataCatalog) {
        this.dataCatalog = dataCatalog;
    }

    @Override
    public DatabaseConnection connect(@Nonnull URI url, @Nonnull Options connectionOptions) throws RelationalException {
        return connect(url,null,connectionOptions);
    }

    @Override
    public DatabaseConnection connect(@Nonnull URI url,  @Nullable Transaction existingTransaction,@Nonnull Options connectionOptions) throws RelationalException {
        int formatVersion = parseFormatVersion(url,connectionOptions);
        /*
         * Basic Algorithm for Opening a connection:
         *
         * 1. Go to Catalog and verify that the given Database exists
         */
        RelationalDatabase frl = dataCatalog.getDatabase(url);
        assert frl instanceof RecordLayerDatabase: "Catalog does not produce RecordLayer Databases, use a different driver for type <"+frl.getClass()+">";
        if (existingTransaction != null && !(existingTransaction instanceof RecordContextTransaction)) {
            throw new InvalidTypeException("Invalid Transaction type to use to connect to FDB");
        }
        RecordStoreConnection conn =  new RecordStoreConnection((RecordLayerDatabase)frl, (RecordContextTransaction) existingTransaction);
        ((RecordLayerDatabase)frl).setConnection(conn);
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

    /*private helper methods*/
    private int parseFormatVersion(URI url, Options connectionOptions) {
        return 1;
    }
}
