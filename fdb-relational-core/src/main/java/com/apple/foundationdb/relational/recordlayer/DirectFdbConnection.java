/*
 * DirectFdbConnection.java
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

import com.apple.foundationdb.record.RecordCoreException;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.relational.api.TransactionConfig;
import com.apple.foundationdb.relational.api.TransactionManager;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.recordlayer.util.ExceptionUtil;

public class DirectFdbConnection implements FdbConnection {
    private final FDBDatabase fdb;
    private final TransactionManager txnManager;

    public DirectFdbConnection(FDBDatabase fdb) {
        this(fdb, TransactionConfig.DEFAULT, new FDBStoreTimer());
    }

    public DirectFdbConnection(FDBDatabase fdb, FDBStoreTimer storeTimer) {
        this(fdb, TransactionConfig.DEFAULT, storeTimer);
    }

    public DirectFdbConnection(FDBDatabase fdb, TransactionConfig config, FDBStoreTimer storeTimer) {
        this.fdb = fdb;
        this.txnManager = new RecordLayerTransactionManager(fdb, config, storeTimer);
    }

    public static DirectFdbConnection connect(String clusterFile) {
        return new DirectFdbConnection(FDBDatabaseFactory.instance().getDatabase(clusterFile));
    }

    @Override
    public TransactionManager getTransactionManager() {
        return txnManager;
    }

    @Override
    public void close() throws RelationalException {
        try {
            fdb.close();
        } catch (RecordCoreException rce) {
            throw ExceptionUtil.toRelationalException(rce);
        }
    }
}
