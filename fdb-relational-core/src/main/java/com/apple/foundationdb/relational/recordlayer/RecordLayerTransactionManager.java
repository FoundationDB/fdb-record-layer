/*
 * RecordLayerTransactionManager.java
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

import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContextConfig;
import com.apple.foundationdb.record.provider.foundationdb.FDBStoreTimer;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.TransactionManager;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import javax.annotation.Nonnull;

public class RecordLayerTransactionManager implements TransactionManager {
    private final FDBDatabase fdbDb;
    private final FDBStoreTimer storeTimer;

    public RecordLayerTransactionManager(FDBDatabase fdbDb, FDBStoreTimer storeTimer) {
        this.fdbDb = fdbDb;
        this.storeTimer = storeTimer;
    }

    @Override
    public Transaction createTransaction(@Nonnull Options connectionOptions) throws RelationalException {
        return new RecordContextTransaction(fdbDb.openContext(getFDBRecordContextConfig(connectionOptions, storeTimer)));
    }

    @Override
    public void abort(Transaction txn) throws RelationalException {
        txn.abort();
    }

    @Override
    public void commit(Transaction txn) throws RelationalException {
        txn.commit();
    }

    private FDBRecordContextConfig getFDBRecordContextConfig(@Nonnull Options options, FDBStoreTimer storeTimer) {
        FDBRecordContextConfig.Builder builder = FDBRecordContextConfig.newBuilder()
                .setTimer(storeTimer);
        Long transactionTimeout = options.getOption(Options.Name.TRANSACTION_TIMEOUT);
        if (transactionTimeout != null) {
            builder.setTransactionTimeoutMillis(transactionTimeout);
        }
        return builder.build();
    }
}
