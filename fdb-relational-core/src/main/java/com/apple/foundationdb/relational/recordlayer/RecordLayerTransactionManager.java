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
import com.apple.foundationdb.record.provider.foundationdb.FDBTransactionPriority;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.TransactionConfig;
import com.apple.foundationdb.relational.api.TransactionManager;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import javax.annotation.Nullable;

public class RecordLayerTransactionManager implements TransactionManager {
    private final FDBDatabase fdbDb;
    private final TransactionConfig defaultConfig;
    private final FDBStoreTimer storeTimer;

    public RecordLayerTransactionManager(FDBDatabase fdbDb, TransactionConfig defaultConfig, FDBStoreTimer storeTimer) {
        this.fdbDb = fdbDb;
        this.defaultConfig = defaultConfig;
        this.storeTimer = storeTimer;
    }

    @Override
    public Transaction createTransaction(@Nullable TransactionConfig config) throws RelationalException {
        return new RecordContextTransaction(fdbDb.openContext(getFDBRecordContextConfig(config == null ? this.defaultConfig : config, storeTimer)));
    }

    @Override
    public void abort(Transaction txn) throws RelationalException {
        txn.abort();
    }

    @Override
    public void commit(Transaction txn) throws RelationalException {
        txn.commit();
    }

    private FDBRecordContextConfig getFDBRecordContextConfig(@Nullable TransactionConfig config, FDBStoreTimer storeTimer) throws RelationalException {
        if (config != null) {
            TransactionConfig.WeakReadSemantics weakReadSemantics = config.getWeakReadSemantics();
            return FDBRecordContextConfig.newBuilder()
                    .setTransactionId(config.getTransactionId())
                    .setMdcContext(config.getLoggingContext())
                    .setPriority(getPriorityForFDB(config.getTransactionPriority()))
                    .setWeakReadSemantics(weakReadSemantics == null ? null :
                            new FDBDatabase.WeakReadSemantics(weakReadSemantics.getMinVersion(),
                                    weakReadSemantics.getStalenessBoundMillis(), weakReadSemantics.isCausalReadRisky()))
                    .setTransactionTimeoutMillis(config.getTransactionTimeoutMillis())
                    .setEnableAssertions(config.isEnableAssertions())
                    .setLogTransaction(config.isLogTransaction())
                    .setTrackOpen(config.isTrackOpen())
                    .setSaveOpenStackTrace(config.isSaveOpenStackTrace())
                    .setTimer(storeTimer)
                    .build();
        } else {
            return FDBRecordContextConfig.newBuilder()
                    .setTimer(storeTimer)
                    .setPriority(FDBTransactionPriority.DEFAULT)
                    .build();
        }
    }

    private FDBTransactionPriority getPriorityForFDB(TransactionConfig.Priority priority) throws RelationalException {
        switch (priority) {
            case DEFAULT:
                return FDBTransactionPriority.DEFAULT;
            case BATCH:
                return FDBTransactionPriority.BATCH;
            case SYSTEM_IMMEDIATE:
                return FDBTransactionPriority.SYSTEM_IMMEDIATE;
            default:
                throw new RelationalException("Invalid transaction priority in the config: <" + priority.name() + ">",
                        ErrorCode.INVALID_PARAMETER);
        }
    }
}
