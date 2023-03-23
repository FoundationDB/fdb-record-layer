/*
 * RecordStoreAndRecordContextTransaction.java
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

import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStoreBase;
import com.apple.foundationdb.relational.api.ConnectionScoped;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import com.google.protobuf.Message;

/**
 * This transaction object must be destroyed when it's creating connection is destroyed. Note that this is
 * <em>not</em> the same as saying the transaction itself is terminated, just that this specific object is
 * no longer valid
 */
@ConnectionScoped
public class RecordStoreAndRecordContextTransaction implements Transaction {
    FDBRecordStoreBase<Message> store;
    RecordContextTransaction transaction;

    public RecordStoreAndRecordContextTransaction(FDBRecordStoreBase<Message> store, FDBRecordContext context) {
        this.store = store;
        this.transaction = new RecordContextTransaction(context);
    }

    @Override
    public void commit() throws RelationalException {
        transaction.commit();
    }

    @Override
    public void abort() throws RelationalException {
        transaction.abort();
    }

    @Override
    public void close() throws RelationalException {
        transaction.close();
    }

    public RecordContextTransaction getRecordContextTransaction() {
        return transaction;
    }

    public FDBRecordStoreBase<Message> getRecordStore() {
        return store;
    }
}
