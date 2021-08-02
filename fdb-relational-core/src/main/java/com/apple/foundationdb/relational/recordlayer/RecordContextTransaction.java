/*
 * RecordContextTransaction.java
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
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.relational.api.ConnectionScoped;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalException;

import javax.annotation.Nonnull;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

/**
 * This transaction object must be destroyed when it's creating connection is destroyed. Note that this is
 * <em>not</em> the same as saying the transaction itself is terminated, just that this specific object is
 * no longer valid
 */
@ConnectionScoped
public class RecordContextTransaction implements Transaction {

    /*
     * Collection of Runnables that run whenever a transaction commits or aborts. Once called,
     * the list is cleared and the callable is never called again.
     */
    private final List<Runnable> txnTerminateListeners = new LinkedList<>();

    private final FDBRecordContext context;
    private boolean isClosed = false;

    public RecordContextTransaction(FDBRecordContext context) {
        this.context = context;
    }

    @Override
    public void commit() {
        context.commit();
        notifyTerminated();
    }

    @Override
    public void abort() {
        isClosed = true;
        notifyTerminated();
        context.close();
    }

    @Override
    public void close() throws RelationalException {
        abort();
    }

    @Nonnull
    @Override
    public <T> T unwrap(@Nonnull Class<? extends T> type) throws ClassCastException {
        if(FDBRecordContext.class.isAssignableFrom(type)){
            return type.cast(context);
        }
        return Transaction.super.unwrap(type);
    }

    private void notifyTerminated(){
        for(Runnable callable: txnTerminateListeners){
            callable.run();
        }
    }

    void addTerminationListener(@Nonnull Runnable onTerminateListener){
        assert !isClosed : "Cannot add a termination listener to a closed transaction!";
        txnTerminateListeners.add(onTerminateListener);
    }

    public FDBRecordContext getContext() {
        return context;
    }
}
