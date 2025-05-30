/*
 * InMemoryTransactionManager.java
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

package com.apple.foundationdb.relational.utils;

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.TransactionManager;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.metadata.SchemaTemplate;
import org.apache.commons.lang3.NotImplementedException;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class InMemoryTransactionManager implements TransactionManager {

    private long txnId;
    private final Map<Long, Transaction> openTransactions = new HashMap<>();

    @Override
    public Transaction createTransaction(@Nonnull Options options) {
        final TestTransaction testTransaction = new TestTransaction(txnId++, this);
        openTransactions.put(testTransaction.txnId, testTransaction);
        return testTransaction;
    }

    @Override
    public void abort(Transaction txn) {
        openTransactions.remove(((TestTransaction) txn).txnId);
    }

    @Override
    public void commit(Transaction txn) {
        openTransactions.remove(((TestTransaction) txn).txnId);
    }

    private static final class TestTransaction implements Transaction {
        private final long txnId;
        private final TransactionManager txnManager;

        private TestTransaction(long txnId, TransactionManager txnManager) {
            this.txnId = txnId;
            this.txnManager = txnManager;
        }

        @Override
        public void commit() throws RelationalException {
            txnManager.commit(this);
        }

        @Override
        public void abort() throws RelationalException {
            txnManager.abort(this);
        }

        @Override
        public Optional<SchemaTemplate> getBoundSchemaMaybe() {
            throw new NotImplementedException("method is not implemented");
        }

        @Override
        public void setBoundSchema(@Nonnull final SchemaTemplate schemaTemplate) {
            throw new NotImplementedException("method is not implemented");
        }

        @Override
        public void close() {
            //no-op
        }

        @Override
        public boolean isClosed() {
            return false;
        }
    }
}
