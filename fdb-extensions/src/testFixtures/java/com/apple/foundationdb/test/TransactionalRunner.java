/*
 * TransactionalRunner.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.test;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * A simple runner that opens and commits raw FDB transactions and ensures they are all closed on runner close.
 * <p>
 * This is the raw-FDB analog of the record-layer {@code TransactionalRunner}. It operates at the
 * {@link Database}/{@link Transaction} level, with no record-layer dependencies.
 * </p>
 */
public class TransactionalRunner implements AutoCloseable {

    @Nonnull
    private final Database database;
    private boolean closed;
    @Nonnull
    private final List<Transaction> transactionsToClose;

    public TransactionalRunner(@Nonnull Database database) {
        this.database = database;
        this.transactionsToClose = new ArrayList<>();
    }

    /**
     * Run some code with a transaction, then commit it.
     * <p>
     * The transaction will be committed if the future returned by the runnable completes successfully;
     * otherwise it will not be committed. Either way, the transaction is closed when the future completes.
     * If this runner is {@link #close() closed}, any open transaction will also be closed.
     * </p>
     *
     * @param runnable code to run with a fresh {@link Transaction}
     * @param <T> the type of value returned by the future
     * @return a future containing the result, after the transaction has been committed
     */
    @Nonnull
    @SuppressWarnings({"PMD.CloseResource", "PMD.UseTryWithResources"})
    public <T> CompletableFuture<T> runAsync(@Nonnull Function<? super Transaction, CompletableFuture<? extends T>> runnable) {
        return runAsync(true, runnable);
    }

    /**
     * A flavor of {@link #runAsync(Function)} that supports read-only (non-committing) transactions.
     *
     * @param commitWhenDone if {@code true} the transaction is committed on success; if {@code false} it is only closed
     * @param runnable code to run with a fresh {@link Transaction}
     * @param <T> the type of value returned by the future
     * @return a future containing the result; if {@code commitWhenDone} is {@code true}, after the transaction has been committed
     */
    @Nonnull
    @SuppressWarnings({"PMD.CloseResource", "PMD.UseTryWithResources"})
    public <T> CompletableFuture<T> runAsync(boolean commitWhenDone,
                                              @Nonnull Function<? super Transaction, CompletableFuture<? extends T>> runnable) {
        Transaction transaction = openTransaction();
        boolean returnedFuture = false;
        try {
            CompletableFuture<T> future = runnable.apply(transaction)
                    .thenCompose(val -> {
                        if (commitWhenDone) {
                            return transaction.commit().thenApply(vignore -> val);
                        } else {
                            return CompletableFuture.completedFuture(val);
                        }
                    });
            returnedFuture = true;
            return future.whenComplete((result, exception) -> transaction.close());
        } finally {
            if (!returnedFuture) {
                transaction.close();
            }
        }
    }

    /**
     * Open a new transaction that will be closed when this runner is closed.
     *
     * @return a new {@link Transaction}
     * @throws RunnerClosed if this runner has already been closed
     */
    @Nonnull
    public Transaction openTransaction() {
        if (closed) {
            throw new RunnerClosed();
        }
        Transaction transaction = database.createTransaction();
        addTransactionToClose(transaction);
        return transaction;
    }

    private synchronized void addTransactionToClose(@Nonnull Transaction transaction) {
        if (closed) {
            transaction.close();
            throw new RunnerClosed();
        }
        transactionsToClose.add(transaction);
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        transactionsToClose.forEach(Transaction::close);
        transactionsToClose.clear();
        this.closed = true;
    }

    /**
     * Exception thrown when an operation is attempted on a closed runner.
     */
    public static class RunnerClosed extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public RunnerClosed() {
            super("Runner has been closed");
        }
    }
}
