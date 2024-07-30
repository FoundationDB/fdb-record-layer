/*
 * MultipleTransactions.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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
import com.apple.foundationdb.FDBError;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Class to use to test how multiple concurrent transactions interact. This allows for multiple transactions to
 * be {@linkplain #create(Database, int) created}, and then {@linkplain #commit(Set) committed}, with the option
 * to specify a subset that are expected to fail due to transaction conflicts.
 */
public class MultipleTransactions implements AutoCloseable, Iterable<Transaction> {
    // Dummy key used to add a write conflict to ensure otherwise read-only transactions commit
    private static final byte[] DEADC0DE = new byte[]{(byte)0xde, (byte)0xad, (byte)0xc0, (byte)0xde};

    private final List<Transaction> transactions;

    private MultipleTransactions(List<Transaction> transactions) {
        this.transactions = transactions;
    }

    public Transaction get(int i) {
        return transactions.get(i);
    }

    @Override
    @Nonnull
    public Iterator<Transaction> iterator() {
        return transactions.iterator();
    }

    public void forEach(Consumer<? super Transaction> consumer) {
        transactions.forEach(consumer);
    }

    @Override
    public void close() {
        transactions.forEach(Transaction::close);
    }

    public int size() {
        return transactions.size();
    }

    public void commit() {
        commit(Collections.emptySet());
    }

    public void commit(Set<Integer> expectedConflicts) {
        for (int i = 0; i < transactions.size(); i++) {
            Transaction tr = transactions.get(i);
            if (expectedConflicts.contains(i)) {
                CompletionException err = assertThrows(CompletionException.class, () -> tr.commit().join(),
                        "Transaction " + i + " should not commit successfully");
                Throwable cause = err.getCause();
                assertNotNull(cause);
                assertTrue(cause instanceof FDBException);
                assertEquals(FDBError.NOT_COMMITTED.code(), ((FDBException)cause).getCode());
            } else {
                assertDoesNotThrow(() -> tr.commit().join(), "Transaction " + i + " should commit successfully");
            }
        }
    }

    public static MultipleTransactions create(Database db, int n) {
        List<Transaction> transactions = new ArrayList<>(n);
        boolean returned = false;
        try {
            for (int i = 0; i < n; i++) {
                transactions.add(db.createTransaction());
            }
            // Initialize all of the transactions so that the commit version for each of them
            // will be after the read version for all of them. Also give each transaction a
            // write conflict key so that it will be committed.
            transactions.forEach(tr -> {
                tr.getReadVersion().join();
                tr.addWriteConflictKey(DEADC0DE);
            });
            MultipleTransactions multi = new MultipleTransactions(ImmutableList.copyOf(transactions));
            returned = true;
            return multi;
        } finally {
            if (!returned) {
                transactions.forEach(Transaction::close);
            }
        }
    }
}
