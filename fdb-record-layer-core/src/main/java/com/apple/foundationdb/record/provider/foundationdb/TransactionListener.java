/*
 * TransactionListener.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2021 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.record.provider.common.StoreTimer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A class that is notified of events that take place within the scope of a transaction, such as
 * when the transaction is created, committed, or closed. Exceptions thrown (explicitly or implicitly) by the
 * implementation of these methods will escape to the caller, possibly masking underlying exceptions, so
 * implementations must take care to avoid this situation or unless they are certain this desirable behavior.
 */
public interface TransactionListener {

    /**
     * Called immediately upon the creation of a new transaction.
     *
     * @param database the database for which the transaction was created.
     * @param transaction the transaction that was created
     */
    void create(@Nonnull FDBDatabase database, @Nonnull Transaction transaction);

    /**
     * Called when a transaction commit completes, either successfully or unsuccessfully. While the
     * transaction is made available, care must be taken not to perform operations that are not valid
     * for a committed transaction.
     *
     * <p>Note that the received {@code storeTimer} is not the timer that was provided to the
     * {@code FDBRecordContext} from which the transaction was created, but is a separate timer
     * that contains only low-level metrics (reads/writes/etc.) specific to this transaction.
     *
     * @param database the database in which the commit occurred
     * @param transaction the transaction for which the commit occurred
     * @param storeTimer low level I/O metrics accumulated by the transaction
     * @param exception if present, the exception encountered during the commit
     */
    void commit(@Nonnull FDBDatabase database,
                @Nonnull Transaction transaction,
                @Nullable StoreTimer storeTimer,
                @Nullable Throwable exception);

    /**
     * Called immediately following the closing of a transaction. While the transaction is made available,
     * care must be taken not to perform operations that are not valid for a closed transaction.
     *
     * <p>Note that the received {@code storeTimer} is not the timer that was provided to the
     * {@code FDBRecordContext} from which the transaction was created, but is a separate timer
     * that contains only low-level metrics (reads/writes/etc.) specific to this transaction.
     *
     * @param database the database for which the transaction was created
     * @param transaction the transaction for which the commit occurred
     * @param storeTimer low level I/O metrics accumulated by the transaction
     */
    void close(@Nonnull FDBDatabase database,
               @Nonnull Transaction transaction,
               @Nullable StoreTimer storeTimer);
}
