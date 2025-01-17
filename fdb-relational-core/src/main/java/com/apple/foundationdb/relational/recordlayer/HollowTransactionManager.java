/*
 * HollowTransactionManager.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

import com.apple.foundationdb.annotation.API;

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.TransactionManager;
import com.apple.foundationdb.relational.api.exceptions.OperationUnsupportedException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.util.ExcludeFromJacocoGeneratedReport;

import javax.annotation.Nonnull;

@ExcludeFromJacocoGeneratedReport
@API(API.Status.EXPERIMENTAL)
public class HollowTransactionManager implements TransactionManager {
    public static final HollowTransactionManager INSTANCE = new HollowTransactionManager();

    @Override
    public Transaction createTransaction(@Nonnull Options options) throws RelationalException {
        throw new OperationUnsupportedException("This Transaction manager is hollow and does not support calls.");
    }

    @Override
    public void abort(Transaction txn) throws RelationalException {
        throw new OperationUnsupportedException("This Transaction manager is hollow and does not support calls.");

    }

    @Override
    public void commit(Transaction txn) throws RelationalException {
        throw new OperationUnsupportedException("This Transaction manager is hollow and does not support calls.");
    }
}
