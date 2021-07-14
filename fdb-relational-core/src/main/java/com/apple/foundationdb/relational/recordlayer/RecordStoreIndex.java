/*
 * RecordStoreIndex.java
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

import com.apple.foundationdb.relational.api.Index;
import com.apple.foundationdb.relational.api.KeyValue;
import com.apple.foundationdb.relational.api.NestableTuple;
import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Scanner;
import com.apple.foundationdb.relational.api.Table;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.RelationalException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class RecordStoreIndex implements Index {
    public RecordStoreIndex(com.apple.foundationdb.record.metadata.Index index, RecordStoreConnection conn) {
        throw new UnsupportedOperationException("Not Implemented in the Relational layer");
    }

    @Nonnull
    @Override
    public String getName() {
        return null;
    }

    @Override
    public Table getTable() {
        return null;
    }

    @Override
    public void close() throws RelationalException {

    }

    @Override
    public String[] getKeyNames() {
        throw new UnsupportedOperationException("Not Implemented in the Relational layer");
    }

    @Override
    public Scanner<KeyValue> openScan(@Nonnull Transaction t, @Nullable NestableTuple startKey, @Nullable NestableTuple endKey, @Nonnull Options scanOptions) throws RelationalException {
        return null;
    }

    @Override
    public String[] getFieldNames() {
        return new String[0];
    }

    @Override
    public String[] getKeyFieldNames() {
        return new String[0];
    }
}
