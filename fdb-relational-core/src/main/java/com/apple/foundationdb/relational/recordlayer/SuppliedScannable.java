/*
 * SuppliedScannable.java
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

import com.apple.foundationdb.relational.api.Continuation;
import com.apple.foundationdb.relational.api.QueryProperties;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.util.SpotBugsSuppressWarnings;

import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class SuppliedScannable implements Scannable {
    private final Supplier<ResumableIterator<Row>> iteratorSupplier;
    private final String[] keyFieldNames;
    private final String[] fieldNames;

    @SpotBugsSuppressWarnings(value = "EI_EXPOSE_REP2",
            justification = "internal implementation class, proper usage is expected")
    public SuppliedScannable(@Nonnull Supplier<ResumableIterator<Row>> iteratorSupplier, @Nonnull String[] keyFieldNames, @Nonnull String[] fieldNames) {
        this.iteratorSupplier = iteratorSupplier;
        this.keyFieldNames = keyFieldNames;
        this.fieldNames = fieldNames;
    }

    @Nonnull
    @Override
    public ResumableIterator<Row> openScan(@Nonnull Transaction transaction, @Nullable Row startKey, @Nullable Row endKey, @Nullable Continuation continuation, @Nonnull QueryProperties scanProperties) throws RelationalException {
        return iteratorSupplier.get();
    }

    @Override
    public Row get(@Nonnull Transaction t, @Nonnull Row key, @Nonnull QueryProperties queryProperties) throws RelationalException {
        throw new UnsupportedOperationException("Doesn't matter here");
    }

    @SpotBugsSuppressWarnings("EI_EXPOSE_REP")
    @Override
    public String[] getFieldNames() throws RelationalException {
        return fieldNames;
    }

    @SpotBugsSuppressWarnings("EI_EXPOSE_REP")
    @Override
    public String[] getKeyFieldNames() {
        return keyFieldNames;
    }

    @Override
    public KeyBuilder getKeyBuilder() {
        throw new UnsupportedOperationException("Also doesn't matter here, cause the Scannable interface sucks");
    }

    @Nonnull
    @Override
    public String getName() {
        return "Supplied scannable";
    }
}
