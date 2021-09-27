/*
 * IterableScannable.java
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

import com.apple.foundationdb.relational.api.KeyValue;
import com.apple.foundationdb.relational.api.NestableTuple;
import com.apple.foundationdb.relational.api.QueryProperties;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.google.common.collect.Iterators;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Function;

/**
 * A Scannable which moves over an Iterable.
 *
 * @param <T> the type of the iterator which is iterated over
 */
public class IterableScannable<T> implements Scannable {
    private final Iterable<T> iterable;
    private final Function<T, KeyValue> transform;
    private final String[] keyFieldNames;
    private final String[] fieldNames;

    public IterableScannable(@Nonnull Iterable<T> iterable,
                             @Nonnull Function<T, KeyValue> transform,
                             @Nonnull String[] keyFieldNames,
                             @Nonnull String[] fieldNames) {
        this.iterable = iterable;
        this.transform = transform;
        this.keyFieldNames = keyFieldNames;
        this.fieldNames = fieldNames;
    }

    @Nonnull
    @Override
    public String getName() {
        return "iterator";
    }

    @Override
    public Scanner<KeyValue> openScan(@Nonnull Transaction t,
                                      @Nullable NestableTuple startKey,
                                      @Nullable NestableTuple endKey,
                                      @Nonnull QueryProperties scanOptions) throws RelationalException {
        return new IteratorScanner<>(Iterators.transform(iterable.iterator(), transform::apply), false);
    }

    @Override
    public KeyValue get(@Nonnull Transaction t,
                        @Nonnull NestableTuple key,
                        @Nonnull QueryProperties scanOptions) throws RelationalException {
        Scanner<KeyValue> kvs = openScan(t, key, key, scanOptions);
        while (kvs.hasNext()) {
            final KeyValue next = kvs.next();
            if (next.key().equals(key)) {
                return next;
            }
        }
        return null;
    }

    @Override
    public String[] getFieldNames() {
        return fieldNames;
    }

    @Override
    public String[] getKeyFieldNames() {
        return keyFieldNames;
    }

    @Override
    public KeyBuilder getKeyBuilder() {
        throw new UnsupportedOperationException();
    }
}
