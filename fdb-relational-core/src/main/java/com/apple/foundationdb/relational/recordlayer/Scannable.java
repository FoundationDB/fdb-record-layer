/*
 * Scannable.java
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
import com.apple.foundationdb.relational.api.KeyValue;
import com.apple.foundationdb.relational.api.NestableTuple;
import com.apple.foundationdb.relational.api.QueryProperties;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface Scannable {

    /**
     * Open a scan against this entity.
     *
     * @param transaction    the transaction to use.
     * @param startKey       the key to start the scan from(inclusive), or {@code null} if we wish to start at the beginning.
     * @param endKey         the key to end the scan at (exclusive), or {@code null} if we wish to scan all the way to the end.
     * @param continuation   the continuation that could be used to resume a previous scan.
     * @param scanProperties the properties for the scan
     * @return a Scanner over the range [startKey,endKey), with the specified options and using the specified transaction.
     * @throws RelationalException if something goes wrong during scanning.
     */
    @Nonnull
    ResumableIterator<KeyValue> openScan(@Nonnull Transaction transaction, @Nullable NestableTuple startKey, @Nullable NestableTuple endKey,
                                         @Nullable Continuation continuation, @Nonnull QueryProperties scanProperties) throws RelationalException;

    KeyValue get(@Nonnull Transaction t, @Nonnull NestableTuple key, @Nonnull QueryProperties queryProperties) throws RelationalException;

    /**
     * The index is the position in the KeyValue(key first, then value), and the value is
     * the name of the field at that position.
     *
     * @return the field-name map
     */
    String[] getFieldNames();

    /**
     * The index is the position in the Key, the value is the name of the field in that position.
     *
     * @return the field-name map that contains <em>only</em> keys.
     */
    String[] getKeyFieldNames();

    KeyBuilder getKeyBuilder();

    /**
     * If table, returns the table name or index name if it is an index.
     *
     * @return the table name if it is a table, or index name if it is an index
     */
    @Nonnull
    String getName();
}
