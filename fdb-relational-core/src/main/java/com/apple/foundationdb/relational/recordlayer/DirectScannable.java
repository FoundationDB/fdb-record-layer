/*
 * DirectScannable.java
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

import com.apple.foundationdb.relational.api.Options;
import com.apple.foundationdb.relational.api.Row;
import com.apple.foundationdb.relational.api.StructMetaData;
import com.apple.foundationdb.relational.api.Transaction;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface DirectScannable {

    /**
     * Validate that this Scannable is able to satisfy the query options that are presented to it.
     *
     * This is present so that high level operations can fail quickly with different types of Unsupported errors,
     * rather than throwing something which is not as useful.
     *
     * @param scanOptions the options for the scan/get.
     * @throws RelationalException if the options are not supported by this scannable.
     */
    void validate(Options scanOptions) throws RelationalException;

    /**
     * Open a scan against this entity.
     *
     * @param transaction    the transaction to use.
     * @param startKey       the key to start the scan from(inclusive), or {@code null} if we wish to start at the beginning.
     * @param endKey         the key to end the scan at (exclusive), or {@code null} if we wish to scan all the way to the end.
     * @param options        the options for the scan
     * @return a Scanner over the range [startKey,endKey), with the specified options and using the specified transaction.
     * @throws RelationalException if something goes wrong during scanning.
     */
    @Nonnull
    ResumableIterator<Row> openScan(@Nonnull Transaction transaction, @Nullable Row startKey, @Nullable Row endKey,
                                    Options options) throws RelationalException;

    Row get(@Nonnull Transaction t, @Nonnull Row key, @Nonnull Options options) throws RelationalException;

    KeyBuilder getKeyBuilder() throws RelationalException;

    /**
     * If table, returns the table name or index name if it is an index.
     *
     * @return the table name if it is a table, or index name if it is an index
     */
    @Nonnull
    String getName();

    StructMetaData getMetaData() throws RelationalException;
}
