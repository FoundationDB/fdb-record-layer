/*
 * Statement.java
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

package com.apple.foundationdb.relational.api;

import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.google.protobuf.Message;

import javax.annotation.Nonnull;
import java.util.Iterator;

public interface Statement extends AutoCloseable {

    default RelationalResultSet executeQuery(@Nonnull String query, @Nonnull Options options) throws RelationalException {
        return executeQuery(query, options, QueryProperties.DEFAULT);
    }

    RelationalResultSet executeQuery(@Nonnull String query, @Nonnull Options options, @Nonnull QueryProperties queryProperties) throws RelationalException;

    default RelationalResultSet executeQuery(@Nonnull Queryable query, @Nonnull Options options) throws RelationalException {
        return executeQuery(query, options, QueryProperties.DEFAULT);
    }

    RelationalResultSet executeQuery(@Nonnull Queryable query, @Nonnull Options options, @Nonnull QueryProperties queryProperties) throws RelationalException;

    default String explainQuery(String query, Options options) throws RelationalException {
        try (RelationalResultSet rrs = executeQuery("explain plan for " + query, options)) {
            rrs.next();
            return rrs.getString(0);
        }
    }


    @Nonnull
    RelationalResultSet executeScan(@Nonnull TableScan scan, @Nonnull Options options) throws RelationalException;

    default RelationalResultSet executeGet(@Nonnull String tableName, @Nonnull KeySet key, @Nonnull Options options) throws RelationalException {
        return executeGet(tableName, key, options, QueryProperties.DEFAULT);
    }

    @Nonnull
    RelationalResultSet executeGet(@Nonnull String tableName, @Nonnull KeySet key, @Nonnull Options options, @Nonnull QueryProperties queryProperties) throws RelationalException;

    int executeInsert(@Nonnull String tableName, @Nonnull Iterator<Message> data, @Nonnull Options options) throws RelationalException;

    default int executeInsert(@Nonnull String tableName, @Nonnull Iterable<Message> data, @Nonnull Options options) throws RelationalException {
        return executeInsert(tableName, data.iterator(), options);
    }

    int executeDelete(@Nonnull String tableName, @Nonnull Iterator<KeySet> keys, @Nonnull Options options) throws RelationalException;

    default int executeDelete(@Nonnull String tableName, @Nonnull Iterable<KeySet> keys, @Nonnull Options options) throws RelationalException {
        return executeDelete(tableName, keys.iterator(), options);
    }

    Continuation getContinuation();

    @Override
    void close() throws RelationalException;

    /* ****************************************************************************************************************/
    /*DDL type functions*/
    //TODO(bfines) eventually this should be replaced with a query language

}
