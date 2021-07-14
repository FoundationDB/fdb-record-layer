/*
 * OperationOption.java
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

import java.util.function.Predicate;

/**
 * Options related to a specific table operation.
 */
public class OperationOption<T> {



    /**
     * A policy for how to deal with timeouts that occur in the middle of a query. In general,
     * one should expect that a timeout of any form also results in the failure of the associated
     * transaction (although that's not always the case, the exact specification of when timeouts
     * also fail the underlying transaction are left to the implementation, and thus it's useful
     * to assume that any timeout may abort the transaction as well).
     */
    enum TimeoutPolicy{
        /**
         * Fail the query with an error. This will also abort the transaction.
         */
        FAIL,
        /**
         * Retry the query from the beginning. This may also create a new transaction
         */
        RETRY,
        /**
         * When scanning, it may be possible to retry the scan from a last known "continuation point".
         * When that happens, if a new transaction is created, the scan's isolation level will decrease
         * to no better than READ_COMMITTED. If that happens,
         * {@link RelationalResultSet#getActualIsolationLevel()} will return {@link IsolationLevel#READ_COMMITTED},
         * even if the scan was initiated with a higher level.
         */
        DOWNGRADE_ISOLATION,
    };

    final String optionName;
    final T value;

    public OperationOption(String optionName, T value) {
        this.optionName = optionName;
        this.value = value;
    }

    String getOptionName(){
        return optionName;
    }

    T getValue(){
        return value;
    }

    public static final String TIMEOUT_POLICY_NAME ="timeout.policy";
    public static final String INDEX_HINT_NAME = "use.index";
    public static final String RETRY_COUNT = "retry.limit";

    public static final String RETRY_POLICY= "retry.policy";
    private static final String CONTINUATION_NAME = "continuation";

    public static final String FORCE_VERIFY_DDL = "ddl.forceVerify";

    static OperationOption<Predicate<RelationalException>> retryPolicy(Predicate<RelationalException> predicate){
        return new OperationOption<>(RETRY_POLICY,predicate);
    }

    static OperationOption<TimeoutPolicy> timeoutPolicy(TimeoutPolicy policy){
        return new OperationOption<>(TIMEOUT_POLICY_NAME, policy);
    }

    static OperationOption<String> index(String indexName){
        return new OperationOption<>(INDEX_HINT_NAME,indexName);
    }

    public static OperationOption<Continuation> continuation(Continuation continuation) {
        return new OperationOption<>(CONTINUATION_NAME,continuation);
    }

    /**
     * Use this option when you want to force schema, table, and index loading to verify their existence
     * at load time (as opposed to lazily at first request time).
     *
     * @return an option for forcing ddl to verify itself
     */
    public static OperationOption<Boolean> forceVerifyDdl(){
        return new OperationOption<>(FORCE_VERIFY_DDL,Boolean.TRUE);
    }
}
