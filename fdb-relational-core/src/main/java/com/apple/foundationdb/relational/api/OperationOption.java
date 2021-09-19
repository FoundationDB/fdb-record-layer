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

import com.apple.foundationdb.relational.api.exceptions.RelationalException;

import java.util.function.Predicate;

/**
 * Options related to a specific table operation.
 *
 * @param <T> the type of the value held in the operation.
 */
public class OperationOption<T> {

    /**
     * A policy for how to deal with timeouts that occur in the middle of a query. In general,
     * one should expect that a timeout of any form also results in the failure of the associated
     * transaction (although that's not always the case, the exact specification of when timeouts
     * also fail the underlying transaction are left to the implementation, and thus it's useful
     * to assume that any timeout may abort the transaction as well).
     */
    enum TimeoutPolicy {
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
    }

    ;

    public enum SchemaExistenceCheck {
        /**
         * No special action. This should be used with care, since if the schema already has records,
         * there is no guarantee that they were written at the current versions (meta-data and format).
         * It is really only appropriate in development when switching from uncheckedOpen or build to a checked open.
         */
        NONE,
        /**
         * Throw if the schema does not have an info header but does have have at least one record.
         * This differs from ERROR_IF_NO_INFO_AND_NOT_EMPTY in that there is data stored in the schema other than
         * just the records and the indexes, including meta-data about which indexes have been built.
         * A schema that is missing a header but has this other data is in a corrupt state, but as there are no records,
         * it can be recovered when creating the schema in a straightforward way.
         */
        ERROR_IF_NO_INFO_AND_HAS_RECORDS_OR_INDEXES,
        /**
         * Throw if the record store does not have an info header but is not empty.
         * Unlike with ERROR_IF_NO_INFO_AND_HAS_RECORDS_OR_INDEXES, this existence check will throw an error even
         * if there are no records in the store, only data stored internally by the Relational.
         */
        ERROR_IF_NO_INFO_AND_NOT_EMPTY,
        /**
         * Throw if the schema already exists
         */
        ERROR_IF_EXISTS,
        /**
         * Throw if the schema does not already exist
         */
        ERROR_IF_NOT_EXISTS
    }

    final String optionName;
    final T value;

    public OperationOption(String optionName, T value) {
        this.optionName = optionName;
        this.value = value;
    }

    String getOptionName() {
        return optionName;
    }

    T getValue() {
        return value;
    }

    public static final String TIMEOUT_POLICY_NAME = "timeout.policy";
    public static final String INDEX_HINT_NAME = "use.index";
    public static final String RETRY_COUNT = "retry.limit";

    public static final String RETRY_POLICY = "retry.policy";
    private static final String CONTINUATION_NAME = "continuation";

    public static final String FORCE_VERIFY_DDL = "ddl.forceVerify";

    public static final String SCHEMA_EXISTENCE_CHECK = "schema.existenceCheck";

    static OperationOption<Predicate<RelationalException>> retryPolicy(Predicate<RelationalException> predicate) {
        return new OperationOption<>(RETRY_POLICY, predicate);
    }

    static OperationOption<TimeoutPolicy> timeoutPolicy(TimeoutPolicy policy) {
        return new OperationOption<>(TIMEOUT_POLICY_NAME, policy);
    }

    public static OperationOption<String> index(String indexName) {
        return new OperationOption<>(INDEX_HINT_NAME, indexName);
    }

    public static OperationOption<Continuation> continuation(Continuation continuation) {
        return new OperationOption<>(CONTINUATION_NAME, continuation);
    }

    /**
     * Use this option when you want to force schema, table, and index loading to verify their existence
     * at load time (as opposed to lazily at first request time).
     *
     * @return an option for forcing ddl to verify itself
     */
    public static OperationOption<Boolean> forceVerifyDdl() {
        return new OperationOption<>(FORCE_VERIFY_DDL, Boolean.TRUE);
    }

    public static OperationOption<SchemaExistenceCheck> schemaExistenceCheck(SchemaExistenceCheck existenceCheck) {
        return new OperationOption<>(SCHEMA_EXISTENCE_CHECK, existenceCheck);
    }

}
