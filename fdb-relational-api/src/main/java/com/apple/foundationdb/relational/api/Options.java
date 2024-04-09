/*
 * Options.java
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

import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.options.OptionContract;
import com.apple.foundationdb.relational.api.options.RangeContract;
import com.apple.foundationdb.relational.api.options.TypeContract;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class Options {

    public enum Name {
        /**
         * Continuation.
         * Scope: Direct Access API
         */
        CONTINUATION,
        INDEX_HINT,
        /**
         * Limit the maximum number of records to return before prompting for continuation.
         * Scope: Direct Access API
         */
        CONTINUATION_PAGE_SIZE,
        /**
         * When set, only tables which were created at or before the specified version can be opened.
         * If this is set to -1, then it only requires that a version number exists.
         * <p>
         * This is something of a weird carryover from development work which happened before Relational existed,
         * and should only be used sparingly except in those specific use-cases.
         * Scope: Direct Access API
         */
        REQUIRED_METADATA_TABLE_VERSION,
        /**
         * Transaction timeout in milliseconds.
         * Scope: Connection
         */
        TRANSACTION_TIMEOUT,
        /**
         * During insertion, if the primary key of the inserted row is already in the table, replace the old row with the new row.
         * Scope: Direct Access API
         */
        REPLACE_ON_DUPLICATE_PK,

        /**
         * Limit of Relational's primary plan cache.
         * Settings the limit to zero effectively disables the plan cache.
         * Scope: Engine
         */
        PLAN_CACHE_PRIMARY_MAX_ENTRIES,

        /**
         * Limit of Relational's secondary plan cache.
         * Scope: Engine
         */
        PLAN_CACHE_SECONDARY_MAX_ENTRIES,

        /**
         * Limit of Relational's tertiary plan cache.
         * Scope: Engine
         */
        PLAN_CACHE_TERTIARY_MAX_ENTRIES,

        /**
         * Read time-to-live duration (in milliseconds) of items in the primary cache.
         * Scope: Engine
         */
        PLAN_CACHE_PRIMARY_TIME_TO_LIVE_MILLIS,

        /**
         * Write time-to-live duration (in milliseconds) of items living in the secondary cache.
         * Scope: Engine
         */
        PLAN_CACHE_SECONDARY_TIME_TO_LIVE_MILLIS,

        /**
         * Write time-to-live duration (in milliseconds) of items living in the tertiary cache.
         * Scope: Engine
         */
        PLAN_CACHE_TERTIARY_TIME_TO_LIVE_MILLIS,

        /**
         * An indicator for the index fetch method to use for a query or an index scan.
         * Possible values are:
         * <UL>
         * <LI>{@link IndexFetchMethod#SCAN_AND_FETCH} use regular index scan followed by fetch</LI>
         * <LI>{@link IndexFetchMethod#USE_REMOTE_FETCH} use remote fetch feature from FDB</LI>
         * <LI>{@link IndexFetchMethod#USE_REMOTE_FETCH_WITH_FALLBACK} use remote fetch ability with fallback to regular
         * scan and fetch in case of failure. This is a safety measure meant to be used while the
         * remote fetch mechanism is being tested</LI>
         * </UL>
         * Scope: Connection
         */
        INDEX_FETCH_METHOD,

        /**
         * A boolean indicating if a query should be logged or not.
         * Scope: Connection, Query
         */
        LOG_QUERY,

        /**
         * Log a query at info level if it is slower than `LOG_SLOW_QUERY_THRESHOLD` microseconds.
         * Scope: Engine
         */
        LOG_SLOW_QUERY_THRESHOLD_MICROS,

        /**
         * Set a time limit per transaction.
         * If the limit is hit, a `SCAN_LIMIT_REACHED` SQLException is thrown. The continuation in the result can be used to resume.
         * Scope: Connection
         */
        EXECUTION_TIME_LIMIT,

        /**
         * Set a scanned bytes limit per transaction.
         * If the limit is hit, a `SCAN_LIMIT_REACHED` SQLException is thrown. The continuation in the result set can be used to resume.
         * Scope: Connection
         */
        EXECUTION_SCANNED_BYTES_LIMIT,

        /**
         * Set a scanned row limit per transaction.
         * If the limit is hit, a `SCAN_LIMIT_REACHED` SQLException is thrown. The continuation in the result set can be used to resume.
         * Scope: Connection
         */
        EXECUTION_SCANNED_ROWS_LIMIT,

        /**
         * Execute this insert / update / delete without persisting data to disk.
         * Scope: Query
         */
        DRY_RUN,

        /**
         * Treat identifiers as-is in terms of case without upper-casing non-quoted ones.
         * Scope: Connection
         */
        CASE_SENSITIVE_IDENTIFIERS,

        /**
         * Current plan hash mode. Must be a valid version or assumed current if not set.
         */
        CURRENT_PLAN_HASH_MODE,

        /**
         * Acceptable plan hash modes (string-delimited list). Allows the plan validation to utilize and accept
         * an older plan hash mode.
         */
        VALID_PLAN_HASH_MODES,

        /**
         * Boolean indicator if continuations generated for query responses may contain serialized compiled statements
         * that can be used in EXECUTE CONTINUATION statements.
         */
        CONTINUATIONS_CONTAIN_COMPILED_STATEMENTS,
    }

    public enum IndexFetchMethod {
        SCAN_AND_FETCH,
        USE_REMOTE_FETCH,
        USE_REMOTE_FETCH_WITH_FALLBACK
    }

    private static final Map<Name, List<OptionContract>> OPTIONS = makeContracts();

    @Nonnull
    private static final Map<Name, Object> OPTIONS_DEFAULT_VALUES;

    static {
        final var builder = ImmutableMap.<Name, Object>builder();
        builder.put(Name.CONTINUATION_PAGE_SIZE, Integer.MAX_VALUE);
        builder.put(Name.INDEX_FETCH_METHOD, IndexFetchMethod.USE_REMOTE_FETCH_WITH_FALLBACK);
        builder.put(Name.PLAN_CACHE_PRIMARY_MAX_ENTRIES, 1024);
        builder.put(Name.PLAN_CACHE_PRIMARY_TIME_TO_LIVE_MILLIS, 10_000L);
        builder.put(Name.PLAN_CACHE_SECONDARY_MAX_ENTRIES, 256);
        builder.put(Name.PLAN_CACHE_SECONDARY_TIME_TO_LIVE_MILLIS, 30_000L);
        builder.put(Name.PLAN_CACHE_TERTIARY_MAX_ENTRIES, 8);
        builder.put(Name.PLAN_CACHE_TERTIARY_TIME_TO_LIVE_MILLIS, 30_000L);
        builder.put(Name.REPLACE_ON_DUPLICATE_PK, false);
        builder.put(Name.LOG_QUERY, false);
        builder.put(Name.LOG_SLOW_QUERY_THRESHOLD_MICROS, 2_000_000L);
        builder.put(Name.EXECUTION_SCANNED_BYTES_LIMIT, Long.MAX_VALUE);
        builder.put(Name.EXECUTION_TIME_LIMIT, 0L);
        builder.put(Name.EXECUTION_SCANNED_ROWS_LIMIT, Integer.MAX_VALUE);
        builder.put(Name.DRY_RUN, false);
        builder.put(Name.CASE_SENSITIVE_IDENTIFIERS, false);
        builder.put(Name.CONTINUATIONS_CONTAIN_COMPILED_STATEMENTS, false);
        OPTIONS_DEFAULT_VALUES = builder.build();
    }

    public static final Options NONE = Options.builder().build();

    private final Options parentOptions;
    private final Map<Name, Object> optionsMap;

    @Nonnull
    public static Map<Name, Object> defaultOptions() {
        return OPTIONS_DEFAULT_VALUES;
    }

    private Options(Map<Name, Object> optionsMap, Options parentOptions) {
        this.optionsMap = optionsMap;
        this.parentOptions = parentOptions;
    }

    @SuppressWarnings("unchecked")
    public <T> T getOption(Name name) {
        T option = getOptionInternal(name);
        if (option == null) {
            return (T) OPTIONS_DEFAULT_VALUES.get(name);
        } else {
            return option;
        }
    }

    @Nonnull
    @SuppressWarnings({"PMD.CompareObjectsWithEquals"})
    public static Options combine(@Nonnull Options parentOptions, @Nonnull Options childOptions) throws SQLException {
        if (childOptions.parentOptions != null) {
            throw new SQLException("Cannot override parent options", ErrorCode.INTERNAL_ERROR.getErrorCode());
        }
        if (parentOptions == childOptions) {
            // We should not combine options with itself
            return childOptions;
        }

        return new Options(childOptions.optionsMap, parentOptions);
    }

    @Nonnull
    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        @Nonnull
        private final Map<Name, Object> optionsMap;

        @Nullable
        private Options parentOptions;

        private Builder() {
            optionsMap = Maps.newHashMap();
        }

        @Nonnull
        public Builder withOption(Name name, Object value) throws SQLException {
            validateOption(name, value);
            optionsMap.put(name, value);
            return this;
        }

        @Nonnull
        public Builder fromOptions(Options options) throws SQLException {
            optionsMap.putAll(options.optionsMap);
            if (parentOptions != null) {
                // Replace Assert.that(parentOptions == null);
                // ... so we don't have to have recordlayer in this module.
                throw new SQLException("parentOptions are NOT null", ErrorCode.INTERNAL_ERROR.getErrorCode());
            }
            parentOptions = options.parentOptions;
            return this;
        }

        @VisibleForTesting
        public void setParentOption(@Nullable final Options parentOptions) {
            this.parentOptions = parentOptions;
        }

        @Nonnull
        public Options build() {
            return new Options(ImmutableMap.copyOf(optionsMap), parentOptions);
        }
    }

    private static void validateOption(@Nonnull final Name name, Object value) throws SQLException {
        for (OptionContract contract : Objects.requireNonNull(OPTIONS).get(name)) {
            contract.validate(name, value);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T getOptionInternal(Name name) {
        T option = (T) optionsMap.get(name);
        if (option == null && parentOptions != null) {
            return parentOptions.getOption(name);
        } else {
            return option;
        }
    }

    private static Map<Name, List<OptionContract>> makeContracts() {
        EnumMap<Name, List<OptionContract>> data = new EnumMap<>(Name.class);
        data.put(Name.CONTINUATION, List.of(new TypeContract<>(Continuation.class)));
        data.put(Name.CONTINUATION_PAGE_SIZE, List.of(TypeContract.intType(), RangeContract.of(0, Integer.MAX_VALUE)));
        data.put(Name.INDEX_FETCH_METHOD, List.of(new TypeContract<>(IndexFetchMethod.class)));
        data.put(Name.INDEX_HINT, List.of(TypeContract.stringType()));
        data.put(Name.PLAN_CACHE_PRIMARY_MAX_ENTRIES, List.of(TypeContract.intType(), RangeContract.of(0, Integer.MAX_VALUE)));
        data.put(Name.PLAN_CACHE_PRIMARY_TIME_TO_LIVE_MILLIS, List.of(TypeContract.longType(), RangeContract.of(10L, Long.MAX_VALUE)));
        data.put(Name.PLAN_CACHE_SECONDARY_MAX_ENTRIES, List.of(TypeContract.intType(), RangeContract.of(1, Integer.MAX_VALUE)));
        data.put(Name.PLAN_CACHE_SECONDARY_TIME_TO_LIVE_MILLIS, List.of(TypeContract.longType(), RangeContract.of(10L, Long.MAX_VALUE)));
        data.put(Name.PLAN_CACHE_TERTIARY_MAX_ENTRIES, List.of(TypeContract.intType(), RangeContract.of(1, Integer.MAX_VALUE)));
        data.put(Name.PLAN_CACHE_TERTIARY_TIME_TO_LIVE_MILLIS, List.of(TypeContract.longType(), RangeContract.of(10L, Long.MAX_VALUE)));
        data.put(Name.REPLACE_ON_DUPLICATE_PK, List.of(TypeContract.booleanType()));
        data.put(Name.REQUIRED_METADATA_TABLE_VERSION, List.of(TypeContract.intType(), RangeContract.of(-1, Integer.MAX_VALUE)));
        data.put(Name.TRANSACTION_TIMEOUT, List.of(TypeContract.longType(), RangeContract.of(-1L, Long.MAX_VALUE)));
        data.put(Name.LOG_QUERY, List.of(new TypeContract<>(Boolean.class)));
        data.put(Name.LOG_SLOW_QUERY_THRESHOLD_MICROS, List.of(new TypeContract<>(Long.class), RangeContract.of(0L, Long.MAX_VALUE)));
        data.put(Name.EXECUTION_TIME_LIMIT, List.of(new TypeContract<>(Long.class), RangeContract.of(0L, Long.MAX_VALUE)));
        data.put(Name.EXECUTION_SCANNED_ROWS_LIMIT, List.of(new TypeContract<>(Integer.class), RangeContract.of(0, Integer.MAX_VALUE)));
        data.put(Name.EXECUTION_SCANNED_BYTES_LIMIT, List.of(new TypeContract<>(Long.class), RangeContract.of(0L, Long.MAX_VALUE)));
        data.put(Name.DRY_RUN, List.of(TypeContract.booleanType()));
        data.put(Name.CASE_SENSITIVE_IDENTIFIERS, List.of(TypeContract.booleanType()));
        data.put(Name.CURRENT_PLAN_HASH_MODE, List.of(TypeContract.stringType()));
        data.put(Name.VALID_PLAN_HASH_MODES, List.of(TypeContract.stringType()));
        data.put(Name.CONTINUATIONS_CONTAIN_COMPILED_STATEMENTS, List.of(TypeContract.booleanType()));

        return Collections.unmodifiableMap(data);
    }
}
