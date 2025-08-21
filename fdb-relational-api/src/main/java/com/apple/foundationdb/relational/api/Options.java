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

import com.apple.foundationdb.annotation.API;
import com.apple.foundationdb.relational.api.exceptions.ErrorCode;
import com.apple.foundationdb.relational.api.options.CollectionContract;
import com.apple.foundationdb.relational.api.options.OptionContract;
import com.apple.foundationdb.relational.api.options.OptionContractWithConversion;
import com.apple.foundationdb.relational.api.options.RangeContract;
import com.apple.foundationdb.relational.api.options.TypeContract;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

@API(API.Status.EXPERIMENTAL)
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
         * This can also be set via JDBC's setMaxRows
         * Scope: Connection, Direct Access API.
         */
        MAX_ROWS,
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
         * A set of planner rules (by name) that the query planner should not execute.
         * By default, no rules are disabled, and the user should be able to leave this
         * unset. This is intended as an escape hatch in case the introduction of a new
         * planner rule causes trouble for an existing query.
         * <p>
         * This option is experimental, and is meant to be used for debugging purposes only.
         * </p>
         * Scope: Connection, Query
         */
        DISABLED_PLANNER_RULES,

        /**
         * A boolean indicating if the query planner should disable planner rewrite rules.
         * If this is set to {@code true}, then the planner will skip all of the rules used
         * in the rewrite phase of the planner. This can result in sub-optimal plans, as
         * the planning phase may now need to match a more complicated query, but it can
         * be used as a way to disable the rewrite phase if certain queries either
         * encounter an error or take too much time exploring different rewrites.
         * <p>
         * This option is experimental, and is meant to be used for debugging purposes only.
         * </p>
         * Scope: Connection, Query
         */
        DISABLE_PLANNER_REWRITING,

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

        /**
         * Timeout for asynchronous operations in milliseconds, this is usually used to set an upperbound time limit for
         * operations interacting with FDB.
         * Scope: Engine
         */
        ASYNC_OPERATIONS_TIMEOUT_MILLIS,

        /**
         * A boolean indicating whether to encrypt records when saving and decrypt when loading.
         */
        ENCRYPT_WHEN_SERIALIZING,

        /**
         * The key store file containing the encryption key to use.
         */
        ENCRYPTION_KEY_STORE,

        /**
         * The key store entry containing the encryption key to use.
         */
        ENCRYPTION_KEY_ENTRY,

        /**
         * The integrity key of the key store and the encryption key of the key entry.
         */
        ENCRYPTION_KEY_PASSWORD,
    }

    public enum IndexFetchMethod {
        SCAN_AND_FETCH,
        USE_REMOTE_FETCH,
        USE_REMOTE_FETCH_WITH_FALLBACK
    }

    private static final char[] HEX_CHARS = "0123456789ABCDEF".toCharArray();

    @SuppressWarnings("PMD.AvoidFieldNameMatchingTypeName")
    private static final Map<Name, List<OptionContract>> OPTIONS = makeContracts();

    @Nonnull
    private static final Map<Name, Object> OPTIONS_DEFAULT_VALUES;

    static {
        final var builder = ImmutableMap.<Name, Object>builder();
        builder.put(Name.MAX_ROWS, Integer.MAX_VALUE);
        builder.put(Name.INDEX_FETCH_METHOD, IndexFetchMethod.USE_REMOTE_FETCH_WITH_FALLBACK);
        builder.put(Name.DISABLE_PLANNER_REWRITING, false);
        builder.put(Name.DISABLED_PLANNER_RULES, ImmutableSet.of());
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
        builder.put(Name.CONTINUATIONS_CONTAIN_COMPILED_STATEMENTS, true);
        builder.put(Name.ASYNC_OPERATIONS_TIMEOUT_MILLIS, 10_000L);
        builder.put(Name.ENCRYPT_WHEN_SERIALIZING, false);
        builder.put(Name.ENCRYPTION_KEY_PASSWORD, "");
        OPTIONS_DEFAULT_VALUES = builder.build();
    }

    public static final Options NONE = Options.builder().build();

    @Nullable
    private final Options parentOptions;
    @Nonnull
    private final Map<Name, Object> optionsMap;

    @Nonnull
    public static Options none() {
        return NONE;
    }

    @Nonnull
    public static Map<Name, Object> defaultOptions() {
        return OPTIONS_DEFAULT_VALUES;
    }

    private Options(@Nonnull Map<Name, Object> optionsMap, @Nullable Options parentOptions) {
        this.optionsMap = optionsMap;
        this.parentOptions = parentOptions;
    }

    @SuppressWarnings("unchecked")
    public <T> T getOption(@Nonnull Name name) {
        T option = getOptionInternal(name);
        if (option == null) {
            return (T) OPTIONS_DEFAULT_VALUES.get(name);
        } else {
            return option;
        }
    }

    public Options withOption(@Nonnull Name name, @Nullable Object value) throws SQLException {
        return builder().fromOptions(this).withOption(name, value).build();
    }

    public Options withChild(@Nonnull Options childOptions) throws SQLException {
        return Options.combine(this, childOptions);
    }

    @Nonnull
    @SuppressWarnings({"PMD.CompareObjectsWithEquals"})
    private static Options combine(@Nonnull Options parentOptions, @Nonnull Options childOptions) throws SQLException {
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
        public Builder withOptionFromString(Name name, String valueAsString) throws SQLException {
            final Object value = parseStringOption(name, valueAsString);
            return withOption(name, value);
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

    @Nullable
    private static Object parseStringOption(@Nonnull final Name name, String valueAsString) throws SQLException {
        for (OptionContract contract : Objects.requireNonNull(OPTIONS).get(name)) {
            if (contract instanceof OptionContractWithConversion<?>) {
                return ((OptionContractWithConversion<?>)contract).fromString(valueAsString);
            }
        }
        throw new SQLException("option must have at least one type contract", ErrorCode.INTERNAL_ERROR.getErrorCode());
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

    @Nonnull
    public Iterable<? extends Map.Entry<Name, ?>> entries() {
        if (parentOptions != null) {
            return Iterables.concat(parentOptions.entries(), optionsMap.entrySet());
        } else {
            return optionsMap.entrySet();
        }
    }

    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof Options)) {
            return false;
        }

        final Options options = (Options)o;
        return Objects.equals(parentOptions, options.parentOptions) && optionsMap.equals(options.optionsMap);
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(parentOptions);
        result = 31 * result + optionsMap.hashCode();
        return result;
    }

    public static Options fromProperties(@Nullable Properties properties) throws SQLException {
        if (properties == null) {
            return none();
        }
        Builder builder = builder();
        for (String key : properties.stringPropertyNames()) {
            final Name name = Name.valueOf(key);
            // This does not work for CONTINUATION; throws an error. The Impl class is not accessible here.
            // Furthermore, there is no module into which the translation methods could be moved that has
            // access and is accessible to JDBC.
            builder.withOptionFromString(name, properties.getProperty(key));
        }
        return builder.build();
    }

    @Nonnull
    @SuppressWarnings("unchecked")
    public static Properties toProperties(final Options options) {
        final Properties result = new Properties();
        for (Map.Entry<Name, ?> entry : options.entries()) {
            final String prop;
            switch (entry.getKey()) {
                case CONTINUATION:
                    prop = bytesToHex(((Continuation)entry.getValue()).serialize());
                    break;
                case DISABLED_PLANNER_RULES:
                    prop = String.join(",", (Collection<String>)entry.getValue());
                    break;
                default:
                    prop = entry.getValue().toString();
                    break;
            }
            result.put(entry.getKey().name(), prop);
        }
        return result;
    }

    // TODO: This is just to avoid dependencies; use HexFormat when upgraded to JDK 17.
    private static String bytesToHex(@Nonnull byte[] bytes) {
        char[] hex = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++ ) {
            int b = bytes[i] & 0xFF;
            hex[i * 2] = HEX_CHARS[b >>> 4];
            hex[i * 2 + 1] = HEX_CHARS[b & 0x0F];
        }
        return new String(hex);
    }

    private static Map<Name, List<OptionContract>> makeContracts() {
        EnumMap<Name, List<OptionContract>> data = new EnumMap<>(Name.class);
        data.put(Name.CONTINUATION, List.of(TypeContract.of(Continuation.class, ignored -> { throw new UnsupportedOperationException(); })));
        data.put(Name.MAX_ROWS, List.of(TypeContract.intType(), RangeContract.of(0, Integer.MAX_VALUE)));
        data.put(Name.INDEX_FETCH_METHOD, List.of(TypeContract.of(IndexFetchMethod.class, IndexFetchMethod::valueOf)));
        data.put(Name.DISABLE_PLANNER_REWRITING, List.of(TypeContract.booleanType()));
        data.put(Name.DISABLED_PLANNER_RULES, List.of(new CollectionContract<>(TypeContract.stringType())));
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
        data.put(Name.LOG_QUERY, List.of(TypeContract.booleanType()));
        data.put(Name.LOG_SLOW_QUERY_THRESHOLD_MICROS, List.of(TypeContract.longType(), RangeContract.of(0L, Long.MAX_VALUE)));
        data.put(Name.EXECUTION_TIME_LIMIT, List.of(TypeContract.longType(), RangeContract.of(0L, Long.MAX_VALUE)));
        data.put(Name.EXECUTION_SCANNED_ROWS_LIMIT, List.of(TypeContract.intType(), RangeContract.of(0, Integer.MAX_VALUE)));
        data.put(Name.EXECUTION_SCANNED_BYTES_LIMIT, List.of(TypeContract.longType(), RangeContract.of(0L, Long.MAX_VALUE)));
        data.put(Name.DRY_RUN, List.of(TypeContract.booleanType()));
        data.put(Name.CASE_SENSITIVE_IDENTIFIERS, List.of(TypeContract.booleanType()));
        data.put(Name.CURRENT_PLAN_HASH_MODE, List.of(TypeContract.stringType()));
        data.put(Name.VALID_PLAN_HASH_MODES, List.of(TypeContract.stringType()));
        data.put(Name.CONTINUATIONS_CONTAIN_COMPILED_STATEMENTS, List.of(TypeContract.booleanType()));
        data.put(Name.ASYNC_OPERATIONS_TIMEOUT_MILLIS, List.of(TypeContract.longType(), RangeContract.of(0L, Long.MAX_VALUE)));
        data.put(Name.ENCRYPT_WHEN_SERIALIZING, List.of(TypeContract.booleanType()));
        data.put(Name.ENCRYPTION_KEY_STORE, List.of(TypeContract.stringType()));
        data.put(Name.ENCRYPTION_KEY_ENTRY, List.of(TypeContract.stringType()));
        data.put(Name.ENCRYPTION_KEY_PASSWORD, List.of(TypeContract.stringType()));

        return Collections.unmodifiableMap(data);
    }
}
