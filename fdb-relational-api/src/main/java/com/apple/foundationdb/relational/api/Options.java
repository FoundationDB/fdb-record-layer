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

import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public final class Options {
    public enum Name {
        CONTINUATION,
        INDEX_HINT,
        /**
         * Limit the maximum number of records to return before prompting for continuation.
         */
        CONTINUATION_PAGE_SIZE,
        /**
         * When set, only tables which were created at or before the specified version can be opened.
         * If this is set to -1, then it only requires that a version number exists.
         *
         * This is something of a weird carryover from development work which happened before Relational existed,
         * and should only be used sparingly except in those specific use-cases.
         */
        REQUIRED_METADATA_TABLE_VERSION,
        /**
         * Transaction timeout in milliseconds.
         */
        TRANSACTION_TIMEOUT,
        /**
         * During insertion, if the primary key of the inserted row is already in the table, replace the old row with the new row.
         */
        REPLACE_ON_DUPLICATE_PK,

        /**
         * Capacity limit of Relational's plan cache. Entries will be evicted from the cache following an LRU model.
         */
        PLAN_CACHE_MAX_ENTRIES,

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
         */
        INDEX_FETCH_METHOD
    }

    public enum IndexFetchMethod { SCAN_AND_FETCH, USE_REMOTE_FETCH, USE_REMOTE_FETCH_WITH_FALLBACK }

    private static final Map<Name, List<OptionContract>> contracts = Map.of(
            Name.CONTINUATION, List.of(new TypeContract<>(Continuation.class)),
            Name.INDEX_HINT, List.of(new TypeContract<>(String.class)),
            Name.CONTINUATION_PAGE_SIZE, List.of(new TypeContract<>(Integer.class), new RangeContract<>(0, Integer.MAX_VALUE)),
            Name.REQUIRED_METADATA_TABLE_VERSION, List.of(new TypeContract<>(Integer.class), new RangeContract<>(-1, Integer.MAX_VALUE)),
            Name.TRANSACTION_TIMEOUT, List.of(new TypeContract<>(Long.class), new RangeContract<>(-1L, Long.MAX_VALUE)),
            Name.REPLACE_ON_DUPLICATE_PK, List.of(new TypeContract<>(Boolean.class)),
            Name.PLAN_CACHE_MAX_ENTRIES, List.of(new TypeContract<>(Integer.class)),
            Name.INDEX_FETCH_METHOD, List.of(new TypeContract<>(IndexFetchMethod.class))
    );

    private static final Map<Name, Object> defaults = Map.of(
            Name.CONTINUATION_PAGE_SIZE, Integer.MAX_VALUE,
            Name.REPLACE_ON_DUPLICATE_PK, false,
            Name.PLAN_CACHE_MAX_ENTRIES, 0,
            Name.INDEX_FETCH_METHOD, IndexFetchMethod.USE_REMOTE_FETCH_WITH_FALLBACK);

    public static final Options NONE = Options.builder().build();

    private final Options parentOptions;
    private final Map<Name, Object> optionsMap;

    private Options(Map<Name, Object> optionsMap, Options parentOptions) {
        this.optionsMap = optionsMap;
        this.parentOptions = parentOptions;
    }

    @SuppressWarnings("unchecked")
    public <T> T getOption(Name name) {
        T option = getOptionInternal(name);
        if (option == null) {
            return (T) defaults.get(name);
        } else {
            return option;
        }
    }

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

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        ImmutableMap.Builder<Name, Object> optionsMapBuilder = ImmutableMap.builder();
        Options parentOptions;

        private Builder() {
        }

        public Builder withOption(Name name, Object value) throws SQLException {
            validateOption(name, value);
            optionsMapBuilder.put(name, value);
            return this;
        }

        public Builder fromOptions(Options options) throws SQLException {
            optionsMapBuilder.putAll(options.optionsMap);
            if (parentOptions != null) {
                // Replace Assert.that(parentOptions == null);
                // ... so we don't have to have recordlayer in this module.
                throw new SQLException("parentOptions are NOT null", ErrorCode.INTERNAL_ERROR.getErrorCode());
            }
            parentOptions = options.parentOptions;
            return this;
        }

        public Options build() {
            return new Options(optionsMapBuilder.build(), parentOptions);
        }
    }

    private static void validateOption(Name name, Object value) throws SQLException {
        for (OptionContract contract : contracts.get(name)) {
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
}
