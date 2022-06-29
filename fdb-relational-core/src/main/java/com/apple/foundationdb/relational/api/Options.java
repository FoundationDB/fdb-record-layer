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

import com.apple.foundationdb.relational.api.exceptions.InternalErrorException;
import com.apple.foundationdb.relational.api.exceptions.RelationalException;
import com.apple.foundationdb.relational.api.options.OptionContract;
import com.apple.foundationdb.relational.api.options.RangeContract;
import com.apple.foundationdb.relational.api.options.TypeContract;

import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

public final class Options {
    public enum Name {
        CONTINUATION,
        INDEX_HINT,
        CONTINUATION_PAGE_SIZE, // Limit the maximum number of records to return before prompting for continuation
        /*
         * When set, only tables which were created at or before the specified version can be opened.
         * If this is set to -1, then it only requires that a version number exists.
         *
         * This is something of a weird carryover from development work which happened before Relational existed,
         * and should only be used sparingly except in those specific use-cases.
         */
        REQUIRED_METADATA_TABLE_VERSION
    }

    private static final Map<Name, List<OptionContract>> contracts = Map.of(
            Name.CONTINUATION, List.of(new TypeContract<>(Continuation.class)),
            Name.INDEX_HINT, List.of(new TypeContract<>(String.class)),
            Name.CONTINUATION_PAGE_SIZE, List.of(new TypeContract<>(Integer.class), new RangeContract<>(0, Integer.MAX_VALUE)),
            Name.REQUIRED_METADATA_TABLE_VERSION, List.of(new TypeContract<>(Integer.class), new RangeContract<>(-1, Integer.MAX_VALUE))
    );

    public static final Options NONE = Options.builder().build();

    private final Options parentOptions;
    private final Map<Name, Object> optionsMap;

    private Options(Map<Name, Object> optionsMap) {
        this(optionsMap, null);
    }

    private Options(Map<Name, Object> optionsMap, Options parentOptions) {
        this.optionsMap = optionsMap;
        this.parentOptions = parentOptions;
    }

    @SuppressWarnings("unchecked")
    public <T> T getOption(Name name) {
        T option = (T) optionsMap.get(name);
        if (option == null && parentOptions != null) {
            return parentOptions.getOption(name);
        } else {
            return option;
        }
    }

    @SuppressWarnings({"PMD.CompareObjectsWithEquals"})
    public static Options combine(@Nonnull Options parentOptions, @Nonnull Options childOptions) {
        if (childOptions.parentOptions != null) {
            throw new InternalErrorException("Cannot override parent options").toUncheckedWrappedException();
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

        private Builder() {
        }

        public Builder withOption(Name name, Object value) throws RelationalException {
            validateOption(name, value);
            optionsMapBuilder.put(name, value);
            return this;
        }

        public Options build() {
            return new Options(optionsMapBuilder.build());
        }
    }

    private static void validateOption(Name name, Object value) throws RelationalException {
        for (OptionContract contract : contracts.get(name)) {
            contract.validate(name, value);
        }
    }
}
