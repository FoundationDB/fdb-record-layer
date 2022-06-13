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

import com.google.common.collect.ImmutableMap;

import java.util.Map;

import javax.annotation.Nonnull;

public final class Options {
    public enum Name {
        CONTINUATION,
        INDEX_HINT,
        ROW_LIMIT // Limit the maximum number of records to return
    }

    private Options parentOptions;
    private final Map<Name, Object> optionsMap;

    private Options(Map<Name, Object> optionsMap) {
        this(optionsMap, null);
    }

    private Options(Map<Name, Object> optionsMap, Options parentOptions) {
        this.optionsMap = optionsMap;
        this.parentOptions = parentOptions;
    }

    public static Options none() {
        return Options.builder().build();
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

    public int size() {
        if (parentOptions != null) {
            return parentOptions.size() + optionsMap.size();
        } else {
            return optionsMap.size();
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

        public Builder withOption(Name name, Object value) {
            optionsMapBuilder.put(name, value);
            return this;
        }

        public Options build() {
            return new Options(optionsMapBuilder.build());
        }
    }
}
