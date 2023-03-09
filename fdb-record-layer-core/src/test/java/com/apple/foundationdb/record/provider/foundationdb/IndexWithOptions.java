/*
 * IndexWithOptions.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2023 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.record.provider.foundationdb;

import com.apple.foundationdb.record.metadata.Index;
import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.Map;

/**
 * A test class that allows changing of an index' Options.
 */
class IndexWithOptions extends Index {
    /**
     * Create a new Index from teh given original and the given options.
     * The new options will be either merged into, or replace, the original index' options.
     * @param orig teh original index to clone
     * @param newOptions the set of new options to use
     * @param merge whether to merge (true) or replace (false) the original options
     */
    public IndexWithOptions(final Index orig, final Map<String, String> newOptions, boolean merge) {
        super(orig.getName(), orig.getRootExpression(), orig.getType(), merge(orig.getOptions(), newOptions, merge), orig.getPredicate());
        if (orig.getPrimaryKeyComponentPositions() != null) {
            setPrimaryKeyComponentPositions(Arrays.copyOf(orig.getPrimaryKeyComponentPositions(), orig.getPrimaryKeyComponentPositions().length));
        } else {
            setPrimaryKeyComponentPositions(null);
        }
        setSubspaceKey(orig.getSubspaceKey());
        setAddedVersion(orig.getAddedVersion());
        setLastModifiedVersion(orig.getLastModifiedVersion());
    }

    /**
     * Create a new index and add an option to the list of options.
     * @param orig teh original index to clone
     * @param optionName the name of the option to add
     * @param optionValue the value of the option to add
     */
    public IndexWithOptions(final Index orig, String optionName, String optionValue) {
        super(orig.getName(), orig.getRootExpression(), orig.getType(), merge(orig.getOptions(), Map.of(optionName, optionValue), true), orig.getPredicate());
        if (orig.getPrimaryKeyComponentPositions() != null) {
            setPrimaryKeyComponentPositions(Arrays.copyOf(orig.getPrimaryKeyComponentPositions(), orig.getPrimaryKeyComponentPositions().length));
        } else {
            setPrimaryKeyComponentPositions(null);
        }
        setSubspaceKey(orig.getSubspaceKey());
        setAddedVersion(orig.getAddedVersion());
        setLastModifiedVersion(orig.getLastModifiedVersion());
    }

    private static Map<String, String> merge(final Map<String, String> options, final Map<String, String> newOptions, boolean merge) {
        if (!merge) {
            return newOptions;
        } else {
            ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
            builder.putAll(options);
            builder.putAll(newOptions);
            return builder.buildKeepingLast();
        }
    }
}
