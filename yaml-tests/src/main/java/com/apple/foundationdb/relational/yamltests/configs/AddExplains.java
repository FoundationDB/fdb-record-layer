/*
 * AddExplains.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.yamltests.configs;

import com.apple.foundationdb.relational.yamltests.YamlExecutionContext;

import javax.annotation.Nonnull;

/**
 * A configuration that runs an underlying configuration and adds {@code explain} blocks to all
 * queries in YAMSQL files that do not already have one, and corrects existing {@code explain} blocks
 * whose values have changed.
 * <p>
 *     For each query without an {@code explain:} block, the actual query plan is written into the YAMSQL
 *     source file immediately after the {@code query:} line (before any other config entries). For queries
 *     that already have an {@code explain:} block whose value differs from the actual plan, the block is
 *     updated in place.
 * </p>
 * <p>
 *     See {@link YamlExecutionContext#OPTION_ADD_EXPLAIN}.
 * </p>
 */
public class AddExplains extends ConfigWithOptions {
    public AddExplains(@Nonnull final YamlTestConfig underlying) {
        super(underlying, YamlExecutionContext.ContextOptions.of(YamlExecutionContext.OPTION_ADD_EXPLAIN, true));
    }
}