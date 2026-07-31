/*
 * AddResultMetadata.java
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
 * A configuration that runs an underlying configuration and adds {@code resultMetadata} blocks to all
 * queries in YAMSQL files that do not already have one.
 * <p>
 *     For each query without a {@code resultMetadata:} block, the actual column names and SQL type names
 *     reported by the driver are written into the YAMSQL source file immediately after the {@code query:} line.
 *     This is a one-shot bootstrap tool: run it once to populate metadata, then switch to
 *     {@link CorrectResultMetadata} (or plain execution) to keep it up to date.
 * </p>
 * <p>
 *     See {@link YamlExecutionContext#OPTION_ADD_RESULT_METADATA}.
 * </p>
 */
public class AddResultMetadata extends ConfigWithOptions {
    public AddResultMetadata(@Nonnull final YamlTestConfig underlying) {
        super(underlying, YamlExecutionContext.ContextOptions.of(YamlExecutionContext.OPTION_ADD_RESULT_METADATA, true));
    }
}
