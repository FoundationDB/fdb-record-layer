/*
 * ShowPlanOnDiff.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2025 Apple Inc. and the FoundationDB project authors
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
 * A configuration that runs an underlying configuration, but shows the dots of the expected and the actual plan where
 * expected and actual explains differ.
 * <p>
 *     See {@link YamlExecutionContext#OPTION_SHOW_PLAN_ON_DIFF}.
 * </p>
 */
public class ShowPlanOnDiff extends ConfigWithOptions {
    public ShowPlanOnDiff(@Nonnull final YamlTestConfig underlying) {
        super(underlying, YamlExecutionContext.ContextOptions.of(YamlExecutionContext.OPTION_SHOW_PLAN_ON_DIFF, true));
    }
}
