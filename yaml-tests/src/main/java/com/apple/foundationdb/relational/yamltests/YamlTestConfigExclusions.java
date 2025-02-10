/*
 * YamlTestConfigExclusions.java
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

package com.apple.foundationdb.relational.yamltests;

import com.apple.foundationdb.relational.yamltests.configs.ConfigWithOptions;
import com.apple.foundationdb.relational.yamltests.configs.JDBCInProcessConfig;
import com.apple.foundationdb.relational.yamltests.configs.YamlTestConfig;

/**
 * An enum of reasons to disable a {@link YamlTest} test for a given {@link YamlTestConfig}.
 * <p>
 *     Note: Part of the reason this exists as an enum, is that fields on annotations can only be certain types, and
 *     enum seemed like the cleanest.
 * </p>
 */
public enum YamlTestConfigExclusions {
    /**
     * Used to skip the test if the config uses JDBC.
     */
    USES_JDBC {
        @Override
        boolean check(final YamlTestConfig config) {
            return config instanceof JDBCInProcessConfig ||
                    (config instanceof ConfigWithOptions && ((ConfigWithOptions)config).getUnderlying() instanceof JDBCInProcessConfig);
        }
    },
    /**
     * Used to skip the test if the config sets {@link YamlExecutionContext#OPTION_FORCE_CONTINUATIONS} to {@code true}.
     */
    FORCES_CONTINUATIONS {
        @Override
        boolean check(final YamlTestConfig config) {
            return config.getRunnerOptions().getOrDefault(YamlExecutionContext.OPTION_FORCE_CONTINUATIONS, false);
        }
    };

    /**
     * Returns whether the associated test should be excluded for the provided config.
     * @param config a {@link YamlTestConfig}
     * @return {@code true} if the test should be excluded.
     */
    abstract boolean check(YamlTestConfig config);
}
