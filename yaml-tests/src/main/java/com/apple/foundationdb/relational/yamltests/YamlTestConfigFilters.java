/*
 * YamlTestConfigFilters.java
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
public enum YamlTestConfigFilters {
    /**
     * Used to skip the test if the config uses JDBC.
     */
    DO_NOT_USE_JDBC {
        @Override
        boolean filter(final YamlTestConfig config) {
            return !(config instanceof JDBCInProcessConfig) &&
                    !(config instanceof ConfigWithOptions && ((ConfigWithOptions)config).getUnderlying() instanceof JDBCInProcessConfig);
        }
    },
    /**
     * Used to skip the test if the config sets {@link YamlExecutionContext#OPTION_FORCE_CONTINUATIONS} to {@code true}.
     */
    DO_NOT_FORCE_CONTINUATIONS {
        @Override
        boolean filter(final YamlTestConfig config) {
            return !config.getRunnerOptions().getOrDefault(YamlExecutionContext.OPTION_FORCE_CONTINUATIONS, false);
        }
    },
    /**
     * Used to correct plans.
     */
    CORRECT_EXPLAINS {
        @Override
        boolean filter(final YamlTestConfig config) {
            return config.getRunnerOptions().getOrDefault(YamlExecutionContext.OPTION_CORRECT_EXPLAIN, false) &&
                    !config.getRunnerOptions().getOrDefault(YamlExecutionContext.OPTION_CORRECT_METRICS, false);
        }
    },
    /**
     * Used to correct metrics.
     */
    CORRECT_METRICS {
        @Override
        boolean filter(final YamlTestConfig config) {
            return !config.getRunnerOptions().getOrDefault(YamlExecutionContext.OPTION_CORRECT_EXPLAIN, false) &&
                    config.getRunnerOptions().getOrDefault(YamlExecutionContext.OPTION_CORRECT_METRICS, false);
        }
    },
    /**
     * Used to correct both.
     */
    CORRECT_EXPLAIN_AND_METRICS {
        @Override
        boolean filter(final YamlTestConfig config) {
            return config.getRunnerOptions().getOrDefault(YamlExecutionContext.OPTION_CORRECT_EXPLAIN, false) &&
                    config.getRunnerOptions().getOrDefault(YamlExecutionContext.OPTION_CORRECT_METRICS, false);
        }
    },
    /**
     * Used to show the plan diffs graphically.
     */
    SHOW_PLAN_ON_DIFF {
        @Override
        boolean filter(final YamlTestConfig config) {
            return (boolean)config.getRunnerOptions().getOrDefault(YamlExecutionContext.OPTION_SHOW_PLAN_ON_DIFF, false);
        }
    };

    /**
     * Returns whether the associated test should be excluded for the provided config.
     * @param config a {@link YamlTestConfig}
     * @return {@code false} if the test should be excluded.
     */
    abstract boolean filter(YamlTestConfig config);
}
