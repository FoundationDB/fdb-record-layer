/*
 * YamlTestConfig.java
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
import com.apple.foundationdb.relational.yamltests.YamlRunner;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * Interface for configuring how to run a {@code .yamsql} file.
 * <p>
 *     Primarily this is concerned with global configuration that allows us to run <em>all</em> tests with a different
 *     configuration, such as a different server, or mixed-mode.
 * </p>
 */
public interface YamlTestConfig {

    /**
     * Creates the connection to the database.
     * @return a new connection factory
     */
    YamlRunner.YamlConnectionFactory createConnectionFactory();

    /**
     * A list of options to be provided to {@link YamlRunner}.
     * @return the options for this config
     */
    @Nonnull
    YamlExecutionContext.ContextOptions getRunnerOptions();

    void beforeAll() throws Exception;

    void afterAll() throws Exception;
}
