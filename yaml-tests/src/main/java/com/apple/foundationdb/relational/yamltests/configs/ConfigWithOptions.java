/*
 * ConfigWithOptions.java
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

import com.apple.foundationdb.relational.yamltests.YamlRunner;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

/**
 * An implementation of {@link YamlTestConfig} that sets additional options on top of a base config.
 */
public class ConfigWithOptions implements YamlTestConfig {
    @Nonnull
    private final YamlTestConfig underlying;
    @Nonnull
    private final Map<String, Object> runnerOptions;

    public ConfigWithOptions(@Nonnull final YamlTestConfig underlying, @Nonnull Map<String, Object> newOptions) {
        this.underlying = underlying;
        final HashMap<String, Object> options = new HashMap<>(underlying.getRunnerOptions());
        options.putAll(newOptions);
        this.runnerOptions = Map.copyOf(options);
    }


    @Override
    public YamlRunner.YamlConnectionFactory createConnectionFactory() {
        return underlying.createConnectionFactory();
    }

    @Override
    public @Nonnull Map<String, Object> getRunnerOptions() {
        return runnerOptions;
    }

    @Override
    public void beforeAll() throws Exception {
        underlying.beforeAll();
    }

    @Override
    public void afterAll() throws Exception {
        underlying.afterAll();
    }

    @Override
    public String toString() {
        return underlying + " WITH " + runnerOptions;
    }

    public YamlTestConfig getUnderlying() {
        return underlying;
    }
}
