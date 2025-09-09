/*
 * WorkloadConfig.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2025 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.relational.autotest;

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class WorkloadConfig implements ParameterResolver {
    public static final String SAMPLE_SIZE = "sampleSize";
    public static final String INSERT_BATCH_SIZE = "insertBatchSize";

    private final long seed;
    private final String reportDirectory;
    private final Map<String, Object> configs;

    public WorkloadConfig(String reportDirectory, Map<String, Object> otherConfigs) {
        final boolean includeRandom = Boolean.parseBoolean(System.getProperty("tests.includeRandom", "false"));
        if (includeRandom) {
            this.seed = ThreadLocalRandom.current().nextLong();
        } else {
            this.seed = Long.parseLong(System.getProperty("tests.autoSeed", "2363712622230246740"));
        }
        this.reportDirectory = reportDirectory;
        this.configs = Map.copyOf(otherConfigs);
    }

    public long getSeed() {
        return seed;
    }

    public String getReportDirectory() {
        return reportDirectory;
    }

    public Object get(String key) {
        return configs.get(key);
    }

    public Object get(String key, Object defaultValue) {
        return configs.getOrDefault(key, defaultValue);
    }

    public int getInt(String configKey) {
        return (Integer) get(configKey);
    }

    public long getLong(String configKey) {
        return (Long) get(configKey);
    }

    public int getInt(String configKey, int defaultValue) {
        return (Integer) configs.getOrDefault(configKey, defaultValue);
    }

    public long getLong(String configKey, long defaultValue) {
        return (Long) configs.getOrDefault(configKey, defaultValue);
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return parameterContext.getParameter().getType().isAssignableFrom(WorkloadConfig.class);
    }

    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        return this;
    }

    public Map<String, Object> asMap() {
        return configs;
    }

    public void setDefaults(WorkloadConfig defaultConfig) {
        defaultConfig.asMap().forEach(configs::putIfAbsent);
    }
}
