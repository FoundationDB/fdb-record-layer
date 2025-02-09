/*
 * ExcludeYamlTestConfig.java
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

import com.apple.foundationdb.relational.yamltests.configs.YamlTestConfig;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * Mark specific {@link YamlTestConfig} that are designated test maintenance helper configs as enabled for the current
 * test in an {@link YamlTest} test class.
 * <p>
 *     Any config in {@link #value()} will be included for the annotated test. Any config not included will be skipped.
 * </p>
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface MaintainYamlTestConfig {
    /**
     * Any {@code YamlTestConfig} class in this list will run
     * {@link org.junit.jupiter.api.TestTemplate} test.
     */
    YamlTestConfigFilters value();
}
