/*
 * YamlTest.java
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

import org.junit.jupiter.api.extension.ExtendWith;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * An annotation for test classes that run {@code .yamsql} files.
 * <p>
 *     Test classes annotated with this should have all their tests annotated with
 *     {@link org.junit.jupiter.api.TestTemplate}, instead of {@link org.junit.jupiter.api.Test}.
 *     Note: This doesn't work with {@link org.junit.jupiter.params.ParameterizedTest}.
 *     Each {@link org.junit.jupiter.api.TestTemplate} will be run with each of the varying
 *     {@link com.apple.foundationdb.relational.yamltests.configs.YamlTestConfig}, and passed in a parameter:
 *     {@link YamlTest.Runner} to run the {@code .yamsql} file.
 * </p>
 * <p>
 *     If a specific test cannot be run with a specific config due to a bug, it can be ignored by either checking it in
 *     {@link com.apple.foundationdb.relational.yamltests.configs.YamlTestConfig#assumeSupport}, or by adding the
 *     annotation {@link ExcludeYamlTestConfig}. Ideally, these are short lived, primarily to support adding a config,
 *     and then fixing some tests that may fail with that config.
 * </p>
 */
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(YamlTestExtension.class)
public @interface YamlTest {
    /**
     * Simple interface to run a {@code .yamsql} file, based on the config.
     * <p>
     *     This is primarily a nested class because I couldn't figure out a better way to avoid a naming conflict with
     *     the existing {@link YamlRunner}.
     * </p>
     */
    interface Runner {
        /**
         * Run a {@code .yamsql} test.
         * @param fileName the filename of the {@code .yamsql} resource
         * @throws Exception if the test has issues
         */
        void runYamsql(String fileName) throws Exception;

        /**
         * Run a {@code .yamsql} test, but update all the `explain` assertions in the file with what `explain` actually
         * returns, so that you, as a developer, can compare the explain results in a diff.
         * This should never be committed
         * @param fileName the filename of the {@code .yamsql} resource
         * @param correctExplain {@code true} to replace the expected explains in the associated file
         * @throws Exception if the test has issues
         */
        void runYamsql(String fileName, boolean correctExplain) throws Exception;
    }
}
