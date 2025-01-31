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
 */
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(YamlTestExtension.class)
public @interface YamlTest {
    interface Runner {
        /**
         * Run a {@code .yamsql} test.
         * @param fileName the filename of the {@code .yamsql} resource
         * @throws Exception if the test has issues
         */
        void run(String fileName) throws Exception;

        /**
         * Run a {@code .yamsql} test, but update all the `explain` assertions in the file with what `explain` actually
         * returns, so that you, as a developer, can compare the explain results in a diff.
         * This should never be committed
         * @param fileName the filename of the {@code .yamsql} resource
         * @param correctExplain {@code true} to replace the expected explains in the associated file
         * @throws Exception if the test has issues
         */
        void run(String fileName, boolean correctExplain) throws Exception;
    }
}
