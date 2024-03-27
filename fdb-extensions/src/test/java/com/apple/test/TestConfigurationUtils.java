/*
 * TestConfigurationUtils.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2024 Apple Inc. and the FoundationDB project authors
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

package com.apple.test;

import java.util.stream.Stream;

/**
 * Utils around the configuration parameters and what tests should be run.
 */
public final class TestConfigurationUtils {
    private TestConfigurationUtils() {
    }

    /**
     * Provide the given parameters to a parameterized test only if we are running nightly tests.
     * @param arguments a list of arguments for a parameterized test
     * @param <T> the type of arguments
     * @return if we are running the nightly tests, the provided arguments, otherwise {@code Stream.of()}.
     */
    public static <T> Stream<T> onlyNightly(Stream<T> arguments) {
        if (includeNightlyTests()) {
            return arguments;
        } else {
            return Stream.of();
        }
    }


    private static boolean includeNightlyTests() {
        return Boolean.parseBoolean(System.getProperty("tests.nightly", "false"));
    }
}
