/*
 * RandomizedTestUtils.java
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

import org.junit.jupiter.params.provider.Arguments;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * A Utility class for adding randomness to tests.
 */
public final class RandomizedTestUtils {
    private RandomizedTestUtils() {
    }

    /**
     * Return a stream of randomly generated arguments based on the gradle properties.
     * <p>
     *     Tip: Use {@code Stream.concat(streamOfFixedArguments, RandomizedTestUtils.randomArguments(random -> { ... }} to have some fixed
     *     arguments, and also add randomized arguments when including random tests.
     * </p>
     * @param randomArguments A mapper from a {@link Random} to the arguments to be provided to
     * @return a stream of {@link Arguments} for a {@link org.junit.jupiter.params.ParameterizedTest}
     */
    public static Stream<Arguments> randomArguments(Function<Random, Arguments> randomArguments) {
        if (includeRandomTests()) {
            Random random = ThreadLocalRandom.current();
            return IntStream.range(0, getIterations()).mapToObj(i -> randomArguments.apply(random));
        } else {
            return Stream.of();
        }
    }

    private static int getIterations() {
        return Integer.parseInt(System.getProperty("tests.iterations", "0"));
    }

    private static boolean includeRandomTests() {
        return Boolean.parseBoolean(System.getProperty("tests.includeRandom", "false"));
    }
}
