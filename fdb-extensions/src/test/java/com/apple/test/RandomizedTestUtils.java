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

import javax.annotation.Nonnull;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * A Utility class for adding randomness to tests.
 */
public final class RandomizedTestUtils {
    private static final long FIXED_SEED = 0xf17ed5eedL;

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
            return Stream.generate(() -> randomArguments.apply(random))
                    .limit(getIterations());
        } else {
            return Stream.of();
        }
    }

    /**
     * Return a stream of random {@code long}s to be used to seed random number generators.
     * This can be supplied to a {@link org.junit.jupiter.params.ParameterizedTest} to run a test
     * under different scenarios. This is preferred over, say, a {@code Stream<Random>} because
     * the seed can be included in the test display name, which means that a failing seed can be
     * recorded and the test re-run with that seed.
     *
     * <p>
     * To ensure tests are consistent when random tests are not included, users are encouraged
     * to provide a set of random seeds that are always included in the returned stream via
     * the {@code staticSeeds} parameter. However, if this argument is left empty, this will
     * still always include at least one fixed seed, {@value FIXED_SEED}.
     * </p>
     *
     * @param staticSeeds a set of seeds to always include in the returned random seeds
     * @return a stream of random {@code long}s to initialize {@link Random}s
     */
    @Nonnull
    public static Stream<Long> randomSeeds(long... staticSeeds) {
        LongStream longStream = staticSeeds.length == 0 ? LongStream.of(FIXED_SEED) : LongStream.of(staticSeeds);
        if (includeRandomTests()) {
            Random random = ThreadLocalRandom.current();
            longStream = LongStream.concat(longStream,
                    LongStream.generate(random::nextLong).limit(getIterations()));
        }
        return longStream.boxed();
    }

    private static int getIterations() {
        return Integer.parseInt(System.getProperty("tests.iterations", "0"));
    }

    private static boolean includeRandomTests() {
        return Boolean.parseBoolean(System.getProperty("tests.includeRandom", "false"));
    }
}
