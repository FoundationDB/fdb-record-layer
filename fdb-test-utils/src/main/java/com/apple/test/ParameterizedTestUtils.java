/*
 * BooleanArguments.java
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

package com.apple.test;

import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.provider.Arguments;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Helper utility class for interacting with {@link org.junit.jupiter.params.ParameterizedTest}s.
 */
public final class ParameterizedTestUtils {

    private ParameterizedTestUtils() {
    }

    /**
     * Provides a stream of boolean, named arguments.
     * @param trueName the name to provide for {@code true}
     * @param falseName the name to provide for {@code false}
     * @return a stream to be used as a return value for a {@link org.junit.jupiter.params.provider.MethodSource}
     */
    public static Stream<Named<Boolean>> booleans(String trueName, String falseName) {
        return Stream.of(Named.of(falseName, false), Named.of(trueName, true));
    }

    /**
     * Provides a stream of boolean, named arguments.
     * @param name the name to provide for {@code true}, {@code false} will prefix with {@code "!"}. If blank,
     * {@code "true"} and {@code "false"} will be used respectively.
     * @return a stream to be used as a return value for a {@link org.junit.jupiter.params.provider.MethodSource}
     */
    public static Stream<Named<Boolean>> booleans(String name) {
        if (name.isBlank()) {
            return booleans("true", "false");
        } else {
            return booleans(name, "!" + name);
        }
    }

    /**
     * Produce the cartesian product of the given streams as {@link Arguments}.
     * @param sources a list of sources to combine.
     * @return a stream of {@link Arguments} where the 0th element is from the first source, the 1st is from the first,
     * and so-on. Producing all combinations of an element from each source.
     */
    public static Stream<Arguments> cartesianProduct(Stream<?>... sources) {
        return cartesianProduct(
                Stream.of(sources)
                        .map(stream -> stream.collect(Collectors.toList())).collect(Collectors.toList()))
                .map(arguments -> Arguments.of(arguments.toArray()));
    }

    private static Stream<Stream<Object>> cartesianProduct(List<List<?>> sources) {
        if (sources.isEmpty()) {
            return Stream.of();
        }
        if (sources.size() == 1) {
            return sources.get(0).stream().map(Stream::of);
        }
        return sources.get(0).stream().flatMap(arg ->
                cartesianProduct(sources.subList(1, sources.size()))
                        .map(recursed -> Stream.concat(Stream.of(arg), recursed)));
    }
}
