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
 * Helper class for generating boolean arguments for {@link org.junit.jupiter.params.ParameterizedTest}s.
 */
public class ParameterizedTestUtils {

    /**
     * Provides a stream of boolean, named arguments.
     * @param trueName the name to provide for {@code true}
     * @param falseName the name to provide for {@code false}
     * @return a stream to be used as a return value for a {@link org.junit.jupiter.params.provider.MethodSource}
     */
    public static Stream<Named<Boolean>> booleans(String trueName, String falseName) {
        return Stream.of(Named.of(trueName, true), Named.of(falseName, false));
    }

    /**
     * Provides a stream of boolean, named arguments.
     * @param name the name to provide for {@code true}, {@code false} will prefix with {@code "not "}
     * @return a stream to be used as a return value for a {@link org.junit.jupiter.params.provider.MethodSource}
     */
    public static Stream<Named<Boolean>> booleans(String name) {
        return Stream.of(Named.of(name, true), Named.of("not " + name, false));
    }

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
