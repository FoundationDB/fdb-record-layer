/*
 * ParameterizedTestUtilsTest.java
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
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ParameterizedTestUtilsTest {

    @Test
    void cartesianProduct1() {
        final Supplier<Stream<Integer>> integers = () -> Stream.of(1, 2, 3);
        assertEquals(integers.get().map(List::of).collect(Collectors.toList()),
                ParameterizedTestUtils.cartesianProduct(integers.get())
                        .map(args -> List.of(args.get())).collect(Collectors.toList()));
    }

    @Test
    void cartesianProduct2() {
        final Supplier<Stream<Integer>> integers = () -> Stream.of(1, 2, 3);
        final Supplier<Stream<Boolean>> booleans = () -> Stream.of(true, false);
        assertEquals(
                integers.get().flatMap(i ->
                                booleans.get().map(b -> List.of(i, b)))
                        .collect(Collectors.toList()),
                ParameterizedTestUtils.cartesianProduct(
                        integers.get(),
                        booleans.get()
                ).map(args -> List.of(args.get())).collect(Collectors.toList()));
    }

    @Test
    void cartesianProduct5() {
        final List<Integer> integers = List.of(1, 2, 3);
        final List<Boolean> booleans = List.of(true, false);
        final List<String> strings = List.of("foo", "bar", "something", "or other");
        final List<Named<Supplier<String>>> namedThings = List.of(Named.of("banana", () -> "banana"),
                Named.of("apple", () -> "apple"));
        assertEquals(
                integers.stream().flatMap(i ->
                                booleans.stream().flatMap(b ->
                                        strings.stream().flatMap(s ->
                                                namedThings.stream().map(named ->
                                                        List.of(i, b, s, named, 4.2)))))
                        .collect(Collectors.toList()),
                ParameterizedTestUtils.cartesianProduct(
                        integers.stream(),
                        booleans.stream(),
                        strings.stream(),
                        namedThings.stream(),
                        Stream.of(4.2)
                ).map(args -> List.of(args.get())).collect(Collectors.toList()));
    }
}
