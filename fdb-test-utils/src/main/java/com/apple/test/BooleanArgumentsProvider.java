/*
 * BooleanArgumentsProvider.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2019 Apple Inc. and the FoundationDB project authors
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

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.support.AnnotationConsumer;
import org.junit.jupiter.params.support.ParameterDeclarations;

import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Argument provider for the {@link BooleanSource} annotation for providing booleans to parameterized
 * tests. Regardless of the source or context, this always returns {@code false} and {@code true} in that order for each
 * argument.
 */
class BooleanArgumentsProvider implements ArgumentsProvider, AnnotationConsumer<BooleanSource> {
    private String[] names;

    @Override
    public void accept(BooleanSource booleanSource) {
        this.names = booleanSource.value();
    }

    @Override
    public Stream<? extends Arguments> provideArguments(final ParameterDeclarations parameterDeclarations,
                                                        final ExtensionContext extensionContext) throws Exception {
        if (names.length == 0) {
            throw new IllegalStateException("@BooleanSource has an empty list of names");
        }
        return ParameterizedTestUtils.cartesianProduct(Arrays.stream(names)
                .map(ParameterizedTestUtils::booleans)
                .toArray(Stream[]::new));
    }
}
