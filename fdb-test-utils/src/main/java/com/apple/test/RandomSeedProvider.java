/*
 * RandomSeedProvider.java
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

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.support.AnnotationConsumer;

import java.util.stream.Stream;

/**
 * Provider of random seeds, generally used indirectly by applying the annotation {@link RandomSeedSource} to a
 * {@link org.junit.jupiter.params.ParameterizedTest} test.
 */
public class RandomSeedProvider implements ArgumentsProvider, AnnotationConsumer<RandomSeedSource> {
    private long[] fixedSeeds;

    @Override
    public void accept(final RandomSeedSource annotation) {
        this.fixedSeeds = annotation.value();
    }

    @Override
    public Stream<? extends Arguments> provideArguments(final ExtensionContext extensionContext) throws Exception {
        return RandomizedTestUtils.randomSeeds(fixedSeeds).map(Arguments::of);
    }
}
