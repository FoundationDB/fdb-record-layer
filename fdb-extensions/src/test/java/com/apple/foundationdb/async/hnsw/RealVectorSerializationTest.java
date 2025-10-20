/*
 * RealVectorSerializationTest.java
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

package com.apple.foundationdb.async.hnsw;

import com.apple.foundationdb.linear.DoubleRealVector;
import com.apple.foundationdb.linear.FloatRealVector;
import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.linear.RealVector;
import com.apple.test.RandomizedTestUtils;
import com.google.common.collect.ImmutableSet;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.Random;
import java.util.stream.Stream;

public class RealVectorSerializationTest {
    @Nonnull
    private static Stream<Arguments> randomSeedsWithNumDimensions() {
        return RandomizedTestUtils.randomSeeds(0xdeadc0deL, 0xfdb5ca1eL, 0xf005ba1L)
                .flatMap(seed -> ImmutableSet.of(3, 5, 10, 128, 768, 1000).stream()
                        .map(numDimensions -> Arguments.of(seed, numDimensions)));
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensions")
    void testSerializationDeserializationHalfVector(final long seed, final int numDimensions) {
        final Random random = new Random(seed);
        final HalfRealVector randomVector = createRandomHalfVector(random, numDimensions);
        final RealVector deserializedVector =
                StorageAdapter.vectorFromBytes(HNSW.DEFAULT_CONFIG_BUILDER.build(numDimensions), randomVector.getRawData());
        Assertions.assertThat(deserializedVector).isInstanceOf(HalfRealVector.class);
        Assertions.assertThat(deserializedVector).isEqualTo(randomVector);
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensions")
    void testSerializationDeserializationFloatVector(final long seed, final int numDimensions) {
        final Random random = new Random(seed);
        final FloatRealVector randomVector = createRandomFloatVector(random, numDimensions);
        final RealVector deserializedVector =
                StorageAdapter.vectorFromBytes(HNSW.DEFAULT_CONFIG_BUILDER.build(numDimensions), randomVector.getRawData());
        Assertions.assertThat(deserializedVector).isInstanceOf(FloatRealVector.class);
        Assertions.assertThat(deserializedVector).isEqualTo(randomVector);
    }

    @ParameterizedTest
    @MethodSource("randomSeedsWithNumDimensions")
    void testSerializationDeserializationDoubleVector(final long seed, final int numDimensions) {
        final Random random = new Random(seed);
        final DoubleRealVector randomVector = createRandomDoubleVector(random, numDimensions);
        final RealVector deserializedVector =
                StorageAdapter.vectorFromBytes(HNSW.DEFAULT_CONFIG_BUILDER.build(numDimensions), randomVector.getRawData());
        Assertions.assertThat(deserializedVector).isInstanceOf(DoubleRealVector.class);
        Assertions.assertThat(deserializedVector).isEqualTo(randomVector);
    }

    @Nonnull
    static HalfRealVector createRandomHalfVector(@Nonnull final Random random, final int dimensionality) {
        final double[] components = new double[dimensionality];
        for (int d = 0; d < dimensionality; d ++) {
            components[d] = random.nextDouble();
        }
        return new HalfRealVector(components);
    }

    @Nonnull
    public static FloatRealVector createRandomFloatVector(@Nonnull final Random random, final int dimensionality) {
        final float[] components = new float[dimensionality];
        for (int d = 0; d < dimensionality; d ++) {
            components[d] = random.nextFloat();
        }
        return new FloatRealVector(components);
    }

    @Nonnull
    public static DoubleRealVector createRandomDoubleVector(@Nonnull final Random random, final int dimensionality) {
        final double[] components = new double[dimensionality];
        for (int d = 0; d < dimensionality; d ++) {
            components[d] = random.nextDouble();
        }
        return new DoubleRealVector(components);
    }
}
