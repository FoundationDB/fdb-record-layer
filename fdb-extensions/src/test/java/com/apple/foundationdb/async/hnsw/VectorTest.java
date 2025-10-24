/*
 * VectorTest.java
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

import com.christianheina.langx.half4j.Half;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nonnull;
import java.util.Random;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class VectorTest {
    private static Stream<Long> randomSeeds() {
        return LongStream.generate(() -> new Random().nextLong())
                .limit(5)
                .boxed();
    }

    @ParameterizedTest(name = "seed={0}")
    @MethodSource("randomSeeds")
    void testSerializationDeserializationHalfVector(final long seed) {
        final Random random = new Random(seed);
        final Vector.HalfVector randomVector = createRandomHalfVector(random, 128);
        final Vector deserializedVector = StorageAdapter.vectorFromBytes(randomVector.getRawData());
        Assertions.assertThat(deserializedVector).isInstanceOf(Vector.HalfVector.class);
        Assertions.assertThat(deserializedVector).isEqualTo(randomVector);
    }

    @ParameterizedTest(name = "seed={0}")
    @MethodSource("randomSeeds")
    void testSerializationDeserializationDoubleVector(final long seed) {
        final Random random = new Random(seed);
        final Vector.DoubleVector randomVector = createRandomDoubleVector(random, 128);
        final Vector deserializedVector = StorageAdapter.vectorFromBytes(randomVector.getRawData());
        Assertions.assertThat(deserializedVector).isInstanceOf(Vector.DoubleVector.class);
        Assertions.assertThat(deserializedVector).isEqualTo(randomVector);
    }

    @Nonnull
    static Vector.HalfVector createRandomHalfVector(@Nonnull final Random random, final int dimensionality) {
        final Half[] components = new Half[dimensionality];
        for (int d = 0; d < dimensionality; d ++) {
            // don't ask
            components[d] = HNSWHelpers.halfValueOf(random.nextDouble());
        }
        return new Vector.HalfVector(components);
    }

    @Nonnull
    static Vector.DoubleVector createRandomDoubleVector(@Nonnull final Random random, final int dimensionality) {
        final double[] components = new double[dimensionality];
        for (int d = 0; d < dimensionality; d ++) {
            // don't ask
            components[d] = random.nextDouble();
        }
        return new Vector.DoubleVector(components);
    }
}
