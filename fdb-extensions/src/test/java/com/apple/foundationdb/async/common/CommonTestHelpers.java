/*
 * CommonTestHelpers.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2015-2026 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.async.common;

import com.apple.foundationdb.linear.HalfRealVector;
import com.apple.foundationdb.linear.Metric;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.tuple.Tuple;
import com.google.common.base.Verify;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeSet;
import java.util.function.ToDoubleBiFunction;

import static com.apple.foundationdb.linear.RealVectorTest.createRandomHalfVector;

public final class CommonTestHelpers {
    private CommonTestHelpers() {
        // nothing
    }

    @Nonnull
    public static List<PrimaryKeyAndVector> randomVectors(@Nonnull final Random random, final int numDimensions,
                                                          final int numberOfVectors) {
        final ImmutableList.Builder<PrimaryKeyAndVector> resultBuilder = ImmutableList.builder();
        for (int i = 0; i < numberOfVectors; i ++) {
            final var primaryKey = createPrimaryKey(i);
            final HalfRealVector dataVector = createRandomHalfVector(random, numDimensions);
            resultBuilder.add(new PrimaryKeyAndVector(primaryKey, dataVector));
        }
        return resultBuilder.build();
    }

    @Nonnull
    public static List<PrimaryKeyAndVector> pickRandomVectors(@Nonnull final Random random,
                                                              @Nonnull final Collection<PrimaryKeyAndVector> vectors,
                                                              final int numberOfVectors) {
        Verify.verify(numberOfVectors <= vectors.size());
        final List<PrimaryKeyAndVector> remainingVectors = Lists.newArrayList(vectors);
        final ImmutableList.Builder<PrimaryKeyAndVector> resultBuilder = ImmutableList.builder();
        for (int i = 0; i < numberOfVectors; i ++) {
            resultBuilder.add(remainingVectors.remove(random.nextInt(remainingVectors.size())));
        }
        return resultBuilder.build();
    }

    @Nonnull
    public static NavigableSet<PrimaryKeyVectorAndDistance> orderedByDistances(@Nonnull final Metric metric,
                                                                               @Nonnull final List<PrimaryKeyAndVector> vectors,
                                                                               @Nonnull final RealVector queryVector) {
        return orderedByDistances(metric::distance, vectors, queryVector);
    }

    @Nonnull
    public static NavigableSet<PrimaryKeyVectorAndDistance> orderedByDistances(@Nonnull final ToDoubleBiFunction<RealVector, RealVector> distanceFunction,
                                                                               @Nonnull final List<PrimaryKeyAndVector> vectors,
                                                                               @Nonnull final RealVector queryVector) {
        final TreeSet<PrimaryKeyVectorAndDistance> vectorsOrderedByDistance =
                new TreeSet<>(Comparator.comparing(PrimaryKeyVectorAndDistance::getDistance)
                        .thenComparing(PrimaryKeyAndVector::getPrimaryKey));
        for (final PrimaryKeyAndVector vector : vectors) {
            final double distance = distanceFunction.applyAsDouble(vector.getVector(), queryVector);
            final PrimaryKeyVectorAndDistance record =
                    new PrimaryKeyVectorAndDistance(vector.getPrimaryKey(), vector.getVector(), distance);
            vectorsOrderedByDistance.add(record);
        }
        return vectorsOrderedByDistance;
    }

    @Nonnull
    public static Tuple createRandomPrimaryKey(final @Nonnull Random random) {
        return createPrimaryKey(random.nextLong());
    }

    @Nonnull
    public static Tuple createPrimaryKey(final long nextId) {
        return Tuple.from(nextId);
    }
}
