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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.linear.DoubleRealVector;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.ToDoubleBiFunction;

import static com.apple.foundationdb.linear.RealVectorTest.createRandomHalfVector;

public final class CommonTestHelpers {
    /**
     * Time budget for a single logical database operation in a test, including any retries.
     * <p>
     * A test must never block indefinitely. {@link CompletableFuture#join()} is uninterruptible, so JUnit's
     * default per-test timeout ({@code junit.jupiter.execution.timeout.default}, see
     * {@code gradle/testing.gradle}) cannot stop a test that blocks on it: the interrupt JUnit sends is
     * swallowed, and the timeout failure is only reported once the test finishes on its own. Waiting with a
     * bounded {@code get} instead fails the test rather than hanging the whole suite.
     */
    public static final long ASYNC_TO_SYNC_TIMEOUT_MINUTES = 2L;

    private CommonTestHelpers() {
        // nothing
    }

    /**
     * Blocks until {@code future} completes, failing after {@link #ASYNC_TO_SYNC_TIMEOUT_MINUTES} minutes.
     * <p>
     * This module sits below {@code fdb-record-layer-core}, so the record layer's {@code asyncToSync} is not
     * reachable from here; this is the stand-in for it.
     *
     * @param future the future to wait for
     * @param <T> the type {@code future} completes with
     *
     * @return the result of {@code future}
     *
     * @throws ExecutionException if {@code future} completes exceptionally
     * @throws InterruptedException if the current thread is interrupted while waiting
     * @throws TimeoutException if {@code future} does not complete within the time budget
     */
    public static <T> T asyncToSync(@Nonnull final CompletableFuture<T> future)
            throws ExecutionException, InterruptedException, TimeoutException {
        return future.get(ASYNC_TO_SYNC_TIMEOUT_MINUTES, TimeUnit.MINUTES);
    }

    /**
     * Runs {@code retryable} in a transaction and blocks for its result, bounding the wait as described in
     * {@link #asyncToSync(CompletableFuture)}.
     * <p>
     * Prefer this over {@code db.run(tr -> something(tr).join())}: the time budget covers the entire
     * {@link Database#runAsync(Function)} retry loop, which is otherwise unbounded.
     *
     * @param db the database to run against
     * @param retryable the transactional operation to run
     * @param <T> the type the operation completes with
     *
     * @return the result of {@code retryable}
     *
     * @throws ExecutionException if the operation completes exceptionally
     * @throws InterruptedException if the current thread is interrupted while waiting
     * @throws TimeoutException if the operation does not complete within the time budget
     */
    public static <T> T runAsyncToSync(@Nonnull final Database db,
                                       @Nonnull final Function<? super Transaction, ? extends CompletableFuture<T>> retryable)
            throws ExecutionException, InterruptedException, TimeoutException {
        return asyncToSync(db.runAsync(retryable));
    }

    @Nonnull
    public static List<PrimaryKeyAndVector> randomVectors(@Nonnull final Random random, final int numDimensions,
                                                          final int numberOfVectors) {
        final ImmutableList.Builder<PrimaryKeyAndVector> resultBuilder = ImmutableList.builder();
        for (int i = 0; i < numberOfVectors; i ++) {
            final Tuple primaryKey = createPrimaryKey(i);
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
                new TreeSet<>(Comparator.comparing(PrimaryKeyVectorAndDistance::distance)
                        .thenComparing(PrimaryKeyVectorAndDistance::primaryKey));
        for (final PrimaryKeyAndVector vector : vectors) {
            final double distance = distanceFunction.applyAsDouble(vector.vector(), queryVector);
            final PrimaryKeyVectorAndDistance record =
                    new PrimaryKeyVectorAndDistance(vector.primaryKey(), vector.vector(), distance);
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

    /**
     * Returns a copy of {@code base} with independent Gaussian noise (scaled by {@code sigma}) added to every
     * component. Used to synthesize clusters of near-duplicate vectors around a handful of seed vectors.
     *
     * @param base the vector to perturb (not mutated)
     * @param sampler the Gaussian source; drives the per-component noise
     * @param sigma the standard deviation of the per-component noise
     *
     * @return a fresh perturbed vector
     */
    @Nonnull
    public static DoubleRealVector perturb(@Nonnull final DoubleRealVector base,
                                           @Nonnull final RandomHelpers.GaussianSampler sampler,
                                           final double sigma) {
        final double[] data = base.getData().clone();
        for (int i = 0; i < data.length; i++) {
            data[i] += sigma * sampler.nextGaussian();
        }
        return new DoubleRealVector(data);
    }
}
