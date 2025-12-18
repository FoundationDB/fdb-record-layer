/*
 * DataRecordsTest.java
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

import com.apple.foundationdb.linear.AffineOperator;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.RealVectorTest;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.RandomSeedSource;
import com.google.common.collect.ImmutableList;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.function.BiFunction;
import java.util.function.Function;

class DataRecordsTest {
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testAccessInfo(final long randomSeed) {
        assertHashCodeEqualsToString(randomSeed, DataRecordsTest::accessInfo, DataRecordsTest::accessInfo);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testAggregatedVector(final long randomSeed) {
        assertHashCodeEqualsToString(randomSeed, DataRecordsTest::aggregatedVector, DataRecordsTest::aggregatedVector);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testCompactNode(final long randomSeed) {
        final Random random = new Random(randomSeed);
        final long dependentRandomSeed = random.nextLong();

        final CompactNode compactNode1 = compactNode(new Random(dependentRandomSeed));
        final CompactNode compactNode1Clone = compactNode(new Random(dependentRandomSeed));
        Assertions.assertThat(compactNode1).hasToString(compactNode1Clone.toString());

        final CompactNode compactNode2 = compactNode(random, compactNode1);
        Assertions.assertThat(compactNode1).doesNotHaveToString(compactNode2.toString());

        Assertions.assertThatThrownBy(compactNode1::asInliningNode).isInstanceOf(IllegalStateException.class);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testInliningNode(final long randomSeed) {
        final Random random = new Random(randomSeed);
        final long dependentRandomSeed = random.nextLong();

        final InliningNode inliningNode1 = inliningNode(new Random(dependentRandomSeed));
        final InliningNode inliningNode1Clone = inliningNode(new Random(dependentRandomSeed));
        Assertions.assertThat(inliningNode1).hasToString(inliningNode1Clone.toString());

        final InliningNode inliningNode2 = inliningNode(random, inliningNode1);
        Assertions.assertThat(inliningNode1).doesNotHaveToString(inliningNode2.toString());

        Assertions.assertThatThrownBy(inliningNode1::asCompactNode).isInstanceOf(IllegalStateException.class);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testEntryNodeReference(final long randomSeed) {
        assertHashCodeEqualsToString(randomSeed, DataRecordsTest::entryNodeReference, DataRecordsTest::entryNodeReference);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testNodeReference(final long randomSeed) {
        assertHashCodeEqualsToString(randomSeed, DataRecordsTest::nodeReference, DataRecordsTest::nodeReference);
        final NodeReference nodeReference = nodeReference(new Random(randomSeed));
        Assertions.assertThat(nodeReference.isNodeReferenceWithVector()).isFalse();
        Assertions.assertThatThrownBy(nodeReference::asNodeReferenceWithVector).isInstanceOf(IllegalStateException.class);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testNodeReferenceWithVector(final long randomSeed) {
        assertHashCodeEqualsToString(randomSeed, DataRecordsTest::nodeReferenceWithVector,
                DataRecordsTest::nodeReferenceWithVector);
        final NodeReferenceWithVector nodeReference = nodeReferenceWithVector(new Random(randomSeed));
        Assertions.assertThat(nodeReference.isNodeReferenceWithVector()).isTrue();
        Assertions.assertThat(nodeReference.asNodeReferenceWithVector()).isInstanceOf(NodeReferenceWithVector.class);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testNodeReferenceWithDistance(final long randomSeed) {
        assertHashCodeEqualsToString(randomSeed, DataRecordsTest::nodeReferenceWithDistance,
                DataRecordsTest::nodeReferenceWithDistance);
        final NodeReferenceWithDistance nodeReference = nodeReferenceWithDistance(new Random(randomSeed));
        Assertions.assertThat(nodeReference.isNodeReferenceWithVector()).isTrue();
        Assertions.assertThat(nodeReference.asNodeReferenceWithVector()).isInstanceOf(NodeReferenceWithDistance.class);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testResultEntry(final long randomSeed) {
        assertHashCodeEqualsToString(randomSeed, DataRecordsTest::resultEntry, DataRecordsTest::resultEntry);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testNodeReferenceAndNode(final long randomSeed) {
        assertToString(randomSeed, DataRecordsTest::nodeReferenceAndNode, DataRecordsTest::nodeReferenceAndNode);
    }

    private static <T> void assertToString(final long randomSeed,
                                           @Nonnull final Function<Random, T> createFunction,
                                           @Nonnull final BiFunction<Random, T, T> createDifferentFunction) {
        final Random random = new Random(randomSeed);
        final long dependentRandomSeed = random.nextLong();
        final T t1 = createFunction.apply(new Random(dependentRandomSeed));
        final T t1Clone = createFunction.apply(new Random(dependentRandomSeed));
        Assertions.assertThat(t1).hasToString(t1Clone.toString());

        final T t2 = createDifferentFunction.apply(random, t1);
        Assertions.assertThat(t1).doesNotHaveToString(t2.toString());
    }

    private static <T> void assertHashCodeEqualsToString(final long randomSeed,
                                                         @Nonnull final Function<Random, T> createFunction,
                                                         @Nonnull final BiFunction<Random, T, T> createDifferentFunction) {
        final Random random = new Random(randomSeed);
        final long dependentRandomSeed = random.nextLong();
        final T t1 = createFunction.apply(new Random(dependentRandomSeed));
        final T t1Clone = createFunction.apply(new Random(dependentRandomSeed));
        Assertions.assertThat(t1.hashCode()).isEqualTo(t1Clone.hashCode());
        Assertions.assertThat(t1).isEqualTo(t1Clone);
        Assertions.assertThat(t1).hasToString(t1Clone.toString());

        final T t2 = createDifferentFunction.apply(random, t1);
        Assertions.assertThat(t1).isNotEqualTo(t2);
        Assertions.assertThat(t1).doesNotHaveToString(t2.toString());
    }

    @Nonnull
    private static NodeReferenceAndNode<NodeReferenceWithDistance, NodeReferenceWithVector>
                   nodeReferenceAndNode(@Nonnull final Random random) {
        return new NodeReferenceAndNode<>(nodeReferenceWithDistance(random), inliningNode(random));
    }

    @Nonnull
    private static NodeReferenceAndNode<NodeReferenceWithDistance, NodeReferenceWithVector>
                   nodeReferenceAndNode(@Nonnull final Random random,
                                        @Nonnull final NodeReferenceAndNode<NodeReferenceWithDistance, NodeReferenceWithVector> original) {
        return new NodeReferenceAndNode<>(nodeReferenceWithDistance(random, original.getNodeReference()),
                inliningNode(random, original.getNode().asInliningNode()));
    }

    @Nonnull
    private static ResultEntry resultEntry(@Nonnull final Random random) {
        return new ResultEntry(primaryKey(random), rawVector(random), random.nextDouble(), random.nextInt(100));
    }

    @Nonnull
    private static ResultEntry resultEntry(@Nonnull final Random random, @Nonnull final ResultEntry original) {
        return new ResultEntry(primaryKey(random, original.getPrimaryKey()),
                rawVector(random, Objects.requireNonNull(original.getVector())),
                differentDouble(random, original.getDistance()),
                differentInteger(random, original.getRankOrRowNumber(), 100));
    }

    @Nonnull
    private static CompactNode compactNode(@Nonnull final Random random) {
        return CompactNode.factory()
                .create(primaryKey(random), vector(random), nodeReferences(random))
                .asCompactNode();
    }

    @Nonnull
    private static CompactNode compactNode(@Nonnull final Random random, @Nonnull CompactNode original) {
        return CompactNode.factory()
                .create(primaryKey(random, original.getPrimaryKey()),
                        vector(random, original.getVector()),
                        nodeReferences(random, original.getNeighbors()))
                .asCompactNode();
    }

    @Nonnull
    private static InliningNode inliningNode(@Nonnull final Random random) {
        return InliningNode.factory()
                .create(primaryKey(random), null, nodeReferenceWithVectors(random))
                .asInliningNode();
    }

    private static InliningNode inliningNode(@Nonnull final Random random, @Nonnull final InliningNode original) {
        return InliningNode.factory()
                .create(primaryKey(random, original.getPrimaryKey()),
                        null,
                        nodeReferenceWithVectors(random, original.getNeighbors()))
                .asInliningNode();
    }

    @Nonnull
    private static NodeReferenceWithDistance nodeReferenceWithDistance(@Nonnull final Random random) {
        return new NodeReferenceWithDistance(primaryKey(random), vector(random), random.nextDouble());
    }

    @Nonnull
    private static NodeReferenceWithDistance nodeReferenceWithDistance(@Nonnull final Random random,
                                                                       @Nonnull final NodeReferenceWithDistance original) {
        return new NodeReferenceWithDistance(
                primaryKey(random, original.getPrimaryKey()),
                vector(random, original.getVector()),
                differentDouble(random, original.getDistance()));
    }

    @Nonnull
    private static List<NodeReferenceWithVector> nodeReferenceWithVectors(@Nonnull final Random random) {
        return nodeReferenceWithVectors(random, null);
    }

    @Nonnull
    private static List<NodeReferenceWithVector> nodeReferenceWithVectors(@Nonnull final Random random,
                                                                          @Nullable final List<NodeReferenceWithVector> original) {
        final int size = original == null
                         ? random.nextInt(20)
                         : differentInteger(random, original.size(), 20);
        final ImmutableList.Builder<NodeReferenceWithVector> resultBuilder = ImmutableList.builder();
        for (int i = 0; i < size; i ++) {
            resultBuilder.add(nodeReferenceWithVector(random));
        }
        return resultBuilder.build();
    }

    @Nonnull
    private static NodeReferenceWithVector nodeReferenceWithVector(@Nonnull final Random random) {
        return new NodeReferenceWithVector(primaryKey(random), vector(random));
    }

    @Nonnull
    private static NodeReferenceWithVector nodeReferenceWithVector(@Nonnull final Random random,
                                                                   @Nonnull final NodeReferenceWithVector original) {
        return new NodeReferenceWithVector(primaryKey(random, original.getPrimaryKey()),
                vector(random, original.getVector()));
    }

    @Nonnull
    private static List<NodeReference> nodeReferences(@Nonnull final Random random) {
        return nodeReferences(random, null);
    }

    @Nonnull
    private static List<NodeReference> nodeReferences(@Nonnull final Random random,
                                                      @Nullable final List<NodeReference> original) {
        final int size = original == null
                         ? random.nextInt(20)
                         : differentInteger(random, original.size(), 20);
        final ImmutableList.Builder<NodeReference> resultBuilder = ImmutableList.builder();
        for (int i = 0; i < size; i ++) {
            resultBuilder.add(nodeReference(random));
        }
        return resultBuilder.build();
    }

    @Nonnull
    private static NodeReference nodeReference(@Nonnull final Random random) {
        return new NodeReference(primaryKey(random));
    }

    @Nonnull
    private static NodeReference nodeReference(@Nonnull final Random random, @Nonnull NodeReference original) {
        return new NodeReference(primaryKey(random, original.getPrimaryKey()));
    }

    @Nonnull
    private static AggregatedVector aggregatedVector(@Nonnull final Random random) {
        return new AggregatedVector(random.nextInt(100), vector(random));
    }

    @Nonnull
    private static AggregatedVector aggregatedVector(@Nonnull final Random random,
                                                     @Nonnull final AggregatedVector original) {
        return new AggregatedVector(differentInteger(random, original.getPartialCount(), 100),
                vector(random, original.getPartialVector()));
    }

    @Nonnull
    private static AccessInfo accessInfo(@Nonnull final Random random) {
        return new AccessInfo(entryNodeReference(random), random.nextLong(), rawVector(random));
    }

    @Nonnull
    private static AccessInfo accessInfo(@Nonnull final Random random, @Nonnull final AccessInfo original) {
        return new AccessInfo(entryNodeReference(random, original.getEntryNodeReference()),
                differentLong(random, original.getRotatorSeed()),
                rawVector(random, Objects.requireNonNull(original.getNegatedCentroid())));
    }

    @Nonnull
    private static EntryNodeReference entryNodeReference(@Nonnull final Random random) {
        return new EntryNodeReference(primaryKey(random), vector(random), random.nextInt(10));
    }

    @Nonnull
    private static EntryNodeReference entryNodeReference(@Nonnull final Random random,
                                                         @Nonnull final EntryNodeReference original) {
        return new EntryNodeReference(primaryKey(random, original.getPrimaryKey()),
                vector(random, original.getVector()),
                differentInteger(random, original.getLayer(), 10));
    }

    @Nonnull
    private static Tuple primaryKey(@Nonnull final Random random) {
        return Tuple.from(random.nextInt(100));
    }

    @Nonnull
    private static Tuple primaryKey(@Nonnull final Random random, @Nonnull final Tuple original) {
        return Tuple.from(differentInteger(random, Math.toIntExact(original.getLong(0)), 100));
    }

    @Nonnull
    private static Transformed<RealVector> vector(@Nonnull final Random random) {
        return AffineOperator.identity().transform(rawVector(random));
    }

    @Nonnull
    private static Transformed<RealVector> vector(@Nonnull final Random random,
                                                  @Nonnull final Transformed<RealVector> original) {
        return AffineOperator.identity().transform(rawVector(random, original.getUnderlyingVector()));
    }

    @Nonnull
    private static RealVector rawVector(@Nonnull final Random random) {
        return RealVectorTest.createRandomDoubleVector(random, 768);
    }

    @Nonnull
    private static RealVector rawVector(@Nonnull final Random random, @Nonnull final RealVector original) {
        RealVector randomVector;
        do {
            randomVector = RealVectorTest.createRandomDoubleVector(random, 768);
        } while (randomVector.equals(original));
        return randomVector;
    }

    private static int differentInteger(@Nonnull final Random random, final int original, final int bound) {
        int randomInteger;
        do {
            randomInteger = random.nextInt(bound);
        } while (randomInteger == original);
        return randomInteger;
    }

    private static long differentLong(@Nonnull final Random random, final long original) {
        long randomLong;
        do {
            randomLong = random.nextLong();
        } while (randomLong == original);
        return randomLong;
    }

    private static double differentDouble(@Nonnull final Random random, final double original) {
        double randomDouble;
        do {
            randomDouble = random.nextDouble();
        } while (randomDouble == original);
        return randomDouble;
    }
}
