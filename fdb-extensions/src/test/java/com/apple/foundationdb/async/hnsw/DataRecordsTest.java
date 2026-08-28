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

import com.apple.foundationdb.async.common.AggregatedVector;
import com.apple.foundationdb.async.common.ResultEntry;
import com.apple.foundationdb.linear.AffineOperator;
import com.apple.foundationdb.linear.RealVector;
import com.apple.foundationdb.linear.RealVectorTest;
import com.apple.foundationdb.linear.Transformed;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.test.RandomSeedSource;
import com.google.common.collect.ImmutableList;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;

import org.jspecify.annotations.Nullable;
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

        final Random random = new Random(randomSeed);
        final NodeReferenceAndNode<NodeReferenceWithDistance, NodeReferenceWithVector> nodeReferenceAndNode1 =
                nodeReferenceAndNode(random);
        final NodeReferenceAndNode<NodeReferenceWithDistance, NodeReferenceWithVector> nodeReferenceAndNode2 =
                nodeReferenceAndNode(random);

        final var nodeReferenceAndNodes =
                ImmutableList.of(nodeReferenceAndNode1, nodeReferenceAndNode2);
        Assertions.assertThat(NodeReferenceAndNode.references(nodeReferenceAndNodes))
                .containsExactly(nodeReferenceAndNode1.getNodeReference(), nodeReferenceAndNode2.getNodeReference());
        Assertions.assertThat(NodeReferenceAndNode.primaryKeys(nodeReferenceAndNodes))
                .containsExactly(nodeReferenceAndNode1.getNodeReference().getPrimaryKey(),
                        nodeReferenceAndNode2.getNodeReference().getPrimaryKey());
    }

    private static <T> void assertToString(final long randomSeed,
                                           final Function<Random, T> createFunction,
                                           final BiFunction<Random, T, T> createDifferentFunction) {
        final Random random = new Random(randomSeed);
        final long dependentRandomSeed = random.nextLong();
        final T t1 = createFunction.apply(new Random(dependentRandomSeed));
        final T t1Clone = createFunction.apply(new Random(dependentRandomSeed));
        Assertions.assertThat(t1).hasToString(t1Clone.toString());

        final T t2 = createDifferentFunction.apply(random, t1);
        Assertions.assertThat(t1).doesNotHaveToString(t2.toString());
    }

    private static <T> void assertHashCodeEqualsToString(final long randomSeed,
                                                         final Function<Random, T> createFunction,
                                                         final BiFunction<Random, T, T> createDifferentFunction) {
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

    private static NodeReferenceAndNode<NodeReferenceWithDistance, NodeReferenceWithVector>
                   nodeReferenceAndNode(final Random random) {
        return new NodeReferenceAndNode<>(nodeReferenceWithDistance(random), inliningNode(random));
    }

    private static NodeReferenceAndNode<NodeReferenceWithDistance, NodeReferenceWithVector>
                   nodeReferenceAndNode(final Random random,
                                        final NodeReferenceAndNode<NodeReferenceWithDistance, NodeReferenceWithVector> original) {
        return new NodeReferenceAndNode<>(nodeReferenceWithDistance(random, original.getNodeReference()),
                inliningNode(random, original.getNode().asInliningNode()));
    }

    private static ResultEntry resultEntry(final Random random) {
        return new ResultEntry(primaryKey(random), rawVector(random), null, random.nextDouble(),
                random.nextInt(100));
    }

    private static ResultEntry resultEntry(final Random random, final ResultEntry original) {
        return new ResultEntry(primaryKey(random, original.primaryKey()),
                rawVector(random, Objects.requireNonNull(original.vector())), null,
                differentDouble(random, original.distance()),
                differentInteger(random, original.rankOrRowNumber(), 100));
    }

    private static CompactNode compactNode(final Random random) {
        return CompactNode.factory()
                .create(primaryKey(random), vector(random), null, nodeReferences(random))
                .asCompactNode();
    }

    private static CompactNode compactNode(final Random random, CompactNode original) {
        return CompactNode.factory()
                .create(primaryKey(random, original.getPrimaryKey()), vector(random, original.getVector()),
                        null, nodeReferences(random, original.getNeighbors())
                )
                .asCompactNode();
    }

    private static InliningNode inliningNode(final Random random) {
        return InliningNode.factory()
                .create(primaryKey(random), null, null, nodeReferenceWithVectors(random))
                .asInliningNode();
    }

    private static InliningNode inliningNode(final Random random, final InliningNode original) {
        return InliningNode.factory()
                .create(primaryKey(random, original.getPrimaryKey()),
                        null, null, nodeReferenceWithVectors(random, original.getNeighbors())
                )
                .asInliningNode();
    }

    private static NodeReferenceWithDistance nodeReferenceWithDistance(final Random random) {
        return new NodeReferenceWithDistance(primaryKey(random), vector(random), random.nextDouble());
    }

    private static NodeReferenceWithDistance nodeReferenceWithDistance(final Random random,
                                                                       final NodeReferenceWithDistance original) {
        return new NodeReferenceWithDistance(
                primaryKey(random, original.getPrimaryKey()),
                vector(random, original.getVector()),
                differentDouble(random, original.getDistance()));
    }

    private static List<NodeReferenceWithVector> nodeReferenceWithVectors(final Random random) {
        return nodeReferenceWithVectors(random, null);
    }

    private static List<NodeReferenceWithVector> nodeReferenceWithVectors(final Random random,
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

    private static NodeReferenceWithVector nodeReferenceWithVector(final Random random) {
        return new NodeReferenceWithVector(primaryKey(random), vector(random));
    }

    private static NodeReferenceWithVector nodeReferenceWithVector(final Random random,
                                                                   final NodeReferenceWithVector original) {
        return new NodeReferenceWithVector(primaryKey(random, original.getPrimaryKey()),
                vector(random, original.getVector()));
    }

    private static List<NodeReference> nodeReferences(final Random random) {
        return nodeReferences(random, null);
    }

    private static List<NodeReference> nodeReferences(final Random random,
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

    private static NodeReference nodeReference(final Random random) {
        return new NodeReference(primaryKey(random));
    }

    private static NodeReference nodeReference(final Random random, NodeReference original) {
        return new NodeReference(primaryKey(random, original.getPrimaryKey()));
    }

    private static AggregatedVector aggregatedVector(final Random random) {
        return new AggregatedVector(random.nextInt(100), vector(random));
    }

    private static AggregatedVector aggregatedVector(final Random random,
                                                     final AggregatedVector original) {
        return new AggregatedVector(differentInteger(random, original.partialCount(), 100),
                vector(random, original.partialVector()));
    }

    private static AccessInfo accessInfo(final Random random) {
        return new AccessInfo(entryNodeReference(random), random.nextLong(), rawVector(random));
    }

    private static AccessInfo accessInfo(final Random random, final AccessInfo original) {
        return new AccessInfo(entryNodeReference(random, original.getEntryNodeReference()),
                differentLong(random, original.getRotatorSeed()),
                rawVector(random, Objects.requireNonNull(original.getNegatedCentroid())));
    }

    private static EntryNodeReference entryNodeReference(final Random random) {
        return new EntryNodeReference(primaryKey(random), vector(random), random.nextInt(10));
    }

    private static EntryNodeReference entryNodeReference(final Random random,
                                                         final EntryNodeReference original) {
        return new EntryNodeReference(primaryKey(random, original.getPrimaryKey()),
                vector(random, original.getVector()),
                differentInteger(random, original.getLayer(), 10));
    }

    private static Tuple primaryKey(final Random random) {
        return Tuple.from(random.nextInt(100));
    }

    private static Tuple primaryKey(final Random random, final Tuple original) {
        return Tuple.from(differentInteger(random, Math.toIntExact(original.getLong(0)), 100));
    }

    private static Transformed<RealVector> vector(final Random random) {
        return AffineOperator.identity().transform(rawVector(random));
    }

    private static Transformed<RealVector> vector(final Random random,
                                                  final Transformed<RealVector> original) {
        return AffineOperator.identity().transform(rawVector(random, original.getUnderlyingVector()));
    }

    private static RealVector rawVector(final Random random) {
        return RealVectorTest.createRandomDoubleVector(random, 768);
    }

    private static RealVector rawVector(final Random random, final RealVector original) {
        RealVector randomVector;
        do {
            randomVector = RealVectorTest.createRandomDoubleVector(random, 768);
        } while (randomVector.equals(original));
        return randomVector;
    }

    private static int differentInteger(final Random random, final int original, final int bound) {
        int randomInteger;
        do {
            randomInteger = random.nextInt(bound);
        } while (randomInteger == original);
        return randomInteger;
    }

    private static long differentLong(final Random random, final long original) {
        long randomLong;
        do {
            randomLong = random.nextLong();
        } while (randomLong == original);
        return randomLong;
    }

    private static double differentDouble(final Random random, final double original) {
        double randomDouble;
        do {
            randomDouble = random.nextDouble();
        } while (randomDouble == original);
        return randomDouble;
    }
}
