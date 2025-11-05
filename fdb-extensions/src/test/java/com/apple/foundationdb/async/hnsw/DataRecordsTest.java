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
import java.util.List;
import java.util.Random;
import java.util.function.Function;

class DataRecordsTest {
    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testAccessInfo(final long randomSeed) {
        assertHashCodeEqualsToString(randomSeed, DataRecordsTest::accessInfo);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testAggregatedVector(final long randomSeed) {
        assertHashCodeEqualsToString(randomSeed, DataRecordsTest::aggregatedVector);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testCompactNode(final long randomSeed) {
        final Random random = new Random(randomSeed);
        final long dependentRandomSeed = random.nextLong();

        final CompactNode compactNode1 = compactNode(new Random(dependentRandomSeed));
        final CompactNode compactNode1Clone = compactNode(new Random(dependentRandomSeed));
        Assertions.assertThat(compactNode1).hasToString(compactNode1Clone.toString());

        final CompactNode compactNode2 = compactNode(random);
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

        final InliningNode inliningNode2 = inliningNode(random);
        Assertions.assertThat(inliningNode1).doesNotHaveToString(inliningNode2.toString());

        Assertions.assertThatThrownBy(inliningNode1::asCompactNode).isInstanceOf(IllegalStateException.class);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testEntryNodeReference(final long randomSeed) {
        assertHashCodeEqualsToString(randomSeed, DataRecordsTest::entryNodeReference);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testNodeReference(final long randomSeed) {
        assertHashCodeEqualsToString(randomSeed, DataRecordsTest::nodeReference);
        final NodeReference nodeReference = nodeReference(new Random(randomSeed));
        Assertions.assertThat(nodeReference.isNodeReferenceWithVector()).isFalse();
        Assertions.assertThatThrownBy(nodeReference::asNodeReferenceWithVector).isInstanceOf(IllegalStateException.class);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testNodeReferenceWithVector(final long randomSeed) {
        assertHashCodeEqualsToString(randomSeed, DataRecordsTest::nodeReferenceWithVector);
        final NodeReferenceWithVector nodeReference = nodeReferenceWithVector(new Random(randomSeed));
        Assertions.assertThat(nodeReference.isNodeReferenceWithVector()).isTrue();
        Assertions.assertThat(nodeReference.asNodeReferenceWithVector()).isInstanceOf(NodeReferenceWithVector.class);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testNodeReferenceWithDistance(final long randomSeed) {
        assertHashCodeEqualsToString(randomSeed, DataRecordsTest::nodeReferenceWithDistance);
        final NodeReferenceWithDistance nodeReference = nodeReferenceWithDistance(new Random(randomSeed));
        Assertions.assertThat(nodeReference.isNodeReferenceWithVector()).isTrue();
        Assertions.assertThat(nodeReference.asNodeReferenceWithVector()).isInstanceOf(NodeReferenceWithDistance.class);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testResultEntry(final long randomSeed) {
        assertHashCodeEqualsToString(randomSeed, DataRecordsTest::resultEntry);
    }

    private static <T> void assertHashCodeEqualsToString(final long randomSeed, final Function<Random, T> createFunction) {
        final Random random = new Random(randomSeed);
        final long dependentRandomSeed = random.nextLong();
        final T t1 = createFunction.apply(new Random(dependentRandomSeed));
        final T t1Clone = createFunction.apply(new Random(dependentRandomSeed));
        Assertions.assertThat(t1.hashCode()).isEqualTo(t1Clone.hashCode());
        Assertions.assertThat(t1).isEqualTo(t1Clone);
        Assertions.assertThat(t1).hasToString(t1Clone.toString());

        final T t2 = createFunction.apply(random);
        Assertions.assertThat(t1).isNotEqualTo(t2);
        Assertions.assertThat(t1).doesNotHaveToString(t2.toString());
    }

    @Nonnull
    private static ResultEntry resultEntry(@Nonnull final Random random) {
        return new ResultEntry(primaryKey(random), rawVector(random), random.nextDouble(), random.nextInt(100));
    }

    @Nonnull
    private static CompactNode compactNode(@Nonnull final Random random) {
        return CompactNode.factory()
                .create(primaryKey(random), vector(random), nodeReferences(random))
                .asCompactNode();
    }

    @Nonnull
    private static InliningNode inliningNode(@Nonnull final Random random) {
        return InliningNode.factory()
                .create(primaryKey(random), vector(random), nodeReferenceWithVectors(random))
                .asInliningNode();
    }

    @Nonnull
    private static NodeReferenceWithDistance nodeReferenceWithDistance(@Nonnull final Random random) {
        return new NodeReferenceWithDistance(primaryKey(random), vector(random), random.nextDouble());
    }

    @Nonnull
    private static List<NodeReferenceWithVector> nodeReferenceWithVectors(@Nonnull final Random random) {
        int size = random.nextInt(20);
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
    private static List<NodeReference> nodeReferences(@Nonnull final Random random) {
        int size = random.nextInt(20);
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
    private static AggregatedVector aggregatedVector(@Nonnull final Random random) {
        return new AggregatedVector(random.nextInt(100), vector(random));
    }

    @Nonnull
    private static AccessInfo accessInfo(@Nonnull final Random random) {
        return new AccessInfo(entryNodeReference(random), random.nextLong(), rawVector(random));
    }

    @Nonnull
    private static EntryNodeReference entryNodeReference(@Nonnull final Random random) {
        return new EntryNodeReference(primaryKey(random), vector(random), random.nextInt(10));
    }

    @Nonnull
    private static Tuple primaryKey(@Nonnull final Random random) {
        return Tuple.from(random.nextInt(100));
    }

    @Nonnull
    private static Transformed<RealVector> vector(@Nonnull final Random random) {
        return AffineOperator.identity().transform(rawVector(random));
    }

    @Nonnull
    private static RealVector rawVector(@Nonnull final Random random) {
        return RealVectorTest.createRandomDoubleVector(random, 768);
    }
}
