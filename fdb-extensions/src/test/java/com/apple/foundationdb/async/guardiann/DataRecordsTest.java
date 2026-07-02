/*
 * DataRecordsTest.java
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

package com.apple.foundationdb.async.guardiann;

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
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Consolidated unit tests for the guardiann pure value/record types — {@link VectorId}, {@link PrimaryCopy},
 * {@link ReplicatedCopy}, {@link AccessInfo}, {@link ClusterMetadata}, {@link Cluster}, and {@link ClusterReference} —
 * plus the identity-preserving conversion/lens methods layered on top of them.
 * <p>
 * Modeled on {@code com.apple.foundationdb.async.hnsw.DataRecordsTest}: rather than a class per record (which would
 * fragment coverage for a pile of trivial {@code toString}/{@code equals} overrides), a single generic harness pins
 * the {@code hashCode}/{@code equals}/{@code toString} contract of each type from a seeded builder, and a handful of
 * focused tests cover the load-bearing behavior — the {@code ==} short-circuits in {@code withVector}/
 * {@code toReplicatedCopy}, the role/priority transitions between primary and replicated copies, and the vector lens.
 */
class DataRecordsTest {
    /** Small dimensionality: these tests exercise record identity, not vector math, so wide vectors add only cost. */
    private static final int DIMENSIONS = 16;

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testVectorId(final long randomSeed) {
        assertHashCodeEqualsToString(randomSeed, DataRecordsTest::vectorId, DataRecordsTest::vectorId);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testPrimaryCopy(final long randomSeed) {
        assertHashCodeEqualsToString(randomSeed, DataRecordsTest::primaryCopy, DataRecordsTest::primaryCopy);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testReplicatedCopy(final long randomSeed) {
        assertHashCodeEqualsToString(randomSeed, DataRecordsTest::replicatedCopy, DataRecordsTest::replicatedCopy);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testAccessInfo(final long randomSeed) {
        assertHashCodeEqualsToString(randomSeed, DataRecordsTest::accessInfo, DataRecordsTest::accessInfo);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testClusterMetadata(final long randomSeed) {
        assertHashCodeEqualsToString(randomSeed, DataRecordsTest::clusterMetadata, DataRecordsTest::clusterMetadata);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testCluster(final long randomSeed) {
        assertHashCodeEqualsToString(randomSeed, DataRecordsTest::cluster, DataRecordsTest::cluster);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void testClusterReference(final long randomSeed) {
        assertHashCodeEqualsToString(randomSeed, DataRecordsTest::clusterReference, DataRecordsTest::clusterReference);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void primaryCopyWithVectorShortCircuitsOnIdentity(final long randomSeed) {
        final Random random = new Random(randomSeed);
        final Transformed<RealVector> vector = transformed(random);
        final PrimaryCopy primary = (PrimaryCopy) VectorReference.primaryCopy(vectorId(random), vector, true, false);

        // Same reference in → same instance out (== short-circuit, not equals).
        Assertions.assertThat(primary.withVector(vector)).isSameAs(primary);

        final Transformed<RealVector> other = transformed(random);
        final VectorReference rewritten = primary.withVector(other);
        Assertions.assertThat(rewritten).isNotSameAs(primary);
        Assertions.assertThat(rewritten.vector()).isSameAs(other);
        Assertions.assertThat(rewritten.id()).isEqualTo(primary.id());
        Assertions.assertThat(rewritten.isUnderreplicated()).isEqualTo(primary.isUnderreplicated());
        Assertions.assertThat(rewritten.isCollapsed()).isEqualTo(primary.isCollapsed());
        Assertions.assertThat(rewritten.isPrimaryCopy()).isTrue();
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void replicatedCopyConversionsPreserveIdentityAndRewriteRole(final long randomSeed) {
        final Random random = new Random(randomSeed);
        final Transformed<RealVector> vector = transformed(random);
        final ReplicatedCopy replicated =
                (ReplicatedCopy) VectorReference.replicatedCopy(vectorId(random), vector, 0.5d, true);

        // toReplicatedCopy: unchanged score short-circuits; a new score allocates; NaN never equals itself so it
        // must allocate too (guarding against a silent identity return on re-scoring to NaN).
        Assertions.assertThat(replicated.toReplicatedCopy(0.5d)).isSameAs(replicated);
        final VectorReference rescored = replicated.toReplicatedCopy(0.7d);
        Assertions.assertThat(rescored).isNotSameAs(replicated);
        Assertions.assertThat(rescored.replicationPriority()).isEqualTo(0.7d);
        final VectorReference nanScored = replicated.toReplicatedCopy(Double.NaN);
        Assertions.assertThat(nanScored).isNotSameAs(replicated);
        Assertions.assertThat(nanScored.replicationPriority()).isNaN();

        Assertions.assertThat(replicated.withVector(vector)).isSameAs(replicated);

        // Promotion to a primary drops the replication priority to the primary sentinel (-1.0) and only differs
        // between the two forms by the under-replicated bit.
        final VectorReference primary = replicated.toPrimaryCopy();
        Assertions.assertThat(primary.isPrimaryCopy()).isTrue();
        Assertions.assertThat(primary.isUnderreplicated()).isFalse();
        Assertions.assertThat(primary.replicationPriority()).isEqualTo(-1.0d);
        Assertions.assertThat(primary.id()).isEqualTo(replicated.id());
        Assertions.assertThat(primary.isCollapsed()).isEqualTo(replicated.isCollapsed());

        final VectorReference underreplicatedPrimary = replicated.toPrimaryUnderreplicatedCopy();
        Assertions.assertThat(underreplicatedPrimary.isPrimaryCopy()).isTrue();
        Assertions.assertThat(underreplicatedPrimary.isUnderreplicated()).isTrue();
        Assertions.assertThat(underreplicatedPrimary.replicationPriority()).isEqualTo(-1.0d);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void vectorLensGetsAndSetsTheUnderlyingVector(final long randomSeed) {
        final Random random = new Random(randomSeed);
        final VectorReference reference =
                VectorReference.primaryCopy(vectorId(random), transformed(random), false, false);
        final var lens = VectorReference.vectorLens();

        final RealVector newVector = rawVector(random);
        final VectorReference updated = lens.set(reference, newVector);

        Assertions.assertThat(lens.get(updated)).isEqualTo(newVector);
        Assertions.assertThat(updated).isNotSameAs(reference);
        Assertions.assertThat(updated.id()).isEqualTo(reference.id());
        Assertions.assertThat(updated.isPrimaryCopy()).isTrue();
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void clusterReferenceFactoryProjectsAndItsLensSetIsUnsupported(final long randomSeed) {
        final Random random = new Random(randomSeed);
        final ClusterMetadataWithDistance first =
                new ClusterMetadataWithDistance(clusterMetadata(random), transformed(random), random.nextDouble());
        final ClusterMetadataWithDistance second =
                new ClusterMetadataWithDistance(clusterMetadata(random), transformed(random), random.nextDouble());

        final List<ClusterReference> references =
                ClusterReference.fromClusterMetadataAndDistances(ImmutableList.of(first, second));
        Assertions.assertThat(references).hasSize(2);
        Assertions.assertThat(references.get(0).clusterId()).isEqualTo(first.clusterMetadata().id());
        Assertions.assertThat(references.get(0).centroid()).isEqualTo(first.centroid());
        Assertions.assertThat(references.get(1).clusterId()).isEqualTo(second.clusterMetadata().id());

        Assertions.assertThat(ClusterReference.fromClusterMetadataAndDistances(
                ImmutableList.<ClusterMetadataWithDistance>of())).isEmpty();

        // The projection lens is read-only: it can extract a reference but not write one back.
        Assertions.assertThatThrownBy(() ->
                        ClusterReference.FROM_CLUSTER_METADATA_AND_DISTANCE.set(first, references.get(0)))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @ParameterizedTest
    @RandomSeedSource({0x0fdbL, 0x5ca1eL, 123456L, 78910L, 1123581321345589L})
    void accessInfoRendersNullCentroid(final long randomSeed) {
        // The generic harness only ever builds a non-null centroid, so pin the null-centroid rendering here.
        final long rotatorSeed = new Random(randomSeed).nextLong();
        Assertions.assertThat(new AccessInfo(rotatorSeed, null).toString())
                .isEqualTo("AccessInfo[rotatorSeed=" + rotatorSeed + ", negatedCentroid=null]");
    }

    // ---------------------------------------------------------------------------------------------------------
    // Generic hashCode/equals/toString harness (mirrors hnsw DataRecordsTest)
    // ---------------------------------------------------------------------------------------------------------

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

    // ---------------------------------------------------------------------------------------------------------
    // Per-type builders: each has a create(Random) and a create(Random, original) that guarantees a different value
    // ---------------------------------------------------------------------------------------------------------

    @Nonnull
    private static VectorId vectorId(@Nonnull final Random random) {
        return new VectorId(primaryKey(random), new UUID(random.nextLong(), random.nextLong()));
    }

    @Nonnull
    private static VectorId vectorId(@Nonnull final Random random, @Nonnull final VectorId original) {
        return new VectorId(primaryKey(random, original.primaryKey()),
                new UUID(differentLong(random, original.uuid().getMostSignificantBits()), random.nextLong()));
    }

    @Nonnull
    private static VectorReference primaryCopy(@Nonnull final Random random) {
        return VectorReference.primaryCopy(vectorId(random), transformed(random),
                random.nextBoolean(), random.nextBoolean());
    }

    @Nonnull
    private static VectorReference primaryCopy(@Nonnull final Random random, @Nonnull final VectorReference original) {
        return VectorReference.primaryCopy(vectorId(random, original.id()), transformed(random, original.vector()),
                !original.isUnderreplicated(), !original.isCollapsed());
    }

    @Nonnull
    private static VectorReference replicatedCopy(@Nonnull final Random random) {
        return VectorReference.replicatedCopy(vectorId(random), transformed(random),
                random.nextDouble(), random.nextBoolean());
    }

    @Nonnull
    private static VectorReference replicatedCopy(@Nonnull final Random random, @Nonnull final VectorReference original) {
        return VectorReference.replicatedCopy(vectorId(random, original.id()), transformed(random, original.vector()),
                differentDouble(random, original.replicationPriority()), !original.isCollapsed());
    }

    @Nonnull
    private static AccessInfo accessInfo(@Nonnull final Random random) {
        return new AccessInfo(random.nextLong(), rawVector(random));
    }

    @Nonnull
    private static AccessInfo accessInfo(@Nonnull final Random random, @Nonnull final AccessInfo original) {
        return new AccessInfo(differentLong(random, original.rotatorSeed()),
                differentRawVector(random, original.negatedCentroid()));
    }

    @Nonnull
    private static ClusterMetadata clusterMetadata(@Nonnull final Random random) {
        return clusterMetadata(random, new UUID(random.nextLong(), random.nextLong()));
    }

    @Nonnull
    private static ClusterMetadata clusterMetadata(@Nonnull final Random random, @Nonnull final ClusterMetadata original) {
        return clusterMetadata(random,
                new UUID(differentLong(random, original.id().getMostSignificantBits()), random.nextLong()));
    }

    @Nonnull
    private static ClusterMetadata clusterMetadata(@Nonnull final Random random, @Nonnull final UUID id) {
        final int numPrimaryUnderreplicatedVectors = random.nextInt(4);
        final int numReplicatedVectors = random.nextInt(6);
        // The constructor requires numElements >= numPrimaryUnderreplicatedVectors, so seed at least that many.
        RunningStats stats = RunningStats.identity();
        final int numElements = numPrimaryUnderreplicatedVectors + random.nextInt(4);
        for (int i = 0; i < numElements; i++) {
            stats = stats.add(random.nextDouble() * 10.0d);
        }
        return new ClusterMetadata(id, numPrimaryUnderreplicatedVectors, numReplicatedVectors, stats,
                random.nextInt(8));
    }

    @Nonnull
    private static Cluster cluster(@Nonnull final Random random) {
        return new Cluster(clusterMetadata(random), transformed(random), vectorReferences(random));
    }

    @Nonnull
    private static Cluster cluster(@Nonnull final Random random, @Nonnull final Cluster original) {
        return new Cluster(clusterMetadata(random, original.clusterMetadata()),
                transformed(random, original.centroid()), vectorReferences(random));
    }

    @Nonnull
    private static ClusterReference clusterReference(@Nonnull final Random random) {
        return new ClusterReference(new UUID(random.nextLong(), random.nextLong()), transformed(random));
    }

    @Nonnull
    private static ClusterReference clusterReference(@Nonnull final Random random,
                                                    @Nonnull final ClusterReference original) {
        return new ClusterReference(
                new UUID(differentLong(random, original.clusterId().getMostSignificantBits()), random.nextLong()),
                transformed(random, original.centroid()));
    }

    @Nonnull
    private static List<VectorReference> vectorReferences(@Nonnull final Random random) {
        final ImmutableList.Builder<VectorReference> builder = ImmutableList.builder();
        final int size = random.nextInt(4);
        for (int i = 0; i < size; i++) {
            builder.add(random.nextBoolean() ? primaryCopy(random) : replicatedCopy(random));
        }
        return builder.build();
    }

    @Nonnull
    private static Tuple primaryKey(@Nonnull final Random random) {
        return Tuple.from(random.nextInt(1000));
    }

    @Nonnull
    private static Tuple primaryKey(@Nonnull final Random random, @Nonnull final Tuple original) {
        final int originalKey = Math.toIntExact(original.getLong(0));
        int key;
        do {
            key = random.nextInt(1000);
        } while (key == originalKey);
        return Tuple.from(key);
    }

    @Nonnull
    private static Transformed<RealVector> transformed(@Nonnull final Random random) {
        return AffineOperator.identity().transform(rawVector(random));
    }

    @Nonnull
    private static Transformed<RealVector> transformed(@Nonnull final Random random,
                                                       @Nonnull final Transformed<RealVector> original) {
        return AffineOperator.identity().transform(differentRawVector(random, original.getUnderlyingVector()));
    }

    @Nonnull
    private static RealVector rawVector(@Nonnull final Random random) {
        return RealVectorTest.createRandomDoubleVector(random, DIMENSIONS);
    }

    @Nonnull
    private static RealVector differentRawVector(@Nonnull final Random random, @Nonnull final RealVector original) {
        RealVector randomVector;
        do {
            randomVector = RealVectorTest.createRandomDoubleVector(random, DIMENSIONS);
        } while (randomVector.equals(original));
        return randomVector;
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
