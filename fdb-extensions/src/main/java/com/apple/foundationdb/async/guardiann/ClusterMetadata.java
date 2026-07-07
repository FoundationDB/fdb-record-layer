/*
 * ClusterMetadata.java
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

import com.google.common.base.Preconditions;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Persistent metadata describing a single Guardiann cluster (a node in the centroid HNSW). It records the
 * cluster's id, the number of primary-underreplicated and replicated vectors it holds, the running statistics of
 * member distances to the centroid (from which the total primary count and standard deviation are derived), and
 * the set of in-flight maintenance {@link State}s the cluster is currently subject to.
 *
 * @param id the unique id of the cluster
 * @param numPrimaryUnderreplicatedVectors the number of primary vectors in the cluster that are underreplicated
 *        (see preamble of {@link VectorReference} for an explanation of the different vector kinds)
 * @param numReplicatedVectors the number of replicated (non-primary) vectors in the cluster
 * @param runningStandardDeviation running statistics of member distances to the centroid; its element count is the
 *        number of primary vectors
 * @param states the set of maintenance operations currently in flight for this cluster
 */
record ClusterMetadata(@Nonnull UUID id, int numPrimaryUnderreplicatedVectors, int numReplicatedVectors,
                       @Nonnull RunningStats runningStandardDeviation, @Nonnull EnumSet<State> states) {
    public ClusterMetadata(@Nonnull final UUID id, final int numPrimaryUnderreplicatedVectors,
                           final int numReplicatedVectors,
                           @Nonnull final RunningStats runningStandardDeviation, final int stateCode) {
        this(id, numPrimaryUnderreplicatedVectors, numReplicatedVectors, runningStandardDeviation,
                State.ofCode(stateCode));
    }

    ClusterMetadata {
        Preconditions.checkArgument(runningStandardDeviation.numElements() >= numPrimaryUnderreplicatedVectors);
    }

    public int getNumPrimaryVectors() {
        return Math.toIntExact(runningStandardDeviation.numElements());
    }

    public double meanDistance() {
        return runningStandardDeviation.runningMean();
    }

    public double standardDeviation() {
        return runningStandardDeviation.populationStandardDeviation();
    }

    public int getStatesCode() {
        int result = 0;
        for (final State state : states()) {
            result |= state.getCode();
        }
        return result;
    }

    @Nonnull
    public ClusterMetadata withNewVectors(final int numPrimaryUnderreplicatedVectors,
                                          final int numReplicatedVectors,
                                          @Nonnull final RunningStats newStandardDeviation,
                                          @Nonnull final EnumSet<State> states) {
        final EnumSet<State> newStates = EnumSet.copyOf(states);
        return new ClusterMetadata(id(), numPrimaryUnderreplicatedVectors, numReplicatedVectors,
                newStandardDeviation, newStates);
    }

    @Nonnull
    public ClusterMetadata withAdditionalVectors(final int numPrimaryUnderreplicatedVectorsAdded,
                                                 final int numReplicatedVectorsAdded,
                                                 @Nonnull final RunningStats newStandardDeviation) {
        return withAdditionalVectorsAndStates(numPrimaryUnderreplicatedVectorsAdded,
                numReplicatedVectorsAdded, newStandardDeviation, EnumSet.noneOf(State.class));
    }

    @Nonnull
    public ClusterMetadata withNewStates(@Nonnull final EnumSet<State> newStates) {
        return new ClusterMetadata(id(), numPrimaryUnderreplicatedVectors(), numReplicatedVectors(),
                runningStandardDeviation(), newStates);
    }

    @Nonnull
    public ClusterMetadata withAdditionalVectorsAndStates(final int numPrimaryUnderreplicatedVectorsAdded,
                                                          final int numReplicatedVectorsAdded,
                                                          @Nonnull final RunningStats newStandardDeviation,
                                                          @Nonnull final EnumSet<State> additionalStates) {
        final EnumSet<State> newStates = EnumSet.copyOf(states());
        newStates.addAll(additionalStates);
        return new ClusterMetadata(id(),
                numPrimaryUnderreplicatedVectors() + numPrimaryUnderreplicatedVectorsAdded,
                numReplicatedVectors() + numReplicatedVectorsAdded, newStandardDeviation,
                newStates);
    }

    @Override
    @Nonnull
    public String toString() {
        return "CM[id=" + id() +
                ", numPrimaryVectors=" + getNumPrimaryVectors() +
                ", numPrimaryUnderreplicatedVectors=" + numPrimaryUnderreplicatedVectors() +
                ", numReplicatedVectors=" + numReplicatedVectors() +
                ", states=" + states() +
                ']';
    }

    /**
     * The kinds of in-flight maintenance a cluster may be subject to. Each constant has a distinct power-of-two
     * {@linkplain #getCode() code} so that a set of states can be packed into a single integer bit mask and
     * restored via {@link #ofCode(int)}.
     */
    public enum State {
        /**
         * The cluster's primary count has crossed a size bound — above {@link Config#primaryClusterMax()} (needs
         * splitting) or below {@link Config#primaryClusterMin()} (needs merging) — and a pending {@link SplitMergeTask}
         * will repartition it into new clusters or dissolve it into its neighbors. Suppressed while {@link #COLLAPSE}
         * is set, since collapsing duplicates changes the cluster's effective size and may make the split/merge moot.
         */
        SPLIT_MERGE(1),
        /**
         * The cluster's replication/assignment layer needs repair, and a pending {@link ReassignTask} will re-home
         * primaries that have drifted to a nearer cluster, top up {@linkplain VectorReference#isUnderreplicated()
         * underreplicated} primaries, and prune excess replicas. Raised in the wake of a neighboring split/merge or
         * when the cluster exceeds its replicated-write or underreplicated-primary bounds. Yields to {@link #SPLIT_MERGE}
         * and {@link #COLLAPSE}: the reassign no-ops if either is also set, since both reshape membership first.
         */
        REASSIGN(2),
        /**
         * The cluster holds enough identical primary vectors (sharing one content signature) to be worth
         * deduplicating, and a pending {@link CollapseTask} will fold each duplicate group into a single collapsed
         * representative. Takes precedence over {@link #SPLIT_MERGE} and {@link #REASSIGN} — both no-op while it is set —
         * because collapsing changes the cluster's effective size and membership out from under them.
         */
        COLLAPSE(4);

        private static final Map<Integer, State> BY_CODE =
                Arrays.stream(values())
                        .collect(Collectors.toMap(s -> s.code, s -> s));

        private final int code;

        State(final int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        public static EnumSet<State> ofCode(final int code) {
            final EnumSet<State> resultSet = EnumSet.noneOf(State.class);
            for (int i = 0; i < 32; i++) {
                final int bitValue = 1 << i;
                if ((code & bitValue) != 0) {
                    final State lookup = BY_CODE.getOrDefault(bitValue, null);
                    Objects.requireNonNull(lookup, "unable to look up state");
                    resultSet.add(lookup);
                }
            }
            return resultSet;
        }
    }
}
