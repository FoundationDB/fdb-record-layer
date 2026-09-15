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
 * @param maxEverNumPrimaryVectors the high-water mark of {@link #getNumPrimaryVectors()} over this cluster's
 *        lifetime, so the merge trigger can fire when the current count falls to a fraction of this peak rather than
 *        below a fixed absolute floor. Must never be below the current primary count; whoever changes the running
 *        statistics is responsible for raising it, and the compact constructor rejects any instance that would
 *        violate this
 */
record ClusterMetadata(@Nonnull UUID id, int numPrimaryUnderreplicatedVectors, int numReplicatedVectors,
                       @Nonnull RunningStats runningStandardDeviation, @Nonnull EnumSet<State> states,
                       int maxEverNumPrimaryVectors) {
    public ClusterMetadata(@Nonnull final UUID id, final int numPrimaryUnderreplicatedVectors,
                           final int numReplicatedVectors,
                           @Nonnull final RunningStats runningStandardDeviation, final int stateCode,
                           final int maxEverNumPrimaryVectors) {
        this(id, numPrimaryUnderreplicatedVectors, numReplicatedVectors, runningStandardDeviation,
                State.ofCode(stateCode), maxEverNumPrimaryVectors);
    }

    ClusterMetadata {
        Preconditions.checkArgument(runningStandardDeviation.numElements() >= numPrimaryUnderreplicatedVectors);
        // The high-water mark is checked, not corrected: a caller that changes the running statistics raises it
        // itself, so that the raise is visible where it happens rather than applied invisibly here. Silently
        // patching it would also hide a caller that dropped the prior peak, which is the one mistake that matters —
        // a peak that fails to grow degrades the merge trigger to the bare primaryClusterMin floor.
        Preconditions.checkArgument(maxEverNumPrimaryVectors >= runningStandardDeviation.numElements(),
                "maxEverNumPrimaryVectors (%s) must be >= the current primary count (%s)",
                maxEverNumPrimaryVectors, runningStandardDeviation.numElements());
    }

    /**
     * Returns the high-water mark this cluster would carry after adopting {@code newStandardDeviation}: the larger of
     * the peak it holds now and the primary count those statistics imply.
     * <p>
     * Every caller that replaces the running statistics must route the mark through here, since the compact
     * constructor rejects a mark below the current count rather than quietly raising it.
     *
     * @param newStandardDeviation the statistics about to be adopted
     * @return the peak to pass to the constructor
     */
    private int raisedMaxEver(@Nonnull final RunningStats newStandardDeviation) {
        return Math.max(maxEverNumPrimaryVectors(), Math.toIntExact(newStandardDeviation.numElements()));
    }

    public int getNumPrimaryVectors() {
        return Math.toIntExact(runningStandardDeviation.numElements());
    }

    /**
     * Computes the primary-vector count below which <em>this</em> cluster becomes merge-eligible: the larger of the
     * absolute {@link Config#primaryClusterMin()} floor and {@link Config#mergeMaxEverFraction()} of the cluster's own
     * {@linkplain #maxEverNumPrimaryVectors() lifetime peak}.
     * <p>
     * The fraction term is a <em>shrinkage</em> detector, and only that. It fires once a cluster has shed all but that
     * fraction of the largest it has ever been, which consolidates a cluster that has drained away instead of leaving
     * it to linger at a size the floor alone would tolerate. It has no say over a cluster sitting at its peak: such a
     * cluster has {@code current == maxEver}, and since {@code fraction * maxEver < maxEver} for any fraction below
     * one, the comparison reduces to {@code current < primaryClusterMin} — the floor decides alone. In particular the
     * fraction does <em>not</em> shield a freshly split child; what keeps a child from being born already
     * merge-eligible is {@link Config#minChildFraction()}, which bounds how small a split may make one.
     * <p>
     * The two terms have distinct roles: the floor sets the smallest cluster worth keeping, and the fraction decides
     * when a once-large cluster has shrunk enough to be merged. A non-zero fraction affects the outcome only for a
     * cluster whose peak is large enough that the fraction of that peak exceeds the floor; for any smaller peak the
     * floor is the larger of the two terms and determines the threshold on its own. A fraction of {@code 0} removes
     * the fraction term for every cluster, so the floor is the entire trigger.
     * <p>
     * This lives here rather than on {@link Config} because the threshold is a property of a cluster, not of the
     * configuration: the peak it is derived from belongs to this record.
     *
     * @param config the configuration supplying the floor and the fraction
     * @return the merge threshold; this cluster wants to merge once it holds fewer primaries than this
     */
    public int mergeThreshold(@Nonnull final Config config) {
        return Math.max(config.primaryClusterMin(),
                (int) Math.floor(config.mergeMaxEverFraction() * maxEverNumPrimaryVectors()));
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
                newStandardDeviation, newStates, raisedMaxEver(newStandardDeviation));
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
                runningStandardDeviation(), newStates, maxEverNumPrimaryVectors());
    }

    /**
     * Returns a copy with the two auxiliary counts adjusted by the given deltas, the running statistics replaced, and
     * the given states raised on top of the existing ones.
     * <p>
     * Note the asymmetry: the underreplicated and replicated counts arrive as <em>deltas</em>, but the primary count
     * does not, because it has no independent representation —
     * {@link #getNumPrimaryVectors()} is the element count of the running statistics. So a primary added or removed is
     * expressed solely by {@code newStandardDeviation} carrying one more or one fewer distance sample, and neither
     * delta parameter has any bearing on it.
     * <p>
     * That is also why this method passes the existing {@link #maxEverNumPrimaryVectors()} through untouched rather
     * than trying to raise it: the compact constructor clamps the mark up to the new primary count on every
     * construction, so the caller's only obligation is not to <em>lose</em> the prior peak. Since
     * {@code Math.max} can only move it up, preservation across a shrink falls out for free.
     *
     * @param numPrimaryUnderreplicatedVectorsAdded change in the number of underreplicated primary vectors
     * @param numReplicatedVectorsAdded change in the number of replicated vectors
     * @param newStandardDeviation the running statistics to adopt, whose element count <em>is</em> the new primary
     *        count
     * @param additionalStates states to raise in addition to those already set
     * @return the updated metadata
     */
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
                newStates, raisedMaxEver(newStandardDeviation));
    }

    @Override
    @Nonnull
    public String toString() {
        return "CM[id=" + id() +
                ", numPrimaryVectors=" + getNumPrimaryVectors() +
                ", maxEverNumPrimaryVectors=" + maxEverNumPrimaryVectors() +
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
         * splitting) or below its {@link ClusterMetadata#mergeThreshold(Config) merge threshold} (needs merging) — and
         * a pending {@link SplitMergeTask} will repartition it into new clusters or dissolve it into its neighbors.
         * Suppressed while {@link #COLLAPSE} is set, since collapsing duplicates changes the cluster's effective size
         * and may make the split/merge moot.
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
