/*
 * ClusterMetadataDelta.java
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.EnumSet;
import java.util.UUID;

/**
 * An incremental change to a single cluster's {@link ClusterMetadata}, expressed so that it can be persisted with an
 * FDB {@code APPEND_IF_FITS} mutation rather than a read-modify-write of the whole metadata value.
 * <p>
 * A whole-value rewrite requires reading the current value first, and that read is what makes two otherwise
 * unrelated writers to the same cluster conflict. Appending a delta needs no read at all, so concurrent inserts and
 * deletes into one cluster can commit without conflicting; the accumulated deltas are folded back into a single
 * value on read (see {@code StorageAdapter.clusterMetadataFromTuple}) and are periodically compacted away.
 * <p>
 * Deltas are replayed through {@link RunningStats#add}/{@link RunningStats#remove}, so the running distance
 * statistics keep Welford's exact numerical behaviour — nothing about the statistics is approximated to make them
 * appendable.
 *
 * @param statsOp how this delta changes the running distance statistics
 * @param distance the vector-to-centroid distance to add or remove; ignored when {@code statsOp} is
 *        {@link StatsOp#NONE}
 * @param numPrimaryUnderreplicatedVectorsDelta the change in the number of underreplicated primary vectors
 * @param numReplicatedVectorsDelta the change in the number of replicated vectors
 * @param statesToSet maintenance states to raise, applied before {@code statesToClear}
 * @param statesToClear maintenance states to drop, applied after {@code statesToSet}
 */
record ClusterMetadataDelta(@Nonnull StatsOp statsOp,
                            double distance,
                            int numPrimaryUnderreplicatedVectorsDelta,
                            int numReplicatedVectorsDelta,
                            @Nonnull EnumSet<ClusterMetadata.State> statesToSet,
                            @Nonnull EnumSet<ClusterMetadata.State> statesToClear) {
    private static final Logger logger = LoggerFactory.getLogger(ClusterMetadataDelta.class);

    ClusterMetadataDelta {
        // Defensive copies: EnumSet is mutable and a delta is shared with the fold that replays it.
        statesToSet = EnumSet.copyOf(statesToSet);
        statesToClear = EnumSet.copyOf(statesToClear);
    }

    /**
     * Applies this delta to {@code clusterMetadata}, returning the resulting metadata.
     * <p>
     * This is deliberately <em>total</em>: it never throws. The fold that replays deltas runs on every read of a
     * cluster's metadata, so a delta sequence that is internally inconsistent — over-removing from the statistics,
     * or driving a count negative — must not make the cluster permanently unreadable. Such a sequence indicates a
     * lost or duplicated append, so each occurrence is clamped and logged rather than propagated.
     * <p>
     * The high-water mark {@link ClusterMetadata#maxEverNumPrimaryVectors()} needs no explicit handling: the
     * {@link ClusterMetadata} constructor raises it to the current primary count and never lowers it, so replaying
     * the deltas in order automatically records the peak the cluster passed through between compactions.
     *
     * @param clusterMetadata the metadata to apply this delta to
     * @return the metadata with this delta applied
     */
    @Nonnull
    ClusterMetadata applyTo(@Nonnull final ClusterMetadata clusterMetadata) {
        final RunningStats newStandardDeviation =
                applyToStandardDeviation(clusterMetadata.runningStandardDeviation(), clusterMetadata.id());

        final EnumSet<ClusterMetadata.State> newStates = EnumSet.copyOf(clusterMetadata.states());
        newStates.addAll(statesToSet());
        newStates.removeAll(statesToClear());

        final int numPrimaryVectors = Math.toIntExact(newStandardDeviation.numElements());
        final int newNumPrimaryUnderreplicatedVectors =
                clamp(clusterMetadata.numPrimaryUnderreplicatedVectors() + numPrimaryUnderreplicatedVectorsDelta(),
                        numPrimaryVectors, "numPrimaryUnderreplicatedVectors", clusterMetadata.id());
        final int newNumReplicatedVectors =
                clamp(clusterMetadata.numReplicatedVectors() + numReplicatedVectorsDelta(),
                        Integer.MAX_VALUE, "numReplicatedVectors", clusterMetadata.id());

        return new ClusterMetadata(clusterMetadata.id(), newNumPrimaryUnderreplicatedVectors,
                newNumReplicatedVectors, newStandardDeviation, newStates,
                clusterMetadata.maxEverNumPrimaryVectors());
    }

    @Nonnull
    private RunningStats applyToStandardDeviation(@Nonnull final RunningStats runningStandardDeviation,
                                                  @Nonnull final UUID clusterId) {
        switch (statsOp()) {
            case ADD:
                return runningStandardDeviation.add(distance());
            case REMOVE:
                if (runningStandardDeviation.numElements() == 0) {
                    // An over-removing delta sequence: treat as a no-op rather than letting RunningStats.remove
                    // throw, which would make every subsequent read of this cluster fail.
                    logger.warn("dropping a REMOVE delta against empty running statistics; clusterId={}", clusterId);
                    return runningStandardDeviation;
                }
                return runningStandardDeviation.remove(distance());
            case NONE:
            default:
                return runningStandardDeviation;
        }
    }

    private static int clamp(final int value, final int upperBound, @Nonnull final String what,
                             @Nonnull final UUID clusterId) {
        if (value < 0) {
            logger.warn("clamping negative {} to 0 while folding cluster metadata deltas; clusterId={}, value={}",
                    what, clusterId, value);
            return 0;
        }
        if (value > upperBound) {
            logger.warn("clamping {} to {} while folding cluster metadata deltas; clusterId={}, value={}",
                    what, upperBound, clusterId, value);
            return upperBound;
        }
        return value;
    }

    /**
     * Returns the change this delta makes to the cluster's primary-vector count, which follows from
     * {@link #statsOp()}: the running statistics hold exactly one sample per primary vector, so adding a sample adds
     * a primary and removing one removes a primary. The size-threshold predicates need this separately from the
     * statistics themselves.
     *
     * @return {@code +1}, {@code -1} or {@code 0}
     */
    int primaryVectorsDelta() {
        switch (statsOp()) {
            case ADD:
                return 1;
            case REMOVE:
                return -1;
            case NONE:
            default:
                return 0;
        }
    }

    /**
     * Returns the {@link #statesToSet} bit mask, in the same encoding as {@link ClusterMetadata#getStatesCode()}.
     * @return the bit mask of states this delta raises
     */
    int getStatesToSetCode() {
        return ClusterMetadata.State.codeOf(statesToSet());
    }

    /**
     * Returns the {@link #statesToClear} bit mask, in the same encoding as {@link ClusterMetadata#getStatesCode()}.
     * @return the bit mask of states this delta drops
     */
    int getStatesToClearCode() {
        return ClusterMetadata.State.codeOf(statesToClear());
    }

    @Nonnull
    @Override
    public String toString() {
        return "CMD[" + statsOp() +
                (statsOp() == StatsOp.NONE ? "" : "(" + distance() + ")") +
                ", dPrimaryUnderreplicated=" + numPrimaryUnderreplicatedVectorsDelta() +
                ", dReplicated=" + numReplicatedVectorsDelta() +
                ", set=" + statesToSet() +
                ", clear=" + statesToClear() +
                ']';
    }

    /**
     * How a {@link ClusterMetadataDelta} changes the running distance statistics. Each constant carries a stable
     * {@linkplain #getCode() code} so the operation survives a round trip through the persisted tuple.
     */
    enum StatsOp {
        /** Leave the statistics untouched — the delta only changes counts or states. */
        NONE(0),
        /** Add {@link ClusterMetadataDelta#distance()} as a new sample (a primary vector was inserted). */
        ADD(1),
        /** Remove {@link ClusterMetadataDelta#distance()} from the samples (a primary vector was deleted). */
        REMOVE(2);

        private final int code;

        StatsOp(final int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        /**
         * Resolves a persisted {@linkplain #getCode() code} back to its constant.
         *
         * @param code the persisted code
         * @return the matching constant
         * @throws IllegalArgumentException if the code is not one this version understands
         */
        @Nonnull
        public static StatsOp ofCode(final int code) {
            switch (code) {
                case 0:
                    return NONE;
                case 1:
                    return ADD;
                case 2:
                    return REMOVE;
                default:
                    throw new IllegalArgumentException("unknown cluster metadata delta stats op code: " + code);
            }
        }
    }
}
